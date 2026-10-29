"""Create the one isolated checkout used by every CI-fix phase.

The runner workspace is reserved for trusted action state. Target setup, test
commands, and the edit/review agents instead share one physical checkout
created directly below the operating system's canonical temporary directory.
The path never changes, so generated build files may safely embed it.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from scripts.ci_fix.ci_job import JobSpec
from scripts.ci_fix.models import FixPlan
from scripts.ci_fix.paths import checkout_subdirectory
from scripts.ci_fix.run import JOB_FILE, PLAN_FILE
from scripts.ci_fix.state import load
from scripts.common.git_clone import REPO_RE, is_full_sha, shallow_clone_at_sha
from scripts.common.proc import git_output

_STATE_HEADER = "ci-fix-sandbox-v2"
_PREFIX = "valkey-ci-fix-"
_REPO_NAME = "repo"
_CLEANUP_PREFIX = ".valkey-ci-fix-cleanup-"
_CANONICAL_TEMP_ROOTS = frozenset({Path("/tmp"), Path("/private/tmp")})
_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")
_MINIMUM_FREE_BYTES = 1024 * 1024 * 1024


@dataclass(frozen=True)
class WorkspaceState:
    """Trusted identity of the checkout carried between workflow steps."""

    path: Path
    repository: str
    commit: str
    in_job: bool
    pull_number: int
    anchor_device: int
    anchor_inode: int


def create_workspace(plan_dir: Path, state_file: Path) -> WorkspaceState:
    """Clone the plan's exact checkout and seal its physical pathname."""
    plan_root = plan_dir.resolve(strict=True)
    state_path = _state_path(state_file)
    if state_path.exists() or state_path.is_symlink():
        raise ValueError("the CI-fix sandbox state already exists")

    plan = load(plan_root / PLAN_FILE, FixPlan)
    repository = plan.task.failure.repo_full_name
    commit = plan.task.target.expected_sha or plan.task.failure.head_sha
    if not REPO_RE.fullmatch(repository):
        raise ValueError("the planned CI-fix repository is malformed")
    if not is_full_sha(commit):
        raise ValueError("the planned CI-fix commit is not a full SHA")

    root = canonical_temp_root()
    if shutil.disk_usage(root).free < _MINIMUM_FREE_BYTES:
        raise RuntimeError(
            "the canonical temporary filesystem has less than 1 GiB available"
        )
    anchor = Path(tempfile.mkdtemp(prefix=_PREFIX, dir=root))
    anchor.chmod(0o711)
    workspace = anchor / _REPO_NAME
    try:
        _verify_executable(anchor)
        if not shallow_clone_at_sha(
            repository,
            workspace,
            commit,
            pull_number=plan.task.failure.pr_number,
        ):
            raise RuntimeError(
                f"could not clone {repository} at the exact planned commit"
            )
        head = git_output(str(workspace), "rev-parse", "HEAD").strip()
        if head.lower() != commit.lower():
            raise RuntimeError(
                f"the isolated checkout resolved {head[:12]} instead of {commit[:12]}"
            )

        job_file = plan_root / JOB_FILE
        in_job = job_file.is_file()
        if in_job:
            spec = load(job_file, JobSpec)
            if spec.checkout is not None:
                checkout = spec.checkout
                if not REPO_RE.fullmatch(checkout.repository):
                    raise ValueError("the planned extra repository is malformed")
                if not is_full_sha(checkout.ref):
                    raise ValueError("the planned extra checkout is not pinned to a full SHA")
                relative = checkout_subdirectory(checkout.path)
                destination = workspace / relative
                _prepare_parents(workspace, relative)
                if not shallow_clone_at_sha(
                    checkout.repository,
                    destination,
                    checkout.ref,
                ):
                    raise RuntimeError(
                        "could not clone the job's extra repository at its exact commit"
                    )
                extra_head = git_output(str(destination), "rev-parse", "HEAD").strip()
                if extra_head.lower() != checkout.ref.lower():
                    raise RuntimeError(
                        "the job's extra checkout resolved a different commit"
                    )

        anchor_info = anchor.lstat()
        state = WorkspaceState(
            _validate_workspace(workspace, must_exist=True),
            repository,
            commit.lower(),
            in_job,
            plan.task.failure.pr_number,
            anchor_info.st_dev,
            anchor_info.st_ino,
        )
        _write_state(state_path, state)
        return state
    except BaseException:
        shutil.rmtree(anchor, ignore_errors=True)
        raise


def read_workspace_state(state_file: Path) -> WorkspaceState:
    """Read and revalidate the sealed sandbox state."""
    state_path = _state_path(state_file)
    if state_path.is_symlink() or not state_path.is_file():
        raise ValueError("the CI-fix sandbox state is missing or redirected")
    lines = state_path.read_text(encoding="utf-8").splitlines()
    if len(lines) != 8 or lines[0] != _STATE_HEADER:
        raise ValueError("the CI-fix sandbox state is malformed")
    repository, commit, mode, raw_pull_number, raw_device, raw_inode = lines[2:]
    if not REPO_RE.fullmatch(repository) or not is_full_sha(commit):
        raise ValueError("the CI-fix sandbox identity is malformed")
    if mode not in {"job", "local"}:
        raise ValueError("the CI-fix sandbox mode is malformed")
    try:
        pull_number = int(raw_pull_number)
    except ValueError as exc:
        raise ValueError("the CI-fix pull number is malformed") from exc
    if pull_number < 0 or str(pull_number) != raw_pull_number:
        raise ValueError("the CI-fix pull number is malformed")
    try:
        anchor_device = int(raw_device)
        anchor_inode = int(raw_inode)
    except ValueError as exc:
        raise ValueError("the CI-fix sandbox anchor identity is malformed") from exc
    if anchor_device < 0 or anchor_inode <= 0:
        raise ValueError("the CI-fix sandbox anchor identity is malformed")
    workspace = _validate_workspace(Path(lines[1]), must_exist=True)
    anchor_info = workspace.parent.lstat()
    if (anchor_info.st_dev, anchor_info.st_ino) != (anchor_device, anchor_inode):
        raise ValueError("the CI-fix sandbox anchor changed identity")
    return WorkspaceState(
        workspace,
        repository,
        commit.lower(),
        mode == "job",
        pull_number,
        anchor_device,
        anchor_inode,
    )


def cleanup_workspace(state_file: Path) -> None:
    """Delete only the sealed sandbox anchor after accounts are quiescent."""
    state_path = _state_path(state_file)
    if not state_path.exists() and not state_path.is_symlink():
        return
    anchor, anchor_identity = _cleanup_anchor_from_state(state_path)
    try:
        cleanup = _stage_for_cleanup(anchor, anchor_identity)
    except OSError:
        # An exact mount on the anchor can make the rename busy.
        _disarm_tree(anchor, anchor)
        try:
            cleanup = _stage_for_cleanup(anchor, anchor_identity)
        except BaseException:
            _quarantine(anchor)
            raise
    except BaseException:
        _quarantine(anchor)
        raise
    try:
        _disarm_tree(cleanup, cleanup)
        # The protected rename removed every pathname known to disposable
        # accounts. This final check is adjacent to the only recursive delete.
        if _descendant_mounts(cleanup):
            raise RuntimeError(
                "the quarantined CI-fix tree acquired a mount before deletion"
            )
        shutil.rmtree(cleanup)
    except BaseException:
        _quarantine(cleanup)
        raise
    state_path.unlink()


def _disarm_tree(root: Path, anchor: Path) -> None:
    try:
        mounts = _descendant_mounts(root)
        for mount in sorted(mounts, key=lambda path: len(path.parts), reverse=True):
            _unmount(mount)
        remaining = _descendant_mounts(root)
    except BaseException:
        _quarantine(anchor)
        raise
    if remaining:
        _quarantine(anchor)
        raise RuntimeError(
            "the CI-fix sandbox still contains a mounted filesystem; "
            "it was quarantined instead of recursively traversed"
        )


def canonical_temp_root() -> Path:
    """Return the physical system temporary root accepted by CI-fix."""
    root = Path("/tmp").resolve(strict=True)
    try:
        mode = root.lstat().st_mode
    except OSError as exc:
        raise ValueError("the canonical temporary directory is unavailable") from exc
    if root not in _CANONICAL_TEMP_ROOTS or not stat.S_ISDIR(mode) or root.parent == root:
        raise ValueError("the canonical temporary directory is not /tmp or /private/tmp")
    return root


def _validate_workspace(path: Path, *, must_exist: bool) -> Path:
    if not path.is_absolute() or path.name != _REPO_NAME:
        raise ValueError("the CI-fix sandbox path is not recognized")
    try:
        physical = path.resolve(strict=must_exist)
    except OSError as exc:
        raise ValueError("the CI-fix sandbox path is unavailable") from exc
    root = canonical_temp_root()
    anchor = physical.parent
    if (
        physical != path
        or anchor.parent != root
        or not anchor.name.startswith(_PREFIX)
        or anchor.is_symlink()
    ):
        raise ValueError(
            "the CI-fix sandbox must be below a physical canonical-temp anchor"
        )
    try:
        anchor_mode = anchor.lstat().st_mode
    except OSError as exc:
        raise ValueError("the CI-fix sandbox anchor is unavailable") from exc
    if not stat.S_ISDIR(anchor_mode) or stat.S_IMODE(anchor_mode) != 0o711:
        raise ValueError("the CI-fix sandbox anchor has unsafe permissions")
    if must_exist:
        try:
            mode = path.lstat().st_mode
        except OSError as exc:
            raise ValueError("the CI-fix sandbox path is unavailable") from exc
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise ValueError("the CI-fix sandbox is not a physical directory")
    return physical


def _state_path(path: Path) -> Path:
    if not path.is_absolute() or "\n" in str(path):
        raise ValueError("the CI-fix sandbox state path is invalid")
    parent = path.parent.resolve(strict=True)
    if path.parent != parent:
        raise ValueError("the CI-fix sandbox state parent is redirected")
    return path


def _prepare_parents(workspace: Path, relative: PurePosixPath) -> None:
    parent = workspace
    for component in relative.parts[:-1]:
        parent = parent / component
        try:
            mode = parent.lstat().st_mode
        except FileNotFoundError:
            parent.mkdir(mode=0o755)
            continue
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise ValueError("the planned extra checkout has an unsafe parent")


def _write_state(path: Path, state: WorkspaceState) -> None:
    payload = "\n".join(
        (
            _STATE_HEADER,
            str(state.path),
            state.repository,
            state.commit,
            "job" if state.in_job else "local",
            str(state.pull_number),
            str(state.anchor_device),
            str(state.anchor_inode),
            "",
        )
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _descendant_mounts(anchor: Path) -> set[Path]:
    """Return listed mount points and foreign-device directory roots below anchor."""
    anchor = anchor.resolve(strict=True)
    listed = {
        mount
        for mount in _listed_mounts(anchor)
        if mount == anchor or _is_below(mount, anchor)
    }
    return listed | _foreign_device_roots(anchor, listed)


def _listed_mounts(anchor: Path) -> set[Path]:
    mountinfo = Path("/proc/self/mountinfo")
    if mountinfo.is_file():
        mounts: set[Path] = set()
        for line in mountinfo.read_text(encoding="utf-8").splitlines():
            fields = line.split()
            if len(fields) < 5:
                raise RuntimeError("the kernel mount table is malformed")
            mounts.add(_mount_path(fields[4]))
        return mounts

    mount_bin = Path("/sbin/mount")
    if not mount_bin.is_file() or not os.access(mount_bin, os.X_OK):
        raise RuntimeError("a trusted mount-table reader is unavailable")
    result = subprocess.run(
        (str(mount_bin),),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"},
    )
    if result.returncode:
        raise RuntimeError("the operating-system mount table could not be read")
    return _macos_mounts(result.stdout, anchor)


def _macos_mounts(output: str, anchor: Path) -> set[Path]:
    mounts = set()
    marker = f" on {anchor}"
    for line in output.splitlines():
        body, separator, _options = line.rpartition(" (")
        if not separator:
            raise RuntimeError("the operating-system mount table is malformed")
        start = body.rfind(marker)
        if start < 0:
            continue
        mount = Path(body[start + 4 :])
        if mount == anchor or _is_below(mount, anchor):
            mounts.add(mount)
    return mounts


def _mount_path(encoded: str) -> Path:
    decoded = _MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), encoded)
    path = Path(decoded)
    if not path.is_absolute():
        raise RuntimeError("the operating-system mount table contains a relative path")
    return Path(os.path.normpath(path))


def _foreign_device_roots(anchor: Path, listed_mounts: set[Path]) -> set[Path]:
    """Find device transitions without ever descending into a known mount."""
    if anchor in listed_mounts:
        return set()
    root_device = anchor.lstat().st_dev
    foreign: set[Path] = set()
    pending = [anchor]
    while pending:
        parent = pending.pop()
        with os.scandir(parent) as entries:
            for entry in entries:
                mode = entry.stat(follow_symlinks=False)
                if not stat.S_ISDIR(mode.st_mode):
                    continue
                path = Path(entry.path)
                if path in listed_mounts:
                    continue
                if mode.st_dev != root_device:
                    foreign.add(path)
                    continue
                pending.append(path)
    return foreign


def _is_below(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return path != parent


def _unmount(path: Path) -> bool:
    candidates = (Path("/sbin/umount"), Path("/usr/bin/umount"), Path("/bin/umount"))
    binary = next(
        (
            candidate
            for candidate in candidates
            if candidate.is_file() and os.access(candidate, os.X_OK)
        ),
        None,
    )
    if binary is None:
        return False
    commands: list[tuple[str, ...]] = [(str(binary), str(path))]
    if Path("/proc/self/mountinfo").is_file():
        commands.append((str(binary), "-l", str(path)))
    else:
        commands.append((str(binary), "-f", str(path)))
    for command in commands:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"},
        )
        if result.returncode == 0:
            return True
    return False


def _quarantine(anchor: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(anchor, flags)
    except OSError:
        return
    try:
        os.fchmod(fd, 0)
    except OSError:
        pass
    finally:
        os.close(fd)


def _cleanup_anchor_from_state(state_path: Path) -> tuple[Path, tuple[int, int]]:
    """Read the protected anchor identity without traversing its tainted repo."""
    lines = state_path.read_text(encoding="utf-8").splitlines()
    if len(lines) != 8 or lines[0] != _STATE_HEADER:
        raise ValueError("the CI-fix sandbox state is malformed")
    repo = Path(lines[1])
    root = canonical_temp_root()
    anchor = repo.parent
    if (
        not repo.is_absolute()
        or repo.name != _REPO_NAME
        or anchor.parent != root
        or not anchor.name.startswith(_PREFIX)
    ):
        raise ValueError("the CI-fix sandbox path is not recognized")
    try:
        identity = (int(lines[6]), int(lines[7]))
    except ValueError as exc:
        raise ValueError("the CI-fix sandbox anchor identity is malformed") from exc
    if identity[0] < 0 or identity[1] <= 0:
        raise ValueError("the CI-fix sandbox anchor identity is malformed")
    return anchor, identity


def _stage_for_cleanup(anchor: Path, expected_identity: tuple[int, int]) -> Path:
    """Atomically hide the entire live anchor below canonical temporary storage."""
    root = canonical_temp_root()
    if anchor.parent != root or not anchor.name.startswith(_PREFIX):
        raise RuntimeError("the CI-fix sandbox anchor is not recognized")
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(root, flags)
    cleanup_name = ""
    try:
        named = os.stat(anchor.name, dir_fd=fd, follow_symlinks=False)
        current = anchor.lstat()
        named_identity = (named.st_dev, named.st_ino)
        current_identity = (current.st_dev, current.st_ino)
        if (
            not stat.S_ISDIR(named.st_mode)
            or named_identity != current_identity
            or named_identity != expected_identity
        ):
            raise RuntimeError("the CI-fix sandbox anchor changed identity")
        cleanup_name = f"{_CLEANUP_PREFIX}{secrets.token_hex(16)}"
        os.mkdir(cleanup_name, mode=0o700, dir_fd=fd)
        os.rename(
            anchor.name,
            f"{cleanup_name}/sandbox",
            src_dir_fd=fd,
            dst_dir_fd=fd,
        )
    except BaseException:
        if cleanup_name:
            try:
                os.rmdir(cleanup_name, dir_fd=fd)
            except OSError:
                pass
        raise
    finally:
        os.close(fd)
    return root / cleanup_name


def _verify_executable(anchor: Path) -> None:
    probe = anchor / ".exec-probe"
    probe.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    probe.chmod(0o700)
    try:
        result = subprocess.run(
            (str(probe),),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        )
    except OSError as exc:
        raise RuntimeError(
            "the canonical temporary filesystem does not permit execution"
        ) from exc
    finally:
        probe.unlink(missing_ok=True)
    if result.returncode:
        raise RuntimeError(
            "the canonical temporary filesystem does not permit execution"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="create and clone the stable sandbox")
    create.add_argument("--plan", type=Path, required=True)
    create.add_argument("--state-file", type=Path, required=True)
    cleanup = commands.add_parser("cleanup", help="remove a previously sealed sandbox")
    cleanup.add_argument("--state-file", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "create":
        create_workspace(args.plan, args.state_file)
    else:
        cleanup_workspace(args.state_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
