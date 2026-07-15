"""Isolated execution of untrusted CI-fix verification commands."""

from __future__ import annotations

import json
import logging
import os
import re
import select
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from scripts.ci_fix.models import RunResult
from scripts.common.proc import (
    NETWORK_ENV,
    PROCESS_BASICS,
    filter_env,
    terminate_process_group,
)
from scripts.common.text_utils import strip_ansi

logger = logging.getLogger(__name__)

DEFAULT_LINUX_VERIFIER_IMAGE = (
    "gcc@sha256:5e927c284bf55a7dc796262e311a0703344f62f41f5621eb56843111b1d37e15"
)
MACOS_SANDBOX_USER = "ci-fix-verifier"

_VERIFY_ENV_ALLOWLIST = PROCESS_BASICS + NETWORK_ENV
_DEFAULT_TIMEOUT_S = 30 * 60
_OUTPUT_TAIL_CHARS = 32 * 1024
_MAX_CAPTURED_BYTES = 8 * 1024 * 1024
_MAX_CHECKOUT_BYTES = 1024 * 1024 * 1024
_MAX_CHECKOUT_ENTRIES = 200_000
_CPU_LIMIT = 2
_MEMORY_MB = 6144
_PID_LIMIT = 512
_WORKSPACE_MB = 4096
_TMPFS_MB = 512
_MACOS_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,30}$")


@dataclass(frozen=True)
class VerificationRuntime:
    """A prepared execution policy that can create fresh isolated sandboxes."""

    platform: str
    requested_image: str
    effective_image: str
    image_identity: str
    sandbox_user: str
    sandbox_uid: int | None

    def sandbox(self, repo_dir: str) -> "_VerificationSandbox":
        return _VerificationSandbox(self, repo_dir)

    def contract(self) -> dict[str, object]:
        return verification_runtime_contract(
            platform=self.platform,
            container_image=self.requested_image,
            image_identity=self.image_identity,
            sandbox_uid=self.sandbox_uid,
        )


def prepare_verification_runtime(
    *,
    platform: str,
    container_image: str = "",
) -> VerificationRuntime:
    """Prepare and attest the platform isolation boundary before target code runs."""
    if platform == "linux":
        requested = container_image or DEFAULT_LINUX_VERIFIER_IMAGE
        identity = _pull_and_resolve_image(requested)
        return VerificationRuntime(
            platform=platform,
            requested_image=container_image,
            effective_image=identity,
            image_identity=identity,
            sandbox_user="numeric-nonroot",
            sandbox_uid=os.getuid(),
        )
    if platform == "macos":
        user = os.environ.get("CI_FIX_MACOS_SANDBOX_USER", "")
        if user != MACOS_SANDBOX_USER or not _MACOS_USER_RE.fullmatch(user):
            raise RuntimeError(
                "macOS validation requires the dedicated ci-fix-verifier user",
            )
        uid_result = _control_run(
            ["/usr/bin/id", "-u", user],
            timeout=10,
        )
        try:
            uid = int(uid_result.stdout.strip())
        except ValueError as exc:
            raise RuntimeError("could not resolve macOS verifier UID") from exc
        if uid <= 0 or uid == os.getuid():
            raise RuntimeError("macOS verifier must use a distinct non-root UID")
        if not Path("/usr/bin/sandbox-exec").is_file():
            raise RuntimeError("macOS Seatbelt sandbox-exec is unavailable")
        if not Path("/usr/bin/sudo").is_file():
            raise RuntimeError("macOS verifier requires non-interactive sudo")
        return VerificationRuntime(
            platform=platform,
            requested_image="",
            effective_image="",
            image_identity="",
            sandbox_user=user,
            sandbox_uid=uid,
        )
    raise RuntimeError(f"unsupported verification platform: {platform!r}")


def verification_runtime_contract(
    *,
    platform: str,
    container_image: str = "",
    image_identity: str = "",
    sandbox_uid: int | None = None,
) -> dict[str, object]:
    """Describe the exact isolation and resource contract for an attestation."""
    resources = {
        "timeout_seconds": _DEFAULT_TIMEOUT_S,
        "captured_output_bytes": _MAX_CAPTURED_BYTES,
        "reported_output_chars": _OUTPUT_TAIL_CHARS,
        "cpu_limit": _CPU_LIMIT,
        "memory_mb": _MEMORY_MB,
        "pid_limit": _PID_LIMIT,
        "workspace_mb": _WORKSPACE_MB,
        "tmpfs_mb": _TMPFS_MB,
    }
    if platform == "linux":
        return {
            "executor": "ephemeral-docker-cgroup",
            "requested_container_image": container_image,
            "effective_container_image": image_identity,
            "container_image_identity": image_identity,
            "platform": "linux/amd64",
            "shell": [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-O",
                "inherit_errexit",
                "-euo",
                "pipefail",
                "-c",
            ],
            "environment_allowlist": sorted(set(_VERIFY_ENV_ALLOWLIST)),
            "network": "none",
            "filesystem": "quota-tmpfs-checkout-rw-without-git",
            "host_mounts": "none",
            "docker_socket": "absent",
            "capabilities": "none",
            "no_new_privileges": True,
            "read_only_root": True,
            "separate_pid_namespace": True,
            "process_group": True,
            "resources": resources,
        }
    if platform == "macos":
        return {
            "executor": "credentialless-macos-vm-seatbelt",
            "requested_container_image": "",
            "effective_container_image": "",
            "container_image_identity": "",
            "platform": "macos",
            "shell": [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-O",
                "inherit_errexit",
                "-euo",
                "pipefail",
                "-c",
            ],
            "environment_allowlist": sorted(set(_VERIFY_ENV_ALLOWLIST)),
            "network": "none-seatbelt-default-deny",
            "filesystem": "quota-apfs-checkout-rw-without-git",
            "host_mounts": "dedicated-apfs-volume-only",
            "docker_socket": "absent",
            "capabilities": "dedicated-non-admin-uid",
            "sandbox_uid": sandbox_uid,
            "no_new_privileges": True,
            "read_only_root": True,
            "separate_pid_namespace": "ephemeral-github-hosted-vm",
            "process_group": True,
            "resources": resources,
        }
    raise ValueError(f"unsupported verification platform: {platform!r}")


def run_verification_command(
    repo_dir: str,
    command: str,
    *,
    workdir: str = "",
    timeout: int = _DEFAULT_TIMEOUT_S,
    env_allowlist: tuple[str, ...] = _VERIFY_ENV_ALLOWLIST,
    container_image: str = "",
) -> RunResult:
    """Fail closed when a caller has not supplied an isolated runtime.

    Production validation obtains a bound runner from
    ``VerificationRuntime.sandbox``. Keeping this signature prevents accidental
    callers of the older host runner from silently executing target code.
    """
    del repo_dir, workdir, timeout, env_allowlist, container_image
    return RunResult(
        ran=False,
        passed=False,
        exit_code=-1,
        command=command.strip(),
        output_tail="direct host verification is disabled; isolated runtime required",
    )


class _VerificationSandbox(AbstractContextManager["_VerificationSandbox"]):
    def __init__(self, runtime: VerificationRuntime, repo_dir: str) -> None:
        self.runtime = runtime
        self.repo_root = Path(repo_dir).resolve()
        self._temporary: tempfile.TemporaryDirectory[str] | None = None
        self._staging: Path | None = None
        self._container_name = ""
        self._macos_mount: Path | None = None
        self._macos_image: Path | None = None
        self._macos_profile = ""

    def __enter__(self) -> "_VerificationSandbox":
        if not self.repo_root.is_dir():
            raise RuntimeError("verification checkout does not exist")
        self._temporary = tempfile.TemporaryDirectory(
            prefix="ci-fix-verification-sandbox-",
        )
        root = Path(self._temporary.name)
        _validate_checkout_size(self.repo_root)
        if self.runtime.platform == "linux":
            self._staging = root / "staging"
            _copy_checkout(self.repo_root, self._staging)
            self._start_linux_container()
        else:
            self._start_macos_sandbox(root)
        self._probe_boundary(root)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        if self.runtime.platform == "linux":
            self._remove_linux_container()
        else:
            self._kill_macos_processes()
            if self._macos_mount is not None:
                _control_run(
                    [
                        "/usr/bin/hdiutil",
                        "detach",
                        "-force",
                        str(self._macos_mount),
                    ],
                    timeout=60,
                    check=False,
                )
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None
        return None

    def run(
        self,
        repo_dir: str,
        command: str,
        *,
        workdir: str = "",
        timeout: int = _DEFAULT_TIMEOUT_S,
        env_allowlist: tuple[str, ...] = _VERIFY_ENV_ALLOWLIST,
        container_image: str = "",
    ) -> RunResult:
        """Run one command in this sandbox and derive the verdict from its exit."""
        del env_allowlist
        command = command.strip()
        if not command:
            return _not_run(command, "empty command")
        if Path(repo_dir).resolve() != self.repo_root:
            return _not_run(command, "sandbox is bound to a different checkout")
        if container_image != self.runtime.requested_image:
            return _not_run(command, "container image differs from prepared runtime")
        relative = _resolve_relative_workdir(self.repo_root, workdir)
        if relative is None:
            return _not_run(
                command,
                f"workdir {workdir!r} escapes or does not exist under repo",
            )

        if self.runtime.platform == "linux":
            args = self._linux_exec_args(command, relative)
            cwd = self.repo_root
        else:
            args = self._macos_exec_args(command, relative, timeout)
            assert self._macos_mount is not None
            cwd = self._macos_mount / "workspace" / relative

        logger.info(
            "Running verification in %s sandbox (timeout=%ds): %s",
            self.runtime.platform,
            timeout,
            command,
        )
        try:
            _, exit_code, output, timed_out = _run_capped(
                args,
                cwd,
                filter_env(("PATH", "HOME")),
                timeout,
            )
        except OSError as exc:
            return _not_run(command, f"failed to start isolated command: {exc}")
        if timed_out:
            if self.runtime.platform == "linux":
                self._remove_linux_container()
            else:
                self._kill_macos_processes()
            return RunResult(
                ran=True,
                passed=False,
                exit_code=-1,
                command=command,
                output_tail=_tail(output) or f"timed out after {timeout}s",
                timed_out=True,
            )
        return RunResult(
            ran=True,
            passed=exit_code == 0,
            exit_code=exit_code,
            command=command,
            output_tail=_tail(output),
        )

    def _start_linux_container(self) -> None:
        self._container_name = f"valkey-ci-fix-{uuid.uuid4().hex[:16]}"
        create = [
            "docker",
            "create",
            "--name",
            self._container_name,
            "--platform",
            "linux/amd64",
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--read-only",
            "--pids-limit",
            str(_PID_LIMIT),
            "--cpus",
            str(_CPU_LIMIT),
            "--memory",
            f"{_MEMORY_MB}m",
            "--tmpfs",
            f"/workspace:rw,nosuid,nodev,size={_WORKSPACE_MB}m,mode=1777",
            "--tmpfs",
            f"/tmp:rw,nosuid,nodev,noexec,size={_TMPFS_MB}m,mode=1777",
            "--ulimit",
            "core=0:0",
            "--ulimit",
            "nofile=1024:1024",
            "--entrypoint",
            "/bin/bash",
            self.runtime.effective_image,
            "--noprofile",
            "--norc",
            "-c",
            "while :; do sleep 3600 & wait $!; done",
        ]
        try:
            _control_run(create, timeout=60)
            _control_run(
                ["docker", "start", self._container_name],
                timeout=30,
            )
            assert self._staging is not None
            self._stream_linux_checkout()
            self._assert_linux_boundary()
        except BaseException:
            self._remove_linux_container()
            raise

    def _stream_linux_checkout(self) -> None:
        assert self._staging is not None
        with tempfile.TemporaryFile(
            prefix="ci-fix-verification-checkout-",
            suffix=".tar",
        ) as archive:
            with tarfile.open(fileobj=archive, mode="w") as bundle:
                for path in sorted(self._staging.iterdir()):
                    bundle.add(path, arcname=path.name, recursive=True)
            archive.seek(0)
            _control_run(
                [
                    "docker",
                    "exec",
                    "-i",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    self._container_name,
                    "/bin/tar",
                    "--extract",
                    "--file",
                    "-",
                    "--directory",
                    "/workspace",
                    "--no-same-owner",
                    "--same-permissions",
                ],
                timeout=300,
                stdin=archive,
            )

    def _linux_exec_args(self, command: str, relative: Path) -> list[str]:
        workdir = "/workspace"
        if relative != Path("."):
            workdir += "/" + relative.as_posix()
        return [
            "docker",
            "exec",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--workdir",
            workdir,
            "--env",
            "CI=1",
            "--env",
            "HOME=/tmp",
            "--env",
            "LANG=C.UTF-8",
            self._container_name,
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-O",
            "inherit_errexit",
            "-euo",
            "pipefail",
            "-c",
            command,
        ]

    def _assert_linux_boundary(self) -> None:
        inspected = _control_run(
            ["docker", "inspect", self._container_name],
            timeout=30,
        )
        try:
            data = json.loads(inspected.stdout)
            item = data[0]
            host = item["HostConfig"]
            mounts = item["Mounts"]
        except (IndexError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("could not inspect verifier container boundary") from exc
        required = (
            host.get("NetworkMode") == "none"
            and host.get("ReadonlyRootfs") is True
            and set(host.get("CapDrop") or ()) == {"ALL"}
            and any(
                str(option).split(":", 1)[0] == "no-new-privileges"
                for option in (host.get("SecurityOpt") or ())
            )
            and host.get("PidsLimit") == _PID_LIMIT
            and host.get("Memory") == _MEMORY_MB * 1024 * 1024
            and host.get("NanoCpus") == _CPU_LIMIT * 1_000_000_000
            and "/workspace" in (host.get("Tmpfs") or {})
            and "/tmp" in (host.get("Tmpfs") or {})
            and not any(mount.get("Type") == "bind" for mount in mounts)
        )
        if not required:
            raise RuntimeError("verifier container does not match isolation policy")

    def _remove_linux_container(self) -> None:
        if not self._container_name:
            return
        _control_run(
            ["docker", "rm", "--force", self._container_name],
            timeout=30,
            check=False,
        )
        self._container_name = ""

    def _start_macos_sandbox(self, root: Path) -> None:
        root.chmod(0o711)
        volume_name = f"ci-fix-{uuid.uuid4().hex[:12]}"
        image = root / "workspace.sparseimage"
        mount = root / "volume"
        mount.mkdir()
        _control_run(
            [
                "/usr/bin/hdiutil",
                "create",
                "-quiet",
                "-size",
                f"{_WORKSPACE_MB}m",
                "-fs",
                "APFS",
                "-type",
                "SPARSE",
                "-volname",
                volume_name,
                str(image),
            ],
            timeout=120,
        )
        _control_run(
            [
                "/usr/bin/hdiutil",
                "attach",
                "-quiet",
                "-nobrowse",
                "-mountpoint",
                str(mount),
                str(image),
            ],
            timeout=120,
        )
        workspace = mount / "workspace"
        _copy_checkout(self.repo_root, workspace)
        (workspace / ".home").mkdir()
        (workspace / ".tmp").mkdir()
        _control_run(
            [
                "/usr/bin/sudo",
                "-n",
                "/usr/sbin/chown",
                "-R",
                self.runtime.sandbox_user,
                str(workspace),
            ],
            timeout=300,
        )
        self._macos_mount = mount
        self._macos_image = image
        self._macos_profile = _macos_profile(workspace)

    def _macos_exec_args(
        self,
        command: str,
        relative: Path,
        timeout: int,
    ) -> list[str]:
        assert self._macos_mount is not None
        workspace = self._macos_mount / "workspace"
        home = workspace / ".home"
        temporary = workspace / ".tmp"
        wrapper = (
            "ulimit -c 0; "
            f"ulimit -f {_WORKSPACE_MB * 2048}; "
            "ulimit -n 1024; "
            f"ulimit -u {_PID_LIMIT}; "
            f"ulimit -v {_MEMORY_MB * 1024}; "
            "ulimit -t \"$3\"; "
            "exec /usr/bin/sandbox-exec -p \"$1\" "
            "/bin/bash --noprofile --norc -O inherit_errexit "
            "-euo pipefail -c \"$2\""
        )
        return [
            "/usr/bin/sudo",
            "-n",
            "-u",
            self.runtime.sandbox_user,
            "/usr/bin/env",
            "-i",
            "CI=1",
            f"HOME={home}",
            f"TMPDIR={temporary}",
            "PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "LANG=C",
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-O",
            "inherit_errexit",
            "-euo",
            "pipefail",
            "-c",
            wrapper,
            "ci-fix-sandbox",
            self._macos_profile,
            command,
            str(max(1, timeout + 60)),
        ]

    def _kill_macos_processes(self) -> None:
        _control_run(
            [
                "/usr/bin/sudo",
                "-n",
                "/usr/bin/pkill",
                "-KILL",
                "-u",
                self.runtime.sandbox_user,
            ],
            timeout=30,
            check=False,
        )

    def _probe_boundary(self, root: Path) -> None:
        if self.runtime.platform == "linux":
            host_secret = root / "host-boundary-secret"
            marker = f"ci-fix-host-secret-{uuid.uuid4().hex}"
            host_secret.write_text(marker, encoding="utf-8")
            probe = self.run(
                str(self.repo_root),
                (
                    "test ! -e .git && "
                    "test ! -S /var/run/docker.sock && "
                    "test ! -S /run/docker.sock && "
                    "test \"$(id -u)\" != 0 && "
                    f"test ! -e {_shell_quote(str(host_secret))} && "
                    f"! grep -a -s {_shell_quote(marker)} /proc/[0-9]*/environ && "
                    "! touch /ci-fix-root-write-probe && "
                    "test ! -e /dev/mem && "
                    "! timeout 3 /bin/bash -c "
                    "'exec 3<>/dev/tcp/169.254.169.254/80'"
                ),
                container_image=self.runtime.requested_image,
                timeout=30,
            )
            if not probe.passed:
                raise RuntimeError(
                    "Linux verifier boundary probe failed: " + probe.output_tail,
                )
            return

        assert self._macos_mount is not None
        read_escape = root / "outside-read-probe"
        read_escape.write_text(
            f"ci-fix-host-secret-{uuid.uuid4().hex}",
            encoding="utf-8",
        )
        escape = root / "outside-write-probe"
        escape.mkdir()
        _control_run(
            [
                "/usr/bin/sudo",
                "-n",
                "/usr/sbin/chown",
                self.runtime.sandbox_user,
                str(escape),
            ],
            timeout=30,
        )
        _control_run(
            [
                "/usr/bin/sudo",
                "-n",
                "/usr/sbin/chown",
                self.runtime.sandbox_user,
                str(read_escape),
            ],
            timeout=30,
        )
        fixed = self.run(
            str(self.repo_root),
            (
                f'test "$(id -u)" = "{self.runtime.sandbox_uid}" '
                "&& test ! -e .git"
            ),
            container_image="",
            timeout=30,
        )
        if not fixed.passed:
            raise RuntimeError(
                "macOS verifier identity probe failed: " + fixed.output_tail,
            )
        network = self.run(
            str(self.repo_root),
            (
                "/usr/bin/python3 - <<'PY'\n"
                "import errno\n"
                "import socket\n"
                "import sys\n"
                "try:\n"
                "    socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
                "except OSError as exc:\n"
                "    sys.exit(0 if exc.errno in (errno.EPERM, errno.EACCES) else 3)\n"
                "sys.exit(2)\n"
                "PY"
            ),
            container_image="",
            timeout=30,
        )
        if not network.passed:
            raise RuntimeError("macOS Seatbelt network denial probe failed")
        read = self.run(
            str(self.repo_root),
            f"/bin/cat {_shell_quote(str(read_escape))}",
            container_image="",
            timeout=30,
        )
        if read.exit_code == 0:
            raise RuntimeError("macOS Seatbelt filesystem read denial probe failed")
        write = self.run(
            str(self.repo_root),
            f"/usr/bin/touch {_shell_quote(str(escape / 'escaped'))}",
            container_image="",
            timeout=30,
        )
        if write.exit_code == 0 or (escape / "escaped").exists():
            raise RuntimeError("macOS Seatbelt filesystem denial probe failed")


def _pull_and_resolve_image(image: str) -> str:
    if shutil.which("docker") is None:
        raise RuntimeError("Docker is required for isolated Linux verification")
    _control_run(
        ["docker", "pull", "--platform", "linux/amd64", image],
        timeout=600,
    )
    inspected = _control_run(
        ["docker", "image", "inspect", "--format={{.Id}}", image],
        timeout=30,
    )
    identity = inspected.stdout.strip().lower()
    if (
        not identity.startswith("sha256:")
        or len(identity) != 71
        or any(character not in "0123456789abcdef" for character in identity[7:])
    ):
        raise RuntimeError("verifier image did not resolve to a sha256 content ID")
    return identity


def _control_run(
    args: list[str],
    *,
    timeout: int,
    check: bool = True,
    stdin: BinaryIO | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=stdin,
            env=filter_env(
                ("PATH", "HOME", "DOCKER_HOST", "DOCKER_TLS_VERIFY"),
            ),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        if not check:
            logger.warning("Isolation cleanup command failed: %s", exc)
            return subprocess.CompletedProcess(args, -1, "", str(exc))
        raise RuntimeError(f"isolation control command failed: {exc}") from exc
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise RuntimeError(
            f"isolation control command exited {result.returncode}: {detail}",
        )
    return result


def _validate_checkout_size(repo_root: Path) -> None:
    entries = 0
    total = 0
    for current, directories, files in os.walk(repo_root, followlinks=False):
        directories[:] = [name for name in directories if name != ".git"]
        entries += len(directories) + len(files)
        if entries > _MAX_CHECKOUT_ENTRIES:
            raise RuntimeError("verification checkout contains too many entries")
        current_path = Path(current)
        for name in files:
            if name == ".git":
                continue
            path = current_path / name
            if path.is_symlink():
                continue
            try:
                total += path.stat().st_size
            except OSError as exc:
                raise RuntimeError(f"could not inspect checkout path {path}") from exc
            if total > _MAX_CHECKOUT_BYTES:
                raise RuntimeError("verification checkout exceeds 1 GiB")


def _copy_checkout(source: Path, destination: Path) -> None:
    shutil.copytree(
        source,
        destination,
        symlinks=True,
        ignore=shutil.ignore_patterns(".git"),
    )


def _resolve_relative_workdir(repo_root: Path, workdir: str) -> Path | None:
    candidate = (repo_root / workdir).resolve() if workdir else repo_root
    if repo_root != candidate and repo_root not in candidate.parents:
        return None
    if not candidate.is_dir():
        return None
    return candidate.relative_to(repo_root) if candidate != repo_root else Path(".")


def _macos_profile(workspace: Path) -> str:
    escaped = str(workspace).replace("\\", "\\\\").replace('"', '\\"')
    return "\n".join(
        (
            "(version 1)",
            "(deny default)",
            "(allow process*)",
            "(allow file-read*",
            f'  (subpath "{escaped}")',
            '  (subpath "/System")',
            '  (subpath "/usr")',
            '  (subpath "/bin")',
            '  (subpath "/sbin")',
            '  (subpath "/Library/Developer")',
            '  (subpath "/Library/Frameworks")',
            '  (subpath "/Applications/Xcode.app")',
            '  (subpath "/opt/homebrew")',
            '  (subpath "/usr/local")',
            '  (subpath "/private/etc")',
            '  (subpath "/private/var/db")',
            '  (subpath "/dev"))',
            "(allow file-ioctl)",
            "(allow sysctl-read)",
            "(allow mach-lookup)",
            "(allow ipc-posix*)",
            f'(allow file-write* (subpath "{escaped}"))',
            '(allow file-write* (literal "/dev/null"))',
            '(allow file-write* (literal "/dev/zero"))',
            '(allow file-write* (literal "/dev/random"))',
            '(allow file-write* (literal "/dev/urandom"))',
        ),
    )


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _not_run(command: str, detail: str) -> RunResult:
    return RunResult(
        ran=False,
        passed=False,
        exit_code=-1,
        command=command,
        output_tail=detail,
    )


def _tail(text: str) -> str:
    text = strip_ansi(text)
    if len(text) <= _OUTPUT_TAIL_CHARS:
        return text
    return "...[truncated]...\n" + text[-_OUTPUT_TAIL_CHARS:]


def _run_capped(
    command: str | list[str],
    cwd: Path,
    env: dict[str, str],
    timeout: int,
) -> tuple[bool, int, str, bool]:
    """Run one command with a wall timeout and bounded tail capture."""
    shell: str | list[str]
    if isinstance(command, str):
        shell = [
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-O",
            "inherit_errexit",
            "-euo",
            "pipefail",
            "-c",
            command,
        ]
    else:
        shell = command
    proc = subprocess.Popen(
        shell,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    assert proc.stdout is not None
    fd = proc.stdout
    buf = bytearray()
    deadline = time.monotonic() + timeout
    timed_out = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            ready, _, _ = select.select([fd], [], [], min(remaining, 1.0))
            if not ready:
                if proc.poll() is not None:
                    break
                continue
            chunk = fd.read1(65536) if hasattr(fd, "read1") else fd.read(65536)
            if not chunk:
                break
            buf.extend(chunk)
            if len(buf) > _MAX_CAPTURED_BYTES:
                del buf[:-_MAX_CAPTURED_BYTES]
    finally:
        fd.close()
        if timed_out:
            terminate_process_group(proc)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            terminate_process_group(proc)
            proc.wait()
    return (
        True,
        proc.returncode if proc.returncode is not None else -1,
        buf.decode("utf-8", errors="replace"),
        timed_out,
    )
