"""Run untrusted commands as a separate, unprivileged account.

A process running as the workflow user can read the credentialed parent's
``/proc/<pid>/environ`` (the job's credentials are there), rewrite files the
publish step later trusts, or leave a process behind. When ``CI_FIX_AGENT_USER``
names an unprivileged account (the workflows create one), a command runs as
that account over a checkout handed to it for the command, with a fresh home
and temp directory; afterwards every process it owns is killed, every file it
left outside the checkout is deleted, and the checkout is handed back.
``.git`` stays the workflow user's throughout, so the command cannot move
HEAD, add a commit, or install a filter that changes the patch built later.

The workflow user gains root with ``sudo`` on a GitHub-hosted runner, and is
root already in a job's container, which has no sudo. On macOS, which has no
``runuser``, the command runs through ``sudo -u``.
"""

from __future__ import annotations

import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

USER_ENV = "CI_FIX_AGENT_USER"
COMMAND_USER_ENV = "CI_FIX_COMMAND_USER"

# Where an unprivileged account can leave files: the shared temp directories
# (on macOS, also its own under /var/folders) and its own home.
_WRITABLE_ROOTS = (
    ("/private/tmp", "/private/var/tmp", "/private/var/folders") if sys.platform == "darwin"
    else ("/tmp", "/var/tmp", "/dev/shm", "/run/lock", "/var/crash")
)
_CLEAN_SYSTEM_ENV = {
    "HOME": "/",
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
}


@dataclass(frozen=True)
class IsolatedInvocation:
    """A user-switch launcher and the scrubbed environment it must inherit.

    Environment values deliberately stay out of ``argv``. In particular, the
    short-lived AWS credentials used by the model must never be visible in a
    privileged ``sudo``/``runuser`` command line.
    """

    argv: tuple[str, ...]
    env: dict[str, str]


def isolated_user() -> str:
    """The account to run an isolated model as, or "" to run it directly."""
    return _isolated_user(USER_ENV)


def isolated_command_user() -> str:
    """The account to run target build and test commands as."""
    return _isolated_user(COMMAND_USER_ENV)


def _isolated_user(variable: str) -> str:
    """The account named by ``variable``, required inside GitHub Actions.

    Inside GitHub Actions the account is required: running a command that can
    reach the job's credentials is never the fallback.
    """
    user = os.environ.get(variable, "").strip()
    if not user and os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        raise RuntimeError(
            f"{variable} is not set; refusing to run untrusted commands as the workflow user"
        )
    return user


@contextmanager
def handed_to(user: str, root: Path, readable: tuple[Path, ...] = ()) -> Iterator[str]:
    """Give ``root``'s files (not ``.git``) to ``user``; yield a fresh home directory.

    ``readable`` directories are made readable to ``user`` for the command.
    ``chown -h -P`` changes symbolic links themselves and never follows them,
    so a link in the checkout cannot redirect the privileged chown.
    """
    root = root.resolve(strict=True)
    readable = tuple(directory.resolve(strict=True) for directory in readable)
    if root.parent == root:
        raise ValueError(f"refusing to hand filesystem root {root} to {user}")
    for directory in readable:
        if directory.parent == directory:
            raise ValueError(
                f"refusing to make filesystem root {directory} recursively readable"
            )
    # Nothing owned by this identity may still be running while privileged
    # code inspects or changes a tree it previously controlled.
    _quiesce(user)
    checked: set[Path] = set()
    for path in (root, *readable):
        if mounts := _nested_mounts(path):
            raise OSError(
                f"refusing recursive access to {path}, which contains mount points: "
                f"{', '.join(str(item) for item in mounts[:5])}"
            )
        # Check from the filesystem root down, so the error names the first
        # component the account cannot cross. Avoid repeating the same
        # privileged probes for parents shared by the checkout and logs.
        for parent in reversed(path.parents):
            if parent in checked:
                continue
            checked.add(parent)
            if not _traversable(user, parent):
                raise OSError(f"{user} cannot reach {path}: {parent} is not traversable")
    for directory in readable:
        # ``--`` before the mode: macOS's chmod reads its first operand as
        # the mode, so a ``--`` after it is taken for a file name.
        _as_root("chmod", "-R", "--", "a+rX", str(directory))
    runner = f"{os.getuid()}:{os.getgid()}"
    account = pwd.getpwnam(user)
    home = tempfile.mkdtemp(prefix="ci-fix-home-", dir=root.parent)
    root_identity = _identity(root)
    home_identity = _identity(Path(home))
    git_dir = root / ".git"
    git_identity = _identity(git_dir)
    # ``-P`` does not help against a hard link: the chown would reach the
    # file it names, wherever that is. A checkout has none; one is refused.
    linked = _hard_links(root)
    if linked:
        raise OSError(f"{root} holds hard links, which the chown would follow: {', '.join(linked[:5])}")
    _as_root("chown", "-R", "-h", "-P", "--", f"{account.pw_uid}:{account.pw_gid}", str(root), home)
    if git_identity is not None:
        _as_root("chown", "-R", "-h", "-P", "--", runner, str(git_dir))
    try:
        yield home
    finally:
        _reclaim(
            user,
            root,
            home,
            runner,
            git_dir,
            root_identity,
            home_identity,
            git_identity,
        )


def _reclaim(
    user: str,
    root: Path,
    home: str,
    runner: str,
    git_dir: Path,
    root_identity: tuple[int, int] | None,
    home_identity: tuple[int, int] | None,
    git_identity: tuple[int, int] | None,
) -> None:
    """Reclaim one tainted checkout and report every failed safety mechanism."""
    try:
        _quiesce(user)
    except Exception:
        # A live process could race every check below. Do not recurse into any
        # path the account controlled unless quiescence is complete.
        raise

    errors: list[Exception] = []
    detached: list[Path] = []
    for path in (root, Path(home)):
        try:
            detached.extend(_detach_nested_mounts(path))
        except Exception as exc:
            errors.append(exc)

    # These checks are adjacent to mount disarm and precede every privileged
    # recursive operation. A swapped .git could otherwise conceal hard links
    # from the pre-chown scan.
    if _identity(root) != root_identity:
        errors.append(OSError(f"{user} replaced {root}; the checkout can no longer be trusted"))
    if _identity(Path(home)) != home_identity:
        errors.append(OSError(f"{user} replaced {home}; its home can no longer be trusted"))
    if _identity(git_dir) != git_identity:
        errors.append(OSError(f"{user} replaced {git_dir}; the checkout can no longer be trusted"))
    _raise_reclaim_errors(root, errors)

    try:
        # Where hard links are not protected (macOS), ``user`` could have
        # linked a file of the runner's, or root's, into the checkout.
        _remove_hard_links(root)
        _as_root("chown", "-R", "-h", "-P", "--", runner, str(root))
        if sys.platform == "darwin":
            _as_root(
                "find", str(root), "-xdev", "-type", "d",
                "-exec", "chmod", "-N", "{}", ";",
                "-exec", "chmod", "u+rwx,go-w", "{}", ";",
            )
            _as_root(
                "find", str(root), "-xdev", "-type", "f",
                "-exec", "chmod", "-N", "{}", "+",
            )
        else:
            _as_root(
                "find", str(root), "-xdev", "-type", "d",
                "-exec", "chmod", "u+rwx,go-w", "{}", ";",
            )
            # The account may have left access or inheritable default ACLs.
            # CI-fix never relies on them; remove both before another identity
            # receives this persistent build tree.
            _as_root("setfacl", "-R", "-b", "-k", "-P", "--", str(root))
        _as_root(
            "find", str(root), "-xdev", "-type", "f",
            "-exec", "chmod", "u+rw,go-w", "{}", "+",
        )
        special = _special_files(root)
        if special:
            _as_root(
                "find", str(root), "-xdev", "-mindepth", "1",
                "!", "-type", "d", "!", "-type", "f", "!", "-type", "l",
                "-delete",
            )
        _remove_nested_git_dirs(root)
        if special:
            errors.append(
                OSError(
                    f"{user} left {len(special)} non-regular filesystem object(s) "
                    "in the checkout"
                )
            )
    except Exception as exc:
        errors.append(exc)
    try:
        _as_root("rm", "-rf", "--", home)
    except Exception as exc:
        errors.append(exc)
    try:
        _remove_files_of(user)
    except Exception as exc:
        errors.append(exc)
    if detached:
        errors.append(
            OSError(
                f"{user} mounted {len(set(detached))} filesystem(s) in isolated paths; "
                "they were detached and the checkout was rejected"
            )
        )
    _raise_reclaim_errors(root, errors)


def _raise_reclaim_errors(root: Path, errors: list[Exception]) -> None:
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"could not safely reclaim {root}", errors)


def _traversable(user: str, directory: Path) -> bool:
    """Whether ``user`` can pass through ``directory``.

    Resolve ``directory/.`` as the account itself. Making the directory a
    non-final path component forces the kernel to check search permission,
    including any platform-specific access rules, without requiring permission
    to list the directory.
    This deliberately does not use ``test -x`` or infer access from mode bits:
    those can disagree with the kernel's access decision on runner images.
    """
    probe = [
        _program("env"), "-i",
        _program("stat"), os.path.join(str(directory), "."),
    ]
    if sys.platform == "darwin":
        argv = [_program("sudo"), "-n", "-u", user, "--", *probe]
    else:
        argv = [*_root(), _program("runuser"), "-u", user, "--", *probe]
    return subprocess.run(argv, capture_output=True, check=False).returncode == 0


# Every file under ``root`` with more than one link, ``.git`` aside, which the
# workflow user keeps and git never hard-links.
def _hard_link_find(root: Path) -> list[str]:
    git_dir = str(root / ".git")
    return [
        "find", str(root), "-xdev", "-mindepth", "1",
        "!", "-path", git_dir, "!", "-path", f"{git_dir}/*", "!", "-type", "d", "-links", "+1",
    ]


def _hard_links(root: Path) -> list[str]:
    """The hard links under ``root``, read as the workflow user, who holds the checkout between sessions."""
    command = _hard_link_find(root)
    command[0] = _program(command[0])
    result = subprocess.run([*command, "-print"], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise OSError(f"find in {root} failed: {result.stderr.strip()}")
    return [line for line in result.stdout.splitlines() if line]


def _remove_hard_links(root: Path) -> None:
    _as_root(*_hard_link_find(root), "-delete")


def _remove_nested_git_dirs(root: Path) -> None:
    """Delete every ``.git`` (directory or gitlink file) under ``root`` but its own."""
    _as_root(
        "find", str(root), "-mindepth", "2", "-name", ".git", "-prune",
        "-exec", "rm", "-rf", "{}", "+",  # paths under ``root``: none starts with a dash
    )


def _special_files(root: Path) -> list[bytes]:
    """Non-directory, non-regular, non-symlink nodes below ``root``."""
    result = subprocess.run(
        [
            _program("find"), str(root), "-xdev", "-mindepth", "1",
            "!", "-type", "d", "!", "-type", "f", "!", "-type", "l",
            "-print0",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise OSError(f"find in {root} failed while checking filesystem object types")
    output = result.stdout.encode() if isinstance(result.stdout, str) else result.stdout
    return [path for path in output.split(b"\0") if path]


def _identity(path: Path) -> tuple[int, int] | None:
    """(device, inode) of a real directory at ``path``, without following links."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    return (info.st_dev, info.st_ino) if stat.S_ISDIR(info.st_mode) else (-1, -1)


def _nested_mounts(root: Path) -> list[Path]:
    """Physical mount points at or below ``root``, deepest first."""
    root = root.resolve()
    if sys.platform == "linux":
        try:
            lines = Path("/proc/self/mountinfo").read_text(
                encoding="utf-8",
                errors="surrogateescape",
            ).splitlines()
        except OSError as exc:
            raise OSError("could not inspect Linux mount points") from exc
        values = [
            _unescape_mount_path(fields[4])
            for line in lines
            if len(fields := line.split()) >= 5
        ]
    elif sys.platform == "darwin":
        result = subprocess.run(
            [_program("mount")],
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if result.returncode != 0:
            raise OSError(f"could not inspect macOS mount points: {result.stderr.strip()}")
        values = []
        marker = f" on {root}"
        for line in result.stdout.splitlines():
            body, separator, _options = line.rpartition(" (")
            if not separator:
                raise OSError("could not parse macOS mount points")
            start = body.rfind(marker)
            if start >= 0:
                values.append(_unescape_mount_path(body[start + 4 :]))
    else:
        return []

    mounts: list[Path] = []
    for value in values:
        path = Path(value)
        try:
            path.relative_to(root)
        except ValueError:
            continue
        mounts.append(path)
    return sorted(set(mounts), key=lambda path: len(path.parts), reverse=True)


def _unescape_mount_path(value: str) -> str:
    """Decode the octal escapes used by mountinfo and BSD mount output."""
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _detach_nested_mounts(root: Path) -> list[Path]:
    """Detach every mount at or below ``root``; return the mounts that were found."""
    mounts = _nested_mounts(root)
    for mount in mounts:
        result = subprocess.run(
            [*_root(), _program("umount"), str(mount)],
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if result.returncode != 0:
            raise OSError(f"could not detach mount {mount}: {result.stderr.strip()}")
    if remaining := _nested_mounts(root):
        raise OSError(
            f"mount points survived cleanup below {root}: "
            f"{', '.join(str(item) for item in remaining[:5])}"
        )
    return mounts


def as_user(
    user: str,
    env: dict[str, str],
    argv: list[str],
    *,
    home: str,
) -> IsolatedInvocation:
    """Describe ``argv`` as ``user`` with a scrubbed inherited environment.

    The values are passed in the child process environment, never as
    ``KEY=value`` command-line arguments. The account switch preserves only
    this already-scrubbed environment.
    """
    if not argv:
        raise ValueError("an isolated invocation needs a command")
    scrubbed = {
        **env, "HOME": home, "TMPDIR": home, "TMP": home, "USER": user, "LOGNAME": user,
        # The worktree is the account's but .git is not; let git read it.
        "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "safe.directory", "GIT_CONFIG_VALUE_0": "*",
    }
    # ``env`` resolves the requested command from the scrubbed PATH after the
    # UID switch. It does not receive values as arguments.
    command = [_program("env"), *argv]
    if sys.platform == "darwin":
        return IsolatedInvocation(
            (_program("sudo"), "-n", "-E", "-u", user, "--", *command),
            scrubbed,
        )
    # runuser, not ``sudo -u``: it needs only root, which a runner's sudo
    # grants. no-new-privileges prevents target code from using a setuid
    # helper such as newuidmap to escape cleanup under a subordinate host UID.
    root = () if os.geteuid() == 0 else (_program("sudo"), "-n", "-E")
    return IsolatedInvocation(
        (
            *root, _program("runuser"), "--preserve-environment", "-u", user, "--",
            _program("setpriv"), "--no-new-privs", "--", *command,
        ),
        scrubbed,
    )


def _program(name: str) -> str:
    return shutil.which(name, path=_CLEAN_SYSTEM_ENV["PATH"]) or name


def _remove_files_of(user: str) -> None:
    roots = [root for root in (*_WRITABLE_ROOTS, pwd.getpwnam(user).pw_dir) if os.path.isdir(root)]
    _as_root("find", *roots, "-xdev", "-mindepth", "1", "-user", user, "-delete")


def clear_delayed_jobs(user: str) -> None:
    """Remove cron and at jobs owned by a disposable isolation account.

    A live-process sweep cannot see a future cron/at invocation. Clear those
    schedulers before and after every handoff so an earlier uncredentialed
    phase cannot wake during a later credentialed one.
    """
    crontab = shutil.which("crontab", path=_CLEAN_SYSTEM_ENV["PATH"])
    if crontab:
        removed = subprocess.run(
            [*_root(), crontab, "-u", user, "-r"],
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if removed.returncode != 0 and not _no_crontab(removed.stderr):
            raise OSError(f"could not clear crontab for {user}: {removed.stderr.strip()}")
        listed = subprocess.run(
            [*_root(), crontab, "-u", user, "-l"],
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if listed.returncode == 0 and listed.stdout.strip():
            raise OSError(f"crontab entries for {user} survived cleanup")
        if listed.returncode != 0 and not _no_crontab(listed.stderr):
            raise OSError(f"could not verify crontab cleanup for {user}: {listed.stderr.strip()}")

    atq = shutil.which("atq", path=_CLEAN_SYSTEM_ENV["PATH"])
    if not atq:
        return
    atrm = shutil.which("atrm", path=_CLEAN_SYSTEM_ENV["PATH"])
    if not atrm:
        raise OSError("atq is installed but atrm is unavailable")
    jobs = _at_jobs(user, atq)
    for job in jobs:
        removed = subprocess.run(
            [*_as_account(user), atrm, job],
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if removed.returncode != 0:
            raise OSError(f"could not remove at job {job} for {user}: {removed.stderr.strip()}")
    if remaining := _at_jobs(user, atq):
        raise OSError(f"at jobs for {user} survived cleanup: {', '.join(remaining)}")


def _no_crontab(stderr: str) -> bool:
    text = stderr.casefold()
    return "no crontab" in text or "no crontab for" in text


def _at_jobs(user: str, atq: str) -> list[str]:
    result = subprocess.run(
        [*_as_account(user), atq],
        capture_output=True,
        text=True,
        check=False,
        env=_CLEAN_SYSTEM_ENV,
    )
    if result.returncode != 0:
        raise OSError(f"could not list at jobs for {user}: {result.stderr.strip()}")
    jobs: list[str] = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if not fields:
            continue
        job = fields[-1] if sys.platform == "darwin" else fields[0]
        if job.isdigit():
            jobs.append(job)
            continue
        # BSD atq prints a heading; no other unparsed output is safe to ignore.
        if {"date", "owner", "queue", "job"}.intersection(word.casefold().rstrip("#") for word in fields):
            continue
        raise OSError(f"could not parse atq output for {user}: {line}")
    return jobs


def _as_account(user: str) -> tuple[str, ...]:
    if sys.platform == "darwin":
        return (_program("sudo"), "-n", "-u", user, "--")
    return (*_root(), _program("runuser"), "-u", user, "--")


def _quiesce(user: str) -> None:
    """Stop current and delayed work of ``user``, attempting every cleanup."""
    quiesce_account(user)


def quiesce_account(user: str, *, expected_uid: int | None = None) -> None:
    """Stop work owned by one verified disposable account.

    ``expected_uid`` lets cleanup keep killing processes after the passwd
    entry has disappeared, while refusing to act if a fixed account name was
    rebound to a different identity.
    """
    try:
        account = pwd.getpwnam(user)
    except KeyError:
        if expected_uid is None:
            return
        kill_uid(expected_uid)
        return
    if expected_uid is not None and account.pw_uid != expected_uid:
        raise RuntimeError(
            f"{user} has uid {account.pw_uid}, expected disposable uid {expected_uid}"
        )
    uid = account.pw_uid
    errors: list[Exception] = []
    for operation in (
        lambda: kill_uid(uid),
        lambda: clear_delayed_jobs(user),
        lambda: kill_uid(uid),
    ):
        try:
            operation()
        except Exception as exc:  # cleanup must continue after one failed mechanism
            errors.append(exc)
    if errors:
        raise ExceptionGroup(f"could not quiesce {user}", errors)


def kill_all(user: str) -> None:
    """Kill every process of ``user``, and wait until none can run.

    A command run as ``user`` is started through root's ``sudo`` or
    ``runuser``, which the workflow user cannot signal: on a timeout this is
    how it is stopped, and the root-owned wrapper then exits by itself.
    """
    kill_uid(pwd.getpwnam(user).pw_uid)


def kill_uid(uid: int) -> None:
    """Kill every process with real or effective ``uid``."""
    deadline = time.monotonic() + 30
    while True:
        processes = _account_processes(uid)
        if not processes:
            return
        for pid, _state in processes:
            subprocess.run(
                [*_root(), _program("kill"), "-KILL", str(pid)],
                capture_output=True,
                check=False,
                env=_CLEAN_SYSTEM_ENV,
            )
        if time.monotonic() > deadline:
            raise OSError(f"processes of uid {uid} survived the command")
        time.sleep(0.2)


def _live_processes(uid: int) -> list[str]:
    """The states of ``uid``'s processes that can still run, zombies excluded."""
    return [state for _pid, state in _account_processes(uid)]


def _account_processes(uid: int) -> list[tuple[int, str]]:
    """Processes whose real or effective UID is ``uid``, zombies excluded.

    In a job's container PID 1 is ``tail -f /dev/null``, which never reaps:
    a killed process the command orphaned (a test server) stays a zombie for
    good. A zombie runs nothing and holds nothing open, so it is gone.
    """
    listed = subprocess.run(
        [
            _program("ps"), "-A", "-o", "pid=", "-o", "ruid=", "-o", "uid=", "-o", "stat=",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_CLEAN_SYSTEM_ENV,
    )
    if listed.returncode != 0:
        raise OSError(f"ps failed: {listed.stderr.strip()}")
    processes = []
    for line in listed.stdout.splitlines():
        fields = line.split()
        if (
            len(fields) == 4
            and (fields[1] == str(uid) or fields[2] == str(uid))
            and not fields[3].startswith("Z")
        ):
            processes.append((int(fields[0]), fields[3]))
    return processes


def _root() -> list[str]:
    """What runs a command as root: nothing for root itself, else sudo."""
    return [] if os.geteuid() == 0 else [_program("sudo"), "-n"]


def _as_root(*argv: str) -> None:
    command = (_program(argv[0]), *argv[1:])
    result = subprocess.run(
        [*_root(), *command],
        capture_output=True,
        text=True,
        check=False,
        env=_CLEAN_SYSTEM_ENV,
    )
    if result.returncode != 0:
        raise OSError(f"{' '.join(argv)} as root failed: {result.stderr.strip()}")
