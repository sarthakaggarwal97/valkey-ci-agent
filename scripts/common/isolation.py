"""Run untrusted commands as a separate, unprivileged account.

A process running as the workflow user can read the credentialed parent's
``/proc/<pid>/environ`` (the GitHub write token is there), rewrite files the
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
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

USER_ENV = "CI_FIX_AGENT_USER"

# Where an unprivileged account can leave files: the shared temp directories
# (on macOS, also its own under /var/folders) and its own home.
_WRITABLE_ROOTS = (
    ("/private/tmp", "/private/var/tmp", "/private/var/folders") if sys.platform == "darwin"
    else ("/tmp", "/var/tmp", "/dev/shm")
)


def isolated_user() -> str:
    """The account to run untrusted commands as, or "" to run them directly.

    Inside GitHub Actions the account is required: running a command that can
    reach the job's write token is never the fallback.
    """
    user = os.environ.get(USER_ENV, "").strip()
    if not user and os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        raise RuntimeError(f"{USER_ENV} is not set; refusing to run untrusted commands as the workflow user")
    return user


@contextmanager
def handed_to(user: str, root: Path, readable: tuple[Path, ...] = ()) -> Iterator[str]:
    """Give ``root``'s files (not ``.git``) to ``user``; yield a fresh home directory.

    ``readable`` directories are made readable to ``user`` for the command.
    ``chown -h -P`` changes symbolic links themselves and never follows them,
    so a link in the checkout cannot redirect the privileged chown.
    """
    root = root.resolve()
    for parent in root.parents:
        if not parent.stat().st_mode & stat.S_IXOTH:
            raise OSError(f"{user} cannot reach {root}: {parent} is not traversable")
    for directory in readable:
        _as_root("chmod", "-R", "a+rX", "--", str(directory))
    runner = f"{os.getuid()}:{os.getgid()}"
    account = pwd.getpwnam(user)
    home = tempfile.mkdtemp(prefix="ci-fix-home-", dir=root.parent)
    git_dir = root / ".git"
    git_identity = _identity(git_dir)
    _as_root("chown", "-R", "-h", "-P", "--", f"{account.pw_uid}:{account.pw_gid}", str(root), home)
    if git_identity is not None:
        _as_root("chown", "-R", "-h", "-P", "--", runner, str(git_dir))
    try:
        yield home
    finally:
        _kill_all(user)
        _as_root("rm", "-rf", "--", home)
        # Take the checkout back first: then nothing in it belongs to
        # ``user``, and the sweep below cannot delete any of it.
        _as_root("chown", "-R", "-h", "-P", "--", runner, str(root))
        _remove_files_of(user)
        # ``user`` owned the checkout's directory, so it could have renamed
        # .git and left one of its own: the chown above would hand that to
        # the workflow user, whose next git command would run any filter or
        # merge driver it configures. Only the original .git is trusted.
        if git_identity is not None and _identity(git_dir) != git_identity:
            raise OSError(f"{user} replaced {git_dir}; the checkout can no longer be trusted")


def _identity(path: Path) -> tuple[int, int] | None:
    """(device, inode) of a real directory at ``path``, without following links."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    return (info.st_dev, info.st_ino) if stat.S_ISDIR(info.st_mode) else (-1, -1)


def as_user(user: str, env: dict[str, str], argv: list[str], *, home: str) -> list[str]:
    """``argv`` run as ``user`` with ``env``, in ``home`` as its home and temp directory."""
    scrubbed = {
        **env, "HOME": home, "TMPDIR": home, "TMP": home, "USER": user, "LOGNAME": user,
        # The worktree is the account's but .git is not; let git read it.
        "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "safe.directory", "GIT_CONFIG_VALUE_0": "*",
    }
    command = ["env", "-i", *(f"{key}={value}" for key, value in scrubbed.items()), *argv]
    if sys.platform == "darwin":
        return ["sudo", "-n", "-u", user, "--", *command]
    # runuser, not ``sudo -u``: it needs only root, which a runner's sudo
    # grants. Its full path: it is often in an sbin directory, and the
    # command may be started with an empty environment.
    return [*_root(), shutil.which("runuser") or "runuser", "-u", user, "--", *command]


def _remove_files_of(user: str) -> None:
    roots = [root for root in (*_WRITABLE_ROOTS, pwd.getpwnam(user).pw_dir) if os.path.isdir(root)]
    _as_root("find", *roots, "-xdev", "-mindepth", "1", "-user", user, "-delete")


def _kill_all(user: str) -> None:
    deadline = time.monotonic() + 30
    while True:
        subprocess.run([*_root(), "pkill", "-KILL", "-u", user], capture_output=True, check=False)
        if subprocess.run(["pgrep", "-u", user], capture_output=True, check=False).returncode == 1:
            return
        if time.monotonic() > deadline:
            raise OSError(f"processes of {user} survived the command")
        time.sleep(0.2)


def _root() -> list[str]:
    """What runs a command as root: nothing for root itself, else sudo."""
    return [] if os.geteuid() == 0 else ["sudo", "-n"]


def _as_root(*argv: str) -> None:
    result = subprocess.run([*_root(), *argv], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise OSError(f"{' '.join(argv)} as root failed: {result.stderr.strip()}")
