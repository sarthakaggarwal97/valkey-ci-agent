"""Small, shared subprocess and environment helpers."""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
from typing import Any

# Bot identity for agent-authored commits. Lives here (not in any one
# workflow) because it is cross-cutting: backport and ci_fix both author
# commits as the bot.
BOT_NAME = "valkeyrie-bot[bot]"
BOT_EMAIL = "3692572+valkeyrie-bot[bot]@users.noreply.github.com"

# Bound every git invocation so a hung network/lock cannot stall the run.
# Generous enough for a clone or push, short enough to fail fast on a hang.
_GIT_TIMEOUT_S = 5 * 60


def git_output(repo_dir: str, *args: str, timeout: int = _GIT_TIMEOUT_S) -> str:
    """Run a git command in ``repo_dir`` and return its stdout.

    Raises ``subprocess.CalledProcessError`` (with git's stderr attached to the
    message) on a non-zero exit, or ``subprocess.TimeoutExpired`` if it does not
    finish within ``timeout`` (so a hung git/network/lock never blocks the run
    indefinitely). Use this when the command's output is the point (``diff``,
    ``rev-parse``); for fire-and-forget commands use the caller's own runner.

    Runs with the same *scrubbed* environment as ``run_git`` (process basics
    only, no inherited tokens). These commands run after untrusted PR code has
    executed in the worktree, so a planted repo-local git config
    (``core.sshCommand``, ``core.fsmonitor``, a diff/filter driver) must never
    execute with the parent's credentials in scope. ``LOCKED_GIT_CONFIG``
    disables hooks; the scrubbed env removes the secrets such config could
    otherwise read. Every caller is a local operation that needs no credentials.
    """
    return run_git(repo_dir, *args, timeout=timeout).stdout


def run_git(
    repo_dir: str | None,
    *args: str,
    env: dict[str, str] | None = None,
    input: str | None = None,
    timeout: int = _GIT_TIMEOUT_S,
    check: bool = True,
    errors: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a git command, raising on failure, with an explicit environment.

    Unlike ``git_output`` this returns the full result and is for commands run
    for effect (``commit``, ``push``, ``clone``, ``apply``). ``repo_dir`` may be
    ``None`` for commands that do not run inside a repository (e.g. ``clone``).
    ``input`` feeds stdin (e.g. ``git apply -``). ``env`` defaults to a
    *scrubbed* environment (process basics only, no inherited secrets) so a
    repository git hook can never read tokens from the ambient environment
    during, e.g., ``git commit``. Pass an explicit ``env`` (such as a
    ``GitAuth`` env) when a command needs credentials.

    On a non-zero exit the raised ``CalledProcessError`` carries git's stderr,
    so failures are diagnosable.

    Repository-defined hooks are disabled on every invocation
    (``core.hooksPath=/dev/null``): the working tree may have been written by
    untrusted code, so a ``pre-push``/``pre-commit`` hook must never run - least
    of all the authenticated ``push`` that carries credentials.
    """
    _reject_unsafe_config_args(args)
    git_env = _locked_git_env(env)
    repo_config = _repository_execution_overrides(repo_dir, git_env, timeout)
    result = subprocess.run(
        ["git", *LOCKED_GIT_CONFIG, *repo_config, *_locked_operation_args(args)],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        input=input,
        env=git_env,
        timeout=timeout,
        errors=errors,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr,
        )
    return result


def run_git_bytes(
    repo_dir: str | None,
    *args: str,
    env: dict[str, str] | None = None,
    input: bytes | None = None,
    timeout: int = _GIT_TIMEOUT_S,
    check: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    """Byte-preserving locked Git for NUL-delimited paths and object content."""
    _reject_unsafe_config_args(args)
    git_env = _locked_git_env(env)
    repo_config = _repository_execution_overrides(repo_dir, git_env, timeout)
    result = subprocess.run(
        ["git", *LOCKED_GIT_CONFIG, *repo_config, *_locked_operation_args(args)],
        cwd=repo_dir,
        capture_output=True,
        text=False,
        input=input,
        env=git_env,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr,
        )
    return result


class GitPathEncodingError(ValueError):
    """Raised when Git reports a path that cannot be represented safely."""


def decode_git_paths(data: bytes, *, context: str = "Git path output") -> tuple[str, ...]:
    """Decode NUL-delimited Git paths without lossy text conversion.

    Repository paths are arbitrary bytes, but the agent's artifacts and AI
    interfaces are UTF-8. Refuse paths outside that contract so they become an
    explicit human handoff instead of being split on newlines or rewritten with
    replacement characters.
    """
    if not data:
        return ()
    if not data.endswith(b"\0"):
        raise GitPathEncodingError(f"{context} is not NUL-terminated")
    paths: list[str] = []
    for raw_path in data[:-1].split(b"\0"):
        if not raw_path:
            raise GitPathEncodingError(f"{context} contains an empty path")
        try:
            paths.append(raw_path.decode("utf-8", errors="strict"))
        except UnicodeDecodeError as exc:
            rendered = raw_path[:64].hex()
            suffix = "..." if len(raw_path) > 64 else ""
            raise GitPathEncodingError(
                f"{context} contains a non-UTF-8 path (hex={rendered}{suffix})"
            ) from exc
    return tuple(paths)


# Disable repo-controlled code execution paths. The working tree is untrusted
# (an AI or a test may have written files, .git/hooks/*, or .git/config), so no
# git command may execute hooks, credential helpers, or external diff drivers.
# ``--literal-pathspecs`` makes every path argument a literal path: a file whose
# name carries pathspec magic (e.g. ``:(glob)*``) must not be able to broaden a
# ``diff``/``ls-files`` beyond the approved paths.
LOCKED_GIT_CONFIG = (
    "--literal-pathspecs",
    "-c", "core.hooksPath=/dev/null",
    "-c", "core.fsmonitor=false",
    "-c", "core.sshCommand=false",
    "-c", "core.gitProxy=none",
    "-c", "core.pager=cat",
    "-c", "commit.gpgSign=false",
    "-c", "tag.gpgSign=false",
    "-c", "gpg.program=false",
    "-c", "gpg.ssh.program=false",
    "-c", "credential.helper=",
    "-c", "credential.interactive=never",
    "-c", "interactive.diffFilter=",
    "-c", "fetch.recurseSubmodules=false",
    "-c", "submodule.recurse=false",
    "-c", "protocol.ext.allow=never",
)

_EXECUTABLE_CONFIG_PATTERN = (
    r"^(filter\..*\.(clean|smudge|process|required)"
    r"|diff\..*\.(command|textconv|cachetextconv)"
    r"|merge\..*\.driver"
    r"|credential\..*\.helper"
    r"|remote\..*\.(uploadpack|receivepack|vcs)"
    r"|difftool\..*\.cmd"
    r"|mergetool\..*\.cmd"
    r"|submodule\..*\.update"
    r"|trailer\..*\.command"
    r"|pager\..*)$"
)
_EXECUTABLE_CONFIG_RE = re.compile(_EXECUTABLE_CONFIG_PATTERN, re.IGNORECASE)

_LOCKED_CONFIG_KEYS = {
    item.split("=", 1)[0].casefold()
    for index, item in enumerate(LOCKED_GIT_CONFIG)
    if index > 0 and LOCKED_GIT_CONFIG[index - 1] == "-c"
}

# Process-runtime environment variables that carry no credential: the minimal
# set any scrubbed subprocess needs. Other allowlists extend this.
PROCESS_BASICS = ("PATH", "HOME", "TMPDIR", "TMP", "USER", "LOGNAME", "LANG", "LC_ALL")

# CA-bundle locations a scrubbed subprocess needs to make HTTPS calls. Carry no
# credential, so they extend an allowlist without widening the trust boundary.
NETWORK_ENV = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")

# Minimal environment for local git commands: process basics only.
_GIT_SAFE_ENV = PROCESS_BASICS

_GIT_AUTH_ENV = NETWORK_ENV + (
    "GIT_ASKPASS",
    "GIT_PASSWORD",
    "GIT_TERMINAL_PROMPT",
)


def _locked_git_env(env: dict[str, str] | None) -> dict[str, str]:
    """Return a minimal Git environment, retaining only explicit askpass auth."""
    source = os.environ if env is None else env
    allowed = set(_GIT_SAFE_ENV + _GIT_AUTH_ENV)
    locked = {
        name: value
        for name, value in source.items()
        if name in allowed and value
    }
    locked.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": locked.get("GIT_TERMINAL_PROMPT", "0"),
            "GIT_PAGER": "cat",
            "GIT_EDITOR": "true",
            "GIT_SEQUENCE_EDITOR": "true",
        }
    )
    return locked


def _repository_execution_overrides(
    repo_dir: str | None,
    env: dict[str, str],
    timeout: int,
) -> tuple[str, ...]:
    """Neutralize every named executable driver found in local Git config.

    Driver names are repository-defined through ``.gitattributes``, so static
    ``-c`` flags cannot enumerate them. Query the complete local config,
    including local include files, without executing worktree code and add a
    highest-precedence override for every executable filter, diff, or merge
    key. A malformed or unreadable config fails closed before the requested Git
    operation.
    """
    if (
        repo_dir is None
        or not os.path.isdir(repo_dir)
        or not os.path.lexists(os.path.join(repo_dir, ".git"))
    ):
        return ()
    result = subprocess.run(
        [
            "git",
            *LOCKED_GIT_CONFIG,
            "config",
            "--local",
            "--includes",
            "--name-only",
            "-z",
            "--get-regexp",
            _EXECUTABLE_CONFIG_PATTERN,
        ],
        cwd=repo_dir,
        capture_output=True,
        text=False,
        env=env,
        timeout=min(timeout, 30),
    )
    if result.returncode == 1:
        return ()
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            result.stdout,
            result.stderr,
        )
    try:
        names = decode_git_paths(
            result.stdout,
            context="executable Git config key list",
        )
    except GitPathEncodingError as exc:
        raise ValueError(str(exc)) from exc
    overrides: list[str] = []
    for name in sorted(set(names), key=str.lower):
        if not _EXECUTABLE_CONFIG_RE.fullmatch(name):
            raise ValueError(f"unexpected executable Git config key {name!r}")
        lowered = name.lower()
        if lowered.startswith("remote.") and lowered.endswith(
            (".uploadpack", ".receivepack", ".vcs")
        ):
            raise ValueError(
                f"repository config may not override a Git transport command: {name}"
            )
        if lowered.endswith((".required", ".cachetextconv")):
            value = "false"
        elif lowered.endswith(".helper"):
            value = ""
        elif lowered.endswith(".update"):
            value = "checkout"
        elif lowered.startswith("pager."):
            value = "cat"
        elif lowered.startswith("merge.") and lowered.endswith(".driver"):
            value = "false"
        elif lowered.startswith("filter.") and lowered.endswith((".clean", ".smudge")):
            value = "cat"
        else:
            value = ""
        overrides.extend(("-c", f"{name}={value}"))
    return tuple(overrides)


def _reject_unsafe_config_args(args: tuple[str, ...]) -> None:
    """Prevent a caller from overriding the executable-driver lock."""
    if "--ext-diff" in args or "--textconv" in args:
        raise ValueError("external Git diff execution is not permitted")
    for index, value in enumerate(args):
        if value == "--config-env" or value.startswith("--config-env="):
            raise ValueError("--config-env is not permitted by the locked Git API")
        if value != "-c" or index + 1 >= len(args):
            continue
        key = args[index + 1].split("=", 1)[0]
        if (
            _EXECUTABLE_CONFIG_RE.fullmatch(key)
            or key.casefold() in _LOCKED_CONFIG_KEYS
        ):
            raise ValueError(f"locked Git config override is not permitted: {key}")


def _locked_operation_args(args: tuple[str, ...]) -> tuple[str, ...]:
    """Force diff-capable porcelain to ignore repository external drivers."""
    index = 0
    while index < len(args):
        value = args[index]
        if value == "-c":
            index += 2
            continue
        if value.startswith("-"):
            index += 1
            continue
        if value in {"diff", "log", "show"}:
            return (
                *args[: index + 1],
                "--no-ext-diff",
                "--no-textconv",
                *args[index + 1 :],
            )
        return args
    return args


def filter_env(allowlist: tuple[str, ...]) -> dict[str, str]:
    """Return os.environ filtered to non-empty allowlisted names.

    The single place that turns an env allowlist into a concrete environment,
    so subprocess sandboxing semantics stay identical everywhere.
    """
    allowed = set(allowlist)
    return {name: value for name, value in os.environ.items() if name in allowed and value}


def fail_fast_shell(command: str) -> list[str]:
    """Return a fixed shell argv whose exit status cannot hide pipeline errors."""
    bash = shutil.which("bash")
    if not bash:
        raise FileNotFoundError("bash is required for fail-fast command execution")
    return [bash, "--noprofile", "--norc", "-euo", "pipefail", "-c", command]


def terminate_process_group(process: subprocess.Popen[Any]) -> None:
    """Terminate a subprocess and every descendant in its process group."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            process.kill()
        except ProcessLookupError:
            pass


def worktree_changed_paths(repo_dir: str) -> tuple[str, ...]:
    """Return tracked-but-modified and untracked paths in the working tree.

    Combines ``git diff --name-only HEAD`` (tracked changes) with
    ``git ls-files --others --exclude-standard`` (new files), NUL-delimited so
    paths with whitespace survive. Used to see exactly which files an edit
    step touched.
    """
    paths: set[str] = set()
    for args in (
        ("diff", "--name-only", "-z", "HEAD"),
        ("ls-files", "--others", "--exclude-standard", "-z"),
    ):
        out = run_git_bytes(repo_dir, *args).stdout
        paths.update(decode_git_paths(out, context=f"git {' '.join(args)}"))
    return tuple(sorted(paths))


class EmptyPatch(Exception):
    """Raised when the approved paths produce no patch content."""


def build_approved_patch(repo_dir: str, changed_paths: tuple[str, ...]) -> str:
    """Return the exact patch for ``changed_paths`` that a push would apply.

    Includes untracked approved files (staged with ``--intent-to-add`` so they
    appear in the diff) and uses ``--binary`` so the patch reapplies verbatim
    in a clean clone. This is the single source of truth for "the change": the
    skeptic review inspects this patch, and the push reapplies it - so the
    review never judges something different from what ships.

    Raises ``EmptyPatch`` when there are no approved paths, or when the approved
    paths yield no content, so an empty or vanished edit is refused rather than
    silently approved (and never broadened to the whole tree).
    """
    if not changed_paths:
        raise EmptyPatch("no approved paths to build a patch from")
    untracked = decode_git_paths(
        run_git_bytes(
            repo_dir,
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            *changed_paths,
        ).stdout,
        context="approved untracked path list",
    )
    if untracked:
        run_git(repo_dir, "add", "--intent-to-add", "--", *untracked)
    patch = git_output(repo_dir, "diff", "--no-ext-diff", "--binary", "HEAD", "--", *changed_paths)
    if not patch.strip():
        raise EmptyPatch("approved paths produced an empty patch")
    return patch
