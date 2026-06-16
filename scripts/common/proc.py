"""Small, shared subprocess and environment helpers."""

from __future__ import annotations

import os
import subprocess

# Bot identity for agent-authored commits. Lives here (not in any one
# workflow) because it is cross-cutting: backport and ci_fix both author
# commits as the bot.
BOT_NAME = "valkeyrie-bot[bot]"
BOT_EMAIL = "3692572+valkeyrie-bot[bot]@users.noreply.github.com"


def git_output(repo_dir: str, *args: str) -> str:
    """Run a git command in ``repo_dir`` and return its stdout.

    Raises ``subprocess.CalledProcessError`` (with git's stderr attached to the
    message) on a non-zero exit. Use this when the command's output is the
    point (``diff``, ``rev-parse``); for fire-and-forget commands use the
    caller's own runner.
    """
    result = subprocess.run(
        ["git", *LOCKED_GIT_CONFIG, *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr,
        )
    return result.stdout


def run_git(
    repo_dir: str | None,
    *args: str,
    env: dict[str, str] | None = None,
    input: str | None = None,
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
    untrusted code, so a ``pre-push``/``pre-commit`` hook must never run — least
    of all the authenticated ``push`` that carries credentials.
    """
    result = subprocess.run(
        ["git", *LOCKED_GIT_CONFIG, *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        input=input,
        env=env if env is not None else filter_env(_GIT_SAFE_ENV),
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr,
        )
    return result


# Disable repo-controlled code execution paths. The working tree is untrusted
# (an AI or a test may have written files, .git/hooks/*, or .git/config), so no
# git command may execute hooks, credential helpers, or external diff drivers.
LOCKED_GIT_CONFIG = (
    "-c", "core.hooksPath=/dev/null",
    "-c", "credential.helper=",
    "-c", "diff.external=",
)

# Process-runtime environment variables that carry no credential: the minimal
# set any scrubbed subprocess needs. Other allowlists extend this.
PROCESS_BASICS = ("PATH", "HOME", "TMPDIR", "TMP", "USER", "LOGNAME", "LANG", "LC_ALL")

# CA-bundle locations a scrubbed subprocess needs to make HTTPS calls. Carry no
# credential, so they extend an allowlist without widening the trust boundary.
NETWORK_ENV = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")

# Minimal environment for local git commands: process basics only.
_GIT_SAFE_ENV = PROCESS_BASICS


def filter_env(allowlist: tuple[str, ...]) -> dict[str, str]:
    """Return os.environ filtered to non-empty allowlisted names.

    The single place that turns an env allowlist into a concrete environment,
    so subprocess sandboxing semantics stay identical everywhere.
    """
    allowed = set(allowlist)
    return {name: value for name, value in os.environ.items() if name in allowed and value}


def worktree_changed_paths(repo_dir: str) -> tuple[str, ...]:
    """Return tracked-but-modified and untracked paths in the working tree.

    Combines ``git diff --name-only HEAD`` (tracked changes) with
    ``git ls-files --others --exclude-standard`` (new files), NUL-delimited so
    paths with spaces survive. Used to see exactly which files an edit step
    touched.
    """
    paths: set[str] = set()
    for args in (
        ("diff", "--name-only", "-z", "HEAD"),
        ("ls-files", "--others", "--exclude-standard", "-z"),
    ):
        out = run_git(repo_dir, *args).stdout
        paths.update(p for p in out.split("\0") if p)
    return tuple(sorted(paths))


class EmptyPatch(Exception):
    """Raised when the approved paths produce no patch content."""


def build_approved_patch(repo_dir: str, changed_paths: tuple[str, ...]) -> str:
    """Return the exact patch for ``changed_paths`` that a push would apply.

    Includes untracked approved files (staged with ``--intent-to-add`` so they
    appear in the diff) and uses ``--binary`` so the patch reapplies verbatim
    in a clean clone. This is the single source of truth for "the change": the
    skeptic review inspects this patch, and the push reapplies it — so the
    review never judges something different from what ships.

    Raises ``EmptyPatch`` when there are no approved paths, or when the approved
    paths yield no content, so an empty or vanished edit is refused rather than
    silently approved (and never broadened to the whole tree).
    """
    if not changed_paths:
        raise EmptyPatch("no approved paths to build a patch from")
    untracked = tuple(
        p
        for p in run_git(
            repo_dir, "ls-files", "--others", "--exclude-standard", "-z", "--", *changed_paths
        ).stdout.split("\0")
        if p
    )
    if untracked:
        run_git(repo_dir, "add", "--intent-to-add", "--", *untracked)
    patch = git_output(repo_dir, "diff", "--no-ext-diff", "--binary", "HEAD", "--", *changed_paths)
    if not patch.strip():
        raise EmptyPatch("approved paths produced an empty patch")
    return patch
