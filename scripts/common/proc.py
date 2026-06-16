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

    Raises ``subprocess.CalledProcessError`` on a non-zero exit. Use this when
    the command's output is the point (``diff``, ``rev-parse``); for
    fire-and-forget commands use the caller's own runner.
    """
    return subprocess.run(
        ["git", *_LOCKED_GIT_CONFIG, *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def run_git(
    repo_dir: str,
    *args: str,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a git command, raising on failure, with an explicit environment.

    Unlike ``git_output`` this returns the full result and is for commands run
    for effect (``commit``, ``push``). ``env`` defaults to a *scrubbed*
    environment (process basics only, no inherited secrets) so a repository
    git hook can never read tokens from the ambient environment during, e.g.,
    ``git commit``. Pass an explicit ``env`` (such as a ``GitAuth`` env) when a
    command needs credentials.

    Repository-defined hooks are disabled on every invocation
    (``core.hooksPath=/dev/null``): the working tree may have been written by
    untrusted code, so a ``pre-push``/``pre-commit`` hook must never run — least
    of all the authenticated ``push`` that carries credentials.
    """
    return subprocess.run(
        ["git", *_LOCKED_GIT_CONFIG, *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
        env=env if env is not None else filter_env(_GIT_SAFE_ENV),
    )


# Disable repo-controlled code execution paths. The working tree is untrusted
# (an AI or a test may have written files, .git/hooks/*, or .git/config), so no
# git command may execute hooks, credential helpers, or external diff drivers.
_LOCKED_GIT_CONFIG = (
    "-c", "core.hooksPath=/dev/null",
    "-c", "credential.helper=",
    "-c", "diff.external=",
)

# Minimal environment for local git commands: enough to run git, nothing that
# could carry a credential.
_GIT_SAFE_ENV = ("PATH", "HOME", "TMPDIR", "TMP", "USER", "LOGNAME", "LANG", "LC_ALL")


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
