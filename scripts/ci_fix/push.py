"""Commit a validated fix and push it to the backport PR's own branch.

This is the only place ``ci_fix`` mutates a remote, so it carries the push
discipline:

- The fix is committed with a DCO sign-off, authored as the bot. Local git
  commands run with a scrubbed environment so a repository git hook can never
  read a credential from the ambient environment.
- The push target must live in the allowed agent namespace
  (``agent/backport/...``) on the PR's own head repo. Anything else is refused.
- The push is fast-forward only: the refspec is ``HEAD:<branch>`` with no
  ``+``, so git itself rejects a non-fast-forward rather than overwriting.

The branch is never merged. The push re-triggers the PR's normal CI.
"""

from __future__ import annotations

import logging
import re
import subprocess
import tempfile
from pathlib import Path

from scripts.ci_fix.models import FixProposal
from scripts.common.git_auth import github_https_url
from scripts.common.proc import BOT_EMAIL, BOT_NAME, filter_env, git_output, run_git

logger = logging.getLogger(__name__)

ALLOWED_BRANCH_PREFIX = "agent/backport/"
_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)
_GIT_SAFE_ENV = ("PATH", "HOME", "TMPDIR", "TMP", "USER", "LOGNAME", "LANG", "LC_ALL")
_GIT_LOCKDOWN = (
    "-c", "core.hooksPath=/dev/null",
    "-c", "credential.helper=",
    "-c", "diff.external=",
)


class PushRefused(Exception):
    """Raised when a push target falls outside the allowed namespace."""


def commit_and_push_fix(
    repo_dir: str,
    *,
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    proposal: FixProposal,
    changed_paths: tuple[str, ...],
    git_env: dict[str, str],
) -> str:
    """Commit the working-tree fix and push it to the PR head branch.

    The verified checkout is treated as untrusted: test commands may have
    modified ``.git/config`` or hooks. We only extract a binary patch for the
    approved paths, then apply it in a fresh clone at ``head_sha``. The clean
    clone is the only checkout that receives credentials. Returns the new
    commit SHA. Raises ``PushRefused`` if any trust-boundary check fails.
    """
    if not head_branch.startswith(ALLOWED_BRANCH_PREFIX):
        raise PushRefused(
            f"Refusing to push to {head_branch!r}: ci_fix only pushes to branches "
            f"under {ALLOWED_BRANCH_PREFIX}."
        )
    if not _REPO_RE.fullmatch(head_repo_full_name):
        raise PushRefused(f"Refusing to push to malformed repo {head_repo_full_name!r}.")
    if not _SHA_RE.fullmatch(head_sha):
        raise PushRefused(f"Refusing to push from malformed head SHA {head_sha!r}.")
    if not changed_paths:
        raise PushRefused("Refusing to push: no approved changed paths to stage.")
    if not _is_valid_branch_name(head_branch):
        raise PushRefused(f"Refusing to push to malformed branch {head_branch!r}.")

    patch = _approved_patch(repo_dir, changed_paths)

    with tempfile.TemporaryDirectory(prefix="ci-fix-push-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(head_repo_full_name, clean_repo)
        try:
            run_git(str(clean_repo), "checkout", head_sha)
            run_git(str(clean_repo), "checkout", "-B", head_branch)
            _apply_patch(str(clean_repo), patch)

            staged = _staged_paths(str(clean_repo))
            if staged != tuple(sorted(changed_paths)):
                raise PushRefused(
                    "Refusing to push: approved patch staged unexpected paths "
                    f"{staged!r} (expected {tuple(sorted(changed_paths))!r})."
                )

            run_git(str(clean_repo), "config", "user.name", BOT_NAME)
            run_git(str(clean_repo), "config", "user.email", BOT_EMAIL)
            run_git(str(clean_repo), "commit", "-s", "-m", _commit_message(proposal))

            run_git(str(clean_repo), "remote", "set-url", "origin", github_https_url(head_repo_full_name))
            run_git(str(clean_repo), "push", "origin", f"HEAD:{head_branch}", env=git_env)
        except subprocess.CalledProcessError as exc:
            # Keep the pipeline's "every outcome is a comment" guarantee: a git
            # failure in the clean clone (unreachable SHA, non-fast-forward
            # push, etc.) becomes a refusal, never an uncaught crash.
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to push: git failed: {detail}") from exc

        return git_output(str(clean_repo), "rev-parse", "HEAD").strip()


def _approved_patch(repo_dir: str, changed_paths: tuple[str, ...]) -> str:
    untracked = _untracked_approved_paths(repo_dir, changed_paths)
    if untracked:
        run_git(repo_dir, "add", "--intent-to-add", "--", *untracked)
    patch = git_output(
        repo_dir, "diff", "--no-ext-diff", "--binary", "HEAD", "--", *changed_paths,
    )
    if not patch.strip():
        raise PushRefused("Refusing to push: approved paths produced an empty patch.")
    return patch


def _untracked_approved_paths(repo_dir: str, changed_paths: tuple[str, ...]) -> tuple[str, ...]:
    out = git_output(
        repo_dir, "ls-files", "--others", "--exclude-standard", "-z", "--", *changed_paths,
    )
    return tuple(path for path in out.split("\0") if path)


def _clone_clean(head_repo_full_name: str, dest: Path) -> None:
    url = github_https_url(head_repo_full_name)
    _run_git_cmd("clone", "--filter=blob:none", url, str(dest), cwd=None)


def _apply_patch(repo_dir: str, patch: str) -> None:
    try:
        subprocess.run(
            ["git", *_GIT_LOCKDOWN, "apply", "--index", "--whitespace=nowarn", "-"],
            cwd=repo_dir,
            input=patch,
            capture_output=True,
            text=True,
            check=True,
            env=filter_env(_GIT_SAFE_ENV),
        )
    except subprocess.CalledProcessError as exc:
        raise PushRefused(f"Refusing to push: approved patch did not apply cleanly: {exc.stderr[:300]}") from exc


def _staged_paths(repo_dir: str) -> tuple[str, ...]:
    out = git_output(repo_dir, "diff", "--cached", "--name-only", "-z", "HEAD")
    return tuple(sorted(path for path in out.split("\0") if path))


def _is_valid_branch_name(branch: str) -> bool:
    result = subprocess.run(
        ["git", "check-ref-format", "--branch", branch],
        capture_output=True,
        text=True,
        env=filter_env(_GIT_SAFE_ENV),
    )
    return result.returncode == 0


def _run_git_cmd(*args: str, cwd: str | None) -> None:
    try:
        subprocess.run(
            ["git", *_GIT_LOCKDOWN, *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
            env=filter_env(_GIT_SAFE_ENV),
        )
    except subprocess.CalledProcessError as exc:
        raise PushRefused(f"Refusing to push: git {' '.join(args[:2])} failed: {exc.stderr[:300]}") from exc


def _commit_message(proposal: FixProposal) -> str:
    """A focused commit message: a subject naming the test, then the cause.

    Mirrors the maintainer-authored style of the reference fixes.
    """
    subject = f"Fix CI test failure: {proposal.failing_test}"[:72]
    return f"{subject}\n\n{proposal.root_cause}\n"
