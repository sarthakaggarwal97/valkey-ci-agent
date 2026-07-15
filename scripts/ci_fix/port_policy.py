"""Read-only policy checks for an upstream commit proposed as a CI fix."""

from __future__ import annotations

import subprocess

from scripts.ci_fix.port_discovery import resolve_default_branch
from scripts.common.proc import git_output, run_git

ALLOWED_BRANCH_PREFIX = "agent/backport/"


class PortRefused(ValueError):
    """Raised when a proposed upstream commit violates port policy."""


def verify_portable_commit(
    repo_dir: str,
    fix_commit: str,
    head_sha: str,
) -> None:
    """Require a merged default-branch commit that is absent from the PR."""
    default_branch = resolve_default_branch(repo_dir)
    ref = f"origin/{default_branch}"
    try:
        git_output(repo_dir, "rev-parse", "--verify", ref)
    except subprocess.CalledProcessError:
        run_git(
            repo_dir,
            "fetch",
            "origin",
            f"refs/heads/{default_branch}:refs/remotes/origin/{default_branch}",
        )
    if not _is_ancestor(repo_dir, fix_commit, ref):
        raise PortRefused(
            f"Refusing to port {fix_commit[:12]}: it is not reachable from "
            f"{ref}, so it is not a merged upstream fix."
        )
    if _is_ancestor(repo_dir, fix_commit, head_sha):
        raise PortRefused(
            f"Refusing to port {fix_commit[:12]}: it is already present on "
            "the PR head."
        )


def _is_ancestor(repo_dir: str, maybe_ancestor: str, descendant: str) -> bool:
    try:
        git_output(
            repo_dir,
            "merge-base",
            "--is-ancestor",
            maybe_ancestor,
            descendant,
        )
        return True
    except subprocess.CalledProcessError:
        return False
