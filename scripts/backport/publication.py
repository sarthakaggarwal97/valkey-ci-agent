"""Publication-boundary checks shared by manual and sweep backports."""

from __future__ import annotations

from typing import Any

from scripts.backport.git import git_output
from scripts.common.github_client import retry_github_call


class TargetHeadChanged(RuntimeError):
    """Raised when validation no longer describes the target branch tip."""


def capture_target_head(repo_dir: str, target_branch: str) -> str:
    """Return the target tip represented by a freshly cloned checkout."""

    sha = git_output(
        repo_dir,
        "rev-parse",
        "--verify",
        f"refs/remotes/origin/{target_branch}^{{commit}}",
    )
    if not sha:
        raise RuntimeError(
            f"could not capture origin/{target_branch} after cloning target branch"
        )
    return sha


def get_target_head(gh: Any, repo_full_name: str, target_branch: str) -> str:
    """Read the authoritative target branch tip from GitHub."""

    repo = retry_github_call(
        lambda: gh.get_repo(repo_full_name),
        retries=2,
        description=f"get {repo_full_name} for target-head check",
    )
    branch = retry_github_call(
        lambda: repo.get_branch(target_branch),
        retries=2,
        description=f"get {repo_full_name}:{target_branch} head",
    )
    sha = getattr(getattr(branch, "commit", None), "sha", None)
    if not isinstance(sha, str) or not sha:
        raise RuntimeError(
            f"GitHub returned no commit SHA for {repo_full_name}:{target_branch}"
        )
    return sha


def assert_target_head_unchanged(
    gh: Any,
    repo_full_name: str,
    target_branch: str,
    expected_sha: str,
) -> None:
    """Refuse publication if the validated target snapshot is no longer current."""

    actual_sha = get_target_head(gh, repo_full_name, target_branch)
    if actual_sha != expected_sha:
        raise TargetHeadChanged(
            f"{repo_full_name}:{target_branch} moved during backport validation "
            f"(validated {expected_sha}, current {actual_sha}); refusing to publish "
            "a stale backport. Retry against the new target tip."
        )
