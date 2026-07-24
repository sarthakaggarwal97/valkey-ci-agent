"""Durable suppression for unchanged sweep campaigns that made no progress."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Sequence

from github.GithubException import GithubException

from scripts.backport.models import BackportCandidate, CandidateResult
from scripts.common.github_client import retry_github_call

logger = logging.getLogger(__name__)

_MARKER_ROOT = "heads/agent/backport/failed-campaign"
_MARKER_SCHEMA = 1
_TERMINAL_NO_PROGRESS_OUTCOMES = {
    "skipped-conflict",
    "skipped-existing",
    "skipped-validation-failed",
}
_FAILED_OUTCOMES = {
    "skipped-conflict",
    "skipped-validation-failed",
}


def failure_marker_ref(
    target_branch: str,
    target_sha: str,
    candidates: Sequence[BackportCandidate],
) -> str:
    """Return the short Git ref that identifies one exact failed campaign."""

    payload = {
        "schema": _MARKER_SCHEMA,
        "target_branch": target_branch,
        "target_sha": target_sha,
        "candidates": [
            {
                "source_pr": candidate.source_pr_number,
                "merge_commit": candidate.merge_commit_sha,
                "source_commits": list(candidate.commit_shas),
                "source_commits_complete": candidate.source_commits_complete,
            }
            for candidate in candidates
        ],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    campaign_digest = hashlib.sha256(encoded).hexdigest()
    return f"{_marker_prefix(target_branch)}/{campaign_digest}"


def campaign_made_no_progress(
    candidates: Sequence[BackportCandidate],
    results: Sequence[CandidateResult],
    *,
    error: str = "",
    pr_url: str = "",
) -> bool:
    """Return whether every candidate ended durably without a publishable change."""

    if error or pr_url or len(results) != len(candidates):
        return False
    if not any(result.outcome in _FAILED_OUTCOMES for result in results):
        return False
    return all(
        result.source_pr_number == candidate.source_pr_number
        and result.outcome in _TERMINAL_NO_PROGRESS_OUTCOMES
        for candidate, result in zip(candidates, results)
    )


def failure_marker_exists(
    gh: Any,
    repo_full_name: str,
    marker_ref: str,
    *,
    target_sha: str,
) -> bool:
    """Return whether an intact marker already records this exact campaign."""

    return _failure_marker_exists(
        _get_repo(gh, repo_full_name),
        repo_full_name,
        marker_ref,
        target_sha=target_sha,
    )


def _failure_marker_exists(
    repo: Any,
    repo_full_name: str,
    marker_ref: str,
    *,
    target_sha: str,
) -> bool:
    try:
        ref = retry_github_call(
            lambda: repo.get_git_ref(marker_ref),
            retries=2,
            description=f"get failed-campaign ref {marker_ref}",
        )
    except GithubException as exc:
        if exc.status == 404:
            return False
        raise

    marker_sha = getattr(getattr(ref, "object", None), "sha", None)
    if marker_sha != target_sha:
        raise RuntimeError(
            f"failed-campaign marker {repo_full_name}:{marker_ref} points to "
            f"{marker_sha or 'no SHA'}, expected {target_sha}"
        )
    return True


def record_failure_marker(
    gh: Any,
    repo_full_name: str,
    marker_ref: str,
    *,
    target_branch: str,
    target_sha: str,
) -> None:
    """Record this campaign and remove obsolete markers for the target branch."""

    repo = _get_repo(gh, repo_full_name)
    if not _failure_marker_exists(
        repo,
        repo_full_name,
        marker_ref,
        target_sha=target_sha,
    ):
        try:
            retry_github_call(
                lambda: repo.create_git_ref(
                    ref=f"refs/{marker_ref}",
                    sha=target_sha,
                ),
                retries=2,
                description=f"create failed-campaign ref {marker_ref}",
            )
        except GithubException as exc:
            if exc.status != 422 or not _failure_marker_exists(
                repo,
                repo_full_name,
                marker_ref,
                target_sha=target_sha,
            ):
                raise

    _delete_matching_markers(
        repo,
        _marker_prefix(target_branch),
        keep=marker_ref,
    )


def clear_failure_markers(
    gh: Any,
    repo_full_name: str,
    target_branch: str,
) -> None:
    """Delete all failed-campaign markers owned by one target branch."""

    repo = _get_repo(gh, repo_full_name)
    _delete_matching_markers(repo, _marker_prefix(target_branch))


def _marker_prefix(target_branch: str) -> str:
    branch_slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", target_branch).strip("-")
    branch_slug = branch_slug or "branch"
    branch_digest = hashlib.sha256(target_branch.encode("utf-8")).hexdigest()[:8]
    return f"{_MARKER_ROOT}/{branch_slug}-{branch_digest}"


def _get_repo(gh: Any, repo_full_name: str) -> Any:
    return retry_github_call(
        lambda: gh.get_repo(repo_full_name),
        retries=2,
        description=f"get {repo_full_name} for failed-campaign marker",
    )


def _delete_matching_markers(
    repo: Any,
    prefix: str,
    *,
    keep: str | None = None,
) -> None:
    refs = retry_github_call(
        lambda: list(repo.get_git_matching_refs(prefix)),
        retries=2,
        description=f"list failed-campaign refs under {prefix}",
    )
    for ref in refs:
        short_ref = str(getattr(ref, "ref", "")).removeprefix("refs/")
        if short_ref == keep:
            continue
        if not short_ref.startswith(f"{prefix}/"):
            raise RuntimeError(
                f"GitHub returned out-of-scope ref {short_ref!r} for prefix {prefix!r}"
            )
        retry_github_call(
            ref.delete,
            retries=2,
            description=f"delete obsolete failed-campaign ref {short_ref}",
        )
        logger.info("Deleted obsolete failed-campaign marker %s", short_ref)
