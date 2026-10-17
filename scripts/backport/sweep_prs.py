"""Pull request and remote-branch operations for backport sweeps."""

from __future__ import annotations

import logging
from typing import Any

from scripts.backport.diff_comments import DIFF_COMMENT_LOGIN, marked_source_pr_urls, reconcile_diff_comments
from scripts.backport.pr_creator import (
    apply_labels,
    create_pull_from_push_repo,
    find_pulls_by_head,
)
from scripts.backport.sweep_models import BranchSweepResult, CandidateResult
from scripts.backport.sweep_reporting import build_pr_body, result_is_on_backport_branch
from scripts.common.github_client import retry_github_call
from scripts.common.github_graphql import GitHubGraphQLClient

logger = logging.getLogger(__name__)


def find_existing_pr(gh: Any, base_repo: str, push_repo: str, branch: str) -> Any | None:
    repo = retry_github_call(lambda: gh.get_repo(base_repo), retries=2, description=f"get {base_repo}")
    pulls = find_pulls_by_head(repo, base_repo=base_repo, push_repo=push_repo, branch=branch)
    return pulls[0] if pulls else None


def upsert_pr(
    gh: Any,
    base_repo: str,
    push_repo: str,
    target_branch: str,
    head_branch: str,
    result: BranchSweepResult,
    existing_pr: Any | None,
    gql: GitHubGraphQLClient | None = None,
    branch_applied: list[CandidateResult] | None = None,
    backport_label: str = "backport",
    llm_conflict_label: str = "ai-resolved-conflicts",
) -> str:
    repo = retry_github_call(lambda: gh.get_repo(base_repo), retries=2, description=f"get {base_repo}")
    previous_body = getattr(existing_pr, "body", None) if existing_pr else None
    body = build_pr_body(
        result,
        branch_applied=branch_applied,
        previous_body=previous_body if isinstance(previous_body, str) else None,
    )
    title = f"[backport] Backport sweep for {target_branch}"

    # The AI-resolved signal is durable on each candidate, so scan this run's
    # results and everything already applied to the branch; a PR that first
    # gains an AI-resolved commit in a later top-up run still gets the label.
    # Skipped and failed candidates left no commit behind, so they do not count.
    labels = [backport_label]
    all_candidates = [
        c for c in result.results if result_is_on_backport_branch(c)
    ] + list(branch_applied or [])
    if any(getattr(c, "resolved_by_ai", False) for c in all_candidates):
        labels.append(llm_conflict_label)

    if existing_pr:
        retry_github_call(lambda: existing_pr.edit(title=title, body=body), retries=2, description="update PR")
        apply_labels(repo, existing_pr, labels)
        # The sweep branch is always green, so any PR we update is ready for
        # review. Promote a leftover draft (e.g. from an older sweep) back to
        # ready. PyGithub does not expose this transition, so use GraphQL.
        if getattr(existing_pr, "draft", False) and gql is not None:
            node_id = getattr(existing_pr, "node_id", None)
            if node_id:
                mark_pr_ready_for_review(gql, node_id)
                logger.info(
                    "Marked PR #%d on %s ready for review",
                    existing_pr.number, base_repo,
                )
        logger.info("Updated PR #%d on %s", existing_pr.number, base_repo)
        comment_urls = _reconcile_sweep_diff_comments(
            existing_pr, result, branch_applied=branch_applied,
        )
        _relink_body_to_comments(
            existing_pr, result, branch_applied, comment_urls,
        )
        return existing_pr.html_url

    pr = retry_github_call(
        lambda: create_pull_from_push_repo(
            repo,
            base_repo=base_repo,
            push_repo=push_repo,
            title=title,
            body=body,
            head_branch=head_branch,
            base_branch=target_branch,
            draft=False,
        ),
        retries=2,
        description="create PR",
    )
    logger.info("Created PR #%d on %s", pr.number, base_repo)
    apply_labels(repo, pr, labels)
    comment_urls = _reconcile_sweep_diff_comments(pr, result, branch_applied=branch_applied)
    _relink_body_to_comments(pr, result, branch_applied, comment_urls)
    return pr.html_url


def _relink_body_to_comments(
    pr: Any,
    result: BranchSweepResult,
    branch_applied: list[CandidateResult] | None,
    comment_urls: dict[int, str],
) -> None:
    """Rebuild the PR body so each applied row links to its AI-diff comment.

    Best-effort: the comments already exist, so a body re-edit failure here
    must never fail the sweep.
    """
    if not comment_urls:
        return
    try:
        linked = build_pr_body(
            result,
            branch_applied=branch_applied,
            previous_body=getattr(pr, "body", None) if isinstance(getattr(pr, "body", None), str) else None,
            comment_urls=comment_urls,
        )
        retry_github_call(
            lambda: pr.edit(body=linked), retries=2, description="relink PR body to comments",
        )
    except Exception as exc:
        logger.warning("Failed to relink sweep PR body to comments: %s", exc)


def _reconcile_sweep_diff_comments(
    pr: Any,
    result: BranchSweepResult,
    *,
    branch_applied: list[CandidateResult] | None = None,
) -> dict[int, str]:
    """Reconcile AI-resolution diff comments on the sweep PR.

    Each currently-applied source PR's comments are reconciled against its
    fresh resolutions. A prior comment group is deleted only when its source
    PR is no longer on the sweep branch at all. A source PR that is still on
    the branch but was not freshly re-resolved this run (e.g. it shows up as
    already-on-branch) keeps its comments untouched, so reruns do not wipe
    still-relevant diffs. The marker identity is the source PR number.

    Returns ``{source_pr: comment_url}`` so the PR body can link each row to its
    AI-resolution comment. Best-effort: a comment failure must never fail the
    sweep.
    """
    # Fresh resolutions by source PR, only for candidates on the branch this run.
    desired_by_pr: dict[int, CandidateResult] = {
        c.source_pr_number: c
        for c in result.results
        if c.outcome == "applied" and c.resolutions
    }
    # Every source PR still represented on the sweep branch (applied this run or
    # already on the branch from a prior sweep). Their comments must be kept.
    on_branch = {
        c.source_pr_number
        for c in result.results
        if result_is_on_backport_branch(c)
    }
    if branch_applied is not None:
        on_branch.update(
            c.source_pr_number
            for c in branch_applied
            if result_is_on_backport_branch(c)
        )

    try:
        prior_urls = marked_source_pr_urls(pr, bot_login=DIFF_COMMENT_LOGIN)
        prior = set(prior_urls)
    except Exception as exc:
        logger.warning("Could not list prior AI-diff comment groups on sweep PR: %s", exc)
        prior_urls = {}
        prior = set()

    # Reconcile fresh resolutions; delete only groups whose source PR has left
    # the branch entirely.
    stale = {pr_num for pr_num in prior if pr_num not in on_branch and pr_num not in desired_by_pr}
    comment_urls: dict[int, str] = {
        pr_num: url
        for pr_num, url in prior_urls.items()
        if pr_num in on_branch and pr_num not in desired_by_pr
    }
    for source_pr in sorted(desired_by_pr.keys() | stale):
        candidate = desired_by_pr.get(source_pr)
        try:
            links = reconcile_diff_comments(
                pr,
                source_pr,
                candidate.resolutions if candidate else [],
                source_title=candidate.source_pr_title if candidate else None,
                resolved_commit_sha=candidate.resolved_commit_sha if candidate else None,
                bot_login=DIFF_COMMENT_LOGIN,
            )
            # All paths for a source PR point at the same grouped comment.
            if links:
                comment_urls[source_pr] = next(iter(links.values()))
        except Exception as exc:
            logger.warning(
                "Failed to reconcile diff comments for source PR #%d on sweep PR: %s",
                source_pr, exc,
            )
    return comment_urls


def mark_pr_ready_for_review(gql: GitHubGraphQLClient, pr_node_id: str) -> None:
    mutation = """
    mutation($id: ID!) {
      markPullRequestReadyForReview(input: {pullRequestId: $id}) {
        pullRequest { isDraft }
      }
    }
    """
    gql.execute(mutation, {"id": pr_node_id})
