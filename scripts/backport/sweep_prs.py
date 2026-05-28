"""Pull request and remote-branch operations for backport sweeps."""

from __future__ import annotations

import logging
from typing import Any

from scripts.backport.pr_creator import (
    build_pull_search_head_ref,
    create_pull_from_push_repo,
    pull_matches_push_repo,
)
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_models import BranchSweepResult
from scripts.backport.sweep_reporting import build_pr_body
from scripts.common.github_client import retry_github_call

logger = logging.getLogger(__name__)


def find_existing_pr(gh: Any, base_repo: str, push_repo: str, branch: str) -> Any | None:
    repo = retry_github_call(lambda: gh.get_repo(base_repo), retries=2, description=f"get {base_repo}")
    head_ref = build_pull_search_head_ref(base_repo, push_repo, branch)
    pulls = retry_github_call(
        lambda: list(repo.get_pulls(state="open", head=head_ref)),
        retries=2, description="list PRs",
    )
    for pull in pulls:
        if pull_matches_push_repo(pull, push_repo):
            return pull
    return None


def delete_stale_backport_branch(gh: Any, push_repo: str, branch: str) -> None:
    try:
        repo = retry_github_call(lambda: gh.get_repo(push_repo), retries=2, description=f"get {push_repo}")
        ref = retry_github_call(
            lambda: repo.get_git_ref(f"heads/{branch}"),
            retries=1,
            description=f"check ref {branch}",
        )
        if ref is None:
            return
        logger.info("Deleting stale backport branch %s on %s (no open PR)", branch, push_repo)
        retry_github_call(lambda: ref.delete(), retries=2, description=f"delete ref {branch}")
    except Exception as exc:
        msg = str(exc).lower()
        if "not found" in msg or "404" in msg:
            return
        logger.warning("Could not prune stale backport branch %s: %s", branch, exc)


def upsert_pr(
    gh: Any,
    base_repo: str,
    push_repo: str,
    target_branch: str,
    head_branch: str,
    result: BranchSweepResult,
    existing_pr: Any | None,
    gql: GitHubGraphQLClient | None = None,
    draft: bool = False,
) -> str:
    repo = retry_github_call(lambda: gh.get_repo(base_repo), retries=2, description=f"get {base_repo}")
    body = build_pr_body(result, validation_failed=draft)
    title = f"[backport] Backport sweep for {target_branch}"

    if existing_pr:
        retry_github_call(lambda: existing_pr.edit(title=title, body=body), retries=2, description="update PR")
        # Validation can fail and later recover on scheduled sweeps. PyGithub
        # does not expose either draft transition, so use GraphQL for both.
        if draft and not getattr(existing_pr, "draft", False) and gql is not None:
            node_id = getattr(existing_pr, "node_id", None)
            if node_id:
                mark_pr_draft(gql, node_id)
                logger.info(
                    "Converted PR #%d on %s back to draft after validation failure",
                    existing_pr.number, base_repo,
                )
        elif not draft and getattr(existing_pr, "draft", False) and gql is not None:
            node_id = getattr(existing_pr, "node_id", None)
            if node_id:
                mark_pr_ready_for_review(gql, node_id)
                logger.info(
                    "Marked PR #%d on %s ready for review",
                    existing_pr.number, base_repo,
                )
        logger.info("Updated PR #%d on %s", existing_pr.number, base_repo)
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
            draft=draft,
        ),
        retries=2,
        description="create PR",
    )
    logger.info("Created PR #%d on %s", pr.number, base_repo)
    return pr.html_url


def mark_pr_draft(gql: GitHubGraphQLClient, pr_node_id: str) -> None:
    mutation = """
    mutation($id: ID!) {
      convertPullRequestToDraft(input: {pullRequestId: $id}) {
        pullRequest { isDraft }
      }
    }
    """
    gql.execute(mutation, {"id": pr_node_id})


def mark_pr_ready_for_review(gql: GitHubGraphQLClient, pr_node_id: str) -> None:
    mutation = """
    mutation($id: ID!) {
      markPullRequestReadyForReview(input: {pullRequestId: $id}) {
        pullRequest { isDraft }
      }
    }
    """
    gql.execute(mutation, {"id": pr_node_id})
