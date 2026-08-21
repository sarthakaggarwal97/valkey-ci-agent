"""Poll automated release PRs and dispatch one batch per PR."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from github import Auth, Github

from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.ci_fix.gate import is_authorized
from scripts.common.github_client import retry_github_call
from scripts.common.polling import env_seconds, run_poll_loop
from scripts.release_notes.review import (
    REPOSITORIES,
    ReleasePR,
    ReviewRefused,
    list_review_threads,
    parse_status,
    review_batch_id,
    review_payload_json,
    selected_reviews,
    status_body,
    validate_release_pr,
)

logger = logging.getLogger(__name__)

_MAX_LOOP_SECONDS = 55 * 60
DispatchFn = Callable[[ReleasePR, int], None]


def poll_once(
    gh: Github,
    gql: GitHubGraphQLClient,
    *,
    repositories: tuple[str, ...],
    bot_login: str,
    dispatch: DispatchFn,
    authorize: Callable[[str], bool] | None = None,
    org: str = "valkey-io",
    team_slug: str = "contributors",
) -> int:
    """Scan all open PRs and dispatch every complete actionable batch."""

    auth_cache: dict[str, bool] = {}

    def authorized(login: str) -> bool:
        if login not in auth_cache:
            auth_cache[login] = (
                authorize(login)
                if authorize is not None
                else is_authorized(gh, org, team_slug, login)
            )
        return auth_cache[login]

    dispatched = 0
    for repo_full_name in repositories:
        try:
            repo = retry_github_call(
                lambda: gh.get_repo(repo_full_name),
                retries=3,
                description=f"get release-review repository {repo_full_name}",
            )
            pulls = retry_github_call(
                lambda: list(repo.get_pulls(state="open")),
                retries=3,
                description=f"list open PRs in {repo_full_name}",
            )
        except Exception as exc:  # noqa: BLE001 - continue with sibling repos
            logger.warning("Could not scan %s: %s", repo_full_name, exc)
            continue

        for pr in pulls:
            try:
                release = validate_release_pr(pr, repo_full_name)
                if _process_pr(
                    pr,
                    release,
                    gql,
                    bot_login=bot_login,
                    authorized=authorized,
                    dispatch=dispatch,
                ):
                    dispatched += 1
            except ReviewRefused as exc:
                logger.info(
                    "Skipping %s#%s: %s",
                    repo_full_name,
                    getattr(pr, "number", "?"),
                    exc,
                )
            except Exception as exc:  # noqa: BLE001 - continue with sibling PRs
                logger.warning(
                    "Could not process %s#%s: %s",
                    repo_full_name,
                    getattr(pr, "number", "?"),
                    exc,
                )
    return dispatched


def _process_pr(
    pr: Any,
    release: ReleasePR,
    gql: GitHubGraphQLClient,
    *,
    bot_login: str,
    authorized: Callable[[str], bool],
    dispatch: DispatchFn,
) -> bool:
    reviews = selected_reviews(
        list_review_threads(gql, release.repo_full_name, release.number),
        release.notes_path,
        authorized,
    )
    if not reviews:
        return False
    review_payload_json(reviews)
    batch_id = review_batch_id(reviews)

    comments = retry_github_call(
        lambda: list(pr.get_issue_comments()),
        retries=3,
        description=f"list status comments on {release.repo_full_name}#{release.number}",
    )
    for comment in comments:
        marker = parse_status(comment, bot_login)
        if (
            marker is not None
            and marker.head_sha == release.head_sha
            and (
                marker.status == "addressing"
                or (
                    marker.batch_id == batch_id
                    and marker.status in {"addressed", "refused"}
                )
            )
        ):
            return False

    status = retry_github_call(
        lambda: pr.create_issue_comment(
            status_body(
                "addressing",
                release.head_sha,
                batch_id,
                len(reviews),
            )
        ),
        retries=3,
        description=f"post status on {release.repo_full_name}#{release.number}",
    )
    status_id = getattr(status, "id", 0)
    if isinstance(status_id, bool) or not isinstance(status_id, int) or status_id <= 0:
        raise RuntimeError("GitHub created a status comment without an id")
    try:
        dispatch(release, status_id)
    except Exception as exc:
        reason = str(exc)
        retry_github_call(
            lambda: status.edit(
                status_body(
                    "failed",
                    release.head_sha,
                    batch_id,
                    len(reviews),
                    reason=reason,
                )
            ),
            retries=3,
            description="mark failed release-review dispatch",
        )
        raise
    return True


def dispatch_release_review(
    gh: Github,
    *,
    agent_repo: str = "valkey-io/valkey-ci-agent",
    workflow: str = "release-notes-review.yml",
    ref: str = "main",
) -> DispatchFn:
    def dispatch(release: ReleasePR, status_comment_id: int) -> None:
        target = retry_github_call(
            lambda: gh.get_repo(agent_repo).get_workflow(workflow),
            retries=3,
            description=f"get workflow {workflow}",
        )
        retry_github_call(
            lambda: target.create_dispatch(
                ref,
                {
                    "repo": release.repo_full_name.rsplit("/", 1)[-1],
                    "pr": str(release.number),
                    "head_sha": release.head_sha,
                    "status_comment_id": str(status_comment_id),
                },
            ),
            retries=3,
            description=f"dispatch release review for {release.repo_full_name}#{release.number}",
        )

    return dispatch


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    token = os.environ["RELEASE_NOTES_REVIEW_TOKEN"]
    gh = Github(auth=Auth.Token(token))
    gql = GitHubGraphQLClient(token)
    bot_login = f"{os.environ['RELEASE_NOTES_REVIEW_APP_SLUG']}[bot]"
    dispatch = dispatch_release_review(gh)

    results = run_poll_loop(
        lambda: poll_once(
            gh,
            gql,
            repositories=REPOSITORIES,
            bot_login=bot_login,
            dispatch=dispatch,
        ),
        interval_seconds=env_seconds(
            "RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS",
            0,
            minimum=0,
        ),
        duration_seconds=env_seconds(
            "RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS",
            0,
            minimum=0,
            maximum=_MAX_LOOP_SECONDS,
        ),
        logger=logger,
    )
    logger.info(
        "Dispatched %d release-review batch(es) across %d iteration(s)",
        sum(results),
        len(results),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
