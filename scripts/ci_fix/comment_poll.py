"""Comment-triggered entry point for the CI fix bot.

A maintainer comments ``@valkeyrie-ops fix <ci-link>`` on a PR or open issue in
a registry-enabled repository. This scheduled poller finds that comment and
dispatches the existing ``ci-fix`` workflow, which does the actual
diagnose/verify/publish work. Bot-created detector issues may omit the link and
reuse their recorded run. The poller is only the trigger; it owns no fix logic.

Idempotency is a reaction marker on GitHub, not external state. The claim is
atomic: GitHub's create-reaction returns ``201`` when this call added the
reaction and ``200`` when it already existed, so only the run that observes
``201`` dispatches. Two overlapping ticks therefore cannot both fire.

Order per comment: parse, reject bots, classify the parent as a PR or eligible
source issue, authorize the verified comment author, skip if already marked,
claim (atomic), dispatch. Guards fail before claiming so a later tick can retry.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError, loads
from typing import Any, Callable, Optional

from github import Auth, Github
from github.GithubException import GithubException

from scripts.backport.registry import load_registry
from scripts.ci_fix.gate import ParsedCommand, is_authorized, parse_command
from scripts.ci_fix.issue_gate import (
    IssueFixInvocation,
    is_issue_fix_target,
    parse_issue_fix_invocation,
)
from scripts.ci_fix.registry import enabled_ci_fix_repositories
from scripts.common.git_clone import REPO_RE
from scripts.common.github_client import retry_github_call
from scripts.common.polling import env_int, env_seconds, run_poll_loop

logger = logging.getLogger(__name__)

# Reaction used as the claim marker. ``eyes`` reads as "seen, working on it".
_CLAIM_REACTION = "eyes"

# Manual backfill must not page unbounded history on a typo. A week is well past
# any realistic outage while keeping the comment listing cheap.
_MAX_LOOKBACK_MINUTES = 7 * 24 * 60
# GitHub throttles scheduled workflows on a busy public repo: the cron asks for
# a tick every few minutes but runs land roughly hourly, and have been seen over
# two hours apart under load. The lookback must comfortably exceed that real
# worst-case gap, or a comment posted between two ticks falls out of the window
# and is never seen. The reaction marker prevents reprocessing, so a generous
# window only costs a slightly larger comment listing.
_DEFAULT_LOOKBACK_MINUTES = 180
# GitHub App installation tokens are short-lived. Sustained polling is capped
# below an hour so a job does not keep sleeping past token expiry.
_MAX_LOOP_SECONDS = 55 * 60

DispatchFn = Callable[[str, int, ParsedCommand, str, int], None]
"""(repo_full_name, pr_number, command, commenter, comment_id) -> None."""
IssueDispatchFn = Callable[[str, int, IssueFixInvocation, str, int], None]
"""(repo_full_name, issue_number, invocation, commenter, comment_id) -> None."""


@dataclass(frozen=True)
class Claim:
    """An acquired reaction claim that can be released before dispatch."""

    release: Callable[[], None]


ClaimFn = Callable[[Any], Optional[Claim]]


class AllRepositoriesUnavailable(RuntimeError):
    """Raised when a poll iteration cannot read any configured repository."""


class WorkflowDispatchRejected(RuntimeError):
    """Raised when GitHub does not accept a workflow dispatch request."""


def poll_once(
    gh: Github,
    *,
    target_repo: str,
    org: str,
    team_slug: str,
    bot_login: str,
    lookback_minutes: int,
    dispatch: DispatchFn,
    issue_dispatch: IssueDispatchFn | None = None,
    claim: ClaimFn,
) -> int:
    """Scan recent PR and eligible issue comments for new fix commands.

    Returns the number of comments dispatched. ``bot_login`` is the App's own
    login (``<app-slug>[bot]``) used to recognize our own claim reaction.
    ``claim`` performs the atomic reaction claim and returns a releasable claim
    only when this run acquired it; ``dispatch`` triggers the ci-fix workflow.
    Both are injected so the orchestration is testable without real GitHub side
    effects.
    """
    repo = gh.get_repo(target_repo)
    since = time.time() - lookback_minutes * 60
    dispatched = 0

    for comment in _recent_comments(repo, since):
        try:
            if _process_comment(
                gh, repo, comment,
                target_repo=target_repo, org=org, team_slug=team_slug,
                bot_login=bot_login, dispatch=dispatch,
                issue_dispatch=issue_dispatch, claim=claim,
            ):
                dispatched += 1
        except Exception as exc:  # noqa: BLE001 - one bad comment must not abort the tick
            logger.warning("Skipping comment %s after error: %s", getattr(comment, "id", "?"), exc)

    return dispatched


def _process_comment(
    gh: Github,
    repo: Any,
    comment: Any,
    *,
    target_repo: str,
    org: str,
    team_slug: str,
    bot_login: str,
    dispatch: DispatchFn,
    issue_dispatch: IssueDispatchFn | None,
    claim: ClaimFn,
) -> bool:
    """Handle one comment; return True iff it was claimed and dispatched.

    Every guard that cannot proceed returns False without claiming, so a later
    tick can retry. The caller isolates this per comment, so an API error on one
    comment does not stop the rest of the tick.
    """
    if _is_bot(comment):
        return False

    body = comment.body or ""
    command = parse_command(body)
    issue_invocation = parse_issue_fix_invocation(body)
    if command is None and issue_invocation is None:
        return False

    parent = _parent_issue(repo, comment)
    if parent is None:
        return False
    number, issue = parent
    is_pull = getattr(issue, "pull_request", None) is not None
    if is_pull and command is None:
        return False
    if not is_pull and (
        issue_dispatch is None
        or issue_invocation is None
        or not is_issue_fix_target(
            issue,
            issue_invocation,
            detector_login=bot_login,
        )
    ):
        return False

    commenter = _login(comment)
    if not is_authorized(gh, org, team_slug, commenter):
        logger.info("Skipping comment %s from unauthorized %s", comment.id, commenter)
        return False
    if _already_claimed(comment, bot_login):
        return False
    acquired = claim(comment)
    if acquired is None:
        # Another concurrent tick won the claim, or the claim call failed.
        return False

    try:
        if is_pull:
            assert command is not None
            dispatch(target_repo, number, command, commenter, comment.id)
            target_kind = "PR"
        else:
            assert issue_dispatch is not None and issue_invocation is not None
            issue_dispatch(target_repo, number, issue_invocation, commenter, comment.id)
            target_kind = "issue"
    except Exception:
        # A rejected dispatch must not strand the permanent idempotency marker.
        # Releasing the exact reaction created above preserves atomicity: this
        # cannot delete another poller's claim.
        acquired.release()
        raise
    logger.info(
        "Dispatched ci-fix for %s %s#%d (commenter %s)",
        target_repo,
        target_kind,
        number,
        commenter,
    )
    return True


def _recent_comments(repo: Any, since_epoch: float) -> list[Any]:
    """List repository issue comments updated since ``since_epoch``.

    Uses the issue-comments listing (not issue search, which returns issues, not
    comment objects). Covers comments on both issues and PRs; the caller applies
    the target-specific gate.
    """
    since = _from_epoch(since_epoch)
    return list(
        retry_github_call(
            lambda: repo.get_issues_comments(sort="updated", direction="desc", since=since),
            retries=3,
            description=f"list issue comments for {repo.full_name}",
        )
    )


def _pull_request_number(repo: Any, comment: Any) -> int | None:
    """Return the PR number this comment belongs to, or None if it is an issue.

    An issue comment carries the parent issue URL (``.../issues/<n>``). We fetch
    that issue from the known repo and require a ``pull_request`` field. This
    legacy helper remains for callers that specifically need a PR parent.
    The repo is passed in because a listed ``IssueComment`` does not expose its
    repository.
    """
    parent = _parent_issue(repo, comment)
    if parent is None:
        return None
    number, issue = parent
    return number if getattr(issue, "pull_request", None) is not None else None


def _parent_issue(repo: Any, comment: Any) -> tuple[int, Any] | None:
    number = _issue_number_from_url(getattr(comment, "issue_url", "") or "")
    if number is None:
        return None
    issue = retry_github_call(
        lambda: repo.get_issue(number),
        retries=3,
        description=f"get issue {number} for comment {comment.id}",
    )
    return number, issue


def _already_claimed(comment: Any, bot_login: str) -> bool:
    """True if the bot's own App identity already reacted to this comment."""
    reactions = retry_github_call(
        lambda: comment.get_reactions(),
        retries=2,
        description=f"read reactions on comment {comment.id}",
    )
    return any(r.content == _CLAIM_REACTION and r.user.login == bot_login for r in reactions)


def _issue_number_from_url(url: str) -> int | None:
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _is_bot(comment: Any) -> bool:
    return getattr(getattr(comment, "user", None), "type", "") == "Bot"


def _login(comment: Any) -> str:
    return getattr(getattr(comment, "user", None), "login", "") or ""


def _from_epoch(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def claim_via_status(comment: Any) -> Claim | None:
    """Atomically claim ``comment`` and return a releasable claim on a win.

    The win condition is the raw HTTP status: ``201`` means this call created the
    reaction (we own the claim), ``200`` means it already existed (another tick
    owns it). PyGithub's ``create_reaction`` hides this, so we issue the request
    through the requester, which returns the status. The reactions endpoint is
    derived from the comment's own API URL (``comment.url``), since a listed
    comment does not expose its repository. A failed call is treated as "not
    claimed" so a later tick can retry. The create response's reaction id lets
    us delete exactly our claim if workflow dispatch is rejected.
    """
    requester = comment._requester  # noqa: SLF001 - the only status-exposing path
    url = f"{comment.url}/reactions"
    try:
        status, _headers, data = retry_github_call(
            lambda: requester.requestJson("POST", url, input={"content": _CLAIM_REACTION}),
            retries=2,
            description=f"claim reaction on comment {comment.id}",
        )
    except Exception as exc:  # noqa: BLE001 - a failed claim is a clean skip
        logger.warning("Claim failed for comment %s: %s", comment.id, exc)
        return None
    if status != 201:
        return None

    try:
        payload = loads(data) if isinstance(data, str) else data
        reaction_id = payload["id"]
        if not isinstance(reaction_id, int):
            raise TypeError("reaction id is not an integer")
    except (JSONDecodeError, KeyError, TypeError) as exc:
        # GitHub documents an id in every successful create-reaction response.
        # Without it the claim cannot be rolled back safely, so do not dispatch.
        logger.error("Claim response for comment %s had no usable reaction id: %s", comment.id, exc)
        return None

    def _release() -> None:
        def _delete() -> None:
            delete_status, headers, body = requester.requestJson(
                "DELETE",
                f"{url}/{reaction_id}",
            )
            if delete_status not in {204, 404}:
                raise GithubException(delete_status, body, headers)

        retry_github_call(
            _delete,
            retries=3,
            description=f"release claim reaction on comment {comment.id}",
        )

    return Claim(release=_release)


def dispatch_ci_fix(
    gh: Github,
    *,
    agent_repo: str,
    workflow: str,
    ref: str,
) -> DispatchFn:
    """Build a dispatcher that triggers the ci-fix workflow on the agent repo."""

    def _dispatch(
        repo_full_name: str, pr_number: int, command: ParsedCommand, commenter: str,
        comment_id: int = 0,
    ) -> None:
        run_url = (
            f"https://github.com/{command.run_owner}/{command.run_repo}"
            f"/actions/runs/{command.run_id}"
        )
        inputs = {
            "repo": repo_full_name,
            "pr": str(pr_number),
            "run_url": run_url,
            "hint": command.hint,
            "commenter": commenter,
        }
        if comment_id:
            inputs["comment_id"] = str(comment_id)
        wf = gh.get_repo(agent_repo).get_workflow(workflow)

        def _create_dispatch() -> None:
            if not wf.create_dispatch(ref, inputs):
                raise WorkflowDispatchRejected(
                    f"GitHub rejected {workflow} for {repo_full_name}#{pr_number}"
                )

        retry_github_call(
            _create_dispatch,
            retries=2,
            description=f"dispatch {workflow} for {repo_full_name}#{pr_number}",
        )

    return _dispatch


def dispatch_issue_fix(
    gh: Github,
    *,
    agent_repo: str,
    workflow: str,
    ref: str,
) -> IssueDispatchFn:
    """Build a dispatcher for issue-to-draft-PR fixes."""

    def _dispatch(
        repo_full_name: str,
        issue_number: int,
        invocation: IssueFixInvocation,
        commenter: str,
        comment_id: int = 0,
    ) -> None:
        inputs = {
            "repo": repo_full_name,
            "issue": str(issue_number),
            "run_url": invocation.run_url,
            "hint": invocation.hint,
            "commenter": commenter,
        }
        if comment_id:
            inputs["comment_id"] = str(comment_id)
        wf = gh.get_repo(agent_repo).get_workflow(workflow)

        def _create_dispatch() -> None:
            if not wf.create_dispatch(ref, inputs):
                raise WorkflowDispatchRejected(
                    f"GitHub rejected {workflow} for issue "
                    f"{repo_full_name}#{issue_number}"
                )

        retry_github_call(
            _create_dispatch,
            retries=2,
            description=f"dispatch {workflow} for issue {repo_full_name}#{issue_number}",
        )

    return _dispatch


def _lookback_minutes() -> int:
    return env_int(
        "CI_FIX_POLL_LOOKBACK_MINUTES",
        _DEFAULT_LOOKBACK_MINUTES,
        minimum=1,
        maximum=_MAX_LOOKBACK_MINUTES,
    )


def _target_repos() -> tuple[str, ...]:
    """Return an explicit test override or registry-enabled production targets."""
    override = os.environ.get("CI_FIX_POLL_TARGET_REPO")
    if override is not None:
        repos = tuple(
            dict.fromkeys(entry.strip() for entry in override.split(",") if entry.strip())
        )
        if not repos:
            raise ValueError("CI_FIX_POLL_TARGET_REPO did not contain a repository")
        malformed = tuple(repo for repo in repos if not REPO_RE.fullmatch(repo))
        if malformed:
            raise ValueError(
                "CI_FIX_POLL_TARGET_REPO contains malformed repositories: "
                + ", ".join(malformed)
            )
        return repos

    registry_path = os.environ.get("CI_FIX_POLL_REGISTRY", "repos.yml")
    repos = tuple(
        entry.repo
        for entry in enabled_ci_fix_repositories(load_registry(registry_path))
    )
    if not repos:
        raise ValueError(f"No repositories in {registry_path} are enabled for CI fixing")
    return repos


def poll_repositories(
    gh: Github,
    *,
    target_repos: tuple[str, ...],
    org: str,
    team_slug: str,
    bot_login: str,
    lookback_minutes: int,
    dispatch: DispatchFn,
    issue_dispatch: IssueDispatchFn | None = None,
    claim: ClaimFn,
) -> int:
    """Poll all targets, isolating API failures unless every target fails."""
    dispatched = 0
    failures: list[tuple[str, GithubException]] = []
    for target_repo in target_repos:
        try:
            dispatched += poll_once(
                gh,
                target_repo=target_repo,
                org=org,
                team_slug=team_slug,
                bot_login=bot_login,
                lookback_minutes=lookback_minutes,
                dispatch=dispatch,
                issue_dispatch=issue_dispatch,
                claim=claim,
            )
        except GithubException as exc:
            failures.append((target_repo, exc))
            logger.warning("Skipping repository %s after GitHub API error: %s", target_repo, exc)

    if failures and len(failures) == len(target_repos):
        failed_names = ", ".join(repo for repo, _exc in failures)
        raise AllRepositoriesUnavailable(
            f"GitHub API access failed for every CI-fix repository: {failed_names}"
        ) from failures[-1][1]
    return dispatched


def _bot_login() -> str:
    """The login whose claim reaction the poller recognizes as its own.

    Production derives it from the App slug (``<slug>[bot]``). An explicit
    ``CI_FIX_POLL_BOT_LOGIN`` override exists for environments where the actor
    is not an App, such as fork testing with a personal access token.
    """
    override = os.environ.get("CI_FIX_POLL_BOT_LOGIN", "").strip()
    if override:
        return override
    return f"{os.environ['CI_FIX_POLL_APP_SLUG']}[bot]"


def _poll_interval_seconds() -> int:
    return env_seconds("CI_FIX_POLL_INTERVAL_SECONDS", 0, minimum=0)


def _poll_duration_seconds() -> int:
    return env_seconds(
        "CI_FIX_POLL_DURATION_SECONDS",
        0,
        minimum=0,
        maximum=_MAX_LOOP_SECONDS,
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    token = os.environ["CI_FIX_POLL_TOKEN"]
    target_repos = _target_repos()
    agent_repo = os.environ.get("CI_FIX_POLL_AGENT_REPO", "valkey-io/valkey-ci-agent")
    workflow = os.environ.get("CI_FIX_POLL_WORKFLOW", "ci-fix.yml")
    ref = os.environ.get("CI_FIX_POLL_REF", "main")
    org = os.environ.get("CI_FIX_AUTH_ORG", "valkey-io")
    team_slug = os.environ.get("CI_FIX_AUTH_TEAM", "contributors")
    # The App slug (from the token-mint action) identifies our own claim
    # reaction. The installation token cannot reliably call GET /user, so the
    # bot login is derived from the slug rather than looked up.
    bot_login = _bot_login()

    gh = Github(auth=Auth.Token(token))
    dispatch = dispatch_ci_fix(gh, agent_repo=agent_repo, workflow=workflow, ref=ref)
    issue_dispatch = dispatch_issue_fix(
        gh, agent_repo=agent_repo, workflow=workflow, ref=ref
    )

    def _poll() -> int:
        return poll_repositories(
            gh,
            target_repos=target_repos,
            org=org,
            team_slug=team_slug,
            bot_login=bot_login,
            lookback_minutes=_lookback_minutes(),
            dispatch=dispatch,
            issue_dispatch=issue_dispatch,
            claim=claim_via_status,
        )

    results = run_poll_loop(
        _poll,
        interval_seconds=_poll_interval_seconds(),
        duration_seconds=_poll_duration_seconds(),
        logger=logger,
    )
    logger.info(
        "CI fix comment poll dispatched %d fix(es) across %d iteration(s)",
        sum(results),
        len(results),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
