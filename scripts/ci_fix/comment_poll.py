"""Comment-triggered entry point for the CI fix bot.

A maintainer comments ``@valkeyrie-ops fix <ci-link>`` on a valkey-io/valkey PR.
This scheduled poller finds that comment and dispatches the existing ``ci-fix``
workflow, which does the actual diagnose/verify/push. The poller is only the
trigger; it owns no fix logic.

Idempotency uses both an atomic reaction claim and a workflow-run correlation
marker. GitHub's create-reaction returns ``201`` when this call added the
reaction and ``200`` when it already existed. A later serialized tick may
reconcile an existing claim: it checks for a run named with that comment id
before attempting dispatch. This recovers a failed dispatch without duplicating
one whose HTTP response was lost after GitHub accepted it.

Order per comment: parse, reject bots, confirm it is a PR, authorize the
verified comment author, claim if needed (atomic), reconcile, dispatch. Guards
before the claim remain retryable, and a dispatch failure retains the claim so
the next tick can reconcile ambiguous network outcomes.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from github import Auth, Github

from scripts.ci_fix.gate import ParsedCommand, is_authorized, parse_command
from scripts.common.github_client import retry_github_call
from scripts.common.polling import env_int, env_seconds, run_poll_loop

logger = logging.getLogger(__name__)

# Reaction used as the claim marker. ``eyes`` reads as "seen, working on it".
_CLAIM_REACTION = "eyes"
_OUTCOME_REACTIONS = frozenset({"+1", "-1"})

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

DispatchFn = Callable[[str, int, ParsedCommand, str, int], bool]
"""Dispatch and return True only when this call created a new workflow run."""


def poll_once(
    gh: Github,
    *,
    target_repo: str,
    org: str,
    team_slug: str,
    bot_login: str,
    lookback_minutes: int,
    dispatch: DispatchFn,
    claim: Callable[[Any], bool],
) -> int:
    """Scan recent PR comments once and dispatch a fix for each new command.

    Returns the number of comments dispatched. ``bot_login`` is the App's own
    login (``<app-slug>[bot]``) used to recognize our own claim reaction.
    ``claim`` performs the atomic reaction claim and returns True only when this
    run acquired it. ``dispatch`` first reconciles the comment correlation marker
    and returns True only when it creates a new run. Both are injected so the
    orchestration is testable without real GitHub side effects.
    """
    repo = gh.get_repo(target_repo)
    since = time.time() - lookback_minutes * 60
    dispatched = 0

    for comment in _recent_comments(repo, since):
        try:
            if _process_comment(
                gh, repo, comment,
                target_repo=target_repo, org=org, team_slug=team_slug,
                bot_login=bot_login, dispatch=dispatch, claim=claim,
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
    claim: Callable[[Any], bool],
) -> bool:
    """Handle one comment; return True iff this call dispatched a new run.

    A bot-owned existing claim is reconciled rather than skipped. The workflow
    run's comment marker makes this safe after an ambiguous dispatch response.
    The workflow serializes pollers per target repository, so only one recovery
    tick performs that check-and-dispatch sequence at a time.
    """
    command = parse_command(comment.body or "")
    if command is None:
        return False
    if _is_bot(comment):
        return False
    pr_number = _pull_request_number(repo, comment)
    if pr_number is None:
        return False
    commenter = _login(comment)
    if not is_authorized(gh, org, team_slug, commenter):
        logger.info("Skipping comment %s from unauthorized %s", comment.id, commenter)
        return False
    already_claimed, completed = _bot_reaction_state(comment, bot_login)
    if completed:
        return False
    if not already_claimed and not claim(comment):
        # Another concurrent tick won the claim, or the claim call failed.
        return False
    created = dispatch(target_repo, pr_number, command, commenter, comment.id)
    if created:
        logger.info(
            "Dispatched ci-fix for %s#%d (commenter %s)",
            target_repo,
            pr_number,
            commenter,
        )
    elif already_claimed:
        logger.info("Claimed comment %s already has a ci-fix run", comment.id)
    return created


def _recent_comments(repo: Any, since_epoch: float) -> list[Any]:
    """List repository issue comments updated since ``since_epoch``.

    Uses the issue-comments listing (not issue search, which returns issues, not
    comment objects). Covers comments on both issues and PRs; the caller filters
    to PRs.
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
    that issue from the known repo and require a ``pull_request`` field: the
    ci-fix engine only acts on PRs, so an issue comment must never be claimed.
    The repo is passed in because a listed ``IssueComment`` does not expose its
    repository.
    """
    number = _issue_number_from_url(getattr(comment, "issue_url", "") or "")
    if number is None:
        return None
    issue = retry_github_call(
        lambda: repo.get_issue(number),
        retries=3,
        description=f"get issue {number} for comment {comment.id}",
    )
    return number if getattr(issue, "pull_request", None) is not None else None


def _already_claimed(comment: Any, bot_login: str) -> bool:
    """True if the bot's own App identity already reacted to this comment."""
    claimed, _completed = _bot_reaction_state(comment, bot_login)
    return claimed


def _bot_reaction_state(comment: Any, bot_login: str) -> tuple[bool, bool]:
    """Return the bot-owned (claimed, terminal-outcome) reaction state."""
    reactions = retry_github_call(
        lambda: comment.get_reactions(),
        retries=2,
        description=f"read reactions on comment {comment.id}",
    )
    owned = {
        reaction.content
        for reaction in reactions
        if getattr(getattr(reaction, "user", None), "login", "") == bot_login
    }
    return _CLAIM_REACTION in owned, bool(owned & _OUTCOME_REACTIONS)


def _issue_number_from_url(url: str) -> int | None:
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _is_bot(comment: Any) -> bool:
    return getattr(getattr(comment, "user", None), "type", "") == "Bot"


def _login(comment: Any) -> str:
    return getattr(getattr(comment, "user", None), "login", "") or ""


def _from_epoch(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def claim_via_status(comment: Any) -> bool:
    """Atomically claim ``comment`` by creating the reaction, return True on win.

    The win condition is the raw HTTP status: ``201`` means this call created the
    reaction (we own the claim), ``200`` means it already existed (another tick
    owns it). PyGithub's ``create_reaction`` hides this, so we issue the request
    through the requester, which returns the status. The reactions endpoint is
    derived from the comment's own API URL (``comment.url``), since a listed
    comment does not expose its repository. A failed call is treated as "not
    claimed" so a later tick can retry.
    """
    requester = comment._requester  # noqa: SLF001 - the only status-exposing path
    url = f"{comment.url}/reactions"
    try:
        status, _headers, _data = retry_github_call(
            lambda: requester.requestJson("POST", url, input={"content": _CLAIM_REACTION}),
            retries=2,
            description=f"claim reaction on comment {comment.id}",
        )
    except Exception as exc:  # noqa: BLE001 - a failed claim is a clean skip
        logger.warning("Claim failed for comment %s: %s", comment.id, exc)
        return False
    return status == 201


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
    ) -> bool:
        if comment_id <= 0:
            raise ValueError("comment-triggered dispatch requires a positive comment id")
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
        marker = _dispatch_marker(repo_full_name, comment_id)
        if _workflow_has_marker(wf, marker):
            return False

        # Do not automatically retry this POST. A connection can fail after
        # GitHub accepted the dispatch, and replaying it would create a duplicate.
        # The claim remains on the comment; the next serialized poll tick checks
        # the run-name marker above and retries only if no run appeared.
        created = wf.create_dispatch(ref, inputs)
        if not created:
            raise RuntimeError(
                f"GitHub did not accept {workflow} for {repo_full_name}#{pr_number}"
            )
        return True

    return _dispatch


def _dispatch_marker(repo_full_name: str, comment_id: int) -> str:
    return f"[ci-fix-comment:{repo_full_name}:{comment_id}]"


def _workflow_has_marker(workflow: Any, marker: str) -> bool:
    """Return whether a recent workflow_dispatch run carries ``marker``."""
    runs = retry_github_call(
        lambda: list(workflow.get_runs(event="workflow_dispatch")[:100]),
        retries=3,
        description=f"reconcile CI-fix dispatch {marker}",
    )
    return any(
        marker
        in (
            f"{getattr(run, 'display_title', '') or ''} "
            f"{getattr(run, 'name', '') or ''}"
        )
        for run in runs
    )


def _lookback_minutes() -> int:
    return env_int(
        "CI_FIX_POLL_LOOKBACK_MINUTES",
        _DEFAULT_LOOKBACK_MINUTES,
        minimum=1,
        maximum=_MAX_LOOKBACK_MINUTES,
    )


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
    dispatch_token = os.environ.get("CI_FIX_DISPATCH_TOKEN", token)
    target_repo = os.environ.get("CI_FIX_POLL_TARGET_REPO", "valkey-io/valkey")
    agent_repo = os.environ.get("CI_FIX_POLL_AGENT_REPO", "valkey-io/valkey-ci-agent")
    workflow = os.environ.get("CI_FIX_POLL_WORKFLOW", "ci-fix.yml")
    ref = os.environ.get("CI_FIX_POLL_REF", "main")
    org = os.environ.get("CI_FIX_AUTH_ORG", "valkey-io")
    team_slug = os.environ.get("CI_FIX_AUTH_TEAM", "contributors")
    # The App slug (from the token-mint action) identifies our own claim
    # reaction. The installation token cannot reliably call GET /user, so the
    # bot login is derived from the slug rather than looked up.
    bot_login = _bot_login()

    # Keep the target-repository token and the agent-repository dispatch token
    # separate. Each installation token is scoped to one repository and one
    # responsibility; a poller credential that can react to target comments
    # does not also need actions:write on the control-plane repository.
    gh = Github(auth=Auth.Token(token))
    dispatch_gh = Github(auth=Auth.Token(dispatch_token))
    dispatch = dispatch_ci_fix(
        dispatch_gh, agent_repo=agent_repo, workflow=workflow, ref=ref,
    )

    def _poll() -> int:
        return poll_once(
            gh,
            target_repo=target_repo,
            org=org,
            team_slug=team_slug,
            bot_login=bot_login,
            lookback_minutes=_lookback_minutes(),
            dispatch=dispatch,
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
