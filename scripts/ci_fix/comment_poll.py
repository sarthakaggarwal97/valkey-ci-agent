"""Durable comment-triggered entry point for the CI-fix workflow.

The poller records each parsed PR command in an append-only GitHub issue ledger
before authorization or dispatch. It reconciles active ledger entries before
scanning the recent-comment window, so a transient dispatch failure cannot lose
an authorized request after the source comment ages out. Reactions are
best-effort presentation only and never determine durable state.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable
from datetime import datetime, timezone
from itertools import islice
from typing import Any, Optional

from github import Auth, Github

from scripts.ci_fix.dispatch_ledger import DispatchEvent, DispatchLedger
from scripts.ci_fix.gate import authorization_status, parse_command
from scripts.common.github_client import retry_github_call
from scripts.common.polling import env_int, env_seconds, run_poll_loop

logger = logging.getLogger(__name__)

_SEEN_REACTION = "eyes"
_MAX_LOOKBACK_MINUTES = 7 * 24 * 60
_DEFAULT_LOOKBACK_MINUTES = 180
_MAX_LOOP_SECONDS = 55 * 60
_MAX_AUTHORIZATION_ATTEMPTS = 3
_AUTHORIZATION_RETRY_SECONDS = 60
_MAX_DISPATCH_ATTEMPTS = 4
_DISPATCH_DISCOVERY_GRACE_SECONDS = 5 * 60
_MAX_ACTIVE_RUN_SECONDS = 24 * 60 * 60
_MAX_RUN_SCAN = 500
_RUN_CREATED_TOLERANCE_SECONDS = 120
_CORRELATION_RE = re.compile(r"^[0-9a-f]{32}$")

AuthorizationFn = Callable[[str], Optional[bool]]
DispatchFn = Callable[[DispatchEvent], None]
RunLookupFn = Callable[[DispatchEvent], Optional[Any]]
ReactionFn = Callable[[Any], None]


def poll_once(
    gh: Github,
    *,
    target_repo: str,
    lookback_minutes: int,
    ledger: DispatchLedger,
    authorize: AuthorizationFn,
    dispatch: DispatchFn,
    lookup_run: RunLookupFn,
    react: ReactionFn = lambda _comment: None,
    now: int | None = None,
) -> int:
    """Reconcile durable requests, then observe and process recent comments."""
    timestamp = int(time.time() if now is None else now)
    successful_dispatches = 0

    for event in ledger.active():
        try:
            successful_dispatches += _advance_event(
                event,
                ledger=ledger,
                authorize=authorize,
                dispatch=dispatch,
                lookup_run=lookup_run,
                now=timestamp,
            )
        except Exception as exc:  # noqa: BLE001 - one event must not block the ledger
            logger.warning(
                "Could not reconcile CI-fix comment %s: %s",
                event.comment_id,
                exc,
            )

    repo = retry_github_call(
        lambda: gh.get_repo(target_repo),
        retries=3,
        description=f"get target repository {target_repo}",
    )
    since = timestamp - lookback_minutes * 60
    for comment in _recent_comments(repo, since):
        try:
            observed = _observe_comment(
                repo,
                comment,
                target_repo=target_repo,
                ledger=ledger,
                now=timestamp,
            )
            if observed is None:
                continue
            _mark_seen(comment, react)
            successful_dispatches += _advance_event(
                observed,
                ledger=ledger,
                authorize=authorize,
                dispatch=dispatch,
                lookup_run=lookup_run,
                now=timestamp,
            )
        except Exception as exc:  # noqa: BLE001 - one bad comment must not abort the tick
            logger.warning(
                "Skipping comment %s after error: %s",
                getattr(comment, "id", "?"),
                exc,
            )

    return successful_dispatches


def _observe_comment(
    repo: Any,
    comment: Any,
    *,
    target_repo: str,
    ledger: DispatchLedger,
    now: int,
) -> DispatchEvent | None:
    comment_id = getattr(comment, "id", 0)
    if not isinstance(comment_id, int) or isinstance(comment_id, bool) or comment_id <= 0:
        return None
    existing = ledger.get(comment_id)
    if existing is not None:
        return None
    command = parse_command(str(getattr(comment, "body", "") or ""))
    if command is None or _is_bot(comment):
        return None
    pr_number = _pull_request_number(repo, comment)
    if pr_number is None:
        return None
    commenter = _login(comment)
    if not commenter:
        return None
    return ledger.observe(
        comment_id=comment_id,
        repository=target_repo,
        pr_number=pr_number,
        run_owner=command.run_owner,
        run_repo=command.run_repo,
        source_run_id=command.run_id,
        hint=command.hint,
        commenter=commenter,
        now=now,
    )


def _advance_event(
    event: DispatchEvent,
    *,
    ledger: DispatchLedger,
    authorize: AuthorizationFn,
    dispatch: DispatchFn,
    lookup_run: RunLookupFn,
    now: int,
) -> int:
    """Advance one event as far as one reconciliation tick safely permits."""
    if now < event.updated_at:
        raise RuntimeError("local clock predates the dispatch ledger")

    if event.state == "observed":
        if (
            event.authorization_attempts
            and now - event.updated_at < _AUTHORIZATION_RETRY_SECONDS
        ):
            return 0
        if event.authorization_attempts >= _MAX_AUTHORIZATION_ATTEMPTS:
            ledger.transition(
                event,
                "completed",
                completion="authorization-unavailable",
                now=now,
            )
            return 0
        event = ledger.transition(
            event,
            "observed",
            authorization_attempts=event.authorization_attempts + 1,
            now=now,
        )
        authorized = authorize(event.commenter)
        if authorized is None:
            return 0
        if not authorized:
            ledger.transition(event, "completed", completion="unauthorized", now=now)
            return 0
        event = ledger.transition(event, "authorized", now=now)

    if event.state == "authorized":
        event = ledger.transition(
            event,
            "dispatching",
            dispatch_attempts=event.dispatch_attempts + 1,
            now=now,
        )
        dispatch(event)
        return 1

    if event.state == "dispatching":
        run = lookup_run(event)
        if run is not None:
            event = ledger.transition(
                event,
                "dispatched",
                workflow_run_id=_positive_run_id(run),
                now=now,
            )
            return _complete_run_if_terminal(event, run, ledger=ledger, now=now)
        if now - event.updated_at < _DISPATCH_DISCOVERY_GRACE_SECONDS:
            return 0
        if event.dispatch_attempts >= _MAX_DISPATCH_ATTEMPTS:
            ledger.transition(
                event,
                "completed",
                completion="dispatch-unobserved",
                now=now,
            )
            return 0
        event = ledger.transition(
            event,
            "dispatching",
            dispatch_attempts=event.dispatch_attempts + 1,
            now=now,
        )
        dispatch(event)
        return 1

    if event.state == "dispatched":
        run = lookup_run(event)
        if run is not None:
            return _complete_run_if_terminal(event, run, ledger=ledger, now=now)
        if now - event.observed_at >= _MAX_ACTIVE_RUN_SECONDS:
            ledger.transition(
                event,
                "completed",
                completion="run-unavailable",
                now=now,
            )
        return 0

    return 0


def _complete_run_if_terminal(
    event: DispatchEvent,
    run: Any,
    *,
    ledger: DispatchLedger,
    now: int,
) -> int:
    if str(getattr(run, "status", "") or "") == "completed":
        conclusion = str(getattr(run, "conclusion", "") or "unknown").lower()
        conclusion = re.sub(r"[^a-z0-9_-]", "-", conclusion)[:80] or "unknown"
        ledger.transition(
            event,
            "completed",
            completion=f"run-{conclusion}",
            now=now,
        )
    elif now - event.observed_at >= _MAX_ACTIVE_RUN_SECONDS:
        ledger.transition(event, "completed", completion="run-timeout", now=now)
    return 0


def _positive_run_id(run: Any) -> int:
    run_id = getattr(run, "id", 0)
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
        raise RuntimeError("correlated CI-fix workflow run has no valid ID")
    return run_id


def _recent_comments(repo: Any, since_epoch: float) -> list[Any]:
    since = _from_epoch(since_epoch)
    return list(
        retry_github_call(
            lambda: repo.get_issues_comments(
                sort="updated",
                direction="desc",
                since=since,
            ),
            retries=3,
            description=f"list issue comments for {repo.full_name}",
        )
    )


def _pull_request_number(repo: Any, comment: Any) -> int | None:
    number = _issue_number_from_url(getattr(comment, "issue_url", "") or "")
    if number is None:
        return None
    issue = retry_github_call(
        lambda: repo.get_issue(number),
        retries=3,
        description=f"get issue {number} for comment {comment.id}",
    )
    return number if getattr(issue, "pull_request", None) is not None else None


def _issue_number_from_url(url: str) -> int | None:
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _is_bot(comment: Any) -> bool:
    return getattr(getattr(comment, "user", None), "type", "") == "Bot"


def _login(comment: Any) -> str:
    return getattr(getattr(comment, "user", None), "login", "") or ""


def _from_epoch(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _mark_seen(comment: Any, react: ReactionFn) -> None:
    try:
        react(comment)
    except Exception as exc:  # noqa: BLE001 - presentation never controls state
        logger.warning("Could not mark comment %s as seen: %s", comment.id, exc)


def react_seen(comment: Any) -> None:
    """Add the display-only seen reaction through PyGithub's public API."""
    retry_github_call(
        lambda: comment.create_reaction(_SEEN_REACTION),
        retries=2,
        description=f"mark comment {comment.id} as seen",
    )


def dispatch_ci_fix(
    gh: Github,
    *,
    agent_repo: str,
    workflow: str,
    ref: str,
) -> DispatchFn:
    """Build a dispatcher that includes the ledger correlation in every run."""

    def _dispatch(event: DispatchEvent) -> None:
        if not _CORRELATION_RE.fullmatch(event.correlation_id):
            raise ValueError("refusing to dispatch an invalid correlation ID")
        run_url = (
            f"https://github.com/{event.run_owner}/{event.run_repo}"
            f"/actions/runs/{event.source_run_id}"
        )
        inputs = {
            "repo": event.repository,
            "pr": str(event.pr_number),
            "run_url": run_url,
            "hint": event.hint,
            "commenter": event.commenter,
            "comment_id": str(event.comment_id),
            "correlation_id": event.correlation_id,
        }
        wf = retry_github_call(
            lambda: gh.get_repo(agent_repo).get_workflow(workflow),
            retries=3,
            description=f"get workflow {workflow}",
        )
        retry_github_call(
            lambda: wf.create_dispatch(ref, inputs),
            retries=2,
            description=f"dispatch {workflow} for {event.repository}#{event.pr_number}",
        )

    return _dispatch


def lookup_ci_fix_run(
    gh: Github,
    *,
    agent_repo: str,
    workflow: str,
    bot_login: str,
) -> RunLookupFn:
    """Build an exact workflow-run resolver for durable dispatch reconciliation."""

    def _lookup(event: DispatchEvent) -> Any | None:
        repo = retry_github_call(
            lambda: gh.get_repo(agent_repo),
            retries=3,
            description=f"get agent repository {agent_repo}",
        )
        if event.workflow_run_id:
            return retry_github_call(
                lambda: repo.get_workflow_run(event.workflow_run_id),
                retries=3,
                description=f"reload CI-fix run {event.workflow_run_id}",
            )
        wf = retry_github_call(
            lambda: repo.get_workflow(workflow),
            retries=3,
            description=f"get workflow {workflow}",
        )
        runs = retry_github_call(
            lambda: list(
                islice(
                    wf.get_runs(event="workflow_dispatch"),
                    _MAX_RUN_SCAN,
                )
            ),
            retries=3,
            description=f"list workflow runs for {workflow}",
        )
        expected_title = f"CI fix [dispatch:{event.correlation_id}]"
        matches = [
            run
            for run in runs
            if str(getattr(run, "display_title", "") or "") == expected_title
            and str(getattr(run, "event", "") or "") == "workflow_dispatch"
            and _login_of(getattr(run, "actor", None)) == bot_login
            and _run_created_after(run, event.observed_at)
        ]
        if not matches:
            return None
        if len(matches) > 1:
            logger.warning(
                "Found %d CI-fix runs for correlation %s; reconciling earliest",
                len(matches),
                event.correlation_id,
            )
        return min(matches, key=_positive_run_id)

    return _lookup


def _login_of(user: Any) -> str:
    return str(getattr(user, "login", "") or "")


def _run_created_after(run: Any, observed_at: int) -> bool:
    created_at = getattr(run, "created_at", None)
    if not isinstance(created_at, datetime):
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return created_at.timestamp() >= observed_at - _RUN_CREATED_TOLERANCE_SECONDS


def _lookback_minutes() -> int:
    return env_int(
        "CI_FIX_POLL_LOOKBACK_MINUTES",
        _DEFAULT_LOOKBACK_MINUTES,
        minimum=1,
        maximum=_MAX_LOOKBACK_MINUTES,
    )


def _bot_login() -> str:
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
    target_repo = os.environ.get("CI_FIX_POLL_TARGET_REPO", "valkey-io/valkey")
    agent_repo = os.environ.get("CI_FIX_POLL_AGENT_REPO", "valkey-io/valkey-ci-agent")
    workflow = os.environ.get("CI_FIX_POLL_WORKFLOW", "ci-fix.yml")
    ref = os.environ.get("CI_FIX_POLL_REF", "main")
    org = os.environ.get("CI_FIX_AUTH_ORG", "valkey-io")
    team_slug = os.environ.get("CI_FIX_AUTH_TEAM", "contributors")

    gh = Github(auth=Auth.Token(token))
    bot_login = _bot_login()
    ledger = DispatchLedger(
        gh,
        agent_repo=agent_repo,
        writer_login=bot_login,
    )
    dispatch = dispatch_ci_fix(gh, agent_repo=agent_repo, workflow=workflow, ref=ref)
    lookup_run = lookup_ci_fix_run(
        gh,
        agent_repo=agent_repo,
        workflow=workflow,
        bot_login=bot_login,
    )

    def _poll() -> int:
        return poll_once(
            gh,
            target_repo=target_repo,
            lookback_minutes=_lookback_minutes(),
            ledger=ledger,
            authorize=lambda username: authorization_status(
                gh,
                org,
                team_slug,
                username,
            ),
            dispatch=dispatch,
            lookup_run=lookup_run,
            react=react_seen,
        )

    results = run_poll_loop(
        _poll,
        interval_seconds=_poll_interval_seconds(),
        duration_seconds=_poll_duration_seconds(),
        logger=logger,
    )
    logger.info(
        "CI-fix comment poll dispatched %d workflow request(s) across %d iteration(s)",
        sum(results),
        len(results),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
