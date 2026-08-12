"""Required-check evaluation on the exact candidate SHA (informational).

Split from reconcile so the one question it answers ("what did the policy's
required checks report on this exact commit, from the right workflow?")
lives behind :func:`evaluate_required_checks`. The results feed the
tracker's Required Checks table for a human to read; they are NOT a gate:
they never produce blockers, never hold a phase, and never affect
readiness. Qualification (the exact-SHA no-publish build) is the only
pre-publication technical gate. The branch-level daily-CI gate
(:func:`evaluate_daily`, :func:`daily_blockers`) lives here as its sibling
and, unlike the per-commit checks, does still gate READY when configured.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import CheckState, DailyCiState, DailyCiStatus, RequiredCheck
from scripts.release.policy import RepoReleasePolicy
from scripts.release.qualification import RUN_SCAN_LIMIT
from scripts.release.release_refs import humanize_minutes

logger = logging.getLogger(__name__)


def evaluate_required_checks(
    repo: Any, policy: RepoReleasePolicy, sha: str,
) -> tuple[RequiredCheck, ...]:
    """Evaluate every policy-required check against the exact *sha*.

    The results are informational: they render on the tracker so a human
    sees the candidate's CI state before approving, but they never gate
    the release (qualification is the only pre-publication technical
    gate).

    Only runs from the policy's ``checks_workflow`` count: check-run names
    are not unique across workflows (valkey's ci.yml and daily.yml share job
    names), so a same-named run from another workflow on the candidate SHA
    must neither satisfy a requirement nor clobber a passed one -- the
    display would otherwise misreport which workflow's verdict it shows.
    The mapping is check run -> check suite -> workflow run, all from list
    payloads.

    Within the workflow, the *latest* run per name wins (by start time, then
    id), so a maintainer-triggered rerun of a failed job on the same SHA
    supersedes the failed attempt. A required check with no run at all is
    reported MISSING: absence of evidence is not displayed as passing.
    """
    suite_ids = _checks_workflow_suite_ids(repo, policy, sha)
    commit = retry_github_call(
        lambda: repo.get_commit(sha),
        retries=2, description=f"get commit {sha[:12]}",
    )
    runs = retry_github_call(
        lambda: list(commit.get_check_runs()),
        retries=2, description=f"list check runs on {sha[:12]}",
    )
    latest_by_name: dict[str, Any] = {}
    for run in runs:
        if _suite_id(run) not in suite_ids:
            continue
        current = latest_by_name.get(run.name)
        if current is None or _run_order_key(run) > _run_order_key(current):
            latest_by_name[run.name] = run

    results: list[RequiredCheck] = []
    stalled_before = datetime.now(timezone.utc) - timedelta(
        minutes=policy.check_timeout_minutes,
    )
    for name in policy.required_checks:
        run = latest_by_name.get(name)
        if run is None:
            results.append(RequiredCheck(name=name, state=CheckState.MISSING))
            continue
        if run.status != "completed":
            # A run that never started has no started_at; fall back to its
            # creation time so it cannot dodge STALLED forever.
            started = getattr(run, "started_at", None) or getattr(run, "created_at", None)
            state = (CheckState.STALLED
                     if started is not None and started < stalled_before
                     else CheckState.PENDING)
        elif run.conclusion == "success":
            state = CheckState.PASSED
        else:
            # failure, cancelled, timed_out, action_required, stale, neutral,
            # skipped: none of these is evidence the required check passed.
            state = CheckState.FAILED
        results.append(RequiredCheck(name=name, state=state, url=run.html_url or ""))
    return tuple(results)


def _checks_workflow_suite_ids(repo: Any, policy: RepoReleasePolicy, sha: str) -> set[int]:
    """Check-suite ids of *sha*'s runs of the policy's ``checks_workflow``.

    An empty set (workflow never ran on this SHA) makes every required check
    display as MISSING, mirroring the no-run case.
    """
    workflow_runs = retry_github_call(
        lambda: list(repo.get_workflow_runs(head_sha=sha)),
        retries=2, description=f"list workflow runs on {sha[:12]}",
    )
    return {
        run.check_suite_id for run in workflow_runs
        if (run.path or "").rsplit("/", 1)[-1] == policy.checks_workflow
    }


def _suite_id(run: Any) -> int | None:
    """The check suite id a check run belongs to, from the list payload.

    Read from ``_rawData`` (the already-stored payload) rather than the
    deprecated ``check_suite_id`` property or the ``check_suite`` attribute,
    either of which can trigger a per-run completion GET (same rationale as
    issue_dedup's ``_drop_pull_requests``).
    """
    suite = run._rawData.get("check_suite") or {}
    return suite.get("id")


def _run_order_key(run: Any) -> tuple[Any, int]:
    started = getattr(run, "started_at", None)
    return ((started is not None, started), run.id or 0)


def evaluate_daily(
    repo: Any, policy: RepoReleasePolicy, branch: str, now: datetime,
) -> DailyCiStatus:
    """Evaluate the branch-level daily-CI gate for *branch* at *now*.

    Branch-level observation, complementing the per-commit required checks:
    the scheduled daily workflow does not run per commit, so the question is
    "is the release branch's most recent completed daily run green and
    fresh?", never "did it run on the candidate SHA?".

    Binding is by workflow file (the policy's ``daily_workflow``), so a
    same-named job in another workflow can never satisfy the gate. The scan
    is capped like the other run scans. Only *completed* runs carry a
    verdict; a run still executing is mentioned in the detail but ignored
    otherwise. Staleness compares the newest completed run's creation time
    to *now* with a strict greater-than, so an age exactly at the bound is
    still fresh. An unreadable workflow fails closed as MISSING.
    """
    if policy.daily_workflow is None:
        return DailyCiStatus(state=DailyCiState.SKIPPED)
    try:
        workflow = retry_github_call(
            lambda: repo.get_workflow(policy.daily_workflow),
            retries=2, description=f"get workflow {policy.daily_workflow}",
        )
    except GithubException:
        logger.warning("Cannot read %s on %s; the daily gate fails closed",
                       policy.daily_workflow, policy.repo)
        return DailyCiStatus(state=DailyCiState.MISSING,
                             detail="Cannot read the daily workflow")
    runs = retry_github_call(
        workflow.get_runs,
        retries=2, description=f"list {policy.daily_workflow} runs",
    )
    newest_completed = None
    saw_in_progress = False  # newer branch activity still executing
    for index, run in enumerate(runs):
        if index >= RUN_SCAN_LIMIT:
            break
        if run.head_branch != branch:
            continue
        if run.status != "completed":
            saw_in_progress = True
            continue
        newest_completed = run
        break
    if newest_completed is None:
        detail = f"No completed daily run on branch {branch} yet"
        if saw_in_progress:
            detail += " (one is in progress)"
        return DailyCiStatus(state=DailyCiState.MISSING, detail=detail)

    in_progress_note = "; a newer daily run is in progress" if saw_in_progress else ""
    age = now - newest_completed.created_at
    age_text = humanize_minutes(max(0, int(age.total_seconds() // 60)))
    if age > timedelta(hours=policy.daily_max_age_hours or 0):
        return DailyCiStatus(
            state=DailyCiState.STALE,
            run_id=newest_completed.id,
            url=newest_completed.html_url or "",
            detail=(f"Newest daily run is {age_text} old, older than the "
                    f"{policy.daily_max_age_hours}-hour freshness bound"
                    f"{in_progress_note}"),
        )
    if newest_completed.conclusion != "success":
        return DailyCiStatus(
            state=DailyCiState.FAILED,
            run_id=newest_completed.id,
            url=newest_completed.html_url or "",
            detail=f"Daily run failed{in_progress_note}",
        )
    return DailyCiStatus(
        state=DailyCiState.PASSED,
        run_id=newest_completed.id,
        url=newest_completed.html_url or "",
        detail=f"Passed ({age_text} ago{in_progress_note})",
    )


def daily_blockers(daily: DailyCiStatus) -> list[str]:
    """The blocker line a non-green daily verdict adds; empty when the gate
    is satisfied (PASSED) or unconfigured (SKIPPED)."""
    if daily.state in (DailyCiState.PASSED, DailyCiState.SKIPPED):
        return []
    return [f"Daily CI: {daily.detail}"]
