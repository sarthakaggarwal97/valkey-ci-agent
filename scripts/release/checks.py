"""Required-check evaluation on the exact candidate SHA.

Split from reconcile so the one question it answers — "did the policy's
required checks pass on this exact commit, from the right workflow?" —
lives behind a two-function interface (:func:`evaluate_required_checks`,
:func:`check_blockers`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.common.github_client import retry_github_call
from scripts.release.models import CheckState, RequiredCheck
from scripts.release.policy import RepoReleasePolicy

logger = logging.getLogger(__name__)


def evaluate_required_checks(
    repo: Any, policy: RepoReleasePolicy, sha: str,
) -> tuple[RequiredCheck, ...]:
    """Evaluate every policy-required check against the exact *sha*.

    Only runs from the policy's ``checks_workflow`` count: check-run names
    are not unique across workflows (valkey's ci.yml and daily.yml share job
    names), so a same-named run from another workflow on the candidate SHA
    must neither satisfy a requirement nor clobber a passed one. The mapping
    is check run -> check suite -> workflow run, all from list payloads.

    Within the workflow, the *latest* run per name wins (by start time, then
    id), so a maintainer-triggered rerun of a failed job on the same SHA
    supersedes the failed attempt. A required check with no run at all is
    MISSING, which blocks readiness: absence of evidence is not passing.
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
        elif run.status != "completed":
            # A run that never started has no started_at; fall back to its
            # creation time so it cannot dodge STALLED forever.
            started = getattr(run, "started_at", None) or getattr(run, "created_at", None)
            state = (
                CheckState.STALLED
                if started is not None and started < stalled_before
                else CheckState.PENDING
            )
            results.append(RequiredCheck(name=name, state=state, url=run.html_url or ""))
        elif run.conclusion == "success":
            results.append(RequiredCheck(name=name, state=CheckState.PASSED, url=run.html_url or ""))
        else:
            # failure, cancelled, timed_out, action_required, stale, neutral,
            # skipped: none of these is evidence the required check passed.
            results.append(RequiredCheck(name=name, state=CheckState.FAILED, url=run.html_url or ""))
    return tuple(results)


def _checks_workflow_suite_ids(repo: Any, policy: RepoReleasePolicy, sha: str) -> set[int]:
    """Check-suite ids of *sha*'s runs of the policy's ``checks_workflow``.

    An empty set (workflow never ran on this SHA) makes every required check
    MISSING — fail closed, mirroring the no-run case.
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
    either of which can trigger a per-run completion GET — same rationale as
    issue_dedup's ``_drop_pull_requests``.
    """
    suite = run._rawData.get("check_suite") or {}
    return suite.get("id")


def _run_order_key(run: Any) -> tuple[Any, int]:
    started = getattr(run, "started_at", None)
    return ((started is not None, started), run.id or 0)


def check_blockers(checks: tuple[RequiredCheck, ...]) -> list[str]:
    messages = {
        CheckState.FAILED: "Required check failed on the candidate SHA: `{}` (rerun on the same SHA to retry).",
        CheckState.PENDING: "Required check has not completed on the candidate SHA: `{}`.",
        CheckState.MISSING: "Required check has no run on the candidate SHA: `{}`.",
        CheckState.STALLED: (
            "Required check has been running past the policy timeout on the "
            "candidate SHA: `{}` (cancel and rerun it)."
        ),
    }
    return [
        messages[check.state].format(check.name)
        for check in checks
        if check.state is not CheckState.PASSED
    ]
