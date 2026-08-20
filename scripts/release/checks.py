"""Fail-closed required-check validation for one exact candidate commit."""

from __future__ import annotations

from typing import Any

from scripts.common.github_client import retry_github_call
from scripts.release.models import ReleasePolicy


def require_green_checks(repo: Any, policy: ReleasePolicy, sha: str) -> None:
    """Require every named check to pass in the configured workflow on *sha*.

    Check names are not globally unique.  We first select check-suite ids from
    runs of the configured workflow and then ignore same-named checks emitted
    by every other workflow.
    """
    workflow_runs = retry_github_call(
        lambda: list(repo.get_workflow_runs(head_sha=sha)),
        retries=2,
        description=f"list workflow runs on {sha[:12]}",
    )
    matching_runs = [run for run in workflow_runs if (run.path or "").rsplit("/", 1)[-1] == policy.checks_workflow]
    if not matching_runs:
        raise ValueError(f"no {policy.checks_workflow} run exists on candidate {sha[:12]}")
    latest_workflow = max(matching_runs, key=_workflow_order)
    if latest_workflow.status != "completed":
        raise ValueError(
            f"latest {policy.checks_workflow} run on {sha[:12]} is {latest_workflow.status}; wait for it to complete"
        )
    suite_id = latest_workflow.check_suite_id
    if not suite_id:
        raise ValueError(f"latest {policy.checks_workflow} run on {sha[:12]} has no check suite")
    commit = retry_github_call(
        lambda: repo.get_commit(sha),
        retries=2,
        description=f"get candidate {sha[:12]}",
    )
    check_runs = retry_github_call(
        lambda: list(commit.get_check_runs()),
        retries=2,
        description=f"list checks on {sha[:12]}",
    )

    latest: dict[str, Any] = {}
    for run in check_runs:
        suite = (getattr(run, "_rawData", {}) or {}).get("check_suite") or {}
        if suite.get("id") != suite_id:
            continue
        current = latest.get(run.name)
        if current is None or _order(run) > _order(current):
            latest[run.name] = run

    blockers: list[str] = []
    for name in policy.required_checks:
        run = latest.get(name)
        if run is None:
            blockers.append(f"{name} (missing)")
        elif run.status != "completed":
            blockers.append(f"{name} ({run.status})")
        elif run.conclusion != "success":
            blockers.append(f"{name} ({run.conclusion or 'no conclusion'})")
    if blockers:
        raise ValueError(f"required candidate CI is not green on {sha[:12]}: {', '.join(blockers)}")


def _order(run: Any) -> tuple[bool, Any, int]:
    started = getattr(run, "started_at", None)
    return started is not None, started, getattr(run, "id", 0) or 0


def _workflow_order(run: Any) -> tuple[float, int]:
    created = getattr(run, "created_at", None)
    timestamp = created.timestamp() if created is not None else 0.0
    return timestamp, getattr(run, "id", 0) or 0
