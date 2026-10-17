"""Reads of one GitHub Actions run's jobs, shared by every bot that inspects a run.

A run can be re-run while a bot reads it, so a bot fetches one exact-attempt
snapshot of its jobs and derives everything (job URLs, failed jobs, their
logs) from that one snapshot.
"""

from __future__ import annotations

from fnmatch import fnmatch
from typing import Any, Iterable

from scripts.common.github_client import retry_github_call

# A cancelled job is usually a fail-fast skip after another job failed, not a
# real failure. Only these conclusions are genuine job failures.
FAILED_JOB_CONCLUSIONS = frozenset({"failure", "timed_out"})


def jobs_for_run_attempt(
    gh: Any,
    repo_full_name: str,
    run_id: int,
    *,
    run_attempt: int,
    retries: int = 2,
) -> tuple[Any, ...]:
    """Return the raw jobs snapshot from ``run_id``'s current attempt, ``run_attempt``.

    Every caller acts on what it reads, so a newer rerun stops it. All
    attempts are requested and every returned job must identify a valid
    attempt, so the snapshot never mixes URLs, steps, or conclusions between
    them. The immutable tuple is intended to be fetched once and shared by
    all consumers that inspect this occurrence.
    """
    if (
        isinstance(run_attempt, bool)
        or not isinstance(run_attempt, int)
        or run_attempt < 1
    ):
        raise ValueError(f"run attempt must be positive, got {run_attempt}")

    def _fetch() -> tuple[int, tuple[Any, ...]]:
        run = gh.get_repo(repo_full_name).get_workflow_run(run_id)
        observed_attempt = _attempt_number(
            getattr(run, "run_attempt", run_attempt),
            description=f"run {run_id}",
        )
        if observed_attempt != run_attempt:
            raise ValueError(
                f"run {run_id} is currently attempt {observed_attempt}, "
                f"not expected attempt {run_attempt}"
            )
        return observed_attempt, tuple(run.jobs(_filter="all"))

    observed_attempt, jobs = retry_github_call(
        _fetch,
        retries=retries,
        description=f"list jobs for run {run_id}",
    )
    selected: list[Any] = []
    for job in jobs:
        job_id = int(getattr(job, "id", 0) or 0)
        job_attempt = _attempt_number(
            getattr(job, "run_attempt", None),
            description=f"job {job_id} of run {run_id}",
        )
        if job_attempt > observed_attempt:
            raise ValueError(
                f"job {job_id} belongs to run {run_id} attempt {job_attempt}, "
                f"but the run has only reached attempt {observed_attempt}"
            )
        if job_attempt == run_attempt:
            selected.append(job)
    return tuple(selected)


def _attempt_number(value: Any, *, description: str) -> int:
    """Return a positive attempt number, rejecting ambiguous API values."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{description} does not identify a valid run attempt")
    return value


def job_is_ignored(name: str, patterns: Iterable[str]) -> bool:
    """Return whether ``name`` matches a case-insensitive ignore glob."""
    return any(fnmatch(name.casefold(), pattern.casefold()) for pattern in patterns)
