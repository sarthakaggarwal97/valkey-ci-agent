"""GitHub Actions run mechanics, isolated from the verifier logic.

Code (not the AI) determines which jobs actually failed in a run, so the
verifier layer can require the AI's hinted job to be a real failure and
classify that exact job. When a front door has to choose one failure among
several, ``select_job`` picks the one most likely to have a deterministic fix.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from scripts.ci_fix.verify.base import FailedJob
from scripts.common.github_client import retry_github_call

logger = logging.getLogger(__name__)

# A cancelled job is usually a fail-fast skip after another job failed, not a
# real failure; fixing it would be wrong. Only genuine failures are targets.
FAILED_JOB_CONCLUSIONS = frozenset({"failure", "timed_out"})


def failed_jobs_for_run(gh: Any, repo_full_name: str, run_id: int, *, retries: int = 2) -> list[FailedJob]:
    """Return the jobs that did not succeed in ``run_id``.

    A read failure yields an empty list, which the caller treats as "cannot
    confirm the failed job" and refuses.
    """
    def _fetch() -> list[Any]:
        run = gh.get_repo(repo_full_name).get_workflow_run(run_id)
        return list(run.jobs())

    try:
        jobs = retry_github_call(_fetch, retries=retries, description=f"list jobs for run {run_id}")
    except Exception as exc:  # noqa: BLE001 - fail closed
        logger.warning("Could not list jobs for run %s: %s", run_id, exc)
        return []
    return [
        FailedJob(name=str(getattr(j, "name", "") or ""),
                  conclusion=str(getattr(j, "conclusion", "") or ""),
                  id=int(getattr(j, "id", 0) or 0))
        for j in jobs
        if str(getattr(j, "conclusion", "") or "") in FAILED_JOB_CONCLUSIONS
    ]


def job_priority(name: str) -> int:
    """Rank a failed job by how deterministic its failure usually is.

    A formatting or build break has one obvious fix, a test failure needs
    diagnosis, and a sanitizer or valgrind failure is the most likely to be
    flaky or to need a judgement call. Unrecognised names sort last.
    """
    lowered = name.lower()
    if any(
        token in lowered
        # Not "schema": Daily's reply-schemas-validator runs the whole test
        # suite; the reply-schemas-linter is already matched by "lint".
        for token in ("format", "lint", "generated", "build", "compile")
    ):
        return 0
    if any(
        token in lowered
        for token in ("asan", "ubsan", "tsan", "sanitizer", "valgrind")
    ):
        return 2
    if any(token in lowered for token in ("unit", "integration", "test")):
        return 1
    return 3


def select_job(jobs: Iterable[FailedJob]) -> FailedJob:
    """Choose one failure: the most deterministic tier, then the lowest job id."""
    return min(
        jobs,
        key=lambda job: (job_priority(job.name), int(job.id), job.name.lower()),
    )
