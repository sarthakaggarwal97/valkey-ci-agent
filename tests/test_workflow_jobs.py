"""Tests for the one exact-attempt jobs snapshot every bot reads a run through."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.common.workflow_jobs import job_is_ignored, jobs_for_run_attempt


def _gh_with_jobs(jobs, *, run_attempt=2):
    run = SimpleNamespace(
        jobs=lambda *, _filter: jobs,
        run_attempt=run_attempt,
    )
    repo = MagicMock()
    repo.get_workflow_run.return_value = run
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def test_raw_jobs_snapshot_preserves_job_objects():
    job = SimpleNamespace(
        id=44,
        name="build",
        conclusion="failure",
        run_attempt=2,
        html_url="https://example.test/job/44",
    )

    snapshot = jobs_for_run_attempt(
        _gh_with_jobs([job]),
        "o/r",
        1,
        run_attempt=2,
    )

    assert snapshot == (job,)
    assert snapshot[0] is job

def test_future_attempt_is_rejected_even_when_it_has_no_jobs():
    gh = _gh_with_jobs([], run_attempt=2)

    with pytest.raises(ValueError, match="currently attempt 2"):
        jobs_for_run_attempt(gh, "o/r", 1, run_attempt=3)

@pytest.mark.parametrize("run_attempt", [True, "2", 0, -1])
def test_invalid_requested_attempt_is_rejected(run_attempt):
    with pytest.raises(ValueError, match="run attempt must be positive"):
        jobs_for_run_attempt(
            _gh_with_jobs([]),
            "o/r",
            1,
            run_attempt=run_attempt,
        )

def test_ignored_job_globs_are_shared_and_case_insensitive():
    assert job_is_ignored("Notify-About-Job-Results", ("notify-*",))
    assert not job_is_ignored("build-linux", ("notify-*",))
