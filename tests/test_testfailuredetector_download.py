"""Tests for artifact download logic (mocked GitHub API)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

try:
    from scripts.test_failure_detector.download import (
        download_all_test_failures,
        get_latest_scheduled_run,
        job_urls_for_jobs,
    )

    _SKIP_REASON = None
except ImportError as _exc:
    _SKIP_REASON = f"Import failed: {_exc}"

pytestmark = pytest.mark.skipif(_SKIP_REASON is not None, reason=_SKIP_REASON or "")


def _make_mock_run(run_number: int, run_id: int, conclusion: str, status: str = "completed"):
    run = MagicMock()
    run.run_number = run_number
    run.id = run_id
    run.conclusion = conclusion
    run.status = status
    run.created_at = "2026-06-01 00:00:00+00:00"
    return run


class TestGetLatestDailyRun:
    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_skips_cancelled_runs(self, mock_retry) -> None:
        """Cancelled runs should be skipped."""
        cancelled_run = _make_mock_run(10, 100, "cancelled")
        success_run = _make_mock_run(9, 99, "success")

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = [cancelled_run, success_run]

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result == success_run

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_skips_skipped_runs(self, mock_retry) -> None:
        """Skipped runs should be skipped."""
        skipped_run = _make_mock_run(13, 200, "skipped")
        failure_run = _make_mock_run(12, 199, "failure")

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = [skipped_run, failure_run]

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result == failure_run

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_returns_first_success_or_failure(self, mock_retry) -> None:
        """Should return the most recent run with conclusion success or failure."""
        runs = [
            _make_mock_run(15, 300, "skipped"),
            _make_mock_run(14, 299, "cancelled"),
            _make_mock_run(13, 298, "success"),
            _make_mock_run(12, 297, "failure"),
        ]

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = runs

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result.id == 298
        assert result.conclusion == "success"

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_returns_none_when_no_qualifying_run(self, mock_retry) -> None:
        """Should return None if all runs are cancelled/skipped."""
        runs = [
            _make_mock_run(10, 100, "cancelled"),
            _make_mock_run(9, 99, "skipped"),
        ]

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = runs

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result is None

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_returns_none_when_workflow_not_found(self, mock_retry) -> None:
        """Should return None if the workflow doesn't exist."""
        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = []

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result is None

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_filters_to_scheduled_runs_only(self, mock_retry) -> None:
        """Run selection must restrict to event='schedule'.

        The Daily workflow also runs on pull_request, and those PR runs are
        more recent than the nightly. Without the event filter the detector
        picks a PR run (which has no test artifacts) instead of the nightly.
        """
        scheduled_run = _make_mock_run(12, 199, "failure")

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = [scheduled_run]

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result == scheduled_run
        mock_workflow.get_runs.assert_called_once_with(
            branch="unstable", status="completed", event="schedule",
        )

    @patch("scripts.test_failure_detector.download.retry_github_call")
    def test_skips_action_required_runs(self, mock_retry) -> None:
        """Runs awaiting approval (e.g. fork PRs) never executed and have no
        artifacts, so they must be skipped rather than treated as a pass."""
        pending_run = _make_mock_run(14, 299, "action_required")
        failure_run = _make_mock_run(12, 297, "failure")

        mock_workflow = MagicMock()
        mock_workflow.path = ".github/workflows/daily.yml"
        mock_workflow.get_runs.return_value = [pending_run, failure_run]

        mock_repo = MagicMock()
        mock_repo.get_workflows.return_value = [mock_workflow]

        mock_retry.side_effect = lambda op, **kwargs: op()

        mock_gh = MagicMock()
        mock_gh.get_repo.return_value = mock_repo

        result = get_latest_scheduled_run(mock_gh, "owner/repo")
        assert result == failure_run


class TestDownloadAllTestFailures:
    """Download now delegates to a (mocked) ArtifactClient."""

    @staticmethod
    def _make_artifact(name: str, artifact_id: int = 555, expired: bool = False):
        from scripts.common.workflow_artifacts import WorkflowArtifact

        return WorkflowArtifact(
            artifact_id=artifact_id, name=name, size_in_bytes=10, expired=expired,
        )

    def test_downloads_and_extracts_json(self) -> None:
        """Should locate the artifact and return the extracted JSON content."""
        failures_data = {"job-1": {"suite": [{"test_name": "t", "test_file": "f.tcl", "error": "e"}]}}

        client = MagicMock()
        client.list_run_artifacts.return_value = [self._make_artifact("all-test-failures")]
        client.download_artifact.return_value = {
            "all-test-failures.json": json.dumps(failures_data).encode(),
        }

        result = download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
        )
        assert result is not None
        assert json.loads(result) == failures_data
        client.download_artifact.assert_called_once_with(
            "owner/repo", 555, damaged=None,
        )

    def test_returns_none_when_no_artifact(self) -> None:
        """Should return None if no all-test-failures artifact exists."""
        client = MagicMock()
        client.list_run_artifacts.return_value = [self._make_artifact("some-other-artifact")]

        result = download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
        )
        assert result is None
        client.download_artifact.assert_not_called()

    def test_reports_why_an_existing_artifact_is_unusable(self) -> None:
        client = MagicMock()
        client.list_run_artifacts.return_value = [
            self._make_artifact("all-test-failures", expired=True)
        ]
        unusable: list[str] = []
        download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
            unusable=unusable,
        )
        assert len(unusable) == 1 and "expired" in unusable[0]

        client.list_run_artifacts.return_value = [self._make_artifact("all-test-failures")]
        client.download_artifact.return_value = {"other.txt": b""}
        unusable = []
        download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
            unusable=unusable,
        )
        assert len(unusable) == 1 and "does not contain" in unusable[0]

        client.list_run_artifacts.return_value = []
        unusable = []
        download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
            unusable=unusable,
        )
        assert unusable == []

    def test_returns_none_when_no_artifacts_at_all(self) -> None:
        """Should return None if the run has no artifacts."""
        client = MagicMock()
        client.list_run_artifacts.return_value = []

        result = download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
        )
        assert result is None

    def test_returns_none_when_artifact_expired(self) -> None:
        """Should return None (without downloading) if the artifact is expired."""
        client = MagicMock()
        client.list_run_artifacts.return_value = [
            self._make_artifact("all-test-failures", expired=True)
        ]

        result = download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
        )
        assert result is None
        client.download_artifact.assert_not_called()

    def test_returns_none_when_json_missing_from_zip(self) -> None:
        """Should return None if the zip lacks the expected JSON file."""
        client = MagicMock()
        client.list_run_artifacts.return_value = [self._make_artifact("all-test-failures")]
        client.download_artifact.return_value = {"something-else.txt": b"nope"}

        result = download_all_test_failures(
            MagicMock(), "owner/repo", 123, "fake-token", artifact_client=client,
        )
        assert result is None


class TestJobUrlsForJobs:
    @staticmethod
    def _job(name: str, url: str) -> MagicMock:
        job = MagicMock()
        job.name = name
        job.html_url = url
        return job

    def test_maps_exact_and_normalized_names(self) -> None:
        job = self._job("test ubuntu (arm64)", "https://example.com/job/1")

        assert job_urls_for_jobs((job,), run_id=123) == {
            "test ubuntu (arm64)": "https://example.com/job/1",
            "test-ubuntu-arm64": "https://example.com/job/1",
        }

    def test_alias_does_not_overwrite_exact_job_name(self) -> None:
        """A normalized alias of one job must not clobber another job's exact
        name mapping (which would attach the wrong CI URL)."""
        # job1's normalized form is "test-ubuntu-arm64", which is exactly job2's name.
        job1 = self._job("test ubuntu (arm64)", "https://example.com/job/1")
        job2 = self._job("test-ubuntu-arm64", "https://example.com/job/2")

        result = job_urls_for_jobs((job1, job2), run_id=1)

        assert result["test-ubuntu-arm64"] == job2.html_url
        assert result["test ubuntu (arm64)"] == job1.html_url

    def test_empty_jobs_returns_empty_dict(self) -> None:
        assert job_urls_for_jobs((), run_id=1) == {}


class TestScheduledRunGuards:
    @staticmethod
    def _gh(*runs):
        workflow = MagicMock()
        workflow.path = ".github/workflows/weekly.yml"
        workflow.get_runs.return_value = list(runs)
        gh = MagicMock()
        gh.get_repo.return_value.get_workflows.return_value = [workflow]
        return gh

    @patch("scripts.test_failure_detector.download.retry_github_call", side_effect=lambda op, **_: op())
    def test_a_run_that_never_started_is_skipped(self, _retry) -> None:
        from datetime import datetime, timezone

        never = _make_mock_run(2, 20, "startup_failure")
        ran = _make_mock_run(1, 10, "failure")
        ran.created_at = datetime.now(timezone.utc)

        assert get_latest_scheduled_run(self._gh(never, ran), "o/r", "weekly.yml") is ran

    @patch("scripts.test_failure_detector.download.retry_github_call", side_effect=lambda op, **_: op())
    def test_last_weeks_run_is_not_read_again(self, _retry) -> None:
        from datetime import datetime, timedelta, timezone

        old = _make_mock_run(1, 10, "failure")
        old.created_at = datetime.now(timezone.utc) - timedelta(days=8)

        assert get_latest_scheduled_run(self._gh(old), "o/r", "weekly.yml", max_age=timedelta(days=7)) is None
