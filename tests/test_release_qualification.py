"""Tests for qualification dispatch and evidence evaluation (stage 3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.release.qualification import dispatch_qualification, evaluate_qualification
from tests.release_fixtures import MERGE_SHA, MOVED_SHA, gh_mock, make_policy, qualification_run

_POLICY = make_policy()


def _gh_with_runs(runs: "list[MagicMock]") -> MagicMock:
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_workflow.return_value.get_runs.return_value = runs
    return gh_mock(repo)


def _archive_jobs() -> "list[MagicMock]":
    """The four green archive-build legs (x86 + ARM), no package matrix."""
    jobs = []
    for name in ("Qualify x86 archives / Build package ubuntu-22.04 x86_64",
                 "Qualify x86 archives / Build package ubuntu-24.04 x86_64",
                 "Qualify ARM archives / Build package ubuntu-22.04-arm arm64",
                 "Qualify ARM archives / Build package ubuntu-24.04-arm arm64"):
        job = MagicMock(conclusion="success")
        job.name = name
        jobs.append(job)
    return jobs


class TestEvaluate:
    def test_no_run_for_sha_is_empty_status(self) -> None:
        status = evaluate_qualification(_gh_with_runs([]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.run_id == 0 and not status.passed and not status.pending

    def test_run_matched_by_exact_sha_in_run_name(self) -> None:
        status = evaluate_qualification(
            _gh_with_runs([qualification_run(sha=MOVED_SHA), qualification_run()]),
            _POLICY, tag="9.1.1", sha=MERGE_SHA,
        )
        assert status.run_id == 900 and status.passed

    def test_successful_run_with_enough_jobs_passes(self) -> None:
        status = evaluate_qualification(_gh_with_runs([qualification_run()]),
                                        _POLICY, tag="9.1.1", sha=MERGE_SHA)
        assert status.passed and not status.failed_jobs

    def test_in_progress_run_is_pending(self) -> None:
        run = qualification_run(status="in_progress", conclusion=None)
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.pending and not status.passed

    def test_failed_run_reports_failed_job_names(self) -> None:
        bad = MagicMock(conclusion="failure")
        bad.name = "DEB · Debian 12 (arm64)"
        run = qualification_run(conclusion="failure", jobs=[bad])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "DEB · Debian 12 (arm64)" in status.failed_jobs

    def test_successful_run_with_hidden_failed_job_does_not_pass(self) -> None:
        # A run can conclude success while a job was cancelled if the
        # workflow mishandles it; job-level evidence is required.
        bad = MagicMock(conclusion="cancelled")
        bad.name = "tarball-jammy-x86_64"
        run = qualification_run(jobs=[bad])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "tarball-jammy-x86_64" in status.failed_jobs

    def test_truncated_matrix_fails_structural_evidence(self) -> None:
        # A run that succeeded with only a generate job (empty matrix) has
        # no archive or package evidence and must not pass.
        only = MagicMock(conclusion="success")
        only.name = "generate"
        run = qualification_run(jobs=[only])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("Evidence mismatch" in item for item in status.failed_jobs)

    def test_ga_with_skipped_package_matrix_does_not_pass(self) -> None:
        # The exact reviewed hole: archives green, packages absent, GA tag.
        run = qualification_run(jobs=_archive_jobs())
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("RPM package builds" in item for item in status.failed_jobs)

    def test_rc_passes_without_the_package_matrix(self) -> None:
        run = qualification_run(tag="9.2.0-rc1", jobs=_archive_jobs())
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.2.0-rc1", sha=MERGE_SHA)
        assert status.passed

    def test_extra_package_legs_also_fail_the_exact_inventory(self) -> None:
        # Exact counts cut both ways: a platform added without the reviewed
        # policy bump is flagged, keeping the inventory deliberate.
        run = qualification_run()
        extra = MagicMock(conclusion="success")
        extra.name = "Qualify RPM/DEB packages / RPM · Surprise Linux (x86_64) · v9"
        run.jobs.return_value = list(run.jobs.return_value) + [extra]
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed

    def test_expired_artifacts_are_not_evidence(self) -> None:
        run = qualification_run()
        for artifact in run.get_artifacts.return_value:
            artifact.expired = True
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("usable" in item for item in status.failed_jobs)


class TestDispatch:
    def test_dispatch_targets_the_qualification_workflow_with_exact_sha(self) -> None:
        repo = MagicMock()
        repo.default_branch = "main"
        repo.get_workflow.return_value.create_dispatch.return_value = True
        gh = gh_mock(repo)

        dispatch_qualification(gh, _POLICY, tag="9.1.1", sha=MERGE_SHA)

        gh.get_repo.assert_called_with("valkey-io/valkey-release-automation")
        repo.get_workflow.assert_called_with("qualify-release.yml")
        repo.get_workflow.return_value.create_dispatch.assert_called_once_with(
            "main", inputs={"version": "9.1.1", "source_sha": MERGE_SHA},
        )

    def test_rejected_dispatch_raises(self) -> None:
        repo = MagicMock()
        repo.default_branch = "main"
        repo.get_workflow.return_value.create_dispatch.return_value = False
        with pytest.raises(RuntimeError, match="rejected"):
            dispatch_qualification(gh_mock(repo), _POLICY,
                                   tag="9.1.1", sha=MERGE_SHA)


def test_startup_failed_run_is_no_evidence_and_allows_redispatch() -> None:
    # A startup_failure never planned jobs: nothing about the candidate was
    # tested, so it must read as "no run" (reconcile then redispatches),
    # unlike a real build failure which demands a human.
    broken = qualification_run(conclusion="startup_failure")
    status = evaluate_qualification(_gh_with_runs([broken]), _POLICY,
                                    tag="9.1.1", sha=MERGE_SHA)
    assert status == type(status)()  # pristine: run_id 0, not failed
