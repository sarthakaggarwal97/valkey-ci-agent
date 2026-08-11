"""Tests for qualification dispatch and evidence evaluation (stage 3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.release.models import QualificationStatus
from scripts.release.qualification import (
    MANIFEST_ARTIFACT,
    STARTUP_FAILURE_JOB,
    dispatch_qualification,
    evaluate_qualification,
)
from tests.release_fixtures import MERGE_SHA, MOVED_SHA, gh_mock, make_policy, qualification_run

_POLICY = make_policy()


def _gh_with_runs(runs: "list[MagicMock]") -> MagicMock:
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_workflow.return_value.get_runs.return_value = runs
    return gh_mock(repo)


def _manifest_artifact(*, expired: bool = False,
                       size_in_bytes: int = 512) -> MagicMock:
    artifact = MagicMock(expired=expired, size_in_bytes=size_in_bytes)
    artifact.name = MANIFEST_ARTIFACT
    return artifact


def _without_manifest(run: MagicMock) -> MagicMock:
    """The shared fixture carries the manifest artifact; a run modeling a
    legacy (pre-manifest) qualification needs it stripped."""
    run.get_artifacts.return_value = [
        artifact for artifact in run.get_artifacts.return_value
        if artifact.name != MANIFEST_ARTIFACT
    ]
    return run


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
            _gh_with_runs([qualification_run(sha=MOVED_SHA),
                           qualification_run()]),
            _POLICY, tag="9.1.1", sha=MERGE_SHA,
        )
        assert status.run_id == 900 and status.passed
        assert not status.failed_jobs

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

    @pytest.mark.parametrize("mutate", ["expired", "empty"])
    def test_expired_or_empty_artifacts_are_not_evidence(self, mutate: str) -> None:
        # An artifact with size 0 but expired False is still a name, not
        # evidence, exactly like an expired one.
        run = qualification_run()
        for artifact in run.get_artifacts.return_value:
            if mutate == "expired":
                artifact.expired = True
            else:
                artifact.size_in_bytes = 0
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("usable" in item for item in status.failed_jobs)

    def test_all_jobs_skipped_with_the_right_names_does_not_pass(self) -> None:
        # A run whose every job skipped concludes success on GitHub and
        # shows exactly the right job names; zero jobs SUCCEEDED, so the
        # exact-count evidence must fail it.
        run = qualification_run()
        for job in run.jobs.return_value:
            job.conclusion = "skipped"
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("Evidence mismatch" in item for item in status.failed_jobs)

    def test_duplicate_job_names_do_not_inflate_the_inventory(self) -> None:
        jobs = _archive_jobs()
        dropped = jobs.pop(1)  # lose the second x86 platform entirely
        assert "x86" in dropped.name
        duplicate = MagicMock(conclusion="success")
        duplicate.name = jobs[0].name  # same platform, listed twice
        run = qualification_run(tag="9.2.0-rc1", jobs=jobs + [duplicate])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.2.0-rc1", sha=MERGE_SHA)
        assert not status.passed

    @pytest.mark.parametrize("hostile_title", [
        None,                                    # GitHub can serve a null title
        f"Qualify 9.1.10 @ {MERGE_SHA}",         # superset tag
        f"Qualify 9.1.1-rc1 @ {MERGE_SHA}",      # rc of the same version
        f"Qualify 9.1.1 @ {MERGE_SHA[:12]}",     # truncated sha
        f"Qualify 9.1.1 @ {MOVED_SHA}",          # right tag, wrong sha
    ])
    def test_neighbor_run_titles_are_never_evidence(self, hostile_title) -> None:
        run = qualification_run()
        run.display_title = hostile_title
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.run_id == 0 and not status.passed and not status.pending

    def test_run_from_a_non_default_branch_is_never_evidence(self) -> None:
        # The binding this proves: a doctored qualify workflow on a side
        # branch, run-name and all, must not manufacture evidence. If the
        # head_branch check regressed, this run would pass.
        run = qualification_run(head_branch="attacker/qualify-fork")
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.run_id == 0 and not status.passed and not status.pending

    def test_missing_qualification_workflow_reads_as_no_evidence(self) -> None:
        from github.GithubException import GithubException
        repo = MagicMock()
        repo.default_branch = "main"
        repo.get_workflow.side_effect = GithubException(404, "missing", {})
        status = evaluate_qualification(gh_mock(repo), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status == type(status)()


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


class TestStartupFailure:
    """F14: a startup_failure run must not be erased into the no-run state
    (that made reconciliation redispatch every pass, forever). Its identity
    is preserved as a failed run, which actions.advance routes through the
    marker-gated one-retry path."""

    def test_startup_failure_reads_as_a_failed_run_not_no_run(self) -> None:
        broken = qualification_run(conclusion="startup_failure")
        status = evaluate_qualification(_gh_with_runs([broken]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status != QualificationStatus()  # never mistaken for no-run
        assert status.run_id == 900
        assert status.url == "https://x/qruns/900"
        assert not status.pending
        assert not status.passed
        assert status.failed_jobs == (STARTUP_FAILURE_JOB,)

    def test_startup_failure_never_lists_jobs_or_artifacts(self) -> None:
        # No job was ever planned; querying them would be wasted calls and
        # could raise on a half-created run.
        broken = qualification_run(conclusion="startup_failure")
        evaluate_qualification(_gh_with_runs([broken]), _POLICY,
                               tag="9.1.1", sha=MERGE_SHA)
        broken.jobs.assert_not_called()
        broken.get_artifacts.assert_not_called()


class TestManifestEvidence:
    """The qualification-manifest artifact is required evidence: presence
    plus unexpired, metadata-only (content is never fetched this round)."""

    def test_manifest_present_passes(self) -> None:
        run = qualification_run()
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.passed
        assert not status.failed_jobs

    def test_manifest_absent_fails_with_the_gap_text(self) -> None:
        # A legacy run (green, full matrix, no manifest) fails closed.
        run = _without_manifest(qualification_run())
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "(Evidence mismatch: no qualification manifest)" in status.failed_jobs

    @pytest.mark.parametrize(("expired", "size"), [
        pytest.param(True, 512, id="expired"),
        pytest.param(False, 0, id="empty"),
    ])
    def test_expired_or_empty_manifest_is_a_name_not_evidence(
        self, expired: bool, size: int,
    ) -> None:
        run = _without_manifest(qualification_run())
        run.get_artifacts.return_value = (
            list(run.get_artifacts.return_value)
            + [_manifest_artifact(expired=expired, size_in_bytes=size)]
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "(Evidence mismatch: no qualification manifest)" in status.failed_jobs

    def test_manifest_alone_does_not_satisfy_the_other_evidence(self) -> None:
        # The manifest is one more requirement, never a substitute for the
        # job and artifact inventory.
        only = MagicMock(conclusion="success")
        only.name = "generate"
        run = qualification_run(jobs=[only])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("Evidence mismatch" in item for item in status.failed_jobs)
        assert ("(Evidence mismatch: no qualification manifest)"
                not in status.failed_jobs)
