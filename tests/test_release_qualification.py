"""Tests for qualification dispatch and evidence evaluation (stage 3)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.release.models import QualificationStatus
from scripts.release.qualification import (
    MANIFEST_ARTIFACT,
    STARTUP_FAILURE_JOB,
    dispatch_qualification,
    evaluate_qualification,
)
from tests.release_fixtures import (
    MERGE_SHA,
    MOVED_SHA,
    build_manifest_payload,
    build_manifest_zip_bytes,
    gh_mock,
    make_policy,
    qualification_run,
)

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
        job = MagicMock(status="completed", conclusion="success")
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

    def test_incomplete_job_reads_as_pending_never_failed(self) -> None:
        # Observed live: GitHub reported run.conclusion success while two
        # test jobs were still in_progress. The verdict is not in yet:
        # pending, never a failure page.
        straggler = MagicMock(status="in_progress", conclusion="")
        straggler.name = "Qualify RPM/DEB packages / Test RPM · AL2023"
        done = MagicMock(status="completed", conclusion="success")
        done.name = "Qualify x86 archives / Build package ubuntu-22.04 x86_64"
        run = qualification_run(jobs=[straggler, done])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert status.pending
        assert not status.failed_jobs

    def test_failed_run_reports_failed_job_names(self) -> None:
        bad = MagicMock(status="completed", conclusion="failure")
        bad.name = "DEB · Debian 12 (arm64)"
        run = qualification_run(conclusion="failure", jobs=[bad])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "DEB · Debian 12 (arm64)" in status.failed_jobs

    def test_successful_run_with_hidden_failed_job_does_not_pass(self) -> None:
        # A run can conclude success while a job was cancelled if the
        # workflow mishandles it; job-level evidence is required.
        bad = MagicMock(status="completed", conclusion="cancelled")
        bad.name = "tarball-jammy-x86_64"
        run = qualification_run(jobs=[bad])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert "tarball-jammy-x86_64" in status.failed_jobs

    def test_truncated_matrix_fails_structural_evidence(self) -> None:
        # A run that succeeded with only a generate job (empty matrix) has
        # no archive or package evidence and must not pass.
        only = MagicMock(status="completed", conclusion="success")
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
        extra = MagicMock(status="completed", conclusion="success")
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
        duplicate = MagicMock(status="completed", conclusion="success")
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
    """The qualification-manifest artifact is required evidence: content is
    downloaded, parsed, and each field must match the release identity
    being qualified (F5). Presence plus unexpired is necessary but not
    sufficient."""

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
        only = MagicMock(status="completed", conclusion="success")
        only.name = "generate"
        run = qualification_run(jobs=[only])
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("Evidence mismatch" in item for item in status.failed_jobs)
        assert ("(Evidence mismatch: no qualification manifest)"
                not in status.failed_jobs)


class TestManifestRedirectFollowing:
    """GitHub answers artifact downloads with a 302 to signed blob storage.
    The live E2E caught the reader treating that redirect as an error and
    wedging every release at qualification (fail-closed, but wrongly)."""

    def test_302_redirect_is_followed_to_the_blob(self) -> None:
        from scripts.release import qualification as qual_mod
        blob = build_manifest_zip_bytes(build_manifest_payload())
        run = qualification_run()
        artifact = run.get_artifacts.return_value[0]
        artifact.requester.requestBlob.return_value = (
            302, {"Location": "https://blobs.example/signed"}, "")
        with patch.object(qual_mod, "_fetch_signed_url",
                          return_value=blob) as fetch:
            payload = qual_mod._load_manifest_payload(artifact)
        fetch.assert_called_once_with("https://blobs.example/signed")
        assert payload["schema"] == 1

    def test_redirect_without_location_refuses(self) -> None:
        from scripts.release import qualification as qual_mod
        run = qualification_run()
        artifact = run.get_artifacts.return_value[0]
        artifact.requester.requestBlob.return_value = (302, {}, "")
        with pytest.raises(qual_mod._ManifestReadError, match="no Location"):
            qual_mod._load_manifest_payload(artifact)

    def test_non_https_redirect_refuses(self) -> None:
        from scripts.release import qualification as qual_mod
        with pytest.raises(qual_mod._ManifestReadError, match="non-https"):
            qual_mod._fetch_signed_url("http://blobs.example/signed")


class TestManifestContentValidation:
    """F5: the manifest is downloaded, parsed, and every field bound to
    the release identity being qualified. Any download failure, parse
    failure, or field mismatch is an evidence mismatch that names what
    differed. The producer schema is fixed at 1
    (valkey-release-automation/qualify-release.yml)."""

    def _run_with_manifest(self, **payload_overrides: object):
        """A run whose manifest carries valid content minus the overrides."""
        from tests.release_fixtures import build_manifest_payload
        payload = build_manifest_payload()
        payload.update(payload_overrides)
        return qualification_run(manifest_payload=payload)

    def test_valid_content_manifest_passes(self) -> None:
        # Sanity: the fixture default satisfies every binding.
        status = evaluate_qualification(
            _gh_with_runs([qualification_run()]), _POLICY,
            tag="9.1.1", sha=MERGE_SHA,
        )
        assert status.passed
        assert not status.failed_jobs

    def test_wrong_source_sha_names_the_field(self) -> None:
        # An attacker-authored manifest for a different SHA must never
        # count as evidence, even if the run and jobs look correct.
        wrong_sha = "d" * 40
        run = self._run_with_manifest(source_sha=wrong_sha)
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("source_sha" in item for item in status.failed_jobs)

    def test_wrong_tag_names_the_field(self) -> None:
        # A manifest for a different tag on the same SHA (an rc-suffixed
        # dispatch that legitimately skipped packages) must not satisfy
        # this release's qualification.
        run = self._run_with_manifest(tag="9.1.2")
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("manifest tag" in item for item in status.failed_jobs)

    def test_wrong_version_names_the_field(self) -> None:
        # The manifest carries version separately from tag; both must bind.
        run = self._run_with_manifest(version="9.1.0")
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("manifest version" in item for item in status.failed_jobs)

    @pytest.mark.parametrize(("field", "wrong_value"), [
        ("rpm_jobs", 1),           # policy expects 2
        ("deb_jobs", 5),           # policy expects 1
        ("archive_jobs", 3),       # policy expects 4 (2 x86 + 2 ARM)
    ])
    def test_wrong_job_counts_are_named_individually(
        self, field: str, wrong_value: int,
    ) -> None:
        # Each miscount is a different failure and each one blocks the
        # release; the reviewed inventory (policy file) is authoritative.
        run = self._run_with_manifest(**{field: wrong_value})
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any(field in item for item in status.failed_jobs)

    def test_stringified_count_is_still_wrong(self) -> None:
        # A producer that emits "2" instead of 2 is a schema drift; the
        # exact-int discipline refuses coercion.
        run = self._run_with_manifest(rpm_jobs="2")
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("rpm_jobs" in item for item in status.failed_jobs)

    def test_wrong_schema_version_refuses(self) -> None:
        # Schema drift by a producer change: the controller cannot trust
        # the shape and must not read fields blindly.
        run = self._run_with_manifest(schema=2)
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("schema" in item for item in status.failed_jobs)

    def test_missing_required_field_refuses(self) -> None:
        from tests.release_fixtures import (
            build_manifest_payload,
            build_manifest_zip_bytes,
        )
        payload = build_manifest_payload()
        del payload["automation_sha"]  # dispatch identity binding lost
        run = qualification_run(
            manifest_zip=build_manifest_zip_bytes(payload),
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("automation_sha" in item for item in status.failed_jobs)

    def test_malformed_json_body_refuses(self) -> None:
        from tests.release_fixtures import build_manifest_zip_bytes
        run = qualification_run(
            manifest_zip=build_manifest_zip_bytes(
                json_body='{"schema": 1, "tag":'  # unterminated
            ),
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("malformed JSON" in item for item in status.failed_jobs)

    def test_top_level_json_list_refuses(self) -> None:
        # A producer emitting a list at the top level cannot be indexed by
        # field name; every field lookup would raise. Refuse cleanly.
        from tests.release_fixtures import build_manifest_zip_bytes
        run = qualification_run(
            manifest_zip=build_manifest_zip_bytes(json_body='[1, 2, 3]'),
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("JSON" in item or "object" in item
                   for item in status.failed_jobs)

    def test_empty_artifact_body_refuses(self) -> None:
        # An artifact with size_in_bytes > 0 but no bytes on the wire (a
        # broken upload, a stubbed body) must not pass silently as "no
        # gaps found".
        from tests.release_fixtures import _manifest_artifact_mock
        run = _without_manifest(qualification_run())
        empty_requester = MagicMock()
        empty_requester.requestBlob.return_value = (200, {}, b"")
        run.get_artifacts.return_value = (
            list(run.get_artifacts.return_value)
            + [_manifest_artifact_mock(requester=empty_requester)]
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("unreadable" in item or "no bytes" in item
                   for item in status.failed_jobs)

    def test_corrupt_zip_body_refuses(self) -> None:
        # Not a zip at all: an upstream mangle should never look like
        # a passable manifest.
        from tests.release_fixtures import _manifest_artifact_mock
        run = _without_manifest(qualification_run())
        bad_requester = MagicMock()
        bad_requester.requestBlob.return_value = (200, {}, b"not a zip file")
        run.get_artifacts.return_value = (
            list(run.get_artifacts.return_value)
            + [_manifest_artifact_mock(requester=bad_requester)]
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("corrupt" in item or "unreadable" in item
                   for item in status.failed_jobs)

    def test_download_http_error_refuses(self) -> None:
        # A non-2xx from the artifact-download endpoint (blob-storage error,
        # signed-URL expiry, rate limiting) is not evidence.
        from tests.release_fixtures import _manifest_artifact_mock
        run = _without_manifest(qualification_run())
        error_requester = MagicMock()
        error_requester.requestBlob.return_value = (503, {}, b"")
        run.get_artifacts.return_value = (
            list(run.get_artifacts.return_value)
            + [_manifest_artifact_mock(requester=error_requester)]
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("HTTP 503" in item or "unreadable" in item
                   for item in status.failed_jobs)

    def test_wrong_source_sha_via_direct_helper_names_the_hash_diff(self) -> None:
        # A more direct test of the helper's message shape: the SHA
        # comparison prints truncated hashes so the operator can diff the
        # values at a glance.
        wrong = "e" * 40
        run = self._run_with_manifest(source_sha=wrong)
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        # The message includes both the wrong prefix and the expected prefix
        assert any(wrong[:12] in item for item in status.failed_jobs)
        assert any(MERGE_SHA[:12] in item for item in status.failed_jobs)

    def test_multiple_json_files_in_zip_refuses(self) -> None:
        # A producer accidentally packing more than one JSON file: refuse
        # rather than guess which one is authoritative.
        import io
        import json as _json
        import zipfile as _zipfile

        from tests.release_fixtures import (
            _manifest_artifact_mock,
            build_manifest_payload,
        )
        buf = io.BytesIO()
        with _zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("manifest.json",
                        _json.dumps(build_manifest_payload()))
            zf.writestr("other.json", _json.dumps({"unrelated": True}))
        run = _without_manifest(qualification_run())
        multi_requester = MagicMock()
        multi_requester.requestBlob.return_value = (200, {}, buf.getvalue())
        run.get_artifacts.return_value = (
            list(run.get_artifacts.return_value)
            + [_manifest_artifact_mock(requester=multi_requester)]
        )
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.1.1", sha=MERGE_SHA)
        assert not status.passed
        assert any("JSON files" in item or "unreadable" in item
                   for item in status.failed_jobs)

    def test_rc_manifest_expects_zero_package_counts(self) -> None:
        # RC dispatch skips packages; the manifest reflects that with
        # rpm_jobs=0, deb_jobs=0. A leaked count of 1 must fail closed.
        from tests.release_fixtures import build_manifest_payload
        payload = build_manifest_payload(tag="9.2.0-rc1", rpm_jobs=1)
        run = qualification_run(tag="9.2.0-rc1", manifest_payload=payload,
                                jobs=_archive_jobs())
        status = evaluate_qualification(_gh_with_runs([run]), _POLICY,
                                        tag="9.2.0-rc1", sha=MERGE_SHA)
        assert not status.passed
        assert any("rpm_jobs" in item for item in status.failed_jobs)
