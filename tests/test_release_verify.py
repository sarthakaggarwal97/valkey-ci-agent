"""Tests for downstream public-output verification (stages 5 and 6).

Public endpoints are patched: these tests pin the decision logic (states,
ordering gates, applicability) against controlled registry/download answers.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from scripts.release import verify
from scripts.release.models import DownstreamOutput, OutputState
from tests.release_fixtures import MERGE_SHA, gh_mock, make_policy

_POLICY = make_policy()


def _content_file(text: str) -> MagicMock:
    f = MagicMock()
    f.decoded_content = text.encode("utf-8")
    return f


def _pr(number: int = 5, *, merged: bool = True, state: str = "closed") -> MagicMock:
    return MagicMock(number=number, merged_at="2026-08-07T00:00:00Z" if merged else None,
                     state="closed" if merged else state,
                     html_url=f"https://x/pull/{number}")


def _repo_serving(contents: "dict[str, str]", *,
                  pulls: "list[MagicMock] | None" = None,
                  tags: "set[str] | None" = None) -> MagicMock:
    repo = MagicMock()
    repo.get_contents.side_effect = lambda path, **kw: _content_file(contents[path])
    repo.get_pulls.return_value = pulls or []

    from github.GithubException import GithubException

    def _get_ref(ref: str) -> MagicMock:
        if ref.split("/", 1)[1] in (tags or set()):
            return MagicMock()
        raise GithubException(404, "missing", {})

    repo.get_git_ref.side_effect = _get_ref
    return repo


class TestTarballs:
    def test_all_public_verifies(self) -> None:
        with patch.object(verify.pub, "url_exists", return_value=True):
            output = verify._verify_tarballs(_POLICY.downstream, "9.1.1")
        assert output.state is OutputState.VERIFIED

    def test_missing_sha256_is_pending_and_named(self) -> None:
        def _exists(url: str, **kw: object) -> bool:
            return not url.endswith(".sha256")
        with patch.object(verify.pub, "url_exists", side_effect=_exists):
            output = verify._verify_tarballs(_POLICY.downstream, "9.1.1")
        assert output.state is OutputState.PENDING
        assert "sha256" in output.detail


class TestHashes:
    @pytest.mark.parametrize("tag", ["9.1.1", "9.2.0-rc1"])
    def test_recorded_hash_line_verifies(self, tag: str) -> None:
        repo = _repo_serving({"README": f"hash valkey-{tag}.tar.gz sha256 abc url"})
        output = verify._verify_hashes(gh_mock(repo), _POLICY.downstream, tag)
        assert output.state is OutputState.VERIFIED

    def test_absent_hash_line_is_pending(self) -> None:
        repo = _repo_serving({"README": "hash valkey-9.1.0.tar.gz sha256 abc url"})
        output = verify._verify_hashes(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert output.state is OutputState.PENDING


class TestContainer:
    def test_merged_pr_and_public_tags_verify(self) -> None:
        repo = MagicMock()
        repo.get_pulls.return_value = [_pr()]
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            pr_out, images = verify._verify_container(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert pr_out.state is OutputState.VERIFIED
        assert images.state is OutputState.VERIFIED

    def test_missing_variant_tag_blocks_images(self) -> None:
        repo = MagicMock()
        repo.get_pulls.return_value = [_pr()]
        with patch.object(verify.pub, "dockerhub_tag_exists",
                          side_effect=lambda r, t: not t.endswith("-alpine")), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            _pr_out, images = verify._verify_container(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert images.state is OutputState.PENDING
        assert "9.1.1-alpine" in images.detail

    def test_closed_unmerged_pr_is_failed(self) -> None:
        repo = MagicMock()
        repo.get_pulls.return_value = [_pr(merged=False, state="closed")]
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=False), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=False), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=False):
            pr_out, _images = verify._verify_container(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert pr_out.state is OutputState.FAILED


class TestDocsAndWebsite:
    def test_rc_skips_docs_and_website(self) -> None:
        docs = verify._verify_docs(MagicMock(), _POLICY.downstream, "9.1.0", "rc1")
        site = verify._verify_website(MagicMock(), _POLICY.downstream, "9.1.0", "rc1")
        assert docs.state is OutputState.SKIPPED
        assert site.state is OutputState.SKIPPED

    def test_patch_release_verifies_by_docs_tag(self) -> None:
        repo = _repo_serving({}, tags={"9.1.1"})
        output = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga")
        assert output.state is OutputState.VERIFIED

    def test_minor_release_verifies_by_docs_pr(self) -> None:
        repo = _repo_serving({}, pulls=[_pr()])
        output = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        assert output.state is OutputState.VERIFIED


class TestBundle:
    def test_old_line_is_skipped(self) -> None:
        output = verify._verify_bundle(MagicMock(), _POLICY.downstream, "8.0.9", "8.0.9",
                                       images_public=True)
        assert output.state is OutputState.SKIPPED

    def test_blocked_until_base_images_public(self) -> None:
        output = verify._verify_bundle(MagicMock(), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=False)
        assert output.state is OutputState.BLOCKED

    def test_stale_versions_json_with_no_pr_requests_dispatch(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions}, pulls=[])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == "dispatch-bundle"

    def test_open_update_pr_means_no_dispatch(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="open")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == ""

    def test_malformed_versions_json_is_failed_not_a_crash(self) -> None:
        # P1: a malformed downstream file must degrade to this output
        # failing, not raise through _guarded and abort the pass.
        repo = _repo_serving({"versions.json": "{not json"})
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.FAILED
        assert "could not parse `versions.json`" in output.detail

    def test_merged_and_public_everywhere_verifies(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.2", "valkey-server": {"version": "9.1.1"}}})
        repo = _repo_serving({"versions.json": versions})
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                           images_public=True)
        assert output.state is OutputState.VERIFIED
        assert "9.1.2" in output.detail

    def test_merged_but_missing_registry_is_pending_and_named(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.2", "valkey-server": {"version": "9.1.1"}}})
        repo = _repo_serving({"versions.json": versions})
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=False):
            output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                           images_public=True)
        assert output.state is OutputState.PENDING
        assert "ECR" in output.detail


_CHART = 'apiVersion: v2\nname: valkey\nversion: 0.11.0\nappVersion: "9.1.0"\n'


class TestHelm:
    def test_rc_is_skipped(self) -> None:
        output = verify._verify_helm(MagicMock(), _POLICY.downstream, "9.1.0", "rc1",
                                     image_public=True)
        assert output.state is OutputState.SKIPPED

    def test_chart_tracking_newer_line_is_skipped(self) -> None:
        repo = _repo_serving({"valkey/Chart.yaml": _CHART})
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "8.1.9", "ga",
                                     image_public=True)
        assert output.state is OutputState.SKIPPED

    def test_blocked_until_image_public(self) -> None:
        repo = _repo_serving({"valkey/Chart.yaml": _CHART})
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=False)
        assert output.state is OutputState.BLOCKED

    def test_stale_chart_with_no_pr_requests_the_bump(self) -> None:
        repo = _repo_serving({"valkey/Chart.yaml": _CHART}, pulls=[])
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == "open-helm-pr"

    def test_bumped_chart_verifies_only_when_release_and_oci_public(self) -> None:
        chart = 'apiVersion: v2\nname: valkey\nversion: 0.12.0\nappVersion: "9.1.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart}, tags={"valkey-0.12.0"})
        with patch.object(verify.pub, "ghcr_tag_exists", return_value=False):
            pending = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                          image_public=True)
        assert pending.state is OutputState.PENDING
        with patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "fetch_text",
                          return_value="entries:\n  valkey:\n  - version: 0.12.0\n"):
            done = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                       image_public=True)
        assert done.state is OutputState.VERIFIED

    def test_chart_release_missing_tag_is_pending(self) -> None:
        chart = 'apiVersion: v2\nname: valkey\nversion: 0.12.0\nappVersion: "9.1.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart}, tags=set())
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=True)
        assert output.state is OutputState.PENDING
        assert "valkey-0.12.0" in output.detail

    def test_unparseable_app_version_is_failed_not_a_crash(self) -> None:
        # P1: CHART_APP_VERSION_RE accepts "9.1", which parse_version
        # rejects; that must degrade to this output failing, not raise
        # through _guarded and abort the pass.
        chart = 'apiVersion: v2\nname: valkey\nversion: 0.12.0\nappVersion: "9.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart})
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=True)
        assert output.state is OutputState.FAILED
        assert "could not parse `valkey/Chart.yaml`" in output.detail


class TestOrderingGate:
    def test_ordered_outputs_gate_on_container_images(self) -> None:
        core_blocked = (DownstreamOutput(name="container-images",
                                         state=OutputState.PENDING),)
        with patch.object(verify, "_verify_bundle") as bundle, \
             patch.object(verify, "_verify_helm") as helm:
            verify.verify_ordered_outputs(MagicMock(), _POLICY, version="9.1.1",
                                          tag="9.1.1", stage="ga", core=core_blocked)
        assert bundle.call_args.kwargs["images_public"] is False
        assert helm.call_args.kwargs["image_public"] is False

    def test_all_settled_requires_every_output_verified_or_skipped(self) -> None:
        settled = (DownstreamOutput(name="a", state=OutputState.VERIFIED),
                   DownstreamOutput(name="b", state=OutputState.SKIPPED))
        assert verify.outputs_all_settled(settled)
        for bad in (OutputState.PENDING, OutputState.FAILED, OutputState.BLOCKED):
            assert not verify.outputs_all_settled(
                settled + (DownstreamOutput(name="c", state=bad),)
            )


class TestRCTagThreading:
    def test_rc_artifacts_are_verified_by_tag_not_bare_version(self) -> None:
        # The P1 that wedged every rc at PUBLISHED: artifacts carry the tag
        # (9.2.0-rc1), so verifying with the bare version never completes.
        seen: "list[str]" = []

        def _exists(url: str, **kw: object) -> bool:
            seen.append(url)
            return True

        with patch.object(verify.pub, "url_exists", side_effect=_exists):
            output = verify._verify_tarballs(_POLICY.downstream, "9.2.0-rc1")
        assert output.state is OutputState.VERIFIED
        assert all("valkey-9.2.0-rc1-" in url for url in seen)


class TestClosedPRsNeedHumans:
    def test_closed_unmerged_bundle_pr_is_failed_with_no_action(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="closed")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.FAILED
        assert output.action == ""

    def test_closed_unmerged_helm_pr_is_failed_with_no_action(self) -> None:
        repo = _repo_serving({"valkey/Chart.yaml": _CHART},
                             pulls=[_pr(merged=False, state="closed")])
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=True)
        assert output.state is OutputState.FAILED
        assert output.action == ""


class TestVerifierDegradation:
    def test_missing_downstream_repo_degrades_to_failed_output(self) -> None:
        # A 404 on one downstream repo (e.g. no valkey-hashes fork) must not
        # abort the pass; it reports as that output failing.
        from github.GithubException import GithubException
        gh = MagicMock()
        gh.get_repo.side_effect = GithubException(404, "missing", {})
        outputs = verify.verify_core_outputs(gh, _POLICY, tag="9.1.2", stage="ga",
                                             gh_source=gh, published_at=None)
        hashes = next(o for o in outputs if o.name == "hashes")
        assert hashes.state is OutputState.FAILED
        assert "404" in hashes.detail


def _wf_run(title: str, *, run_id: int = 700, status: str = "completed",
            conclusion: str = "success", created=None) -> MagicMock:
    from datetime import datetime, timezone
    run = MagicMock(id=run_id, status=status, conclusion=conclusion,
                    display_title=title,
                    html_url=f"https://x/runs/{run_id}",
                    created_at=created or datetime(2026, 8, 8, tzinfo=timezone.utc))
    return run


def _run_source(runs_by_workflow: "dict[str, list]") -> MagicMock:
    gh = MagicMock()

    def _get_repo(name: str) -> MagicMock:
        repo = MagicMock()

        def _get_workflow(wf: str) -> MagicMock:
            workflow = MagicMock()
            workflow.get_runs.return_value = runs_by_workflow.get(wf, [])
            return workflow

        repo.get_workflow.side_effect = _get_workflow
        return repo

    gh.get_repo.side_effect = _get_repo
    return gh


class TestBuildRunObservation:
    def test_failed_trigger_with_no_build_run_reads_failed(self) -> None:
        gh = _run_source({"build-release.yml": []})
        gh_source = _run_source({
            "trigger-build-release.yml": [_wf_run("9.1.2", conclusion="failure")],
        })
        out, run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.FAILED
        assert "trigger" in out.detail
        # The exact failed-trigger case requests the bounded auto-dispatch.
        assert out.action == "dispatch-build-release"
        assert run is None

    def test_dev_dispatch_never_requests_the_auto_dispatch(self) -> None:
        gh = _run_source({
            "build-release.yml": [_wf_run("Build Release 9.1.2 (dev)")],
        })
        gh_source = _run_source({"trigger-build-release.yml": []})
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.PENDING
        assert out.action == ""

    def test_successful_build_run_supersedes_a_failed_trigger(self) -> None:
        # Recovery may dispatch build-release directly; the build must win.
        gh = _run_source({
            "build-release.yml": [_wf_run("Build Release 9.1.2 (prod)")],
        })
        gh_source = _run_source({
            "trigger-build-release.yml": [_wf_run("9.1.2", conclusion="failure")],
        })
        out, run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.VERIFIED
        assert out.run_id == 700
        assert run is not None and run.id == 700

    def test_dev_dispatch_never_satisfies(self) -> None:
        gh = _run_source({
            "build-release.yml": [_wf_run("Build Release 9.1.2 (dev)")],
        })
        gh_source = _run_source({"trigger-build-release.yml": []})
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.PENDING

    def test_neighbor_version_does_not_match_the_boundary_anchor(self) -> None:
        gh = _run_source({"build-release.yml": []})
        gh_source = _run_source({
            "trigger-build-release.yml": [_wf_run("9.1.20", conclusion="failure")],
        })
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.PENDING  # 9.1.20 is not 9.1.2

    def test_rc_suffixed_title_does_not_match_the_ga_marker(self) -> None:
        gh = _run_source({"build-release.yml": []})
        gh_source = _run_source({
            "trigger-build-release.yml": [_wf_run("9.1.2-rc1", conclusion="failure")],
        })
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.PENDING  # 9.1.2-rc1 is not 9.1.2

    def test_bounded_absence_becomes_failed(self) -> None:
        from datetime import datetime, timedelta, timezone
        published = datetime.now(timezone.utc) - timedelta(
            minutes=_POLICY.check_timeout_minutes + 60)
        gh = _run_source({"build-release.yml": []})
        gh_source = _run_source({"trigger-build-release.yml": []})
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", published)
        assert out.state is OutputState.FAILED
        assert "within" in out.detail
        # Absence needs investigation, not a blind dispatch: no auto-action.
        assert out.action == ""


def _jobs(names_conclusions: "list[tuple[str, str]]") -> "list[MagicMock]":
    jobs = []
    for name, conclusion in names_conclusions:
        job = MagicMock(conclusion=conclusion)
        job.name = name
        jobs.append(job)
    return jobs


def _run_with_artifacts(names: "list[str]") -> MagicMock:
    run = MagicMock(id=700)
    artifacts = []
    for name in names:
        artifact = MagicMock(expired=False)
        artifact.name = name
        artifacts.append(artifact)
    run.get_artifacts.return_value = artifacts
    return run


_BUILD_OK = DownstreamOutput(name="build-run", state=OutputState.VERIFIED,
                             detail="ok", url="https://x/runs/700", run_id=700)


class TestPackagesAndTryValkey:
    def test_rc_skips_packages(self) -> None:
        out = verify._verify_packages("rc1", _BUILD_OK, None)
        assert out.state is OutputState.SKIPPED

    def test_packages_verified_by_publish_and_pages_jobs(self) -> None:
        jobs = _jobs([
            ("release-build-packages / Publish to S3", "success"),
            ("release-build-packages / Deploy Pages", "success"),
        ])
        out = verify._verify_packages("ga", _BUILD_OK, jobs)
        assert out.state is OutputState.VERIFIED

    def test_failed_publish_job_fails_packages(self) -> None:
        jobs = _jobs([
            ("release-build-packages / Publish to S3", "failure"),
        ])
        out = verify._verify_packages("ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED

    def test_packages_blocked_until_build_verified(self) -> None:
        pending = DownstreamOutput(name="build-run", state=OutputState.PENDING)
        out = verify._verify_packages("ga", pending, None)
        assert out.state is OutputState.BLOCKED

    def test_unlistable_jobs_fail_packages(self) -> None:
        # jobs None = the shared job fetch failed for a verified build run.
        out = verify._verify_packages("ga", _BUILD_OK, None)
        assert out.state is OutputState.FAILED
        assert "could not list" in out.detail

    def test_try_valkey_skipped_when_workflow_skipped_it(self) -> None:
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "skipped"),
        ])
        run = _run_with_artifacts([])
        out = verify._verify_try_valkey("ga", _BUILD_OK, run, jobs)
        assert out.state is OutputState.SKIPPED  # not the latest release

    def test_try_valkey_verified_only_with_the_upload_sentinel(self) -> None:
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "success"),
        ])
        run = _run_with_artifacts(["try-valkey-uploaded-9.1.2"])
        out = verify._verify_try_valkey("ga", _BUILD_OK, run, jobs)
        assert out.state is OutputState.VERIFIED

    def test_green_wrapper_without_sentinel_is_skipped_not_verified(self) -> None:
        # The live July 21 pattern: four releases, four green wrapper jobs,
        # only one actual upload. Job success alone must never verify.
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "success"),
        ])
        run = _run_with_artifacts([])
        out = verify._verify_try_valkey("ga", _BUILD_OK, run, jobs)
        assert out.state is OutputState.SKIPPED

    def test_stalled_pending_outputs_escalate_to_failed(self) -> None:
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=999)
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting", action="open-helm-pr"),
                   DownstreamOutput(name="bundle", state=OutputState.BLOCKED, detail="gated"))
        escalated = verify.escalate_stalled_outputs(outputs, old, 360)
        assert escalated[0].state is OutputState.FAILED
        assert "stalled" in escalated[0].detail
        # Escalation pages a human; the auto-action must not keep firing.
        assert escalated[0].action == ""
        assert escalated[1].state is OutputState.BLOCKED  # prerequisite carries it

    def test_open_pr_with_failing_checks_is_failed(self) -> None:
        pr = _pr(merged=False, state="open")
        run = MagicMock(status="completed", conclusion="failure")
        pr.base.repo.get_commit.return_value.get_check_runs.return_value = [run]
        out = verify._pr_progress_output("website", pr, "b", "o/r")
        assert out.state is OutputState.FAILED
        assert "failing checks" in out.detail
