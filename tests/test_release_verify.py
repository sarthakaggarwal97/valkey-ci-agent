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


def _pr(number: int = 5, *, merged: bool = True, state: str = "closed",
        base_ref: str = "main", changed_files: int = 1, title: str = "",
        head_ref: str = "update-branch", created=None) -> MagicMock:
    from datetime import datetime, timezone
    pr = MagicMock(number=number, merged_at="2026-08-07T00:00:00Z" if merged else None,
                   state="closed" if merged else state,
                   changed_files=changed_files, title=title,
                   created_at=created or datetime(2026, 8, 8, tzinfo=timezone.utc),
                   html_url=f"https://x/pull/{number}")
    pr.base.ref = base_ref
    pr.head.ref = head_ref
    return pr


def _repo_serving(contents: "dict[str, str]", *,
                  pulls: "list[MagicMock] | None" = None,
                  tags: "set[str] | None" = None,
                  compare_status: str = "ahead") -> MagicMock:
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_contents.side_effect = lambda path, **kw: _content_file(contents[path])
    repo.get_pulls.return_value = pulls or []
    repo.compare.return_value.status = compare_status

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

    @pytest.mark.parametrize("hostile_line", [
        "hash valkey-9.1.10.tar.gz sha256 abc url",     # superset patch
        "hash valkey-9.1.1-rc1.tar.gz sha256 abc url",  # rc of the same version
        "hash valkey-19.1.1.tar.gz sha256 abc url",     # superset major
    ])
    def test_neighbor_version_hash_lines_never_satisfy_the_tag(
            self, hostile_line: str) -> None:
        # The substring check leans on the "valkey-" prefix and ".tar.gz"
        # suffix as boundaries; every crafted neighbor must stay PENDING.
        repo = _repo_serving({"README": hostile_line})
        output = verify._verify_hashes(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert output.state is OutputState.PENDING

    def test_rc_superset_hash_line_never_satisfies_the_rc_tag(self) -> None:
        # Same attack from the rc side: rc1 must not be satisfied by rc10.
        repo = _repo_serving({"README": "hash valkey-9.2.0-rc10.tar.gz sha256 abc url"})
        output = verify._verify_hashes(gh_mock(repo), _POLICY.downstream, "9.2.0-rc1")
        assert output.state is OutputState.PENDING


class TestContainer:
    def test_merged_pr_and_public_tags_verify(self) -> None:
        repo = _repo_serving({}, pulls=[_pr()])
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            pr_out, images = verify._verify_container(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert pr_out.state is OutputState.VERIFIED
        assert images.state is OutputState.VERIFIED

    def test_missing_variant_tag_blocks_images(self) -> None:
        repo = _repo_serving({}, pulls=[_pr()])
        with patch.object(verify.pub, "dockerhub_tag_exists",
                          side_effect=lambda r, t: not t.endswith("-alpine")), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            _pr_out, images = verify._verify_container(gh_mock(repo), _POLICY.downstream, "9.1.1")
        assert images.state is OutputState.PENDING
        assert "9.1.1-alpine" in images.detail

    def test_closed_unmerged_pr_is_failed(self) -> None:
        repo = _repo_serving({}, pulls=[_pr(merged=False, state="closed")])
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

    def test_patch_release_verifies_by_reachable_docs_tag(self) -> None:
        repo = _repo_serving({}, tags={"9.1.1"})
        output = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga")
        assert output.state is OutputState.VERIFIED
        # Honest evidence level: the tag and its branch placement were
        # checked; the public deployment was not.
        assert "not verified" in output.detail

    def test_docs_tag_off_the_default_branch_is_failed(self) -> None:
        # The tag exists but its commit never landed on the default branch:
        # that is not the release flow's update, not a pending state.
        repo = _repo_serving({}, tags={"9.1.1"}, compare_status="diverged")
        output = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga")
        assert output.state is OutputState.FAILED
        assert "not reachable" in output.detail

    def test_minor_release_verifies_by_docs_pr_with_honest_detail(self) -> None:
        repo = _repo_serving({}, pulls=[_pr()])
        output = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        assert output.state is OutputState.VERIFIED
        assert "Merged PR evidence only" in output.detail

    def test_wrong_base_pr_never_satisfies(self) -> None:
        # A PR retargeted at a side branch never lands the update; it is
        # not release evidence for docs or website.
        repo = _repo_serving({}, pulls=[_pr(base_ref="release-9.2")])
        docs = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        site = verify._verify_website(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        assert docs.state is OutputState.PENDING
        assert site.state is OutputState.PENDING

    def test_zero_file_pr_never_satisfies(self) -> None:
        repo = _repo_serving({}, pulls=[_pr(changed_files=0)])
        docs = verify._verify_docs(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        site = verify._verify_website(gh_mock(repo), _POLICY.downstream, "9.2.0", "ga")
        assert docs.state is OutputState.PENDING
        assert site.state is OutputState.PENDING

    def test_website_merged_pr_verifies_with_honest_detail(self) -> None:
        repo = _repo_serving({}, pulls=[_pr()])
        output = verify._verify_website(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga")
        assert output.state is OutputState.VERIFIED
        assert "public deployment is not verified" in output.detail


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
                             pulls=[_pr(merged=False, state="open",
                                        title="Update valkey-server to 9.1.1")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == ""

    def test_old_closed_pr_no_longer_wedges_the_release(self) -> None:
        # F28: a closed-unmerged PR carrying an OLDER tag is a previous
        # release's history, not this release's rejection; the not-started
        # path holds and the dispatch can proceed.
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="closed",
                                        title="Update valkey-server to 9.1.0")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == "dispatch-bundle"

    def test_old_open_pr_is_ignored(self) -> None:
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="open",
                                        title="Update valkey-server to 9.1.0")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == "dispatch-bundle"

    def test_neighbor_tag_in_pr_title_never_matches(self) -> None:
        # 9.1.1 must not be satisfied by a 9.1.10 (or 9.1.1-rc1) PR title.
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="open",
                                        title="Update valkey-server to 9.1.10")])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.action == "dispatch-bundle"

    def test_pr_created_before_publication_is_ignored(self) -> None:
        from datetime import datetime, timedelta, timezone
        published = datetime(2026, 8, 8, tzinfo=timezone.utc)
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="open",
                                        title="Update valkey-server to 9.1.1",
                                        created=published - timedelta(days=2))])
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True, published_at=published)
        assert output.state is OutputState.PENDING
        assert output.action == "dispatch-bundle"

    def test_malformed_versions_json_is_failed_not_a_crash(self) -> None:
        # P1: a malformed downstream file must degrade to this output
        # failing, not raise through _guarded and abort the pass.
        repo = _repo_serving({"versions.json": "{not json"})
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.FAILED
        assert "Could not parse `versions.json`" in output.detail

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


class TestBundleHostileVersionsJson:
    """Structurally valid but wrong versions.json payloads. Anything a
    downstream commit can serve must degrade to this output failing (or
    pending), never raise through _guarded (which only catches
    GithubException) and abort the pass."""

    @pytest.mark.parametrize("payload", ["null", '"9.1.1"', "[]", '[{"9.1": {}}]'])
    def test_non_object_top_level_is_failed_not_a_crash(self, payload: str) -> None:
        repo = _repo_serving({"versions.json": payload})
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.FAILED
        assert "Could not parse `versions.json`" in output.detail

    def test_null_value_under_a_different_key_degrades(self) -> None:
        # {"valkey": null} has no "9.1" entry at all, so the .get defaults
        # hold and the verifier reads "nothing recorded".
        repo = _repo_serving({"versions.json": json.dumps({"valkey": None})})
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.PENDING
        assert output.action == "dispatch-bundle"

    @pytest.mark.parametrize("payload", [
        {"9.1": None},                       # line present but null
        {"9.1": "9.1.1"},                    # line is a bare string
        {"9.1": {"valkey-server": None}},    # server entry present but null
        {"9.1": {"valkey-server": "9.1.1"}},  # server entry is a bare string
    ])
    def test_null_or_scalar_line_values_degrade_to_failed(self, payload: dict) -> None:
        repo = _repo_serving({"versions.json": json.dumps(payload)})
        output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                       images_public=True)
        assert output.state is OutputState.FAILED

    def test_missing_bundle_version_key_never_verifies(self) -> None:
        versions = json.dumps({"9.1": {"valkey-server": {"version": "9.1.1"}}})
        repo = _repo_serving({"versions.json": versions})
        with patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            output = verify._verify_bundle(gh_mock(repo), _POLICY.downstream, "9.1.1", "9.1.1",
                                           images_public=True)
        assert output.state is not OutputState.VERIFIED


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
        assert "Could not parse `valkey/Chart.yaml`" in output.detail

    def test_chart_yaml_missing_version_line_is_failed_not_a_crash(self) -> None:
        chart = 'apiVersion: v2\nname: valkey\nappVersion: "9.1.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart})
        output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                     image_public=True)
        assert output.state is OutputState.FAILED
        assert "Could not parse appVersion/version" in output.detail

    @pytest.mark.parametrize("index_body", [
        "entries:\n  valkey:\n  - version: 0.12.10\n",   # superset chart version
        "entries:\n  valkey:\n  - version: 0.12.0-rc1\n",  # suffixed entry
        "entries:\n  valkey:\n  - appVersion: 0.12.0\n",   # wrong key
        "",                                                # empty index
    ])
    def test_neighbor_index_entries_never_satisfy_the_chart_version(
            self, index_body: str) -> None:
        # The index regex is line-anchored; every crafted neighbor of
        # 0.12.0 must leave the chart PENDING, not VERIFIED.
        chart = 'apiVersion: v2\nname: valkey\nversion: 0.12.0\nappVersion: "9.1.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart}, tags={"valkey-0.12.0"})
        with patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "fetch_text", return_value=index_body):
            output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                         image_public=True)
        assert output.state is OutputState.PENDING

    def test_other_charts_index_entry_never_satisfies_the_valkey_chart(self) -> None:
        chart = 'apiVersion: v2\nname: valkey\nversion: 0.12.0\nappVersion: "9.1.1"\n'
        repo = _repo_serving({"valkey/Chart.yaml": chart}, tags={"valkey-0.12.0"})
        index = ("entries:\n"
                 "  valkey-bundle:\n"
                 "  - version: 0.12.0\n"
                 "  valkey:\n"
                 "  - version: 0.11.0\n")
        with patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "fetch_text", return_value=index):
            output = verify._verify_helm(gh_mock(repo), _POLICY.downstream, "9.1.1", "ga",
                                         image_public=True)
        assert output.state is OutputState.PENDING


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
        # The PR carries THIS release's tag, so its closure is this
        # release's human decision (unlike an older tag's leftover PR).
        versions = json.dumps({"9.1": {"version": "9.1.1", "valkey-server": {"version": "9.1.0"}}})
        repo = _repo_serving({"versions.json": versions},
                             pulls=[_pr(merged=False, state="closed",
                                        title="Update valkey-server to 9.1.1")])
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

    def test_urlerror_degrades_one_output_and_siblings_still_verify(self) -> None:
        # F29: public_endpoints deliberately raises on 5xx/429 and network
        # failures; those must land as a probe error on THAT output only,
        # never abort the pass.
        import urllib.error

        repo = _repo_serving({"README": "hash valkey-9.1.2.tar.gz sha256 abc url"},
                             tags={"9.1.2"})
        repo.get_workflow.return_value.get_runs.return_value = []
        gh = gh_mock(repo)

        def _boom(url: str, **kw: object) -> bool:
            raise urllib.error.URLError("connection timed out")

        with patch.object(verify.pub, "url_exists", side_effect=_boom), \
             patch.object(verify.pub, "dockerhub_tag_exists", return_value=True), \
             patch.object(verify.pub, "ghcr_tag_exists", return_value=True), \
             patch.object(verify.pub, "ecr_public_tag_exists", return_value=True):
            outputs = verify.verify_core_outputs(gh, _POLICY, tag="9.1.2", stage="ga",
                                                 gh_source=gh, published_at=None)
        tarballs = next(o for o in outputs if o.name == "tarballs")
        assert tarballs.state is OutputState.FAILED
        assert tarballs.detail.startswith("Probe error: ")
        hashes = next(o for o in outputs if o.name == "hashes")
        assert hashes.state is OutputState.VERIFIED
        docs = next(o for o in outputs if o.name == "docs")
        assert docs.state is OutputState.VERIFIED


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

    def test_cancelled_trigger_never_requests_the_auto_dispatch(self) -> None:
        # A human cancelling the trigger is a decision, not a retry
        # condition: FAILED with the correct phrase, but no auto-action.
        gh = _run_source({"build-release.yml": []})
        gh_source = _run_source({
            "trigger-build-release.yml": [_wf_run("9.1.2", conclusion="cancelled")],
        })
        out, run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.FAILED
        assert "was cancelled" in out.detail
        assert out.action == ""
        assert run is None

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

    def test_run_with_a_none_display_title_is_skipped_not_a_crash(self) -> None:
        # GitHub can serve runs whose display_title is null; the scan must
        # skip them, not raise.
        gh = _run_source({
            "build-release.yml": [_wf_run(None),
                                  _wf_run("Build Release 9.1.2 (prod)")],
        })
        gh_source = _run_source({"trigger-build-release.yml": []})
        out, _run = verify._verify_build_run(gh, gh_source, _POLICY, "9.1.2", None)
        assert out.state is OutputState.VERIFIED

    def test_marked_run_predating_publication_is_not_evidence(self) -> None:
        # A perfectly-marked run created BEFORE the release was published
        # (e.g. a previous attempt of the same tag) must not satisfy this
        # publication's build.
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        stale = _wf_run("Build Release 9.1.2 (prod)",
                        created=now - timedelta(minutes=10))
        gh = _run_source({"build-release.yml": [stale]})
        gh_source = _run_source({"trigger-build-release.yml": []})
        out, run = verify._verify_build_run(
            gh, gh_source, _POLICY, "9.1.2", now - timedelta(minutes=1))
        assert out.state is OutputState.PENDING
        assert run is None


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


def _gh_latest(tag_name: str) -> MagicMock:
    """A source-repo gh mock whose latest release carries *tag_name*."""
    gh = MagicMock()
    gh.get_repo.return_value.get_latest_release.return_value = MagicMock(
        tag_name=tag_name)
    return gh


class TestPackagesAndTryValkey:
    def test_rc_skips_packages(self) -> None:
        out = verify._verify_packages(_POLICY.downstream, "rc1", _BUILD_OK, None)
        assert out.state is OutputState.SKIPPED

    def test_packages_verified_by_the_exact_publish_inventory(self) -> None:
        # The fixture policy expects exactly 2 RPM and 1 DEB publish jobs.
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / RPM · el9 (aarch64) · Publish to S3", "success"),
            ("release-build-packages / DEB · bookworm (x86_64) · Publish to S3", "success"),
            ("release-build-packages / Deploy Pages", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.VERIFIED
        # When a Deploy Pages job is present in the succeeded set the
        # detail claims pages succeeded; otherwise the wording must stay
        # honest (see test_pages_wording_admits_when_pages_was_not_checked).
        assert "and the pages jobs succeeded" in out.detail

    def test_pages_wording_admits_when_pages_was_not_checked(self) -> None:
        # F6-adjacent: the RPM/DEB matrix satisfied its inventory but no
        # Deploy Pages job ran, so the detail cannot claim "pages
        # succeeded" — it says (pages not checked) instead. Full
        # digest/provenance verification is a deferred redesign; not
        # lying about pages is the fix here.
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / RPM · el9 (aarch64) · Publish to S3", "success"),
            ("release-build-packages / DEB · bookworm (x86_64) · Publish to S3", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.VERIFIED
        assert "(pages not checked)" in out.detail
        assert "and the pages jobs succeeded" not in out.detail

    def test_dropped_platform_fails_packages_despite_green_jobs(self) -> None:
        # F21: a green-but-smaller matrix (one RPM platform silently
        # dropped) must read FAILED, not VERIFIED.
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / DEB · bookworm (x86_64) · Publish to S3", "success"),
            ("release-build-packages / Deploy Pages", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED
        assert ("(Evidence mismatch: 1 RPM publish jobs succeeded, "
                "expected exactly 2)") in out.detail

    def test_dropped_deb_platform_fails_packages(self) -> None:
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / RPM · el9 (aarch64) · Publish to S3", "success"),
            ("release-build-packages / Deploy Pages", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED
        assert ("(Evidence mismatch: 0 DEB publish jobs succeeded, "
                "expected exactly 1)") in out.detail

    def test_failed_publish_job_fails_packages(self) -> None:
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "failure"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED

    def test_packages_blocked_until_build_verified(self) -> None:
        pending = DownstreamOutput(name="build-run", state=OutputState.PENDING)
        out = verify._verify_packages(_POLICY.downstream, "ga", pending, None)
        assert out.state is OutputState.BLOCKED

    def test_unlistable_jobs_fail_packages(self) -> None:
        # jobs None = the shared job fetch failed for a verified build run.
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, None)
        assert out.state is OutputState.FAILED
        assert "Could not list" in out.detail

    def test_try_valkey_missing_sentinel_is_failed_for_the_latest_release(self) -> None:
        # F22 (also the live July 21 pattern: green wrapper jobs, no
        # upload): when this release IS the repository's latest, a missing
        # sentinel means the public deployment was never updated.
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "success"),
        ])
        run = _run_with_artifacts([])
        out = verify._verify_try_valkey(
            "ga", _BUILD_OK, run, jobs,
            gh_source=_gh_latest("9.1.2"), repo_name="valkey-io/valkey", tag="9.1.2")
        assert out.state is OutputState.FAILED
        assert out.detail == "Try Valkey evidence is missing for the latest release"

    def test_try_valkey_skipped_only_when_provably_not_latest(self) -> None:
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "skipped"),
        ])
        run = _run_with_artifacts([])
        out = verify._verify_try_valkey(
            "ga", _BUILD_OK, run, jobs,
            gh_source=_gh_latest("9.2.0"), repo_name="valkey-io/valkey", tag="9.1.2")
        assert out.state is OutputState.SKIPPED
        assert "provably not" in out.detail

    def test_try_valkey_latest_comparison_failure_fails_closed(self) -> None:
        # An unreadable or unparseable latest release is inconclusive; the
        # missing sentinel must read FAILED, never settle as SKIPPED.
        from github.GithubException import GithubException
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "success"),
        ])
        run = _run_with_artifacts([])
        gh = MagicMock()
        gh.get_repo.return_value.get_latest_release.side_effect = (
            GithubException(500, "boom", {}))
        out = verify._verify_try_valkey(
            "ga", _BUILD_OK, run, jobs,
            gh_source=gh, repo_name="valkey-io/valkey", tag="9.1.2")
        assert out.state is OutputState.FAILED

    def test_try_valkey_verified_only_with_the_upload_sentinel(self) -> None:
        jobs = _jobs([
            ("update-try-valkey / build-try-valkey", "success"),
        ])
        run = _run_with_artifacts(["try-valkey-uploaded-9.1.2"])
        out = verify._verify_try_valkey(
            "ga", _BUILD_OK, run, jobs,
            gh_source=MagicMock(), repo_name="valkey-io/valkey", tag="9.1.2")
        assert out.state is OutputState.VERIFIED

    def test_stalled_pending_outputs_escalate_to_failed(self) -> None:
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=999)
        # The helm output carries attempt evidence (a PR url), so the
        # normal deadline applies even though an action is set.
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting", action="open-helm-pr",
                                    url="https://x/pull/9"),
                   DownstreamOutput(name="bundle", state=OutputState.BLOCKED, detail="gated"))
        escalated = verify.escalate_stalled_outputs(outputs, old, 360)
        assert escalated[0].state is OutputState.FAILED
        assert escalated[0].detail == "Stalled after 6 hours: waiting"
        # Escalation pages a human; the auto-action must not keep firing.
        assert escalated[0].action == ""
        assert escalated[1].state is OutputState.BLOCKED  # prerequisite carries it

    def test_fresh_action_bearing_output_is_exempt_from_the_release_clock(self) -> None:
        # F13: the clock starts at publication, but Bundle/Helm may spend
        # that whole window BLOCKED and only just unblock. With no attempt
        # evidence yet (empty run_id and url), the output keeps its action
        # so the first dispatch can still happen.
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=999)
        outputs = (DownstreamOutput(name="bundle", state=OutputState.PENDING,
                                    detail="the bundle update has not started yet",
                                    action="dispatch-bundle"),)
        escalated = verify.escalate_stalled_outputs(outputs, old, 360)
        assert escalated[0].state is OutputState.PENDING
        assert escalated[0].action == "dispatch-bundle"

    def test_attempted_action_bearing_output_still_escalates(self) -> None:
        # Once an attempt was observed (run_id evidence), the exemption
        # ends and the deadline applies.
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=999)
        outputs = (DownstreamOutput(name="bundle", state=OutputState.PENDING,
                                    detail="waiting", action="dispatch-bundle",
                                    run_id=42),)
        escalated = verify.escalate_stalled_outputs(outputs, old, 360)
        assert escalated[0].state is OutputState.FAILED
        assert escalated[0].action == ""

    def test_stall_detail_strips_the_trailing_period(self) -> None:
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=999)
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting for the chart PR."),)
        escalated = verify.escalate_stalled_outputs(outputs, old, 360)
        assert escalated[0].detail == "Stalled after 6 hours: waiting for the chart PR"

    # Humanized durations flip to whole hours at two hours, flooring:
    # 359 minutes reads "5 hours", 360 reads "6 hours".
    @pytest.mark.parametrize(("minutes", "rendered"), [
        pytest.param(90, "90 minutes", id="under-two-hours"),
        pytest.param(119, "119 minutes", id="last-minutes-value"),
        pytest.param(120, "2 hours", id="first-hours-value"),
        pytest.param(359, "5 hours", id="359-floors-to-5-hours"),
        pytest.param(360, "6 hours", id="360-is-6-hours"),
    ])
    def test_humanized_duration_boundaries(self, minutes: int, rendered: str) -> None:
        from datetime import datetime, timedelta, timezone
        old = datetime.now(timezone.utc) - timedelta(minutes=minutes + 1)
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting"),)
        escalated = verify.escalate_stalled_outputs(outputs, old, minutes)
        assert escalated[0].detail == f"Stalled after {rendered}: waiting"

    def test_open_pr_with_failing_checks_is_failed(self) -> None:
        pr = _pr(merged=False, state="open")
        run = MagicMock(status="completed", conclusion="failure")
        pr.base.repo.get_commit.return_value.get_check_runs.return_value = [run]
        out = verify._pr_progress_output("website", pr, "b", "o/r")
        assert out.state is OutputState.FAILED
        assert "failing checks" in out.detail


class TestHostileJobPayloads:
    """Job lists shaped like what a chaotic API (or a broken matrix) can
    actually serve: empty, oddly concluded, unicode, or null-named."""

    def test_ga_with_an_empty_jobs_list_fails_packages(self) -> None:
        # jobs [] is not None: the fetch worked and returned nothing, which
        # means the publish matrix never ran.
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, [])
        assert out.state is OutputState.FAILED
        assert "no package publish jobs" in out.detail

    def test_empty_string_conclusion_never_verifies_packages(self) -> None:
        jobs = _jobs([("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "")])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED
        assert "Publish to S3" in out.detail

    def test_unicode_job_names_are_matched_safely(self) -> None:
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3 🚀", "success"),
            ("release-build-packages / RPM · el9 (aarch64) · Publish to S3 🚀", "success"),
            ("release-build-packages / DEB · bookworm · Publish to S3 · résumé", "success"),
            ("release-build-packages / Deploy Pages · résumé", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.VERIFIED

    def test_duplicate_job_names_never_satisfy_the_inventory(self) -> None:
        # The same RPM job served twice (rerun attempts listed together)
        # is one platform, not two.
        jobs = _jobs([
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / RPM · el9 (x86_64) · Publish to S3", "success"),
            ("release-build-packages / DEB · bookworm (x86_64) · Publish to S3", "success"),
        ])
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, jobs)
        assert out.state is OutputState.FAILED
        assert "expected exactly 2" in out.detail

    def test_none_job_name_degrades_instead_of_crashing(self) -> None:
        nameless = MagicMock(conclusion="success")
        nameless.name = None
        out = verify._verify_packages(_POLICY.downstream, "ga", _BUILD_OK, [nameless])
        assert out.state is OutputState.FAILED

    def test_expired_upload_sentinel_is_not_evidence(self) -> None:
        # size-0-but-expired-False is fine for a sentinel; expired is not.
        # For the latest release, an expired sentinel therefore reads
        # FAILED, the same as no sentinel at all.
        jobs = _jobs([("update-try-valkey / build-try-valkey", "success")])
        run = _run_with_artifacts(["try-valkey-uploaded-9.1.2"])
        for artifact in run.get_artifacts.return_value:
            artifact.expired = True
        out = verify._verify_try_valkey(
            "ga", _BUILD_OK, run, jobs,
            gh_source=_gh_latest("9.1.2"), repo_name="valkey-io/valkey", tag="9.1.2")
        assert out.state is OutputState.FAILED


class TestStallEscalationBoundary:
    """The deadline comparison is strict: exactly-at-timeout is not yet
    stalled (the next pass escalates); one second past is."""

    @staticmethod
    def _at(seconds_past_deadline: int, timeout_minutes: int = 360):
        from datetime import datetime, timedelta, timezone
        published = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        now = published + timedelta(minutes=timeout_minutes,
                                    seconds=seconds_past_deadline)
        frozen = MagicMock()
        frozen.now.return_value = now
        return published, patch.object(verify, "datetime", frozen)

    def test_exactly_at_the_deadline_does_not_escalate(self) -> None:
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting"),)
        published, clock = self._at(0)
        with clock:
            escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.PENDING

    def test_one_second_before_the_deadline_does_not_escalate(self) -> None:
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting"),)
        published, clock = self._at(-1)
        with clock:
            escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.PENDING

    def test_one_second_past_the_deadline_escalates(self) -> None:
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting"),)
        published, clock = self._at(1)
        with clock:
            escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.FAILED

    def test_unpublished_release_never_escalates(self) -> None:
        outputs = (DownstreamOutput(name="helm", state=OutputState.PENDING,
                                    detail="waiting"),)
        assert verify.escalate_stalled_outputs(outputs, None, 360) == outputs

    def test_fresh_attempt_after_long_upstream_block_is_not_escalated(self) -> None:
        # F25: the release published hours ago (spent long in BLOCKED),
        # but the downstream PR was JUST opened. The escalator's per-
        # attempt clock must run from pr.created_at, not published_at,
        # otherwise the first-ever downstream attempt is killed on its
        # first pass.
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        published = now - timedelta(minutes=999)
        fresh = now - timedelta(minutes=5)
        outputs = (DownstreamOutput(
            name="bundle", state=OutputState.PENDING,
            detail="Bundle update PR #7 is open",
            url="https://x/pull/7", attempt_started_at=fresh,
        ),)
        escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.PENDING

    def test_old_attempt_still_escalates_even_on_a_fresh_release(self) -> None:
        # Symmetry check: a 6h-old attempt escalates on its own clock even
        # when the release itself was only just published.
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        published = now - timedelta(minutes=1)
        old_attempt = now - timedelta(minutes=999)
        outputs = (DownstreamOutput(
            name="bundle", state=OutputState.PENDING,
            detail="Bundle update PR #7 is open",
            url="https://x/pull/7", attempt_started_at=old_attempt,
        ),)
        escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.FAILED
        assert escalated[0].attempt_started_at == old_attempt

    def test_attempt_timestamp_falls_back_to_release_wide_clock(self) -> None:
        # When an output carries no attempt evidence (registry-probe
        # PENDING, hashes not yet recorded, ...), the release-wide clock
        # still governs so an eternal probe eventually escalates.
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        published = now - timedelta(minutes=999)
        outputs = (DownstreamOutput(
            name="hashes", state=OutputState.PENDING, detail="waiting",
        ),)
        escalated = verify.escalate_stalled_outputs(outputs, published, 360)
        assert escalated[0].state is OutputState.FAILED


class TestDetailCellStyle:
    def test_detail_cells_are_capitalized_statements(self) -> None:
        # Representative samples of the Detail-cell style rule: every cell
        # starts with a capital letter, reads as a statement, and never asks
        # a rhetorical question.
        from github.GithubException import GithubException

        def _raise_404() -> None:
            raise GithubException(404, "missing", {})

        guarded = verify._guarded("hashes", _raise_404)

        gh = _run_source({
            "build-release.yml": [_wf_run("Build Release 9.1.2 (prod)",
                                          conclusion="failure")],
        })
        gh_source = _run_source({"trigger-build-release.yml": []})
        failed_build, _run = verify._verify_build_run(gh, gh_source, _POLICY,
                                                      "9.1.2", None)

        for detail in (guarded.detail, failed_build.detail):
            assert detail[0].isupper()
            assert "?" not in detail
            assert "!" not in detail
            assert "\u2014" not in detail

    def test_no_em_dash_in_release_module_sources(self) -> None:
        # The real regression guard: no em-dash character may appear in any
        # release-module source, so no rendered string can ever carry one.
        from pathlib import Path

        module_dir = Path(verify.__file__).parent
        sources = sorted(module_dir.glob("*.py"))
        assert sources
        for source in sources:
            assert "\u2014" not in source.read_text(encoding="utf-8"), source.name
