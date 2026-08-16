"""Tests for protected publication: revalidation, create-at-SHA, post-verify."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest
from github.GithubException import GithubException

from scripts.release import issue as issue_mod
from scripts.release.authorize import NotAuthorizedError
from scripts.release.publish import (
    _APPROVAL_MARKER,
    _PUBLICATION_MARKER,
    PublishPlan,
    _ensure_tag_at_sha,
    _make_latest_decision,
    _previous_tag,
    ensure_environment_protected,
    plan_digest,
    plan_publication,
    post_approval_evidence,
    publish_release,
    render_plan_summary,
    tag_ruleset_protected,
)
from scripts.release.reconcile import ReleaseControlError
from tests.release_fixtures import (
    MERGE_SHA,
    bot_receipt,
    gh_mock,
    make_policy,
    notes_pr,
    qualification_run,
    repo_mock,
    tag_ref,
    tracker,
)

_POLICY = make_policy()

_VERSION_H = (
    '#define VALKEY_VERSION "9.1.1"\n'
    "#define VALKEY_VERSION_NUM 0x00090101\n"
    '#define VALKEY_RELEASE_STAGE "ga"\n'
)
_NOTES = (
    "Valkey 9.1.1  -  Released Tue 21 July 2026\n"
    "=====\n"
    "Upgrade urgency MODERATE.\n\n"
    "### Bug Fixes\n* Fix a thing by @someone (#1234)\n\n"
    "Valkey 9.1.0  -  Released earlier\n=====\nolder section\n"
)


def _contents_for(version_h: str = _VERSION_H, notes: str = _NOTES):
    """A ``get_contents`` side effect serving *version_h* / *notes* by path."""
    def _contents(path: str, **kw: object) -> MagicMock:
        f = MagicMock()
        f.decoded_content = (version_h if path.endswith("version.h") else notes).encode()
        return f
    return _contents


def _ready_repo(**overrides: object) -> MagicMock:
    """A repo mock in the READY state whose contents serve the publish reads."""
    repo = repo_mock(tags=["9.1.0"], **overrides)  # type: ignore[arg-type]
    repo.get_contents.side_effect = _contents_for()
    # The latest decision uses get_releases() enumeration; keep the legacy
    # get_latest_release in sync so both the pre-publication decision and
    # the post-publish pointer verify see the same maximum ("9.1.0" here:
    # this GA advances).
    _latest = MagicMock(tag_name="9.1.0", draft=False, prerelease=False)
    repo.get_latest_release.return_value = _latest
    repo.get_releases.return_value = [_latest]
    return repo


def _with_tracker(repo: MagicMock) -> MagicMock:
    repo.get_issues.return_value = [tracker()]
    return repo


def _publishable_repo() -> MagicMock:
    """A READY repo mock whose create-release call makes the tag resolvable."""
    repo = _with_tracker(_ready_repo())
    release = MagicMock(html_url="https://x/releases/9.1.1")
    repo.create_git_release.return_value = release
    repo.get_issue.return_value = repo.get_issues.return_value[0]

    def _tag_after_create(*args: object, **kwargs: object) -> MagicMock:
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref()
        # Defense-in-depth: publish_release re-reads the latest pointer
        # after create. When the plan asked for make_latest=true, the
        # pointer must now show this release; otherwise the verify step
        # would flag a race.
        make_latest = kwargs.get("make_latest", "false")
        tag = args[0] if args else kwargs.get("tag", "")
        if make_latest == "true" and tag:
            new_latest = MagicMock(tag_name=tag, draft=False, prerelease=False)
            repo.get_latest_release.return_value = new_latest
            repo.get_releases.return_value = [new_latest]
        return release

    repo.create_git_release.side_effect = _tag_after_create
    return repo


class TestPlanPublication:
    def test_ready_release_produces_a_complete_plan(self) -> None:
        repo = _with_tracker(_ready_repo())
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")
        assert plan.tag == "9.1.1"
        assert plan.sha == MERGE_SHA
        assert not plan.prerelease
        assert plan.make_latest == "true"  # 9.1.1 > latest 9.1.0
        assert "Upgrade urgency MODERATE" in plan.body
        assert "compare/9.1.0...9.1.1" in plan.body
        assert "older section" not in plan.body
        summary = render_plan_summary(plan)
        assert "9.1.1" in summary and MERGE_SHA in summary

    def test_not_ready_release_is_refused_with_blockers(self) -> None:
        repo = _with_tracker(_ready_repo(qual_runs=[]))  # qualification missing
        with pytest.raises(ReleaseControlError, match="not ready to publish"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_existing_tag_is_refused(self) -> None:
        # A tag without a release now surfaces upstream in compute_status as
        # an unshippable alert, so planning refuses at the readiness gate
        # (publish.py's own tag check remains as defense-in-depth).
        repo = _with_tracker(_ready_repo())
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref("f" * 40)  # tag exists
        with pytest.raises(ReleaseControlError, match="unshippable"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_version_h_mismatch_is_refused(self) -> None:
        repo = _with_tracker(_ready_repo())
        wrong = _VERSION_H.replace("9.1.1", "9.1.0").replace("0x00090101", "0x00090100")
        repo.get_contents.side_effect = _contents_for(version_h=wrong)
        with pytest.raises(ReleaseControlError, match="version.h"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_missing_notes_section_is_refused(self) -> None:
        repo = _with_tracker(_ready_repo())
        repo.get_contents.side_effect = _contents_for(notes="Valkey 9.1.0 only\n")
        with pytest.raises(ReleaseControlError, match="no dated section"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_old_line_patch_never_steals_latest(self) -> None:
        repo = _with_tracker(_ready_repo())
        # A newer line has already shipped; enumerating releases surfaces
        # 9.2.0 as the current maximum, so publishing 9.1.1 must not move
        # the pointer even if get_latest_release still names the old tag.
        newer = MagicMock(tag_name="9.2.0", draft=False, prerelease=False)
        older = MagicMock(tag_name="9.1.0", draft=False, prerelease=False)
        repo.get_releases.return_value = [older, newer]
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")
        assert plan.make_latest == "false"

    def test_unauthorized_actor_cannot_plan(self) -> None:
        repo = _with_tracker(_ready_repo())
        with pytest.raises(NotAuthorizedError):
            plan_publication(gh_mock(repo, member=False), _POLICY,
                             branch="9.1", actor="drive-by")


class TestPublishRelease:
    def test_publishes_at_exact_sha_with_explicit_flags(self) -> None:
        # Bindings matching the revalidated plan exactly must publish: this
        # is the approved path (main.py always passes both bindings).
        repo = _publishable_repo()
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"
        kwargs = repo.create_git_release.call_args.kwargs
        assert repo.create_git_release.call_args.args[0] == "9.1.1"
        assert kwargs["target_commitish"] == MERGE_SHA
        assert kwargs["prerelease"] is False
        assert kwargs["make_latest"] == "true"
        assert kwargs["draft"] is False
        # Publication is recorded on the tracker.
        comment = repo.get_issue.return_value.create_comment.call_args.kwargs["body"]
        assert "9.1.1" in comment and MERGE_SHA in comment

    def test_expected_tag_mismatch_refuses_before_any_write(self) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="approval was for"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.2")
        repo.create_git_release.assert_not_called()

    def test_tag_pointing_elsewhere_after_create_is_critical(self) -> None:
        repo = _publishable_repo()

        def _wrong_tag(*args: object, **kwargs: object) -> MagicMock:
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = tag_ref("e" * 40)
            return MagicMock(html_url="https://x/releases/9.1.1")

        repo.create_git_release.side_effect = _wrong_tag
        with pytest.raises(ReleaseControlError, match="CRITICAL"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")


class TestPlanDigestBinding:
    def test_matching_digest_publishes(self) -> None:
        # The digest the approver saw, recomputed from an unchanged world,
        # must match and publish.
        repo = _publishable_repo()
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson", controller_sha="f" * 40)
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA,
                              expected_digest=plan_digest(plan),
                              controller_sha="f" * 40)
        assert url == "https://x/releases/9.1.1"

    def test_digest_mismatch_refuses_before_any_write(self) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError,
                           match="plan changed after approval"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA,
                            expected_digest="0" * 64)
        repo.create_git_release.assert_not_called()

    def test_controller_sha_drift_changes_the_digest_and_refuses(self) -> None:
        # Approval bound one controller commit; execution from another
        # commit (a force-push between validate and publish) must refuse.
        repo = _publishable_repo()
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson", controller_sha="f" * 40)
        with pytest.raises(ReleaseControlError,
                           match="plan changed after approval"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA,
                            expected_digest=plan_digest(plan),
                            controller_sha="e" * 40)
        repo.create_git_release.assert_not_called()

    def test_absent_digest_is_legacy_and_the_tag_sha_binding_still_holds(self) -> None:
        repo = _publishable_repo()
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"

    def test_digest_is_stable_and_covers_every_bound_dimension(self) -> None:
        base = _plan()
        assert plan_digest(base) == plan_digest(_plan())
        assert re.fullmatch(r"[0-9a-f]{64}", plan_digest(base))
        variants = [
            _plan(tag="9.1.2"),
            _plan(sha="b" * 40),
            _plan(prerelease=True),
            _plan(make_latest="false"),
            _plan(body="different notes"),
            _plan(qualification_run_id=901),
            _plan(tag_protected=True),
            _plan(tag_protected=True, tag_bypass_integration_ids=(5,)),
            _plan(controller_sha="f" * 40),
        ]
        digests = {plan_digest(v) for v in variants}
        assert plan_digest(base) not in digests
        assert len(digests) == len(variants)  # every dimension binds

    def test_summary_shows_the_short_digest(self) -> None:
        plan = _plan()
        summary = render_plan_summary(plan)
        assert f"Plan digest: `{plan_digest(plan)[:12]}`" in summary


class TestCreateReleaseRecovery:
    """create_git_release is never blind-retried: one attempt, then
    read-after-write recovery for the lost-response case only."""

    def test_lost_response_with_the_release_at_the_approved_sha_recovers(self) -> None:
        repo = _publishable_repo()

        def _lost_response(*args: object, **kwargs: object) -> None:
            # The create landed server-side; only the response was lost.
            # GitHub therefore ALSO moved the latest pointer (make_latest=true
            # took effect); the mocked read-after-write must reflect that or
            # the pointer verify would raise a false CRITICAL.
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = tag_ref()
            repo.get_release.side_effect = None
            repo.get_release.return_value = MagicMock(
                html_url="https://x/releases/9.1.1")
            new_latest = MagicMock(tag_name="9.1.1", draft=False, prerelease=False)
            repo.get_latest_release.return_value = new_latest
            repo.get_releases.return_value = [new_latest]
            raise GithubException(502, "bad gateway", {})

        repo.create_git_release.side_effect = _lost_response
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"
        repo.create_git_release.assert_called_once()

    def test_genuinely_failed_create_re_raises_without_a_retry(self) -> None:
        repo = _publishable_repo()
        repo.create_git_release.side_effect = GithubException(422, "boom", {})
        with pytest.raises(GithubException, match="boom"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_called_once()  # no blind retry

    def test_release_existing_at_the_wrong_sha_re_raises(self) -> None:
        # Someone else's release under the same tag is not our success.
        repo = _publishable_repo()

        def _foreign_release(*args: object, **kwargs: object) -> None:
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = tag_ref("e" * 40)
            repo.get_release.side_effect = None
            repo.get_release.return_value = MagicMock(
                html_url="https://x/releases/9.1.1")
            raise GithubException(502, "bad gateway", {})

        repo.create_git_release.side_effect = _foreign_release
        with pytest.raises(GithubException, match="bad gateway"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_called_once()


class TestTagCreationRace:
    """Create refs/tags/{tag} atomically BEFORE the release, so a
    second writer racing us into create_git_release cannot bind the tag
    to a different SHA. A conflict at a different SHA quarantines; a
    conflict at the approved SHA is a resume and continues.

    Two vantage points are tested independently:
    1. Plan-level: revalidation catches a tag that appeared between
       approval and execution (both a snipe and a partial-publish resume).
    2. Ref-level: :func:`_ensure_tag_at_sha` catches a snipe that lands
       in the narrower window between plan return and ref creation.
    """

    def test_atomic_tag_ref_created_before_release(self) -> None:
        # Order matters: the ref must be created FIRST so the tag exists
        # at the approved SHA before create_git_release runs.
        repo = _publishable_repo()
        publish_release(gh_mock(repo), _POLICY, branch="9.1",
                        actor="madolson", expected_tag="9.1.1",
                        expected_sha=MERGE_SHA)
        repo.create_git_ref.assert_called_once_with(
            ref="refs/tags/9.1.1", sha=MERGE_SHA,
        )
        repo.create_git_release.assert_called_once()

    def test_plan_refuses_tag_snipe_at_a_different_sha(self) -> None:
        # A tag that appeared between approval and execution at a DIFFERENT
        # SHA is a snipe: plan_publication's tag check refuses before any
        # release is created. (The upstream readiness gate also catches
        # this; either refusal is acceptable: a wrong-SHA tag never lets
        # publication proceed.)
        repo = _publishable_repo()
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref("e" * 40)
        with pytest.raises(ReleaseControlError,
                           match="another writer created this tag|"
                                 "unshippable|not ready to publish"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_not_called()

    def test_ensure_tag_helper_refuses_snipe_at_a_different_sha(self) -> None:
        # Direct test of _ensure_tag_at_sha: 422 from create_git_ref plus
        # a tag now resolving to someone else's commit == snipe. This is
        # the narrow window plan_publication cannot see (a snipe landing
        # AFTER revalidation, BEFORE ref creation).
        repo = MagicMock()
        repo.create_git_ref.side_effect = GithubException(
            422, "reference already exists", {},
        )
        repo.get_git_ref.return_value = tag_ref("e" * 40)
        plan = _plan(tag="9.1.1", sha=MERGE_SHA)
        with pytest.raises(ReleaseControlError,
                           match="another writer created this tag"):
            _ensure_tag_at_sha(repo, plan)

    def test_ensure_tag_helper_resumes_at_approved_sha(self) -> None:
        # Direct test: 422 with the tag pointing at the APPROVED SHA is a
        # partial-publish resume: proceed without raising.
        repo = MagicMock()
        repo.create_git_ref.side_effect = GithubException(
            422, "reference already exists", {},
        )
        repo.get_git_ref.return_value = tag_ref()
        plan = _plan(tag="9.1.1", sha=MERGE_SHA)
        _ensure_tag_at_sha(repo, plan)  # must not raise

    def test_ensure_tag_helper_non_422_reraises(self) -> None:
        # A 403 (insufficient scope, ref-protection reject) is not a
        # conflict; it must propagate so the operator knows the write
        # itself was refused, not that a tag already existed somewhere.
        repo = MagicMock()
        repo.create_git_ref.side_effect = GithubException(403, "denied", {})
        plan = _plan(tag="9.1.1", sha=MERGE_SHA)
        with pytest.raises(GithubException, match="denied"):
            _ensure_tag_at_sha(repo, plan)

    def test_tag_at_approved_sha_resumes_the_release(self) -> None:
        # End-to-end resume: plan_publication sees the tag at the approved
        # SHA (partial-publish resume state) and does not refuse; publish
        # proceeds and completes. The resume is only allowed because
        # this repo's qualification evidence exists and passed; readiness
        # is re-proven live, never inferred from the tag's existence (see
        # TestResumeRequiresReadiness for the refusal side).
        repo = _publishable_repo()
        # Tag exists at approved SHA from the outset (a crash after
        # STAGE 1 succeeded but before STAGE 2 ran).
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref()
        # No release yet: that is the state we are resuming.
        repo.get_release.side_effect = GithubException(404, "no release", {})
        # STAGE 1 hits the 422 path; the helper sees the approved SHA and
        # proceeds. STAGE 2 creates the release.
        repo.create_git_ref.side_effect = GithubException(
            422, "reference already exists", {},
        )
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"
        repo.create_git_release.assert_called_once()

    def test_release_exists_resume_still_posts_the_receipt(self) -> None:
        # A crash after STAGE 2 but before the tracker comment: the
        # release already exists at the approved SHA. Recovery treats the
        # create failure as a lost response and STILL posts the receipt so
        # the tracker trail is complete. create_comment fires TWICE: once
        # for the identity binding (from plan_publication's revalidation)
        # and once for the publication receipt: so we filter for the
        # publication marker specifically.
        repo = _publishable_repo()

        def _release_already_exists(*args: object, **kwargs: object) -> None:
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = tag_ref()
            repo.get_release.side_effect = None
            repo.get_release.return_value = MagicMock(
                html_url="https://x/releases/9.1.1")
            new_latest = MagicMock(tag_name="9.1.1", draft=False, prerelease=False)
            repo.get_latest_release.return_value = new_latest
            repo.get_releases.return_value = [new_latest]
            raise GithubException(422, "already_exists", {})

        repo.create_git_release.side_effect = _release_already_exists
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"
        receipt_bodies = [
            call.kwargs.get("body", "")
            for call in repo.get_issue.return_value.create_comment.call_args_list
            if _PUBLICATION_MARKER in call.kwargs.get("body", "")
        ]
        assert len(receipt_bodies) == 1

    def test_publication_receipt_is_idempotent(self) -> None:
        # A resume where the receipt was already posted must not double-post:
        # trusted_comments returns the earlier receipt, so create_comment
        # is skipped for the publication marker (the binding receipt is a
        # separate concern from write_binding and may still fire).
        repo = _publishable_repo()
        existing = MagicMock(body=f"{_PUBLICATION_MARKER}\nearlier receipt")
        # trusted_comments is called in two places: once by write_binding
        # (via issue_mod) and once by _post_publication_receipt. Both look
        # for markers, but only the publication receipt is what we want to
        # suppress. Return the existing receipt from both call sites.
        with patch.object(issue_mod, "trusted_comments",
                          return_value=[existing]):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        # No comment body carrying the publication marker was posted.
        for call in repo.get_issue.return_value.create_comment.call_args_list:
            assert _PUBLICATION_MARKER not in call.kwargs.get("body", "")


class TestPublicationReceiptContent:
    """Receipt write side: the receipt records the tag/SHA carrier line (the
    field the verifier requires) plus the plan digest and the controller
    commit/run that executed the write (evidence for a human; optional on
    read-back so legacy receipts keep verifying)."""

    def _receipt_body(self, repo: MagicMock) -> str:
        bodies = [
            call.kwargs.get("body", "")
            for call in repo.get_issue.return_value.create_comment.call_args_list
            if _PUBLICATION_MARKER in call.kwargs.get("body", "")
        ]
        assert len(bodies) == 1
        return bodies[0]

    def test_receipt_records_digest_controller_and_run(self) -> None:
        repo = _publishable_repo()
        publish_release(gh_mock(repo), _POLICY, branch="9.1",
                        actor="madolson", expected_tag="9.1.1",
                        expected_sha=MERGE_SHA, controller_sha="f" * 40,
                        run_url="https://x/actions/runs/9")
        receipt = self._receipt_body(repo)
        assert f"Published **9.1.1** at `{MERGE_SHA}`" in receipt
        assert re.search(r"Plan digest: `[0-9a-f]{64}`", receipt)
        assert f"Controller commit: `{'f' * 40}`" in receipt
        assert "Controller run: https://x/actions/runs/9" in receipt

    def test_receipt_omits_controller_lines_outside_actions(self) -> None:
        # controller_sha and run_url are "" outside Actions: the lines are
        # omitted rather than recording empty values; the digest is always
        # computable and always recorded.
        repo = _publishable_repo()
        publish_release(gh_mock(repo), _POLICY, branch="9.1",
                        actor="madolson", expected_tag="9.1.1",
                        expected_sha=MERGE_SHA)
        receipt = self._receipt_body(repo)
        assert "Plan digest: `" in receipt
        assert "Controller commit:" not in receipt
        assert "Controller run:" not in receipt


class TestUnreceiptedReleaseResume:
    """Receipt resume seam: a publish that crashed after creating the release
    but before the receipt write leaves reconcile reporting the
    unreceipted-release alert. Re-running the publish workflow is the
    documented recovery, so that ONE alert must not refuse the resume;
    a receipt naming a different SHA is not our crash and still refuses."""

    def _crashed_after_release_repo(self, tracking: MagicMock) -> MagicMock:
        repo = _ready_repo()
        repo.get_issues.return_value = [tracking]
        # The crash left the world with the tag at the approved SHA AND
        # the release created, but no receipt on the tracker.
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref()
        repo.get_release.side_effect = None
        repo.get_release.return_value = MagicMock(
            prerelease=False, draft=False,
            html_url="https://x/releases/9.1.1", published_at=None)
        return repo

    def test_unreceipted_release_at_the_candidate_resumes_planning(self) -> None:
        repo = self._crashed_after_release_repo(tracker())
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson")
        assert plan.tag == "9.1.1"
        assert plan.sha == MERGE_SHA
        # Readiness re-proof still holds on this resume: the plan binds the re-proven
        # qualification run, never run_id 0.
        assert plan.qualification_run_id == 900

    def test_sha_mismatched_receipt_refuses_the_resume(self) -> None:
        # A receipt recording a DIFFERENT SHA is evidence of another
        # publish (or tampering), not of our crash: never resumable.
        repo = self._crashed_after_release_repo(
            tracker(comments=[bot_receipt(sha="d" * 40)]))
        with pytest.raises(ReleaseControlError, match="not READY"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                             actor="madolson")


class TestResumeRequiresReadiness:
    """A same-SHA tag excuses exactly one refusal, the TAG-EXISTS
    check (STAGE 1 of a crashed publish already claimed the tag). It must
    never stand in for readiness itself: an out-of-band writer
    pre-creating the tag at the public candidate SHA must not obtain an
    approvable plan carrying zero qualification evidence."""

    def _repo_with_tag_at_candidate(self, qual_runs: list) -> MagicMock:
        """A READY-shaped repo except the tag already exists at the
        candidate SHA and the qualification evidence is caller-chosen."""
        repo = _with_tracker(_ready_repo(qual_runs=qual_runs))
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = tag_ref()
        return repo

    def test_missing_qualification_refuses_the_resume(self) -> None:
        # The exact attack this closes: pre-create the tag at the candidate SHA
        # with NO qualification run anywhere. The old behavior planned
        # anyway with qualification_run_id=0 bound into the digest.
        repo = self._repo_with_tag_at_candidate(qual_runs=[])
        with pytest.raises(ReleaseControlError) as excinfo:
            plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                             actor="madolson")
        assert ("tag 9.1.1 exists at the candidate but the release is "
                "not READY") in str(excinfo.value)
        assert "no qualification evidence" in str(excinfo.value)

    def test_failed_qualification_refuses_the_resume(self) -> None:
        repo = self._repo_with_tag_at_candidate(
            qual_runs=[qualification_run(conclusion="failure")])
        with pytest.raises(ReleaseControlError) as excinfo:
            plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                             actor="madolson")
        assert ("tag 9.1.1 exists at the candidate but the release is "
                "not READY") in str(excinfo.value)
        assert "did not pass" in str(excinfo.value)

    def test_pending_qualification_refuses_the_resume(self) -> None:
        # Still-running evidence is not evidence yet; the resume waits.
        repo = self._repo_with_tag_at_candidate(
            qual_runs=[qualification_run(status="in_progress")])
        with pytest.raises(ReleaseControlError) as excinfo:
            plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                             actor="madolson")
        assert "not READY" in str(excinfo.value)
        assert "still executing" in str(excinfo.value)

    def test_ready_resume_plans_and_binds_the_real_qualification_run(self) -> None:
        # The legitimate resume still plans, and the plan (hence the
        # digest the approver signs off on) binds the real qualification
        # run instead of run_id 0: the closed hole was precisely an approvable
        # digest over qualification_run_id=0.
        repo = self._repo_with_tag_at_candidate(
            qual_runs=[qualification_run()])
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson")
        assert plan.tag == "9.1.1"
        assert plan.sha == MERGE_SHA
        assert plan.qualification_run_id == 900
        assert plan.qualification_url == "https://x/qruns/900"


class TestLatestPointerRace:
    """Publications on two branches interleaving must not both take
    the latest pointer. The enumeration-based decision is racy on the
    wire; post-create pointer verify catches divergence."""

    def test_plan_reflects_enumeration_not_a_stale_manual_pointer(self) -> None:
        # A maintainer moved the mutable latest pointer back to 9.0.x
        # manually. Enumerating the release list still shows 9.2.0 as the
        # top, so an 8.0.x patch does not pretend to become latest.
        repo = _with_tracker(_ready_repo())
        newer = MagicMock(tag_name="9.2.0", draft=False, prerelease=False)
        older = MagicMock(tag_name="9.0.5", draft=False, prerelease=False)
        # The (mutable) latest pointer sits at the older release
        # (unreliable evidence: a maintainer moved it back).
        repo.get_latest_release.return_value = older
        repo.get_releases.return_value = [older, newer]
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson")
        assert plan.make_latest == "false"

    def test_pointer_verify_flags_a_race_after_make_latest_true(self) -> None:
        # We publish 9.1.1 with make_latest=true. Between our create and
        # our verify, another line's publish moves the pointer elsewhere.
        # The post-verify catches it as CRITICAL, so the operator sees a
        # divergent world before downstream work proceeds.
        repo = _publishable_repo()
        # Override the post-create pointer to a foreign tag despite
        # make_latest=true in the plan.
        original_side = repo.create_git_release.side_effect

        def _pointer_moves_elsewhere(*args: object, **kwargs: object) -> MagicMock:
            release = original_side(*args, **kwargs)  # apply STAGE-2 defaults
            foreign = MagicMock(tag_name="8.0.5", draft=False, prerelease=False)
            repo.get_latest_release.return_value = foreign
            repo.get_releases.return_value = [foreign]
            return release

        repo.create_git_release.side_effect = _pointer_moves_elsewhere
        with pytest.raises(ReleaseControlError,
                           match="CRITICAL.*latest pointer"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)

    def test_pointer_verify_flags_a_race_after_make_latest_false(self) -> None:
        # An older-line 8.0.5 with make_latest=false: but GitHub's default
        # took effect anyway because a bug/race made the pointer move. The
        # verify catches it (defense in depth for the enumeration decision).
        older_notes = (
            "Valkey 8.0.5  -  Released today\n=====\nUpgrade urgency LOW.\n"
        )
        older_version_h = (
            '#define VALKEY_VERSION "8.0.5"\n'
            "#define VALKEY_VERSION_NUM 0x00080005\n"
            '#define VALKEY_RELEASE_STAGE "ga"\n'
        )
        # The current top line is 9.1.0: an 8.0.5 patch must NOT set latest.
        newer = MagicMock(tag_name="9.1.0", draft=False, prerelease=False)
        older_final = MagicMock(tag_name="8.0.4", draft=False, prerelease=False)
        repo = repo_mock(
            pulls=[notes_pr(head_ref="agent/release-cut/8.0.5-ga")],
            issues=[tracker(branch="8.0")],
            tags=["8.0.4", "9.1.0"],
            qual_runs=[qualification_run(tag="8.0.5")],
        )
        repo.get_workflow.return_value.get_runs.return_value = [
            qualification_run(tag="8.0.5"),
        ]
        repo.get_contents.side_effect = _contents_for(older_version_h, older_notes)
        repo.get_releases.return_value = [older_final, newer]
        repo.get_latest_release.return_value = newer
        release = MagicMock(html_url="https://x/releases/8.0.5")
        repo.create_git_release.return_value = release
        repo.get_issue.return_value = repo.get_issues.return_value[0]

        def _tag_after_create(*args: object, **kwargs: object) -> MagicMock:
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = tag_ref()
            # A bug elsewhere lets the pointer drift to us despite the plan.
            stray = MagicMock(tag_name="8.0.5", draft=False, prerelease=False)
            repo.get_latest_release.return_value = stray
            return release

        repo.create_git_release.side_effect = _tag_after_create
        with pytest.raises(ReleaseControlError,
                           match="CRITICAL.*pointer moved to an older line"):
            publish_release(gh_mock(repo), _POLICY, branch="8.0",
                            actor="madolson", expected_tag="8.0.5",
                            expected_sha=MERGE_SHA)


class TestUnconfiguredBranchRefusal:
    """Plan and publish refuse an unconfigured branch BEFORE any API
    call the branch would otherwise reach (repo lookup, status compute)."""

    def test_plan_refuses_before_repo_access(self) -> None:
        gh = MagicMock()
        with pytest.raises(ReleaseControlError,
                           match="not a configured release branch"):
            plan_publication(gh, _POLICY, branch="6.9", actor="madolson")
        gh.get_repo.assert_not_called()
        gh.get_organization.assert_not_called()

    def test_publish_refuses_before_repo_access(self) -> None:
        gh = MagicMock()
        with pytest.raises(ReleaseControlError,
                           match="not a configured release branch"):
            publish_release(gh, _POLICY, branch="6.9", actor="madolson",
                            expected_tag="6.9.0", expected_sha=MERGE_SHA)
        gh.get_repo.assert_not_called()

    def test_wrong_shape_branch_is_also_refused(self) -> None:
        # 'main' passes the "not in branches" check but fails the shape
        # check upstream: validate_release_branch composes both.
        gh = MagicMock()
        with pytest.raises(ReleaseControlError, match="not a release branch"):
            plan_publication(gh, _POLICY, branch="main", actor="madolson")
        gh.get_repo.assert_not_called()


class TestRCPublication:
    def test_rc_plan_is_prerelease_and_never_latest(self) -> None:
        rc_notes = (
            "Valkey 9.2.0-rc1  -  Released today\n=====\nUpgrade urgency LOW.\n"
        )
        rc_version_h = (
            '#define VALKEY_VERSION "9.2.0"\n'
            "#define VALKEY_VERSION_NUM 0x00090200\n"
            '#define VALKEY_RELEASE_STAGE "rc1"\n'
        )
        repo = repo_mock(
            pulls=[notes_pr(head_ref="agent/release-cut/9.2.0-rc1")],
            issues=[tracker(branch="9.2")],
            tags=["9.1.0"],
            qual_runs=[qualification_run(tag="9.2.0-rc1")],
        )
        repo.get_workflow.return_value.get_runs.return_value = [
            qualification_run(tag="9.2.0-rc1")
        ]
        repo.get_contents.side_effect = _contents_for(rc_version_h, rc_notes)
        repo.get_latest_release.return_value = MagicMock(tag_name="9.1.0")
        policy = make_policy(branches=("9.2",))

        plan = plan_publication(gh_mock(repo), policy, branch="9.2", actor="madolson")
        assert plan.tag == "9.2.0-rc1"
        assert plan.prerelease
        assert plan.make_latest == "false"


def _env_repo(protection_rules: "list | None" = None, *,
              can_admins_bypass: bool = False, exists: bool = True,
              deployment_branch_policy: "dict | None | str" = "default") -> MagicMock:
    repo = MagicMock()
    if not exists:
        repo.get_environment.side_effect = GithubException(404, "missing", {})
        return repo
    env = MagicMock()
    env.raw_data = {
        "protection_rules": protection_rules if protection_rules is not None else [
            {"type": "required_reviewers", "prevent_self_review": True,
             "reviewers": [{"type": "Team"}]},
        ],
        "can_admins_bypass": can_admins_bypass,
        "deployment_branch_policy": (
            {"protected_branches": True, "custom_branch_policies": False}
            if deployment_branch_policy == "default" else deployment_branch_policy
        ),
    }
    repo.get_environment.return_value = env
    return repo


_ABSENT_KEY = object()  # parametrize sentinel: delete the key from raw_data


class TestEnvironmentProtection:
    def _gh(self, repo: MagicMock) -> MagicMock:
        gh = MagicMock()
        gh.get_repo.return_value = repo
        return gh

    def test_fully_protected_environment_passes(self) -> None:
        ensure_environment_protected(self._gh(_env_repo()), _POLICY, "o/agent")

    def test_missing_environment_is_refused(self) -> None:
        with pytest.raises(ReleaseControlError, match="does not exist"):
            ensure_environment_protected(self._gh(_env_repo(exists=False)),
                                         _POLICY, "o/agent")

    # "No required reviewers" refusals: the exact live state GitHub
    # auto-creates (no rules, bypass on), and a required_reviewers rule
    # whose reviewers were all removed - a rule with nobody in it must
    # count as "no required reviewers", not as protection.
    @pytest.mark.parametrize(("rules", "bypass"), [
        pytest.param([], True, id="no-rules-at-all"),
        pytest.param([{"type": "required_reviewers",
                       "prevent_self_review": True, "reviewers": []}],
                     False, id="rule-with-empty-reviewers"),
    ])
    def test_missing_reviewer_protection_is_refused(
            self, rules: list, bypass: bool) -> None:
        repo = _env_repo(rules, can_admins_bypass=bypass)
        with pytest.raises(ReleaseControlError, match="no required reviewers"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    # Self-review refusals: absence of prevent_self_review must read as
    # "not prevented" (older API payloads omit the key), an explicit False
    # is refused, and EVERY reviewer rule must prevent self-review - an
    # approval can satisfy whichever rule is laxest.
    @pytest.mark.parametrize("rules", [
        pytest.param([{"type": "required_reviewers",
                       "reviewers": [{"type": "User"}]}],
                     id="absent-key-fails-closed"),
        pytest.param([{"type": "required_reviewers",
                       "prevent_self_review": False,
                       "reviewers": [{"type": "User"}]}],
                     id="explicit-false"),
        pytest.param([
            {"type": "required_reviewers", "prevent_self_review": True,
             "reviewers": [{"type": "Team"}]},
            {"type": "required_reviewers",
             "reviewers": [{"type": "User"}]},  # key absent on this rule
        ], id="one-lax-rule-among-compliant"),
    ])
    def test_self_review_not_prevented_is_refused(self, rules: list) -> None:
        repo = _env_repo(rules)
        with pytest.raises(ReleaseControlError, match="self-review"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_empty_200_payload_fails_closed_on_every_dimension(self) -> None:
        # An environment endpoint answering 200 with {} (proxy stripping,
        # API drift) carries no evidence of protection; both the reviewer
        # requirement and the bypass default must fail closed.
        repo = MagicMock()
        env = MagicMock()
        env.raw_data = {}
        repo.get_environment.return_value = env
        with pytest.raises(ReleaseControlError) as excinfo:
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")
        assert "no required reviewers" in str(excinfo.value)
        assert "bypass" in str(excinfo.value)

    def test_absent_can_admins_bypass_key_fails_closed(self) -> None:
        repo = _env_repo()
        del repo.get_environment.return_value.raw_data["can_admins_bypass"]
        with pytest.raises(ReleaseControlError, match="bypass"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_team_only_reviewers_satisfy_the_reviewer_requirement(self) -> None:
        # A Team reviewer is a real gate (any team member can approve, and
        # prevent_self_review still applies); it must be accepted, not
        # refused for lacking a User entry.
        repo = _env_repo([{"type": "required_reviewers",
                           "prevent_self_review": True,
                           "reviewers": [{"type": "Team", "reviewer": {"slug": "core-team"}}]}])
        ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_admin_bypass_is_refused(self) -> None:
        repo = _env_repo(can_admins_bypass=True)
        with pytest.raises(ReleaseControlError, match="bypass"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    # Deployment-branch-policy refusals: a null policy means ANY branch
    # (including an attacker's topic branch) can deploy to the gated
    # environment; an absent key fails closed the same way, as does a
    # policy carrying neither restriction.
    @pytest.mark.parametrize("policy_value", [
        pytest.param(None, id="null-policy"),
        pytest.param(_ABSENT_KEY, id="absent-key-fails-closed"),
        pytest.param({"protected_branches": False,
                      "custom_branch_policies": False},
                     id="neither-restriction"),
    ])
    def test_unrestricted_deployment_branch_policy_is_refused(
            self, policy_value: object) -> None:
        if policy_value is _ABSENT_KEY:
            repo = _env_repo()
            del repo.get_environment.return_value.raw_data["deployment_branch_policy"]
        else:
            repo = _env_repo(deployment_branch_policy=policy_value)  # type: ignore[arg-type]
        with pytest.raises(ReleaseControlError,
                           match="not restricted to specific branches"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    @pytest.mark.parametrize("policy_value", [
        pytest.param({"protected_branches": False,
                      "custom_branch_policies": True},
                     id="custom-branch-policies"),
        pytest.param({"protected_branches": True,
                      "custom_branch_policies": False},
                     id="protected-branches"),
    ])
    def test_either_branch_restriction_satisfies(self, policy_value: dict) -> None:
        repo = _env_repo(deployment_branch_policy=policy_value)
        ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_fork_user_policy_skips_the_check_entirely(self) -> None:
        gh = MagicMock()
        policy = make_policy(repo="sarthakaggarwal97/valkey",
                             authorized_team="user:sarthakaggarwal97")
        ensure_environment_protected(gh, policy, "o/agent")
        gh.get_repo.assert_not_called()


class TestShaBinding:
    def test_expected_sha_mismatch_refuses_before_any_write(self) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="candidate changed"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha="f" * 40)
        repo.create_git_release.assert_not_called()

    @pytest.mark.parametrize("mangled_sha", [
        MERGE_SHA.upper(),        # case-changed copy of the same commit
        MERGE_SHA[:12],           # abbreviated: prefixes must not bind
        f"{MERGE_SHA}\n",         # trailing newline from a copy-paste
        f" {MERGE_SHA}",          # leading whitespace
    ])
    def test_any_deviation_in_the_expected_sha_refuses(self, mangled_sha: str) -> None:
        # The binding must be an exact string compare of the full SHA: no
        # case folding, no prefix acceptance, no whitespace tolerance.
        # Refusing the uppercase twin of the right commit is deliberate
        # fail-closed behavior; the operator re-copies the exact value.
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="candidate changed"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=mangled_sha)
        repo.create_git_release.assert_not_called()

    @pytest.mark.parametrize("mangled_tag", ["9.1.1\n", " 9.1.1", "9.1.1 "])
    def test_whitespace_in_the_expected_tag_refuses(self, mangled_tag: str) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="approval was for"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag=mangled_tag,
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_not_called()

    def test_plan_approved_for_one_branch_cannot_execute_on_another(self) -> None:
        # A plan produced for 9.1 (tag 9.1.1) executed with --branch 8.0:
        # even when the 9.1 tracker leaks through the issue lookup (the
        # mock ignores label filters, mimicking a mislabeled tracker), the
        # notes-PR version must not match the 8.0 line, so revalidation
        # refuses before the tag/SHA binding is even consulted.
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError):
            publish_release(gh_mock(repo), _POLICY, branch="8.0",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_not_called()


def test_unattended_planning_skips_the_actor_check_only() -> None:
    # The controller dispatches validate as the Actions bot; planning is
    # read-only and the human gate stays at approval + execute.
    repo = _with_tracker(_ready_repo())
    gh = gh_mock(repo, member=False)  # the bot is in no team
    plan = plan_publication(gh, _POLICY, branch="9.1", actor="github-actions[bot]",
                            skip_authorization=True)
    assert plan.tag == "9.1.1"
    with pytest.raises(NotAuthorizedError):
        plan_publication(gh, _POLICY, branch="9.1", actor="github-actions[bot]")


def _plan(tag: str = "9.1.1", **overrides: object) -> PublishPlan:
    values: "dict[str, object]" = dict(
        tag=tag, sha=MERGE_SHA, prerelease=False,
        make_latest="true", body="notes body", issue_number=11,
        tracker_url="https://x/issues/11",
        qualification_url="https://x/qruns/900")
    values.update(overrides)
    return PublishPlan(**values)  # type: ignore[arg-type]


def _ruleset_repo(rulesets: "list[dict]", details: "dict[int, dict]") -> MagicMock:
    """A repo mock whose rulesets endpoints serve the given payloads."""
    repo = MagicMock()
    repo.url = "https://api.github.com/repos/o/r"

    def _request(verb: str, url: str) -> "tuple[dict, object]":
        if url.endswith("/rulesets"):
            return {}, rulesets
        return {}, details[int(url.rsplit("/", 1)[1])]

    repo._requester.requestJsonAndCheck.side_effect = _request
    return repo


_FULL_RULES = [{"type": "deletion"}, {"type": "update"}, {"type": "creation"}]


class TestTagRulesetProbe:
    def test_full_rules_with_zero_bypasses_is_protected(self) -> None:
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES, "bypass_actors": []}},
        )
        verdict = tag_ruleset_protected(repo, "9.1.1")
        assert verdict.protected is True
        assert verdict.bypass_integration_ids == ()

    def test_non_fast_forward_counts_as_the_update_rule(self) -> None:
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": [{"type": "deletion"}, {"type": "non_fast_forward"},
                           {"type": "creation"}],
                 "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "9.1.1").protected is True

    # A matching ruleset with no deletion/update rules restricts nothing,
    # a deletion rule alone still lets the tag be MOVED, and deletion plus
    # update without a creation rule still lets any writer PRE-CREATE the
    # release tag (the snipe): protection needs all three halves. The
    # creation rule stays required even now that an Integration bypass is
    # tolerated: the bypass narrows WHO can create, the rule is what
    # restricts creation at all.
    @pytest.mark.parametrize("rules", [
        pytest.param([], id="ruleless"),
        pytest.param([{"type": "deletion"}], id="deletion-alone"),
        pytest.param([{"type": "deletion"}, {"type": "update"}],
                     id="missing-creation-rule"),
        pytest.param([{"type": "creation"}], id="creation-alone"),
    ])
    def test_insufficient_rules_are_not_protected(self, rules: list) -> None:
        # Even with the workable App-only bypass configured, missing rules
        # restrict nothing: the bypass never substitutes for the rules.
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": rules,
                 "bypass_actors": [{"actor_id": 5,
                                    "actor_type": "Integration",
                                    "bypass_mode": "always"}]}},
        )
        verdict = tag_ruleset_protected(repo, "9.1.1")
        assert verdict.protected is False
        assert verdict.bypass_integration_ids == ()

    def test_single_integration_bypass_passes_with_its_id_surfaced(self) -> None:
        # REACHABILITY: a creation rule with zero bypasses also blocks the
        # publishing App, so the workable protected config is full rules
        # plus the publishing App as the sole Integration bypass. The
        # verdict surfaces the App id so the approval evidence names
        # exactly which App can bypass.
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES,
                 "bypass_actors": [{"actor_id": 5, "actor_type": "Integration",
                                    "bypass_mode": "always"}]}},
        )
        verdict = tag_ruleset_protected(repo, "9.1.1")
        assert verdict.protected is True
        assert verdict.bypass_integration_ids == (5,)

    # HUMAN (and key) bypasses still defeat the claim. A Team, role,
    # org-admin, user, or deploy-key bypass means people can still create,
    # move, or delete the tag, so "immutable" would overstate what was
    # verified: any non-Integration bypass fails closed, regardless of
    # bypass_mode, even when an Integration bypass rides alongside it.
    @pytest.mark.parametrize("actor_type", [
        "Team", "RepositoryRole", "OrganizationAdmin", "User", "DeployKey",
    ])
    @pytest.mark.parametrize("bypass_mode", ["always", "pull_request"])
    def test_any_human_bypass_actor_defeats_the_claim(self, actor_type: str,
                                                      bypass_mode: str) -> None:
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES,
                 "bypass_actors": [{"actor_id": 5, "actor_type": "Integration",
                                    "bypass_mode": "always"},
                                   {"actor_id": 9, "actor_type": actor_type,
                                    "bypass_mode": bypass_mode}]}},
        )
        verdict = tag_ruleset_protected(repo, "9.1.1")
        assert verdict.protected is False
        # The App id is NOT surfaced from a ruleset that did not qualify.
        assert verdict.bypass_integration_ids == ()

    def test_integration_bypass_without_a_nameable_id_defeats_the_claim(self) -> None:
        # An App the evidence cannot NAME is an App the approver cannot
        # check: fail closed rather than claim protection anonymously.
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES,
                 "bypass_actors": [{"actor_type": "Integration",
                                    "bypass_mode": "always"}]}},
        )
        assert tag_ruleset_protected(repo, "9.1.1").protected is False

    def test_invisible_bypass_data_degrades_to_unknown(self) -> None:
        # No bypass_actors key in the payload: a human bypass cannot be
        # ruled out, so the verdict is unknown, never protected.
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES}},
        )
        assert tag_ruleset_protected(repo, "9.1.1").protected is None

    def test_excluded_tag_is_not_protected(self) -> None:
        # Mirrors upstream: the ruleset excludes 1-7.* tags, so a 7.x
        # release must never claim immutability.
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {
                "include": ["~ALL"],
                "exclude": ["refs/tags/[1-7].*"]}},
                "rules": _FULL_RULES, "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "7.2.11").protected is False
        assert tag_ruleset_protected(repo, "9.1.1").protected is True

    def test_no_tag_ruleset_is_not_protected(self) -> None:
        repo = _ruleset_repo(
            [{"id": 2, "target": "branch", "enforcement": "active"},
             {"id": 3, "target": "tag", "enforcement": "disabled"}],
            {},
        )
        assert tag_ruleset_protected(repo, "9.1.1").protected is False

    def test_api_failure_is_unknown(self) -> None:
        repo = MagicMock()
        repo.url = "https://api.github.com/repos/o/r"
        repo._requester.requestJsonAndCheck.side_effect = GithubException(
            403, "forbidden", {},
        )
        assert tag_ruleset_protected(repo, "9.1.1").protected is None


class TestTagRulesetProbePinsPyGithubSurface:
    """The probe rides PyGithub's PRIVATE ``repo._requester`` because the
    pinned PyGithub exposes no supported wrapper for the rulesets
    endpoints. These tests run the probe through a REAL
    ``github.Repository.Repository`` (never a MagicMock, which would
    tolerate any attribute name) fed raw API response shapes, so a
    PyGithub upgrade that renames ``_requester``, changes
    ``requestJsonAndCheck``'s call shape, or alters Repository
    construction surfaces HERE as a test failure instead of the probe
    silently degrading every verdict to None ("not protected")."""

    class _RawRequester:
        """Duck-typed requester serving the documented raw JSON shapes of
        GET /repos/{o}/{r}/rulesets and /rulesets/{id}."""

        # Attributes Repository construction reads off the requester.
        base_url = "https://api.github.com"
        is_not_lazy = False

        def __init__(self, listing: list, details: "dict[int, dict]") -> None:
            self.listing = listing
            self.details = details
            self.calls: "list[tuple[str, str]]" = []

        def requestJsonAndCheck(self, verb: str, url: str) -> "tuple[dict, object]":
            self.calls.append((verb, url))
            if url.endswith("/rulesets"):
                return {}, self.listing
            return {}, self.details[int(url.rsplit("/", 1)[1])]

    def _real_repo(self, requester: "_RawRequester") -> "object":
        from github.Repository import Repository
        return Repository(
            requester=requester,  # type: ignore[arg-type]
            headers={},
            attributes={"url": "https://api.github.com/repos/o/valkey"},
            completed=True,
        )

    def test_still_exposes_no_supported_ruleset_wrapper(self) -> None:
        # The WHY comment in publish.py claims the pinned PyGithub has no
        # supported rulesets accessor; this pins that claim so a version
        # bump that ADDS one prompts migrating off the private requester.
        from github.Repository import Repository
        from github.Requester import Requester
        assert not [attr for attr in dir(Repository) if "ruleset" in attr.lower()], (
            "PyGithub now ships a rulesets accessor on Repository: migrate "
            "tag_ruleset_protected off the private _requester probe"
        )
        # And the raw method the probe rides still exists by that name, so
        # a PyGithub rename fails HERE instead of silently degrading the
        # probe to None on every call.
        assert hasattr(Requester, "requestJsonAndCheck")

    def test_probe_through_a_real_repository_answers_protected(self) -> None:
        requester = self._RawRequester(
            [{"id": 42, "target": "tag", "enforcement": "active"}],
            {42: {
                "id": 42, "target": "tag", "enforcement": "active",
                "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
                "rules": [{"type": "creation"}, {"type": "update"},
                          {"type": "deletion"}],
                "bypass_actors": [],
            }},
        )
        repo = self._real_repo(requester)
        # A degradation to None here (private-attribute rename, call-shape
        # change) is exactly the silent "unprotected" downgrade this test
        # exists to catch: assert the True verdict, not just non-error.
        assert tag_ruleset_protected(repo, "9.1.1").protected is True
        assert requester.calls == [
            ("GET", "https://api.github.com/repos/o/valkey/rulesets"),
            ("GET", "https://api.github.com/repos/o/valkey/rulesets/42"),
        ]

    def test_probe_through_a_real_repository_surfaces_the_app_bypass(self) -> None:
        # The workable production shape: full rules, the publishing App as
        # the sole Integration bypass. Protected, with the App id named.
        requester = self._RawRequester(
            [{"id": 7, "target": "tag", "enforcement": "active"}],
            {7: {
                "id": 7, "target": "tag", "enforcement": "active",
                "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
                "rules": [{"type": "creation"}, {"type": "update"},
                          {"type": "deletion"}],
                "bypass_actors": [{"actor_type": "Integration", "actor_id": 1}],
            }},
        )
        verdict = tag_ruleset_protected(self._real_repo(requester), "9.1.1")
        assert verdict.protected is True
        assert verdict.bypass_integration_ids == (1,)

    def test_probe_through_a_real_repository_answers_unprotected(self) -> None:
        requester = self._RawRequester(
            [{"id": 7, "target": "tag", "enforcement": "active"}],
            {7: {
                "id": 7, "target": "tag", "enforcement": "active",
                "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
                "rules": [{"type": "creation"}, {"type": "update"},
                          {"type": "deletion"}],
                "bypass_actors": [{"actor_type": "Team", "actor_id": 9}],
            }},
        )
        verdict = tag_ruleset_protected(self._real_repo(requester), "9.1.1")
        assert verdict.protected is False


class TestPlanSummaryHonesty:
    def test_protected_tag_states_only_the_strengthened_claim(self) -> None:
        # The protected claim names exactly what the probe
        # verified: creation, update, and deletion restrictions with zero
        # bypass actors.
        summary = render_plan_summary(_plan(tag_protected=True))
        assert ("The created tag is ruleset-protected: tag creation, update, "
                "and deletion are restricted and no bypass actors are "
                "configured, so it cannot be moved or deleted.") in summary
        assert "NOT ruleset-protected" not in summary
        assert "creates the release tag" in summary
        assert "cannot be moved or deleted. Verify" not in summary

    def test_protected_tag_with_app_bypass_names_the_integration_ids(self) -> None:
        # The approver must see exactly which App can bypass, so they can
        # confirm it is the publishing App and nothing else.
        summary = render_plan_summary(_plan(
            tag_protected=True, tag_bypass_integration_ids=(5,)))
        assert ("the only bypass is GitHub App (Integration) id(s) 5. "
                "Confirm that is the publishing App and nothing else") in summary
        assert "no bypass actors are configured" not in summary
        assert "NOT ruleset-protected" not in summary

    @pytest.mark.parametrize("protection", [False, None],
                             ids=["unprotected", "unknown"])
    def test_unprotected_or_unknown_tag_warns(self, protection: "bool | None") -> None:
        summary = render_plan_summary(_plan(tag_protected=protection))
        assert ("**WARNING:** The created tag is NOT ruleset-protected in "
                "this repository; extend tag protection before relying on "
                "immutability.") in summary
        assert "ruleset-protected: tag creation" not in summary

    def test_controller_sha_is_in_the_checklist_when_provided(self) -> None:
        summary = render_plan_summary(_plan(), controller_sha="f" * 40)
        assert f"- [ ] Controller code: `{'f' * 12}`" in summary

    def test_controller_sha_line_is_omitted_when_empty(self) -> None:
        assert "Controller code:" not in render_plan_summary(_plan())


class TestApprovalEvidence:
    def _issue_repo(self) -> "tuple[MagicMock, MagicMock]":
        repo = MagicMock()
        issue = MagicMock()
        repo.get_issue.return_value = issue
        gh = MagicMock()
        gh.get_repo.return_value = repo
        return gh, issue

    def test_first_post_mentions_the_approvers(self) -> None:
        # Creating the comment is what fires the notification; the plan
        # summary alone pings nobody.
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/7")
        body = issue.create_comment.call_args.kwargs["body"]
        assert _APPROVAL_MARKER in body
        assert "> [!IMPORTANT]" in body
        assert "**@valkey-io/core-team: Approval Needed to Publish `9.1.1`.**" in body
        assert "**Approve here:** https://x/runs/7" in body
        assert "\u2014" not in body

    def test_revalidation_edits_in_place_without_a_new_ping(self) -> None:
        # An edit does not re-notify: one ping per approval wait, not one
        # per cron re-validation.
        gh, issue = self._issue_repo()
        existing = MagicMock(body=f"{_APPROVAL_MARKER}\nstale evidence")
        with patch("scripts.release.issue.trusted_comments", return_value=[existing]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/8")
        issue.create_comment.assert_not_called()
        body = existing.edit.call_args.kwargs["body"]
        assert "**@valkey-io/core-team: Approval Needed to Publish `9.1.1`.**" in body
        assert "https://x/runs/8" in body

    def test_evidence_carries_the_controller_sha_when_provided(self) -> None:
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/9",
                                   controller_sha="f" * 40)
        body = issue.create_comment.call_args.kwargs["body"]
        assert f"- [ ] Controller code: `{'f' * 12}`" in body

    def test_evidence_omits_the_controller_line_when_unknown(self) -> None:
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/9")
        assert "Controller code:" not in issue.create_comment.call_args.kwargs["body"]


class TestMakeLatestDecision:
    def _repo(self, *releases: "tuple[str, bool, bool]") -> MagicMock:
        """Build a repo mock whose ``get_releases`` returns the given tuples.

        Each ``(tag, draft, prerelease)`` becomes one release mock.
        ``get_latest_release`` is not consulted by the new logic; it stays
        set here so a regression that still calls it fails loud (default
        MagicMock without a tag_name would silently look "OK").
        """
        repo = MagicMock()
        releases_mocks = [
            MagicMock(tag_name=tag, draft=draft, prerelease=pre)
            for tag, draft, pre in releases
        ]
        repo.get_releases.return_value = releases_mocks
        repo.get_latest_release.side_effect = AssertionError(
            "_make_latest_decision must enumerate releases, not use "
            "the mutable get_latest_release pointer"
        )
        return repo

    def test_first_ever_release_becomes_latest(self) -> None:
        # An empty repo (no releases) yields no maximum; this GA takes over.
        assert _make_latest_decision(self._repo(), "9.1.1", "ga") == "true"

    def test_ga_at_or_above_the_max_becomes_latest(self) -> None:
        # >=: a GA on the current top line is the latest even when equal
        # (the tag-exists gate makes a strict equal reach here only on a
        # partial-publish resume, where the pointer belongs to us).
        repo = self._repo(("9.1.0", False, False), ("9.1.1", False, False))
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_older_line_patch_never_takes_latest(self) -> None:
        # Two-branch interleaving: 8.0.5 racing 9.1.0 out of the door
        # must NOT set latest, even if a mutable pointer earlier claimed it.
        repo = self._repo(("9.1.0", False, False), ("8.0.4", False, False))
        assert _make_latest_decision(repo, "8.0.5", "ga") == "false"

    def test_stale_manual_pointer_does_not_deceive_enumeration(self) -> None:
        # get_latest_release returns a MUTABLE pointer (a maintainer
        # can move it back manually to an older release). Enumeration
        # ignores the pointer entirely; the newer release, still present
        # in the release list, keeps the decision honest.
        repo = self._repo(("9.1.0", False, False), ("9.2.0", False, False))
        # publishing 9.1.5 (still older than the max 9.2.0)
        assert _make_latest_decision(repo, "9.1.5", "ga") == "false"

    def test_non_version_tag_in_enumeration_is_ignored(self) -> None:
        # A rogue ``nightly-build`` release must not skew the maximum.
        repo = self._repo(("nightly-build", False, False),
                          ("9.1.0", False, False))
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_prereleases_never_contribute_to_the_maximum(self) -> None:
        # A prerelease sitting at ``9.2.0-rc1`` must not block the 9.1.1
        # GA from becoming latest.
        repo = self._repo(("9.1.0", False, False),
                          ("9.2.0-rc1", False, True))
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_drafts_never_contribute_to_the_maximum(self) -> None:
        # A draft release (a maintainer preparing a future version) is not
        # published; treating it as part of the latest line would let an
        # unpublished tag block a real GA from taking over.
        repo = self._repo(("9.1.0", False, False),
                          ("9.2.0", True, False))  # draft
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_two_digit_patch_compares_numerically_not_lexically(self) -> None:
        # A string sort would call '9.1.10' < '9.1.9' and strand the
        # latest pointer on the older release.
        repo = self._repo(("9.1.9", False, False))
        assert _make_latest_decision(repo, "9.1.10", "ga") == "true"

    def test_rc_stage_never_asks_github_and_never_takes_latest(self) -> None:
        # rc: early return, no enumeration call, no side_effect trigger.
        repo = MagicMock()
        assert _make_latest_decision(repo, "9.2.0", "rc1") == "false"
        repo.get_releases.assert_not_called()
        repo.get_latest_release.assert_not_called()


class TestPreviousTag:
    def test_rc_follows_the_previous_rc(self) -> None:
        repo = repo_mock(tags=["9.2.0-rc1"])
        assert _previous_tag(repo, "9.2.0", "rc2") == "9.2.0-rc1"

    def test_rc_without_its_predecessor_tag_is_not_guessed(self) -> None:
        repo = repo_mock(tags=["9.1.0"])
        assert _previous_tag(repo, "9.2.0", "rc2") is None

    def test_ga_follows_its_last_rc(self) -> None:
        repo = repo_mock(tags=["9.2.0-rc1", "9.2.0-rc2"])
        assert _previous_tag(repo, "9.2.0", "ga") == "9.2.0-rc2"
