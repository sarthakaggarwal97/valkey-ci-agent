"""Tests for the release reconciliation core.

Covers the stage 1+2 done-when scenarios (duplicate starts reuse one issue,
issue edits cannot authorize actions, wrong-branch / stale-candidate /
failed-CI block readiness), the notes-PR binding rules, workflow-scoped
required checks with the stall timeout, and the phase machine through
qualification, publication, and completion.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from scripts.release import issue as issue_mod
from scripts.release.authorize import NotAuthorizedError
from scripts.release.models import (
    CandidateState,
    CheckState,
    DownstreamOutput,
    OutputState,
    QualificationStatus,
    ReleaseIntent,
    ReleasePhase,
)
from scripts.release.reconcile import (
    ReleaseControlError,
    adopt_candidate,
    compute_status,
    reconcile_branch,
    start_release,
)
from tests.release_fixtures import (
    AFTER_TRACKER,
    BEFORE_TRACKER,
    DAILY_SUITE,
    MERGE_SHA,
    MOVED_SHA,
    TRACKER_CREATED,
    bot_adoption,
    bot_comment,
    check_run,
    gh_mock,
    make_policy,
    notes_pr,
    qualification_run,
    repo_mock,
    tracker,
)

_POLICY = make_policy()


def _status(repo, *, tracking_issue=None, policy=_POLICY, branch="9.1"):
    return compute_status(gh_mock(repo), policy, branch, tracking_issue=tracking_issue)


class TestStartRelease:
    def test_embargoed_intent_refused_before_any_api_call(self) -> None:
        gh = MagicMock()
        with pytest.raises(ReleaseControlError, match="break-glass"):
            start_release(gh, _POLICY, branch="9.1",
                          intent=ReleaseIntent.SECURITY, actor="madolson")
        gh.get_repo.assert_not_called()
        gh.get_organization.assert_not_called()

    def test_wrong_branch_refused(self) -> None:
        gh = MagicMock()
        with pytest.raises(ReleaseControlError, match="not a configured release branch"):
            start_release(gh, _POLICY, branch="unstable",
                          intent=ReleaseIntent.PATCH, actor="madolson")
        gh.get_repo.assert_not_called()

    def test_unauthorized_actor_refused_before_any_write(self) -> None:
        repo = repo_mock()
        gh = gh_mock(repo, member=False)
        with pytest.raises(NotAuthorizedError):
            start_release(gh, _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="drive-by")
        repo.create_issue.assert_not_called()

    def test_fresh_start_creates_tracker_with_derived_version(self) -> None:
        repo = repo_mock(issues=[], tags=["9.1.0", "9.0.0"])
        created = MagicMock(number=11, html_url="https://x/issues/11")
        repo.create_issue.return_value = created

        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")

        assert result.created and result.cut_needed
        assert (result.version, result.stage, result.tag) == ("9.1.1", "ga", "9.1.1")
        kwargs = repo.create_issue.call_args.kwargs
        assert kwargs["title"] == "Release 9.1.1"
        assert issue_mod.identity_marker("9.1") in kwargs["body"]
        assert kwargs["labels"] == ["release-tracker", "release:9.1"]

    def test_duplicate_start_with_notes_pr_in_flight_reuses_and_cuts_nothing(self) -> None:
        repo = repo_mock(issues=[tracker()], tags=["9.1.0"])
        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")
        assert not result.created and not result.cut_needed
        assert result.issue_number == 7
        # Version reported from the in-flight notes PR, not re-derived.
        assert (result.version, result.stage) == ("9.1.1", "ga")
        repo.create_issue.assert_not_called()

    def test_duplicate_start_after_failed_cut_resumes_with_derived_version(self) -> None:
        # Tracker exists but the cut never produced a PR: recovery must not
        # require a hand-typed version.
        repo = repo_mock(issues=[tracker()], pulls=[], tags=["9.1.0"])
        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")
        assert not result.created and result.cut_needed
        assert (result.version, result.stage) == ("9.1.1", "ga")
        repo.create_issue.assert_not_called()

    def test_duplicate_start_after_release_shipped_demands_tracker_close(self) -> None:
        repo = repo_mock(issues=[tracker()], tags=["9.1.0", "9.1.1"])
        with pytest.raises(ReleaseControlError, match="close tracking issue #7"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")

    def test_dry_run_creates_nothing(self) -> None:
        repo = repo_mock(issues=[], tags=["9.1.0"])
        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson", dry_run=True)
        assert result.created and result.cut_needed and result.version == "9.1.1"
        repo.create_issue.assert_not_called()


class TestNotesPRBinding:
    def test_no_notes_pr_blocks(self) -> None:
        repo = repo_mock(pulls=[])
        status = _status(repo)
        assert not status.ready
        assert status.phase is ReleasePhase.NOTES
        assert status.candidate.state is CandidateState.NONE
        assert any("No release-notes PR" in blocker for blocker in status.blockers)
        repo.get_branch.assert_called_with("9.1")
        repo.get_pulls.assert_called_once_with(
            state="all", base="9.1", sort="created", direction="desc",
        )

    # One two-line guard rejects every non-notes head: the prep-branch regex
    # and the exact-line prefix check (version must start with "<branch>.").
    # The trailing dot is load-bearing in both directions: a 9.1 tracker must
    # not bind a 9.10/9.11 PR, and a 9.11 tracker must not bind a 9.1 PR.
    @pytest.mark.parametrize(("head_ref", "branch"), [
        pytest.param("agent/backport/3601-to-9.1", "9.1", id="backport-head"),
        pytest.param("agent/release-cut/9.0.5-ga", "9.1", id="other-line"),
        pytest.param("agent/release-cut/9.10.5-ga", "9.1", id="9.10-not-9.1"),
        pytest.param("agent/release-cut/9.11.5-ga", "9.1", id="9.11-not-9.1"),
        pytest.param("agent/release-cut/9.1.5-ga", "9.11", id="9.1-not-9.11"),
    ])
    def test_non_matching_head_refs_never_bind(self, head_ref: str, branch: str) -> None:
        pr = notes_pr(head_ref=head_ref)
        assert _status(repo_mock(pulls=[pr]), branch=branch).notes_pr_number == 0

    def test_fork_pr_with_notes_style_head_cannot_bind(self) -> None:
        # head.ref of a fork PR is attacker-chosen; only upstream prep
        # branches count, so a fork PR can never displace the real notes PR.
        spoof = notes_pr(head_repo="attacker/valkey", number=99, merged=False)
        real = notes_pr(number=42, created=AFTER_TRACKER - timedelta(minutes=5))
        status = _status(repo_mock(pulls=[spoof, real]), tracking_issue=tracker())
        assert status.notes_pr_number == 42

    def test_headless_pr_cannot_bind(self) -> None:
        assert _status(repo_mock(pulls=[notes_pr(head_repo=None)])).notes_pr_number == 0

    # The head-repo guard is a full-string equality on full_name, so a
    # crafted repo name sharing the upstream string as a prefix or suffix
    # must not pass.
    @pytest.mark.parametrize("full_name", [
        pytest.param("valkey-io/valkey.evil.com", id="suffix-crafted"),
        pytest.param("evil-valkey-io/valkey", id="prefix-crafted"),
        pytest.param("valkey-io/valkey-evil", id="hyphen-extended"),
    ])
    def test_crafted_head_repo_full_name_cannot_bind(self, full_name: str) -> None:
        spoof = notes_pr(head_repo=full_name, number=99)
        assert _status(repo_mock(pulls=[spoof])).notes_pr_number == 0

    def test_pr_created_at_the_tracker_instant_does_not_bind(self) -> None:
        # The binding is strict (created_at <= tracker created_at excludes),
        # so a PR stamped at the exact tracker second fails closed. GitHub
        # timestamps have second granularity, so this tie is reachable.
        pr = notes_pr(created=TRACKER_CREATED)
        status = _status(repo_mock(pulls=[pr]), tracking_issue=tracker())
        assert status.notes_pr_number == 0

    def test_merged_pr_with_empty_string_merge_commit_fails_closed(self) -> None:
        # merge_commit_sha "" (not None) must take the same fail-closed path
        # as a missing merge commit, never become a "" candidate SHA that
        # compares equal to some other empty field.
        status = _status(repo_mock(pulls=[notes_pr(merge_sha="")]))
        assert status.candidate.state is CandidateState.NONE
        assert not status.ready
        assert any("no merge commit" in blocker for blocker in status.blockers)

    def test_previous_releases_merged_notes_pr_is_not_this_releases(self) -> None:
        # rc2 tracker just opened; the rc1 notes PR (merged, older than the
        # tracker) must not bind, or reconcile would demand adoption of a
        # head for a release that already shipped.
        old = notes_pr(head_ref="agent/release-cut/9.1.0-rc1", number=30,
                       created=BEFORE_TRACKER)
        status = _status(repo_mock(pulls=[old]), tracking_issue=tracker())
        assert status.notes_pr_number == 0
        assert status.candidate.state is CandidateState.NONE

    def test_closed_unmerged_cut_is_skipped_in_favor_of_older_valid_pr(self) -> None:
        abandoned = notes_pr(number=60, merged=False, state="closed",
                             created=AFTER_TRACKER + timedelta(hours=2))
        merged = notes_pr(number=42)
        status = _status(repo_mock(pulls=[abandoned, merged]), tracking_issue=tracker())
        assert status.notes_pr_number == 42
        assert status.notes_pr_merged

    def test_newest_matching_pr_wins(self) -> None:
        # rc1 merged, rc2 cut open: the newer PR pins the release.
        newer_open = notes_pr(head_ref="agent/release-cut/9.1.0-rc2", number=70,
                              merged=False, created=AFTER_TRACKER + timedelta(days=1))
        older_merged = notes_pr(head_ref="agent/release-cut/9.1.0-rc1", number=30)
        status = _status(repo_mock(pulls=[newer_open, older_merged]),
                         tracking_issue=tracker())
        assert status.notes_pr_number == 70
        assert (status.version, status.stage) == ("9.1.0", "rc2")


class TestComputeStatus:
    def test_unmerged_notes_pr_blocks(self) -> None:
        status = _status(repo_mock(pulls=[notes_pr(merged=False)]))
        assert not status.ready
        assert status.phase is ReleasePhase.NOTES
        assert (status.version, status.stage) == ("9.1.1", "ga")
        assert any("not merged" in blocker for blocker in status.blockers)

    def test_merged_pr_without_merge_commit_fails_closed(self) -> None:
        status = _status(repo_mock(pulls=[notes_pr(merge_sha=None)]))
        assert status.candidate.state is CandidateState.NONE
        assert not status.ready
        assert any("no merge commit" in blocker for blocker in status.blockers)

    def test_green_ci_and_passed_qualification_is_ready(self) -> None:
        status = _status(repo_mock())
        assert status.ready and not status.blockers
        assert status.phase is ReleasePhase.READY
        assert status.candidate.state is CandidateState.CURRENT
        assert status.candidate.sha == MERGE_SHA
        assert {check.state for check in status.checks} == {CheckState.PASSED}
        assert status.qualification.passed

    def test_green_ci_without_qualification_run_is_qualification_phase(self) -> None:
        status = _status(repo_mock(qual_runs=[]))
        assert status.phase is ReleasePhase.QUALIFICATION
        assert not status.ready
        assert any("No qualification run" in blocker for blocker in status.blockers)

    def test_pending_qualification_blocks(self) -> None:
        pending = qualification_run(status="in_progress", conclusion=None)
        status = _status(repo_mock(qual_runs=[pending]))
        assert status.phase is ReleasePhase.QUALIFICATION
        assert any("still executing" in blocker for blocker in status.blockers)

    def test_failed_qualification_blocks_with_job_names(self) -> None:
        bad_job = MagicMock(conclusion="failure")
        bad_job.name = "RPM · Rocky Linux 9 (x86_64)"
        failed = qualification_run(conclusion="failure", jobs=[bad_job])
        status = _status(repo_mock(qual_runs=[failed]))
        assert status.phase is ReleasePhase.QUALIFICATION
        assert any("Rocky Linux" in blocker for blocker in status.blockers)

    def test_qualification_run_for_other_sha_does_not_count(self) -> None:
        other = qualification_run(sha=MOVED_SHA)
        status = _status(repo_mock(qual_runs=[other]))
        assert status.phase is ReleasePhase.QUALIFICATION
        assert status.qualification.run_id == 0

    def test_truncated_qualification_matrix_does_not_pass(self) -> None:
        one_job = MagicMock(conclusion="success")
        one_job.name = "only-job"
        truncated = qualification_run(jobs=[one_job])
        status = _status(repo_mock(qual_runs=[truncated]))
        assert not status.qualification.passed
        assert any("Evidence mismatch" in job for job in status.qualification.failed_jobs)

    def test_failed_required_check_blocks_before_qualification(self) -> None:
        runs = [check_run("test-ubuntu-latest", conclusion="failure"),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        assert status.phase is ReleasePhase.CANDIDATE
        assert any("failed" in blocker and "test-ubuntu-latest" in blocker
                   for blocker in status.blockers)

    def test_pending_and_missing_required_checks_block(self) -> None:
        runs = [check_run("test-ubuntu-latest", status="in_progress", conclusion=None)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.PENDING
        assert states["build-macos-latest"] is CheckState.MISSING
        assert not status.ready

    def test_check_running_past_timeout_is_stalled(self) -> None:
        old = datetime.now(timezone.utc) - timedelta(minutes=_POLICY.check_timeout_minutes + 30)
        runs = [check_run("test-ubuntu-latest", status="in_progress",
                          conclusion=None, started=old),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.STALLED
        assert any("timeout" in blocker for blocker in status.blockers)

    def test_never_started_check_stalls_from_its_creation_time(self) -> None:
        # A queued run with no started_at must not dodge STALLED forever;
        # its creation time is the fallback clock.
        old = datetime.now(timezone.utc) - timedelta(minutes=_POLICY.check_timeout_minutes + 30)
        runs = [check_run("test-ubuntu-latest", status="queued",
                          conclusion=None, created=old),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.STALLED

    def test_same_sha_rerun_supersedes_failed_attempt(self) -> None:
        early = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
        late = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        runs = [check_run("test-ubuntu-latest", conclusion="failure", run_id=1, started=early),
                check_run("test-ubuntu-latest", conclusion="success", run_id=3, started=late),
                check_run("build-macos-latest", run_id=2, started=early)]
        assert _status(repo_mock(runs=runs)).ready

    def test_run_id_breaks_ties_when_start_times_missing(self) -> None:
        runs = [check_run("test-ubuntu-latest", conclusion="failure", run_id=1),
                check_run("test-ubuntu-latest", conclusion="success", run_id=3),
                check_run("build-macos-latest", run_id=2)]
        assert _status(repo_mock(runs=runs)).ready

    # Newest-run-wins with EQUAL start timestamps: the ordering key falls
    # back to the run id (GitHub ids are creation-ordered), independent of
    # conclusion and of list position (the newer run is listed first here,
    # so a naive last-listed-wins implementation fails too).
    @pytest.mark.parametrize(("newest_conclusion", "expected_ready"), [
        pytest.param("failure", False, id="newer-failure-beats-older-pass"),
        pytest.param("success", True, id="newer-pass-beats-older-failure"),
    ])
    def test_equal_start_times_fall_back_to_run_id(
        self, newest_conclusion: str, expected_ready: bool,
    ) -> None:
        ts = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        older_conclusion = "success" if newest_conclusion == "failure" else "failure"
        runs = [check_run("test-ubuntu-latest", conclusion=newest_conclusion,
                          run_id=9, started=ts),
                check_run("test-ubuntu-latest", conclusion=older_conclusion,
                          run_id=3, started=ts),
                check_run("build-macos-latest", run_id=2, started=ts)]
        assert _status(repo_mock(runs=runs)).ready is expected_ready

    @pytest.mark.parametrize(
        ("required_conclusion", "daily_conclusion", "expected_ready"),
        [
            pytest.param("success", "failure", True, id="daily-failure-invisible"),
            pytest.param("failure", "success", False, id="daily-pass-cannot-satisfy"),
        ],
    )
    def test_same_named_run_from_another_workflow_never_counts(
        self, required_conclusion: str, daily_conclusion: str, expected_ready: bool,
    ) -> None:
        # valkey's ci.yml and daily.yml share job names; a Daily dispatch on
        # the candidate SHA must neither satisfy a requirement nor clobber a
        # passed one.
        late = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        runs = [check_run("test-ubuntu-latest", conclusion=required_conclusion, run_id=1),
                check_run("test-ubuntu-latest", conclusion=daily_conclusion, run_id=9,
                          started=late, suite=DAILY_SUITE),
                check_run("build-macos-latest", run_id=2)]
        assert _status(repo_mock(runs=runs)).ready is expected_ready

    def test_no_qualification_workflow_run_means_missing(self) -> None:
        repo = repo_mock()
        repo.get_workflow_runs.return_value = []  # ci.yml never ran on this SHA
        status = _status(repo)
        assert {check.state for check in status.checks} == {CheckState.MISSING}
        assert not status.ready

    def test_branch_movement_invalidates_candidate_and_skips_checks(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA)
        status = _status(repo, tracking_issue=tracker())
        assert status.candidate.state is CandidateState.INVALIDATED
        assert status.phase is ReleasePhase.CANDIDATE
        assert any("adopt" in blocker for blocker in status.blockers)
        assert status.checks == ()
        repo.get_commit.assert_not_called()

    def test_bot_recorded_adoption_restores_candidate_at_new_head(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA,
                         qual_runs=[qualification_run(sha=MOVED_SHA)])
        status = _status(repo, tracking_issue=tracker(comments=[bot_adoption(MOVED_SHA)]))
        assert status.candidate.state is CandidateState.ADOPTED
        assert status.candidate.sha == MOVED_SHA
        assert status.ready  # checks + qualification pass on the adopted head
        repo.get_commit.assert_called_once_with(MOVED_SHA)

    def test_adoption_marker_from_non_bot_author_does_not_authorize(self) -> None:
        comment = MagicMock()
        comment.user.login = "drive-by"
        comment.body = issue_mod.adopt_marker(MOVED_SHA)
        status = _status(repo_mock(branch_head=MOVED_SHA),
                         tracking_issue=tracker(comments=[comment]))
        assert status.candidate.state is CandidateState.INVALIDATED
        assert not status.ready

    def test_adoption_of_earlier_head_is_stale_after_second_move(self) -> None:
        third = "c" * 40
        status = _status(repo_mock(branch_head=third),
                         tracking_issue=tracker(comments=[bot_adoption(MOVED_SHA)]))
        assert status.candidate.state is CandidateState.INVALIDATED
        assert not status.ready
        # The blocker names the candidate that actually lapsed (the adoption).
        assert status.candidate.sha == MOVED_SHA

    def test_conflicting_adopt_markers_adopt_by_membership_not_order(self) -> None:
        # One comment carrying two different adopt markers: adoption is a
        # set-membership test against the live head, so the head matching
        # the FIRST marker still adopts even with a second marker after it.
        body = f"{issue_mod.adopt_marker(MOVED_SHA)}\n{issue_mod.adopt_marker('c' * 40)}"
        repo = repo_mock(branch_head=MOVED_SHA,
                         qual_runs=[qualification_run(sha=MOVED_SHA)])
        status = _status(repo, tracking_issue=tracker(comments=[bot_comment(body)]))
        assert status.candidate.state is CandidateState.ADOPTED
        assert status.candidate.sha == MOVED_SHA

    def test_conflicting_adopt_markers_matching_neither_head_invalidate(self) -> None:
        # Neither marker matches the live head: still INVALIDATED, and the
        # reported lapsed candidate is the LAST recorded adoption, so the
        # operator message names the most recent acknowledgement.
        third = "c" * 40
        body = f"{issue_mod.adopt_marker(MOVED_SHA)}\n{issue_mod.adopt_marker(third)}"
        status = _status(repo_mock(branch_head="d" * 40),
                         tracking_issue=tracker(comments=[bot_comment(body)]))
        assert status.candidate.state is CandidateState.INVALIDATED
        assert status.candidate.sha == third

    def test_readopted_original_sha_reuses_its_old_passing_qualification(self) -> None:
        # The subtle revival case: notes merge at A, branch moved to B (B
        # adopted and qualified), then the branch moved BACK to A. The old
        # passing qualification run for A revives and the release is READY
        # again with no fresh run. Judgment: correct. Qualification is a
        # pure function of (tag, sha); the tree at A is byte-identical to
        # when run 900 executed, so that run is evidence for exactly this
        # candidate. The branch's detour history does not change what was
        # tested. If a rerun were wanted, the key would have to include the
        # adoption epoch, which nothing in the model supports.
        stale_adoption = bot_adoption(MOVED_SHA)  # the interim head, now stale
        repo = repo_mock()  # head back at MERGE_SHA; run 900 passed on it
        status = _status(repo, tracking_issue=tracker(comments=[stale_adoption]))
        assert status.candidate.state is CandidateState.CURRENT
        assert status.candidate.sha == MERGE_SHA
        assert status.qualification.run_id == 900  # the old run, reused
        assert status.ready


class TestStrayTag:
    def test_tag_without_release_raises_an_unshippable_alert(self) -> None:
        # A manually pushed tag (or partial publish) makes the version
        # permanently unshippable (the ruleset forbids moving/deleting it);
        # the tracker must say so instead of marching toward READY.
        repo = repo_mock()
        ref = MagicMock()
        ref.object.type = "commit"
        ref.object.sha = "f" * 40
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = ref
        status = _status(repo)
        assert not status.ready
        assert any("unshippable" in alert for alert in status.alerts)
        assert any("unshippable" in blocker for blocker in status.blockers)
        # The alert also gates progress: qualification is never consulted or
        # dispatched for an unshippable version (phase stays CANDIDATE), so
        # the state machine cannot march toward READY around the alert.
        assert status.phase is ReleasePhase.CANDIDATE
        assert status.qualification.run_id == 0


class TestQualificationBinding:
    def test_run_for_different_version_same_sha_does_not_count(self) -> None:
        # An rc-suffixed dispatch legitimately skips the package matrix; it
        # must never satisfy the GA qualification for the same SHA.
        spoof = qualification_run(tag="9.1.1-rc9")
        status = _status(repo_mock(qual_runs=[spoof]))
        assert status.qualification.run_id == 0
        assert status.phase is ReleasePhase.QUALIFICATION

    def test_run_from_a_side_branch_does_not_count(self) -> None:
        # A doctored qualify workflow on a non-default ref cannot
        # manufacture evidence.
        doctored = qualification_run(head_branch="attacker/gutted-workflow")
        status = _status(repo_mock(qual_runs=[doctored]))
        assert status.qualification.run_id == 0


def _patched_outputs(core, ordered):
    return (
        patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
              return_value=core),
        patch("scripts.release.reconcile.verify_mod.verify_ordered_outputs",
              return_value=ordered),
    )


def _out(name: str, state: OutputState) -> DownstreamOutput:
    return DownstreamOutput(name=name, state=state, detail=name)


class TestPublishedPhases:
    def test_release_existing_enters_published_phase(self) -> None:
        core = (_out("tarballs", OutputState.PENDING),)
        ordered = (_out("bundle", OutputState.BLOCKED),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True))
        assert status.published
        assert status.phase is ReleasePhase.PUBLISHED
        assert status.release_url
        assert status.candidate.sha == MERGE_SHA  # pinned by the tag

    def test_core_settled_moves_to_bundle_helm(self) -> None:
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True))
        assert status.phase is ReleasePhase.BUNDLE_HELM

    def test_everything_settled_is_complete(self) -> None:
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.VERIFIED),
                   _out("helm", OutputState.SKIPPED))
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True))
        assert status.phase is ReleasePhase.COMPLETE

    def test_wrong_prerelease_flag_blocks(self) -> None:
        repo = repo_mock(released=True)
        repo.get_release.return_value.prerelease = True  # but stage is ga
        status = _status(repo)
        assert status.published
        assert any("prerelease flag" in blocker for blocker in status.blockers)

    def test_published_candidate_stays_pinned_when_branch_moves_away(self) -> None:
        # The tag SHA no longer being the branch head (post-release commits,
        # even a force-push that dropped it) must not resurrect INVALIDATED
        # or demand an adoption: publication is the point of no return and
        # the immutable tag, not the branch, pins the candidate. Judgment:
        # correct; the branch legitimately moves on after a release, and an
        # ancestry check would add an unfixable alert for a shipped version.
        core = (_out("tarballs", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2:
            status = _status(repo_mock(released=True, branch_head=MOVED_SHA))
        assert status.published
        assert status.candidate.state is CandidateState.CURRENT
        assert status.candidate.sha == MERGE_SHA  # the tag's commit
        assert status.candidate.branch_head == MOVED_SHA
        assert status.phase is ReleasePhase.PUBLISHED


class TestPublishedTagTrust:
    """The release tag must point at a SHA the process vetted: the notes-PR
    merge commit or an owner-adopted head recorded on the tracker. Any
    other commit alerts loudly, renders untrusted, skips downstream
    verification, and never reaches COMPLETE."""

    _HOSTILE_SHA = "c" * 40

    def _released_repo(self, tag_sha: str) -> MagicMock:
        repo = repo_mock(released=True)
        repo.get_git_ref.return_value.object.sha = tag_sha
        return repo

    def test_tag_at_a_never_trusted_sha_alerts_and_never_completes(self) -> None:
        # Hostile case: notes merge is MERGE_SHA but the tag points at a
        # SHA nobody vetted. Even with every downstream output verified
        # this must never read as a valid publication.
        core_verifier = MagicMock()
        with patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
                   core_verifier):
            status = _status(self._released_repo(self._HOSTILE_SHA),
                             tracking_issue=tracker())
        assert not status.tag_trusted
        assert status.phase is ReleasePhase.PUBLISHED  # never COMPLETE
        expected = (
            f"Release tag 9.1.1 points at {self._HOSTILE_SHA[:12]}, which "
            f"was never a trusted candidate (notes merge or owner-adopted). "
            f"Manual investigation required."
        )
        assert status.alerts == (expected,)
        assert status.blockers == (expected,)
        # The alert makes has_failures true, so needs-attention and the
        # one-shot notification fire through the existing alert plumbing.
        assert issue_mod.has_failures(status)
        # No downstream verification (and so no bundle/helm actions) runs
        # against an untrusted artifact.
        core_verifier.assert_not_called()
        assert status.outputs == ()

    def test_tag_at_the_notes_merge_stays_clean(self) -> None:
        core = (DownstreamOutput(name="tarballs", state=OutputState.VERIFIED),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2:
            status = _status(self._released_repo(MERGE_SHA),
                             tracking_issue=tracker())
        assert status.tag_trusted
        assert status.alerts == ()
        assert status.phase is ReleasePhase.COMPLETE

    def test_tag_at_an_owner_adopted_sha_stays_clean(self) -> None:
        issue = tracker(comments=[bot_adoption(MOVED_SHA)])
        core = (DownstreamOutput(name="tarballs", state=OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2:
            status = _status(self._released_repo(MOVED_SHA),
                             tracking_issue=issue)
        assert status.tag_trusted
        assert status.alerts == ()
        assert status.phase is ReleasePhase.PUBLISHED

    def test_no_tracker_still_trusts_the_notes_merge_only(self) -> None:
        # Without a tracker (no adoption record) the notes merge is the
        # sole trusted SHA; the hostile SHA still alerts.
        status = _status(self._released_repo(self._HOSTILE_SHA))
        assert not status.tag_trusted
        assert issue_mod.has_failures(status)


class TestReconcileBranch:
    def test_no_active_release_is_a_noop(self) -> None:
        repo = repo_mock(issues=[])
        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1") is None

    def test_reconcile_updates_issue_to_recomputed_state(self) -> None:
        issue = tracker()
        issue.body = issue_mod.identity_marker("9.1") + "\nstale hand-edited text"
        repo = repo_mock(issues=[issue])

        status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert status is not None and status.ready
        kwargs = issue.edit.call_args.kwargs
        assert issue_mod.identity_marker("9.1") in kwargs["body"]
        assert "stale hand-edited text" not in kwargs["body"]

    @staticmethod
    def _frozen_clock():
        # The body's freshness footer has minute resolution; freezing the
        # reconcile clock keeps same-minute comparisons deterministic.
        now = datetime(2026, 8, 10, 17, 30, tzinfo=timezone.utc)
        frozen = MagicMock()
        frozen.now.return_value = now
        return now, patch("scripts.release.reconcile.datetime", frozen)

    def test_reconcile_skips_noop_edit_within_the_same_minute(self) -> None:
        repo = repo_mock(issues=[tracker()])
        now, clock = self._frozen_clock()
        with clock:
            first = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)
            assert first is not None
            issue = repo.get_issues.return_value[0]
            issue.body = issue_mod.render_body(first, now)
            issue.title = issue_mod.render_live_title(first)
            issue.edit.reset_mock()

            reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        issue.edit.assert_not_called()

    def test_a_new_minute_edits_the_body_as_the_freshness_signal(self) -> None:
        # Accepted churn: the footer timestamp IS the staleness signal, so
        # a pass in a new minute edits the body even with no state change.
        repo = repo_mock(issues=[tracker()])
        now, clock = self._frozen_clock()
        with clock:
            first = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)
        assert first is not None
        issue = repo.get_issues.return_value[0]
        issue.body = issue_mod.render_body(first, now - timedelta(minutes=5))
        issue.title = issue_mod.render_live_title(first)
        issue.edit.reset_mock()
        with clock:
            reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)
        issue.edit.assert_called_once()

    def test_title_kept_while_no_notes_pr_pins_a_version(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue], pulls=[])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        # The body still edits (freshness footer) but the start-time title
        # is never clobbered: the edit carries no title at all.
        kwargs = issue.edit.call_args.kwargs
        assert "title" not in kwargs
        assert issue.title == "Release 9.1.1"

    def test_title_heals_to_the_constant_form(self) -> None:
        # The title carries no phase or state; reconcile heals a mangled
        # or legacy phase-suffixed title back to the constant form.
        issue = tracker()
        issue.title = "Release 9.1.1 · Ready to Publish"
        repo = repo_mock(issues=[issue])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert issue.edit.call_args.kwargs["title"] == "Release 9.1.1"

    def test_title_edit_skipped_when_unchanged(self) -> None:
        issue = tracker()
        issue.title = "Release 9.1.1"
        issue.body = "stale"
        repo = repo_mock(issues=[issue])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        kwargs = issue.edit.call_args.kwargs
        assert "title" not in kwargs
        assert "body" in kwargs

    def test_reconcile_runs_actions_by_default(self) -> None:
        repo = repo_mock(issues=[tracker()])
        with patch("scripts.release.actions.advance", return_value=[]) as advance:
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        advance.assert_called_once()

    def test_ready_reconcile_threads_the_approval_run_url(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue])
        gh_agent = MagicMock()
        agent_head = "d" * 40
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = agent_head
        waiting = MagicMock(status="waiting", head_sha=agent_head,
                            display_title="Publish release on 9.1 (requested by x)",
                            html_url="https://x/actions/runs/500")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = [waiting]

        status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False,
                                  gh_agent=gh_agent, agent_repo="o/agent")

        assert status is not None
        assert status.approval_run_url == "https://x/actions/runs/500"
        assert "> **Approve here:** https://x/actions/runs/500" in issue.edit.call_args.kwargs["body"]

    def test_no_agent_client_renders_the_ready_callout_without_the_link(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue])

        status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert status is not None and status.approval_run_url == ""
        assert "Approve here:" not in issue.edit.call_args.kwargs["body"]


class _Label:
    def __init__(self, name: str) -> None:
        self.name = name


def _issue_with_labels(*names: str) -> MagicMock:
    issue = tracker()
    issue.labels = [_Label(name) for name in names]
    return issue


class TestLabelSync:
    def test_healthy_tracker_gets_no_state_labels(self) -> None:
        # Titles are constant and the phase lives inside the tracker, so
        # a healthy tracker carries only its identity labels.
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_not_called()

    def test_legacy_phase_label_is_cleaned_up(self) -> None:
        # Trackers created before the phase:* retirement still carry one;
        # reconcile strips it without adding a replacement.
        issue = _issue_with_labels("release-tracker", "release:9.1",
                                   "phase:qualification")
        repo = repo_mock(issues=[issue])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_called_once_with("phase:qualification")

    def test_failures_add_needs_attention_and_recovery_removes_it(self) -> None:
        failing_runs = [check_run("test-ubuntu-latest", conclusion="failure"),
                        check_run("build-macos-latest", run_id=2)]
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue], runs=failing_runs)

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert set(issue.add_to_labels.call_args.args) == {"needs-attention"}

        recovered = _issue_with_labels("release-tracker", "release:9.1",
                                       "needs-attention")
        repo = repo_mock(issues=[recovered])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        recovered.add_to_labels.assert_not_called()
        recovered.remove_from_labels.assert_called_once_with("needs-attention")

    def test_labels_outside_the_owned_set_are_never_touched(self) -> None:
        issue = _issue_with_labels("release-tracker", "release:9.1",
                                   "bug", "help wanted")
        repo = repo_mock(issues=[issue])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_not_called()

    def test_needs_attention_is_created_through_the_shared_helper(self) -> None:
        failing_runs = [check_run("test-ubuntu-latest", conclusion="failure"),
                        check_run("build-macos-latest", run_id=2)]
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue], runs=failing_runs)
        with patch("scripts.release.reconcile.ensure_label") as ensure:
            reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)
        assert any(call.args[1] == "needs-attention"
                   for call in ensure.call_args_list)


class TestAdoptCandidate:
    def test_unauthorized_actor_cannot_adopt(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[tracker()])
        with pytest.raises(NotAuthorizedError):
            adopt_candidate(gh_mock(repo, member=False), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="drive-by")
        repo.get_issues.return_value[0].create_comment.assert_not_called()

    def test_adoption_requires_exact_current_head(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[tracker()])
        with pytest.raises(ReleaseControlError, match="exact current head"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha="c" * 40, actor="madolson")

    def test_adoption_refused_while_candidate_current(self) -> None:
        repo = repo_mock(issues=[tracker()])  # head == merge sha
        with pytest.raises(ReleaseControlError, match="invalidated"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MERGE_SHA, actor="madolson")

    def test_adoption_records_marker_and_refreshes_issue(self) -> None:
        issue = tracker()
        repo = repo_mock(branch_head=MOVED_SHA, issues=[issue],
                         qual_runs=[qualification_run(sha=MOVED_SHA)])

        def _record_adoption(body: str) -> MagicMock:
            issue.get_comments.return_value = [bot_adoption(MOVED_SHA)]
            return MagicMock()

        issue.create_comment.side_effect = _record_adoption

        status = adopt_candidate(gh_mock(repo), _POLICY,
                                 branch="9.1", sha=MOVED_SHA, actor="madolson")

        comment_body = issue.create_comment.call_args.kwargs["body"]
        assert issue_mod.adopt_marker(MOVED_SHA) in comment_body
        assert "@madolson" in comment_body
        assert status.candidate.state is CandidateState.ADOPTED
        issue.edit.assert_called_once()

    def test_uppercase_sha_is_normalized(self) -> None:
        issue = tracker()
        repo = repo_mock(branch_head=MOVED_SHA, issues=[issue],
                         qual_runs=[qualification_run(sha=MOVED_SHA)])
        issue.create_comment.side_effect = lambda body: (
            issue.get_comments.configure_mock(
                return_value=[bot_adoption(MOVED_SHA)]) or MagicMock()
        )

        status = adopt_candidate(gh_mock(repo), _POLICY,
                                 branch="9.1", sha=MOVED_SHA.upper(), actor="madolson")

        assert status.candidate.state is CandidateState.ADOPTED

    def test_adoption_requires_active_release(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[])
        with pytest.raises(ReleaseControlError, match="no active release"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")

    def test_adoption_refused_after_publication(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[tracker()], released=True)
        core = (_out("tarballs", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2, pytest.raises(ReleaseControlError, match="before publication"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")
