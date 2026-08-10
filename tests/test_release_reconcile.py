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
    bot_adoption,
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

    def test_backport_prs_are_not_mistaken_for_the_notes_pr(self) -> None:
        backport = notes_pr(head_ref="agent/backport/3601-to-9.1", number=50)
        assert _status(repo_mock(pulls=[backport])).notes_pr_number == 0

    def test_notes_pr_for_other_line_is_ignored(self) -> None:
        wrong_line = notes_pr(head_ref="agent/release-cut/9.0.5-ga")
        assert _status(repo_mock(pulls=[wrong_line])).notes_pr_number == 0

    def test_line_prefix_is_exact_9_10_does_not_match_9_1(self) -> None:
        near_miss = notes_pr(head_ref="agent/release-cut/9.10.5-ga")
        assert _status(repo_mock(pulls=[near_miss])).notes_pr_number == 0

    def test_fork_pr_with_notes_style_head_cannot_bind(self) -> None:
        # head.ref of a fork PR is attacker-chosen; only upstream prep
        # branches count, so a fork PR can never displace the real notes PR.
        spoof = notes_pr(head_repo="attacker/valkey", number=99, merged=False)
        real = notes_pr(number=42, created=AFTER_TRACKER - timedelta(minutes=5))
        status = _status(repo_mock(pulls=[spoof, real]), tracking_issue=tracker())
        assert status.notes_pr_number == 42

    def test_headless_pr_cannot_bind(self) -> None:
        assert _status(repo_mock(pulls=[notes_pr(head_repo=None)])).notes_pr_number == 0

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

    def test_reconcile_skips_noop_edit(self) -> None:
        repo = repo_mock(issues=[tracker()])
        first = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)
        assert first is not None
        issue = repo.get_issues.return_value[0]
        issue.body = issue_mod.render_body(first)
        issue.title = issue_mod.render_title("9.1", first.version, first.stage)
        issue.edit.reset_mock()

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        issue.edit.assert_not_called()

    def test_title_kept_while_no_notes_pr_pins_a_version(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue], pulls=[])

        reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert issue.edit.call_args.kwargs["title"] == "Release 9.1.1"

    def test_reconcile_runs_actions_by_default(self) -> None:
        repo = repo_mock(issues=[tracker()])
        with patch("scripts.release.actions.advance", return_value=[]) as advance:
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        advance.assert_called_once()


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
