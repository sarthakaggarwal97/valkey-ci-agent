"""Tests for the release reconciliation core.

Covers the stage 1+2 done-when scenarios (duplicate starts reuse one issue,
issue edits cannot authorize actions, wrong-branch / stale-candidate /
failed-CI block readiness), the notes-PR binding rules, workflow-scoped
required checks with the stall timeout, and the phase machine through
qualification, publication, and completion.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from github.GithubException import GithubException

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
    bot_binding,
    bot_comment,
    bot_receipt,
    check_run,
    gh_mock,
    make_policy,
    qualification_run,
    repo_mock,
    tracker,
)
from tests.release_fixtures import (
    notes_pr as _notes_pr_raw,
)


def notes_pr(
    *args: object,
    user_login: str = "valkeyrie-ops[bot]",
    base_ref: str = "9.1",
    base_repo: str = "valkey-io/valkey",
    **kwargs: object,
) -> MagicMock:
    """Wrap ``release_fixtures.notes_pr`` so the lookalike-preemption
    check and the bound-PR revalidation see the fields they need.

    ``release_fixtures.notes_pr`` predates both fixes: it leaves ``pr.user``
    and ``pr.base`` as unconfigured MagicMocks (truthy but not strings). The
    author check ``pr.user.login not in trusted_authors`` and the base-ref
    check ``pr.base.ref != branch`` would then always trip on fixtures the
    tests intend as "valid notes PR". Populate them with trusted defaults;
    override to model drift.
    """
    pr = _notes_pr_raw(*args, **kwargs)  # type: ignore[arg-type]
    pr.user.login = user_login
    pr.base = SimpleNamespace(
        ref=base_ref, repo=SimpleNamespace(full_name=base_repo),
    )
    return pr


@pytest.fixture(autouse=True)
def _patch_notes_pr_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route ``release_fixtures.notes_pr`` (called internally by
    ``release_fixtures.repo_mock``) through this module's wrapper so the
    author and base-ref fields are populated on default fixtures
    too; monkey-patching keeps the change scoped to this module (the
    shared fixture file serves other test modules unchanged)."""
    monkeypatch.setattr("tests.release_fixtures.notes_pr", notes_pr)

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
        # ``unstable`` is not MAJOR.MINOR-shaped, so validate_release_branch
        # raises through parse_release_branch first ("not a release branch");
        # a right-shape-but-unconfigured branch would raise "not a configured
        # release branch". Match either - both are the branch-allowlist
        # gate firing before any API access.
        with pytest.raises(ReleaseControlError, match="not a"):
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

    def test_duplicate_start_after_complete_release_demands_tracker_close(self) -> None:
        # Tag exists AND the release is COMPLETE-shaped: closing the
        # tracker is the right (and only recommended) next step. The
        # tracker carries the publication receipt: without it the
        # release would read as unverified, not complete.
        repo = repo_mock(issues=[tracker(comments=[bot_receipt()])],
                         tags=["9.1.0", "9.1.1"], released=True)
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.VERIFIED),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2, pytest.raises(ReleaseControlError,
                                   match="close tracking issue #7"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")

    def test_duplicate_start_with_release_in_flight_never_recommends_closing(self) -> None:
        # Tag exists but the release is NOT complete (still verifying, or
        # wedged): recommending closure would invite abandoning a live
        # release; the refusal says it is still in flight instead.
        repo = repo_mock(issues=[tracker()], tags=["9.1.0", "9.1.1"])
        with pytest.raises(ReleaseControlError,
                           match=r"still in flight \(tracker #7\)") as exc:
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")
        assert "close tracking issue" not in str(exc.value)

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
        # Extended policy so branches like 9.10/9.11 pass the
        # validate_release_branch gate - the property under test is the
        # trailing-dot prefix rule, not branch allowlisting.
        policy = make_policy(branches=("9.1", "8.0", "9.10", "9.11"))
        pr = notes_pr(head_ref=head_ref)
        assert _status(repo_mock(pulls=[pr]), branch=branch,
                       policy=policy).notes_pr_number == 0

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


class TestIdentityBinding:
    """The durable binding receipt: read before any scan, never displaced,
    never rebound."""

    def test_newer_rc2_pr_cannot_displace_bound_rc1(self) -> None:
        # RC1 is bound; a newer PR with a notes-style RC2 head must never
        # take over the release identity. The bound PR is fetched by
        # number, so the newer PR is invisible to the binding.
        rc1 = notes_pr(head_ref="agent/release-cut/9.1.0-rc1", number=30)
        rc2 = notes_pr(head_ref="agent/release-cut/9.1.0-rc2", number=70,
                       created=AFTER_TRACKER + timedelta(days=1))
        issue = tracker(comments=[bot_binding("9.1.0", "rc1",
                                              notes_pr_number=30,
                                              merge_sha=MERGE_SHA)])
        status = _status(repo_mock(pulls=[rc2, rc1]), tracking_issue=issue)
        assert status.notes_pr_number == 30
        assert (status.version, status.stage) == ("9.1.0", "rc1")

    def test_scan_eviction_cannot_unbind(self) -> None:
        # The bound PR fell out of the scan window (200 newer PRs, or any
        # listing hiccup): it is fetched by number, never by scan, so the
        # release does not silently lose its notes PR.
        bound = notes_pr(number=42)
        repo = repo_mock(pulls=[bound])
        repo.get_pulls.return_value = []  # evicted from the listing
        issue = tracker(comments=[bot_binding("9.1.1", "ga",
                                              notes_pr_number=42,
                                              merge_sha=MERGE_SHA)])
        status = _status(repo, tracking_issue=issue)
        assert status.notes_pr_number == 42
        assert status.ready

    def test_scan_hit_binds_so_the_next_pass_skips_the_scan(self) -> None:
        # An unbound tracker that finds its notes PR by scan records the
        # binding receipt (PR number + merge SHA) on the tracker.
        issue = tracker()
        _status(repo_mock(), tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert f"notes_pr=42 merge_sha={MERGE_SHA}" in body

    def test_bound_pr_closed_unmerged_alerts_and_never_rebinds(self) -> None:
        # The bound PR was closed without merging while another valid
        # notes PR exists: reconciliation raises a standing alert instead
        # of silently rebinding to the other PR.
        lost = notes_pr(number=42, merged=False, state="closed")
        lost.merged_at = None
        other = notes_pr(number=90, created=AFTER_TRACKER + timedelta(days=1))
        issue = tracker(comments=[bot_binding("9.1.1", "ga",
                                              notes_pr_number=42,
                                              merge_sha=MERGE_SHA)])
        status = _status(repo_mock(pulls=[other, lost]), tracking_issue=issue)
        assert status.alerts and "closed without merging" in status.alerts[0]
        assert status.notes_pr_number == 0  # never the other PR
        assert not status.ready

    def test_bound_pr_missing_alerts_instead_of_rescanning(self) -> None:
        issue = tracker(comments=[bot_binding("9.1.1", "ga",
                                              notes_pr_number=404,
                                              merge_sha=MERGE_SHA)])
        status = _status(repo_mock(), tracking_issue=issue)
        assert status.alerts and "cannot be fetched" in status.alerts[0]
        assert not status.ready

    def test_different_intent_resume_refuses(self) -> None:
        # An rc1 release is bound; a duplicate start with a GA-producing
        # intent must refuse instead of resuming (or re-deriving over) it.
        issue = tracker(comments=[bot_binding("9.1.1", "rc1")])
        repo = repo_mock(issues=[issue], tags=["9.1.0"])
        with pytest.raises(ReleaseControlError, match="refusing to restart"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")

    def test_same_intent_resume_reuses_the_bound_identity(self) -> None:
        # A version-only binding (the cut never ran) resumes with the BOUND
        # version, never a re-derivation that could disagree.
        issue = tracker(comments=[bot_binding("9.1.1", "ga")])
        repo = repo_mock(issues=[issue], pulls=[], tags=["9.1.0"])
        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")
        assert not result.created and result.cut_needed
        assert (result.version, result.stage) == ("9.1.1", "ga")

    def test_start_refusal_when_bound_pr_is_lost(self) -> None:
        lost = notes_pr(number=42, merged=False, state="closed")
        lost.merged_at = None
        issue = tracker(comments=[bot_binding("9.1.1", "ga",
                                              notes_pr_number=42,
                                              merge_sha=MERGE_SHA)])
        repo = repo_mock(issues=[issue], pulls=[lost], tags=["9.1.0"])
        with pytest.raises(ReleaseControlError, match="closed without merging"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")


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
        bad_job = MagicMock(status="completed", conclusion="failure")
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
        one_job = MagicMock(status="completed", conclusion="success")
        one_job.name = "only-job"
        truncated = qualification_run(jobs=[one_job])
        status = _status(repo_mock(qual_runs=[truncated]))
        assert not status.qualification.passed
        assert any("Evidence mismatch" in job for job in status.qualification.failed_jobs)

    def test_recorded_dispatch_nonce_matching_the_manifest_stays_ready(self) -> None:
        # The tracker's dispatch receipt recorded the nonce the fixture
        # manifest carries ("n" * 32): the evidence binds and READY holds.
        issue = tracker(comments=[bot_comment(
            f"{issue_mod.qual_nonce_marker(MERGE_SHA, 'n' * 32)}\ndispatched")])
        status = _status(repo_mock(issues=[issue]), tracking_issue=issue)
        assert status.qualification.passed
        assert status.phase is ReleasePhase.READY

    def test_recorded_dispatch_nonce_mismatch_skips_the_run_and_blocks_visibly(self) -> None:
        # SKIP semantics: the receipt recorded a different nonce than the
        # manifest echoes, so the run is not the controller's dispatch.
        # It is invisible (not evidence, NOT a failure), and the blocker
        # renders the recorded nonce with the exact manual action.
        issue = tracker(comments=[bot_comment(
            f"{issue_mod.qual_nonce_marker(MERGE_SHA, 'f' * 32)}\ndispatched")])
        status = _status(repo_mock(issues=[issue]), tracking_issue=issue)
        assert not status.qualification.passed
        assert status.qualification.run_id == 0  # skipped, never failed
        assert not status.qualification.failed_jobs
        blocker = next(b for b in status.blockers
                       if "dispatch nonce" in b)
        assert "f" * 32 in blocker  # the recorded nonce, rendered visibly
        assert "ignored, not failed" in blocker
        # The action, not just values: what to dispatch, where, with what.
        assert ("Dispatch the qualification workflow for `9.1.1` at "
                f"`{MERGE_SHA[:12]}` with `{'f' * 32}` as its `nonce` "
                f"input.") in blocker

    def test_untrusted_nonce_receipt_is_ignored_like_every_marker(self) -> None:
        # A nonce marker pasted by a random account must not become the
        # binding: without a trusted receipt the legacy behavior holds
        # (nonce is evidence detail only) and the release stays READY.
        issue = tracker(comments=[bot_comment(
            f"{issue_mod.qual_nonce_marker(MERGE_SHA, 'f' * 32)}\nforged",
            author="drive-by-user")])
        status = _status(repo_mock(issues=[issue]), tracking_issue=issue)
        assert status.qualification.passed
        assert status.phase is ReleasePhase.READY

    def test_invalidated_candidate_blocker_links_the_adopt_workflow(self) -> None:
        # The operator-facing blocker names the exact button, the way the
        # READY callout links "Approve here": the Adopt Release Candidate
        # workflow's dispatch page, plus what to paste (the full head SHA).
        status = _status(repo_mock(branch_head=MOVED_SHA))
        assert status.candidate.state is CandidateState.INVALIDATED
        blocker = next(b for b in status.blockers if "moved past" in b)
        assert "Adopt Release Candidate workflow" in blocker
        assert "/actions/workflows/release-adopt.yml" in blocker
        assert "full head SHA" in blocker

    def test_failed_required_check_never_blocks_ready(self) -> None:
        # Pin (a): required-check results are informational. A failed
        # check still renders (the human sees red CI on the tracker) but
        # produces no blocker, and READY is reached on qualification alone.
        runs = [check_run("test-ubuntu-latest", conclusion="failure"),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.FAILED
        assert status.ready and not status.blockers
        assert status.phase is ReleasePhase.READY

    def test_pending_and_missing_required_checks_never_block(self) -> None:
        runs = [check_run("test-ubuntu-latest", status="in_progress", conclusion=None)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.PENDING
        assert states["build-macos-latest"] is CheckState.MISSING
        assert status.ready and not status.blockers

    def test_qualification_proceeds_while_checks_are_still_pending(self) -> None:
        # Pin (d): with the candidate bound and no qualification run yet,
        # the phase is QUALIFICATION even though checks are still pending,
        # so reconciliation dispatches the run without waiting for CI.
        runs = [check_run("test-ubuntu-latest", status="in_progress", conclusion=None)]
        status = _status(repo_mock(runs=runs, qual_runs=[]))
        assert status.phase is ReleasePhase.QUALIFICATION
        assert any("No qualification run" in blocker for blocker in status.blockers)
        assert not any("check" in blocker.lower() for blocker in status.blockers)

    def test_check_running_past_timeout_is_stalled_but_never_blocks(self) -> None:
        old = datetime.now(timezone.utc) - timedelta(minutes=_POLICY.check_timeout_minutes + 30)
        runs = [check_run("test-ubuntu-latest", status="in_progress",
                          conclusion=None, started=old),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        states = {check.name: check.state for check in status.checks}
        assert states["test-ubuntu-latest"] is CheckState.STALLED
        assert not any("timeout" in blocker for blocker in status.blockers)
        assert status.ready

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

    @staticmethod
    def _displayed_state(status, name: str) -> CheckState:
        return {check.name: check.state for check in status.checks}[name]

    def test_same_sha_rerun_supersedes_failed_attempt(self) -> None:
        early = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
        late = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        runs = [check_run("test-ubuntu-latest", conclusion="failure", run_id=1, started=early),
                check_run("test-ubuntu-latest", conclusion="success", run_id=3, started=late),
                check_run("build-macos-latest", run_id=2, started=early)]
        status = _status(repo_mock(runs=runs))
        assert self._displayed_state(status, "test-ubuntu-latest") is CheckState.PASSED

    def test_run_id_breaks_ties_when_start_times_missing(self) -> None:
        runs = [check_run("test-ubuntu-latest", conclusion="failure", run_id=1),
                check_run("test-ubuntu-latest", conclusion="success", run_id=3),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        assert self._displayed_state(status, "test-ubuntu-latest") is CheckState.PASSED

    # Newest-run-wins with EQUAL start timestamps: the ordering key falls
    # back to the run id (GitHub ids are creation-ordered), independent of
    # conclusion and of list position (the newer run is listed first here,
    # so a naive last-listed-wins implementation fails too). The states are
    # display only, but the display must still report the NEWEST run.
    @pytest.mark.parametrize(("newest_conclusion", "expected_state"), [
        pytest.param("failure", CheckState.FAILED, id="newer-failure-beats-older-pass"),
        pytest.param("success", CheckState.PASSED, id="newer-pass-beats-older-failure"),
    ])
    def test_equal_start_times_fall_back_to_run_id(
        self, newest_conclusion: str, expected_state: CheckState,
    ) -> None:
        ts = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        older_conclusion = "success" if newest_conclusion == "failure" else "failure"
        runs = [check_run("test-ubuntu-latest", conclusion=newest_conclusion,
                          run_id=9, started=ts),
                check_run("test-ubuntu-latest", conclusion=older_conclusion,
                          run_id=3, started=ts),
                check_run("build-macos-latest", run_id=2, started=ts)]
        status = _status(repo_mock(runs=runs))
        assert self._displayed_state(status, "test-ubuntu-latest") is expected_state

    @pytest.mark.parametrize(
        ("required_conclusion", "daily_conclusion", "expected_state"),
        [
            pytest.param("success", "failure", CheckState.PASSED,
                         id="daily-failure-invisible"),
            pytest.param("failure", "success", CheckState.FAILED,
                         id="daily-pass-cannot-satisfy"),
        ],
    )
    def test_same_named_run_from_another_workflow_never_counts(
        self, required_conclusion: str, daily_conclusion: str,
        expected_state: CheckState,
    ) -> None:
        # valkey's ci.yml and daily.yml share job names; a Daily dispatch on
        # the candidate SHA must neither satisfy a displayed result nor
        # clobber a passed one, or the tracker misreports the CI state.
        late = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
        runs = [check_run("test-ubuntu-latest", conclusion=required_conclusion, run_id=1),
                check_run("test-ubuntu-latest", conclusion=daily_conclusion, run_id=9,
                          started=late, suite=DAILY_SUITE),
                check_run("build-macos-latest", run_id=2)]
        status = _status(repo_mock(runs=runs))
        assert self._displayed_state(status, "test-ubuntu-latest") is expected_state

    def test_no_qualification_workflow_run_means_missing(self) -> None:
        repo = repo_mock()
        repo.get_workflow_runs.return_value = []  # ci.yml never ran on this SHA
        status = _status(repo)
        assert {check.state for check in status.checks} == {CheckState.MISSING}
        assert status.ready  # display only: MISSING never blocks

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


def _receipted_tracker(sha: str = MERGE_SHA) -> MagicMock:
    """A tracker carrying the publication receipt for the observed release:
    published-state tests need one or the release quarantines as
    unverified. The fixture receipt is legacy-shaped, so these tests also
    pin the migration acceptance."""
    return tracker(comments=[bot_receipt(sha=sha)])


class TestPublishedPhases:
    def test_release_existing_enters_published_phase(self) -> None:
        core = (_out("tarballs", OutputState.PENDING),)
        ordered = (_out("bundle", OutputState.BLOCKED),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True),
                             tracking_issue=_receipted_tracker())
        assert status.published
        assert status.phase is ReleasePhase.PUBLISHED
        assert status.release_url
        assert status.candidate.sha == MERGE_SHA  # pinned by the tag

    def test_published_at_threads_into_ordered_verification(self) -> None:
        # The Bundle verifier needs the publication instant to ignore
        # update PRs predating this release; reconcile must pass it along.
        core = (_out("tarballs", OutputState.VERIFIED),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2 as ordered:
            _status(repo_mock(released=True),
                    tracking_issue=_receipted_tracker())
        assert ordered.call_args.kwargs["published_at"] == AFTER_TRACKER

    def test_core_settled_moves_to_bundle_helm(self) -> None:
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True),
                             tracking_issue=_receipted_tracker())
        assert status.phase is ReleasePhase.BUNDLE_HELM

    def test_everything_settled_is_complete(self) -> None:
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.VERIFIED),
                   _out("helm", OutputState.SKIPPED))
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True),
                             tracking_issue=_receipted_tracker())
        assert status.phase is ReleasePhase.COMPLETE

    def test_wrong_prerelease_flag_blocks(self) -> None:
        repo = repo_mock(released=True)
        repo.get_release.return_value.prerelease = True  # but stage is ga
        status = _status(repo, tracking_issue=_receipted_tracker())
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
            status = _status(repo_mock(released=True, branch_head=MOVED_SHA),
                             tracking_issue=_receipted_tracker())
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
                             tracking_issue=_receipted_tracker())
        assert status.tag_trusted
        assert status.alerts == ()
        assert status.phase is ReleasePhase.COMPLETE

    def test_tag_at_an_owner_adopted_sha_stays_clean(self) -> None:
        issue = tracker(comments=[bot_adoption(MOVED_SHA),
                                  bot_receipt(sha=MOVED_SHA)])
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


class TestPublicationReceipt:
    """An observed release must match the publish path's publication
    receipt (a trusted-author marker comment recording the exact tag+SHA)
    before published verification starts. No receipt, or a receipt naming
    a different tag or SHA, quarantines the release pre-published: a
    standing alert, no downstream verification, and a phase on which
    advance() dispatches nothing."""

    _UNRECEIPTED_ALERT = (
        "Release `9.1.1` exists without a controller publication receipt; "
        "treating as unverified. If this was a legitimate publish whose "
        "receipt write crashed, re-run the publish workflow to resume; an "
        "out-of-band release should be investigated."
    )

    def test_unreceipted_release_quarantines_with_the_alert(self) -> None:
        # The core attack: a release appears with no receipt on the
        # tracker. Published verification must never start and the phase
        # must stay pre-published so downstream dispatch never fires.
        core_verifier = MagicMock()
        with patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
                   core_verifier):
            status = _status(repo_mock(released=True),
                             tracking_issue=tracker())
        assert status.published  # the release exists...
        assert status.phase is ReleasePhase.CANDIDATE  # ...but pre-published
        assert status.alerts == (self._UNRECEIPTED_ALERT,)
        assert status.blockers == (self._UNRECEIPTED_ALERT,)
        core_verifier.assert_not_called()
        assert status.outputs == ()
        # The alert routes through needs-attention and the one-shot
        # notification like every other standing alert.
        assert issue_mod.has_failures(status)

    def test_no_tracker_reads_as_unreceipted(self) -> None:
        # Without a tracker there is nowhere a receipt could live, so the
        # release fails closed exactly like the unreceipted case.
        core_verifier = MagicMock()
        with patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
                   core_verifier):
            status = _status(repo_mock(released=True))
        assert status.phase is ReleasePhase.CANDIDATE
        assert status.alerts == (self._UNRECEIPTED_ALERT,)
        core_verifier.assert_not_called()

    def test_receipted_release_proceeds_to_verification(self) -> None:
        core = (_out("tarballs", OutputState.VERIFIED),)
        ordered = (_out("bundle", OutputState.VERIFIED),)
        p1, p2 = _patched_outputs(core, ordered)
        with p1, p2:
            status = _status(repo_mock(released=True),
                             tracking_issue=_receipted_tracker())
        assert status.alerts == ()
        assert status.phase is ReleasePhase.COMPLETE

    def test_sha_mismatched_receipt_alerts_naming_the_mismatch(self) -> None:
        # A receipt exists but records a different SHA than the tag points
        # at: the same quarantine, with the alert naming both sides.
        core_verifier = MagicMock()
        issue = tracker(comments=[bot_receipt(sha=MOVED_SHA)])
        with patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
                   core_verifier):
            status = _status(repo_mock(released=True), tracking_issue=issue)
        assert status.phase is ReleasePhase.CANDIDATE
        assert len(status.alerts) == 1
        alert = status.alerts[0]
        assert "controller publication receipt" in alert
        assert f"`{MOVED_SHA[:12]}`" in alert  # what the receipt records
        assert f"`{MERGE_SHA[:12]}`" in alert  # what the release tag pins
        core_verifier.assert_not_called()
        assert status.outputs == ()

    def test_tag_mismatched_receipt_alerts(self) -> None:
        issue = tracker(comments=[bot_receipt(tag="9.1.0")])
        status = _status(repo_mock(released=True), tracking_issue=issue)
        assert status.phase is ReleasePhase.CANDIDATE
        assert any("records `9.1.0`" in a for a in status.alerts)

    def test_receipt_from_untrusted_author_does_not_count(self) -> None:
        # A forged receipt pasted by anyone outside the trusted bot set is
        # invisible, exactly like every other marker read-back.
        issue = tracker(comments=[bot_receipt(author="drive-by")])
        status = _status(repo_mock(released=True), tracking_issue=issue)
        assert status.phase is ReleasePhase.CANDIDATE
        assert status.alerts == (self._UNRECEIPTED_ALERT,)

    def test_legacy_field_receipt_is_accepted(self) -> None:
        # MIGRATION: receipts posted before the digest/controller
        # fields existed (8.0.10, 9.0.6,
        # the live 8.0.11 tracker) carry only the marker and the carrier
        # line: no plan digest, no controller lines. They must keep
        # verifying so live trackers do not regress. bot_receipt IS that
        # legacy shape; this test pins the guarantee explicitly.
        core = (_out("tarballs", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2:
            status = _status(repo_mock(released=True),
                             tracking_issue=_receipted_tracker())
        assert status.alerts == ()
        assert status.phase is ReleasePhase.PUBLISHED

    def test_new_field_receipt_is_accepted(self) -> None:
        # The current publish path appends digest and controller lines
        # after the carrier line; the verifier reads only the carrier.
        body = (
            f"{issue_mod.publication_receipt_marker()}\n"
            f"Published **9.1.1** at `{MERGE_SHA}` (publication approved "
            f"by @madolson): https://x/releases/9.1.1\n"
            f"Plan digest: `{'0' * 64}`\n"
            f"Controller commit: `{'f' * 40}`\n"
            f"Controller run: https://x/actions/runs/9\n"
            f"Downstream outputs are now observed by reconciliation."
        )
        issue = tracker(comments=[bot_comment(body)])
        core = (_out("tarballs", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2:
            status = _status(repo_mock(released=True), tracking_issue=issue)
        assert status.alerts == ()
        assert status.phase is ReleasePhase.PUBLISHED

    def test_untrusted_tag_alert_takes_precedence_over_the_receipt_check(self) -> None:
        # A tag at a never-vetted SHA is the more fundamental failure and
        # keeps its existing alert; the receipt check only runs for a
        # trusted tag.
        repo = repo_mock(released=True)
        repo.get_git_ref.return_value.object.sha = "c" * 40
        status = _status(repo, tracking_issue=tracker())
        assert not status.tag_trusted
        assert len(status.alerts) == 1
        assert "never a trusted candidate" in status.alerts[0]


class TestReconcileBranch:
    def test_no_active_release_is_a_noop(self) -> None:
        repo = repo_mock(issues=[])
        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1") is None

    def test_reconcile_updates_issue_to_recomputed_state(self) -> None:
        issue = tracker()
        issue.body = issue_mod.identity_marker("9.1") + "\nstale hand-edited text"
        repo = repo_mock(issues=[issue])

        # act=True writes: act=False is strict observation mode
        # (no edits at all); use the default (act=True) for tests that
        # want to assert on the rendered projection.
        with patch("scripts.release.actions.advance", return_value=[]):
            status = reconcile_branch(gh_mock(repo), _POLICY, "9.1")

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
        with clock, patch("scripts.release.actions.advance", return_value=[]):
            first = reconcile_branch(gh_mock(repo), _POLICY, "9.1")
            assert first is not None
            issue = repo.get_issues.return_value[0]
            issue.body = issue_mod.render_body(first, now)
            issue.title = issue_mod.render_live_title(first)
            issue.edit.reset_mock()

            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        issue.edit.assert_not_called()

    def test_unchanged_state_in_a_new_minute_performs_zero_edits(self) -> None:
        # The footer timestamp is normalized out of the comparison: an idle
        # release never churns the tracker just to refresh "Updated"; the
        # freshness heartbeat lives in the workflow logs.
        repo = repo_mock(issues=[tracker()])
        now, clock = self._frozen_clock()
        with clock, patch("scripts.release.actions.advance", return_value=[]):
            first = reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        assert first is not None
        issue = repo.get_issues.return_value[0]
        issue.body = issue_mod.render_body(first, now - timedelta(minutes=5))
        issue.title = issue_mod.render_live_title(first)
        issue.edit.reset_mock()
        with clock, patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        issue.edit.assert_not_called()

    def test_a_real_state_change_edits_exactly_once(self) -> None:
        # The no-churn comparison must never swallow a real change: a body
        # differing beyond the timestamp gets exactly one edit.
        repo = repo_mock(issues=[tracker()])
        now, clock = self._frozen_clock()
        with clock, patch("scripts.release.actions.advance", return_value=[]):
            first = reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        assert first is not None
        issue = repo.get_issues.return_value[0]
        stale = issue_mod.render_body(first, now - timedelta(minutes=5))
        issue.body = stale.replace("Passed", "Pending", 1)  # state drifted
        issue.title = issue_mod.render_live_title(first)
        issue.edit.reset_mock()
        with clock, patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")
        issue.edit.assert_called_once()

    def test_title_kept_while_no_notes_pr_pins_a_version(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue], pulls=[])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

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

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        assert issue.edit.call_args.kwargs["title"] == "Release 9.1.1"

    def test_title_edit_skipped_when_unchanged(self) -> None:
        issue = tracker()
        issue.title = "Release 9.1.1"
        issue.body = "stale"
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

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
        # Gate-parked publish runs must bind the
        # current tag+candidate in their run-name to hold the slot;
        # unbound runs are ignored. Match the shape the runner writes.
        waiting = MagicMock(status="waiting", head_sha=agent_head,
                            display_title=(f"Publish Release on 9.1 · 9.1.1 @ "
                                           f"{MERGE_SHA} (requested by x)"),
                            html_url="https://x/actions/runs/500")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = [waiting]

        with patch("scripts.release.actions.advance", return_value=[]):
            status = reconcile_branch(gh_mock(repo), _POLICY, "9.1",
                                      gh_agent=gh_agent, agent_repo="o/agent")

        assert status is not None
        assert status.approval_run_url == "https://x/actions/runs/500"
        assert "> **Approve here:** https://x/actions/runs/500" in issue.edit.call_args.kwargs["body"]

    def test_approval_link_is_candidate_bound(self) -> None:
        # A gate-parked run whose run-name binds a DIFFERENT candidate is
        # never presented as the place to approve. Observation mode, so the
        # stale run also is not cancelled (finder-only path).
        issue = tracker()
        repo = repo_mock(issues=[issue])
        gh_agent = MagicMock()
        agent_head = "d" * 40
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = agent_head
        other = MagicMock(status="waiting", head_sha=agent_head,
                          display_title=(f"Publish Release on 9.1 · 9.1.1 @ "
                                         f"{MOVED_SHA} (requested by x)"),
                          html_url="https://x/actions/runs/501")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = [other]

        status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False,
                                  gh_agent=gh_agent, agent_repo="o/agent")

        assert status is not None and status.phase is ReleasePhase.READY
        assert status.approval_run_url == ""
        other.cancel.assert_not_called()

    def test_approval_link_accepts_the_run_bound_to_this_candidate(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue])
        gh_agent = MagicMock()
        agent_head = "d" * 40
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = agent_head
        bound = MagicMock(status="waiting", head_sha=agent_head,
                          display_title=(f"Publish Release on 9.1 · 9.1.1 @ "
                                         f"{MERGE_SHA} (requested by x)"),
                          html_url="https://x/actions/runs/502")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = [bound]

        status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False,
                                  gh_agent=gh_agent, agent_repo="o/agent")

        assert status is not None
        assert status.approval_run_url == "https://x/actions/runs/502"

    def test_no_agent_client_renders_the_ready_callout_without_the_link(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance", return_value=[]):
            status = reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        assert status is not None and status.approval_run_url == ""
        assert "Approve here:" not in issue.edit.call_args.kwargs["body"]


class _Label:
    def __init__(self, name: str) -> None:
        self.name = name


def _issue_with_labels(*names: str) -> MagicMock:
    issue = tracker()
    issue.labels = [_Label(name) for name in names]
    return issue


class TestCloseSeam:
    """Close-seam integration: the AdvanceResult close signal drives the last-write
    close in reconcile_branch. This seam broke silently once (a bool
    isinstance check that an AdvanceResult never satisfies), so the
    end-to-end path is pinned here rather than inferred from unit tests
    on either side."""

    def test_close_signal_closes_after_the_final_render(self) -> None:
        from scripts.release.actions import AdvanceResult

        issue = tracker()
        repo = repo_mock(issues=[issue])
        result = AdvanceResult([], close_when_complete=True)

        with patch("scripts.release.actions.advance", return_value=result):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        close_calls = [
            c for c in issue.edit.call_args_list
            if c.kwargs.get("state") == "closed"
        ]
        assert len(close_calls) == 1, "close_when_complete=True must close"
        # The close is the LAST write: the rendered-body edit precedes it.
        assert issue.edit.call_args_list[-1].kwargs.get("state") == "closed"
        body_edits = [
            c for c in issue.edit.call_args_list if "body" in c.kwargs
        ]
        assert body_edits, "final projection must be rendered before close"

    def test_no_close_signal_never_closes(self) -> None:
        from scripts.release.actions import AdvanceResult

        issue = tracker()
        repo = repo_mock(issues=[issue])
        result = AdvanceResult([], close_when_complete=False)

        with patch("scripts.release.actions.advance", return_value=result):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        assert not any(
            c.kwargs.get("state") == "closed"
            for c in issue.edit.call_args_list
        )

    def test_act_false_never_closes_even_when_signalled(self) -> None:
        issue = tracker()
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance") as advance:
            reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        advance.assert_not_called()
        assert not any(
            c.kwargs.get("state") == "closed"
            for c in issue.edit.call_args_list
        )


class TestLabelSync:
    def test_healthy_tracker_gets_no_state_labels(self) -> None:
        # Titles are constant and the phase lives inside the tracker, so
        # a healthy tracker carries only its identity labels.
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_not_called()

    def test_legacy_phase_label_is_cleaned_up(self) -> None:
        # Trackers created before the phase:* retirement still carry one;
        # reconcile strips it without adding a replacement.
        issue = _issue_with_labels("release-tracker", "release:9.1",
                                   "phase:qualification")
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_called_once_with("phase:qualification")

    @staticmethod
    def _failed_qual_run() -> MagicMock:
        bad_job = MagicMock(status="completed", conclusion="failure")
        bad_job.name = "RPM · Rocky Linux 9 (x86_64)"
        return qualification_run(conclusion="failure", jobs=[bad_job])

    def test_failures_add_needs_attention_and_recovery_removes_it(self) -> None:
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue], qual_runs=[self._failed_qual_run()])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        assert set(issue.add_to_labels.call_args.args) == {"needs-attention"}

        recovered = _issue_with_labels("release-tracker", "release:9.1",
                                       "needs-attention")
        repo = repo_mock(issues=[recovered])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        recovered.add_to_labels.assert_not_called()
        recovered.remove_from_labels.assert_called_once_with("needs-attention")

    def test_failed_required_check_never_adds_needs_attention(self) -> None:
        # Pin (b): a red check on the candidate is informational; it must
        # not add the needs-attention label (and, per TestNotifyOnce, must
        # not notify either).
        failing_runs = [check_run("test-ubuntu-latest", conclusion="failure"),
                        check_run("build-macos-latest", run_id=2)]
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue], runs=failing_runs)

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_not_called()

    def test_labels_outside_the_owned_set_are_never_touched(self) -> None:
        issue = _issue_with_labels("release-tracker", "release:9.1",
                                   "bug", "help wanted")
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        issue.add_to_labels.assert_not_called()
        issue.remove_from_labels.assert_not_called()

    def test_needs_attention_is_created_through_the_shared_helper(self) -> None:
        issue = _issue_with_labels("release-tracker", "release:9.1")
        repo = repo_mock(issues=[issue], qual_runs=[self._failed_qual_run()])
        with patch("scripts.release.reconcile.ensure_label") as ensure, \
             patch("scripts.release.actions.advance", return_value=[]):
            reconcile_branch(gh_mock(repo), _POLICY, "9.1")
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

        def _record(body: str) -> MagicMock:
            # Every controller post (binding receipt, adoption) lands as a
            # trusted comment the next read sees, mirroring GitHub.
            issue.get_comments.return_value = (
                list(issue.get_comments.return_value) + [bot_comment(body)]
            )
            return MagicMock()

        issue.create_comment.side_effect = _record

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
                return_value=list(issue.get_comments.return_value)
                + [bot_comment(body)]) or MagicMock()
        )

        status = adopt_candidate(gh_mock(repo), _POLICY,
                                 branch="9.1", sha=MOVED_SHA.upper(), actor="madolson")

        assert status.candidate.state is CandidateState.ADOPTED

    def test_adoption_requires_active_release(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[])
        with pytest.raises(ReleaseControlError, match="no active release"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")

    def test_pinned_candidate_readoption_reconfirms_shipping_it(self) -> None:
        # The branch moved but the owner wants to ship the pinned candidate
        # anyway: re-adopting the pinned SHA (the notes merge) is the
        # explicit reconfirmation and re-establishes it as the candidate.
        issue = tracker()
        repo = repo_mock(branch_head=MOVED_SHA, issues=[issue])

        def _record(body: str) -> MagicMock:
            issue.get_comments.return_value = (
                list(issue.get_comments.return_value) + [bot_comment(body)]
            )
            return MagicMock()

        issue.create_comment.side_effect = _record

        status = adopt_candidate(gh_mock(repo), _POLICY,
                                 branch="9.1", sha=MERGE_SHA, actor="madolson")

        assert status.candidate.state is CandidateState.ADOPTED
        assert status.candidate.sha == MERGE_SHA
        assert status.candidate.branch_head == MOVED_SHA
        posted = "\n".join(
            call.kwargs["body"] for call in issue.create_comment.call_args_list
        )
        assert issue_mod.adopt_marker(MERGE_SHA) in posted

    def test_arbitrary_sha_still_refused_after_movement(self) -> None:
        # Neither the new head nor the pinned candidate: adoption is never
        # a way to pick an arbitrary commit.
        repo = repo_mock(branch_head=MOVED_SHA, issues=[tracker()])
        with pytest.raises(ReleaseControlError, match="exact current head"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha="e" * 40, actor="madolson")

    def test_adoption_refused_after_publication(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA, issues=[tracker()], released=True)
        core = (_out("tarballs", OutputState.PENDING),)
        p1, p2 = _patched_outputs(core, ())
        with p1, p2, pytest.raises(ReleaseControlError, match="before publication"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")


class TestAbandonedTracker:
    """A tracker closed while the release was still observed gets exactly
    one warning; a controller-closed (complete) tracker stays silent."""

    @staticmethod
    def _repo_with_closed(closed) -> MagicMock:
        repo = repo_mock()
        repo.get_issues.side_effect = lambda state, labels: (
            [closed] if state == "closed" else []
        )
        return repo

    def test_abandoned_closed_tracker_warned_exactly_once(self) -> None:
        closed = tracker()
        closed.state = "closed"
        repo = self._repo_with_closed(closed)

        def _record(body: str) -> MagicMock:
            closed.get_comments.return_value = (
                list(closed.get_comments.return_value) + [bot_comment(body)]
            )
            return MagicMock()

        closed.create_comment.side_effect = _record

        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1") is None
        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1") is None

        closed.create_comment.assert_called_once()
        body = closed.create_comment.call_args.kwargs["body"]
        assert issue_mod.closed_warning_marker() in body
        assert "> [!WARNING]" in body
        assert ("This tracker was closed while the release was still being "
                "observed. Reopen it or dispatch release-start to resume "
                "observation.") in body

    def test_controller_closed_tracker_stays_silent(self) -> None:
        closed = tracker(comments=[bot_comment(
            f"{issue_mod.complete_marker()}\nRelease complete. Closing."
        )])
        closed.state = "closed"
        repo = self._repo_with_closed(closed)

        # Heal-path: a controller-completed CLOSED tracker with a
        # drifted projection has its labels/body fixed in place, without
        # a reopen and without an abandoned-warning comment. The return
        # value is the healed status (not None).
        with patch("scripts.release.actions.advance", return_value=[]):
            status = reconcile_branch(gh_mock(repo), _POLICY, "9.1")

        assert status is not None
        closed.create_comment.assert_not_called()

    def test_observe_mode_never_posts_the_warning(self) -> None:
        closed = tracker()
        closed.state = "closed"
        repo = self._repo_with_closed(closed)

        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False) is None

        closed.create_comment.assert_not_called()

    def test_no_closed_tracker_is_silence(self) -> None:
        repo = repo_mock()
        repo.get_issues.side_effect = lambda state, labels: []
        assert reconcile_branch(gh_mock(repo), _POLICY, "9.1") is None


# ------------------------------------------------------------------
# Regression coverage for review-hardening properties: the branch
# allowlist, the lookalike notes PR, bound-PR revalidation, the
# one-active-release invariant, observation modes, stale adoption,
# and branch-deleted-after-publication.
# ------------------------------------------------------------------


def _writes_of(issue: MagicMock) -> list[str]:
    """Names of writing methods called on *issue*, for zero-write assertions.

    Any of ``edit`` / ``add_to_labels`` / ``remove_from_labels`` /
    ``create_comment`` firing means the pass wrote - this is the audit
    the observation-mode tests apply.
    """
    calls: list[str] = []
    for method in ("edit", "add_to_labels", "remove_from_labels", "create_comment"):
        mock = getattr(issue, method)
        if mock.called:
            calls.append(f"{method}({mock.call_count}x)")
    return calls


class TestObservationMode:
    """start_release dry_run=True and reconcile_branch act=False
    are strict observation modes: no comment, no label, no edit anywhere on
    the tracker or on binding comments, regardless of what the state calls
    for."""

    def test_dry_run_start_on_existing_unbound_tracker_performs_no_writes(self) -> None:
        # Existing tracker without a bound notes PR yet: a real start
        # writes a version-only binding receipt. Dry-run must not.
        issue = tracker()  # no comments = no binding
        repo = repo_mock(issues=[issue], pulls=[], tags=["9.1.0"])

        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson",
                               dry_run=True)

        assert not result.created and result.cut_needed
        assert (result.version, result.stage) == ("9.1.1", "ga")
        assert _writes_of(issue) == []

    def test_dry_run_start_with_discoverable_notes_pr_performs_no_writes(self) -> None:
        # Existing tracker + a discoverable (scan-visible) notes PR: a real
        # start writes the identity-binding receipt on the scan hit. Dry-run
        # must not.
        issue = tracker()
        repo = repo_mock(issues=[issue])  # default pulls carries a valid notes PR

        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson",
                               dry_run=True)

        assert not result.created and not result.cut_needed
        assert _writes_of(issue) == []

    def test_reconcile_act_false_performs_no_writes_on_full_pass(self) -> None:
        # A full reconcile pass in observation mode: no branch-label
        # backfill, no advance actions, no phase-label sync, no tracker
        # edit, no binding write, no comment. Every writing method on
        # every relevant mock stays untouched.
        issue = _issue_with_labels("release-tracker")  # missing branch label = backfill candidate
        issue.body = "stale hand-edited text"  # would drive an edit under act=True
        repo = repo_mock(issues=[issue])

        with patch("scripts.release.actions.advance") as advance:
            status = reconcile_branch(gh_mock(repo), _POLICY, "9.1", act=False)

        assert status is not None
        advance.assert_not_called()
        assert _writes_of(issue) == []


class TestAdoptStaleAcknowledgement:
    """An adoption of a former head that already lapsed is refused
    (its stale acknowledgement was never in the allowed set), and a
    post-adopt candidate that stays INVALIDATED raises loudly instead of
    wedging the release on a recorded no-op."""

    def test_stale_former_head_adoption_is_refused(self) -> None:
        # Branch moved A → B → C. Owner previously adopted B. Now they
        # try to adopt B again: B is neither the current head (C) nor
        # the notes-merge pin (A), so it must be refused. Under the
        # pre-fix code ``status.candidate.sha`` would report B (the last
        # recorded adoption) and quietly accept it - the exact wedge.
        head_c = "c" * 40
        repo = repo_mock(
            branch_head=head_c,
            issues=[tracker(comments=[bot_adoption(MOVED_SHA)])],
        )
        with pytest.raises(ReleaseControlError, match="Genuinely adoptable SHAs"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")
        # No comment landed: the refusal fires before the record write.
        repo.get_issues.return_value[0].create_comment.assert_not_called()

    def test_post_adopt_still_invalidated_raises_loudly(self) -> None:
        # A trusted acknowledgement matching the current head is
        # recorded, but the immediate recompute still reports
        # INVALIDATED (a second branch move raced with the write, or the
        # comment writer's user was outside the trusted set). Refuse
        # loudly rather than record a wedged no-op.
        issue = tracker()
        repo = repo_mock(branch_head=MOVED_SHA, issues=[issue])

        # Simulate a non-trusted echo: create_comment lands but the
        # comment reads back as an untrusted author, so ``adopted_shas``
        # returns nothing and the candidate stays INVALIDATED.
        def _echo_untrusted(body: str) -> MagicMock:
            echo = MagicMock()
            echo.user.login = "drive-by"
            echo.body = body
            issue.get_comments.return_value = (
                list(issue.get_comments.return_value) + [echo]
            )
            return MagicMock()

        issue.create_comment.side_effect = _echo_untrusted

        with pytest.raises(ReleaseControlError,
                           match="STILL invalidated on the immediate recomputation"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="9.1", sha=MOVED_SHA, actor="madolson")


class TestBoundNotesPrRevalidation:
    """Every reconcile pass revalidates the bound PR's identity
    against the binding. A head rename, a base retarget, or a fork
    shove-through surfaces a standing alert and freezes the binding
    (never rebinds, never updates merge_sha)."""

    def _bound_issue(self, **binding_overrides: object) -> MagicMock:
        kwargs: dict[str, object] = {"notes_pr_number": 42, "merge_sha": MERGE_SHA}
        kwargs.update(binding_overrides)
        return tracker(comments=[bot_binding("9.1.1", "ga", **kwargs)])  # type: ignore[arg-type]

    def test_head_rename_freezes_binding(self) -> None:
        # PR is fetched by number, but head.ref now parses to a different
        # release identity (or nothing at all): standing alert; the
        # binding stays pinned to the same PR number.
        drifted = notes_pr(number=42, head_ref="agent/release-cut/9.1.2-ga")
        issue = self._bound_issue()
        status = _status(repo_mock(pulls=[drifted]), tracking_issue=issue)
        assert status.alerts and any("head" in a for a in status.alerts)
        assert not status.ready
        # Binding was NOT updated: no new comment posted.
        issue.create_comment.assert_not_called()

    def test_head_renamed_to_junk_freezes_binding(self) -> None:
        # A head that no longer parses as a notes-cut prep branch at all
        # was previously crashing an assertion; the bound-PR revalidation
        # converts it into a standing alert.
        drifted = notes_pr(number=42, head_ref="some/random/branch")
        issue = self._bound_issue()
        status = _status(repo_mock(pulls=[drifted]), tracking_issue=issue)
        assert status.alerts and any("not a notes-cut prep branch" in a
                                     for a in status.alerts)
        assert not status.ready

    def test_base_retarget_freezes_binding(self) -> None:
        # The PR was retargeted onto a different base branch: merging it
        # would land into the wrong branch. Standing alert; no merge SHA
        # is trusted from a retargeted PR.
        drifted = notes_pr(number=42, base_ref="main")  # not 9.1
        issue = self._bound_issue()
        status = _status(repo_mock(pulls=[drifted]), tracking_issue=issue)
        assert status.alerts and any("targets base" in a for a in status.alerts)
        assert not status.ready
        issue.create_comment.assert_not_called()

    def test_fork_head_shove_through_freezes_binding(self) -> None:
        # A fork-headed PR at the same PR number is a shove-through
        # attempt (post-rebind attacker-controlled head): freeze.
        drifted = notes_pr(number=42, head_repo="attacker/valkey")
        issue = self._bound_issue()
        status = _status(repo_mock(pulls=[drifted]), tracking_issue=issue)
        assert status.alerts and any("fork head" in a.lower() or "head in `" in a
                                     for a in status.alerts)
        assert not status.ready


class TestOneActiveReleaseInvariant:
    """``one active release per branch`` - the create-issue path is
    dedup-safe (readback-on-fail, refuse on ambiguity) and a closed
    tracker without the completion marker blocks the next start."""

    def test_ambiguous_create_failure_readback_returns_existing(self) -> None:
        # ``create_issue`` raised after landing server-side (a 5xx after
        # an accepted POST). The readback finds one matching tracker;
        # use it instead of retrying (which would create the duplicate).
        pre_existing = tracker()
        pre_existing.number = 42
        pre_existing.html_url = "https://x/issues/42"
        pre_existing.labels = [_Label("release-tracker"), _Label("release:9.1")]
        pre_existing.get_comments.return_value = []

        repo = repo_mock(issues=[], tags=["9.1.0"])

        # Toggle flipped by ``create_issue``: readbacks *before* the
        # create attempt see nothing (so start_release proceeds to
        # create); readbacks *after* see the tracker that landed
        # server-side (so the ambiguous-failure recovery uses it).
        def _get_issues(state: str = "open", labels: "list[str] | None" = None) -> list[MagicMock]:
            if state != "open":
                return []
            return [pre_existing] if state_flag["created"] else []

        def _raise_after_landing(**_kwargs: object) -> MagicMock:
            state_flag["created"] = True
            raise GithubException(500, "flaky", {})

        state_flag = {"created": False}

        repo.get_issues.side_effect = _get_issues
        repo.create_issue.side_effect = _raise_after_landing

        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")

        # No second create attempt: readback succeeded.
        assert repo.create_issue.call_count == 1
        assert result.issue_number == 42

    def test_multiple_open_trackers_at_discovery_refuse(self) -> None:
        first = tracker()
        second = tracker()
        second.number = 8
        second.html_url = "https://x/issues/8"
        repo = repo_mock(issues=[first, second])
        with pytest.raises(ReleaseControlError,
                           match="multiple open release trackers"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")

    def test_closed_tracker_without_completion_marker_refuses_start(self) -> None:
        # Newest CLOSED tracker for this branch has no completion marker
        # (was closed manually mid-flight, not by the controller).
        # Starting a new release now would be a duplicate mid-flight
        # release - refuse, naming the closed tracker.
        closed = tracker()
        closed.state = "closed"
        closed.get_comments.return_value = []  # no completion marker

        repo = repo_mock(issues=[], tags=["9.1.0"])
        repo.get_issues.side_effect = lambda state, labels: (
            [closed] if state == "closed" else []
        )

        with pytest.raises(ReleaseControlError, match="#7 on 9.1 was closed"):
            start_release(gh_mock(repo), _POLICY, branch="9.1",
                          intent=ReleaseIntent.PATCH, actor="madolson")
        repo.create_issue.assert_not_called()

    def test_closed_tracker_with_completion_marker_permits_start(self) -> None:
        # The completed-then-closed tracker is fine - the previous
        # release shipped; a new one can start.
        closed = tracker(comments=[bot_comment(
            f"{issue_mod.complete_marker()}\nRelease complete. Closing."
        )])
        closed.state = "closed"

        created = MagicMock(number=99, html_url="https://x/issues/99")
        created.get_comments.return_value = []
        repo = repo_mock(issues=[], tags=["9.1.0"])
        repo.get_issues.side_effect = lambda state, labels: (
            [closed] if state == "closed" else []
        )
        repo.create_issue.return_value = created

        result = start_release(gh_mock(repo), _POLICY, branch="9.1",
                               intent=ReleaseIntent.PATCH, actor="madolson")

        assert result.issue_number == 99
        repo.create_issue.assert_called_once()


class TestBranchDeletedAfterPublication:
    """A published release survives its source branch being deleted
    or renamed. ``compute_status`` tolerates the 404 and routes through
    ``_published_status``; downstream verification runs unchanged."""

    def test_branch_404_with_published_release_computes_through_published(self) -> None:
        # Bind the notes PR before the branch disappears so
        # ``_find_notes_pr`` resolves through the binding (no scan
        # required against a deleted branch).
        issue = tracker(comments=[bot_binding(
            "9.1.1", "ga", notes_pr_number=42, merge_sha=MERGE_SHA,
        ), bot_receipt()])
        repo = repo_mock(released=True, issues=[issue])
        repo.get_branch.side_effect = GithubException(404, "no such branch", {})

        core = (DownstreamOutput(name="tarballs", state=OutputState.VERIFIED,
                                 detail="ok"),)
        with patch("scripts.release.reconcile.verify_mod.verify_core_outputs",
                   return_value=core), \
             patch("scripts.release.reconcile.verify_mod.verify_ordered_outputs",
                   return_value=()):
            status = _status(repo, tracking_issue=issue)

        assert status.published
        assert status.release_url
        assert status.candidate.sha == MERGE_SHA  # pinned by the tag
        assert status.candidate.branch_head == ""  # branch gone

    def test_branch_404_without_release_is_a_hard_blocker(self) -> None:
        issue = tracker(comments=[bot_binding(
            "9.1.1", "ga", notes_pr_number=42, merge_sha=MERGE_SHA,
        )])
        repo = repo_mock(issues=[issue])
        repo.get_branch.side_effect = GithubException(404, "no such branch", {})

        status = _status(repo, tracking_issue=issue)
        assert not status.ready
        assert any("does not exist" in b for b in status.blockers)


class TestBranchAllowlist:
    """adopt and compute_status enforce the same branch allowlist
    as start_release (``validate_release_branch``)."""

    def test_compute_status_refuses_unconfigured_numeric_branch(self) -> None:
        # 6.9 is right-shape but not in the policy's configured set.
        repo = repo_mock()
        with pytest.raises(ReleaseControlError,
                           match="not a configured release branch"):
            compute_status(gh_mock(repo), _POLICY, "6.9")

    def test_adopt_candidate_refuses_unconfigured_numeric_branch(self) -> None:
        repo = repo_mock(branch_head=MOVED_SHA)
        with pytest.raises(ReleaseControlError,
                           match="not a configured release branch"):
            adopt_candidate(gh_mock(repo), _POLICY,
                            branch="6.9", sha=MOVED_SHA, actor="madolson")


class TestLookalikeNotesPr:
    """A convention-matching PR authored by someone outside the
    trusted bot set is a lookalike-preemption attempt: refuse to bind
    it and surface a standing alert."""

    def test_lookalike_pr_by_untrusted_author_is_not_bound(self) -> None:
        lookalike = notes_pr(number=99, user_login="drive-by")
        status = _status(repo_mock(pulls=[lookalike]), tracking_issue=tracker())
        assert status.notes_pr_number == 0
        assert status.alerts and any("not a trusted release-bot identity"
                                     in a for a in status.alerts)
        # The alert renders as a blocker so it shows in the callout.
        assert any("not a trusted release-bot identity" in b
                   for b in status.blockers)

    def test_env_provided_release_bot_login_permits_binding(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A fork's own App slug (from RELEASE_BOT_LOGIN) is trusted.
        monkeypatch.setenv("RELEASE_BOT_LOGIN", "myapp[bot]")
        pr = notes_pr(user_login="myapp[bot]")
        status = _status(repo_mock(pulls=[pr]), tracking_issue=tracker())
        assert status.notes_pr_number == 42
        assert status.alerts == ()
