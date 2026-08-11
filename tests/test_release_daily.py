"""Tests for the branch-level daily-CI release gate (observation-only).

The gate complements the per-commit required checks: a release must not
reach READY unless the release branch's newest completed daily run is green
and fresh. Covers the evaluator's fail-closed behavior, the readiness gate
through compute_status, the once-per-failing-run notification, and the
Details-cell rendering for every verdict.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from github.GithubException import GithubException

from scripts.release import actions
from scripts.release import issue as issue_mod
from scripts.release.checks import daily_blockers, evaluate_daily
from scripts.release.models import (
    Candidate,
    CandidateState,
    DailyCiState,
    DailyCiStatus,
    ReleasePhase,
    ReleaseStatus,
)
from scripts.release.reconcile import compute_status
from tests.release_fixtures import (
    MERGE_SHA,
    daily_run,
    gh_mock,
    make_policy,
    qualification_run,
    repo_mock,
    tracker,
)

_NOW = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

_DAILY_POLICY = make_policy(daily_workflow="daily.yml", daily_max_age_hours=30)


def _daily_repo(daily_runs: "list[MagicMock]", **repo_kwargs: object) -> MagicMock:
    """A repo mock whose get_workflow distinguishes the daily workflow from
    the qualification workflow (the plain repo_mock serves one workflow)."""
    repo = repo_mock(**repo_kwargs)  # type: ignore[arg-type]
    qual_workflow = MagicMock()
    qual_workflow.get_runs.return_value = [qualification_run()]
    daily_workflow = MagicMock()
    daily_workflow.get_runs.return_value = daily_runs
    repo.get_workflow.side_effect = lambda name: (
        daily_workflow if name == "daily.yml" else qual_workflow
    )
    return repo


def _repo_for_evaluate(daily_runs: "list[MagicMock]") -> MagicMock:
    repo = MagicMock()
    repo.get_workflow.return_value.get_runs.return_value = daily_runs
    return repo


class TestEvaluateDaily:
    def test_unconfigured_policy_skips_without_any_api_call(self) -> None:
        repo = MagicMock()
        status = evaluate_daily(repo, make_policy(), "9.1", _NOW)
        assert status.state is DailyCiState.SKIPPED
        repo.get_workflow.assert_not_called()

    def test_fresh_green_run_passes_with_humanized_age(self) -> None:
        runs = [daily_run(created=_NOW - timedelta(hours=21))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.PASSED
        assert status.run_id == 77
        assert status.url == "https://x/druns/77"
        assert status.detail == "Passed (21 hours ago)"

    def test_age_exactly_at_the_bound_is_still_fresh(self) -> None:
        # The bound is a strict greater-than: exactly 30 hours old passes.
        runs = [daily_run(created=_NOW - timedelta(hours=30))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.PASSED

    def test_one_second_past_the_bound_is_stale(self) -> None:
        runs = [daily_run(created=_NOW - timedelta(hours=30, seconds=1))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.STALE

    def test_stale_green_run_reports_age_and_bound(self) -> None:
        runs = [daily_run(created=_NOW - timedelta(hours=40))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.STALE
        assert status.detail == ("Newest daily run is 40 hours old, older "
                                 "than the 30-hour freshness bound")

    def test_staleness_is_judged_before_the_conclusion(self) -> None:
        # An old failed run is STALE, not FAILED: the actionable problem is
        # that no fresh signal exists at all.
        runs = [daily_run(conclusion="failure", created=_NOW - timedelta(hours=40))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.STALE

    def test_fresh_failed_run_is_failed(self) -> None:
        runs = [daily_run(conclusion="failure", created=_NOW - timedelta(hours=2))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.FAILED
        assert status.detail == "Daily run failed"
        assert status.run_id == 77

    def test_cancelled_run_is_not_evidence_of_passing(self) -> None:
        runs = [daily_run(conclusion="cancelled", created=_NOW - timedelta(hours=2))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.FAILED

    def test_no_completed_run_on_the_branch_is_missing(self) -> None:
        status = evaluate_daily(_repo_for_evaluate([]), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.MISSING
        assert status.detail == "No completed daily run on branch 9.1 yet"
        assert status.run_id == 0 and status.url == ""

    def test_green_run_on_another_branch_never_satisfies(self) -> None:
        runs = [daily_run(branch="unstable", created=_NOW - timedelta(hours=1))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.MISSING

    def test_binding_is_by_workflow_file_not_job_names(self) -> None:
        # Resolution asks GitHub for exactly the policy's daily workflow and
        # scans only ITS runs; a green run of any other workflow (even one
        # with identical job names) is never consulted.
        repo = MagicMock()
        daily_workflow = MagicMock()
        daily_workflow.get_runs.return_value = []
        repo.get_workflow.return_value = daily_workflow
        status = evaluate_daily(repo, _DAILY_POLICY, "9.1", _NOW)
        repo.get_workflow.assert_called_once_with("daily.yml")
        assert status.state is DailyCiState.MISSING

    def test_unreadable_workflow_fails_closed_as_missing(self) -> None:
        repo = MagicMock()
        repo.get_workflow.side_effect = GithubException(404, "gone", {})
        status = evaluate_daily(repo, _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.MISSING
        assert status.detail == "Cannot read the daily workflow"

    def test_newest_completed_run_wins_over_an_older_one(self) -> None:
        # Runs list newest-first; the first completed match carries the
        # verdict, so yesterday's failure cannot shadow today's pass.
        runs = [daily_run(run_id=80, created=_NOW - timedelta(hours=3)),
                daily_run(run_id=79, conclusion="failure",
                          created=_NOW - timedelta(hours=27))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.PASSED
        assert status.run_id == 80

    def test_in_progress_run_is_ignored_for_the_verdict_but_mentioned(self) -> None:
        runs = [daily_run(run_id=81, status="in_progress", conclusion=None,
                          created=_NOW - timedelta(minutes=10)),
                daily_run(run_id=80, created=_NOW - timedelta(hours=21))]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.PASSED
        assert status.run_id == 80
        assert status.detail == "Passed (21 hours ago; a newer daily run is in progress)"

    def test_only_an_in_progress_run_is_missing_with_a_mention(self) -> None:
        runs = [daily_run(status="in_progress", conclusion=None)]
        status = evaluate_daily(_repo_for_evaluate(runs), _DAILY_POLICY, "9.1", _NOW)
        assert status.state is DailyCiState.MISSING
        assert status.detail == "No completed daily run on branch 9.1 yet (one is in progress)"


class TestDailyBlockers:
    def test_passed_and_skipped_add_nothing(self) -> None:
        assert daily_blockers(DailyCiStatus(state=DailyCiState.PASSED)) == []
        assert daily_blockers(DailyCiStatus(state=DailyCiState.SKIPPED)) == []

    def test_non_green_verdicts_carry_the_detail(self) -> None:
        stale = DailyCiStatus(state=DailyCiState.STALE, detail="too old")
        assert daily_blockers(stale) == ["Daily CI: too old"]


class TestReadinessGate:
    def _status(self, repo: MagicMock) -> ReleaseStatus:
        return compute_status(gh_mock(repo), _DAILY_POLICY, "9.1")

    def test_fresh_green_daily_keeps_ready_reachable(self) -> None:
        runs = [daily_run(created=datetime.now(timezone.utc) - timedelta(hours=2))]
        status = self._status(_daily_repo(runs))
        assert status.daily.state is DailyCiState.PASSED
        assert status.ready and not status.blockers
        assert status.phase is ReleasePhase.READY

    def test_stale_green_daily_blocks_ready(self) -> None:
        runs = [daily_run(created=datetime.now(timezone.utc) - timedelta(hours=40))]
        status = self._status(_daily_repo(runs))
        assert status.daily.state is DailyCiState.STALE
        assert not status.ready
        # The phase never reaches READY (the publish dispatch and the
        # protected publish path both key on the phase).
        assert status.phase is ReleasePhase.QUALIFICATION
        assert status.qualification.passed  # qualification itself is green
        assert any(blocker.startswith("Daily CI: Newest daily run is")
                   and "30-hour freshness bound" in blocker
                   for blocker in status.blockers)

    def test_red_daily_blocks_ready(self) -> None:
        runs = [daily_run(conclusion="failure",
                          created=datetime.now(timezone.utc) - timedelta(hours=2))]
        status = self._status(_daily_repo(runs))
        assert status.daily.state is DailyCiState.FAILED
        assert not status.ready
        assert status.phase is ReleasePhase.QUALIFICATION
        assert "Daily CI: Daily run failed" in status.blockers
        assert issue_mod.has_failures(status)

    def test_missing_daily_blocks_ready(self) -> None:
        status = self._status(_daily_repo([]))
        assert status.daily.state is DailyCiState.MISSING
        assert not status.ready
        assert "Daily CI: No completed daily run on branch 9.1 yet" in status.blockers

    def test_unconfigured_gate_skips_and_ready_is_reachable(self) -> None:
        status = compute_status(gh_mock(repo_mock()), make_policy(), "9.1")
        assert status.daily.state is DailyCiState.SKIPPED
        assert status.ready
        assert status.phase is ReleasePhase.READY

    def test_stale_or_missing_daily_is_a_blocker_not_a_failure(self) -> None:
        status = self._status(_daily_repo([]))
        assert not issue_mod.has_failures(status)


def _action_status(daily: DailyCiStatus) -> ReleaseStatus:
    # CANDIDATE phase: outside qualification so advance() performs no
    # dispatches and the notification path is isolated.
    return ReleaseStatus(
        repo="valkey-io/valkey", branch="9.1", version="9.1.1", stage="ga",
        candidate=Candidate(state=CandidateState.CURRENT, sha=MERGE_SHA,
                            branch_head=MERGE_SHA),
        phase=ReleasePhase.CANDIDATE, daily=daily,
    )


def _failed_daily(run_id: int) -> DailyCiStatus:
    return DailyCiStatus(state=DailyCiState.FAILED, run_id=run_id,
                         url=f"https://x/druns/{run_id}", detail="Daily run failed")


def _replay_as_bot(issue: MagicMock) -> None:
    posted = MagicMock()
    posted.user.login = "valkeyrie-ops[bot]"
    posted.body = issue.create_comment.call_args.kwargs["body"]
    issue.get_comments.return_value = [posted]
    issue.create_comment.reset_mock()


class TestDailyFailureNotification:
    def test_red_daily_notifies_with_the_run_link(self) -> None:
        issue = tracker()
        performed = actions.advance(gh_mock(MagicMock()), _DAILY_POLICY,
                                    status=_action_status(_failed_daily(77)),
                                    tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        # _problem_cell bolds the "name:" prefix of the failure item.
        assert "**Daily CI run 77 failed:** https://x/druns/77" in body
        assert any("notified" in p for p in performed)

    def test_same_failing_run_never_repings_and_a_new_one_does(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _DAILY_POLICY,
                        status=_action_status(_failed_daily(77)), tracking_issue=issue)
        _replay_as_bot(issue)

        # Same failing run on the next pass: suppressed by the fingerprint.
        actions.advance(gh_mock(MagicMock()), _DAILY_POLICY,
                        status=_action_status(_failed_daily(77)), tracking_issue=issue)
        issue.create_comment.assert_not_called()

        # A NEW failing run id changes the key and re-pings exactly once.
        issue_mod.invalidate_comment_memo(issue)
        actions.advance(gh_mock(MagicMock()), _DAILY_POLICY,
                        status=_action_status(_failed_daily(78)), tracking_issue=issue)
        issue.create_comment.assert_called_once()
        assert "Daily CI run 78 failed" in issue.create_comment.call_args.kwargs["body"]

    def test_stale_and_missing_daily_never_notify(self) -> None:
        for state, detail in ((DailyCiState.STALE, "too old"),
                              (DailyCiState.MISSING, "no run")):
            issue = tracker()
            status = _action_status(DailyCiStatus(state=state, detail=detail))
            actions.advance(gh_mock(MagicMock()), _DAILY_POLICY,
                            status=status, tracking_issue=issue)
            issue.create_comment.assert_not_called()


class TestDailyCell:
    def _render(self, daily: DailyCiStatus, **overrides: object) -> str:
        base: "dict[str, object]" = {
            "repo": "valkey-io/valkey", "branch": "9.1",
            "version": "9.1.1", "stage": "ga",
            "notes_pr_number": 42, "notes_pr_url": "https://x/pull/42",
            "notes_pr_merged": True,
            "candidate": Candidate(state=CandidateState.CURRENT,
                                   sha=MERGE_SHA, branch_head=MERGE_SHA),
            "daily": daily,
        }
        base.update(overrides)
        status = ReleaseStatus(**base)  # type: ignore[arg-type]
        return issue_mod.render_body(status, _NOW)

    def test_passed_cell_carries_the_link_and_the_age(self) -> None:
        body = self._render(DailyCiStatus(
            state=DailyCiState.PASSED, run_id=77, url="https://x/druns/77",
            detail="Passed (21 hours ago)"))
        assert ("| Daily CI | Passed ([daily run](https://x/druns/77), "
                "21 hours ago) |") in body

    def test_failed_cell_links_the_run(self) -> None:
        body = self._render(_failed_daily(77))
        assert "| Daily CI | Failed ([daily run](https://x/druns/77)) |" in body

    def test_stale_cell_links_the_run_and_explains(self) -> None:
        body = self._render(DailyCiStatus(
            state=DailyCiState.STALE, run_id=77, url="https://x/druns/77",
            detail=("Newest daily run is 40 hours old, older than the "
                    "30-hour freshness bound")))
        assert ("| Daily CI | Stale ([daily run](https://x/druns/77)): "
                "Newest daily run is 40 hours old, older than the "
                "30-hour freshness bound |") in body

    def test_missing_cell_is_the_italic_detail_with_no_link(self) -> None:
        body = self._render(DailyCiStatus(
            state=DailyCiState.MISSING,
            detail="No completed daily run on branch 9.1 yet"))
        assert "| Daily CI | _No completed daily run on branch 9.1 yet_ |" in body
        assert "daily run](" not in body

    def test_skipped_cell_reads_not_configured_before_publication(self) -> None:
        body = self._render(DailyCiStatus())
        assert "| Daily CI | _Not configured for this repository_ |" in body

    def test_published_cell_reads_gated_before_publication(self) -> None:
        body = self._render(
            DailyCiStatus(), phase=ReleasePhase.PUBLISHED, published=True,
            release_url="https://x/releases/9.1.1")
        assert ("| Daily CI | _Gated before publication; "
                "not re-evaluated afterward_ |") in body

    def test_daily_cells_carry_no_emoji_and_no_em_dash(self) -> None:
        variants = (
            DailyCiStatus(state=DailyCiState.PASSED, run_id=77,
                          url="https://x/druns/77", detail="Passed (2 hours ago)"),
            _failed_daily(77),
            DailyCiStatus(state=DailyCiState.MISSING, detail="No completed daily run"),
        )
        for daily in variants:
            body = self._render(daily)
            row = next(line for line in body.splitlines()
                       if line.startswith("| Daily CI |"))
            assert "\u2014" not in body
            assert all(ord(ch) < 0x2190 for ch in row)  # no emoji in the cell
