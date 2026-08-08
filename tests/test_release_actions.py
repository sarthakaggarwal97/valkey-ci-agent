"""Tests for reconciliation's idempotent progress actions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.release import actions
from scripts.release import issue as issue_mod
from scripts.release.models import (
    Candidate,
    CandidateState,
    CheckState,
    DownstreamOutput,
    OutputState,
    QualificationStatus,
    ReleasePhase,
    ReleaseStatus,
    RequiredCheck,
)
from tests.release_fixtures import MERGE_SHA, gh_mock, make_policy, tracker

_POLICY = make_policy()


def _status(**overrides: object) -> ReleaseStatus:
    base: "dict[str, object]" = {
        "repo": "valkey-io/valkey",
        "branch": "9.1",
        "version": "9.1.1",
        "stage": "ga",
        "candidate": Candidate(state=CandidateState.CURRENT, sha=MERGE_SHA,
                               branch_head=MERGE_SHA),
        "phase": ReleasePhase.QUALIFICATION,
    }
    base.update(overrides)
    return ReleaseStatus(**base)  # type: ignore[arg-type]


class TestQualificationDispatch:
    def test_dispatches_when_no_run_exists(self) -> None:
        status = _status()  # QUALIFICATION with empty QualificationStatus
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=status, tracking_issue=tracker())
        dispatch.assert_called_once()
        assert any("dispatched qualification" in p for p in performed)

    def test_does_not_redispatch_over_pending_or_failed_run(self) -> None:
        for qualification in (
            QualificationStatus(run_id=1, pending=True),
            QualificationStatus(run_id=1, failed_jobs=("job",)),
        ):
            status = _status(qualification=qualification)
            with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
                actions.advance(gh_mock(MagicMock()), _POLICY,
                                status=status, tracking_issue=tracker())
            dispatch.assert_not_called()

    def test_no_dispatch_outside_qualification_phase(self) -> None:
        status = _status(phase=ReleasePhase.CANDIDATE)
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=tracker())
        dispatch.assert_not_called()


class TestOutputActions:
    def test_bundle_dispatch_fires_the_repository_dispatch(self) -> None:
        status = _status(
            phase=ReleasePhase.BUNDLE_HELM,
            outputs=(DownstreamOutput(name="bundle", state=OutputState.PENDING,
                                      action="dispatch-bundle"),),
        )
        repo = MagicMock()
        gh = gh_mock(repo)
        performed = actions.advance(gh, _POLICY,
                                    status=status, tracking_issue=tracker())
        repo.create_repository_dispatch.assert_called_once_with(
            event_type="valkey-release",
            client_payload={"version": "9.1.1", "component": "valkey"},
        )
        assert any("bundle" in p for p in performed)

    def test_no_action_requested_means_no_side_effects(self) -> None:
        status = _status(
            phase=ReleasePhase.BUNDLE_HELM,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(name="bundle", state=OutputState.PENDING),),
        )
        repo = MagicMock()
        actions.advance(gh_mock(repo), _POLICY,
                        status=status, tracking_issue=tracker())
        repo.create_repository_dispatch.assert_not_called()
        repo.create_pull.assert_not_called()

    def test_helm_pr_bumps_chart_and_readme(self) -> None:
        status = _status(
            phase=ReleasePhase.BUNDLE_HELM,
            outputs=(DownstreamOutput(name="helm", state=OutputState.PENDING,
                                      action="open-helm-pr"),),
        )
        repo = MagicMock()
        repo.default_branch = "main"
        repo.get_branch.return_value.commit.sha = "d" * 40
        chart = MagicMock()
        chart.decoded_content = b'apiVersion: v2\nversion: 0.11.0\nappVersion: "9.1.0"\n'
        chart.sha = "chartsha"
        readme = MagicMock()
        readme.decoded_content = (
            b"![Version: 0.11.0](https://img.shields.io/badge/Version-0.11.0-informational)"
            b"![AppVersion: 9.1.0](https://img.shields.io/badge/AppVersion-9.1.0-informational)"
        )
        readme.sha = "readmesha"
        repo.get_contents.side_effect = lambda path, **kw: (
            chart if path.endswith("Chart.yaml") else readme
        )
        repo.create_pull.return_value = MagicMock(number=9, html_url="https://x/pull/9")

        performed = actions.advance(gh_mock(repo), _POLICY,
                                    status=status, tracking_issue=tracker())

        chart_update = repo.update_file.call_args_list[0]
        new_chart = chart_update.args[2]
        assert "version: 0.11.1" in new_chart
        assert 'appVersion: "9.1.1"' in new_chart
        readme_update = repo.update_file.call_args_list[1]
        new_readme = readme_update.args[2]
        assert "![Version: 0.11.1]" in new_readme
        assert "![AppVersion: 9.1.1]" in new_readme
        assert "AppVersion-9.1.1-informational" in new_readme
        pr_kwargs = repo.create_pull.call_args.kwargs
        assert pr_kwargs["head"] == "agent/release-controller/valkey-9.1.1"
        assert pr_kwargs["base"] == "main"
        assert any("helm" in p for p in performed)


class TestNotifyOnce:
    def test_failure_notifies_the_team_once(self) -> None:
        status = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED),),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert "@valkey-io/core-team" in body
        assert "test-ubuntu-latest" in body
        assert f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in body

    def test_same_failure_state_never_notifies_twice(self) -> None:
        status = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED),),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        first_body = issue.create_comment.call_args.kwargs["body"]
        # The posted comment is now on the issue, authored by the bot.
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = first_body
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)

        issue.create_comment.assert_not_called()

    def test_notify_marker_from_non_bot_author_does_not_suppress(self) -> None:
        # A drive-by user pasting the marker must not silence real alerts.
        status = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED),),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        marker_body = issue.create_comment.call_args.kwargs["body"]
        spoof = MagicMock()
        spoof.user.login = "drive-by"
        spoof.body = marker_body
        issue.get_comments.return_value = [spoof]
        issue.create_comment.reset_mock()

        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)

        issue.create_comment.assert_called_once()

    def test_new_failure_state_notifies_again(self) -> None:
        issue = tracker()
        first = _status(checks=(RequiredCheck(name="a", state=CheckState.FAILED),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=first, tracking_issue=issue)
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        second = _status(checks=(RequiredCheck(name="b", state=CheckState.FAILED),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=second, tracking_issue=issue)

        issue.create_comment.assert_called_once()

    def test_no_failures_means_no_notification(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=_status(qualification=QualificationStatus(run_id=1, passed=True)),
                        tracking_issue=issue)
        issue.create_comment.assert_not_called()


class TestAutoClose:
    def test_complete_release_closes_the_tracker(self) -> None:
        status = _status(phase=ReleasePhase.COMPLETE,
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=status, tracking_issue=issue)
        issue.edit.assert_called_once_with(state="closed")
        assert any("closed tracking issue" in p for p in performed)

    def test_already_closed_tracker_is_left_alone(self) -> None:
        status = _status(phase=ReleasePhase.COMPLETE,
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        issue.state = "closed"
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        issue.edit.assert_not_called()
        issue.create_comment.assert_not_called()


class TestReviewRegressions:
    def test_alerts_feed_the_one_shot_notifier(self) -> None:
        status = _status(alerts=("Tag `9.1.1` exists ... unshippable ...",),
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert "unshippable" in body

    def test_bundle_dispatch_carries_the_tag_for_rc(self) -> None:
        from scripts.release.models import DownstreamOutput, OutputState, ReleasePhase
        status = _status(
            version="9.2.0", stage="rc1", phase=ReleasePhase.BUNDLE_HELM,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(name="bundle", state=OutputState.PENDING,
                                      action="dispatch-bundle"),),
        )
        repo = MagicMock()
        actions.advance(gh_mock(repo), _POLICY,
                        status=status, tracking_issue=tracker())
        payload = repo.create_repository_dispatch.call_args.kwargs["client_payload"]
        assert payload["version"] == "9.2.0-rc1"

    def test_completion_comment_is_not_duplicated_on_rerun(self) -> None:
        status = _status(phase=ReleasePhase.COMPLETE,
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        first_body = issue.create_comment.call_args.kwargs["body"]
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = first_body
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()
        issue.edit.reset_mock()
        # Simulate the close edit having failed last time: issue still open.
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        issue.create_comment.assert_not_called()  # comment not duplicated
        issue.edit.assert_called_once_with(state="closed")  # close retried

    def test_readme_badge_rewrite_is_order_independent(self) -> None:
        # AppVersion badge first must not be corrupted by the Version rewrite.
        readme = (
            "![AppVersion: 9.1.0](https://img.shields.io/badge/AppVersion-9.1.0-informational)"
            "![Version: 0.11.0](https://img.shields.io/badge/Version-0.11.0-informational)"
        )
        out = actions._bump_readme_badges(readme, "0.11.1", "9.1.1")
        assert "AppVersion-9.1.1-informational" in out
        assert "Version-0.11.1-informational" in out
        assert "AppVersion-0.11.1" not in out
