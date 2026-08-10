"""Tests for reconciliation's idempotent progress actions."""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

import pytest
from github.GithubException import GithubException

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
from tests.release_fixtures import MERGE_SHA, MOVED_SHA, gh_mock, make_policy, tracker

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

    def test_does_not_redispatch_over_pending_run(self) -> None:
        status = _status(qualification=QualificationStatus(run_id=1, pending=True))
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=tracker())
        dispatch.assert_not_called()

    @pytest.mark.parametrize("marked", [
        pytest.param(False, id="unmarked-retries-once"),
        pytest.param(True, id="marked-never-retries"),
    ])
    def test_failed_run_retries_exactly_once_per_candidate(self, marked: bool) -> None:
        status = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        issue = tracker()
        if marked:
            fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
            posted = MagicMock()
            posted.user.login = "valkeyrie-ops[bot]"
            posted.body = (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:"
                           f"qual-retry:{fingerprint} -->\nretried")
            issue.get_comments.return_value = [posted]
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=status, tracking_issue=issue)
        if marked:
            dispatch.assert_not_called()
            assert not any("auto-retried" in p for p in performed)
        else:
            dispatch.assert_called_once()
            assert dispatch.call_args.kwargs == {"tag": "9.1.1", "sha": MERGE_SHA}
            assert any("auto-retried qualification" in p for p in performed)

    def test_no_dispatch_outside_qualification_phase(self) -> None:
        status = _status(phase=ReleasePhase.CANDIDATE)
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=tracker())
        dispatch.assert_not_called()


class TestQualificationRetryComment:
    def test_retry_posts_the_marker_callout_before_dispatching(self) -> None:
        status = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        issue = tracker()
        with patch.object(actions.qual_mod, "dispatch_qualification"):
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        autofix = next(b for b in bodies if ":autofix:" in b)
        assert (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:qual-retry:"
                f"{fingerprint} -->") in autofix
        assert "> [!NOTE]" in autofix
        assert "**Auto-remediation:** Retrying qualification for `9.1.1` once" in autofix
        assert "[run 901](https://x/qruns/901)" in autofix
        assert "\u2014" not in autofix

    def test_autofix_marker_from_non_bot_author_does_not_suppress(self) -> None:
        # A drive-by user pasting the marker must not eat the one retry.
        status = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        issue = tracker()
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        spoof = MagicMock()
        spoof.user.login = "drive-by"
        spoof.body = f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:qual-retry:{fingerprint} -->"
        issue.get_comments.return_value = [spoof]
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
        dispatch.assert_called_once()


class TestAutoDispatchBuildRelease:
    def _failed_trigger_status(self, version: str = "9.1.1",
                               stage: str = "ga") -> ReleaseStatus:
        return _status(
            version=version, stage=stage,
            phase=ReleasePhase.PUBLISHED, published=True,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail="The release trigger run failed before dispatching "
                       "the build. Re-run it, or dispatch build-release for "
                       "9.1.1 directly.",
                url="https://x/runs/55", action="dispatch-build-release"),),
        )

    def _dispatchable_repo(self) -> MagicMock:
        repo = MagicMock()
        repo.default_branch = "main"
        repo.get_workflow.return_value.create_dispatch.return_value = True
        return repo

    def test_failed_trigger_dispatches_the_build_once_when_unmarked(self) -> None:
        repo = self._dispatchable_repo()
        issue = tracker()
        performed = actions.advance(gh_mock(repo), _POLICY,
                                    status=self._failed_trigger_status(),
                                    tracking_issue=issue)
        repo.get_workflow.assert_called_with("build-release.yml")
        repo.get_workflow.return_value.create_dispatch.assert_called_once_with(
            "main", inputs={"version": "9.1.1", "environment": "prod"},
        )
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        autofix = next(b for b in bodies if ":autofix:" in b)
        assert (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:build-dispatch:"
                f"{fingerprint} -->") in autofix
        assert "> [!NOTE]" in autofix
        assert "**Auto-remediation:** Dispatching the build pipeline for `9.1.1`" in autofix
        assert "[release trigger run](https://x/runs/55)" in autofix
        assert "\u2014" not in autofix
        assert any("auto-dispatched build-release" in p for p in performed)

    def test_rc_dispatch_carries_the_tag_as_version(self) -> None:
        repo = self._dispatchable_repo()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._failed_trigger_status(version="9.2.0", stage="rc1"),
                        tracking_issue=tracker())
        repo.get_workflow.return_value.create_dispatch.assert_called_once_with(
            "main", inputs={"version": "9.2.0-rc1", "environment": "prod"},
        )

    def test_marked_candidate_never_dispatches_again(self) -> None:
        # Once per candidate SHA, even across distinct failed trigger runs.
        repo = self._dispatchable_repo()
        issue = tracker()
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:build-dispatch:"
                       f"{fingerprint} -->\ndispatched")
        issue.get_comments.return_value = [posted]
        performed = actions.advance(gh_mock(repo), _POLICY,
                                    status=self._failed_trigger_status(),
                                    tracking_issue=issue)
        repo.get_workflow.return_value.create_dispatch.assert_not_called()
        assert not any("auto-dispatched" in p for p in performed)

    def test_failure_notification_still_fires_alongside_the_autofix(self) -> None:
        # Auto-remediation must not swallow the visible failure escalation.
        issue = tracker()
        actions.advance(gh_mock(self._dispatchable_repo()), _POLICY,
                        status=self._failed_trigger_status(), tracking_issue=issue)
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in b for b in bodies)
        assert any(":autofix:build-dispatch:" in b for b in bodies)

    def test_pending_build_run_never_triggers_the_dispatch(self) -> None:
        repo = self._dispatchable_repo()
        status = _status(
            phase=ReleasePhase.PUBLISHED, published=True,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(name="build-run", state=OutputState.PENDING,
                                      detail="No run found yet"),),
        )
        issue = tracker()
        actions.advance(gh_mock(repo), _POLICY, status=status, tracking_issue=issue)
        repo.get_workflow.return_value.create_dispatch.assert_not_called()
        issue.create_comment.assert_not_called()


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
        assert "> [!WARNING]" in body
        assert "**@valkey-io/core-team, release `9.1.1` needs attention.**" in body
        assert "| # | Problem |" in body
        assert "test-ubuntu-latest" in body
        assert "<sub>This notification repeats only if the failure state changes.</sub>" in body
        assert f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in body
        assert "\u2014" not in body

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


class TestNudgeOnce:
    def _notes_pr_open(self) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.NOTES,
            notes_pr_number=42, notes_pr_url="https://x/pull/42",
            notes_pr_merged=False,
            candidate=Candidate(state=CandidateState.NONE, branch_head=MERGE_SHA),
        )

    def _branch_moved(self, head: str = MOVED_SHA) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.CANDIDATE,
            notes_pr_number=42, notes_pr_url="https://x/pull/42",
            notes_pr_merged=True,
            candidate=Candidate(state=CandidateState.INVALIDATED, sha=MERGE_SHA,
                                branch_head=head),
        )

    def _replay_as_bot(self, issue: MagicMock) -> None:
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

    def test_open_notes_pr_nudges_once_with_link_and_tag(self) -> None:
        issue = tracker()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._notes_pr_open(), tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert f"<!-- {issue_mod.MARKER_NAMESPACE}:nudge:" in body
        assert "> [!IMPORTANT]" in body
        assert "**@valkey-io/core-team, action needed for `9.1.1`.**" in body
        assert "Review and merge the release-notes PR https://x/pull/42" in body
        assert "to proceed with `9.1.1`." in body
        assert "<sub>One-time nudge: posts again only if the state changes.</sub>" in body
        assert "\u2014" not in body
        assert any("nudged" in p for p in performed)

    def test_same_state_never_nudges_twice(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._notes_pr_open(), tracking_issue=issue)
        self._replay_as_bot(issue)

        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._notes_pr_open(), tracking_issue=issue)

        issue.create_comment.assert_not_called()

    def test_branch_moved_nudge_names_branch_head_and_recovery(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._branch_moved(), tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert "**@valkey-io/core-team, action needed for `9.1.1`.**" in body
        assert f"Branch `9.1` moved to `{MOVED_SHA[:12]}`" in body
        assert "Adopt the new head (Actions → release-adopt)" in body
        assert "or ship the pinned candidate." in body
        assert "\u2014" not in body

    def test_new_head_after_a_nudge_notifies_again(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._branch_moved(), tracking_issue=issue)
        self._replay_as_bot(issue)

        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._branch_moved(head="c" * 40), tracking_issue=issue)

        issue.create_comment.assert_called_once()
        assert ("c" * 40)[:12] in issue.create_comment.call_args.kwargs["body"]

    def test_merged_notes_pr_with_current_candidate_never_nudges(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=_status(notes_pr_number=42, notes_pr_merged=True,
                                       qualification=QualificationStatus(run_id=1, passed=True)),
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
        body = issue.create_comment.call_args.kwargs["body"]
        assert "> [!NOTE]" in body
        assert "**Release `9.1.1` (ga) is complete.**" in body
        assert "all verified public. Closing." in body
        assert "\u2014" not in body
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


class TestAutoDispatchPublish:
    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def _agent(self, active_runs: "list | None" = None) -> MagicMock:
        gh_agent = MagicMock()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = active_runs or []
        return gh_agent

    def test_ready_dispatches_the_publish_pipeline_once(self) -> None:
        gh_agent = self._agent()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(), tracking_issue=tracker(),
                                    gh_agent=gh_agent, agent_repo="o/agent")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_called_once_with(
            gh_agent.get_repo.return_value.default_branch, inputs={"branch": "9.1"},
        )
        assert any("publish pipeline" in p for p in performed)

    def test_waiting_publish_run_blocks_a_duplicate_dispatch(self) -> None:
        waiting = MagicMock(status="waiting",
                            display_title="Publish release on 9.1 (requested by x)")
        gh_agent = self._agent([waiting])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_other_branch_run_does_not_block(self) -> None:
        other = MagicMock(status="waiting",
                          display_title="Publish release on 8.0 (requested by x)")
        gh_agent = self._agent([other])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent")
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_called_once()

    def test_unreadable_publish_workflow_blocks_dispatch_fail_closed(self) -> None:
        # Cannot see the workflow (404): reconcile must not dispatch blind.
        gh_agent = MagicMock()
        gh_agent.get_repo.return_value.get_workflow.side_effect = GithubException(
            404, "not found", {},
        )
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(), tracking_issue=tracker(),
                                    gh_agent=gh_agent, agent_repo="o/agent")
        assert not any("publish pipeline" in p for p in performed)

    def test_no_agent_client_skips_auto_dispatch(self) -> None:
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(), tracking_issue=tracker())
        assert not any("publish pipeline" in p for p in performed)

    def test_non_ready_phase_never_dispatches(self) -> None:
        gh_agent = self._agent()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=_status(phase=ReleasePhase.PUBLISHED,
                                       qualification=QualificationStatus(run_id=1, passed=True)),
                        tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent")
        gh_agent.get_repo.return_value.get_workflow.return_value.create_dispatch.assert_not_called()
