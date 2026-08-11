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
    DailyCiState,
    DailyCiStatus,
    DownstreamOutput,
    OutputState,
    QualificationStatus,
    ReleasePhase,
    ReleaseStatus,
    RequiredCheck,
)
from scripts.release.qualification import STARTUP_FAILURE_JOB
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
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        if marked:
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
            # The marker comment carries the candidate fingerprint and names
            # the failed run so the human sees which one was retried.
            bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
            autofix = next(b for b in bodies if ":autofix:" in b)
            assert (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:qual-retry:"
                    f"{fingerprint} -->") in autofix
            assert "[run 901](https://x/qruns/901)" in autofix

    def test_no_dispatch_outside_qualification_phase(self) -> None:
        status = _status(phase=ReleasePhase.CANDIDATE)
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=tracker())
        dispatch.assert_not_called()


class TestQualificationRetryComment:
    @pytest.mark.parametrize("author", [
        pytest.param("drive-by", id="drive-by"),
        pytest.param("valkeyrie-ops", id="bare-app-slug"),
        pytest.param("valkeyrie-ops[bot] ", id="trailing-space-lookalike"),
    ])
    def test_autofix_marker_from_untrusted_author_does_not_suppress(
        self, author: str,
    ) -> None:
        # A spoofer pasting the marker (any lookalike of the trusted login)
        # must not eat the one retry.
        status = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        issue = tracker()
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        spoof = MagicMock()
        spoof.user.login = author
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

    def test_marker_for_a_different_candidate_does_not_suppress(self) -> None:
        # Positive re-arm: the once-per-candidate gate is per SHA, so a
        # marker from a previous candidate must not eat the new dispatch.
        repo = self._dispatchable_repo()
        issue = tracker()
        other_fingerprint = hashlib.sha256(MOVED_SHA.encode("utf-8")).hexdigest()[:12]
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:build-dispatch:"
                       f"{other_fingerprint} -->\ndispatched")
        issue.get_comments.return_value = [posted]
        performed = actions.advance(gh_mock(repo), _POLICY,
                                    status=self._failed_trigger_status(),
                                    tracking_issue=issue)
        repo.get_workflow.return_value.create_dispatch.assert_called_once()
        assert any("auto-dispatched build-release" in p for p in performed)

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


class TestAutofixDispatchFailure:
    """A failing dispatch must not escape advance(): notify/nudge/render
    still run, and a plain follow-up comment tells the human the tracker's
    'Dispatching' callout did not land."""

    def _failed_trigger_status(self) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.PUBLISHED, published=True,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail="The release trigger run failed before dispatching "
                       "the build.",
                url="https://x/runs/55", action="dispatch-build-release"),),
        )

    def _failed_qualification_status(self) -> ReleaseStatus:
        return _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))

    def test_rejected_build_dispatch_posts_followup_and_still_notifies(self) -> None:
        repo = MagicMock()
        repo.default_branch = "main"
        # The rejected-dispatch path: create_dispatch returns False, so
        # _dispatch_build_release raises RuntimeError.
        repo.get_workflow.return_value.create_dispatch.return_value = False
        issue = tracker()
        performed = actions.advance(gh_mock(repo), _POLICY,
                                    status=self._failed_trigger_status(),
                                    tracking_issue=issue)
        assert not any("auto-dispatched" in p for p in performed)
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        # Marker-first ordering is preserved: the autofix marker posted.
        assert any(":autofix:build-dispatch:" in b for b in bodies)
        followup = next(b for b in bodies if "Auto-remediation failed:" in b)
        assert "> [!WARNING]" in followup
        assert "The dispatch itself failed." in followup
        assert "Dispatch build-release for `9.1.1` manually." in followup
        assert ":autofix:" not in followup
        assert "\u2014" not in followup
        # The failure notification in the same advance() call still fires.
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in b for b in bodies)

    def test_raising_qualification_dispatch_posts_followup_and_still_notifies(self) -> None:
        issue = tracker()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")):
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=self._failed_qualification_status(),
                                        tracking_issue=issue)
        assert not any("auto-retried" in p for p in performed)
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        assert any(":autofix:qual-retry:" in b for b in bodies)
        followup = next(b for b in bodies if "Auto-remediation failed:" in b)
        assert "> [!WARNING]" in followup
        assert "The dispatch itself failed." in followup
        assert "Dispatch the qualification workflow for `9.1.1` manually." in followup
        assert ":autofix:" not in followup
        assert "\u2014" not in followup
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in b for b in bodies)


class TestMarkerBeforeDispatchOrdering:
    """The documented safety crux: the autofix marker posts BEFORE the
    dispatch runs (fail closed), so a refactor to dispatch-first fails here."""

    def test_build_dispatch_runs_only_after_the_marker_posted(self) -> None:
        repo = MagicMock()
        repo.default_branch = "main"
        issue = tracker()

        def _assert_marker_already_posted(*args: object, **kwargs: object) -> bool:
            bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
            assert any(":autofix:build-dispatch:" in b for b in bodies), \
                "dispatch ran before the autofix marker posted"
            return True

        repo.get_workflow.return_value.create_dispatch.side_effect = \
            _assert_marker_already_posted
        status = _status(
            phase=ReleasePhase.PUBLISHED, published=True,
            qualification=QualificationStatus(run_id=1, passed=True),
            outputs=(DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail="The release trigger run failed before dispatching "
                       "the build.",
                url="https://x/runs/55", action="dispatch-build-release"),),
        )
        actions.advance(gh_mock(repo), _POLICY, status=status, tracking_issue=issue)
        repo.get_workflow.return_value.create_dispatch.assert_called_once()

    def test_qualification_retry_runs_only_after_the_marker_posted(self) -> None:
        issue = tracker()

        def _assert_marker_already_posted(*args: object, **kwargs: object) -> None:
            bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
            assert any(":autofix:qual-retry:" in b for b in bodies), \
                "dispatch ran before the autofix marker posted"

        status = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=_assert_marker_already_posted) as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
        dispatch.assert_called_once()


class TestOutputActions:
    @pytest.mark.parametrize(("version", "stage", "expected_tag"), [
        pytest.param("9.1.1", "ga", "9.1.1", id="ga-bare-version"),
        pytest.param("9.2.0", "rc1", "9.2.0-rc1", id="rc-suffixed-tag"),
    ])
    def test_bundle_dispatch_fires_the_repository_dispatch(
        self, version: str, stage: str, expected_tag: str,
    ) -> None:
        status = _status(
            version=version, stage=stage,
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
            client_payload={"version": expected_tag, "component": "valkey"},
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
        assert "**@valkey-io/core-team: Release `9.1.1` Needs Attention.**" in body
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

    @pytest.mark.parametrize("author", [
        pytest.param("drive-by", id="drive-by"),
        pytest.param("Valkeyrie-Ops[bot]", id="case-variant"),
        pytest.param("valkeyrie-ops", id="bare-app-slug"),
    ])
    def test_notify_marker_from_untrusted_author_does_not_suppress(
        self, author: str,
    ) -> None:
        # A spoofer pasting the marker (under any lookalike of the trusted
        # login) must not silence real alerts.
        status = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED),),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        marker_body = issue.create_comment.call_args.kwargs["body"]
        spoof = MagicMock()
        spoof.user.login = author
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

    def test_output_wording_change_does_not_notify_again(self) -> None:
        # The fingerprint hashes stable keys, not prose: a reworded detail
        # for the same failed output must not re-ping the team.
        issue = tracker()
        first = _status(outputs=(DownstreamOutput(
            name="helm", state=OutputState.FAILED, detail="Old wording"),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=first, tracking_issue=issue)
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        second = _status(outputs=(DownstreamOutput(
            name="helm", state=OutputState.FAILED, detail="New wording"),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=second, tracking_issue=issue)

        issue.create_comment.assert_not_called()

    def test_qualification_notification_names_the_run_and_a_new_run_repings(self) -> None:
        # A failed retry produces a NEW run id: it must re-notify exactly
        # once, and the text must name the run so the human sees which one.
        issue = tracker()
        first = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=first, tracking_issue=issue)
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        notify = next(b for b in bodies if ":notify:" in b)
        assert "**Qualification run 901 failed:** job" in notify
        replayed = []
        for body in bodies:
            posted = MagicMock()
            posted.user.login = "valkeyrie-ops[bot]"
            posted.body = body
            replayed.append(posted)
        issue.get_comments.return_value = replayed
        issue.create_comment.reset_mock()

        second = _status(qualification=QualificationStatus(
            run_id=902, url="https://x/qruns/902", failed_jobs=("job",)))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=second, tracking_issue=issue)

        renotify = next(b for b in (c.kwargs["body"] for c in
                                    issue.create_comment.call_args_list)
                        if ":notify:" in b)
        assert "**Qualification run 902 failed:** job" in renotify

    def test_no_failures_means_no_notification(self) -> None:
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=_status(qualification=QualificationStatus(run_id=1, passed=True)),
                        tracking_issue=issue)
        issue.create_comment.assert_not_called()

    def test_duplicate_output_keys_do_not_collapse_the_fingerprint(self) -> None:
        # Two failed outputs with the same name AND run_id produce the same
        # stable key twice. The fingerprint joins the sorted key list, so
        # the duplicate is preserved and the two-failure state hashes
        # differently from the one-failure state: the second failure still
        # re-pings. (Duplicate names cannot happen today, the verifier emits
        # unique names; this pins the machinery for when that changes.)
        issue = tracker()
        one = _status(outputs=(DownstreamOutput(
            name="pages", state=OutputState.FAILED, detail="first", run_id=7),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=one, tracking_issue=issue)
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        two = _status(outputs=(
            DownstreamOutput(name="pages", state=OutputState.FAILED,
                             detail="first", run_id=7),
            DownstreamOutput(name="pages", state=OutputState.FAILED,
                             detail="second", run_id=7),
        ))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=two, tracking_issue=issue)

        issue.create_comment.assert_called_once()
        body = issue.create_comment.call_args.kwargs["body"]
        assert "| 1 | **pages:** first |" in body
        assert "| 2 | **pages:** second |" in body

    def test_output_failure_with_run_id_zero_then_a_real_run_repings(self) -> None:
        # An output can fail before any run exists (run_id 0). Once a real
        # run id appears for the same output, the key changes and the team
        # is pinged again exactly once. A later relapse BACK to run_id 0
        # reuses the original key, whose marker is still on the issue, so it
        # stays suppressed: markers never expire. Accepted per the source
        # comment ("run_id may be 0/empty; included anyway so a NEW failed
        # run (with a real id) re-pings").
        issue = tracker()
        replayed: "list[MagicMock]" = []

        def _advance(status: ReleaseStatus) -> None:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
            for call in issue.create_comment.call_args_list:
                posted = MagicMock()
                posted.user.login = "valkeyrie-ops[bot]"
                posted.body = call.kwargs["body"]
                replayed.append(posted)
            issue.get_comments.return_value = list(replayed)
            issue.create_comment.reset_mock()

        def _failed(run_id: int, detail: str) -> ReleaseStatus:
            return _status(outputs=(DownstreamOutput(
                name="pages", state=OutputState.FAILED,
                detail=detail, run_id=run_id),))

        _advance(_failed(0, "no run yet"))
        assert len(replayed) == 1  # first ping

        _advance(_failed(55, "run 55 failed"))
        assert len(replayed) == 2  # new run id, second ping

        _advance(_failed(0, "run vanished"))
        assert len(replayed) == 2  # back to the old key: suppressed

    def test_same_check_failing_on_a_new_candidate_notifies_again(self) -> None:
        issue = tracker()
        first = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.FAILED, url="https://x/run/1"),),
        )
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=first, tracking_issue=issue)
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        # The branch moved, the new head was adopted, and the same required
        # check failed again in a NEW run on the NEW candidate SHA.
        second = _status(
            candidate=Candidate(state=CandidateState.ADOPTED, sha=MOVED_SHA,
                                branch_head=MOVED_SHA),
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.FAILED, url="https://x/run/2"),),
        )
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=second, tracking_issue=issue)

        issue.create_comment.assert_called_once()


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
        assert "**@valkey-io/core-team: Action Needed for `9.1.1`.**" in body
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
        assert "**@valkey-io/core-team: Action Needed for `9.1.1`.**" in body
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
        # The nudge state is computed from the recomputed status, never from
        # the tracker body: a stale body still claiming the PR is unmerged
        # must not produce a nudge once the status says merged.
        issue = tracker()
        issue.body = (issue_mod.identity_marker("9.1")
                      + "\nRelease-notes PR #42: Open (not merged)")
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=_status(notes_pr_number=42, notes_pr_merged=True,
                                       qualification=QualificationStatus(run_id=1, passed=True)),
                        tracking_issue=issue)
        issue.create_comment.assert_not_called()

    def test_nudge_marker_from_untrusted_author_does_not_suppress(self) -> None:
        # The nudge gate reads only trusted comments; a bare-slug spoofer
        # pasting the exact marker must not silence the one-time nudge.
        issue = tracker()
        fingerprint = hashlib.sha256(b"notes-pr:42").hexdigest()[:12]
        spoof = MagicMock()
        spoof.user.login = "valkeyrie-ops"
        spoof.body = f"<!-- {issue_mod.MARKER_NAMESPACE}:nudge:{fingerprint} -->"
        issue.get_comments.return_value = [spoof]

        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._notes_pr_open(), tracking_issue=issue)

        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:nudge:" in b for b in bodies)

    def test_unmerged_notes_pr_outranks_branch_moved_for_the_single_nudge(self) -> None:
        # Both awaiting-human states at once (unreachable via compute_status
        # today, which zeroes the candidate while the notes PR is unmerged,
        # but _nudge_item must stay safe if that invariant shifts): exactly
        # one nudge posts, and it is the notes-PR one. Judgment: correct
        # precedence; without a merged notes PR there is no candidate to
        # adopt, so merging is the only actionable ask.
        status = _status(
            phase=ReleasePhase.NOTES,
            notes_pr_number=42, notes_pr_url="https://x/pull/42",
            notes_pr_merged=False,
            candidate=Candidate(state=CandidateState.INVALIDATED, sha=MERGE_SHA,
                                branch_head=MOVED_SHA),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        issue.create_comment.assert_called_once()
        body = issue.create_comment.call_args.kwargs["body"]
        assert "Review and merge the release-notes PR" in body
        assert "moved to" not in body


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
        # The dispatch binds the run to the exact tag and candidate (F16);
        # the workflow stamps them into its run-name for correlation.
        workflow.create_dispatch.assert_called_once_with(
            gh_agent.get_repo.return_value.default_branch,
            inputs={"branch": "9.1", "tag": "9.1.1", "candidate_sha": MERGE_SHA},
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


_AGENT_HEAD = "d" * 40
_STALE_HEAD = "e" * 40


def _publish_run(*, head_sha: str, status: str = "waiting",
                 branch: str = "9.1", run_id: int = 77,
                 conclusion: "str | None" = None,
                 tag: str = "", candidate_sha: str = "") -> MagicMock:
    binding = ""
    if tag and candidate_sha:
        binding = f" · {tag} @ {candidate_sha}"
    run = MagicMock(status=status, conclusion=conclusion, head_sha=head_sha,
                    id=run_id,
                    display_title=f"Publish release on {branch}{binding} "
                                  f"(requested by x)",
                    html_url=f"https://x/actions/runs/{run_id}")
    run.cancel.return_value = True
    return run


def _runs_by_status(runs: "list[MagicMock]") -> "MagicMock":
    """A workflow mock whose get_runs honors the server-side status filter
    (F17): get_runs(status=X) serves only the runs whose status is X."""
    workflow = MagicMock()
    workflow.get_runs.side_effect = lambda status="": [
        r for r in runs if r.status == status
    ]
    return workflow


def _agent_with_workflow(workflow: MagicMock,
                         head: str = _AGENT_HEAD) -> MagicMock:
    gh_agent = MagicMock()
    gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = head
    gh_agent.get_repo.return_value.get_workflow.return_value = workflow
    return gh_agent


class TestStalePublishRuns:
    """A publish run parked at the approval gate on old controller code is
    stale: it is cancelled and replaced, never left to publish stale logic."""

    def _agent(self, runs: "list") -> MagicMock:
        gh_agent = MagicMock()
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = _AGENT_HEAD
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = runs
        return gh_agent

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_stale_run_is_cancelled_and_a_fresh_dispatch_proceeds(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        gh_agent = self._agent([stale])
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(), tracking_issue=tracker(),
                                    gh_agent=gh_agent, agent_repo="o/agent",
                                    agent_head_sha=_AGENT_HEAD)
        stale.cancel.assert_called_once()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_called_once()
        assert any("publish pipeline" in p for p in performed)

    def test_matching_head_waiting_run_still_blocks(self) -> None:
        current = _publish_run(head_sha=_AGENT_HEAD)
        gh_agent = self._agent([current])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        current.cancel.assert_not_called()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_cancel_failure_means_the_run_stays_active_fail_safe(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        stale.cancel.side_effect = GithubException(403, "forbidden", {})
        gh_agent = self._agent([stale])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_rejected_cancel_means_the_run_stays_active_fail_safe(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        stale.cancel.return_value = False
        gh_agent = self._agent([stale])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_no_head_sha_disables_staleness_detection(self) -> None:
        # "" means the trusted head could not be resolved: fail safe, the
        # waiting run blocks and nothing is cancelled (today's behavior).
        stale = _publish_run(head_sha=_STALE_HEAD)
        gh_agent = self._agent([stale])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent")
        stale.cancel.assert_not_called()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_stale_run_is_never_presented_as_the_approval_link(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        gh_agent = self._agent([stale])
        url = actions.waiting_publish_run_url(gh_agent, "o/agent", "9.1",
                                              _AGENT_HEAD)
        assert url == ""

    def test_agent_head_sha_resolves_the_default_branch_head(self) -> None:
        gh_agent = self._agent([])
        assert actions.agent_head_sha(gh_agent, "o/agent") == _AGENT_HEAD

    def test_agent_head_sha_is_empty_when_unresolvable(self) -> None:
        gh_agent = MagicMock()
        gh_agent.get_repo.side_effect = GithubException(404, "not found", {})
        assert actions.agent_head_sha(gh_agent, "o/agent") == ""


class TestAlertsBlockCompletion:
    def test_complete_phase_with_a_standing_alert_never_closes(self) -> None:
        # Defense in depth for the untrusted-tag alert: even if a status
        # ever reported COMPLETE alongside an alert, the tracker must stay
        # open for a human.
        status = _status(
            phase=ReleasePhase.COMPLETE, published=True,
            alerts=("Release tag 9.1.1 points at eeeeeeeeeeee, which was "
                    "never a trusted candidate (notes merge or "
                    "owner-adopted). Manual investigation required.",),
        )
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        issue.edit.assert_not_called()


class TestWaitingPublishRunUrl:
    """The display-only sibling of _publish_run_active: same matching, but
    yields the run's URL for the READY callout's approval link."""

    def _agent(self, runs: "list | None" = None) -> MagicMock:
        gh_agent = MagicMock()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.get_runs.return_value = runs or []
        return gh_agent

    def test_waiting_run_for_the_branch_yields_its_url(self) -> None:
        waiting = MagicMock(status="waiting",
                            display_title="Publish release on 9.1 (requested by x)",
                            html_url="https://x/actions/runs/500")
        url = actions.waiting_publish_run_url(self._agent([waiting]), "o/agent", "9.1")
        assert url == "https://x/actions/runs/500"

    def test_no_active_run_yields_empty(self) -> None:
        done = MagicMock(status="completed",
                         display_title="Publish release on 9.1 (requested by x)",
                         html_url="https://x/actions/runs/400")
        assert actions.waiting_publish_run_url(self._agent([done]), "o/agent", "9.1") == ""

    def test_other_branch_run_yields_empty(self) -> None:
        other = MagicMock(status="waiting",
                          display_title="Publish release on 8.0 (requested by x)",
                          html_url="https://x/actions/runs/300")
        assert actions.waiting_publish_run_url(self._agent([other]), "o/agent", "9.1") == ""

    def test_unreadable_workflow_yields_empty_not_an_error(self) -> None:
        # _publish_run_active fails closed (True) here; the URL companion
        # is display-only, so it degrades to "no link" instead.
        gh_agent = MagicMock()
        gh_agent.get_repo.return_value.get_workflow.side_effect = GithubException(
            404, "not found", {},
        )
        assert actions.waiting_publish_run_url(gh_agent, "o/agent", "9.1") == ""


class _IssueHarness:
    """Tracker double for multi-pass flows: create_comment accumulates
    bot-authored comments, comment.edit rewrites them in place, and
    get_comments always serves the accumulated list, so consecutive
    advance() passes see exactly what a real tracker would."""

    def __init__(self) -> None:
        self.issue = tracker()
        self.comments: "list[MagicMock]" = []
        self.issue.create_comment.side_effect = self._create
        self.issue.get_comments.side_effect = lambda: list(self.comments)

    def _create(self, body: str) -> MagicMock:
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = body
        posted.edit.side_effect = (
            lambda body, _c=posted: setattr(_c, "body", body)
        )
        self.comments.append(posted)
        return posted

    def bodies(self, fragment: str) -> "list[str]":
        return [c.body for c in self.comments if fragment in c.body]


class TestStartupFailureRetry:
    """F14: a startup-failed qualification run carries its run id (it is a
    failed run, not no-evidence), so it goes through the marker-gated
    one-retry path: exactly one more dispatch, never a loop, and the
    failure notification stands."""

    def _startup_failed(self) -> ReleaseStatus:
        return _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901",
            failed_jobs=(STARTUP_FAILURE_JOB,)))

    def test_startup_failure_dispatches_exactly_once_more(self) -> None:
        harness = _IssueHarness()
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=self._startup_failed(),
                            tracking_issue=harness.issue)
        dispatch.assert_called_once()
        assert dispatch.call_args.kwargs == {"tag": "9.1.1", "sha": MERGE_SHA}
        assert harness.bodies(":autofix:qual-retry:")

    def test_second_startup_failure_never_dispatches_again(self) -> None:
        # The retry itself also failed at startup: NEW run id, same
        # candidate. The per-candidate marker stops any further dispatch.
        harness = _IssueHarness()
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=self._startup_failed(),
                            tracking_issue=harness.issue)
            second = _status(qualification=QualificationStatus(
                run_id=902, url="https://x/qruns/902",
                failed_jobs=(STARTUP_FAILURE_JOB,)))
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=second, tracking_issue=harness.issue)
            # And a third pass over the same state stays quiet: no loop.
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=second, tracking_issue=harness.issue)
        dispatch.assert_called_once()

    def test_startup_failure_notifies_and_names_the_sentinel(self) -> None:
        harness = _IssueHarness()
        with patch.object(actions.qual_mod, "dispatch_qualification"):
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=self._startup_failed(),
                            tracking_issue=harness.issue)
        notify = harness.bodies(":notify:")
        assert len(notify) == 1
        assert "**Qualification run 901 failed:** (Workflow startup failed)" \
            in notify[0]


class TestPublishHalt:
    """F15: with no active publish run, a newest COMPLETED run for this
    branch at the current controller head that concluded failure/cancelled
    halts re-dispatch (one-shot warning); a new controller head or a new
    candidate re-arms."""

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def _failed_run(self, *, head_sha: str = _AGENT_HEAD,
                    candidate_sha: str = MERGE_SHA,
                    conclusion: str = "failure") -> MagicMock:
        return _publish_run(status="completed", conclusion=conclusion,
                            head_sha=head_sha, run_id=88,
                            tag="9.1.1", candidate_sha=candidate_sha)

    def test_failed_completed_run_halts_dispatch_and_notifies_once(self) -> None:
        workflow = _runs_by_status([self._failed_run()])
        gh_agent = _agent_with_workflow(workflow)
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(),
                                    tracking_issue=harness.issue,
                                    gh_agent=gh_agent, agent_repo="o/agent",
                                    agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_not_called()
        assert any("halted publish re-dispatch" in p for p in performed)
        halt = harness.bodies(":autofix:publish-halt:")
        assert len(halt) == 1
        assert "> [!WARNING]" in halt[0]
        assert ("the controller will not re-dispatch until the controller "
                "code changes or a human re-runs it") in halt[0]
        assert "[run 88](https://x/actions/runs/88)" in halt[0]
        assert "\u2014" not in halt[0]
        # Second pass: still no dispatch, no second warning.
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=harness.issue,
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_not_called()
        assert len(harness.bodies(":autofix:publish-halt:")) == 1

    @pytest.mark.parametrize("conclusion", ["failure", "cancelled"])
    def test_both_failure_and_cancelled_halt(self, conclusion: str) -> None:
        workflow = _runs_by_status([self._failed_run(conclusion=conclusion)])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_not_called()

    def test_new_controller_head_rearms_dispatch(self) -> None:
        # The failed run executed OLD controller code; the code changed, so
        # dispatch proceeds (that is the documented way out of the halt).
        workflow = _runs_by_status([self._failed_run(head_sha=_STALE_HEAD)])
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(),
                                    tracking_issue=harness.issue,
                                    gh_agent=_agent_with_workflow(workflow),
                                    agent_repo="o/agent",
                                    agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_called_once()
        assert any("dispatched the publish pipeline" in p for p in performed)
        assert not harness.bodies(":autofix:publish-halt:")

    def test_new_candidate_rearms_dispatch(self) -> None:
        # The failed run was bound (via its run-name) to the PREVIOUS
        # candidate; the current candidate is new, so dispatch proceeds.
        workflow = _runs_by_status(
            [self._failed_run(candidate_sha=MOVED_SHA)])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_called_once()

    def test_successful_completed_run_does_not_halt(self) -> None:
        workflow = _runs_by_status(
            [self._failed_run(conclusion="success")])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_called_once()


class TestCandidateBoundPublish:
    """F16: publish runs correlate by branch AND, when the run-name carries
    them, tag + candidate. A gate-parked run for a different candidate is
    stale (cancelled, not active); its URL is never the approval link."""

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_other_candidates_waiting_run_is_cancelled_and_replaced(self) -> None:
        # Candidate A (MOVED_SHA) is parked at the gate; candidate B
        # (MERGE_SHA) is now current: A's run is stale.
        parked = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                              candidate_sha=MOVED_SHA)
        workflow = _runs_by_status([parked])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        parked.cancel.assert_called_once()
        workflow.create_dispatch.assert_called_once()

    def test_current_candidates_waiting_run_blocks_and_is_kept(self) -> None:
        parked = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                              candidate_sha=MERGE_SHA)
        workflow = _runs_by_status([parked])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        parked.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_unbound_manual_run_still_blocks_dispatch(self) -> None:
        # Manual dispatch without inputs has no binding in its run-name; it
        # cannot be proven stale by candidate, so it counts as active.
        parked = _publish_run(head_sha=_AGENT_HEAD)
        workflow = _runs_by_status([parked])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        parked.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_url_is_never_shown_for_a_mismatched_candidate(self) -> None:
        parked = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                              candidate_sha=MOVED_SHA)
        gh_agent = _agent_with_workflow(_runs_by_status([parked]))
        url = actions.waiting_publish_run_url(
            gh_agent, "o/agent", "9.1", _AGENT_HEAD,
            tag="9.1.1", candidate_sha=MERGE_SHA,
        )
        assert url == ""
        parked.cancel.assert_not_called()  # display path never cancels

    def test_url_is_shown_for_the_matching_candidate(self) -> None:
        parked = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                              candidate_sha=MERGE_SHA)
        gh_agent = _agent_with_workflow(_runs_by_status([parked]))
        url = actions.waiting_publish_run_url(
            gh_agent, "o/agent", "9.1", _AGENT_HEAD,
            tag="9.1.1", candidate_sha=MERGE_SHA,
        )
        assert url == "https://x/actions/runs/77"


class TestServerSideRunFiltering:
    """F17: active runs are found via server-side status filters, so a
    long-waiting run can never fall out of a newest-N window no matter how
    many completed runs pile up above it."""

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_waiting_run_older_than_many_completed_runs_still_blocks(self) -> None:
        completed = [
            _publish_run(status="completed", conclusion="success",
                         head_sha=_AGENT_HEAD, run_id=1000 + n, branch="8.0")
            for n in range(20)
        ]
        waiting = _publish_run(head_sha=_AGENT_HEAD, run_id=5,
                               tag="9.1.1", candidate_sha=MERGE_SHA)
        # Newest-first listing: 20 completed runs precede the waiting one.
        workflow = _runs_by_status(completed + [waiting])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_not_called()
        # The lookup queried the server-side filter, not the raw listing.
        statuses = {c.kwargs.get("status") for c in
                    workflow.get_runs.call_args_list}
        assert "waiting" in statuses


class TestNeverCancelInProgress:
    """F18: an in_progress run is past the approval gate; it is always
    active and must never be cancelled, whatever its head or candidate."""

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_in_progress_stale_head_run_is_kept_and_blocks(self) -> None:
        running = _publish_run(status="in_progress", head_sha=_STALE_HEAD)
        workflow = _runs_by_status([running])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        running.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_in_progress_other_candidate_run_is_kept_and_blocks(self) -> None:
        running = _publish_run(status="in_progress", head_sha=_AGENT_HEAD,
                               tag="9.1.1", candidate_sha=MOVED_SHA)
        workflow = _runs_by_status([running])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        running.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_cancel_step_refuses_an_in_progress_run(self) -> None:
        # Defense in depth: even a hand-built list cannot cancel past the gate.
        running = _publish_run(status="in_progress", head_sha=_STALE_HEAD)
        assert actions.cancel_stale_publish_runs([running], act=True) is False
        running.cancel.assert_not_called()


class TestFinderCancelSplit:
    """F19a: find_publish_runs is pure (never cancels); the cancel step is
    separate and a strict no-op with act False."""

    def test_finder_never_calls_cancel(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                             candidate_sha=MOVED_SHA)
        workflow = _runs_by_status([stale])
        active, found_stale = actions.find_publish_runs(
            workflow, "9.1", _AGENT_HEAD, tag="9.1.1",
            candidate_sha=MERGE_SHA,
        )
        assert active is None
        assert found_stale == [stale]
        stale.cancel.assert_not_called()

    def test_cancel_step_with_act_false_is_a_no_op(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        assert actions.cancel_stale_publish_runs([stale], act=False) is False
        stale.cancel.assert_not_called()

    def test_cancel_step_with_act_true_cancels(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD)
        assert actions.cancel_stale_publish_runs([stale], act=True) is True
        stale.cancel.assert_called_once()


class TestWedgeNudge:
    """F24: a MISSING required check or a MISSING/STALE daily gate never
    resolves on its own; a one-shot marker-gated nudge says so. No
    time-based grace this round: the observed state is the trigger."""

    def _missing_check(self) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.CANDIDATE,
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.MISSING),),
        )

    def test_missing_check_nudges_once(self) -> None:
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._missing_check(),
                                    tracking_issue=harness.issue)
        wedges = harness.bodies(":wedge:")
        assert len(wedges) == 1
        assert "> [!IMPORTANT]" in wedges[0]
        assert ("**@valkey-io/core-team: Release `9.1.1` Is Blocked Without "
                "Progress.**") in wedges[0]
        assert ("Blocked without progress: Required check "
                "`test-ubuntu-latest` has no run on the candidate SHA. "
                "This does not resolve on its own.") in wedges[0]
        assert "\u2014" not in wedges[0]
        assert any("wedged gate" in p for p in performed)
        # Same state again: suppressed.
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 1

    def test_stale_daily_gate_nudges_with_its_detail(self) -> None:
        harness = _IssueHarness()
        status = _status(
            phase=ReleasePhase.QUALIFICATION,
            daily=DailyCiStatus(state=DailyCiState.STALE, run_id=77,
                                url="https://x/druns/77",
                                detail="The newest daily run is 30 hours old"),
        )
        actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                        tracking_issue=harness.issue)
        wedges = harness.bodies(":wedge:")
        assert len(wedges) == 1
        assert ("Blocked without progress: The newest daily run is 30 hours "
                "old. This does not resolve on its own.") in wedges[0]

    def test_resolution_then_recurrence_renudges_exactly_once(self) -> None:
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 1
        # The check resolved: a clean pass bumps the recovery generation.
        actions.advance(gh, _POLICY,
                        status=_status(qualification=QualificationStatus(
                            run_id=1, passed=True)),
                        tracking_issue=harness.issue)
        assert harness.bodies(":notify-gen:1")
        # The same check goes MISSING again: new fingerprint, one re-nudge.
        actions.advance(gh, _POLICY, status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 2
        # And a repeat of that state is suppressed again.
        actions.advance(gh, _POLICY, status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 2

    def test_missing_check_is_not_a_failure_item(self) -> None:
        # MISSING escalates through the wedge nudge, never the failure
        # notifier (whose table is for FAILED/STALLED states).
        harness = _IssueHarness()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert not harness.bodies(":notify:")


class TestRecoveryGenerations:
    """F25: fingerprints hash (recovery generation, sorted keys); a clean
    pass advances the generation in one edit-in-place marker comment, so a
    failure recurring after recovery re-pings exactly once."""

    def _failing(self) -> ReleaseStatus:
        return _status(
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.FAILED),),
        )

    def _clean(self) -> ReleaseStatus:
        return _status(qualification=QualificationStatus(run_id=1, passed=True))

    def test_recurrence_after_recovery_repings_exactly_once(self) -> None:
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":notify:")) == 1
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)  # recovery
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)  # recurrence
        assert len(harness.bodies(":notify:")) == 2
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)  # steady: suppressed
        assert len(harness.bodies(":notify:")) == 2

    def test_steady_failure_never_repings_across_passes(self) -> None:
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        for _ in range(3):
            actions.advance(gh, _POLICY, status=self._failing(),
                            tracking_issue=harness.issue)
        assert len(harness.bodies(":notify:")) == 1

    def test_generation_marker_edits_in_place(self) -> None:
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        gen_comments = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments) == 1
        assert ":notify-gen:1 -->" in gen_comments[0].body
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        # Still exactly one bookkeeping comment, edited in place.
        gen_comments = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments) == 1
        assert ":notify-gen:2 -->" in gen_comments[0].body
        gen_comments[0].edit.assert_called_once()

    def test_clean_pass_without_history_posts_no_bookkeeping(self) -> None:
        harness = _IssueHarness()
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        assert harness.comments == []

    def test_alert_keys_survive_id_rewording(self) -> None:
        # The alert key strips hex ids and digits before hashing, so an
        # alert whose prose embeds a changing SHA keeps one identity.
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        first = _status(alerts=(f"Tag `9.1.1` exists (at `{'e' * 12}`) but "
                                f"no release does.",),
                        qualification=QualificationStatus(run_id=1, passed=True))
        actions.advance(gh, _POLICY, status=first, tracking_issue=harness.issue)
        assert len(harness.bodies(":notify:")) == 1
        second = _status(alerts=(f"Tag `9.1.2` exists (at `{'f' * 12}`) but "
                                 f"no release does.",),
                         qualification=QualificationStatus(run_id=1, passed=True))
        actions.advance(gh, _POLICY, status=second, tracking_issue=harness.issue)
        # Same identity: only the ids changed, no re-ping.
        assert len(harness.bodies(":notify:")) == 1


class TestNotesCutRunUrl:
    @staticmethod
    def _agent(runs_by_workflow):
        def get_workflow(name):
            workflow = MagicMock()
            workflow.get_runs.return_value = runs_by_workflow.get(name, [])
            return workflow
        agent_repo = MagicMock()
        agent_repo.get_workflow.side_effect = get_workflow
        gh_agent = MagicMock()
        gh_agent.get_repo.return_value = agent_repo
        return gh_agent

    @staticmethod
    def _run(title, status="in_progress", conclusion=None):
        return MagicMock(display_title=title, status=status,
                         conclusion=conclusion, html_url=f"https://x/runs/{title}")

    def test_active_chained_cut_is_linked(self) -> None:
        gh = self._agent({"release-start.yml": [
            self._run("Start Release on 8.0")]})
        url = actions.notes_cut_run_url(gh, "o/agent", "8.0")
        assert url == "https://x/runs/Start Release on 8.0"

    def test_failed_cut_is_linked_when_nothing_is_active(self) -> None:
        gh = self._agent({"release-start.yml": [
            self._run("Start Release on 8.0", status="completed",
                      conclusion="failure")]})
        assert "Start Release on 8.0" in actions.notes_cut_run_url(gh, "o/agent", "8.0")

    def test_other_branch_never_matches(self) -> None:
        gh = self._agent({"release-start.yml": [
            self._run("Start Release on 8.1")]})
        assert actions.notes_cut_run_url(gh, "o/agent", "8.0") == ""

    def test_standalone_cut_matches_only_with_a_bound_version(self) -> None:
        gh = self._agent({"release-notes-cut.yml": [
            self._run("Cut Release Notes 8.0.11")]})
        assert actions.notes_cut_run_url(gh, "o/agent", "8.0") == ""
        assert "8.0.11" in actions.notes_cut_run_url(gh, "o/agent", "8.0", "8.0.11")

    def test_lookup_failure_returns_empty(self) -> None:
        gh_agent = MagicMock()
        gh_agent.get_repo.side_effect = GithubException(500, {}, {})
        assert actions.notes_cut_run_url(gh_agent, "o/agent", "8.0") == ""
