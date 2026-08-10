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
