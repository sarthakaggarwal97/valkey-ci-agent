"""Tests for reconciliation's idempotent progress actions."""

from __future__ import annotations

import hashlib
import re
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
from tests.release_fixtures import (
    MERGE_SHA,
    MOVED_SHA,
    gh_mock,
    make_policy,
    publish_run,
    tracker,
)

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
        # Default qualification is a no-op for advance(): tests that
        # exercise dispatch/retry paths (TestQualificationDispatch,
        # TestStartupFailureRetry) override this explicitly. Every other
        # test focusing on notification / nudge / halt behaviour should
        # NOT trigger a side-dispatch just from picking the default
        # status - that would flood every fixture with two-phase
        # autofix-intent comments unrelated to what the test measures.
        "qualification": QualificationStatus(run_id=1, passed=True),
    }
    base.update(overrides)
    return ReleaseStatus(**base)  # type: ignore[arg-type]


class TestQualificationDispatch:
    def test_dispatches_when_no_run_exists(self) -> None:
        # The first qualification dispatch goes through the two-
        # phase autofix receipt, so a restart between dispatch and the
        # run appearing in the UI cannot dispatch twice. The intent
        # marker fingerprints the candidate SHA.
        status = _status(qualification=QualificationStatus())
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=status, tracking_issue=tracker())
        dispatch.assert_called_once()
        assert any("dispatched qualification" in p for p in performed)

    def test_first_dispatch_is_marker_gated(self) -> None:
        # With an autofix-done marker in place for this candidate,
        # a fresh advance() (still seeing an empty QualificationStatus)
        # must not re-dispatch. The intent+done pair on a single trusted
        # comment is what a real successful dispatch leaves behind.
        status = _status(qualification=QualificationStatus())
        issue = tracker()
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = (
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-done:qual-dispatch:"
            f"{fingerprint} -->\n"
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:qual-dispatch:"
            f"{fingerprint} -->\ndispatched"
        )
        issue.get_comments.return_value = [posted]
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
        dispatch.assert_not_called()

    def test_dispatches_while_checks_are_still_pending(self) -> None:
        # Pin (d): required-check results are informational, so the first
        # qualification dispatch fires even while the candidate's checks
        # are still pending (or missing entirely).
        status = _status(
            qualification=QualificationStatus(),
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.PENDING),
                    RequiredCheck(name="build-macos-latest",
                                  state=CheckState.MISSING)),
        )
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
            posted.body = (
                f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-done:qual-retry:"
                f"{fingerprint} -->\n"
                f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:qual-retry:"
                f"{fingerprint} -->\nretried"
            )
            issue.get_comments.return_value = [posted]
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=status, tracking_issue=issue)
        if marked:
            dispatch.assert_not_called()
            assert not any("auto-retried" in p for p in performed)
        else:
            dispatch.assert_called_once()
            kwargs = dict(dispatch.call_args.kwargs)
            # The retry is a new dispatch, so it carries its own nonce
            # (recorded on the intent receipt below).
            nonce = kwargs.pop("nonce")
            assert re.fullmatch(r"[0-9a-f]{32}", nonce)
            assert kwargs == {"tag": "9.1.1", "sha": MERGE_SHA}
            assert any("auto-retried qualification" in p for p in performed)
            # The intent marker carries the candidate fingerprint and
            # names the failed run so the human sees which one was
            # retried. The done marker is stamped in place on the intent
            # comment after dispatch success (the two-phase intent/done
            # receipt).
            bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
            intent = next(b for b in bodies if ":autofix-intent:qual-retry:" in b)
            assert (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:qual-retry:"
                    f"{fingerprint} -->") in intent
            # The receipt records the dispatched nonce so the evaluator
            # can require the manifest to echo it, and the VISIBLE part of
            # the callout renders it too (an integrity binding, not a
            # secret) so a manual re-dispatcher can copy it.
            assert issue_mod.qual_nonce_marker(MERGE_SHA, nonce) in intent
            assert f"Dispatch nonce: `{nonce}`" in intent
            assert "[run 901](https://x/qruns/901)" in intent
            # Done stamp lands via edit on the intent comment.
            created = issue.create_comment.return_value
            edit_bodies = [c.kwargs["body"] for c in created.edit.call_args_list]
            assert any(
                (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-done:qual-retry:"
                 f"{fingerprint} -->") in body for body in edit_bodies
            ), "expected the done marker to be stamped on the intent comment"

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
        repo.get_workflow.assert_any_call("build-release.yml")
        # The payload carries source_sha (the exact candidate commit)
        # so the automation repo can verify its checkout against the
        # commit the controller vetted, not just the tag ref.
        repo.get_workflow.return_value.create_dispatch.assert_called_once_with(
            "main", inputs={"version": "9.1.1", "environment": "prod",
                            "source_sha": MERGE_SHA},
        )
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        intent = next(b for b in bodies if ":autofix-intent:build-dispatch:" in b)
        assert (f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:build-dispatch:"
                f"{fingerprint} -->") in intent
        assert "> [!NOTE]" in intent
        assert "**Auto-remediation:** Dispatching the build pipeline for `9.1.1`" in intent
        assert "[release trigger run](https://x/runs/55)" in intent
        assert "\u2014" not in intent
        # Done stamp lands via edit on the intent comment (two-phase receipt).
        created = issue.create_comment.return_value
        edit_bodies = [c.kwargs["body"] for c in created.edit.call_args_list]
        assert any(":autofix-done:build-dispatch:" in b for b in edit_bodies)
        assert any("auto-dispatched build-release" in p for p in performed)

    def test_rc_dispatch_carries_the_tag_as_version(self) -> None:
        repo = self._dispatchable_repo()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._failed_trigger_status(version="9.2.0", stage="rc1"),
                        tracking_issue=tracker())
        repo.get_workflow.return_value.create_dispatch.assert_called_once_with(
            "main", inputs={"version": "9.2.0-rc1", "environment": "prod",
                            "source_sha": MERGE_SHA},
        )

    def test_marked_candidate_never_dispatches_again(self) -> None:
        # Once per candidate SHA, even across distinct failed trigger runs.
        # The done marker (not the old single-shot marker) is what
        # signals "completed successfully" and suppresses re-dispatch.
        repo = self._dispatchable_repo()
        issue = tracker()
        fingerprint = hashlib.sha256(MERGE_SHA.encode("utf-8")).hexdigest()[:12]
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = (
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-done:build-dispatch:"
            f"{fingerprint} -->\n"
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:build-dispatch:"
            f"{fingerprint} -->\ndispatched"
        )
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
        posted.body = (
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-done:build-dispatch:"
            f"{other_fingerprint} -->\n"
            f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix-intent:build-dispatch:"
            f"{other_fingerprint} -->\ndispatched"
        )
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
        assert any(":autofix-intent:build-dispatch:" in b for b in bodies)

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
        # Intent-first ordering preserved: the intent marker posted even
        # though the follow-up dispatch was rejected. The next pass will
        # observe intent-without-done and retry once (two-phase receipt).
        assert any(":autofix-intent:build-dispatch:" in b for b in bodies)
        # The done marker is NOT stamped on the intent when dispatch
        # failed, so re-scanning the intent comment on a next pass
        # picks up "intent-only" and triggers the one bounded retry.
        created = issue.create_comment.return_value
        edit_bodies = [c.kwargs["body"] for c in created.edit.call_args_list]
        assert not any(":autofix-done:build-dispatch:" in b for b in edit_bodies)
        followup = next(b for b in bodies if "Auto-remediation failed:" in b)
        assert "> [!WARNING]" in followup
        assert "The dispatch itself failed." in followup
        assert "Dispatch build-release for `9.1.1` manually." in followup
        assert ":autofix" not in followup
        assert "\u2014" not in followup
        # The failure notification in the same advance() call still fires.
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in b for b in bodies)

    def test_raising_qualification_dispatch_posts_followup_and_still_notifies(self) -> None:
        issue = tracker()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")) as dispatch:
            performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                        status=self._failed_qualification_status(),
                                        tracking_issue=issue)
        assert not any("auto-retried" in p for p in performed)
        nonce = dispatch.call_args.kwargs["nonce"]
        bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
        assert any(":autofix-intent:qual-retry:" in b for b in bodies)
        # No done marker on the intent when dispatch raised (the gate
        # stays armed for the bounded retry).
        created = issue.create_comment.return_value
        edit_bodies = [c.kwargs["body"] for c in created.edit.call_args_list]
        assert not any(":autofix-done:qual-retry:" in b for b in edit_bodies)
        followup = next(b for b in bodies if "Auto-remediation failed:" in b)
        assert "> [!WARNING]" in followup
        assert "The dispatch itself failed." in followup
        # The manual instruction is self-contained: it carries the exact
        # recorded nonce to pass as the `nonce` input, because a manual
        # dispatch without it produces a run the evaluator ignores.
        assert (f"Dispatch the qualification workflow for `9.1.1` manually "
                f"with `{nonce}` as its `nonce` input") in followup
        assert ":autofix" not in followup
        assert "\u2014" not in followup
        assert any(f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in b for b in bodies)


class TestTwoPhaseAutofixRecovery:
    """The two-phase receipt (intent marker before dispatch, done
    marker after success) recovers from a crash between the two writes.

    - Dispatch raises after intent posted -> next pass retries once and
      stamps done regardless of outcome (bounded to two attempts total).
    - Dispatch succeeded but done-marker write was lost (or the follow-up
      pass simply never got there): a run-correlation lookup finds the
      run, the done marker is backfilled, no duplicate dispatch.
    """

    def _failed_qualification_status(self) -> ReleaseStatus:
        return _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))

    def test_intent_only_with_no_matching_run_retries_once_and_stamps_done(self) -> None:
        # Pass 1: intent posts, dispatch RAISES (so no done stamp).
        # Pass 2: sees intent-only, no matching run per _qual_retry_run_exists.
        #         Retries the dispatch (this time succeeding), stamps done.
        # Pass 3: sees done, suppresses.
        harness = _IssueHarness()
        status = self._failed_qualification_status()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        # After pass 1: intent posted, dispatch raised, no done.
        assert harness.bodies(":autofix-intent:qual-retry:")
        assert not harness.bodies(":autofix-done:qual-retry:")

        # Pass 2: dispatch succeeds. Correlation reports no matching run
        # (a fresh qualification.py._find_run miss), so the retry-once
        # path runs - dispatch fires exactly one more time, then done
        # stamps.
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch, \
             patch.object(actions.qual_mod, "_find_run", return_value=None):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        dispatch.assert_called_once()  # ONE retry only
        assert harness.bodies(":autofix-done:qual-retry:")

        # Pass 3: done marker suppresses further work.
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        dispatch.assert_not_called()

    def test_intent_only_with_matching_run_backfills_done_without_duplicate_dispatch(self) -> None:
        # Pass 1: dispatch RAISES after intent posted (so no done stamp).
        # Pass 2: correlation reports the run DOES exist (dispatch had
        #         actually landed on pass 1, only the done-stamp write
        #         was lost). Backfill done, do NOT dispatch again.
        harness = _IssueHarness()
        status = self._failed_qualification_status()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        assert harness.bodies(":autofix-intent:qual-retry:")
        assert not harness.bodies(":autofix-done:qual-retry:")

        # Pass 2: correlation reports a NEW run (id != failed 901).
        fake_run = MagicMock()
        fake_run.id = 902
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch, \
             patch.object(actions.qual_mod, "_find_run", return_value=fake_run):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        # Backfilled: no duplicate dispatch, and done is now stamped.
        dispatch.assert_not_called()
        assert harness.bodies(":autofix-done:qual-retry:")

    def test_dispatch_never_runs_without_the_intent_marker_first(self) -> None:
        # Fail closed: if the intent-post itself raises, the dispatch
        # must not run. Otherwise a crash after dispatch could leave the
        # action unrecorded forever.
        issue = tracker()
        # Make create_comment raise on the FIRST call (the intent post),
        # and succeed on later calls (notify).
        raised = {"once": False}

        def _first_call_raises(**kwargs: object) -> "object":
            if not raised["once"] and ":autofix-intent:" in kwargs.get("body", ""):
                raised["once"] = True
                raise RuntimeError("comment-post outage")
            return MagicMock()

        issue.create_comment.side_effect = _first_call_raises
        status = self._failed_qualification_status()
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            # advance() itself must not raise; the exception bubbles up
            # from the retry_github_call around intent-post, so we accept
            # either the raise or a controlled skip.
            try:
                actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                                tracking_issue=issue)
            except RuntimeError:
                pass
        # Whichever path was taken, dispatch DID NOT run without an
        # intent marker on file.
        dispatch.assert_not_called()

    def test_intent_only_with_dispatch_raising_twice_stops_at_two_attempts(self) -> None:
        # Belt-and-braces: even when the retry ALSO raises, done must
        # stamp so a third pass never dispatches again.
        harness = _IssueHarness()
        status = self._failed_qualification_status()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)  # attempt 1
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)  # attempt 2 (retry)
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)  # attempt 3 (must not fire)
        # Exactly one intent marker on the tracker.
        assert len(harness.bodies(":autofix-intent:qual-retry:")) == 1
        # Done is stamped after the retry-once path runs (regardless of
        # whether the retry itself succeeded), so pass 3 never dispatches.
        assert harness.bodies(":autofix-done:qual-retry:")

    def test_done_stamped_receipt_with_no_run_escalates_through_failure_notification(self) -> None:
        # THE WEDGE SCENARIO: intent posted, the retry dispatch also
        # failed, done stamped (suppressing further attempts), and NO
        # qualification run resulted. The done stamp must not be a silent
        # dead end: the human-facing escalation must already be on the
        # tracker, naming what to do, and later passes must stay quiet
        # without erasing it.
        harness = _IssueHarness()
        status = self._failed_qualification_status()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")), \
             patch.object(actions.qual_mod, "_find_run", return_value=None):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)  # intent + failed dispatch
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)  # failed retry + done stamp

        # The wedge state exists: done stamped, no run anywhere.
        assert harness.bodies(":autofix-done:qual-retry:")
        # Escalation 1: every failed dispatch posted the loud follow-up
        # naming the exact manual action (once per failed attempt),
        # including the recorded nonce a manual dispatch must echo. Both
        # attempts reuse the ONE recorded nonce (the standing intent's),
        # so both instructions carry the same value.
        followups = harness.bodies("Auto-remediation failed:")
        assert len(followups) == 2
        recorded = issue_mod.recorded_qualification_nonce(
            harness.issue, MERGE_SHA)
        assert recorded
        assert all(f"Dispatch the qualification workflow for `9.1.1` "
                   f"manually with `{recorded}` as its `nonce` input"
                   in body for body in followups)
        # Escalation 2: the standing failure notification mentioned the
        # team, naming the failed qualification run.
        notifications = harness.bodies(
            f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:")
        assert notifications
        assert any("Qualification run 901 failed" in body
                   for body in notifications)

        # Later passes: suppressed (no new dispatch), and QUIET, but the
        # escalation comments stand; the state is escalated-and-waiting,
        # never silently wedged.
        before = len(harness.comments)
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        dispatch.assert_not_called()
        assert len(harness.comments) == before


class TestQualificationDispatchNonce:
    """The per-dispatch nonce contract: the intent receipt records the
    nonce, the dispatch sends exactly that nonce, and a crashed dispatch's
    retry reuses the RECORDED nonce (a fresh one would diverge from the
    receipt and the evaluator would skip the resulting run's manifest
    forever: a non-echoing run is invisible)."""

    def _fresh_status(self) -> ReleaseStatus:
        return _status(qualification=QualificationStatus())

    def test_first_dispatch_records_the_dispatched_nonce_on_the_receipt(self) -> None:
        harness = _IssueHarness()
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=self._fresh_status(),
                            tracking_issue=harness.issue)
        dispatch.assert_called_once()
        nonce = dispatch.call_args.kwargs["nonce"]
        assert re.fullmatch(r"[0-9a-f]{32}", nonce)
        intents = harness.bodies(":autofix-intent:qual-dispatch:")
        assert len(intents) == 1
        assert issue_mod.qual_nonce_marker(MERGE_SHA, nonce) in intents[0]
        # The VISIBLE part of the NOTE callout renders the nonce too (an
        # integrity binding, not a secret): a manual re-dispatcher must be
        # able to copy it as the `nonce` input.
        assert f"Dispatch nonce: `{nonce}`" in intents[0]
        # The read-back the evaluator threading uses resolves to it.
        assert issue_mod.recorded_qualification_nonce(
            harness.issue, MERGE_SHA) == nonce

    def test_crashed_dispatch_retry_reuses_the_recorded_nonce(self) -> None:
        # Pass 1: intent (recording nonce N) posts, dispatch raises.
        # Pass 2: the retry must dispatch N again, not a fresh nonce.
        harness = _IssueHarness()
        status = self._fresh_status()
        with patch.object(actions.qual_mod, "dispatch_qualification",
                          side_effect=RuntimeError("boom")) as first:
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        recorded = issue_mod.recorded_qualification_nonce(
            harness.issue, MERGE_SHA)
        assert recorded
        assert first.call_args.kwargs["nonce"] == recorded

        with patch.object(actions.qual_mod, "dispatch_qualification") as retry, \
             patch.object(actions.qual_mod, "_find_run", return_value=None):
            actions.advance(gh_mock(MagicMock()), _POLICY, status=status,
                            tracking_issue=harness.issue)
        retry.assert_called_once()
        assert retry.call_args.kwargs["nonce"] == recorded
        # No second nonce receipt was minted: one intent, one nonce.
        assert len(harness.bodies(":autofix-intent:qual-dispatch:")) == 1

    def test_retry_after_failed_run_records_a_fresh_nonce(self) -> None:
        # The one-shot retry after a FAILED run is a NEW dispatch: its
        # receipt records a fresh nonce that supersedes the original for
        # evaluation (newest wins).
        harness = _IssueHarness()
        # Seed the tracker with the original dispatch's nonce receipt.
        harness.issue.create_comment(
            body=f"{issue_mod.qual_nonce_marker(MERGE_SHA, 'a' * 32)}\nseed")
        failed = _status(qualification=QualificationStatus(
            run_id=901, url="https://x/qruns/901", failed_jobs=("job",)))
        with patch.object(actions.qual_mod, "dispatch_qualification") as dispatch:
            actions.advance(gh_mock(MagicMock()), _POLICY, status=failed,
                            tracking_issue=harness.issue)
        dispatch.assert_called_once()
        retry_nonce = dispatch.call_args.kwargs["nonce"]
        assert retry_nonce != "a" * 32
        # Newest wins on read-back: the retry's nonce is now the one the
        # evaluator will require.
        assert issue_mod.recorded_qualification_nonce(
            harness.issue, MERGE_SHA) == retry_nonce


class TestBundleDispatchIdempotency:
    """Bundle dispatch is marker-gated per (tag, candidate) with the
    two-phase receipt. Without this the dispatch re-fires every pass
    while versions.json remains stale."""

    def _bundle_status(self, *, candidate_sha: str = MERGE_SHA) -> ReleaseStatus:
        return _status(
            candidate=Candidate(state=CandidateState.CURRENT,
                                sha=candidate_sha, branch_head=candidate_sha),
            phase=ReleasePhase.BUNDLE_HELM,
            outputs=(DownstreamOutput(name="bundle", state=OutputState.PENDING,
                                      action="dispatch-bundle"),),
        )

    def test_first_pass_dispatches_and_stamps_done(self) -> None:
        repo = MagicMock()
        harness = _IssueHarness()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        repo.create_repository_dispatch.assert_called_once()
        assert harness.bodies(":autofix-intent:bundle-dispatch:")
        assert harness.bodies(":autofix-done:bundle-dispatch:")

    def test_done_marker_suppresses_re_dispatch_across_passes(self) -> None:
        repo = MagicMock()
        harness = _IssueHarness()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        repo.create_repository_dispatch.reset_mock()
        # Second pass: same tag+candidate, still says "dispatch-bundle"
        # because versions.json is stale, but the done marker suppresses.
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        repo.create_repository_dispatch.assert_not_called()

    def test_new_candidate_re_arms_dispatch(self) -> None:
        # Fingerprint is (tag, candidate.sha): a new candidate produces a
        # new marker family, so dispatch fires again for the new
        # candidate exactly once.
        repo = MagicMock()
        harness = _IssueHarness()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        repo.create_repository_dispatch.reset_mock()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(candidate_sha=MOVED_SHA),
                        tracking_issue=harness.issue)
        repo.create_repository_dispatch.assert_called_once()

    def test_raising_dispatch_leaves_intent_for_retry_once(self) -> None:
        # The two-phase receipt handles crashes/5xx in the
        # dispatch call too. Pass 1 raises -> intent stays, no done.
        # Pass 2 retries once and stamps done (bounded).
        repo = MagicMock()
        repo.create_repository_dispatch.side_effect = RuntimeError("boom")
        harness = _IssueHarness()
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        assert harness.bodies(":autofix-intent:bundle-dispatch:")
        assert not harness.bodies(":autofix-done:bundle-dispatch:")
        # Pass 2: dispatch works this time.
        repo.create_repository_dispatch.side_effect = None
        actions.advance(gh_mock(repo), _POLICY,
                        status=self._bundle_status(),
                        tracking_issue=harness.issue)
        assert harness.bodies(":autofix-done:bundle-dispatch:")


class TestMarkerBeforeDispatchOrdering:
    """The documented safety crux: the autofix marker posts BEFORE the
    dispatch runs (fail closed), so a refactor to dispatch-first fails here."""

    def test_build_dispatch_runs_only_after_the_marker_posted(self) -> None:
        repo = MagicMock()
        repo.default_branch = "main"
        issue = tracker()

        def _assert_marker_already_posted(*args: object, **kwargs: object) -> bool:
            bodies = [c.kwargs["body"] for c in issue.create_comment.call_args_list]
            assert any(":autofix-intent:build-dispatch:" in b for b in bodies), \
                "dispatch ran before the autofix intent marker posted"
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
            assert any(":autofix-intent:qual-retry:" in b for b in bodies), \
                "dispatch ran before the autofix intent marker posted"

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
    @staticmethod
    def _alert_status(detail: str = "Tag `9.1.1` exists but no release does; "
                                    "the version is unshippable.") -> ReleaseStatus:
        return _status(alerts=(detail,),
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_failure_notifies_the_team_once(self) -> None:
        status = self._alert_status()
        issue = tracker()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=status, tracking_issue=issue)
        body = issue.create_comment.call_args.kwargs["body"]
        assert "> [!WARNING]" in body
        assert "**@valkey-io/core-team: Release `9.1.1` Needs Attention.**" in body
        assert "| # | Problem |" in body
        assert "unshippable" in body
        assert "<sub>This notification repeats only if the failure state changes.</sub>" in body
        assert f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:" in body
        assert "\u2014" not in body

    def test_failed_required_check_never_notifies(self) -> None:
        # Pin (b): a red (or stalled) check on the candidate is
        # informational; it must not page the team and must not post any
        # notification comment, on any candidate.
        for sha in (MERGE_SHA, MOVED_SHA):
            issue = tracker()
            status = _status(
                candidate=Candidate(state=CandidateState.CURRENT, sha=sha,
                                    branch_head=sha),
                checks=(RequiredCheck(name="test-ubuntu-latest",
                                      state=CheckState.FAILED,
                                      url="https://x/run/1"),
                        RequiredCheck(name="build-macos-latest",
                                      state=CheckState.STALLED)),
            )
            actions.advance(gh_mock(MagicMock()), _POLICY,
                            status=status, tracking_issue=issue)
            issue.create_comment.assert_not_called()

    def test_same_failure_state_never_notifies_twice(self) -> None:
        status = self._alert_status()
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
        status = self._alert_status()
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
        first = _status(outputs=(DownstreamOutput(
            name="pages", state=OutputState.FAILED, detail="first", run_id=7),))
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=first, tracking_issue=issue)
        posted = MagicMock()
        posted.user.login = "valkeyrie-ops[bot]"
        posted.body = issue.create_comment.call_args.kwargs["body"]
        issue.get_comments.return_value = [posted]
        issue.create_comment.reset_mock()

        second = _status(outputs=(DownstreamOutput(
            name="helm", state=OutputState.FAILED, detail="second", run_id=8),))
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

    def test_check_failure_after_adoption_still_never_notifies(self) -> None:
        # Retarget of the old cross-candidate re-ping pin: the branch moved,
        # the new head was adopted, and the same required check failed again
        # on the NEW candidate SHA. Under informational semantics there is
        # nothing to re-ping: no candidate's check failure ever notifies.
        issue = tracker()
        adopted = _status(
            candidate=Candidate(state=CandidateState.ADOPTED, sha=MOVED_SHA,
                                branch_head=MOVED_SHA),
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.FAILED, url="https://x/run/2"),),
        )
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=adopted, tracking_issue=issue)

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
        assert "Adopt the new head (Actions → Adopt Release Candidate)" in body
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
    """advance() marks the release complete (posts the completion
    comment) and signals close_when_complete=True; it NEVER closes the
    tracker itself. Reconcile closes as its final
    write after rendering the final tracker body."""

    def test_complete_release_marks_and_signals_close(self) -> None:
        status = _status(phase=ReleasePhase.COMPLETE,
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        result = actions.advance(gh_mock(MagicMock()), _POLICY,
                                 status=status, tracking_issue=issue)
        # advance() never closes: reconcile does that as its final write.
        issue.edit.assert_not_called()
        body = issue.create_comment.call_args.kwargs["body"]
        assert "> [!NOTE]" in body
        assert "**Release `9.1.1` (ga) is complete.**" in body
        assert "all verified public. Closing." in body
        assert "\u2014" not in body
        assert any("marked release complete" in p for p in result)
        assert result.close_when_complete is True

    def test_already_closed_tracker_is_left_alone(self) -> None:
        status = _status(phase=ReleasePhase.COMPLETE,
                         qualification=QualificationStatus(run_id=1, passed=True))
        issue = tracker()
        issue.state = "closed"
        result = actions.advance(gh_mock(MagicMock()), _POLICY,
                                 status=status, tracking_issue=issue)
        issue.edit.assert_not_called()
        issue.create_comment.assert_not_called()
        # An already-closed tracker needs no further close.
        assert result.close_when_complete is False

    def test_close_signal_repeats_across_passes_while_open(self) -> None:
        # The completion marker is only posted once, but the close
        # signal keeps firing on every subsequent COMPLETE pass while the
        # tracker remains open. Reconcile's earlier close-write may have
        # failed and the next pass must still request the close.
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

        result = actions.advance(gh_mock(MagicMock()), _POLICY,
                                 status=status, tracking_issue=issue)
        # No new comment (marker suppresses the re-post) but the flag
        # still tells reconcile to close (idempotent close is fine).
        issue.create_comment.assert_not_called()
        assert result.close_when_complete is True


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
        # advance() never closes, so this test now verifies exactly
        # the completion-comment-not-duplicated property. The close is
        # reconcile's job and is asserted separately over there.
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
        result = actions.advance(gh_mock(MagicMock()), _POLICY,
                                 status=status, tracking_issue=issue)
        issue.create_comment.assert_not_called()  # completion not duplicated
        issue.edit.assert_not_called()  # advance() never edits the state
        assert result.close_when_complete is True  # reconcile should retry

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
        # The dispatch binds the run to the exact tag and candidate (the
        # workflow stamps them into its run-name for correlation), and
        # declares itself controller-dispatched through the explicit
        # unattended input, never an actor-name literal.
        workflow.create_dispatch.assert_called_once_with(
            gh_agent.get_repo.return_value.default_branch,
            inputs={"branch": "9.1", "tag": "9.1.1", "candidate_sha": MERGE_SHA,
                    "unattended": "true"},
        )
        assert any("publish pipeline" in p for p in performed)

    def test_waiting_publish_run_blocks_a_duplicate_dispatch(self) -> None:
        # Only a run whose binding matches the current tag+candidate
        # may hold the slot. An unbound run cannot; a bound one does.
        # This test uses a matching-bound run so it still measures the
        # anti-duplicate-dispatch behavior it was written for.
        waiting = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                               candidate_sha=MERGE_SHA)
        gh_agent = self._agent([waiting])
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = _AGENT_HEAD
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_unbound_waiting_run_no_longer_holds_the_slot(self) -> None:
        # Hostile test: an unbound gate-parked run (legacy or a
        # workflow_dispatch that predates the required-input change)
        # must NEVER block a fresh, bound dispatch - that was the DoS
        # vector where a stray unbound run failing the team check could
        # permanently suppress controller re-dispatch.
        stray = _publish_run(head_sha=_AGENT_HEAD)  # unbound
        gh_agent = self._agent([stray])
        gh_agent.get_repo.return_value.get_branch.return_value.commit.sha = _AGENT_HEAD
        performed = actions.advance(
            gh_mock(MagicMock()), _POLICY,
            status=self._ready(), tracking_issue=tracker(),
            gh_agent=gh_agent, agent_repo="o/agent",
            agent_head_sha=_AGENT_HEAD,
        )
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        # Unbound run is ignored entirely - dispatch proceeds and the
        # unbound run is NOT cancelled either (it's not "stale for this
        # candidate", it's simply irrelevant).
        workflow.create_dispatch.assert_called_once()
        stray.cancel.assert_not_called()
        assert any("publish pipeline" in p for p in performed)

    def test_other_branch_run_does_not_block(self) -> None:
        other = MagicMock(status="waiting",
                          display_title="Publish Release on 8.0 (requested by x)")
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


# Promoted to release_fixtures.publish_run; alias keeps 25 call sites terse.
_publish_run = publish_run


def _runs_by_status(runs: "list[MagicMock]") -> "MagicMock":
    """A workflow mock whose get_runs honors the server-side status filter:
    get_runs(status=X) serves only the runs whose status is X."""
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
    stale: it is cancelled and replaced, never left to publish stale logic.
    Under the required-input workflow contract, every real dispatch
    is bound to a tag+candidate, so these tests exercise BOUND runs whose
    head is stale - an unbound run is a separate case tested via
    :class:`TestUnboundRunsIgnored`."""

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
        stale = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                             candidate_sha=MERGE_SHA)
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
        current = _publish_run(head_sha=_AGENT_HEAD, tag="9.1.1",
                               candidate_sha=MERGE_SHA)
        gh_agent = self._agent([current])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        current.cancel.assert_not_called()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_cancel_failure_means_the_run_stays_active_fail_safe(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                             candidate_sha=MERGE_SHA)
        stale.cancel.side_effect = GithubException(403, "forbidden", {})
        gh_agent = self._agent([stale])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent",
                        agent_head_sha=_AGENT_HEAD)
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_rejected_cancel_means_the_run_stays_active_fail_safe(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                             candidate_sha=MERGE_SHA)
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
        # waiting run (bound to the current candidate) blocks and nothing
        # is cancelled (today's behavior). Only HEAD-based staleness is
        # disabled; binding-based staleness still applies.
        current = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                               candidate_sha=MERGE_SHA)
        gh_agent = self._agent([current])
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._ready(), tracking_issue=tracker(),
                        gh_agent=gh_agent, agent_repo="o/agent")
        current.cancel.assert_not_called()
        workflow = gh_agent.get_repo.return_value.get_workflow.return_value
        workflow.create_dispatch.assert_not_called()

    def test_stale_run_is_never_presented_as_the_approval_link(self) -> None:
        stale = _publish_run(head_sha=_STALE_HEAD, tag="9.1.1",
                             candidate_sha=MERGE_SHA)
        gh_agent = self._agent([stale])
        url = actions.waiting_publish_run_url(gh_agent, "o/agent", "9.1",
                                              _AGENT_HEAD,
                                              tag="9.1.1",
                                              candidate_sha=MERGE_SHA)
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
                            display_title="Publish Release on 9.1 (requested by x)",
                            html_url="https://x/actions/runs/500")
        url = actions.waiting_publish_run_url(self._agent([waiting]), "o/agent", "9.1")
        assert url == "https://x/actions/runs/500"

    def test_legacy_lowercase_titled_run_still_matches(self) -> None:
        # Migration alias for the workflow rename ("Publish release on" ->
        # "Publish Release on"): correlation is prefix-agnostic (workflow
        # file + " on <branch> " + binding), so a gate-parked run dispatched
        # under the OLD title is still recognized. Without this, the one
        # pre-rename waiting run could neither hold nor halt the slot and
        # would sit at the gate until manually cancelled. This test can be
        # dropped after the next successful publish.
        legacy = MagicMock(status="waiting",
                           display_title="Publish release on 9.1 (requested by x)",
                           html_url="https://x/actions/runs/499")
        url = actions.waiting_publish_run_url(self._agent([legacy]), "o/agent", "9.1")
        assert url == "https://x/actions/runs/499"

    def test_no_active_run_yields_empty(self) -> None:
        done = MagicMock(status="completed",
                         display_title="Publish Release on 9.1 (requested by x)",
                         html_url="https://x/actions/runs/400")
        assert actions.waiting_publish_run_url(self._agent([done]), "o/agent", "9.1") == ""

    def test_other_branch_run_yields_empty(self) -> None:
        other = MagicMock(status="waiting",
                          display_title="Publish Release on 8.0 (requested by x)",
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
    """A startup-failed qualification run carries its run id (it is a
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
        kwargs = dict(dispatch.call_args.kwargs)
        assert re.fullmatch(r"[0-9a-f]{32}", kwargs.pop("nonce"))
        assert kwargs == {"tag": "9.1.1", "sha": MERGE_SHA}
        # Two-phase receipt: intent marker posts before dispatch, done
        # stamps after success on the same comment.
        assert harness.bodies(":autofix-intent:qual-retry:")
        assert harness.bodies(":autofix-done:qual-retry:")

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
    """With no active publish run, a newest COMPLETED run for this
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

    @pytest.mark.parametrize("conclusion", [
        "failure",
        "cancelled",
        # Halt on EVERY completed non-success conclusion, not
        # just failure/cancelled. Without these, a timed_out or
        # startup_failure publish run would re-dispatch every reconcile
        # pass forever until the controller code changed.
        "timed_out",
        "startup_failure",
        "action_required",
        "skipped",
        "neutral",
        "stale",
    ])
    def test_every_non_success_conclusion_halts(self, conclusion: str) -> None:
        workflow = _runs_by_status([self._failed_run(conclusion=conclusion)])
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._ready(),
                                    tracking_issue=harness.issue,
                                    gh_agent=_agent_with_workflow(workflow),
                                    agent_repo="o/agent",
                                    agent_head_sha=_AGENT_HEAD)
        workflow.create_dispatch.assert_not_called()
        # The halt callout must name the actual concluding state so a
        # human reading the tracker sees which kind of failure blocks.
        halt = harness.bodies(":autofix:publish-halt:")
        assert len(halt) == 1
        assert f"concluded `{conclusion}`" in halt[0]
        assert any("halted publish re-dispatch" in p for p in performed)
        assert any(conclusion in p for p in performed)

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
    """Publish runs correlate by branch AND, when the run-name carries
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

    def test_legacy_lowercase_titled_bound_run_still_holds_the_slot(self) -> None:
        # Migration alias for the workflow rename ("Publish release on" ->
        # "Publish Release on"): one gate-parked run dispatched under the
        # OLD title exists. Correlation never inspects the leading words,
        # so its binding must keep holding the slot (no duplicate dispatch)
        # exactly like a new-title run. Droppable after the next
        # successful publish.
        parked = MagicMock(status="waiting", head_sha=_AGENT_HEAD, id=76,
                           display_title=(f"Publish release on 9.1 · 9.1.1 @ "
                                          f"{MERGE_SHA} (requested by x)"),
                           html_url="https://x/actions/runs/76")
        workflow = _runs_by_status([parked])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        parked.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_unbound_manual_run_never_blocks_dispatch(self) -> None:
        # Unbound gate-parked runs (no tag/sha binding in the
        # run-name - a legacy dispatch or a form-submit that predates the
        # required-input change) are IGNORED entirely. They may neither
        # hold nor halt the current candidate's slot; the DoS vector
        # where a stray unbound run failing the environment gate could
        # permanently suppress controller re-dispatch is closed at this
        # layer even if a legacy run somehow reappears.
        parked = _publish_run(head_sha=_AGENT_HEAD)  # unbound
        workflow = _runs_by_status([parked])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        parked.cancel.assert_not_called()
        workflow.create_dispatch.assert_called_once()

    def test_unbound_failed_run_at_current_head_does_not_suppress_dispatch(self) -> None:
        # Hostile test: even a COMPLETED unbound run at the current
        # controller head, whose conclusion is failure, must not halt
        # re-dispatch. Without this, any repo writer could stage an
        # unbound workflow_dispatch, watch it fail the environment gate,
        # and permanently DoS the controller. Unbound halted
        # runs are ignored just like unbound gate-parked ones.
        failed_unbound = _publish_run(
            status="completed", conclusion="failure",
            head_sha=_AGENT_HEAD,  # matches controller head
        )
        workflow = _runs_by_status([failed_unbound])
        harness = _IssueHarness()
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=harness.issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        # Dispatch proceeds: the unbound halted run does not suppress it.
        workflow.create_dispatch.assert_called_once()
        # No halt-warning comment posted (the unbound run never halted).
        assert not harness.bodies(":autofix:publish-halt:")

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
    """Active runs are found via server-side status filters, so a
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
    """An in_progress run is past the approval gate; it must never be
    cancelled - cancelling a publication in flight would leave a
    half-published release. "Hold the slot" is binding-scoped: only a
    run whose bindings match the current tag+candidate holds the slot for
    THIS candidate. An in_progress run with a mismatched or missing
    binding is neither cancelled nor blocking, so a fresh
    dispatch for the current candidate can proceed alongside an
    in-flight publish for something else (per-branch GitHub concurrency
    still serializes real work)."""

    def _ready(self) -> ReleaseStatus:
        return _status(phase=ReleasePhase.READY,
                       qualification=QualificationStatus(run_id=1, passed=True))

    def test_in_progress_matching_candidate_stale_head_is_kept_and_blocks(self) -> None:
        # A bound in_progress with STALE head but matching candidate: the
        # publication of THIS candidate is in flight. Never cancelled,
        # and it holds the slot for this candidate.
        running = _publish_run(status="in_progress", head_sha=_STALE_HEAD,
                               tag="9.1.1", candidate_sha=MERGE_SHA)
        workflow = _runs_by_status([running])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        running.cancel.assert_not_called()
        workflow.create_dispatch.assert_not_called()

    def test_in_progress_other_candidate_is_never_cancelled_never_blocks(self) -> None:
        # in_progress bound to a DIFFERENT candidate does NOT hold
        # this candidate's slot, and is still never cancelled. Both apply.
        running = _publish_run(status="in_progress", head_sha=_AGENT_HEAD,
                               tag="9.1.1", candidate_sha=MOVED_SHA)
        workflow = _runs_by_status([running])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        running.cancel.assert_not_called()
        # Dispatch for THIS candidate proceeds (the other candidate's
        # in-flight publish does not block this one).
        workflow.create_dispatch.assert_called_once()

    def test_in_progress_unbound_is_never_cancelled_never_blocks(self) -> None:
        # An unbound in_progress (legacy, or a manual dispatch
        # that predates required inputs) is neither cancelled nor blocking.
        running = _publish_run(status="in_progress", head_sha=_STALE_HEAD)
        workflow = _runs_by_status([running])
        actions.advance(gh_mock(MagicMock()), _POLICY, status=self._ready(),
                        tracking_issue=_IssueHarness().issue,
                        gh_agent=_agent_with_workflow(workflow),
                        agent_repo="o/agent", agent_head_sha=_AGENT_HEAD)
        running.cancel.assert_not_called()
        workflow.create_dispatch.assert_called_once()

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
    """A MISSING/STALE daily gate never resolves on its own;
    a one-shot marker-gated nudge says so. Required-check states never
    wedge: nothing gates on them, so a MISSING check must not page anyone.
    No time-based grace this round: the observed state is the trigger."""

    def _missing_check(self) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.CANDIDATE,
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.MISSING),),
        )

    def _stale_daily(self) -> ReleaseStatus:
        return _status(
            phase=ReleasePhase.QUALIFICATION,
            daily=DailyCiStatus(state=DailyCiState.STALE, run_id=77,
                                url="https://x/druns/77",
                                detail="The newest daily run is 30 hours old"),
        )

    def test_missing_check_never_nudges(self) -> None:
        # Retarget of the old missing-check wedge pin: checks are
        # informational, so a MISSING check is not blocked-without-progress
        # and no nudge (or any comment) fires.
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._missing_check(),
                                    tracking_issue=harness.issue)
        assert harness.bodies(":wedge:") == []
        assert not any("wedged gate" in p for p in performed)

    def test_stale_daily_gate_nudges_once_with_its_detail(self) -> None:
        harness = _IssueHarness()
        performed = actions.advance(gh_mock(MagicMock()), _POLICY,
                                    status=self._stale_daily(),
                                    tracking_issue=harness.issue)
        wedges = harness.bodies(":wedge:")
        assert len(wedges) == 1
        assert "> [!IMPORTANT]" in wedges[0]
        assert ("**@valkey-io/core-team: Release `9.1.1` Is Blocked Without "
                "Progress.**") in wedges[0]
        assert ("Blocked without progress: The newest daily run is 30 hours "
                "old. This does not resolve on its own.") in wedges[0]
        assert "\u2014" not in wedges[0]
        assert any("wedged gate" in p for p in performed)
        # Same state again: suppressed.
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._stale_daily(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 1

    def test_resolution_then_recurrence_renudges_exactly_once(self) -> None:
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._stale_daily(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 1
        # The daily gate resolved: a clean pass bumps the recovery generation.
        actions.advance(gh, _POLICY,
                        status=_status(qualification=QualificationStatus(
                            run_id=1, passed=True)),
                        tracking_issue=harness.issue)
        assert harness.bodies(":notify-gen:1")
        # The same gate goes STALE again: new fingerprint, one re-nudge.
        actions.advance(gh, _POLICY, status=self._stale_daily(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 2
        # And a repeat of that state is suppressed again.
        actions.advance(gh, _POLICY, status=self._stale_daily(),
                        tracking_issue=harness.issue)
        assert len(harness.bodies(":wedge:")) == 2

    def test_missing_check_is_not_a_failure_item(self) -> None:
        # MISSING (like every other check state) escalates through neither
        # the failure notifier nor the wedge nudge: checks are display only.
        harness = _IssueHarness()
        actions.advance(gh_mock(MagicMock()), _POLICY,
                        status=self._missing_check(),
                        tracking_issue=harness.issue)
        assert not harness.bodies(":notify:")
        assert not harness.bodies(":wedge:")


class TestRecoveryGenerations:
    """Fingerprints hash (recovery generation, sorted keys); a clean
    pass advances the generation in one edit-in-place marker comment, so a
    failure recurring after recovery re-pings exactly once."""

    def _failing(self) -> ReleaseStatus:
        # A standing alert: checks no longer feed the failure machinery,
        # so the generation bookkeeping is exercised through alerts.
        return _status(
            alerts=("The release metadata is broken; fix it.",),
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
        # The marker is written only on TRANSITION.
        # Failing pass -> notify posted, no marker yet.
        # First healthy pass with notify history -> transition, write
        # generation=1 healthy.
        # Additional healthy passes -> no-op (marker already healthy).
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)
        # No gen marker written yet on the failing pass.
        assert not [c for c in harness.comments if ":notify-gen:" in c.body]
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        gen_comments = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments) == 1
        assert ":notify-gen:1 -->" in gen_comments[0].body
        assert ":notify-state:healthy -->" in gen_comments[0].body
        # Steady healthy: no more writes, no more edits.
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        gen_comments_after = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments_after) == 1
        assert ":notify-gen:1 -->" in gen_comments_after[0].body  # unchanged
        # And .edit was never called on the healthy pass (no transition).
        gen_comments[0].edit.assert_not_called()

    def test_two_consecutive_healthy_passes_produce_exactly_one_edit_total(self) -> None:
        # Transition-only pin: after the initial failing pass, two clean
        # passes in a row must produce EXACTLY ONE bookkeeping edit
        # (the first transition), never two.
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)
        gen_comments = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments) == 1
        # Exactly one write total: one create_comment for the first
        # transition, zero edits on subsequent healthy passes.
        gen_comments[0].edit.assert_not_called()

    def test_dirty_then_healthy_then_recurrence_bumps_correctly(self) -> None:
        # Full cycle: failing -> healthy (transition -> gen 1 healthy)
        # -> failing (transition -> gen 1 dirty, edit-in-place)
        # -> healthy (transition -> gen 2 healthy, edit-in-place).
        harness = _IssueHarness()
        gh = gh_mock(MagicMock())
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)  # gen 1 healthy
        actions.advance(gh, _POLICY, status=self._failing(),
                        tracking_issue=harness.issue)  # gen 1 dirty
        actions.advance(gh, _POLICY, status=self._clean(),
                        tracking_issue=harness.issue)  # gen 2 healthy
        gen_comments = [c for c in harness.comments if ":notify-gen:" in c.body]
        assert len(gen_comments) == 1
        assert ":notify-gen:2 -->" in gen_comments[0].body
        assert ":notify-state:healthy -->" in gen_comments[0].body
        # Two edits total: healthy->dirty edit, then bump edit.
        assert gen_comments[0].edit.call_count == 2

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
