"""Tests for the apply/run/review fix-feedback loop.

The loop is the orchestration core. These tests inject fakes for apply, run,
review, reset, and diff so we exercise the control flow deterministically:
success only when test-passed AND review-approved, retry-on-feedback, and
worktree reset on every non-success exit.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from scripts.ci_fix import review as review_mod
from scripts.ci_fix.models import FixPath, FixProposal, ReviewVerdict, RunResult
from scripts.ci_fix.review import review_fix, run_fix_loop


def _proposal(path: FixPath = FixPath.AUTHOR) -> FixProposal:
    return FixProposal(
        path=path,
        failing_test="t",
        root_cause="rc",
        reasoning="why",
        confidence=0.9,
        build_command="make",
        test_command="./runtest --single x",
    )


def _passed() -> RunResult:
    return RunResult(ran=True, passed=True, exit_code=0, command="c", output_tail="ok")


def _failed() -> RunResult:
    return RunResult(ran=True, passed=False, exit_code=1, command="c", output_tail="boom")


def _approved() -> ReviewVerdict:
    return ReviewVerdict(approved=True, reasoning="looks good")


def _rejected() -> ReviewVerdict:
    return ReviewVerdict(approved=False, reasoning="weakens assertion")


def _loop(*, path: FixPath = FixPath.AUTHOR, **overrides):
    """run_fix_loop with safe fakes; override individual collaborators per test."""
    defaults = dict(
        apply_func=lambda *a, **k: (True, ("test.tcl",)),
        run_command=lambda *a, **k: _passed(),
        review_func=lambda *a, **k: _approved(),
        reset_func=MagicMock(),
        diff_func=lambda *a, **k: "diff",
    )
    defaults.update(overrides)
    return run_fix_loop("/repo", _proposal(path), **defaults)


def test_success_requires_pass_and_approval():
    result = _loop()
    assert result.success is True
    assert result.attempts == 1
    assert result.changed_paths == ("test.tcl",)


def test_refuse_proposal_short_circuits():
    result = _loop(path=FixPath.REFUSE, apply_func=lambda *a, **k: (False, ()))
    assert result.success is False
    assert "not applied" in result.detail


def test_retries_when_test_fails_then_passes():
    runs = [_failed(), _passed()]
    result = _loop(max_attempts=3, run_command=lambda *a, **k: runs.pop(0))
    assert result.success is True
    assert result.attempts == 2


def test_retries_when_review_rejects_then_approves():
    reviews = [_rejected(), _approved()]
    result = _loop(max_attempts=3, review_func=lambda *a, **k: reviews.pop(0))
    assert result.success is True
    assert result.attempts == 2


def test_gives_up_after_max_attempts():
    result = _loop(max_attempts=2, review_func=lambda *a, **k: _rejected())
    assert result.success is False
    assert result.attempts == 2
    assert "review rejected" in result.detail


def test_unrunnable_command_breaks_loop():
    unrunnable = RunResult(ran=False, passed=False, exit_code=-1, command="c", output_tail="no cwd")
    result = _loop(run_command=lambda *a, **k: unrunnable)
    assert result.success is False
    assert "could not run" in result.detail


def test_worktree_reset_on_failure():
    """Every non-success exit must reset the worktree to HEAD."""
    reset = MagicMock()
    _loop(max_attempts=1, review_func=lambda *a, **k: _rejected(), reset_func=reset)
    # One reset at loop start + one on the failure exit.
    assert reset.call_count >= 2


def test_zero_max_attempts_is_clamped():
    """A pathological max_attempts=0 must not raise; it runs at least once."""
    result = _loop(max_attempts=0)
    assert result.success is True
    assert result.attempts == 1


def test_feedback_passed_to_apply_on_retry():
    seen_feedback = []

    def fake_apply(repo, proposal, *, feedback=""):
        seen_feedback.append(feedback)
        return True, ("test.tcl",)

    runs = [_failed(), _passed()]
    _loop(max_attempts=2, apply_func=fake_apply, run_command=lambda *a, **k: runs.pop(0))
    assert seen_feedback[0] == ""           # first attempt: no feedback
    assert "did not make the test pass" in seen_feedback[1]


def _stream_result(obj: dict) -> str:
    return json.dumps({"type": "result", "subtype": "success", "result": json.dumps(obj)})


def test_review_fix_parses_approval(monkeypatch):
    monkeypatch.setattr(
        review_mod, "run_agent",
        MagicMock(return_value=MagicMock(
            stdout=_stream_result({"approved": True, "reasoning": "minimal and correct"}),
            stderr="", returncode=0,
        )),
    )
    verdict = review_fix("/repo", _proposal(), "some diff")
    assert verdict.approved is True
    assert "minimal" in verdict.reasoning


def test_review_fix_rejects_on_agent_failure(monkeypatch):
    monkeypatch.setattr(
        review_mod, "run_agent",
        MagicMock(return_value=MagicMock(stdout="", stderr="", returncode=1)),
    )
    verdict = review_fix("/repo", _proposal(), "diff")
    assert verdict.approved is False


def test_review_fix_rejects_when_no_verdict(monkeypatch):
    monkeypatch.setattr(
        review_mod, "run_agent",
        MagicMock(return_value=MagicMock(stdout="no json", stderr="", returncode=0)),
    )
    verdict = review_fix("/repo", _proposal(), "diff")
    assert verdict.approved is False


def test_review_fix_requires_strict_true(monkeypatch):
    """A truthy non-bool (e.g. the string "yes") must not count as approval."""
    monkeypatch.setattr(
        review_mod, "run_agent",
        MagicMock(return_value=MagicMock(
            stdout=_stream_result({"approved": "yes", "reasoning": "ambiguous"}),
            stderr="", returncode=0,
        )),
    )
    verdict = review_fix("/repo", _proposal(), "diff")
    assert verdict.approved is False
