"""Tests for the edit-only fix application step."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

from scripts.ci_fix import apply as apply_mod
from scripts.ci_fix.apply import ApplyResult, apply_fix, declined_detail
from scripts.ci_fix.models import FixPath, FixProposal, Policy


def _proposal(path: FixPath = FixPath.AUTHOR) -> FixProposal:
    return FixProposal(
        path=path, failing_check="t", root_cause="rc", reasoning="why",
        confidence=0.9, build_command="make", verify_command="./runtest --single x",
    )


def test_refuse_proposal_never_calls_agent(monkeypatch):
    agent = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", agent)
    result = apply_fix("/repo", _proposal(FixPath.REFUSE))
    ok, changed = result.applied, result.changed
    assert ok is False
    assert changed == ()
    agent.assert_not_called()


def test_agent_failure_returns_not_applied(monkeypatch):
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=1, stdout="", stderr="boom")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("test.tcl",))
    result = apply_fix("/repo", _proposal())
    ok, changed = result.applied, result.changed
    assert ok is False
    assert changed == ()


def test_no_edits_treated_as_refusal(monkeypatch):
    """The agent ran cleanly but declined to edit (e.g. fix would weaken assertion)."""
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=0, stdout="", stderr="")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ())
    result = apply_fix("/repo", _proposal())
    ok, changed = result.applied, result.changed
    assert ok is False
    assert changed == ()


def test_successful_edit_returns_changed_paths(monkeypatch):
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=0, stdout="", stderr="")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths",
                        lambda _r: ("tests/integration/corrupt-dump.tcl",))
    result = apply_fix("/repo", _proposal())
    ok, changed = result.applied, result.changed
    assert ok is True
    assert changed == ("tests/integration/corrupt-dump.tcl",)


def test_feedback_included_in_prompt(monkeypatch):
    captured = {}

    def fake_run_agent(profile, prompt, **kwargs):
        captured["prompt"] = prompt
        return MagicMock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(apply_mod, "run_agent", fake_run_agent)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("t",))
    apply_fix("/repo", _proposal(), feedback="the test still failed at line 42")
    assert "still failed at line 42" in captured["prompt"]
    assert "rejected" in captured["prompt"].lower()


def test_apply_fix_declines_port_path(monkeypatch):
    """PORT is cherry-picked in the pipeline with its original authorship, so
    apply_fix (the authored-fix editor) must not act on a PORT proposal."""
    agent = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", agent)
    result = apply_fix("/repo", _proposal(FixPath.PORT))
    ok, changed = result.applied, result.changed
    assert ok is False
    assert changed == ()
    agent.assert_not_called()


def _stream_text(text: str) -> str:
    import json
    return json.dumps({"type": "result", "subtype": "success", "result": text})


def test_declined_edit_reports_the_agents_reason(monkeypatch):
    """A no-edit refusal carries the agent's own explanation, not a generic line."""
    reason = "The only change that passes would remove the assertion on line 12."
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=0, stdout=_stream_text(reason), stderr="")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ())
    result = apply_fix("/repo", _proposal())
    assert result.applied is False
    assert result.reason == reason
    assert declined_detail(result) == f"fix not applied: {reason}"


def test_declined_detail_without_reason_is_generic():
    assert declined_detail(ApplyResult(False, ())) == "fix not applied (agent declined or made no edits)"


def _captured_prompt(monkeypatch, policy: Policy) -> str:
    captured = {}

    def fake_run_agent(profile, prompt, **kwargs):
        captured["prompt"] = prompt
        return MagicMock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(apply_mod, "run_agent", fake_run_agent)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("t",))
    apply_fix("/repo", _proposal(), policy=policy)
    return captured["prompt"]


def test_backport_policy_forbids_product_changes(monkeypatch):
    prompt = _captured_prompt(monkeypatch, Policy.BACKPORT)
    assert "Never change product behavior" in prompt
    assert "A product bug" not in prompt


def test_fix_policy_allows_flaky_and_product_fixes(monkeypatch):
    prompt = _captured_prompt(monkeypatch, Policy.FIX)
    assert "A flaky test" in prompt
    assert "A product bug" in prompt
    assert "Never change product behavior" not in prompt
    # The assertion guardrail holds under every policy.
    assert "NEVER weaken" in prompt


def test_an_edit_to_git_config_is_undone_and_refused(monkeypatch, tmp_path):
    """Git executes commands configured in .git/config; an agent edit there is never trusted."""
    config = tmp_path / ".git" / "config"
    config.parent.mkdir()
    config.write_text("[core]\n")

    def tamper(*_a, **_k):
        config.write_text("[core]\n\tfsmonitor = /tmp/evil\n")
        return MagicMock(returncode=0, stdout="", stderr="")

    changed = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", tamper)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", changed)
    result = apply_fix(str(tmp_path), _proposal())
    assert result == ApplyResult(False, (), "the edit agent changed the repository's git configuration")
    assert config.read_text() == "[core]\n"
    changed.assert_not_called()



def test_the_edit_agent_runs_under_the_confined_ci_fix_profile(monkeypatch):
    profiles = []

    def fake_run_agent(profile, prompt, **kwargs):
        profiles.append(profile)
        return MagicMock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(apply_mod, "run_agent", fake_run_agent)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("t",))
    apply_fix("/repo", _proposal())
    assert profiles == ["ci_fix_apply_edit_only"]
