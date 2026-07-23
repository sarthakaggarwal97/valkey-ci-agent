"""Tests for the edit-only fix application step."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import apply as apply_mod
from scripts.ci_fix.apply import PortApplyError, apply_fix, apply_port_commit
from scripts.ci_fix.models import FailureMode, FixPath, FixProposal


def _proposal(path: FixPath = FixPath.AUTHOR) -> FixProposal:
    return FixProposal(
        path=path, failing_check="t", root_cause="rc", reasoning="why",
        confidence=0.9, build_command="make", verify_command="./runtest --single x",
    )


def test_refuse_proposal_never_calls_agent(monkeypatch):
    agent = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", agent)
    ok, changed = apply_fix("/repo", _proposal(FixPath.REFUSE))
    assert ok is False
    assert changed == ()
    agent.assert_not_called()


def test_agent_failure_returns_not_applied(monkeypatch):
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=1, stdout="", stderr="boom")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("test.tcl",))
    ok, changed = apply_fix("/repo", _proposal())
    assert ok is False
    assert changed == ()


def test_no_edits_treated_as_refusal(monkeypatch):
    """The agent ran cleanly but declined to edit (e.g. fix would weaken assertion)."""
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=0, stdout="", stderr="")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ())
    ok, changed = apply_fix("/repo", _proposal())
    assert ok is False
    assert changed == ()


def test_successful_edit_returns_changed_paths(monkeypatch):
    monkeypatch.setattr(apply_mod, "run_agent",
                        MagicMock(return_value=MagicMock(returncode=0, stdout="", stderr="")))
    monkeypatch.setattr(apply_mod, "worktree_changed_paths",
                        lambda _r: ("tests/integration/corrupt-dump.tcl",))
    ok, changed = apply_fix("/repo", _proposal())
    assert ok is True
    assert changed == ("tests/integration/corrupt-dump.tcl",)


def test_feedback_included_in_prompt(tmp_path, monkeypatch):
    captured = {}
    repo = tmp_path / "repo"
    repo.mkdir()

    def fake_run_agent(profile, prompt, **kwargs):
        captured["profile"] = profile
        captured["prompt"] = prompt
        captured["kwargs"] = kwargs
        return MagicMock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(apply_mod, "run_agent", fake_run_agent)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("t",))
    apply_fix(str(repo), _proposal(), feedback="the test still failed at line 42")
    assert captured["profile"] == "ci_fix_apply_edit_only"
    assert captured["kwargs"]["sandbox_root"] == str(tmp_path)
    assert "still failed at line 42" in captured["prompt"]
    assert "rejected" in captured["prompt"].lower()


def test_apply_fix_declines_port_path(monkeypatch):
    """PORT is cherry-picked in the pipeline with its original authorship, so
    apply_fix (the authored-fix editor) must not act on a PORT proposal."""
    agent = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", agent)
    ok, changed = apply_fix("/repo", _proposal(FixPath.PORT))
    assert ok is False
    assert changed == ()
    agent.assert_not_called()


def test_apply_port_commit_leaves_exact_change_uncommitted(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "target", str(repo)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.com"], check=True)
    (repo / "test.tcl").write_text("old\n")
    subprocess.run(["git", "-C", str(repo), "add", "test.tcl"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "base"], check=True, capture_output=True)
    target_head = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()

    subprocess.run(["git", "-C", str(repo), "checkout", "-b", "unstable"], check=True, capture_output=True)
    (repo / "test.tcl").write_text("fixed\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "Fix test"], check=True, capture_output=True)
    fix_sha = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    subprocess.run(["git", "-C", str(repo), "checkout", "target"], check=True, capture_output=True)

    changed = apply_port_commit(str(repo), fix_sha)

    assert changed == ("test.tcl",)
    assert (repo / "test.tcl").read_text() == "fixed\n"
    assert subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip() == target_head
    assert subprocess.check_output(
        ["git", "-C", str(repo), "diff", "--name-only", "HEAD"],
        text=True,
    ).splitlines() == ["test.tcl"]


def test_apply_port_commit_aborts_conflict(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "target", str(repo)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.com"], check=True)
    (repo / "test.tcl").write_text("base\n")
    subprocess.run(["git", "-C", str(repo), "add", "test.tcl"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "base"], check=True, capture_output=True)

    subprocess.run(["git", "-C", str(repo), "checkout", "-b", "unstable"], check=True, capture_output=True)
    (repo / "test.tcl").write_text("upstream\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "Upstream fix"], check=True, capture_output=True)
    fix_sha = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        text=True,
    ).strip()

    subprocess.run(["git", "-C", str(repo), "checkout", "target"], check=True, capture_output=True)
    (repo / "test.tcl").write_text("release\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "Release change"], check=True, capture_output=True)

    with pytest.raises(PortApplyError, match="did not apply cleanly"):
        apply_port_commit(str(repo), fix_sha)

    assert (repo / "test.tcl").read_text() == "release\n"
    assert subprocess.check_output(
        ["git", "-C", str(repo), "status", "--porcelain"],
        text=True,
    ) == ""


def test_flaky_fix_prompt_requires_root_cause_not_longer_sleep(monkeypatch):
    captured = {}

    def fake_run_agent(profile, prompt, **kwargs):
        captured["prompt"] = prompt
        return MagicMock(returncode=0, stdout="", stderr="")

    proposal = _proposal()
    proposal = proposal.__class__(
        **{**proposal.__dict__, "failure_mode": FailureMode.FLAKY},
    )
    monkeypatch.setattr(apply_mod, "run_agent", fake_run_agent)
    monkeypatch.setattr(apply_mod, "worktree_changed_paths", lambda _r: ("t",))

    apply_fix("/repo", proposal)

    assert "Failure behavior\nflaky" in captured["prompt"]
    assert "Do not merely increase a" in captured["prompt"]
