"""Tests for the edit-only fix application step."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

from scripts.ci_fix import apply as apply_mod
from scripts.ci_fix.apply import apply_fix, apply_port_commit
from scripts.ci_fix.models import FixPath, FixProposal


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


def test_port_cherry_picks_clean_commit(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "f.txt").write_text("base\n")
    subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "base"], check=True)
    subprocess.run(["git", "-C", str(repo), "checkout", "-qb", "unstable"], check=True)
    (repo / "f.txt").write_text("fixed\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "fix upstream", "-q"], check=True)
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "release"], check=True)

    result = apply_port_commit(str(repo), commit)

    assert result.ok is True
    assert result.changed_paths == ("f.txt",)
    assert (repo / "f.txt").read_text() == "fixed\n"


def test_apply_fix_port_never_calls_agent(monkeypatch, tmp_path):
    agent = MagicMock()
    monkeypatch.setattr(apply_mod, "run_agent", agent)
    monkeypatch.setattr(
        apply_mod, "apply_port_commit",
        lambda *_a, **_k: apply_mod.PortApplyResult(ok=True, changed_paths=("f.txt",)),
    )
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": "abc1234"}
    )
    ok, changed = apply_fix(str(tmp_path), proposal)
    assert ok is True
    assert changed == ("f.txt",)
    agent.assert_not_called()


def test_port_conflict_refuses_and_aborts(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "f.txt").write_text("base\n")
    subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "base"], check=True)
    subprocess.run(["git", "-C", str(repo), "checkout", "-qb", "unstable"], check=True)
    (repo / "f.txt").write_text("upstream\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "fix upstream", "-q"], check=True)
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "release"], check=True)
    (repo / "f.txt").write_text("release diverged\n")
    subprocess.run(["git", "-C", str(repo), "commit", "-am", "release edit", "-q"], check=True)

    result = apply_port_commit(str(repo), commit)

    assert result.ok is False
    assert "cherry-pick" in result.detail
    status = subprocess.run(
        ["git", "-C", str(repo), "status", "--short"],
        check=True, capture_output=True, text=True,
    ).stdout
    assert status == ""
