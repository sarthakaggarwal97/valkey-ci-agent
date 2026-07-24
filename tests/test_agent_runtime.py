from __future__ import annotations

import json

import pytest

from scripts.ai import runtime as agent_runtime


def test_run_agent_applies_profile_and_writes_hashed_evidence(tmp_path, monkeypatch) -> None:
    calls = {}
    repo = tmp_path / "repo"
    repo.mkdir()

    def fake_run_claude_code(prompt, **kwargs):
        calls["prompt"] = prompt
        calls.update(kwargs)
        return "secret stdout", "secret stderr", 0

    monkeypatch.setattr(agent_runtime, "run_claude_code", fake_run_claude_code)
    monkeypatch.delenv("CI_AGENT_CLAUDE_MODEL", raising=False)

    result = agent_runtime.run_agent(
        "conflict_resolve_edit_only",
        "review this",
        cwd=str(repo),
        evidence_dir=tmp_path,
        sandbox_root=repo,
    )

    assert result.returncode == 0
    assert calls["allowed_tools"] == "Read,Edit,MultiEdit,Grep,Glob"
    assert calls["disallowed_tools"] == "Bash,Write"
    assert "GITHUB_TOKEN" not in calls["env_allowlist"]
    assert calls["timeout"] == agent_runtime.AGENT_PROFILES["conflict_resolve_edit_only"].timeout
    assert calls["effort"] == "max"
    assert calls["sandbox_root"] == str(repo)
    assert calls["sandbox_writes_allowed"] is True

    evidence_files = list(tmp_path.glob("*.json"))
    assert len(evidence_files) == 1
    evidence = json.loads(evidence_files[0].read_text(encoding="utf-8"))
    assert evidence["profile"]["name"] == "conflict_resolve_edit_only"
    assert "stdout" not in evidence["result"]
    assert "stderr" not in evidence["result"]
    assert "secret stdout" not in evidence_files[0].read_text(encoding="utf-8")


def test_run_agent_writes_default_github_actions_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.delenv("CI_AGENT_EVIDENCE_DIR", raising=False)
    monkeypatch.setattr(
        agent_runtime,
        "run_claude_code",
        lambda *_args, **_kwargs: ("stdout", "", 0),
    )

    agent_runtime.run_agent(
        "conflict_resolve_edit_only",
        "summarize",
        cwd=str(tmp_path),
        sandbox_root=tmp_path,
    )

    evidence_files = list((tmp_path / "agent-evidence").glob("*.json"))
    assert len(evidence_files) == 1


def test_validation_repair_profile_denies_shell_and_write_tools() -> None:
    profile = agent_runtime.AGENT_PROFILES["validation_repair_edit_only"]

    assert profile.allowed_tools == "Read,Edit,MultiEdit,Grep,Glob"
    assert profile.disallowed_tools == "Bash,Write"


def test_test_adaptation_profile_denies_shell_and_write_tools() -> None:
    profile = agent_runtime.AGENT_PROFILES["test_adaptation_edit_only"]

    assert profile.allowed_tools == "Read,Edit,MultiEdit,Grep,Glob"
    assert profile.disallowed_tools == "Bash,Write"


def test_conflict_resolution_profile_denies_shell_and_write_tools() -> None:
    profile = agent_runtime.AGENT_PROFILES["conflict_resolve_edit_only"]

    assert profile.allowed_tools == "Read,Edit,MultiEdit,Grep,Glob"
    assert profile.disallowed_tools == "Bash,Write"


@pytest.mark.parametrize(
    "profile_name",
    [
        "conflict_resolve_edit_only",
        "test_adaptation_edit_only",
        "validation_repair_edit_only",
    ],
)
def test_backport_ai_profiles_require_process_sandbox(profile_name) -> None:
    assert agent_runtime.AGENT_PROFILES[profile_name].sandbox_required is True


@pytest.mark.parametrize(
    "profile_name",
    [
        "conflict_resolve_edit_only",
        "test_adaptation_edit_only",
        "validation_repair_edit_only",
    ],
)
def test_required_backport_sandbox_cannot_be_omitted(profile_name) -> None:
    with pytest.raises(ValueError, match="requires a filesystem sandbox"):
        agent_runtime.run_agent(profile_name, "prompt", cwd="/tmp/repo")


def test_fuzzer_profile_is_readonly() -> None:
    profile = agent_runtime.AGENT_PROFILES["fuzzer_analysis_readonly"]
    assert profile.writes_allowed is False
    assert "Edit" not in profile.allowed_tools
    assert "Bash" not in profile.allowed_tools
    assert "Read" in profile.allowed_tools
