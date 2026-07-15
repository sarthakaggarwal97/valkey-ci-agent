from __future__ import annotations

import json

import pytest

from scripts.ai import runtime as agent_runtime
from scripts.common.ai_evidence import INDEX_FILE, load_ai_evidence_index
from scripts.common.phase_artifact import ArtifactError


def test_run_agent_applies_profile_and_writes_hashed_evidence(tmp_path, monkeypatch) -> None:
    calls = {}

    def fake_run_claude_code(prompt, **kwargs):
        calls["prompt"] = prompt
        calls.update(kwargs)
        return (
            '{"type":"result","num_turns":3,"total_cost_usd":"0.0123456",'
            '"usage":{"input_tokens":100,"output_tokens":25,'
            '"cache_read_input_tokens":40,"cache_creation_input_tokens":5}}\n',
            "secret stderr",
            0,
        )

    monkeypatch.setattr(agent_runtime, "run_claude_code", fake_run_claude_code)
    monkeypatch.delenv("CI_AGENT_CLAUDE_MODEL", raising=False)

    result = agent_runtime.run_agent(
        "conflict_resolve_edit_only",
        "review this",
        cwd="/tmp/repo",
        evidence_dir=tmp_path,
    )

    assert result.returncode == 0
    assert calls["allowed_tools"] == "Read,Edit,MultiEdit,Grep,Glob"
    assert calls["disallowed_tools"] == "Bash,Write"
    assert calls["dangerously_skip_permissions"] is True
    assert "GITHUB_TOKEN" not in calls["env_allowlist"]
    assert calls["timeout"] == agent_runtime.AGENT_PROFILES["conflict_resolve_edit_only"].timeout
    assert calls["effort"] == "max"
    assert result.input_tokens == 100
    assert result.output_tokens == 25
    assert result.cache_read_input_tokens == 40
    assert result.cache_creation_input_tokens == 5
    assert result.turns == 3
    assert result.cost_microusd == 12_346

    index = json.loads((tmp_path / INDEX_FILE).read_text(encoding="utf-8"))
    assert len(index["runs"]) == 1
    evidence_path = tmp_path / index["runs"][0]["manifest_file"]
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["profile"]["name"] == "conflict_resolve_edit_only"
    assert "stdout" not in evidence["result"]
    assert "stderr" not in evidence["result"]
    stdout = tmp_path / evidence["stdout"]["file"]
    stderr = tmp_path / evidence["stderr"]["file"]
    prompt = tmp_path / evidence["prompt"]["file"]
    assert '"input_tokens":100' in stdout.read_text(encoding="utf-8")
    assert stderr.read_text(encoding="utf-8") == "secret stderr"
    assert prompt.read_text(encoding="utf-8") == "review this"
    digest = agent_runtime.hashlib.sha256(
        (tmp_path / INDEX_FILE).read_bytes(),
    ).hexdigest()
    loaded = load_ai_evidence_index(tmp_path, INDEX_FILE, digest)
    assert loaded[0]["runtime"]["network_policy"] == "internal-model-gateway-only"


def test_run_agent_writes_default_github_actions_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.delenv("CI_AGENT_EVIDENCE_DIR", raising=False)
    monkeypatch.setattr(
        agent_runtime,
        "run_claude_code",
        lambda *_args, **_kwargs: ("stdout", "", 0),
    )

    agent_runtime.run_agent("conflict_resolve_edit_only", "summarize", cwd=str(tmp_path))

    evidence_files = list((tmp_path / "agent-evidence").glob("ai-run-*.json"))
    assert len(evidence_files) == 1
    assert (tmp_path / "agent-evidence" / INDEX_FILE).is_file()


def test_ai_evidence_rejects_transcript_tampering(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        agent_runtime,
        "run_claude_code",
        lambda *_args, **_kwargs: ("complete transcript", "", 0),
    )
    agent_runtime.run_agent(
        "fuzzer_analysis_readonly",
        "analyze",
        cwd=str(tmp_path),
        evidence_dir=tmp_path,
    )
    index = json.loads((tmp_path / INDEX_FILE).read_text(encoding="utf-8"))
    manifest = json.loads(
        (tmp_path / index["runs"][0]["manifest_file"]).read_text(encoding="utf-8"),
    )
    (tmp_path / manifest["stdout"]["file"]).write_text(
        "tampered transcript",
        encoding="utf-8",
    )
    digest = agent_runtime.hashlib.sha256(
        (tmp_path / INDEX_FILE).read_bytes(),
    ).hexdigest()

    with pytest.raises(ArtifactError, match="content does not match"):
        load_ai_evidence_index(tmp_path, INDEX_FILE, digest)


def test_validation_repair_profile_denies_shell_and_write_tools() -> None:
    profile = agent_runtime.AGENT_PROFILES["validation_repair_edit_only"]

    assert profile.allowed_tools == "Read,Edit,MultiEdit,Grep,Glob"
    assert profile.disallowed_tools == "Bash,Write"


def test_fuzzer_profile_is_readonly() -> None:
    profile = agent_runtime.AGENT_PROFILES["fuzzer_analysis_readonly"]
    assert profile.writes_allowed is False
    assert "Edit" not in profile.allowed_tools
    assert "Bash" not in profile.allowed_tools
    assert "Read" in profile.allowed_tools
