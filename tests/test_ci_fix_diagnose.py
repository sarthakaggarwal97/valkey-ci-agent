"""Tests for the AI diagnose step.

The parsing is what we own deterministically: the agent's structured output
becomes a typed ``FixProposal``, an unrecognized path collapses to REFUSE, and
agent failures raise. The NAN-payload backport failure (PRs #3988/#3989) is the
end-to-end shape we expect for an "author" proposal.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import diagnose as diagnose_mod
from scripts.ci_fix.diagnose import diagnose_failure, write_log_to_workspace
from scripts.ci_fix.models import FixPath


def _stream_json_result(obj: dict) -> str:
    """Wrap a payload the way Claude Code stream-json emits a final result."""
    return "\n".join([
        json.dumps({"type": "system", "subtype": "init"}),
        json.dumps({"type": "result", "subtype": "success", "result": json.dumps(obj)}),
    ])


def _mock_agent(monkeypatch, stdout: str, returncode: int = 0) -> None:
    monkeypatch.setattr(
        diagnose_mod, "run_agent",
        MagicMock(return_value=MagicMock(stdout=stdout, stderr="", returncode=returncode)),
    )


def test_author_proposal_nan_payload(monkeypatch):
    payload = {
        "path": "author",
        "failing_test": "corrupt payload: zset listpack with NAN score",
        "root_cause": "RESTORE payload embeds RDB version 80; on this branch "
                      "RDB_VERSION is 11, so RESTORE rejects it before the NAN check.",
        "reasoning": "Test scaffolding: set the payload's RDB version byte to the "
                     "branch RDB_VERSION and zero the checksum. Assertion unchanged.",
        "confidence": 0.9,
        "build_command": "make -j4",
        "test_command": "./runtest --single integration/corrupt-dump --dump-logs",
        "workdir": "",
        "unstable_fix_commit": "",
        "other_failing_tests": [],
    }
    _mock_agent(monkeypatch, _stream_json_result(payload))

    proposal = diagnose_failure("/tmp/ci.log", "/tmp/repo")
    assert proposal.path is FixPath.AUTHOR
    assert "NAN score" in proposal.failing_test
    assert proposal.test_command.startswith("./runtest")
    assert proposal.confidence == 0.9


def test_port_proposal_carries_commit(monkeypatch):
    payload = {
        "path": "port",
        "failing_test": "some test",
        "root_cause": "already fixed upstream",
        "reasoning": "clean cherry-pick",
        "confidence": 0.8,
        "build_command": "make",
        "test_command": "./runtest --single unit/x",
        "unstable_fix_commit": "abc123",
    }
    _mock_agent(monkeypatch, _stream_json_result(payload))

    proposal = diagnose_failure("/tmp/ci.log", "/tmp/repo")
    assert proposal.path is FixPath.PORT
    assert proposal.unstable_fix_commit == "abc123"


def test_refuse_proposal(monkeypatch):
    payload = {
        "path": "refuse",
        "failing_test": "flaky timing test",
        "root_cause": "intermittent timing dependency",
        "reasoning": "genuinely flaky; no safe deterministic fix",
        "confidence": 0.2,
    }
    _mock_agent(monkeypatch, _stream_json_result(payload))

    proposal = diagnose_failure("/tmp/ci.log", "/tmp/repo")
    assert proposal.path is FixPath.REFUSE
    assert proposal.build_command == ""


def test_unknown_path_collapses_to_refuse(monkeypatch):
    payload = {"path": "yolo", "failing_test": "t", "confidence": 0.99}
    _mock_agent(monkeypatch, _stream_json_result(payload))

    proposal = diagnose_failure("/tmp/ci.log", "/tmp/repo")
    assert proposal.path is FixPath.REFUSE


def test_confidence_clamped(monkeypatch):
    payload = {"path": "refuse", "confidence": 5.0}
    _mock_agent(monkeypatch, _stream_json_result(payload))
    assert diagnose_failure("/tmp/ci.log", "/tmp/repo").confidence == 1.0


def test_agent_failure_raises(monkeypatch):
    _mock_agent(monkeypatch, "", returncode=1)
    with pytest.raises(RuntimeError, match="diagnosis agent failed"):
        diagnose_failure("/tmp/ci.log", "/tmp/repo")


def test_no_json_raises(monkeypatch):
    _mock_agent(monkeypatch, "no json here at all")
    with pytest.raises(ValueError, match="no diagnosis JSON"):
        diagnose_failure("/tmp/ci.log", "/tmp/repo")


def test_plain_json_without_stream_wrapper(monkeypatch):
    """The agent may emit a bare JSON object, not wrapped in stream-json."""
    payload = {"path": "refuse", "failing_test": "t", "confidence": 0.1}
    _mock_agent(monkeypatch, json.dumps(payload))
    assert diagnose_failure("/tmp/ci.log", "/tmp/repo").path is FixPath.REFUSE


def test_hint_is_included_in_prompt(monkeypatch):
    captured = {}

    def fake_run_agent(profile, prompt, **kwargs):
        captured["prompt"] = prompt
        return MagicMock(
            stdout=_stream_json_result({"path": "refuse", "confidence": 0.0}),
            stderr="", returncode=0,
        )

    monkeypatch.setattr(diagnose_mod, "run_agent", fake_run_agent)
    diagnose_failure("/tmp/ci.log", "/tmp/repo", hint="look at the valgrind timeout")
    assert "valgrind timeout" in captured["prompt"]
    assert "Maintainer hint" in captured["prompt"]


def test_long_hint_is_truncated(monkeypatch):
    captured = {}

    def fake_run_agent(profile, prompt, **kwargs):
        captured["prompt"] = prompt
        return MagicMock(
            stdout=_stream_json_result({"path": "refuse", "confidence": 0.0}),
            stderr="", returncode=0,
        )

    monkeypatch.setattr(diagnose_mod, "run_agent", fake_run_agent)
    long_hint = "Z" * 2000
    diagnose_failure("/tmp/ci.log", "/tmp/repo", hint=long_hint)
    # The 2000-char hint is capped at the 500-char limit ("Z" appears nowhere
    # else in the prompt template).
    assert captured["prompt"].count("Z") == 500


def test_write_log_to_workspace_concatenates(tmp_path):
    logs = {
        "2_test.txt": b"[err]: NAN score",
        "1_build.txt": b"make output",
    }
    combined = write_log_to_workspace(logs, tmp_path)
    text = combined.read_text()
    # Sorted by name: build (1_) before test (2_).
    assert text.index("1_build.txt") < text.index("2_test.txt")
    assert "NAN score" in text
