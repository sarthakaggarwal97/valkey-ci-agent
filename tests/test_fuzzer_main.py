"""Tests for fuzzer main CLI."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import scripts.fuzzer.main as fuzzer_main_mod
from scripts.fuzzer.models import FuzzerRunAnalysis


def _mock_gh_returning(runs: list) -> MagicMock:
    """Build a mocked Github class whose workflow.get_runs() yields `runs`."""
    mock_workflow = MagicMock()
    mock_workflow.get_runs.return_value = iter(runs)
    mock_repo = MagicMock()
    mock_repo.get_workflow.return_value = mock_workflow
    mock_gh_cls = MagicMock()
    mock_gh_cls.return_value.get_repo.return_value = mock_repo
    return mock_gh_cls


def _valid_analysis(**updates) -> FuzzerRunAnalysis:
    values = {
        "repo": "valkey-io/valkey-fuzzer",
        "workflow_file": "fuzzer-run.yml",
        "run_id": 42,
        "run_url": "https://github.com/valkey-io/valkey-fuzzer/actions/runs/42",
        "conclusion": "failure",
        "head_sha": "a" * 40,
        "overall_status": "anomalous",
        "triage_verdict": "needs-human-triage",
        "summary": "Analysis summary",
        "incident_fingerprint": "b" * 20,
    }
    values.update(updates)
    return FuzzerRunAnalysis(**values)


def test_requires_token(capsys, monkeypatch):
    monkeypatch.delenv("TARGET_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        fuzzer_main_mod.main([])
    err = capsys.readouterr().err
    assert "target-token" in err or "TARGET_TOKEN" in err


def test_dry_run_prints_runs(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=42, conclusion="success", html_url="https://x/runs/42")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])):
        rc = fuzzer_main_mod.main(["--dry-run"])
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["runs"][0]["action"] == "would-analyze"


def test_output_flag_writes_file(monkeypatch, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    out = tmp_path / "result.json"
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([])):
        rc = fuzzer_main_mod.main(["--dry-run", "--output", str(out)])
    assert rc == 0
    assert json.loads(out.read_text())["runs"] == []


def test_analysis_error_recorded_and_exits_nonzero(monkeypatch, capsys):
    """An exception inside analyze() is captured per-run, and the monitor
    exits non-zero so the workflow shows ❌ instead of hiding the error.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=99, conclusion="failure", html_url="https://x/runs/99")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls:
        mock_analyzer_cls.return_value.analyze.side_effect = RuntimeError("boom")
        rc = fuzzer_main_mod.main([])
    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["runs"][0]["action"] == "error"
    assert "boom" in payload["runs"][0]["error"]


def test_missing_fingerprint_fails_without_publishing(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=42, conclusion="failure", html_url="https://x/runs/42")
    bad_analysis = _valid_analysis(
        overall_status="anomalous", triage_verdict="needs-human-triage",
        summary="oops", incident_fingerprint=None,
    )
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = bad_analysis
        rc = fuzzer_main_mod.main([])
    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["runs"][0]["action"] == "error"
    assert "incident_fingerprint" in payload["runs"][0]["error"]
    mock_pub_cls.return_value.upsert.assert_not_called()


def test_publish_passes_run_id_as_idempotency_key(monkeypatch, capsys):
    """Run id must be passed as idempotency_key so a re-run of the monitor
    against the same fuzzer run does not bump the occurrence counter.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=7777, conclusion="failure", html_url="https://x/runs/7777")
    analysis = _valid_analysis(
        run_id=7777,
        run_url="https://github.com/valkey-io/valkey-fuzzer/actions/runs/7777",
        overall_status="anomalous", triage_verdict="likely-core-valkey-bug",
        summary="real bug", incident_fingerprint="c" * 20,
    )
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        mock_pub_cls.return_value.upsert.return_value = ("created", "https://x/issues/1")
        fuzzer_main_mod.main([])

    kwargs = mock_pub_cls.return_value.upsert.call_args.kwargs
    assert kwargs["idempotency_key"] == "7777"
    assert kwargs["fingerprint"] == "c" * 20


def test_invalid_analysis_is_not_published(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=42, conclusion="failure", html_url="https://x/runs/42")
    analysis = _valid_analysis(overall_status="invented")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        rc = fuzzer_main_mod.main([])

    assert rc == 1
    assert json.loads(capsys.readouterr().out)["runs"][0]["action"] == "error"
    mock_pub_cls.return_value.upsert.assert_not_called()


def test_analysis_for_different_run_is_not_published(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=42, conclusion="failure", html_url="https://x/runs/42")
    analysis = _valid_analysis(
        run_id=43,
        run_url="https://github.com/valkey-io/valkey-fuzzer/actions/runs/43",
    )
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        rc = fuzzer_main_mod.main([])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert "does not match the requested run" in payload["runs"][0]["error"]
    mock_pub_cls.return_value.upsert.assert_not_called()


def _analysis_obj(*, status: str, verdict: str) -> MagicMock:
    a = MagicMock()
    a.overall_status = status
    a.triage_verdict = verdict
    return a


@pytest.mark.parametrize("status,verdict,expected", [
    ("anomalous", "likely-core-valkey-bug", True),
    ("warning", "possible-core-valkey-bug", True),     # the bug this guards
    ("warning", "needs-human-triage", True),
    ("normal", "expected-chaos-noise", False),
    ("warning", "expected-chaos-noise", False),
    ("warning", "environmental-or-infra", False),
])
def test_should_publish_gate(status, verdict, expected):
    assert fuzzer_main_mod._should_publish(_analysis_obj(status=status, verdict=verdict)) is expected
