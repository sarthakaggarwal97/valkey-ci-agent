"""Tests for fuzzer main CLI."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import scripts.fuzzer.main as fuzzer_main_mod


def _mock_gh_returning(runs: list) -> MagicMock:
    """Build a mocked Github class whose workflow.get_runs() yields `runs`."""
    mock_workflow = MagicMock()
    mock_workflow.get_runs.return_value = iter(runs)
    mock_repo = MagicMock()
    mock_repo.get_workflow.return_value = mock_workflow

    def create_issue(*, title, body):
        issue = MagicMock(number=900, body=body)
        mock_repo.get_issue.return_value = issue
        return issue

    mock_repo.create_issue.side_effect = create_issue
    mock_gh_cls = MagicMock()
    mock_gh_cls.return_value.get_repo.return_value = mock_repo
    mock_gh_cls.return_value.search_issues.return_value = []
    return mock_gh_cls


def _run(
    run_id: int,
    *,
    status: str = "completed",
    conclusion: str = "success",
) -> MagicMock:
    return MagicMock(
        id=run_id,
        status=status,
        conclusion=conclusion,
        html_url=(
            "https://github.com/valkey-io/valkey-fuzzer/"
            f"actions/runs/{run_id}"
        ),
    )


def test_requires_token(capsys, monkeypatch):
    monkeypatch.delenv("TARGET_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        fuzzer_main_mod.main([])
    err = capsys.readouterr().err
    assert "target-token" in err or "TARGET_TOKEN" in err


def test_dry_run_prints_runs(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = _run(42)
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


def test_rejects_invalid_batch_size(monkeypatch):
    monkeypatch.setenv("TARGET_TOKEN", "fake")

    with pytest.raises(SystemExit):
        fuzzer_main_mod.main(["--max-runs", "0"])


def test_workflow_renews_credentials_between_live_backlog_runs():
    workflow = Path(".github/workflows/monitor-fuzzer.yml").read_text(
        encoding="utf-8"
    )

    assert workflow.count("actions/create-github-app-token@") == 2
    assert workflow.count("aws-actions/configure-aws-credentials@") == 2
    assert workflow.count("--max-runs 1") == 1
    assert '--max-runs "${max_runs}"' in workflow
    assert "fuzzer-monitor-result-*.json" in workflow


def test_analysis_error_recorded_and_exits_nonzero(monkeypatch, capsys):
    """An exception inside analyze() is captured per-run, and the monitor
    exits non-zero so the workflow shows ❌ instead of hiding the error.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = _run(99, conclusion="failure")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls:
        mock_analyzer_cls.return_value.analyze.side_effect = RuntimeError("boom")
        rc = fuzzer_main_mod.main([])
    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["runs"][0]["action"] == "error"
    assert "boom" in payload["runs"][0]["error"]


def test_publish_without_fingerprint_blocks_cursor(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = _run(42, conclusion="failure")
    bad_analysis = MagicMock(
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
    assert "without a fingerprint" in payload["runs"][0]["error"]
    assert payload["final_cursor"] == 41
    assert payload["bootstrap_anchor"] == 41
    mock_pub_cls.return_value.upsert.assert_not_called()


def test_publish_passes_run_id_as_idempotency_key(monkeypatch, capsys):
    """Run id must be passed as idempotency_key so a re-run of the monitor
    against the same fuzzer run does not bump the occurrence counter.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = _run(7777, conclusion="failure")
    analysis = MagicMock(
        overall_status="anomalous", triage_verdict="likely-core-valkey-bug",
        summary="real bug", incident_fingerprint="fp-abc",
    )
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        mock_pub_cls.return_value.upsert.return_value = ("created", "https://x/issues/1")
        fuzzer_main_mod.main([])

    kwargs = mock_pub_cls.return_value.upsert.call_args.kwargs
    assert kwargs["idempotency_key"] == "7777"
    assert kwargs["fingerprint"] == "fp-abc"


def test_selects_oldest_completed_runs_after_cursor():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(105),
        _run(104),
        _run(103),
        _run(102),
        _run(100),
    ]

    selected = fuzzer_main_mod._select_unprocessed_runs(
        workflow,
        cursor=100,
        max_runs=2,
    )

    assert [run.id for run in selected] == [102, 103]


def test_pending_run_blocks_newer_completed_run():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(104),
        _run(103, status="in_progress"),
        _run(102),
        _run(100),
    ]

    selected = fuzzer_main_mod._select_unprocessed_runs(
        workflow,
        cursor=100,
        max_runs=4,
    )

    assert [run.id for run in selected] == [102]


def test_bootstrap_starts_at_newest_completed_run():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(105, status="in_progress"),
        _run(104),
        _run(103),
    ]

    selected = fuzzer_main_mod._select_unprocessed_runs(
        workflow,
        cursor=0,
        max_runs=4,
    )

    assert [run.id for run in selected] == [104]


def test_processes_backlog_in_order_and_advances_each_run(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    runs = [_run(103), _run(102), _run(100)]
    state_store = MagicMock()
    state_store.read.return_value = MagicMock(cursor=100)
    normal = MagicMock(
        overall_status="normal",
        triage_verdict="expected-chaos-noise",
        summary="normal",
        incident_fingerprint="unused",
    )

    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning(runs)), \
         patch.object(fuzzer_main_mod, "FuzzerStateStore", return_value=state_store), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as analyzer_cls:
        analyzer_cls.return_value.analyze.return_value = normal
        rc = fuzzer_main_mod.main(["--max-runs", "2"])

    assert rc == 0
    assert [
        call.args[1] for call in analyzer_cls.return_value.analyze.call_args_list
    ] == [102, 103]
    assert [
        call.kwargs["expected_cursor"]
        for call in state_store.advance.call_args_list
    ] == [100, 102]
    payload = json.loads(capsys.readouterr().out)
    assert payload["final_cursor"] == 103


def test_processing_stops_at_first_failed_backlog_run(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    runs = [_run(103), _run(102), _run(101), _run(100)]
    state_store = MagicMock()
    state_store.read.return_value = MagicMock(cursor=100)
    normal = MagicMock(
        overall_status="normal",
        triage_verdict="expected-chaos-noise",
        summary="normal",
        incident_fingerprint="unused",
    )

    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning(runs)), \
         patch.object(fuzzer_main_mod, "FuzzerStateStore", return_value=state_store), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as analyzer_cls:
        analyzer_cls.return_value.analyze.side_effect = [
            normal,
            RuntimeError("analysis failed"),
        ]
        rc = fuzzer_main_mod.main(["--max-runs", "3"])

    assert rc == 1
    assert [
        call.args[1] for call in analyzer_cls.return_value.analyze.call_args_list
    ] == [101, 102]
    state_store.advance.assert_called_once()
    assert state_store.advance.call_args.kwargs["run_id"] == 101
    payload = json.loads(capsys.readouterr().out)
    assert payload["final_cursor"] == 101
    assert [entry["run_id"] for entry in payload["runs"]] == [101, 102]


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
