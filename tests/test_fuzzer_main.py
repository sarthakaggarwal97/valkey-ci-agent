"""Tests for fuzzer main CLI."""
from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

import scripts.fuzzer.main as fuzzer_main_mod


def _mock_gh_returning(runs: list) -> MagicMock:
    """Build a mocked Github class whose workflow.get_runs() yields `runs`."""
    mock_workflow = MagicMock()
    mock_workflow.get_runs.return_value = iter(runs)
    mock_repo = MagicMock()
    mock_repo.get_workflow.return_value = mock_workflow
    mock_gh_cls = MagicMock()
    mock_gh_cls.return_value.get_repo.return_value = mock_repo
    return mock_gh_cls


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


def _batch(*findings) -> MagicMock:
    return MagicMock(overall_status="anomalous", batch_status="FAIL", counts={"FAIL": 2}, findings=list(findings))


def _finding(**kw) -> MagicMock:
    defaults = dict(kind="signature", overall_status="anomalous", triage_verdict="likely-core-valkey-bug",
                    summary="real bug", incident_fingerprint="fp-abc", batch_runs=["a", "b"],
                    signature_key="cc08b3931ccc", rerun_verdict="reproduced")
    defaults.update(kw)
    return MagicMock(**defaults)


def test_publish_skipped_when_fingerprint_missing(monkeypatch, capsys):
    """Refuse to upsert when fingerprint is empty — otherwise unrelated
    findings would collide on a single issue.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=42, conclusion="failure", html_url="https://x/runs/42")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = _batch(
            _finding(triage_verdict="needs-human-triage", incident_fingerprint=None),
        )
        rc = fuzzer_main_mod.main([])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["runs"][0]["findings"][0]["issue_action"] == "skipped-no-fingerprint"
    mock_pub_cls.return_value.upsert.assert_not_called()


def test_each_finding_gets_its_own_issue_keyed_by_the_run(monkeypatch, capsys):
    """One issue per finding, each with the run id as idempotency_key so a
    re-run of the monitor against the same fuzzer run does not bump the
    occurrence counter. A dismissed finding files nothing.
    """
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=7777, conclusion="failure", html_url="https://x/runs/7777")
    findings = [
        _finding(),
        _finding(kind="zoo", triage_verdict="needs-human-triage", incident_fingerprint="fp-zoo",
                 signature_key=None, rerun_verdict=None),
        _finding(kind="run", overall_status="warning", triage_verdict="expected-chaos-noise",
                 incident_fingerprint="fp-noise", signature_key=None, rerun_verdict=None),
    ]
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = _batch(*findings)
        mock_pub_cls.return_value.upsert.side_effect = [("created", "https://x/issues/1"),
                                                        ("updated", "https://x/issues/2")]
        rc = fuzzer_main_mod.main([])

    assert rc == 0
    calls = mock_pub_cls.return_value.upsert.call_args_list
    assert [c.kwargs["fingerprint"] for c in calls] == ["fp-abc", "fp-zoo"]
    assert {c.kwargs["idempotency_key"] for c in calls} == {"7777"}
    entry = json.loads(capsys.readouterr().out)["runs"][0]
    assert (entry["batch_status"], entry["counts"]) == ("FAIL", {"FAIL": 2})
    assert [f.get("issue_action") for f in entry["findings"]] == ["created", "updated", None]
    assert entry["findings"][0]["runs"] == ["a", "b"]
    assert entry["findings"][0]["rerun_verdict"] == "reproduced"


def test_outcome_lists_the_issues():
    level, message = fuzzer_main_mod._outcome([{
        "run_id": 5, "action": "analyzed", "status": "anomalous", "batch_status": "FAIL",
        "findings": [{"issue_action": "created", "issue_url": "https://x/issues/1"}, {}],
    }], dry_run=False)
    assert level == logging.WARNING
    assert message == ("Fuzzer run 5: batch FAIL, 2 finding(s), status anomalous; "
                       "issues created: https://x/issues/1")
    level, message = fuzzer_main_mod._outcome([{
        "run_id": 5, "action": "analyzed", "status": "normal", "batch_status": "PASS", "findings": [],
    }], dry_run=False)
    assert (level, message) == (logging.INFO, "Fuzzer run 5: batch PASS, 0 finding(s), status normal; no issue needed")


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


def test_a_pr_batch_run_is_analyzed_and_filed_on_the_given_repo(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    run = MagicMock(id=11, conclusion="failure", html_url="https://x/runs/11")
    github_cls = MagicMock()
    github_cls.return_value.get_repo.return_value.get_workflow_run.return_value = run
    with patch.object(fuzzer_main_mod, "Github", github_cls), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        analyze = mock_analyzer_cls.return_value.analyze
        analyze.return_value = _batch(_finding())
        mock_pub_cls.return_value.upsert.return_value = ("created", "https://x/issues/1")
        rc = fuzzer_main_mod.main(["--fuzzer-repo", "owner/valkey-fuzzer", "--pr-batch-run", "11",
                                   "--change", "valkey-io/valkey#4716, merged as e082696cf000"])

    assert rc == 0
    github_cls.return_value.get_repo.return_value.get_workflow_run.assert_called_once_with(11)
    analyze.assert_called_once_with(
        "owner/valkey-fuzzer", 11, workflow_file="fuzzer-pr-batch.yml",
        change="valkey-io/valkey#4716, merged as e082696cf000", artifact_name="fuzzer-batch-11",
    )
    assert mock_pub_cls.return_value.upsert.call_args.args == ("owner/valkey-fuzzer",)
    payload = json.loads(capsys.readouterr().out)
    assert (payload["target_repo"], payload["runs"][0]["run_id"]) == ("owner/valkey-fuzzer", 11)


def test_a_finding_without_an_issue_of_its_own_asks_for_one_with_its_root_cause(monkeypatch, capsys):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    mock_run = MagicMock(id=7777, conclusion="failure", html_url="https://x/runs/7777")
    asked = []
    monkeypatch.setattr(fuzzer_main_mod, "same_root_cause",
                        lambda finding, title, issues, **kw: asked.append((finding.incident_fingerprint, issues)) or 5)
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([mock_run])), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher") as mock_pub_cls:
        mock_analyzer_cls.return_value.analyze.return_value = _batch(_finding())
        mock_pub_cls.return_value.upsert.return_value = ("updated", "https://x/issues/5")
        assert fuzzer_main_mod.main([]) == 0

    match = mock_pub_cls.return_value.upsert.call_args.kwargs["match_existing"]
    assert match(["open issue"]) == 5
    assert asked == [("fp-abc", ["open issue"])]


def test_the_change_diff_goes_to_the_analyzer_and_the_change_runs_to_the_output(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    diff = tmp_path / "change.diff"
    diff.write_text("diff --git a/src/t_list.c b/src/t_list.c\n")
    run = MagicMock(id=11, conclusion="failure", html_url="https://x/runs/11")
    github_cls = MagicMock()
    github_cls.return_value.get_repo.return_value.get_workflow_run.return_value = run
    recorded = [{"run": "scenario-lset", "experiment": "proposed.lset", "outcome": "unreached"}]
    with patch.object(fuzzer_main_mod, "Github", github_cls), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls:
        analysis = _batch()
        analysis.change_runs = {"reached": 2, "unreached": 1, "unexplained": 0, "unvetted": 1}
        analysis.recorded = recorded
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        rc = fuzzer_main_mod.main(["--fuzzer-repo", "owner/valkey-fuzzer", "--pr-batch-run", "11",
                                   "--change", "valkey-io/valkey#4823", "--change-diff", str(diff)])

    assert rc == 0
    assert mock_analyzer_cls.call_args.kwargs["change_diff"] == "diff --git a/src/t_list.c b/src/t_list.c\n"
    entry = json.loads(capsys.readouterr().out)["runs"][0]
    assert entry["change_runs"] == {"reached": 2, "unreached": 1, "unexplained": 0, "unvetted": 1}
    assert entry["recorded"] == recorded


def test_the_change_diff_needs_a_pr_batch_run(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    diff = tmp_path / "change.diff"
    diff.write_text("d")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([])), pytest.raises(SystemExit):
        fuzzer_main_mod.main(["--change-diff", str(diff)])
    assert "--change-diff needs --pr-batch-run" in capsys.readouterr().err


def test_outcome_counts_the_change_runs():
    level, message = fuzzer_main_mod._outcome([{
        "run_id": 5, "action": "analyzed", "status": "warning", "batch_status": "ERROR", "findings": [],
        "change_runs": {"reached": 2, "unreached": 1, "unexplained": 0, "unvetted": 0},
    }], dry_run=False)
    assert level == logging.INFO
    assert message == ("Fuzzer run 5: batch ERROR, change runs reached 2, unreached 1, unexplained 0, unvetted 0, "
                       "0 finding(s), status warning; no issue needed")


def test_the_pr_tested_gets_one_spec_comment_that_a_rerun_edits(monkeypatch, capsys):
    from scripts.fuzzer.batch import read_batch_dir, spec_results
    from tests.fuzzer_batch_fixture import SPEC_BATCH

    monkeypatch.setenv("TARGET_TOKEN", "fake")
    monkeypatch.setenv("PR_TOKEN", "pr-fake")
    run = MagicMock(id=11, conclusion="failure", html_url="https://x/runs/11")
    fuzzer_gh, pr_gh = MagicMock(), MagicMock()
    fuzzer_gh.get_repo.return_value.get_workflow_run.return_value = run
    pull = pr_gh.get_repo.return_value.get_pull.return_value
    pull.get_issue_comments.return_value = [MagicMock(body="lgtm")]
    pull.create_issue_comment.return_value = MagicMock(html_url="https://x/pull/4823#c1")
    tokens = []
    analysis = _batch()
    analysis.spec = spec_results(read_batch_dir(SPEC_BATCH))
    argv = ["--fuzzer-repo", "owner/valkey-fuzzer", "--pr-batch-run", "11", "--pr", "valkey-io/valkey#4823"]
    with patch.object(fuzzer_main_mod, "Auth", MagicMock(Token=lambda t: tokens.append(t) or t)), \
         patch.object(fuzzer_main_mod, "Github", lambda auth: pr_gh if auth == "pr-fake" else fuzzer_gh), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher"):
        mock_analyzer_cls.return_value.analyze.return_value = analysis
        assert fuzzer_main_mod.main(argv) == 0
        first = json.loads(capsys.readouterr().out)["runs"][0]

        # The re-run finds the comment it made and edits it in place.
        mine = MagicMock(body=pull.create_issue_comment.call_args.args[0], html_url="https://x/pull/4823#c1")
        pull.get_issue_comments.return_value = [MagicMock(body="lgtm"), mine]
        assert fuzzer_main_mod.main(argv) == 0
        second = json.loads(capsys.readouterr().out)["runs"][0]

    assert tokens == ["fake", "pr-fake", "fake", "pr-fake"]
    pr_gh.get_repo.assert_called_with("valkey-io/valkey")
    pr_gh.get_repo.return_value.get_pull.assert_called_with(4823)
    body = pull.create_issue_comment.call_args.args[0]
    assert body.startswith("<!-- valkey-ci-agent:fuzzer-spec-conform -->")
    assert "fuzzer PR batch [run 11](https://x/runs/11)" in body and "| `cluster-election` | approved |" in body
    assert pull.create_issue_comment.call_count == 1
    mine.edit.assert_called_once_with(body)
    assert (first["spec_comment"], second["spec_comment"]) == ("created", "updated")
    assert first["spec"][0]["bundle"] == "cluster-election"


def test_no_spec_comment_without_a_pr_or_without_bundles(monkeypatch, tmp_path):
    from scripts.fuzzer.batch import read_batch_dir, spec_results
    from tests.fuzzer_batch_fixture import SPEC_BATCH

    monkeypatch.setenv("TARGET_TOKEN", "fake")
    run = MagicMock(id=11, conclusion="failure", html_url="https://x/runs/11")
    github_cls = MagicMock()
    github_cls.return_value.get_repo.return_value.get_workflow_run.return_value = run
    argv = ["--fuzzer-repo", "o/valkey-fuzzer", "--pr-batch-run", "11"]
    with patch.object(fuzzer_main_mod, "Github", github_cls), \
         patch.object(fuzzer_main_mod, "FuzzerRunAnalyzer") as mock_analyzer_cls, \
         patch.object(fuzzer_main_mod, "IssueDedupPublisher"):
        with_bundles = _batch()
        with_bundles.spec = spec_results(read_batch_dir(SPEC_BATCH))
        mock_analyzer_cls.return_value.analyze.return_value = with_bundles
        assert fuzzer_main_mod.main([*argv, "--output", str(tmp_path / "no-pr.json")]) == 0
        without = _batch()
        without.spec = []
        mock_analyzer_cls.return_value.analyze.return_value = without
        assert fuzzer_main_mod.main([*argv, "--pr", "valkey-io/valkey#1", "--output", str(tmp_path / "none.json")]) == 0

    github_cls.return_value.get_repo.return_value.get_pull.assert_not_called()
    for name in ("no-pr.json", "none.json"):
        assert "spec_comment" not in json.loads((tmp_path / name).read_text())["runs"][0]


@pytest.mark.parametrize("argv, message", [
    (["--pr", "valkey-io/valkey#1"], "--pr needs --pr-batch-run"),
    (["--pr-batch-run", "11", "--pr", "valkey-io/valkey/pull/1"], "--pr must be OWNER/NAME#NUMBER"),
])
def test_pr_needs_a_pr_batch_run_and_owner_name_number(monkeypatch, capsys, argv, message):
    monkeypatch.setenv("TARGET_TOKEN", "fake")
    with patch.object(fuzzer_main_mod, "Github", _mock_gh_returning([])), pytest.raises(SystemExit):
        fuzzer_main_mod.main(argv)
    assert message in capsys.readouterr().err
