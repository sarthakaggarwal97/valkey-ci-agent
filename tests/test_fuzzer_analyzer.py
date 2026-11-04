"""Tests for fuzzer analyzer."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts.fuzzer.analyzer import (
    _context_for,
    _dedupe_signals,
    _final_verdict,
    _parse_claude_response,
    _rerun_adjusted,
    _scan_logs,
    _triage,
)
from scripts.fuzzer.batch import read_batch
from scripts.fuzzer.models import FuzzerRunContext, FuzzerSignal
from tests.fuzzer_batch_fixture import BATCH, CRASH, batch_files, batch_summary, crash_report, with_report, with_summary

_BASE = dict(repo="r", workflow_file="w", run_id=1, run_url="u", conclusion="failure", head_sha="h")


def _ctx(**kw) -> FuzzerRunContext:
    defaults = dict(_BASE, job_id="run-1")
    defaults.update(kw)
    return FuzzerRunContext(**defaults)


def test_scan_logs_detects_crash():
    ctx = _ctx()
    ctx.node_logs = {"node-1.log": "ASSERTION FAILED at server.c:123"}
    anomalies = _scan_logs(ctx)
    assert any("crash" in a.title.lower() or "assertion" in a.title.lower() for a in anomalies)


def test_triage_normal():
    assert _triage([]) == ("normal", "expected-chaos-noise")


def test_triage_critical_bug_indicator():
    status, verdict = _triage([FuzzerSignal("Node crash or assertion", "critical", "x")])
    assert status == "anomalous"
    assert verdict == "likely-core-valkey-bug"


def test_triage_critical_non_bug_indicator():
    # OOM is critical but not in the bug-indicator subset.
    status, verdict = _triage([FuzzerSignal("OOM", "critical", "x")])
    assert (status, verdict) == ("anomalous", "possible-core-valkey-bug")


def test_triage_warning():
    assert _triage([FuzzerSignal("X", "warning", "y")]) == ("warning", "possible-core-valkey-bug")


def test_dedupe_signals():
    signals = [
        FuzzerSignal("a", "critical", "x"),
        FuzzerSignal("a", "critical", "x"),
        FuzzerSignal("b", "warning", "y"),
    ]
    assert len(_dedupe_signals(signals)) == 2


def test_parse_claude_response_plain_json():
    assert _parse_claude_response('{"overall_status": "normal"}')["overall_status"] == "normal"


def test_parse_claude_response_stream_json():
    stream = "\n".join([
        '{"type": "progress", "data": "thinking"}',
        '{"type": "result", "result": "{\\"overall_status\\": \\"warning\\"}"}',
    ])
    assert _parse_claude_response(stream)["overall_status"] == "warning"


def test_parse_claude_response_requires_overall_status():
    """Progress events without overall_status must not be returned as the verdict."""
    with pytest.raises(ValueError):
        _parse_claude_response('{"type": "progress", "data": "x"}')


def test_parse_claude_response_rejects_garbage():
    with pytest.raises(ValueError):
        _parse_claude_response("no json here at all")


def test_format_source_note_when_clones_succeed():
    from scripts.fuzzer.analyzer import _format_source_note
    ctx = _ctx(tested_valkey_sha="deadbeef")
    note = _format_source_note(ctx, valkey_ok=True, fuzzer_ok=True)
    assert "valkey/" in note and "deadbeef" in note
    assert "valkey-fuzzer/" in note
    assert "NOT AVAILABLE" not in note


def test_format_source_note_when_clones_fail():
    """A failed clone must be called out so Claude doesn't cite line numbers."""
    from scripts.fuzzer.analyzer import _format_source_note
    note = _format_source_note(
        _ctx(tested_valkey_sha="deadbeef"),
        valkey_ok=False,
        fuzzer_ok=False,
    )
    assert "NOT AVAILABLE" in note
    assert "clone failed" in note
    assert "Do not cite source line numbers" in note


def test_format_source_note_when_sha_unrecorded():
    """Missing SHA must produce a different note than a failed clone — we
    never want to silently fall back to the default branch.
    """
    from scripts.fuzzer.analyzer import _format_source_note
    note = _format_source_note(_ctx(), valkey_ok=False, fuzzer_ok=True)
    assert "NOT AVAILABLE" in note
    assert "run report did not record the tested commit" in note
    assert "Do not cite source line numbers" in note


def test_invoke_claude_skips_valkey_clone_when_sha_missing(monkeypatch, tmp_path):
    """If the report didn't record valkey_sha, the analyzer must NOT clone
    the default branch — that would have Claude triage a different tree.
    """
    from scripts.fuzzer import analyzer as analyzer_mod

    clone_calls: list[tuple] = []

    def fake_clone(repo, dest, sha=None):
        clone_calls.append((repo, sha))
        return True

    fake_result = MagicMock(returncode=0, stdout='{"overall_status": "normal"}', stderr="")
    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", fake_clone)
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda *a, **kw: fake_result)

    ctx = _ctx(head_sha="abc123")  # tested_valkey_sha left as None
    analyzer_mod._invoke_claude(ctx, [], tmp_path)

    cloned_repos = [r for r, _ in clone_calls]
    assert "valkey-io/valkey" not in cloned_repos
    assert "r" in cloned_repos  # fuzzer repo (the test ctx repo) still cloned


def test_build_error_analysis_has_distinct_fingerprint_per_reason():
    """Different infra-failure reasons must NOT collide on a single issue."""
    from scripts.fuzzer.analyzer import _build_error_analysis
    a = _build_error_analysis(_BASE, "no fuzzer artifact bundle found")
    b = _build_error_analysis(_BASE, "fuzzer artifact bundle was empty or unreadable")
    assert a.incident_fingerprint
    assert b.incident_fingerprint
    assert a.incident_fingerprint != b.incident_fingerprint


def test_build_error_analysis_fingerprint_is_stable():
    """Same context + reason produces the same fingerprint across calls."""
    from scripts.fuzzer.analyzer import _build_error_analysis
    a = _build_error_analysis(_BASE, "no fuzzer artifact bundle found")
    b = _build_error_analysis({**_BASE, "run_id": 2}, "no fuzzer artifact bundle found")
    assert a.incident_fingerprint == b.incident_fingerprint
    assert a.analyzer_incomplete and a.triage_verdict == "needs-human-triage" and a.kind == "batch"


def test_invoke_claude_retries_once_then_succeeds(monkeypatch, tmp_path):
    """A transient first-attempt failure is retried; the second success wins."""
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    results = [
        MagicMock(returncode=1, stdout="", stderr="timeout after 1800s"),
        MagicMock(returncode=0, stdout='{"overall_status": "normal"}', stderr=""),
    ]
    calls = {"n": 0}

    def fake_run_agent(*a, **kw):
        out = results[calls["n"]]
        calls["n"] += 1
        return out

    monkeypatch.setattr(analyzer_mod, "run_agent", fake_run_agent)
    payload = analyzer_mod._invoke_claude(_ctx(head_sha="abc"), [], tmp_path)
    assert payload["overall_status"] == "normal"
    assert calls["n"] == 2


def test_invoke_claude_does_not_retry_on_parse_failure(monkeypatch, tmp_path):
    """A successful exit with unparseable output is deterministic, so it is
    raised immediately rather than burning a second Bedrock attempt."""
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    calls = {"n": 0}

    def fake_run_agent(*a, **kw):
        calls["n"] += 1
        return MagicMock(returncode=0, stdout="not json", stderr="")

    monkeypatch.setattr(analyzer_mod, "run_agent", fake_run_agent)
    with pytest.raises(ValueError):
        analyzer_mod._invoke_claude(_ctx(head_sha="abc"), [], tmp_path)
    assert calls["n"] == 1


def test_invoke_claude_raises_after_retry_exhausted(monkeypatch, tmp_path):
    """Both attempts failing surfaces the last error for the caller to handle."""
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    monkeypatch.setattr(
        analyzer_mod, "run_agent",
        lambda *a, **kw: MagicMock(returncode=1, stdout="", stderr="timeout after 1800s"),
    )
    with pytest.raises(RuntimeError, match="timeout after 1800s"):
        analyzer_mod._invoke_claude(_ctx(head_sha="abc"), [], tmp_path)


def test_invoke_claude_guards_against_zero_attempts(monkeypatch, tmp_path):
    """A misconfigured attempt count fails loud, never `raise None`."""
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda *a, **kw: None)
    monkeypatch.setattr(analyzer_mod, "_CLAUDE_MAX_ATTEMPTS", 0)
    with pytest.raises(RuntimeError, match=">= 1"):
        analyzer_mod._invoke_claude(_ctx(head_sha="abc"), [], tmp_path)




# --- Batches -----------------------------------------------------------------
# tests/fixtures/fuzzer/batch is a real batch folder; see tests/fuzzer_batch_fixture.py.

CRASH_KEY = "cc08b3931ccc"


def _claude_down(*_a, **_kw):
    raise RuntimeError("Claude Code failed (rc=1): timeout")


def _analyze(files: dict[str, bytes], monkeypatch, claude=_claude_down, *, run_id: int = 1):
    from scripts.common.workflow_artifacts import WorkflowArtifact
    from scripts.fuzzer import analyzer as analyzer_mod

    client = MagicMock()
    client.list_run_artifacts.return_value = [WorkflowArtifact(9, f"fuzzer-run-artifacts-{run_id}", 1, False)]
    client.download_artifact.return_value = files
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow_run.return_value = MagicMock(
        html_url=f"https://x/runs/{run_id}", conclusion="failure", head_sha="abc",
    )
    monkeypatch.setattr(analyzer_mod, "_invoke_claude", claude)
    analyzer = analyzer_mod.FuzzerRunAnalyzer(gh, github_token="t", artifact_client=client)
    return analyzer.analyze("valkey-io/valkey-fuzzer", run_id, workflow_file="fuzzer-run.yml")


def _by_kind(analysis, kind: str) -> list:
    return [f for f in analysis.findings if f.kind == kind]


def _signature(analysis, key: str = CRASH_KEY):
    return next(f for f in analysis.findings if f.signature_key == key)


def test_real_batch_files_one_finding_per_failure_signature(monkeypatch):
    from scripts.fuzzer.main import _should_publish

    analysis = _analyze(batch_files(), monkeypatch)
    assert (analysis.batch_status, analysis.counts) == ("FAIL", {"PASS": 2, "FAIL": 2, "ERROR": 2})
    assert [f.kind for f in analysis.findings] == ["signature"] * 3
    assert analysis.overall_status == "anomalous"

    crash = _signature(analysis)
    assert crash.batch_runs == [CRASH, "scenario-crash-778"]
    assert (crash.overall_status, crash.triage_verdict) == ("anomalous", "likely-core-valkey-bug")
    assert crash.suggested_labels == ["possible-valkey-bug"]
    assert (crash.rerun_verdict, crash.rerun_detail) == ("reproduced", "1/1")
    assert (crash.run_status, crash.experiment_id, crash.scenario_id, crash.seed, crash.tested_valkey_sha) == (
        "FAIL", "ciagent.debug-crash", "ciagent-crash-777", "777", "dff4c58e",
    )
    assert crash.signature_label.startswith("FAIL | ciagent.debug-crash | liveness:FAIL")
    assert crash.reproduction_hint == (
        "valkey-fuzzer replay batch/runs/scenario-crash-777/report.json --valkey-binary /path/to/valkey-server"
    )
    crashes = sorted(s.evidence for s in crash.anomalies if s.title == "Node crash or assertion")
    assert crashes == ["777-3088e5fa-valkey-0.log: BUG REPORT START", "component liveness: BUG REPORT START"]
    titles = {s.title: s.evidence for s in crash.anomalies}
    assert titles["Reproduced in serial re-runs"] == "1/1 serial re-runs failed with the same signature"
    assert "The AI analysis failed" in crash.summary

    noboot = _signature(analysis, "846478c93288")
    assert noboot.triage_verdict == "needs-human-triage" and noboot.suggested_labels == []
    error = next(s.evidence for s in noboot.anomalies if s.title == "Run error")
    assert error.startswith("runner execution failed during deployment.provision (")

    unknown = _signature(analysis, "9ab128c1af68")
    assert unknown.triage_verdict == "needs-human-triage"
    assert unknown.anomalies[0].title == "Run report unreadable"
    assert unknown.anomalies[0].evidence.startswith("run scenario-unknown: the run left no run report")
    assert unknown.reproduction_hint is None
    assert all(_should_publish(f) for f in analysis.findings)


def test_passing_runs_of_the_real_batch_scan_clean():
    batch = read_batch(batch_files(), "batch/")
    passed = [r for r in batch.main_runs if r.status == "PASS"]
    assert len(passed) == 2
    for run in passed:
        assert _scan_logs(_context_for(_BASE, run, "batch/")) == []


def _with_third_crash_run(files: dict[str, bytes]) -> dict[str, bytes]:
    """The crash seen in a third run: a copy of scenario-crash-778 as scenario-crash-779."""
    files = dict(files)
    for path, payload in list(files.items()):
        if path.startswith("batch/runs/scenario-crash-778/"):
            files[path.replace("scenario-crash-778", "scenario-crash-779")] = payload
    summary = batch_summary()
    entry = dict(next(r for r in summary["runs"] if r["id"] == "scenario-crash-778"))
    entry.update(id="scenario-crash-779", report="runs/scenario-crash-779/report.json")
    summary["runs"].append(entry)
    group = next(g for g in summary["signatures"] if g["key"] == CRASH_KEY)
    group["runs"].append("scenario-crash-779")
    group["count"] = 3
    return with_summary(files, summary)


def test_one_bug_in_three_runs_is_one_issue_across_batches(monkeypatch):
    first = _analyze(_with_third_crash_run(batch_files()), monkeypatch, run_id=1)
    crash = [f for f in first.findings if f.signature_key == CRASH_KEY]
    assert len(crash) == 1
    assert crash[0].batch_runs == [CRASH, "scenario-crash-778", "scenario-crash-779"]
    fingerprints = [f.incident_fingerprint for f in first.findings]
    assert len(set(fingerprints)) == len(fingerprints) == 3

    # The next scheduled batch sees the same signature: the same issue.
    later = _analyze(batch_files(), monkeypatch, run_id=2)
    assert _signature(later).incident_fingerprint == crash[0].incident_fingerprint


def test_claude_gets_the_signature_runs_and_the_batch_summary(monkeypatch, tmp_path):
    from scripts.fuzzer import analyzer as analyzer_mod

    calls: list[dict] = []

    def fake_run_agent(profile, prompt, cwd):
        art = Path(cwd) / "_artifacts"
        calls.append({
            "profile": profile, "prompt": prompt,
            "files": sorted(str(p.relative_to(art)) for p in art.rglob("*") if p.is_file()),
            "summary": (art / "summary.json").read_text(),
        })
        return MagicMock(returncode=0, stdout=json.dumps({
            "overall_status": "anomalous", "triage_verdict": "likely-core-valkey-bug",
            "root_cause_category": "debug-segfault", "summary": "The primary crashed in DEBUG.",
        }), stderr="")

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    monkeypatch.setattr(analyzer_mod, "run_agent", fake_run_agent)
    files = batch_files()
    analysis = _analyze(files, monkeypatch, claude=analyzer_mod._invoke_claude)

    assert len(calls) == 3  # one per signature, none for the passing runs
    crash_call = next(c for c in calls if CRASH_KEY in c["prompt"])
    assert crash_call["profile"] == "fuzzer_analysis_readonly"
    assert crash_call["summary"] == files["batch/summary.json"].decode()
    assert crash_call["files"] == [
        "runs/scenario-crash-777/logs/777-3088e5fa-valkey-0.log",
        "runs/scenario-crash-777/logs/777-3088e5fa-valkey-0.stdio.log",
        "runs/scenario-crash-777/report.json", "runs/scenario-crash-777/scenario.yaml",
        "runs/scenario-crash-778/logs/778-b869c274-valkey-0.log",
        "runs/scenario-crash-778/logs/778-b869c274-valkey-0.stdio.log",
        "runs/scenario-crash-778/report.json", "runs/scenario-crash-778/scenario.yaml",
        "summary.json",
    ]
    prompt = crash_call["prompt"]
    assert f"2 run(s) failed with one failure signature, `{CRASH_KEY}`" in prompt
    assert "Serial re-runs: reproduced (1/1 reproduced the signature)." in prompt
    assert "Start with _artifacts/runs/scenario-crash-777/: fuzzer outcome FAIL" in prompt
    assert "<seed>-<deployment id>-valkey-N.log" in prompt
    assert "Component liveness failed" in prompt
    unknown_call = next(c for c in calls if "9ab128c1af68" in c["prompt"])
    assert "runs/scenario-unknown/error.json" in unknown_call["files"]

    crash = _signature(analysis)
    assert crash.summary == "The primary crashed in DEBUG."
    assert crash.root_cause_category == "debug-segfault"
    # Claude cannot clear a run whose report is missing.
    assert _signature(analysis, "9ab128c1af68").triage_verdict == "needs-human-triage"


def _with_reruns(verdict: str, reproduced: int, ran: int = 2, other: str = "PASS") -> dict[str, bytes]:
    """The crash signature's serial re-runs: ``reproduced`` of ``ran`` the same, the rest ``other``."""
    summary = batch_summary()
    group = next(g for g in summary["signatures"] if g["key"] == CRASH_KEY)
    group["serial_reruns"] = {
        "runs": [
            {"id": f"serial-{i}", "status": "FAIL" if i < reproduced else other, "same_signature": i < reproduced,
             "signature_key": None if i < reproduced or other == "PASS" else "0123abcd",
             "signature_label": None if i < reproduced or other == "PASS" else f"{other} | timeout during run"}
            for i in range(ran)
        ],
        "reproduced": reproduced, "verdict": verdict,
    }
    return with_summary(batch_files(), summary)


def _claude_says(verdict: str):
    return lambda *_a, **_kw: {"overall_status": "anomalous", "triage_verdict": verdict, "summary": "s"}


def test_a_failure_not_reproduced_serially_points_at_flakiness(monkeypatch):
    crash = _signature(_analyze(_with_reruns("not reproduced", 0), monkeypatch,
                                claude=_claude_says("likely-core-valkey-bug")))
    assert crash.triage_verdict == "possible-core-valkey-bug"
    assert (crash.rerun_verdict, crash.rerun_detail) == ("not reproduced", "0/2")
    signal = next(s for s in crash.anomalies if s.title == "Not reproduced in serial re-runs")
    assert "flaky or caused by load" in signal.evidence


def test_a_failure_reproduced_serially_raises_confidence(monkeypatch):
    crash = _signature(_analyze(batch_files(), monkeypatch, claude=_claude_says("possible-core-valkey-bug")))
    assert crash.triage_verdict == "likely-core-valkey-bug"


def test_a_failure_that_failed_differently_serially_keeps_its_verdict(monkeypatch):
    # No re-run failed the same way, but they did not pass either: not a sign of load.
    crash = _signature(_analyze(_with_reruns("failed differently", 0, other="ERROR"), monkeypatch,
                                claude=_claude_says("likely-core-valkey-bug")))
    assert crash.triage_verdict == "likely-core-valkey-bug"
    assert (crash.rerun_verdict, crash.rerun_detail) == ("failed differently", "0/2")
    signal = next(s for s in crash.anomalies if s.title == "Failed differently in serial re-runs")
    assert "ERROR | timeout during run" in signal.evidence


def test_an_intermittent_failure_keeps_its_verdict(monkeypatch):
    crash = _signature(_analyze(_with_reruns("intermittent", 1), monkeypatch,
                                claude=_claude_says("possible-core-valkey-bug")))
    assert crash.triage_verdict == "possible-core-valkey-bug"
    assert any(s.title == "Intermittent in serial re-runs" for s in crash.anomalies)


@pytest.mark.parametrize("verdict,rerun,status,expected", [
    ("possible-core-valkey-bug", "reproduced", "FAIL", "likely-core-valkey-bug"),
    ("possible-core-valkey-bug", "reproduced", "ERROR", "possible-core-valkey-bug"),
    ("likely-core-valkey-bug", "not reproduced", "FAIL", "possible-core-valkey-bug"),
    ("needs-human-triage", "reproduced", "FAIL", "needs-human-triage"),
    ("needs-human-triage", "not reproduced", "FAIL", "needs-human-triage"),
    ("expected-chaos-noise", "not reproduced", "FAIL", "expected-chaos-noise"),
    ("likely-core-valkey-bug", None, "FAIL", "likely-core-valkey-bug"),
])
def test_rerun_adjustment(verdict, rerun, status, expected):
    assert _rerun_adjusted(verdict, rerun, status) == expected


# The shape of a real zoo summary (valkey-fuzzer batch --zoo), made not to hold.
_ZOO = {
    "valkey_commit": "f2ca837edc75e2238f880d89ade60babb6e6ed9e",
    "cases": [
        {
            "case": "replica-skips-writes",
            "catches": "valkey.oracle.ledger replica convergence (check_replicas)",
            "min_detection_rate": 1.0,
            "controls": [{"file": "scenario.yaml", "seed": 1301, "status": "FAIL",
                          "report": "runs/zoo-replica-skips-writes-scenario-control/report.json"}],
            "scenarios": {"scenario.yaml": {
                "expected": {"fail": ["data"], "missing": []},
                "runs": [
                    {"file": "scenario.yaml", "seed": 1301, "status": "PASS", "failed": [], "missing": [],
                     "detected": False, "report": "runs/zoo-replica-skips-writes-scenario-0/report.json"},
                    {"file": "scenario.yaml", "seed": 1302, "status": "FAIL", "failed": ["data"],
                     "missing": [], "detected": True,
                     "report": "runs/zoo-replica-skips-writes-scenario-1/report.json"},
                ],
                "detection_rate": 0.5,
            }},
            "holds": False,
        },
        {"case": "eviction-drops-acknowledged-writes", "catches": "x", "min_detection_rate": 1.0,
         "controls": [], "scenarios": {}, "holds": True},
    ],
    "skipped": [],
    "held": 1,
    "not_held": ["replica-skips-writes"],
}


def test_a_zoo_case_that_does_not_hold_is_a_fuzzer_issue_never_a_valkey_bug(monkeypatch):
    summary = batch_summary()
    summary["zoo"] = _ZOO
    # A zoo run fails on purpose (a broken build); it is never a finding of its own.
    summary["runs"].append({**summary["runs"][2], "id": "zoo-replica-skips-writes-scenario-1", "source": "zoo",
                            "report": None, "signature_key": "c06a88105599"})
    calls = []
    analysis = _analyze(with_summary(batch_files(), summary), monkeypatch,
                        claude=lambda *a, **kw: calls.append(1) or {})
    zoo = _by_kind(analysis, "zoo")
    assert len(zoo) == 1
    finding = zoo[0]
    assert finding.title == "[fuzzer-zoo] Bug zoo case replica-skips-writes no longer caught"
    assert (finding.overall_status, finding.triage_verdict) == ("anomalous", "needs-human-triage")
    assert finding.fuzzer_side and finding.suggested_labels == []
    assert "not a Valkey bug" in finding.summary
    assert [s.title for s in finding.anomalies] == ["Zoo control did not pass", "Zoo bug missed"]
    assert "caught 50% of runs on the broken build, needs 100%" in finding.anomalies[1].evidence
    assert finding.batch_runs == ["zoo-replica-skips-writes-scenario-0", "zoo-replica-skips-writes-scenario-1"]
    assert not any("zoo-" in run for f in _by_kind(analysis, "signature") for run in f.batch_runs)
    assert len(calls) == 3  # the three signatures; the zoo verdict is the fuzzer's own


def test_a_zoo_case_without_a_verdict_in_a_finished_batch_fails_closed(monkeypatch):
    summary = batch_summary()
    summary["zoo"] = {**_ZOO, "cases": [{"case": "replica-skips-writes", "holds": None, "pending": True}]}
    finding = _by_kind(_analyze(with_summary(batch_files(), summary), monkeypatch), "zoo")[0]
    assert finding.anomalies[0].title == "Zoo verdict missing"
    assert finding.triage_verdict == "needs-human-triage"


def test_leaks_are_fuzzer_bugs_and_replace_the_leak_only_signature(monkeypatch):
    summary = batch_summary()
    run = summary["runs"][0]
    run.update(status="ERROR", leaks=1, signature_key="leak0000")
    summary["signatures"].append({
        "key": "leak0000", "label": "ERROR | valkey.aof-durability | leaked server", "status": "ERROR",
        "experiment": "valkey.aof-durability", "signature": {}, "runs": [run["id"]], "count": 1,
    })
    summary["leaks"] = [{"run": run["id"], "kind": "server", "target": "41234", "owner": "123-abc",
                         "age_s": 2.0, "detail": "valkey-server *:23001", "action": "removed", "reason": ""}]
    analysis = _analyze(with_summary(batch_files(), summary), monkeypatch)
    assert "leak0000" not in {f.signature_key for f in analysis.findings}
    leaks = _by_kind(analysis, "leak")
    assert len(leaks) == 1
    leak = leaks[0]
    assert leak.title == "[fuzzer-leak] valkey.aof-durability runs left a server behind"
    assert leak.fuzzer_side and leak.triage_verdict == "needs-human-triage" and leak.suggested_labels == []
    assert leak.batch_runs == [run["id"]]
    assert leak.anomalies[0].evidence == f"run {run['id']}: 41234 (valkey-server *:23001); removed"
    assert "not a Valkey bug" in leak.summary


def test_runs_skipped_as_unsupported_are_not_findings(monkeypatch):
    summary = batch_summary()
    run = summary["runs"][0]
    run.update(status="SKIP_UNSUPPORTED", signature_key="skip0000")
    summary["signatures"].append({
        "key": "skip0000", "label": "SKIP_UNSUPPORTED | valkey.aof-durability", "status": "SKIP_UNSUPPORTED",
        "experiment": "valkey.aof-durability", "signature": {}, "runs": [run["id"]], "count": 1,
    })
    analysis = _analyze(with_summary(batch_files(), summary), monkeypatch)
    assert "skip0000" not in {f.signature_key for f in analysis.findings}
    assert len(_by_kind(analysis, "signature")) == 3
    assert not _by_kind(analysis, "batch")


def _all_skipped(complete: bool = True) -> dict:
    summary = batch_summary()
    for run in summary["runs"]:
        run.update(status="SKIP_UNSUPPORTED", report=None, summary="needs a Linux host", signature_key=None)
    summary.update(signatures=[], counts={"SKIP_UNSUPPORTED": len(summary["runs"])}, status="SKIP_UNSUPPORTED",
                   complete=complete, finished=len(summary["runs"]) if complete else 4)
    return summary


def test_a_batch_where_every_run_was_skipped_is_one_finding_for_a_human(monkeypatch):
    from scripts.fuzzer.main import _should_publish

    analysis = _analyze(with_summary(batch_files(), _all_skipped()), monkeypatch, run_id=1)
    assert [f.kind for f in analysis.findings] == ["batch"]
    finding = analysis.findings[0]
    assert finding.title == "[fuzzer-run] Batch tested nothing"
    assert (finding.overall_status, finding.triage_verdict) == ("warning", "needs-human-triage")
    assert "Every one of the batch's 6 run(s) was skipped as unsupported" in finding.summary
    assert finding.anomalies[0].evidence == "needs a Linux host"
    assert len(finding.batch_runs) == 6 and not any(r.startswith("serial-") for r in finding.batch_runs)
    assert _should_publish(finding)
    # The same host or build problem in the next batch updates the same issue.
    later = _analyze(with_summary(batch_files(), _all_skipped()), monkeypatch, run_id=2)
    assert later.findings[0].incident_fingerprint == finding.incident_fingerprint


def test_a_batch_that_ran_nothing_is_the_same_finding(monkeypatch):
    summary = _all_skipped()
    summary.update(runs=[], jobs=0, finished=0, counts={})
    analysis = _analyze(with_summary(batch_files(), summary), monkeypatch)
    assert [f.title for f in analysis.findings] == ["[fuzzer-run] Batch tested nothing"]
    assert "without running any job" in analysis.findings[0].summary


def test_an_unfinished_batch_of_skipped_runs_is_only_the_unfinished_finding(monkeypatch):
    analysis = _analyze(with_summary(batch_files(), _all_skipped(complete=False)), monkeypatch)
    assert [f.title for f in analysis.findings] == ["[fuzzer-run] Batch did not finish"]


def _passing_run_logging(line: str, *, fault: str | None = None, server_config: dict | None = None):
    """The passing aof-durability run (it kills its primary) logging ``line``, with a fault or config added."""
    batch = read_batch(batch_files(), "batch/")
    run = next(r for r in batch.main_runs if r.status == "PASS")
    context = _context_for(_BASE, run, "batch/")
    context.node_logs = {"valkey-0.log": line}
    plan = context.report["outcome"]["plan"]
    if fault:
        plan["components"].append({"use": fault, "id": "fault", "role": "fault", "config": {}})
    if server_config:
        plan["deployment"]["server_config"].update(server_config)
    return context


@pytest.mark.parametrize("line", ["Failover attempt expired.", "slots still assigned to killed nodes"])
def test_a_passing_run_does_not_report_what_its_chaos_causes(line):
    assert _scan_logs(_passing_run_logging(line)) == []


@pytest.mark.parametrize("line,title", [
    ("# Background saving error", "RDB/AOF failure"), ("Out Of Memory allocating 10 bytes", "OOM"),
])
def test_a_passing_run_reports_a_chaos_line_its_scenario_does_not_cause_as_a_warning(line, title):
    # The run kills its primary; it fills no disk and sets no memory limit of its own.
    assert [(s.title, s.severity) for s in _scan_logs(_passing_run_logging(line))] == [(title, "warning")]


def test_a_passing_run_with_a_full_disk_does_not_report_a_failed_save():
    assert _scan_logs(_passing_run_logging("# Background saving error", fault="valkey.fault.disk-fill")) == []


def test_a_passing_run_with_its_own_memory_limit_does_not_report_an_oom_refusal():
    context = _passing_run_logging("OOM command not allowed when used memory > 'maxmemory'",
                                   server_config={"maxmemory": "4mb", "maxmemory-policy": "allkeys-random"})
    assert _scan_logs(context) == []


def test_the_fuzzer_default_maxmemory_does_not_explain_an_oom_line():
    context = _passing_run_logging("OOM command not allowed when used memory > 'maxmemory'")
    assert context.report["outcome"]["plan"]["instances"]["valkey-0"]["server_config"]["maxmemory"] == "500mb"
    assert [s.title for s in _scan_logs(context)] == ["OOM"]


def test_a_passing_run_without_failover_chaos_reports_an_expired_failover():
    context = _passing_run_logging("Failover attempt expired.")
    plan = context.report["outcome"]["plan"]
    plan["components"] = [c for c in plan["components"] if c["use"] != "valkey.fault.process-signal"]
    assert [(s.title, s.severity) for s in _scan_logs(context)] == [("Failover timeout", "warning")]


def test_a_passing_run_still_reports_a_crash():
    signals = _scan_logs(_passing_run_logging("=== VALKEY BUG REPORT START: Cut & paste starting from here ==="))
    assert [s.title for s in signals] == ["Node crash or assertion"]


def test_a_failed_run_still_reports_what_chaos_may_cause():
    ctx = _ctx(report={"status": "FAIL", "summary": "lost writes",
                       "outcome": {"status": "FAIL", "component_results": []}},
               node_logs={"valkey-0.log": "Failover attempt expired."})
    assert [s.title for s in _scan_logs(ctx)] == ["Run failed", "Failover timeout"]


def test_a_missing_summary_is_one_finding_for_a_human(monkeypatch):
    files = batch_files()
    del files["batch/summary.json"]
    analysis = _analyze(files, monkeypatch)
    assert len(analysis.findings) == 1
    finding = analysis.findings[0]
    assert (finding.kind, finding.triage_verdict, finding.analyzer_incomplete) == (
        "batch", "needs-human-triage", True,
    )
    assert "the artifact has no batch/summary.json" in finding.summary
    assert analysis.batch_status is None


def test_the_old_single_report_artifact_is_not_read(monkeypatch):
    """A results.json bundle, the format before batches, is not a batch."""
    files = {"results.json": (BATCH / "runs" / CRASH / "report.json").read_bytes()}
    finding = _analyze(files, monkeypatch).findings[0]
    assert finding.triage_verdict == "needs-human-triage" and finding.analyzer_incomplete


def test_an_unfinished_batch_is_its_own_finding(monkeypatch):
    summary = batch_summary()
    summary.update(complete=False, exit_code=130, finished=4)
    analysis = _analyze(with_summary(batch_files(), summary), monkeypatch)
    batch = _by_kind(analysis, "batch")
    assert len(batch) == 1 and batch[0].title == "[fuzzer-run] Batch did not finish"
    assert batch[0].triage_verdict == "needs-human-triage"
    assert "4 of 6 jobs ran" in batch[0].summary
    assert len(_by_kind(analysis, "signature")) == 3


def test_an_unreadable_report_sends_its_signature_to_a_human(monkeypatch):
    files = with_report(batch_files(), "runs/scenario-crash-778/report.json", "{not json")
    crash = _signature(_analyze(files, monkeypatch, claude=_claude_says("likely-core-valkey-bug")))
    assert crash.triage_verdict == "needs-human-triage"
    assert crash.overall_status == "anomalous"
    unreadable = [s.evidence for s in crash.anomalies if s.title == "Run report unreadable"]
    assert unreadable == ["run scenario-crash-778: runs/scenario-crash-778/report.json is not valid JSON: "
                          "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"]
    # The readable run still names the finding.
    assert crash.scenario_id == "ciagent-crash-777"


def test_a_passing_run_with_a_crash_in_its_logs_is_a_finding(monkeypatch):
    files = dict(batch_files())
    log = "batch/runs/random-valkey.aof-durability-s11/logs/11-c04a59ba-valkey-1.log"
    files[log] = files[log] + b"\n=== VALKEY BUG REPORT START: Cut & paste starting from here ===\n"
    analysis = _analyze(files, monkeypatch)
    runs = _by_kind(analysis, "run")
    assert len(runs) == 1
    finding = runs[0]
    assert finding.batch_runs == ["random-valkey.aof-durability-s11"]
    assert (finding.run_status, finding.triage_verdict) == ("PASS", "likely-core-valkey-bug")
    assert finding.anomalies[0].evidence == "11-c04a59ba-valkey-1.log: BUG REPORT START"


def test_a_passing_run_whose_report_is_unreadable_goes_to_a_human(monkeypatch):
    files = with_report(batch_files(), "runs/random-valkey.aof-durability-s12/report.json", "[]")
    finding = _by_kind(_analyze(files, monkeypatch, claude=_claude_says("expected-chaos-noise")), "run")[0]
    assert finding.triage_verdict == "needs-human-triage"
    assert finding.overall_status != "normal"
    assert finding.anomalies[0].title == "Run report unreadable"


def test_claude_analyzes_a_bounded_number_of_findings(monkeypatch):
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "_MAX_CLAUDE_FINDINGS", 1)
    calls = []
    analysis = _analyze(batch_files(), monkeypatch,
                        claude=lambda *a, **kw: calls.append(1) or {"overall_status": "anomalous",
                                                                     "triage_verdict": "needs-human-triage"})
    assert len(calls) == 1
    assert len(analysis.findings) == 3
    assert "The AI analysis failed" not in "".join(f.summary for f in analysis.findings)


@pytest.mark.parametrize("claude", [
    {"overall_status": "normal", "triage_verdict": "expected-chaos-noise"},
    {"overall_status": "anomalous", "triage_verdict": "likely-core-valkey-bug"},
])
def test_claude_cannot_move_a_finding_with_an_unreadable_report_off_a_human(claude):
    status, verdict = _final_verdict(claude, [], unreadable=True, run_status="FAIL")
    assert verdict == "needs-human-triage" and status != "normal"


def test_claude_cannot_call_a_failed_run_normal():
    status, verdict = _final_verdict(
        {"overall_status": "normal", "triage_verdict": "expected-chaos-noise"}, [],
        unreadable=False, run_status="FAIL",
    )
    assert (status, verdict) == ("warning", "expected-chaos-noise")


def test_claude_values_outside_the_contract_are_replaced():
    assert _final_verdict({"overall_status": "fine", "triage_verdict": "ship it"}, [],
                          unreadable=False, run_status="PASS") == ("warning", "needs-human-triage")
    assert _final_verdict({"overall_status": "normal", "triage_verdict": "expected-chaos-noise"}, [],
                          unreadable=False, run_status="PASS") == ("normal", "expected-chaos-noise")


def test_skipped_run_is_not_normal():
    ctx = _ctx(report={"status": "SKIP_UNSUPPORTED", "summary": "needs linux",
                       "outcome": {"status": "SKIP_UNSUPPORTED", "component_results": []}})
    assert _triage(_scan_logs(ctx)) == ("warning", "needs-human-triage")


def test_failed_optional_component_in_a_passing_run_is_a_warning():
    batch = read_batch(batch_files(), "batch/")
    run = batch.run("random-valkey.aof-durability-s11")
    run.report["outcome"]["component_results"][0].update(status="FAIL", summary="optional check failed")
    signals = _scan_logs(_context_for(_BASE, run, "batch/"))
    component = run.report["outcome"]["component_results"][0]["component_id"]
    assert [(s.title, s.severity) for s in signals] == [(f"Component {component} failed", "warning")]
    assert _triage(signals)[0] == "warning"


def test_a_pr_batch_is_read_at_the_artifact_root_and_names_the_change(monkeypatch):
    from scripts.common.workflow_artifacts import WorkflowArtifact
    from scripts.fuzzer import analyzer as analyzer_mod

    client = MagicMock()
    # The PR batch uploads its folder at the artifact's root, named for the run.
    client.list_run_artifacts.return_value = [WorkflowArtifact(8, "other", 1, False),
                                              WorkflowArtifact(9, "fuzzer-batch-5", 1, False)]
    client.download_artifact.return_value = batch_files(prefix="")
    github = MagicMock()
    github.get_repo.return_value.get_workflow_run.return_value = MagicMock(
        html_url="https://x/runs/5", conclusion="failure", head_sha="abc",
    )
    monkeypatch.setattr(analyzer_mod, "_invoke_claude", _claude_down)
    analyzer = analyzer_mod.FuzzerRunAnalyzer(github, github_token="t", artifact_client=client)
    analysis = analyzer.analyze(
        "owner/valkey-fuzzer", 5, workflow_file="fuzzer-pr-batch.yml", artifact_name="fuzzer-batch-5",
        change="valkey-io/valkey#4716, merged as e082696cf000",
    )

    client.download_artifact.assert_called_once_with("owner/valkey-fuzzer", 9)
    assert analysis.counts == {"PASS": 2, "FAIL": 2, "ERROR": 2}
    assert {f.change for f in analysis.findings} == {"valkey-io/valkey#4716, merged as e082696cf000"}
    assert _signature(analysis).reproduction_hint.startswith("valkey-fuzzer replay runs/scenario-crash-777/")


def _cause_issue(number: int, marked: bool = True):
    body = "<!-- valkey-ci-agent:fuzzer-issue:abc -->\n## Fuzzer Run Analysis\nForkless save ignores lz4" if marked else "x"
    issue = MagicMock(number=number, title=f"issue {number}")
    issue.body = body
    return issue


def _cause_finding():
    from scripts.fuzzer.models import FuzzerFinding
    return FuzzerFinding(**_BASE, kind="signature", overall_status="anomalous",
                         triage_verdict="likely-core-valkey-bug", summary="The forkless save wrote a plain RDB",
                         experiment_id="valkey.forkless-save-crash")


@pytest.mark.parametrize(("stdout", "rc", "expected"), [
    ('{"same_as": 13, "reason": "one fix"}', 0, 13),
    ('{"same_as": null, "reason": "different"}', 0, None),
    ('{"same_as": 7, "reason": "not a fuzzer issue"}', 0, None),
    ('{"same_as": true}', 0, None),
    ("no answer", 0, None),
    ('{"same_as": 13}', 1, None),
])
def test_same_root_cause_counts_only_an_open_fuzzer_issue(monkeypatch, stdout, rc, expected):
    from scripts.fuzzer import analyzer as analyzer_mod
    calls = []
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda profile, prompt, cwd: calls.append((profile, prompt))
                        or MagicMock(returncode=rc, stdout=stdout))
    issues = [_cause_issue(13), _cause_issue(7, marked=False)]

    number = analyzer_mod.same_root_cause(_cause_finding(), "Forkless crash", issues,
                                          marker_namespace="valkey-ci-agent:fuzzer-issue")

    assert number == expected
    ((profile, prompt),) = calls
    # A question with everything in the prompt: no tools, few turns, a short timeout.
    assert profile == "fuzzer_root_cause_text_only"
    assert "#13: issue 13" in prompt and "#7:" not in prompt
    assert "Title: Forkless crash" in prompt and "<!--" not in prompt


@pytest.mark.parametrize("kind,fuzzer_side", [("zoo", True), ("leak", True), ("batch", False), ("run", True)])
def test_same_root_cause_is_asked_only_for_a_signature_or_run_finding_that_is_not_the_fuzzers(
    monkeypatch, kind, fuzzer_side,
):
    from scripts.fuzzer import analyzer as analyzer_mod
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda *a, **k: pytest.fail("Claude was asked"))
    finding = _cause_finding()
    finding.kind, finding.fuzzer_side = kind, fuzzer_side
    assert analyzer_mod.same_root_cause(finding, "t", [_cause_issue(13)],
                                        marker_namespace="valkey-ci-agent:fuzzer-issue") is None


def test_same_root_cause_without_candidates_does_not_ask(monkeypatch):
    from scripts.fuzzer import analyzer as analyzer_mod
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda *a, **k: pytest.fail("Claude was asked"))
    assert analyzer_mod.same_root_cause(_cause_finding(), "t", [_cause_issue(7, marked=False)],
                                        marker_namespace="valkey-ci-agent:fuzzer-issue") is None


# --- Runs that test a change across two builds (valkey.action.commands) -------
# W2's report shape, built by hand on the fixture's crash run: a FAIL or an
# ERROR whose component compares a command sequence on the merge commit's build
# (candidate) and its parent's (parent).

LSET = "scenario-lset"
LSET_KEY = "15e7000001ab"
_EXPERIMENT_YAML = "id: proposed.lset\ncomponents:\n  - use: valkey.action.commands\n    id: lset\n"


def _replies(*replies):
    return [{"index": i, "reply": r} for i, r in enumerate(replies)]


def _commands_report(status: str, *, reason: str | None = None, commands=(), reached=(), unexplained=(),
                     exited: bool = False, experiment: str = "proposed.lset", parent: bool = True) -> dict:
    report = crash_report()
    summary = {
        "FAIL": "required components did not pass: lset",
        "ERROR": f"component lset errored: {reason}",
    }[status]
    report.update(status=status, summary=summary)
    report["scenario"]["experiment"]["id"] = experiment
    report["scenario"]["scenario_id"] = "lset-1"
    if parent:
        report["scenario"]["artifacts"] = {"parent": {"use": "valkey.artifact.local-binary",
                                                      "with": {"path": "/opt/valkey-parent/src/valkey-server"}}}
    plan = report["outcome"]["plan"]
    plan.update(experiment_id=experiment, scenario_id="lset-1")
    plan["components"] = [{
        "use": "valkey.action.commands", "id": "lset", "role": "action", "required": True, "depends_on": [],
        "config": {"compare_with": "/opt/valkey-parent/src/valkey-server", "commands": [
            {"command": ["RPUSH", "l", "a"]}, {"command": ["LSET", "l", 9223372036854775807, "x"],
                                                "expect_difference": True, "expect": {"error": "out of range"}},
            {"command": ["LRANGE", "l", 0, -1]},
        ]},
    }]
    if parent:
        plan["artifacts"] = [{"id": "parent", "use": "valkey.artifact.local-binary",
                              "config": {"path": "/opt/valkey-parent/src/valkey-server"}}]
    sequence = {
        "candidate": {"replies": _replies(1, {"error": "ERR index out of range"}, ["a"]), "digest": "c" * 64,
                      "exited": exited},
        "parent": {"replies": _replies(1, "OK", ["x"]), "digest": "p" * 64, "exited": False},
        "dropped_config": [], "reached": list(reached), "unexplained": list(unexplained),
    }
    component = {
        "component_id": "lset", "status": "ERROR" if reason else status, "summary": summary,
        "evidence": [{"evidence_id": "valkey.command.sequence", "source": "lset", "phase": "action.completed",
                      "observed_at": 1.0, "value": sequence, "event_sequence": None, "artifacts": [], "metadata": {}}],
        "diagnostics": {"reason": reason, "commands": list(commands)} if reason else {},
    }
    report["outcome"].update(status=status, summary=summary, component_results=[component])
    report["outcome"]["diagnostics"] = {"required_component_failures": ["lset"], "deployment": {}, "events": []}
    return report


def _with_commands_run(files: dict[str, bytes], report: dict, *, prefix: str = "batch/", crash_logs: bool = False,
                       experiments: bool = True) -> dict[str, bytes]:
    """The fixture batch plus one run of the report, grouped under its own signature."""
    files = dict(files)
    files[f"{prefix}runs/{LSET}/report.json"] = json.dumps(report).encode()
    files[f"{prefix}runs/{LSET}/scenario.yaml"] = b"scenario_id: lset-1\n"
    if crash_logs:
        files[f"{prefix}runs/{LSET}/logs/1-aa-valkey-0.log"] = files[f"{prefix}runs/{CRASH}/logs/777-3088e5fa-valkey-0.log"]
    if experiments:
        files[f"{prefix}experiments/proposed.lset.yaml"] = _EXPERIMENT_YAML.encode()
    summary = batch_summary()
    experiment = report["outcome"]["plan"]["experiment_id"]
    summary["runs"].append({
        "id": LSET, "source": "scenarios", "status": report["status"], "summary": report["summary"],
        "experiment": experiment, "seed": 777, "report": f"runs/{LSET}/report.json", "signature_key": LSET_KEY,
        "duration_s": 1.0, "worker": 0, "leaks": 0, "origin": None,
    })
    summary["signatures"].append({
        "key": LSET_KEY, "label": f"{report['status']} | {experiment} | lset:{report['status']}",
        "status": report["status"], "experiment": experiment, "signature": {}, "runs": [LSET], "count": 1,
    })
    return with_summary(files, summary, prefix)


def _pr_batch(report: dict, **kw) -> dict[str, bytes]:
    """A PR batch artifact (the folder at its root) of the fixture batch plus one run of ``report``."""
    return _with_commands_run(batch_files(prefix=""), report, prefix="", **kw)


def _analyze_change(files, monkeypatch, claude=_claude_down, *, diff: str | None = None):
    from scripts.common.workflow_artifacts import WorkflowArtifact
    from scripts.fuzzer import analyzer as analyzer_mod

    client = MagicMock()
    client.list_run_artifacts.return_value = [WorkflowArtifact(9, "fuzzer-batch-5", 1, False)]
    client.download_artifact.return_value = files
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow_run.return_value = MagicMock(
        html_url="https://x/runs/5", conclusion="failure", head_sha="abc",
    )
    monkeypatch.setattr(analyzer_mod, "_invoke_claude", claude)
    analyzer = analyzer_mod.FuzzerRunAnalyzer(gh, github_token="t", artifact_client=client, change_diff=diff)
    return analyzer.analyze("owner/valkey-fuzzer", 5, workflow_file="fuzzer-pr-batch.yml",
                            artifact_name="fuzzer-batch-5", change="valkey-io/valkey#4823, merged as e50b62d0b")


_NO_CHANGE_RUNS = {"reached": 0, "unreached": 0, "unexplained": 0, "disputed": 0, "unvetted": 0}


def test_a_batch_without_change_runs_counts_none(monkeypatch):
    analysis = _analyze(batch_files(), monkeypatch)
    assert analysis.change_runs == _NO_CHANGE_RUNS and analysis.recorded == []


def test_an_unreached_change_is_no_finding_but_is_counted(monkeypatch):
    calls = []
    report = _commands_report("ERROR", reason="unreached-change")
    analysis = _analyze_change(_pr_batch(report), monkeypatch,
                               claude=lambda *a, **kw: calls.append(1) or {})
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert len(calls) == 3  # the fixture's three signatures; nothing to ask about an unreached change
    assert analysis.change_runs == {**_NO_CHANGE_RUNS, "unreached": 1}
    (entry,) = analysis.recorded
    assert (entry["run"], entry["experiment"], entry["outcome"], entry["component"]) == (
        LSET, "proposed.lset", "unreached", "lset",
    )
    assert entry["evidence"]["reached"] == [] and "commands" not in entry


def test_an_expectation_both_builds_break_alike_is_no_finding_but_is_counted(monkeypatch):
    calls = []
    report = _commands_report("ERROR", reason="expect-disputed", commands=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch,
                               claude=lambda *a, **kw: calls.append(1) or {})
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert len(calls) == 3  # only the fixture's own signatures
    assert analysis.change_runs == {**_NO_CHANGE_RUNS, "disputed": 1}
    (entry,) = analysis.recorded
    assert (entry["run"], entry["outcome"], entry["commands"]) == (LSET, "disputed", [2])


def test_an_unexplained_difference_without_the_diff_is_recorded_not_triaged(monkeypatch):
    calls = []
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch,
                               claude=lambda *a, **kw: calls.append(1) or {})
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert len(calls) == 3
    assert analysis.change_runs == {**_NO_CHANGE_RUNS, "unexplained": 1}
    (entry,) = analysis.recorded
    assert (entry["outcome"], entry["filed"], entry["commands"]) == ("unexplained", False, [2])
    assert entry["triage"] == "not triaged: the change's diff was not given (--change-diff)"
    assert entry["evidence"]["parent"]["replies"][2] == {"index": 2, "reply": ["x"]}


def _judges(intended, verdict="likely-core-valkey-bug"):
    return lambda *a, **kw: {"overall_status": "anomalous", "triage_verdict": verdict,
                             "summary": "LRANGE differs because LSET now refuses the index.",
                             "difference_intended": intended}


def test_an_unexplained_difference_the_change_did_not_intend_is_a_possible_bug(monkeypatch):
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch, claude=_judges(False),
                               diff="diff --git a/src/t_list.c b/src/t_list.c\n")
    finding = _signature(analysis, LSET_KEY)
    # Claude's own verdict does not decide; an unintended difference is a possible bug, no more.
    assert (finding.triage_verdict, finding.overall_status) == ("possible-core-valkey-bug", "anomalous")
    assert finding.suggested_labels == ["possible-valkey-bug"]
    assert finding.summary == "LRANGE differs because LSET now refuses the index."
    assert finding.change == "valkey-io/valkey#4823, merged as e50b62d0b"
    assert finding.reproduction_hint == (
        f"valkey-fuzzer replay runs/{LSET}/report.json --experiment-dir experiments "
        "--valkey-binary /path/to/valkey-server --artifact parent=/path/to/parent/valkey-server"
    )
    (entry,) = analysis.recorded
    assert entry["filed"] is True and entry["triage"].startswith("judged unintended: LRANGE differs")
    assert analysis.change_runs["unexplained"] == 1


def test_an_intended_difference_is_recorded_not_filed(monkeypatch):
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch, claude=_judges(True),
                               diff="diff --git a/src/t_list.c b/src/t_list.c\n")
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    (entry,) = analysis.recorded
    assert entry["filed"] is False and entry["triage"].startswith("judged intended: LRANGE differs")


@pytest.mark.parametrize("claude,triage", [
    (_claude_down, "not triaged: the AI analysis failed: Claude Code failed (rc=1): timeout"),
    (_judges(None), "the triage did not say whether the difference was intended"),
])
def test_an_unexplained_difference_claude_did_not_judge_is_not_filed(monkeypatch, claude, triage):
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch, claude=claude, diff="d")
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert analysis.recorded[0]["triage"] == triage


def test_an_unexplained_difference_past_the_claude_budget_is_not_filed(monkeypatch):
    from scripts.fuzzer import analyzer as analyzer_mod

    monkeypatch.setattr(analyzer_mod, "_MAX_CLAUDE_FINDINGS", 0)
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    analysis = _analyze_change(_pr_batch(report), monkeypatch, claude=_judges(False), diff="d")
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert analysis.recorded[0]["triage"] == "not triaged: the batch's Claude budget was spent"


def test_claude_gets_the_diff_the_experiments_and_the_question(monkeypatch):
    from scripts.fuzzer import analyzer as analyzer_mod

    calls: list[dict] = []

    def fake_run_agent(profile, prompt, cwd):
        root = Path(cwd)
        calls.append({"prompt": prompt, "diff": (root / "change.diff").read_text() if (root / "change.diff").is_file()
                      else None, "files": sorted(str(p.relative_to(root / "_artifacts"))
                                                 for p in (root / "_artifacts").rglob("*") if p.is_file())})
        return MagicMock(returncode=0, stdout=json.dumps({"overall_status": "anomalous",
                                                          "triage_verdict": "needs-human-triage"}), stderr="")

    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    monkeypatch.setattr(analyzer_mod, "run_agent", fake_run_agent)
    report = _commands_report("ERROR", reason="unexplained-difference", commands=[2], unexplained=[2])
    _analyze_change(_pr_batch(report), monkeypatch, claude=analyzer_mod._invoke_claude,
                    diff="diff --git a/src/t_list.c b/src/t_list.c\n")

    call = next(c for c in calls if LSET in c["prompt"])
    assert call["diff"] == "diff --git a/src/t_list.c b/src/t_list.c\n"
    assert "experiments/proposed.lset.yaml" in call["files"] and f"runs/{LSET}/report.json" in call["files"]
    prompt = call["prompt"]
    assert "The batch tested valkey-io/valkey#4823, merged as e50b62d0b. Its diff is in change.diff" in prompt
    assert "- change.diff - the diff of the change the batch tested." in prompt
    assert "experiments/<id>.yaml: the experiments the proposals wrote for this change" in prompt
    assert "errored with unexplained-difference: commands [2] replied differently" in prompt
    assert "answer in difference_intended" in prompt and '"difference_intended": true|false|null' in prompt
    # The other findings of the batch see the diff and the experiments too, but are not asked the question.
    other = next(c for c in calls if CRASH_KEY in c["prompt"])
    assert other["diff"] is not None and "experiments/proposed.lset.yaml" in other["files"]
    assert "answer in difference_intended" not in other["prompt"]


def test_without_a_diff_or_experiments_the_prompt_says_nothing_of_them(monkeypatch):
    from scripts.fuzzer import analyzer as analyzer_mod

    prompts = []
    monkeypatch.setattr(analyzer_mod, "shallow_clone_at_sha", lambda *a, **kw: True)
    monkeypatch.setattr(analyzer_mod, "run_agent", lambda profile, prompt, cwd: prompts.append(prompt) or MagicMock(
        returncode=0, stdout='{"overall_status": "normal"}', stderr=""))
    _analyze(batch_files(), monkeypatch, claude=analyzer_mod._invoke_claude)
    assert all("## The change" not in p and "experiments/<id>.yaml" not in p and "change.diff" not in p
               for p in prompts)


def test_a_proposed_experiments_fail_that_reached_the_change_is_filed(monkeypatch):
    report = _commands_report("FAIL", reached=[1])
    analysis = _analyze_change(_pr_batch(report), monkeypatch)
    finding = _signature(analysis, LSET_KEY)
    assert finding.run_status == "FAIL" and finding.experiment_id == "proposed.lset"
    assert analysis.change_runs == {**_NO_CHANGE_RUNS, "reached": 1} and analysis.recorded == []


@pytest.mark.parametrize("exited,crash_logs", [(True, False), (False, True)])
def test_a_proposed_experiments_fail_that_crashed_is_filed(monkeypatch, exited, crash_logs):
    report = _commands_report("FAIL", exited=exited)
    analysis = _analyze_change(_pr_batch(report, crash_logs=crash_logs), monkeypatch)
    assert _signature(analysis, LSET_KEY).run_status == "FAIL"
    assert analysis.change_runs["unvetted"] == 0


def test_a_proposed_experiments_fail_that_neither_reached_nor_crashed_is_unvetted(monkeypatch):
    calls = []
    report = _commands_report("FAIL")
    analysis = _analyze_change(_pr_batch(report), monkeypatch,
                               claude=lambda *a, **kw: calls.append(1) or {})
    assert LSET_KEY not in {f.signature_key for f in analysis.findings}
    assert len(calls) == 3
    assert analysis.change_runs == {**_NO_CHANGE_RUNS, "unvetted": 1}
    (entry,) = analysis.recorded
    assert (entry["outcome"], entry["summary"]) == ("unvetted", "required components did not pass: lset")
    assert entry["evidence"]["candidate"]["replies"][1] == {"index": 1, "reply": {"error": "ERR index out of range"}}


def test_an_installed_experiments_fail_is_filed_whether_or_not_it_reached_the_change(monkeypatch):
    report = _commands_report("FAIL", experiment="valkey.aof-durability")
    analysis = _analyze_change(_pr_batch(report), monkeypatch)
    finding = _signature(analysis, LSET_KEY)
    assert finding.reproduction_hint == (
        f"valkey-fuzzer replay runs/{LSET}/report.json --valkey-binary /path/to/valkey-server "
        "--artifact parent=/path/to/parent/valkey-server"
    )
    assert analysis.change_runs["unvetted"] == 0


def test_an_unreadable_run_of_a_proposed_experiment_still_goes_to_a_human(monkeypatch):
    files = _pr_batch(_commands_report("FAIL"))
    files[f"runs/{LSET}/report.json"] = b"{not json"
    finding = _signature(_analyze_change(files, monkeypatch), LSET_KEY)
    assert finding.triage_verdict == "needs-human-triage"


def test_the_reproduction_line_from_a_scheduled_batch_names_its_folder():
    batch = read_batch(_with_commands_run(batch_files(), _commands_report("FAIL")), "batch/")
    context = _context_for(_BASE, batch.run(LSET), "batch/")
    from scripts.fuzzer.analyzer import _reproduction_hint
    assert _reproduction_hint(context) == (
        f"valkey-fuzzer replay batch/runs/{LSET}/report.json --experiment-dir batch/experiments "
        "--valkey-binary /path/to/valkey-server --artifact parent=/path/to/parent/valkey-server"
    )
    plain = read_batch(_with_commands_run(batch_files(), _commands_report("FAIL", parent=False)), "batch/")
    assert _reproduction_hint(_context_for(_BASE, plain.run(LSET), "batch/")) == (
        f"valkey-fuzzer replay batch/runs/{LSET}/report.json --experiment-dir batch/experiments "
        "--valkey-binary /path/to/valkey-server"
    )


def test_the_reproduction_line_of_a_run_without_a_report_reads_its_scenario():
    from scripts.fuzzer.analyzer import _reproduction_hint
    scenario = "experiment: {id: proposed.lset}\nartifacts:\n  parent: {use: valkey.artifact.local-binary}\n"
    context = _ctx(scenario_yaml=scenario, experiment_id="proposed.lset", batch_prefix="")
    assert _reproduction_hint(context) == (
        "valkey-fuzzer run runs/run-1/scenario.yaml --experiment-dir experiments "
        "--valkey-binary /path/to/valkey-server --artifact parent=/path/to/parent/valkey-server"
    )
    assert _reproduction_hint(_ctx(scenario_yaml="- [", experiment_id="x")) == (
        "valkey-fuzzer run runs/run-1/scenario.yaml --valkey-binary /path/to/valkey-server"
    )


def test_a_signature_with_an_open_issue_skips_claude(monkeypatch):
    from scripts.common.workflow_artifacts import WorkflowArtifact
    from scripts.fuzzer import analyzer as analyzer_mod

    asked = []
    monkeypatch.setattr(analyzer_mod, "_invoke_claude", lambda *a, **kw: asked.append(1) or {})
    client = MagicMock()
    client.list_run_artifacts.return_value = [WorkflowArtifact(9, "fuzzer-run-artifacts-1", 1, False)]
    client.download_artifact.return_value = batch_files()
    github = MagicMock()
    github.get_repo.return_value.get_workflow_run.return_value = MagicMock(
        html_url="https://x/runs/1", conclusion="failure", head_sha="abc")
    seen: set[str] = set()

    def is_known(fingerprint):
        seen.add(fingerprint)
        return True

    analysis = analyzer_mod.FuzzerRunAnalyzer(
        github, github_token="t", artifact_client=client, known=is_known,
    ).analyze("r", 1, workflow_file="w")
    signatures = [f for f in analysis.findings if f.kind == "signature"]
    readable = [f for f in signatures if not any(a.title == "Run report unreadable" for a in f.anomalies)]
    assert readable and all("already has an open issue" in f.summary for f in readable)
    assert {f.incident_fingerprint for f in readable} <= seen
    # Claude is asked only about a signature whose report could not be read.
    assert len(asked) == len(signatures) - len(readable)
