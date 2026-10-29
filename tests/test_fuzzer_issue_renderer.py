"""Tests for fuzzer-specific issue rendering."""
from __future__ import annotations

from scripts.fuzzer.issue_renderer import (
    MARKER_NAMESPACE,
    _build_title,
    _render_body,
    _render_comment,
    render_for,
)
from scripts.fuzzer.models import FuzzerFinding, FuzzerSignal


def _analysis(**kw) -> FuzzerFinding:
    defaults = dict(
        repo="valkey-io/valkey-fuzzer", workflow_file="fuzzer-run.yml",
        run_id=100, run_url="https://github.com/r/actions/runs/100",
        conclusion="failure", head_sha="abc", kind="run", overall_status="anomalous",
        triage_verdict="likely-core-valkey-bug", summary="crash found",
        anomalies=[FuzzerSignal("Node crash", "critical", "segfault")],
        incident_fingerprint="fp_test_12345678901",
        suggested_labels=["possible-valkey-bug"],
    )
    defaults.update(kw)
    return FuzzerFinding(**defaults)


def test_build_title_from_root_cause():
    assert _build_title(_analysis(root_cause_category="split-brain")) == "[fuzzer-run] Split Brain"


def test_build_title_from_anomaly():
    assert _build_title(_analysis()) == "[fuzzer-run] Node crash"


def test_build_title_for_analyzer_failure():
    """A run the analyzer could not verdict gets an honest title, not
    "Anomalous behavior detected"."""
    analysis = _analysis(
        overall_status="warning", triage_verdict="needs-human-triage",
        summary="Run 100: the analyzer could not complete. ... AI analysis failed: timeout after 1800s",
        anomalies=[], root_cause_category=None, analyzer_incomplete=True,
    )
    assert _build_title(analysis) == "[fuzzer-run] Analyzer could not complete"


def test_build_title_does_not_claim_failure_on_completed_triage_verdict():
    """A completed analysis whose verdict is "needs-human-triage" must NOT be
    mislabeled as an analyzer failure. This is the #104 conflation guard."""
    analysis = _analysis(
        overall_status="warning", triage_verdict="needs-human-triage",
        summary="Cluster recovered but consensus was slow; a human should look.",
        anomalies=[], root_cause_category=None, analyzer_incomplete=False,
    )
    assert _build_title(analysis) == "[fuzzer-run] Anomalous behavior detected"


def test_build_title_for_missing_artifact_error():
    """A run that could not be analyzed because artifacts were missing gets the
    same honest title as an AI-analysis failure, not "Anomalous behavior"."""
    from scripts.fuzzer.analyzer import _build_error_analysis
    base = dict(repo="r", workflow_file="w", run_id=1, run_url="u", conclusion="failure", head_sha="h")
    analysis = _build_error_analysis(base, "no fuzzer artifact bundle found")
    assert _build_title(analysis) == "[fuzzer-run] Analyzer could not complete"


def test_render_body_surfaces_analyzer_error():
    analysis = _analysis(
        overall_status="warning", triage_verdict="needs-human-triage",
        summary="Run 100: the analyzer could not complete. ... AI analysis failed: timeout after 1800s",
        anomalies=[], analyzer_incomplete=True,
    )
    body = _render_body(analysis, "<!-- marker -->", occurrences=1)
    assert "timeout after 1800s" in body
    assert "Anomalous behavior detected" not in body


def test_render_body_contains_essentials():
    body = _render_body(_analysis(), "<!-- marker -->", occurrences=1)
    assert "<!-- marker -->" in body
    assert f"<!-- {MARKER_NAMESPACE}:occurrences:1 -->" in body
    assert "Node crash" in body
    assert "crash found" in body


def test_render_body_names_the_fuzzer_outcome_and_experiment():
    body = _render_body(
        _analysis(run_status="FAIL", experiment_id="valkey.cluster-chaos"), "<!-- marker -->", occurrences=1,
    )
    assert "| Fuzzer outcome | `FAIL` |" in body
    assert "| Experiment | `valkey.cluster-chaos` |" in body
    unknown = _render_body(_analysis(), "<!-- marker -->", occurrences=1)
    assert "| Fuzzer outcome | `unknown` |" in unknown


def test_render_for_returns_callable_with_labels():
    """The factory hands IssueDedupPublisher.upsert a callable that produces
    fully-populated IssueContent including labels."""
    cb = render_for(_analysis())
    content = cb("<!-- marker -->", 3)
    assert content.title == "[fuzzer-run] Node crash"
    assert "<!-- marker -->" in content.body
    assert f"<!-- {MARKER_NAMESPACE}:occurrences:3 -->" in content.body
    assert "Occurrence #3" in content.comment
    assert content.labels == ("possible-valkey-bug",)


def _signature_finding(**kw) -> FuzzerFinding:
    defaults = dict(
        kind="signature", run_status="FAIL", experiment_id="ciagent.debug-crash",
        batch_runs=["scenario-crash-777", "scenario-crash-778"], signature_key="cc08b3931ccc",
        signature_label="FAIL | ciagent.debug-crash | liveness:FAIL | valkey-N: process not running",
        rerun_verdict="reproduced", rerun_detail="1/1", anomalies=[FuzzerSignal("Run failed", "critical", "x")],
    )
    defaults.update(kw)
    return _analysis(**defaults)


def test_a_signature_names_its_runs_signature_and_serial_reruns():
    finding = _signature_finding()
    assert _build_title(finding) == (
        "[fuzzer-run] FAIL | ciagent.debug-crash | liveness:FAIL | valkey-N: process not running"
    )
    body = _render_body(finding, "<!-- marker -->", occurrences=1)
    assert "| Batch runs | `scenario-crash-777`, `scenario-crash-778` (2) |" in body
    assert ("| Failure signature | `cc08b3931ccc`: FAIL \\| ciagent.debug-crash \\| liveness:FAIL "
            "\\| valkey-N: process not running |") in body
    assert "| Serial re-runs | reproduced (1/1 reproduced) |" in body
    assert "not a Valkey bug" not in body
    comment = _render_comment(finding, 2)
    assert "Batch runs: `scenario-crash-777`, `scenario-crash-778` (2)" in comment
    assert "Serial re-runs: reproduced (1/1 reproduced)" in comment


def test_a_long_signature_title_is_cut_and_a_root_cause_wins():
    long = _signature_finding(signature_label="FAIL | " + "x" * 200)
    assert len(_build_title(long)) == len("[fuzzer-run] ") + 100
    assert _build_title(_signature_finding(root_cause_category="debug-segfault")) == "[fuzzer-run] Debug Segfault"


def test_many_runs_are_counted_not_listed():
    body = _render_body(_signature_finding(batch_runs=[f"r{i}" for i in range(12)]), "<!-- m -->", occurrences=1)
    assert "`r9` and 2 more (12) |" in body and "`r10`" not in body


def test_a_fuzzer_side_finding_says_it_is_not_a_valkey_bug():
    finding = _analysis(
        kind="zoo", title="[fuzzer-zoo] Bug zoo case replica-skips-writes no longer caught",
        triage_verdict="needs-human-triage", fuzzer_side=True, suggested_labels=[],
        batch_runs=["zoo-replica-skips-writes-scenario-0"],
    )
    content = render_for(finding)("<!-- marker -->", 1)
    assert content.title == "[fuzzer-zoo] Bug zoo case replica-skips-writes no longer caught"
    assert "> This is a problem in valkey-fuzzer itself, not a Valkey bug." in content.body
    assert "Fuzzer outcome" not in content.body and "Serial re-runs" not in content.body
    assert content.labels == ()
