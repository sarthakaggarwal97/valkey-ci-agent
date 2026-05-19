"""Tests for fuzzer analyzer."""
from __future__ import annotations

import pytest

from scripts.fuzzer.analyzer import (
    _dedupe_signals,
    _load_artifacts,
    _parse_claude_response,
    _scan_logs,
    _triage,
)
from scripts.fuzzer.models import FuzzerRunContext, FuzzerSignal


def _ctx(**kw) -> FuzzerRunContext:
    defaults = dict(repo="r", workflow_file="w", run_id=1, run_url="u",
                    conclusion="failure", head_sha="h")
    defaults.update(kw)
    return FuzzerRunContext(**defaults)


def test_scan_logs_detects_crash():
    ctx = _ctx()
    ctx.node_logs = {"node-1.log": "ASSERTION FAILED at server.c:123"}
    anomalies, _ = _scan_logs(ctx)
    assert any("crash" in a.title.lower() or "assertion" in a.title.lower() for a in anomalies)


def test_scan_logs_validation_failure():
    ctx = _ctx()
    ctx.results = {
        "success": False, "error_message": "failed",
        "final_validation": {"checks": {"slot_coverage": {"success": False, "error": "lost slots"}}},
    }
    anomalies, _ = _scan_logs(ctx)
    assert any("slot" in a.title.lower() for a in anomalies)


def test_load_artifacts_reads_manifest_and_results():
    ctx = _ctx()
    _load_artifacts(ctx, {
        "manifest.json": b'{"scenario_id": "chaos-1", "seed": 42, "valkey_sha": "deadbeef1234567"}',
        "results.json": b'{"results": [{"success": false}]}',
        "node-1.log": b"log output",
    })
    assert ctx.scenario_id == "chaos-1"
    assert ctx.seed == "42"
    assert ctx.tested_valkey_sha == "deadbeef1234567"
    assert "node-1.log" in ctx.node_logs


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
