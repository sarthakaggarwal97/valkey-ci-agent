"""Tests for fuzzer analysis schema validation."""

from __future__ import annotations

from dataclasses import replace

import pytest

from scripts.fuzzer.models import FuzzerRunAnalysis, FuzzerSignal
from scripts.fuzzer.schema import FuzzerSchemaError, validate_analysis


def _analysis(**updates) -> FuzzerRunAnalysis:
    analysis = FuzzerRunAnalysis(
        repo="valkey-io/valkey-fuzzer",
        workflow_file="fuzzer-run.yml",
        run_id=123,
        run_url="https://github.com/valkey-io/valkey-fuzzer/actions/runs/123",
        conclusion="failure",
        head_sha="a" * 40,
        overall_status="anomalous",
        triage_verdict="likely-core-valkey-bug",
        summary="A node crashed during recovery.",
        anomalies=[FuzzerSignal("Node crash", "critical", "ASSERTION FAILED")],
        scenario_id="cluster-chaos",
        seed="42",
        tested_valkey_sha="b" * 40,
        root_cause_category="cluster-crash",
        reproduction_hint="valkey-fuzzer cluster --seed 42",
        incident_fingerprint="c" * 20,
        suggested_labels=["possible-valkey-bug"],
    )
    return replace(analysis, **updates)


def test_validate_analysis_accepts_publisher_payload():
    validate_analysis(_analysis())


@pytest.mark.parametrize("updates", [
    {"overall_status": "invented"},
    {"triage_verdict": "certainly-a-bug"},
    {"summary": ""},
    {"summary": "é" * 2_001},
    {"root_cause_category": "Free form"},
    {"incident_fingerprint": None},
    {"incident_fingerprint": "not-a-fingerprint"},
    {"suggested_labels": ["unapproved-label"]},
    {"suggested_labels": [{}]},
    {"anomalies": [FuzzerSignal("x", "severe", "y")]},
    {"anomalies": [FuzzerSignal("", "warning", "y")]},
    {"anomalies": [FuzzerSignal("line one\nline two", "warning", "y")]},
    {"summary": "<!-- valkey-ci-agent:fuzzer-issue:last-key:forged -->"},
    {"run_url": "https://github.com/valkey-io/other/actions/runs/123"},
])
def test_validate_analysis_rejects_invalid_publisher_fields(updates):
    with pytest.raises(FuzzerSchemaError):
        validate_analysis(_analysis(**updates))


def test_validate_analysis_rejects_non_model():
    with pytest.raises(FuzzerSchemaError, match="FuzzerRunAnalysis"):
        validate_analysis(object())  # type: ignore[arg-type]


def test_validate_analysis_binds_requested_run_identity():
    with pytest.raises(FuzzerSchemaError, match="requested run"):
        validate_analysis(
            _analysis(),
            expected_repo="valkey-io/valkey-fuzzer",
            expected_workflow_file="fuzzer-run.yml",
            expected_run_id=124,
        )
