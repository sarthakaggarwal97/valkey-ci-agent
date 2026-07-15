from __future__ import annotations

import pytest

from scripts.fuzzer.models import FuzzerRunAnalysis
from scripts.fuzzer.policy import should_publish


def _analysis(status: str, verdict: str) -> FuzzerRunAnalysis:
    return FuzzerRunAnalysis(
        repo="valkey-io/valkey-fuzzer",
        workflow_file="fuzzer-run.yml",
        run_id=1,
        run_url="https://example.test/run/1",
        head_sha="a" * 40,
        conclusion="failure",
        overall_status=status,
        triage_verdict=verdict,
        root_cause_category=None,
        summary="summary",
        anomalies=[],
        suggested_labels=[],
        reproduction_hint=None,
        incident_fingerprint=None,
    )


@pytest.mark.parametrize(
    ("status", "verdict", "expected"),
    [
        ("anomalous", "likely-core-valkey-bug", True),
        ("warning", "possible-core-valkey-bug", True),
        ("warning", "needs-human-triage", True),
        ("normal", "expected-chaos-noise", False),
        ("warning", "expected-chaos-noise", False),
        ("warning", "environmental-or-infra", False),
    ],
)
def test_should_publish(status: str, verdict: str, expected: bool) -> None:
    assert should_publish(_analysis(status, verdict)) is expected
