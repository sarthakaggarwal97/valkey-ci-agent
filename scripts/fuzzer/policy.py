"""Deterministic fuzzer publication policy."""

from __future__ import annotations

from scripts.fuzzer.models import FuzzerRunAnalysis

_NO_PUBLISH_VERDICTS = frozenset(
    {"expected-chaos-noise", "environmental-or-infra"}
)


def should_publish(analysis: FuzzerRunAnalysis) -> bool:
    """Publish anomalies and unresolved bug candidates."""
    if analysis.overall_status == "anomalous":
        return True
    return analysis.triage_verdict not in _NO_PUBLISH_VERDICTS
