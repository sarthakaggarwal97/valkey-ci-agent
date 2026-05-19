from __future__ import annotations

from scripts.fuzzer.incidents import compute_fingerprint
from scripts.fuzzer.models import FuzzerSignal


def test_same_shape_same_fingerprint():
    kwargs = dict(
        repo="valkey-io/valkey-fuzzer", workflow_file="fuzzer-run.yml",
        root_cause_category="split-brain",
        anomalies=[FuzzerSignal("Split-brain", "critical", "detected")],
    )
    assert compute_fingerprint(**kwargs) == compute_fingerprint(**kwargs)


def test_different_failures_differ():
    fp1 = compute_fingerprint(
        repo="r", workflow_file="w", root_cause_category="split-brain",
        anomalies=[FuzzerSignal("a", "critical", "x")],
    )
    fp2 = compute_fingerprint(
        repo="r", workflow_file="w", root_cause_category="crash",
        anomalies=[FuzzerSignal("b", "critical", "y")],
    )
    assert fp1 != fp2


def test_volatile_parts_normalized_before_slice():
    """Normalization happens before sort/slice so volatile addresses, node IDs,
    and run-specific numbers don't change which 8 shapes survive the cap."""
    fp1 = compute_fingerprint(
        repo="r", workflow_file="w", root_cause_category="crash",
        anomalies=[
            FuzzerSignal("crash", "critical", "node-1 at 0xaaa"),
            FuzzerSignal("crash", "critical", "node-2 at 0xbbb"),
            FuzzerSignal("timeout", "critical", "after 120 seconds"),
        ],
    )
    fp2 = compute_fingerprint(
        repo="r", workflow_file="w", root_cause_category="crash",
        anomalies=[
            FuzzerSignal("crash", "critical", "node-9 at 0xdeadbeef"),
            FuzzerSignal("crash", "critical", "node-10 at 0xfeedface"),
            FuzzerSignal("timeout", "critical", "after 999 seconds"),
        ],
    )
    assert fp1 == fp2
