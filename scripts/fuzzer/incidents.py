"""Stable fingerprints for fuzzer incident deduplication."""

from __future__ import annotations

import hashlib
import re
from typing import Iterable

from scripts.fuzzer.models import FuzzerSignal

_VOLATILE_RE = re.compile(
    r"0x[0-9a-f]+|\b[0-9a-f]{7,40}\b|\bnode[-_ ]?\d+\b|\b\d+\b",
    re.IGNORECASE,
)


def compute_fingerprint(
    *,
    repo: str,
    workflow_file: str,
    root_cause_category: str | None,
    anomalies: Iterable[FuzzerSignal],
) -> str:
    """Stable hash grouping repeated failures by shape, not run ID."""
    parts = [repo.lower(), workflow_file.lower(), (root_cause_category or "").lower()]
    # Normalize each anomaly's shape (strip volatile addresses, IDs, numbers)
    # before deduplicating and slicing, so two runs with the same underlying
    # failure but different node IDs produce the same fingerprint regardless
    # of which 8 shapes survive the slice.
    normalized = sorted({
        _VOLATILE_RE.sub("_", f"{s.title}:{s.evidence}".lower())
        for s in anomalies
    })[:8]
    parts.extend(normalized)
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]
