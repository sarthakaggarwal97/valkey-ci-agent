"""Stable fingerprints for incident deduplication.

Workflow-agnostic. Each caller passes:
- a `namespace` tuple of identity strings (e.g. repo + workflow file + root
  cause) that disambiguate one workflow's findings from another's.
- a list of raw `shapes` describing each finding. Volatile substrings
  (memory addresses, hex SHAs, node IDs, run-specific numbers) are
  normalized so two runs of the same underlying failure with different
  node IDs produce the same fingerprint.

Fingerprints are stable across processes — never tied to PID, time, or run ID.
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable

_VOLATILE_RE = re.compile(
    r"0x[0-9a-f]+|\b[0-9a-f]{7,40}\b|\bnode[-_ ]?\d+\b|\b\d+\b",
    re.IGNORECASE,
)


def compute_fingerprint(*, namespace: Iterable[str], shapes: Iterable[str],
                        max_shapes: int = 8) -> str:
    """Stable hash grouping repeated failures by shape, not run ID.

    Normalization runs *before* dedup and slicing so volatile variants do
    not change which `max_shapes` survive the cap.
    """
    parts = [n.lower() for n in namespace]
    normalized = sorted({_VOLATILE_RE.sub("_", s.lower()) for s in shapes})[:max_shapes]
    parts.extend(normalized)
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]
