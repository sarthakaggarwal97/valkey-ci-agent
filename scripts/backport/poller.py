"""Compatibility entry point for phased rolling-backport polling operations.

Polling and scheduled sweeps now share the same candidate and aggregate
engines. This module retains the former operator path and routes to them.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.backport import sweep


def main(argv: list[str] | None = None) -> int:
    """Run one polling discovery, candidate, or aggregate operation."""
    return sweep.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
