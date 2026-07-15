"""Compatibility entry point for credential-separated CI-fix phases.

The module path remains available to local operators, but each invocation now
performs exactly one phase through ``phased.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ci_fix import phased


def main(argv: list[str] | None = None) -> int:
    """Run one hardened CI-fix phase."""
    return phased.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
