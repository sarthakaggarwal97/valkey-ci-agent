"""Compatibility entry point for the credential-separated backport phases.

The legacy module ran discovery, AI preparation, target validation, and
publication in one credentialed process. The module path remains supported,
but each invocation now performs exactly one phase through ``phased.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.backport import phased


def main(argv: list[str] | None = None) -> int:
    """Run one hardened backport phase."""
    return phased.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
