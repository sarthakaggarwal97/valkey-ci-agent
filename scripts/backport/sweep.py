"""Compatibility router for phased rolling-backport operator commands.

Use ``candidates`` for read-only Project discovery, ``candidate`` for one
candidate's hardened phases, and ``aggregate`` for rolling-PR phases.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.backport import aggregate, candidate_matrix, phased

_COMMANDS = ("candidates", "candidate", "aggregate")


def main(argv: list[str] | None = None) -> int:
    """Route a sweep operation to its hardened phase implementation."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=_COMMANDS)
    parser.add_argument(
        "arguments",
        nargs=argparse.REMAINDER,
        help="arguments passed to the selected phased command",
    )
    args = parser.parse_args(arguments)
    if args.command == "candidates":
        return candidate_matrix.main(args.arguments)
    if args.command == "candidate":
        return phased.main(args.arguments)
    return aggregate.main(args.arguments)


if __name__ == "__main__":
    raise SystemExit(main())
