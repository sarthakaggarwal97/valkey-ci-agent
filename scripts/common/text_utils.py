"""Shared text processing utilities."""

from __future__ import annotations

import re

# Matches ECMA-48 escape sequences: CSI sequences (ESC [ ... final-byte), OSC
# sequences (ESC ] ... terminated by BEL or ST), and 7-bit C1 Fe escapes (ESC
# followed by a byte in @-_). OSC and CSI are matched first so the C1 catch-all
# does not consume their leading byte.
_ANSI_ESCAPE_RE = re.compile(
    r"\x1B(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1B]*(?:\x07|\x1B\\)|[@-Z\\-_])"
)


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences (SGR colors, CSI commands, OSC) from text."""
    return _ANSI_ESCAPE_RE.sub("", text)
