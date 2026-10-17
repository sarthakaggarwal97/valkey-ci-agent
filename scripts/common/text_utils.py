"""Shared text processing utilities."""

from __future__ import annotations

import re

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
_LINEBREAK_RE = re.compile(r"[\r\n]+")
_CELL_ESCAPE_RE = re.compile(r"\\([\\|])")


def strip_ansi(text: str) -> str:
    """Remove ANSI SGR color codes from text."""
    return _ANSI_ESCAPE_RE.sub("", text)


def compact_text(value: str, limit: int) -> str:
    """Collapse whitespace and truncate one human-facing text field."""
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def escape_cell(value: object) -> str:
    """Escape backslashes, pipes and line breaks for a markdown table cell.

    ``split_table_row`` reverses it.
    """
    escaped = str(value).replace("\\", "\\\\").replace("|", "\\|")
    return _LINEBREAK_RE.sub(" ", escaped).strip()


def split_table_row(row: str) -> list[str]:
    """Split a markdown table row on unescaped ``|`` and unescape each cell."""
    text = row.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if char == "\\" and not escaped:
            escaped = True
            current.append(char)
            continue
        if char == "|" and not escaped:
            cells.append(_CELL_ESCAPE_RE.sub(r"\1", "".join(current).strip()))
            current = []
            continue
        current.append(char)
        escaped = False
    cells.append(_CELL_ESCAPE_RE.sub(r"\1", "".join(current).strip()))
    return cells


def markdown_section(body: str, heading: str) -> str:
    """Return the body of the ``## <heading>`` section, or ``""`` if absent."""
    pattern = re.compile(
        rf"(?ims)^##\s+{re.escape(heading)}\s*$([\s\S]*?)(?=^##\s+|\Z)"
    )
    match = pattern.search(body)
    return match.group(1) if match else ""

