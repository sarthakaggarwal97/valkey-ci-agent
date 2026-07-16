"""Render AI-generated CI-fix text without active Markdown side effects."""

from __future__ import annotations

_MARKDOWN_ENTITIES = str.maketrans(
    {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "@": "&#64;",
        "#": "&#35;",
        "[": "&#91;",
        "]": "&#93;",
        "`": "&#96;",
        "*": "&#42;",
        "_": "&#95;",
        "~": "&#126;",
        "|": "&#124;",
        "\\": "&#92;",
    }
)


def normalize_generated_text(value: str, *, limit: int = 2_000) -> str:
    """Return bounded single-line text with control characters removed."""
    if limit < 1:
        raise ValueError("limit must be positive")
    printable = "".join(
        character if character >= " " and character != "\x7f" else " "
        for character in value
    )
    normalized = " ".join(printable.split())
    if len(normalized) <= limit:
        return normalized
    if limit <= 3:
        return normalized[:limit]
    return normalized[: limit - 3].rstrip() + "..."


def markdown_generated_text(value: str, *, limit: int = 2_000) -> str:
    """Return inert inline Markdown suitable for comments and PR bodies."""
    return normalize_generated_text(value, limit=limit).translate(_MARKDOWN_ENTITIES)
