from __future__ import annotations

import pytest

from scripts.common.markdown import (
    GITHUB_BODY_MAX_BYTES,
    blockquote,
    bounded_body,
    fenced_code,
    inline_code,
    markdown_link,
    safe_url,
    sanitize_text,
    table_cell,
    truncate_utf8,
    with_required_suffix,
)


def test_dynamic_fence_contains_injected_fence_and_neutralizes_mentions() -> None:
    rendered = fenced_code("failure\n```\n@maintainer\n## heading")
    assert rendered.startswith("````\n")
    assert rendered.endswith("\n````")
    assert "@\u200bmaintainer" in rendered


def test_inline_and_table_content_cannot_break_structure() -> None:
    assert inline_code("a`b").startswith("``")
    cell = table_cell("x|y\n@team")
    assert "\\|" in cell
    assert "\n" not in cell
    assert "@\u200bteam" in cell
    with pytest.raises(TypeError):
        table_cell({"not": "scalar"})


def test_urls_are_absolute_http_without_credentials() -> None:
    assert safe_url("https://github.com/o/r/actions/runs/1")
    assert safe_url("javascript:alert(1)") is None
    assert safe_url("https://user:secret@example.com/x") is None
    assert markdown_link("run", "javascript:alert(1)") == "run"


def test_byte_bounds_do_not_split_unicode_and_preserve_required_suffix() -> None:
    assert len(truncate_utf8("\N{SNOWMAN}" * 100, 20).encode("utf-8")) <= 20
    suffix = "<!-- durable:marker -->"
    result = with_required_suffix("x" * GITHUB_BODY_MAX_BYTES, suffix)
    assert len(result.encode("utf-8")) <= GITHUB_BODY_MAX_BYTES
    assert result.endswith(suffix)
    assert len(bounded_body("x" * (GITHUB_BODY_MAX_BYTES + 1)).encode("utf-8")) <= GITHUB_BODY_MAX_BYTES


def test_plain_text_policy_rejects_non_strings_and_quotes_safely() -> None:
    with pytest.raises(TypeError):
        sanitize_text(7)  # type: ignore[arg-type]
    assert blockquote("hello\n> injected") == "> hello\n> &gt; injected"
