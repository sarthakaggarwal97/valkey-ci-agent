"""Safe, size-bounded GitHub Markdown primitives for untrusted content."""

from __future__ import annotations

import html
import re
from urllib.parse import urlsplit

GITHUB_TITLE_MAX_BYTES = 256
GITHUB_BODY_MAX_BYTES = 65_536
GITHUB_COMMENT_MAX_BYTES = 65_536

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_MARKDOWN_META_RE = re.compile(r"([\\`*_{}\[\]()#+!|>~])")
_LANGUAGE_RE = re.compile(r"^[A-Za-z0-9_+-]{0,32}$")
_TRUNCATED = "\n\n_Content truncated to the GitHub size limit._"


def truncate_utf8(value: str, max_bytes: int, *, suffix: str = "...") -> str:
    """Truncate without splitting a UTF-8 sequence, including ``suffix``."""
    if max_bytes < 0:
        raise ValueError("max_bytes must be nonnegative")
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    suffix_bytes = suffix.encode("utf-8")
    if len(suffix_bytes) > max_bytes:
        return suffix_bytes[:max_bytes].decode("utf-8", errors="ignore")
    prefix = encoded[:max_bytes - len(suffix_bytes)].decode(
        "utf-8",
        errors="ignore",
    )
    return prefix + suffix


def sanitize_text(
    value: str,
    *,
    max_bytes: int | None = None,
    multiline: bool = True,
) -> str:
    """Normalize controls and neutralize GitHub mentions in plain text."""
    if not isinstance(value, str):
        raise TypeError("Markdown text values must be strings")
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_RE.sub("", text)
    if not multiline:
        text = " ".join(text.split())
    text = text.replace("@", "@\u200b")
    if max_bytes is not None:
        text = truncate_utf8(text, max_bytes)
    return text


def escape_text(
    value: str,
    *,
    max_bytes: int | None = None,
    multiline: bool = True,
) -> str:
    """Render plain untrusted text without allowing Markdown structure."""
    text = sanitize_text(value, max_bytes=max_bytes, multiline=multiline)
    escaped = _MARKDOWN_META_RE.sub(r"\\\1", html.escape(text, quote=False))
    escaped = re.sub(r"(?m)^(\s*)-(?=\s)", r"\1\\-", escaped)
    escaped = re.sub(r"(?m)^(\s*\d+)\.(?=\s)", r"\1\\.", escaped)
    return escaped


def inline_code(value: str, *, max_bytes: int = 2048) -> str:
    """Render an inline code span with a delimiter longer than its contents."""
    text = sanitize_text(value, max_bytes=max_bytes, multiline=False)
    longest = max((len(match) for match in re.findall(r"`+", text)), default=0)
    fence = "`" * max(1, longest + 1)
    if text.startswith(("`", " ")) or text.endswith(("`", " ")):
        text = f" {text} "
    return f"{fence}{text}{fence}"


def fenced_code(
    value: str,
    *,
    language: str = "",
    max_content_bytes: int = 16 * 1024,
) -> str:
    """Render bounded code with a fence that embedded backticks cannot close."""
    if not _LANGUAGE_RE.fullmatch(language):
        raise ValueError("invalid fenced-code language")
    text = sanitize_text(value, multiline=True)
    text = truncate_utf8(
        text,
        max_content_bytes,
        suffix="\n[... evidence truncated; use the linked workflow artifact ...]",
    )
    longest = max((len(match) for match in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}{language}\n{text}\n{fence}"


def table_cell(value: object, *, max_bytes: int = 2048) -> str:
    """Render a value in one Markdown table cell.

    Table cells explicitly coerce scalar values to strings. Other helpers
    require strings so malformed structured input cannot silently stringify.
    """
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        raise TypeError("Markdown table values must be scalar")
    text = sanitize_text(
        "" if value is None else str(value),
        max_bytes=max_bytes,
        multiline=False,
    )
    text = html.escape(text, quote=False)
    return text.replace("\\", "\\\\").replace("|", "\\|")


def safe_url(value: str, *, max_bytes: int = 2048) -> str | None:
    """Return a safe absolute HTTP(S) URL, otherwise ``None``."""
    if not isinstance(value, str):
        return None
    url = sanitize_text(value, max_bytes=max_bytes, multiline=False)
    if not url or any(character.isspace() for character in url):
        return None
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme not in {"https", "http"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    return url


def markdown_link(label: str, url: str, *, max_label_bytes: int = 512) -> str:
    """Render a validated link or safe unlinked label."""
    rendered_label = escape_text(
        label,
        max_bytes=max_label_bytes,
        multiline=False,
    )
    target = safe_url(url)
    if target is None:
        return rendered_label
    target = target.replace("(", "%28").replace(")", "%29")
    return f"[{rendered_label}]({target})"


def blockquote(value: str, *, max_bytes: int = 4096) -> str:
    text = escape_text(value, max_bytes=max_bytes, multiline=True)
    return "\n".join(f"> {line}" if line else ">" for line in text.splitlines())


def bounded_title(value: str) -> str:
    return truncate_utf8(
        sanitize_text(value, multiline=False),
        GITHUB_TITLE_MAX_BYTES,
    )


def bounded_body(value: str) -> str:
    return bounded_markdown(value, max_bytes=GITHUB_BODY_MAX_BYTES)


def bounded_comment(value: str) -> str:
    return bounded_markdown(value, max_bytes=GITHUB_COMMENT_MAX_BYTES)


def bounded_markdown(value: str, *, max_bytes: int) -> str:
    if not isinstance(value, str):
        raise TypeError("Markdown documents must be strings")
    if len(value.encode("utf-8")) <= max_bytes:
        return value
    return truncate_utf8(value, max_bytes, suffix=_TRUNCATED)


def with_required_suffix(
    value: str,
    suffix: str,
    *,
    max_bytes: int = GITHUB_BODY_MAX_BYTES,
) -> str:
    """Append metadata while preserving it when content must be truncated."""
    required = suffix if not value else f"\n{suffix}"
    if len(required.encode("utf-8")) > max_bytes:
        raise ValueError("required Markdown suffix exceeds the document budget")
    if len((value + required).encode("utf-8")) <= max_bytes:
        return value + required
    reserve = len(required.encode("utf-8")) + len(_TRUNCATED.encode("utf-8"))
    prefix = truncate_utf8(value, max(0, max_bytes - reserve), suffix="")
    return prefix.rstrip() + _TRUNCATED + required
