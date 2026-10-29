"""Shared logging setup for workflow entry points.

Three things make an Actions log readable: one compact line format, the
run's outcome surfaced as an annotation at the top of the run page, and
noisy output (AI streams, build output) folded into collapsible groups so
the decision lines stay visible.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from scripts.common.text_utils import strip_ansi

LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(tag)s] %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"

_ANNOTATION = {logging.ERROR: "error", logging.WARNING: "warning"}
_group_open = False
_UNTRUSTED_OUTPUT_LIMIT = 1 << 20


def _physical_lines(text: str | bytes) -> list[str]:
    """Render untrusted output as terminal-safe physical lines.

    GitHub's legacy workflow commands are recognized at physical line
    boundaries. Normalize every kind of carriage-return boundary, strip ANSI
    controls that could obscure the rendered prefix, and make the remaining
    C0 controls visible rather than sending them to the runner.
    """
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    normalized = strip_ansi(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    if lines and not lines[-1]:
        lines.pop()
    return [
        "".join(
            character
            if (
                character == "\t"
                or (
                    ord(character) >= 0x20
                    and ord(character) != 0x7F
                    and unicodedata.category(character) not in {"Cc", "Cf", "Cs", "Zl", "Zp"}
                )
            )
            else (
                f"\\x{ord(character):02x}"
                if ord(character) <= 0xFF
                else f"\\u{ord(character):04x}"
            )
            for character in line
        )
        for line in lines
    ]


class _TagFormatter(logging.Formatter):
    """Tag each record with its feature area (``release``, ``backport``)
    instead of a full dotted module path. Shared helpers under
    ``scripts/common`` and ``scripts/ai`` are tagged by file (``proc``,
    ``polling``, ``runtime``), since the area is the caller's.
    """

    def format(self, record: logging.LogRecord) -> str:
        path = Path(record.pathname)
        parent = path.parent.name
        record.tag = path.stem if parent in ("common", "ai") else parent.replace("_", "-")
        lines = _physical_lines(super().format(record))
        if not lines:
            return ""
        # The configured format gives the first line a timestamp/tag. Prefix
        # it too when a caller constructs this formatter with a message-only
        # format, and always prefix continuation lines. No untrusted physical
        # line can consequently begin with ``::``.
        first = f"| {lines[0]}" if lines[0].startswith("::") else lines[0]
        return "\n".join((first, *(f"| {line}" for line in lines[1:])))


def configure_logging(*, verbose: bool = False) -> None:
    """Configure the one log format every workflow entry point uses."""
    handler = logging.StreamHandler()
    handler.setFormatter(_TagFormatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=[handler])


def emit_untrusted_output(text: str | bytes, *, label: str) -> None:
    """Write captured child output without exposing workflow-command syntax."""
    prefix = f"[{' '.join(label.split()) or 'output'}] "
    for line in _physical_lines(text):
        # The fixed prefix is part of every physical write. Replacing the
        # command delimiter as well keeps copied/rendered output visibly inert.
        print(f"{prefix}{line.replace('::', ': :')}", file=sys.stderr, flush=True)


@contextmanager
def capture_untrusted_output(
    *,
    label: str,
    limit: int = _UNTRUSTED_OUTPUT_LIMIT,
) -> Iterator[BinaryIO]:
    """Yield a pipe sink whose output is drained, bounded, and made inert.

    The reader keeps draining after ``limit`` so an untrusted child cannot
    block on a full pipe or make the trusted parent retain unbounded output.
    Callers must end all descendant processes before leaving this context, so
    every inherited copy of the write descriptor is closed.
    """
    read_fd, write_fd = os.pipe()
    reader = os.fdopen(read_fd, "rb", buffering=0)
    writer = os.fdopen(write_fd, "wb", buffering=0)
    total = 0

    def drain() -> None:
        nonlocal total
        while chunk := reader.read(8192):
            before = total
            total += len(chunk)
            if before < limit:
                emit_untrusted_output(chunk[: max(0, limit - before)], label=label)

    thread = threading.Thread(target=drain, name="untrusted-output-drain", daemon=True)
    thread.start()
    try:
        yield writer
    finally:
        writer.close()
        thread.join(timeout=30)
        if thread.is_alive():
            reader.close()
            raise RuntimeError("the captured child output pipe did not close")
        reader.close()
        if total > limit:
            print(
                f"[{' '.join(label.split()) or 'output'}] "
                f"output truncated after {limit} bytes",
                file=sys.stderr,
                flush=True,
            )


def _in_actions() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def _escape_command_data(text: str) -> str:
    # GitHub's own escaping for workflow-command data: a newline inside the
    # message can then never start a second (forged) command.
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def annotate(level: int, text: str) -> None:
    """Pin *text* to the top of the run page (Actions only) without logging it."""
    if _in_actions():
        kind = _ANNOTATION.get(level, "notice")
        print(f"::{kind}::{_escape_command_data(text)}", file=sys.stderr, flush=True)


def log_outcome(target_logger: logging.Logger, level: int, message: str, *args: object) -> None:
    """Log the run's final outcome and pin it to the top of the run page.

    In Actions this also emits a ``::notice::``/``::warning::``/``::error::``
    annotation, so the result is visible without opening the log. Written
    to stderr: several workflows tee stdout into a JSON result file.
    """
    # Render once and collapse every whitespace run (newlines included): the
    # log line is printed raw, so an embedded "\n::warning::" in a value would
    # otherwise reach the runner as a workflow command of its own.
    text = " ".join((message % args if args else message).split())
    # stacklevel: tag the record with the caller's file, not this helper's.
    target_logger.log(level, "%s", text, stacklevel=2)
    annotate(level, text)


@contextmanager
def log_group(title: str) -> Iterator[None]:
    """Fold everything logged inside into one collapsible Actions group.

    Actions groups cannot nest, so an inner group is a no-op and its output
    simply stays inside the outer one.
    """
    global _group_open
    if not _in_actions() or _group_open:
        yield
        return
    sys.stderr.flush()
    print(f"::group::{_escape_command_data(' '.join(title.split()))}", file=sys.stderr, flush=True)
    _group_open = True
    try:
        yield
    finally:
        _group_open = False
        sys.stderr.flush()
        print("::endgroup::", file=sys.stderr, flush=True)


LOG_HIGHLIGHT_RULE = "*" * 88


def compact_log_value(
    value: object,
    *,
    fallback: str = "(untitled)",
    limit: int = 200,
) -> str:
    """Collapse untrusted text into one bounded, terminal-safe log value."""
    compact = " ".join(strip_ansi(str(value or "")).split())
    if not compact:
        return fallback
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def log_highlight(
    target_logger: logging.Logger,
    message: str,
    *args: object,
) -> None:
    """Surround one important log record with an easy-to-scan rule."""
    # stacklevel: tag the records with the caller's file, not this helper's.
    target_logger.info(LOG_HIGHLIGHT_RULE, stacklevel=2)
    target_logger.info(message, *args, stacklevel=2)
    target_logger.info(LOG_HIGHLIGHT_RULE, stacklevel=2)
