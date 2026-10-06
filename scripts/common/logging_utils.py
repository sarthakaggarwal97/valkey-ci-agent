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
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from scripts.common.text_utils import strip_ansi

LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(tag)s] %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"

_ANNOTATION = {logging.ERROR: "error", logging.WARNING: "warning"}
_group_open = False


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
        return super().format(record)


def configure_logging(*, verbose: bool = False) -> None:
    """Configure the one log format every workflow entry point uses."""
    handler = logging.StreamHandler()
    handler.setFormatter(_TagFormatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=[handler])


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
