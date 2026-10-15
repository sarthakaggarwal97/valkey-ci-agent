"""Shared logging setup for workflow entry points.

Every entry point logs ``<time> <LEVEL> <message>``. The few records an
operator scans for (a sweep's plan and verdict, each attempted PR) are framed
by asterisk rules. Client libraries are held at WARNING so their per-request
chatter never reaches the log, debug detail appears only in a run re-run with
"Enable debug logging", and the run's outcome is pinned as an annotation at
the top of the run page.
"""

from __future__ import annotations

import logging
import os
import sys

from scripts.common.text_utils import strip_ansi

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
LOG_HIGHLIGHT_RULE = "*" * 88

_ANNOTATION = {logging.ERROR: "error", logging.WARNING: "warning"}

# Client libraries log every HTTP request at DEBUG. That is never what an
# operator opens the log for, so they are held at WARNING even in debug runs.
# PyGithub is held at INFO instead: that is where it reports rate-limit
# backoffs, the usual reason a run stalls.
_QUIET_LOGGERS = (
    "urllib3", "requests", "botocore", "boto3", "s3transfer",
    "httpx", "httpcore", "anthropic", "asyncio", "filelock",
)


class _SkipAnnotated(logging.Filter):
    """Drop the plain copy of a line that is also printed as an annotation.

    In Actions the ``::notice::`` / ``::warning::`` command itself shows up
    in the log, highlighted, so printing the same sentence just above it
    only doubles it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return not (getattr(record, "annotated", False) and _in_actions())


def _debug_requested(verbose: bool) -> bool:
    # RUNNER_DEBUG is set when a run is re-run with "Enable debug logging".
    return verbose or os.environ.get("RUNNER_DEBUG") == "1"


def configure_logging(*, verbose: bool = False) -> None:
    """Configure the one log format every workflow entry point uses.

    DEBUG records appear with ``verbose`` or in an Actions re-run with debug
    logging enabled; client-library chatter stays at WARNING regardless.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(_SkipAnnotated())
    logging.basicConfig(
        level=logging.DEBUG if _debug_requested(verbose) else logging.INFO,
        handlers=[handler],
    )
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger("github").setLevel(logging.INFO)


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
    # stacklevel: report the caller's location, not this helper's.
    # In Actions the annotation below is the visible copy (_SkipAnnotated).
    target_logger.log(level, "%s", text, stacklevel=2, extra={"annotated": True})
    annotate(level, text)


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
    level: int = logging.INFO,
) -> None:
    """Frame one important log record with asterisk rules."""
    # stacklevel: report the caller's location, not this helper's.
    target_logger.log(level, LOG_HIGHLIGHT_RULE, stacklevel=2)
    target_logger.log(level, message, *args, stacklevel=2)
    target_logger.log(level, LOG_HIGHLIGHT_RULE, stacklevel=2)


def log_box(
    target_logger: logging.Logger,
    title: str,
    lines: list[str],
    *,
    level: int = logging.INFO,
) -> None:
    """Log *title* and *lines* between asterisk rules: a run's plan or verdict.

    One record per line, so each carries its own timestamp. Pass untrusted
    values through :func:`compact_log_value` first.
    """
    # stacklevel: report the caller's location, not this helper's.
    target_logger.log(level, LOG_HIGHLIGHT_RULE, stacklevel=2)
    for line in (title, *lines):
        target_logger.log(level, "%s", line, stacklevel=2)
    target_logger.log(level, LOG_HIGHLIGHT_RULE, stacklevel=2)
