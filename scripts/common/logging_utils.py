"""Shared logging setup for workflow entry points.

What makes an Actions log readable for an operator:

* one short line per decision, with warnings and errors coloured so they
  stand out, and the few lines that matter most (what the run is about,
  what happened to each item) highlighted;
* nothing they did not come for: no per-request HTTP chatter from client
  libraries, no timestamps the runner already shows, and debug detail only
  when a run is re-run with "Enable debug logging";
* a run's plan and verdict in an asterisk box, so they are found at a glance;
* the run's outcome pinned as an annotation at the top of the run page.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from scripts.common.text_utils import strip_ansi

LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(tag)s] %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"

_ANNOTATION = {logging.ERROR: "error", logging.WARNING: "warning"}

# Client libraries log every HTTP request at DEBUG. That is never what an
# operator opens the log for, so they are held at WARNING even in debug runs.
# PyGithub is held at INFO instead: that is where it reports rate-limit
# backoffs, the usual reason a run stalls.
_QUIET_LOGGERS = (
    "urllib3", "requests", "botocore", "boto3", "s3transfer",
    "httpx", "httpcore", "anthropic", "asyncio", "filelock",
)

HIGHLIGHT_MARKER = "▶ "
_RESET = "\x1b[0m"
_DIM = "90"
_LEVEL_COLOUR = {
    logging.DEBUG: _DIM,
    logging.WARNING: "33",
    logging.ERROR: "31",
    logging.CRITICAL: "31",
}
_HIGHLIGHT_COLOUR = {"notice": "1;36", "success": "1;32"}


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


class _ConsoleFormatter(_TagFormatter):
    """The workflow log line, with colour and highlights.

    In Actions the runner already timestamps every line and INFO is the
    normal case, so a line is just ``[tag] message``; only DEBUG, WARNING and
    ERROR carry their level. Outside Actions the full ``LOG_FORMAT`` is kept.
    Colour covers the first line only: Actions resets styling per line, and
    a traceback reads better plain.
    """

    def __init__(self, *, actions: bool, colour: bool) -> None:
        super().__init__(LOG_FORMAT, LOG_DATE_FORMAT)
        self._actions = actions
        self._colour = colour

    def formatMessage(self, record: logging.LogRecord) -> str:
        if getattr(record, "highlight", None):
            record.message = HIGHLIGHT_MARKER + record.message
        return super().formatMessage(record)

    def format(self, record: logging.LogRecord) -> str:
        if not self._actions:
            text = super().format(record)
            head, sep, tail = text.partition("\n")
            return self._paint(record, head) + sep + tail

        super().format(record)  # sets record.tag, record.message and exc_text
        level = "" if record.levelno == logging.INFO else f"{record.levelname} "
        tag = f"[{getattr(record, 'tag', '')}]"
        if self._colour:
            tag = f"\x1b[{_DIM}m{tag}{_RESET}"
        text = f"{tag} {self._paint(record, level + record.message)}"
        if record.exc_text:
            text += "\n" + record.exc_text
        if record.stack_info:
            text += "\n" + self.formatStack(record.stack_info)
        return text

    def _paint(self, record: logging.LogRecord, text: str) -> str:
        style = getattr(record, "highlight", None)
        if not self._colour:
            return text
        if style:
            code = {
                logging.WARNING: "1;33", logging.ERROR: "1;31", logging.CRITICAL: "1;31",
            }.get(record.levelno, _HIGHLIGHT_COLOUR.get(style, "1"))
        else:
            code = _LEVEL_COLOUR.get(record.levelno, "")
        return f"\x1b[{code}m{text}{_RESET}" if code else text


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


def _use_colour(actions: bool) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return actions or sys.stderr.isatty()


def configure_logging(*, verbose: bool = False) -> None:
    """Configure the one log format every workflow entry point uses.

    DEBUG records appear with ``verbose`` or in an Actions re-run with debug
    logging enabled; client-library chatter stays at WARNING regardless.
    """
    actions = _in_actions()
    handler = logging.StreamHandler()
    handler.setFormatter(_ConsoleFormatter(actions=actions, colour=_use_colour(actions)))
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
    # stacklevel: tag the record with the caller's file, not this helper's.
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
    style: str = "notice",
) -> None:
    """Log one line an operator should not miss, marked and in bold colour.

    ``style`` picks the colour of an INFO line: ``notice`` (cyan) for
    context such as what the run is working on, ``success`` (green) for a
    good result. WARNING and ERROR lines take their level's colour.
    """
    # stacklevel: tag the record with the caller's file, not this helper's.
    target_logger.log(level, message, *args, stacklevel=2, extra={"highlight": style})


BOX_RULE = "*" * 88


def log_box(
    target_logger: logging.Logger,
    title: str,
    lines: list[str],
    *,
    level: int = logging.INFO,
) -> None:
    """Log *title* and *lines* inside an asterisk box: the run's plan or verdict.

    Each line is its own record, so every line keeps its tag and colour. Pass
    untrusted values through :func:`compact_log_value` first.
    """
    # stacklevel: tag the records with the caller's file, not this helper's.
    target_logger.log(level, BOX_RULE, stacklevel=2)
    target_logger.log(level, "%s", title, stacklevel=2, extra={"highlight": "notice"})
    for line in lines:
        target_logger.log(level, "%s", line, stacklevel=2)
    target_logger.log(level, BOX_RULE, stacklevel=2)
