"""What the bots know about Valkey's CI: its scheduled workflow and the
result lines its Tcl test runner prints.

``runtest`` prints ``[ok]: <name> (<ms> ms)`` for a passing test and
``[err]: <name> in tests/<file>.tcl`` (or ``[TIMEOUT]``) for a failing one,
again prefixed ``*** `` in its closing summary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from scripts.common.text_utils import strip_ansi


class Source(str, Enum):
    DAILY = "daily"

    @property
    def workflow_file(self) -> str:
        return f"{self.value}.yml"

    @property
    def label(self) -> str:
        return self.value.capitalize()


def source_for_run(run: Any, *, scheduled_only: bool = True) -> Source | None:
    """The source ``run`` executed, by its workflow file, or None for any other.

    ``scheduled_only`` also requires a scheduled run, as a fix does; the
    detector reads a manually dispatched run of the same workflow the same way.
    """
    if scheduled_only and str(getattr(run, "event", "") or "") != "schedule":
        return None
    path = str(getattr(run, "path", "") or "")
    for source in Source:
        if path == f".github/workflows/{source.workflow_file}":
            return source
    return None


# --- runtest result lines -----------------------------------------------------

_LOG_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z ")
_RESULT_LINE_RE = re.compile(r"\[(?P<status>ok|err|TIMEOUT|exception)\]: (?P<data>.+)$")
_TEST_IN_FILE_RE = re.compile(r"^(?P<name>.+) in (?P<file>tests/\S+\.tcl)$")
_OK_RE = re.compile(r"^(?P<name>.+) \(\d+ ms\)$")
# A test client that times out outside a test is reported by its state, not a
# test name (test_helper.tcl: "pid:74389", "port 21581", "ASSIGNED: sock5f0
# (unit/x)", "SLEEPING, ..."). Those change every run, so the failure is keyed
# by this stable name for its file instead.
_CLIENT_STATE_RE = re.compile(r"^(?:pid:\d+|port \d+|ASSIGNED: .*|SLEEPING\b.*)$")
FILE_TIMEOUT_TEST_NAME = "test client timed out"
# What a failing runtest run prints; a build break or a missing Tcl does not.
FAILURE_MARKERS = ("[err]:", "[TIMEOUT]:", "[exception]:", "Valgrind error", "Sanitizer error")


@dataclass(frozen=True)
class ResultLine:
    """One ``[status]: ...`` line: ``name`` and ``file`` when it names a test."""

    status: str
    name: str = ""
    file: str = ""


def strip_log_prefix(line: str) -> str:
    """``line`` without its Actions timestamp, colours, or surrounding space."""
    return _LOG_TIMESTAMP_RE.sub("", strip_ansi(line)).strip()


def test_in_file(data: str) -> tuple[str, str] | None:
    """``(name, file)`` for ``<name> in tests/<file>.tcl``, client states folded."""
    match = _TEST_IN_FILE_RE.match(data)
    if match is None:
        return None
    name = match.group("name")
    if _CLIENT_STATE_RE.fullmatch(name):
        name = FILE_TIMEOUT_TEST_NAME
    return name, match.group("file")


def parse_result_line(line: str) -> ResultLine | None:
    """The runtest result a (possibly timestamped) log line reports, or None."""
    match = _RESULT_LINE_RE.search(strip_log_prefix(line))
    if match is None:
        return None
    status, data = match.group("status"), match.group("data").strip()
    if status == "ok":
        passed = _OK_RE.match(data)
        return ResultLine(status, name=passed.group("name") if passed else data)
    test = test_in_file(data)
    return ResultLine(status, *(test or ("", "")))


def failed_tests_in_log(log: str) -> list[tuple[str, str]]:
    """``(test name, tests/... file)`` for each test a log shows failing, in order."""
    found: list[tuple[str, str]] = []
    for line in (log or "").splitlines():
        result = parse_result_line(line)
        if result is None or result.status not in ("err", "TIMEOUT") or not result.file:
            continue
        if (result.name, result.file) not in found:
            found.append((result.name, result.file))
    return found
