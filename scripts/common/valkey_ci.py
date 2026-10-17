"""What the bots know about the result lines Valkey's Tcl test runner prints.

``runtest`` prints ``[ok]: <name> (<ms> ms)`` for a passing test and
``[err]: <name> in tests/<file>.tcl`` (or ``[TIMEOUT]``) for a failing one,
again prefixed ``*** `` in its closing summary.
"""

from __future__ import annotations

import re

from scripts.common.text_utils import strip_ansi

_LOG_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z ")
_TEST_IN_FILE_RE = re.compile(r"^(?P<name>.+) in (?P<file>tests/\S+\.tcl)$")
# A test client that times out outside a test is reported by its state, not a
# test name (test_helper.tcl: "pid:74389", "port 21581", "ASSIGNED: sock5f0
# (unit/x)", "SLEEPING, ..."). Those change every run, so the failure is keyed
# by this stable name for its file instead.
_CLIENT_STATE_RE = re.compile(r"^(?:pid:\d+|port \d+|ASSIGNED: .*|SLEEPING\b.*)$")
FILE_TIMEOUT_TEST_NAME = "test client timed out"


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
