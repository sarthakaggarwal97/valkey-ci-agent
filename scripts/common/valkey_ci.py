"""What the bots know about Valkey's CI: its scheduled workflows, its job
names, and the result lines its Tcl test runner prints.

Daily runs ``daily.yml`` on one branch. Weekly runs ``weekly.yml``, which
calls ``daily.yml`` once per release branch inside the same run, so:

- every job name carries its caller and branch:
  ``run-daily-for-release-branches (8.1) / test-valgrind-test (unit)``;
- the run's own ``head_branch`` and ``head_sha`` are the scheduling branch
  (``unstable``), not what was tested;
- each called ``daily.yml`` uploads its own ``all-test-failures`` artifact
  under the same name, and each one merges every branch's per-job artifacts
  that exist at that moment, keyed by the bare job name. No artifact can be
  attributed to one branch, so Weekly failures are read from job logs only.

``runtest`` prints ``[ok]: <name> (<ms> ms)`` for a passing test and
``[err]: <name> in tests/<file>.tcl`` (or ``[TIMEOUT]``) for a failing one,
again prefixed ``*** `` in its closing summary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any

from scripts.common.text_utils import strip_ansi


class Source(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"

    @property
    def workflow_file(self) -> str:
        return f"{self.value}.yml"

    @property
    def label(self) -> str:
        return self.value.capitalize()

    @property
    def max_age(self) -> timedelta | None:
        """How old the newest completed run may be and still be this cycle's."""
        return timedelta(days=7) if self is Source.WEEKLY else None

    @property
    def artifact_is_attributable(self) -> bool:
        """Whether ``all-test-failures`` describes this run's jobs exactly."""
        return self is Source.DAILY


# ``<caller job> (<release branch>) / <called job>``. The branch is the
# weekly matrix value, always ``<major>.<minor>``.
_CALLED_JOB_RE = re.compile(r"^[^/()]+\((?P<branch>\d+\.\d+)\)\s*/\s*(?P<job>.+)$")


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


def tested_branch(source: Source, run: Any, job_name: str) -> str:
    """The branch a job of ``run`` checked out and tested, or ""."""
    if source is Source.WEEKLY:
        return release_branch(job_name)
    return str(getattr(run, "head_branch", "") or "")


def release_branch(job_name: str) -> str:
    """``8.1`` for a job Weekly ran for that release branch, else ""."""
    match = _CALLED_JOB_RE.match(job_name)
    return match.group("branch") if match else ""


def called_job_name(job_name: str) -> str:
    """``test-valgrind-test (unit)`` for a Weekly job; ``job_name`` otherwise."""
    match = _CALLED_JOB_RE.match(job_name)
    return match.group("job").strip() if match else job_name


# actions/checkout names the repository it syncs, then prints the commit it
# checked out after ``git log -1 --format=%H``.
_SYNCING_RE = re.compile(r"Syncing repository: (?P<repo>\S+)\s*$", re.MULTILINE)
_CHECKOUT_SHA_RE = re.compile(
    r"git log -1 --format=['\"]?%H['\"]?\s*\n(?:\S+Z )?(?P<sha>[0-9a-f]{40})\s*$",
    re.MULTILINE,
)


def checked_out_sha(job_log: str, repo_full_name: str) -> str:
    """The full SHA a job's ``actions/checkout`` of ``repo_full_name`` printed, or "".

    A job checks out other repositories too (Valkey's checks out
    libbacktrace), so the SHA must follow that repository's sync line.
    """
    log = job_log or ""
    for sync in _SYNCING_RE.finditer(log):
        if sync.group("repo").lower() != repo_full_name.lower():
            continue
        following = _SYNCING_RE.search(log, sync.end())
        section = log[sync.end():following.start() if following else len(log)]
        match = _CHECKOUT_SHA_RE.search(section)
        return match.group("sha") if match else ""
    return ""


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
