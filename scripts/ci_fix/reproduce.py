"""Reproduce a failing Valkey test before anyone tries to fix it.

A fix is only evidence of anything if the failure was seen first. Code (not
the agent) builds the reproduction command from what the CI job ran: inside
the failed job itself (``scripts/ci_fix/job.sh``), the job's own ``make`` and
``runtest`` commands (:class:`JobCommands`); anywhere else, plain ``make`` and
``runtest``. For a Tcl test, ``runtest`` runs only the failing tests
(``--single <unit> --only <name>``) many times in one invocation
(``--loops N --fastfail``). With several test clients the loop copies run in
parallel, which is the load that exposes most timing flakes. For a C unit
test (Google Test), the unit-test binary runs only the failing tests
(``--gtest_filter``) as many times (``--gtest_repeat``).

A flaky failure may not show up in a few loops on an idle runner, so a second
attempt runs the same loops while every core is busy. A failure that still
does not reproduce is reported as such: the fix can still be prepared from
the log, but it cannot be shown to fix anything, and the report says so.
"""

from __future__ import annotations

import re
import shlex
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable

from scripts.ci_fix.ci_job import JobCommands
from scripts.ci_fix.models import FailedTest
from scripts.common.valkey_ci import FAILURE_MARKERS, FILE_TIMEOUT_TEST_NAME, ResultLine, parse_result_line

# Valkey's Google Test binary, and the make target that builds it with the server.
UNIT_TEST_BINARY = "./src/unit/valkey-unit-gtests"
UNIT_TEST_BUILD = "make all-with-unit-tests"

# Loops per reproduction attempt, and with the fix applied. Valgrind is ~20x
# slower and runs one client at a time, as the Daily job does.
REPRO_LOOPS = 20
VERIFY_LOOPS = 40
VALGRIND_REPRO_LOOPS = 2
VALGRIND_VERIFY_LOOPS = 4
# A failure seen on both of two plain runs. It may still be a frequent flake,
# so the fix must pass enough loops that a no-op would not.
DETERMINISTIC_VERIFY_LOOPS = 10
# Stands for the loop count in ``ReproPlan.test``; no test name can hold a NUL.
LOOPS = "\0loops\0"
# Do not start a looped attempt with less time than this.
_MIN_ATTEMPT_S = 3 * 60

# The lines of a run's output that say what failed: the failure lines, and
# the Tcl stack frames that name the test an exception happened in. ``[ok]``
# lines say which tests ran at all.
_FAILURE_LINE_RE = re.compile("|".join(map(re.escape, FAILURE_MARKERS)) + r"|\btest [\"{]")
_OK_LINE_RE = re.compile(r"\[ok\]:|\[       OK \]")
_TEST_FRAME_RE = re.compile(r"\btest [\"{]")
_MAX_FAILURE_LINES = 200
_MAX_OK_LINES = 2_000
# runtest's no-progress watchdog (default 20 minutes, Daily's valgrind jobs
# 40). It must fire well inside the reproduction budget, or a hanging test is
# killed from outside before runtest can report it as ``[TIMEOUT]``. One test,
# even under valgrind, makes progress well within it.
_RUNTEST_TIMEOUT_S = 600

# Busy-loop every core around the test command, and stop the loops
# afterwards whatever the command's exit status. The command runs in a
# subshell on lines of its own, so nothing in it can skip the cleanup.
# One line, like every command a report quotes: a Markdown code span holds no
# newline, and a maintainer can paste this one as it stands.
_CONTENTION = (
    'cores="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)"; '
    'for _ in $(seq "$cores"); do (while :; do :; done) & done; '
    "( @COMMAND@ ); status=$?; "
    "{ kill $(jobs -p); wait; } 2>/dev/null; "
    "exit $status"
)


class ReproStatus(str, Enum):
    REPRODUCED = "reproduced"
    NOT_REPRODUCED = "not-reproduced"
    # The command could not run the test (a build break, a missing tool).
    UNRUNNABLE = "unrunnable"


@dataclass(frozen=True)
class ReproPlan:
    """How to build and run the failing tests the way their CI job did."""

    build: str
    test: str  # with a ``LOOPS`` placeholder
    valgrind: bool = False
    notes: tuple[str, ...] = ()
    tests: tuple[FailedTest, ...] = ()

    def command(self, loops: int, *, under_load: bool = False) -> str:
        """The build and test command; ``under_load`` busies every core around it."""
        command = f"{self.build} && {self.test.replace(LOOPS, str(loops))}"
        return _CONTENTION.replace("@COMMAND@", command) if under_load else command

    @property
    def repro_loops(self) -> int:
        return VALGRIND_REPRO_LOOPS if self.valgrind else REPRO_LOOPS

    def verify_loops(self, reproduced_on_first_loop: bool) -> int:
        if self.valgrind:
            return VALGRIND_VERIFY_LOOPS
        return DETERMINISTIC_VERIFY_LOOPS if reproduced_on_first_loop else VERIFY_LOOPS

    def failed(self, result: TestRun) -> bool:
        """Whether ``result`` shows one of the planned tests failing.

        A build break, or a harness error outside the tests, is a failed
        command but not a failed test.
        """
        return not result.passed and shows_failure_of(result.report, self.tests)

    def ran(self, result: TestRun) -> bool:
        """Whether ``result`` shows the planned tests running at all."""
        return ran_tests(result.report, self.tests)


@dataclass(frozen=True)
class Reproduction:
    status: ReproStatus
    command: str = ""
    output: str = ""
    attempts: tuple[str, ...] = ()
    # Why an unrunnable command could not run, when code knows.
    reason: str = ""
    # The failure showed up on both of two plain runs, before any loop.
    deterministic: bool = False
    # It showed up only with every core busy, so the fix is verified that way.
    under_load: bool = False

    @property
    def reproduced(self) -> bool:
        return self.status is ReproStatus.REPRODUCED

    def describe(self) -> str:
        if self.status is ReproStatus.REPRODUCED:
            kind = "on every run" if self.deterministic else "intermittently"
            return f"Reproduced {kind} with `{self.command}` ({'; '.join(self.attempts)})."
        if self.status is ReproStatus.NOT_REPRODUCED:
            return f"Did not reproduce with `{self.command}` ({'; '.join(self.attempts)})."
        reason = f" ({self.reason})" if self.reason else ""
        return f"Could not run the test on this runner: `{self.command}`{reason}."


@dataclass(frozen=True)
class TestRun:
    """One command's result: its exit status, an excerpt, and its ``failure_report``."""

    __test__ = False  # not a pytest test class

    passed: bool
    output: str
    report: str = ""
    # The command ran out of time and was killed, so it says nothing.
    timed_out: bool = False


def failure_report(output: str) -> str:
    """The lines of ``output`` that say which tests failed and how, then which passed."""
    failures: list[str] = []
    passed: list[str] = []
    for line in output.splitlines():
        if _FAILURE_LINE_RE.search(line):
            failures.append(line.strip())
        elif _OK_LINE_RE.search(line):
            passed.append(line.strip())
    return "\n".join(failures[:_MAX_FAILURE_LINES] + passed[:_MAX_OK_LINES])


def ran_tests(report: str, tests: tuple[FailedTest, ...]) -> bool:
    """Whether ``report`` shows every named test in ``tests`` running.

    ``--only`` selects nothing when a test was renamed or tagged out, and
    runtest then passes without running it: that is no evidence at all.
    """
    results = [result for result in map(parse_result_line, report.splitlines()) if result]
    finished = [result for result in results if result.status in ("ok", "err")]
    # An ``[ok]`` line does not name its file, so a client timeout's file ran
    # when anything did; every named test must still show up itself.
    return all(
        any(test.whole_file or _is(result, test) for result in finished) for test in tests
    )


def _is(result: ResultLine, test: FailedTest) -> bool:
    """Whether a result line is about exactly ``test``, not a renamed or sibling one.

    A test client that timed out outside any named test is that: a timeout in
    its file, not any other test failing there.
    """
    if test.whole_file:
        return result.name == FILE_TIMEOUT_TEST_NAME and result.file == test.file
    return result.name == test.name and result.file in ("", test.file)


def shows_failure_of(report: str, tests: tuple[FailedTest, ...]) -> bool:
    """Whether ``report`` shows one of ``tests`` failing, not something else.

    A server that cannot start, or a test client that loses its connection,
    raises an ``[exception]`` outside any test: that is the harness failing,
    not a reproduction. A valgrind or sanitizer report is the failure itself.
    """
    for line in report.splitlines():
        if "Valgrind error" in line or "Sanitizer error" in line:
            return True
        result = parse_result_line(line)
        if result and result.status in ("err", "TIMEOUT") and any(_is(result, test) for test in tests):
            return True
    if "[exception]:" in report:
        # Only the Tcl stack frame of the test an exception happened in names
        # it; an ``[ok]`` line from an earlier loop does not.
        frames = [line for line in report.splitlines() if _TEST_FRAME_RE.search(line)]
        return any(
            not test.whole_file
            and any(f'test "{test.name}"' in line or f"test {{{test.name}}}" in line for line in frames)
            for test in tests
        )
    return False


def plan_for(
    repo_dir: str,
    tests: tuple[FailedTest, ...],
    *,
    scheduled: bool,
    job: JobCommands | None = None,
) -> ReproPlan | None:
    """The plan that rebuilds and runs ``tests`` the way their CI ``job`` did, or None.

    Tests in the main Tcl suite (sentinel and the legacy cluster suite use
    other harnesses) and C unit tests are planned; None when none is, or the
    checkout has no ``runtest``. Without the job's own commands, plain
    ``make`` and ``runtest`` stand in for them; scheduled runs pass
    ``--accurate``. The module API tests run through ``runtest-moduleapi``,
    which builds the test modules; a plan runs one suite, the first test's.
    """
    unit_tests = tuple(test for test in tests if test.is_unit_test)
    tcl_tests = tuple(test for test in tests if _in_main_suite(test.file))
    if unit_tests and (not tcl_tests or tests[0].is_unit_test):
        return _unit_test_plan(repo_dir, unit_tests, job=job)
    tests = tcl_tests
    if not tests or not (Path(repo_dir) / "runtest").is_file():
        return None
    notes: list[str] = []
    default = job is None
    if job is None:
        accurate = " --accurate" if scheduled else ""
        job = JobCommands(
            build="make",
            runtest=f"./runtest --dump-logs{accurate}",
            moduleapi=f"./runtest-moduleapi --dump-logs{accurate}",
        )
    moduleapi = bool(job.moduleapi) and _in_moduleapi(tests[0].file)
    tests = tuple(test for test in tests if _in_moduleapi(test.file) == moduleapi)
    base = job.moduleapi if moduleapi else job.runtest
    # runtest keeps the last --timeout. The job's own watchdog (Daily's
    # valgrind jobs pass 2400) is kept when it is shorter; a longer one would
    # let a hang outlive the reproduction budget unreported.
    options = ["--timeout", str(min(_job_timeout(base) or _RUNTEST_TIMEOUT_S, _RUNTEST_TIMEOUT_S))]
    if "--clients" not in base.split() and any(_runs_solo(repo_dir, test) for test in tests):
        # Under --loops, runtest runs a run_solo block inline, so with several
        # clients its copies would run side by side: a race of the bot's own
        # making, which no fix could pass.
        options += ["--clients", "1"]
        notes.append("A planned file uses run_solo, so its loops run one at a time.")
    selection = _selection(repo_dir, tests, notes)
    if default:
        notes.append("The failed job could not host the fix, so plain make and runtest stand in for it.")
    command = " ".join([base, *selection, "--loops", LOOPS, *options])
    return ReproPlan(build=_one_line_build(job.build), test=command, valgrind="--valgrind" in base.split(),
                     notes=tuple(notes), tests=tests)


def _one_line_build(build: str) -> str:
    """The job's build as one line that stops at its first failure, on every core.

    One line because every report quotes the command it ran.
    """
    return " && ".join(
        _MAKE_LINE_RE.sub(r'\1 -j"$(nproc)"', line.strip())
        for line in _CONTINUATION_RE.sub(" ", build).splitlines() if line.strip()
    )


def _unit_test_plan(repo_dir: str, tests: tuple[FailedTest, ...], *, job: JobCommands | None) -> ReproPlan | None:
    """The plan that builds the unit-test binary like the job and repeats only ``tests``.

    None without ``src/unit`` in the checkout (a branch before Google Test,
    whose own harness runs the whole suite). The job's build must produce the
    binary: one that does not gets ``make all-with-unit-tests`` after it.
    """
    if not (Path(repo_dir) / "src" / "unit").is_dir():
        return None
    notes: list[str] = []
    build = job.build if job is not None else UNIT_TEST_BUILD
    if "unit-tests" not in build:
        build = f"{build}\n{UNIT_TEST_BUILD}"
        notes.append("The job's build does not build the unit tests, so `make all-with-unit-tests` runs after it.")
    if job is None:
        notes.append("The failed job could not host the fix, so a plain build stands in for it.")
    names = ":".join(test.name for test in tests)
    command = f"{UNIT_TEST_BINARY} --gtest_filter={shlex.quote(names)} --gtest_repeat={LOOPS}"
    return ReproPlan(build=_one_line_build(build), test=command, notes=tuple(notes), tests=tests)


_MAKE_LINE_RE = re.compile(r"^(\s*make)(?=\s|$)(?!.*\s-j)", re.MULTILINE)
# A line the job's own script continued on the next one.
_CONTINUATION_RE = re.compile(r"\s*\\\n\s*")


def _selection(repo_dir: str, tests: tuple[FailedTest, ...], notes: list[str]) -> list[str]:
    """The runtest options that run only ``tests``; why it runs more goes to ``notes``."""
    selection: list[str] = []
    for unit in dict.fromkeys(test.unit for test in tests):
        selection += ["--single", shlex.quote(unit)]
    if any(test.whole_file for test in tests):
        notes.append("A test client timed out outside any named test; the whole files run.")
    elif any(test.name.startswith("/") for test in tests):
        # ``--only`` would read a leading "/" as a regexp; run the files whole.
        notes.append("A test name starts with '/', which runtest reads as a regexp; the whole files run.")
    else:
        for test in tests:
            selection += ["--only", shlex.quote(test.name)]
    # Valkey 7.2's runtest has no --fastfail; its loops then run to the end,
    # which costs time but still fails the run.
    if _runtest_supports(repo_dir, "--fastfail"):
        selection += ["--fastfail"]
    else:
        notes.append("This branch's runtest has no --fastfail, so every loop runs to the end.")
    return selection


def reproduce(
    plan: ReproPlan,
    run: Callable[[str, int], TestRun],
    *,
    deadline: float,
) -> Reproduction:
    """Run ``plan`` on the unfixed code: once, then looped, then under load.

    ``run(command, timeout)`` runs one command. Attempts stop at ``deadline``
    (a ``time.monotonic()`` value); the build in the first run is the slow part.
    """
    attempts: list[str] = []
    plain = plan.command(1)
    first = run(plain, _remaining(deadline))
    if plan.failed(first):
        # One failure may be a frequent flake; a second plain run tells.
        second = run(plain, _remaining(deadline))
        both = plan.failed(second)
        return Reproduction(
            ReproStatus.REPRODUCED, command=plain, output=first.output, deterministic=both,
            attempts=("failed on both of two runs",) if both else ("failed on the first run, passed on the second",),
        )
    if not first.passed:
        return Reproduction(ReproStatus.UNRUNNABLE, command=plain, output=first.output,
                            reason=_wrong_argument(first.output))
    if not plan.ran(first):
        return Reproduction(ReproStatus.UNRUNNABLE, command=plain, output=first.output,
                            reason="runtest passed without running the failing test")
    attempts.append("passed once")

    for label, under_load in (
        (f"{plan.repro_loops} loops", False),
        (f"{plan.repro_loops} loops with every core busy", True),
    ):
        if _remaining(deadline) < _MIN_ATTEMPT_S:
            attempts.append(f"no time left for {label}")
            break
        command = plan.command(plan.repro_loops, under_load=under_load)
        result = run(command, _remaining(deadline))
        if plan.failed(result):
            attempts.append(f"failed within {label}")
            return Reproduction(ReproStatus.REPRODUCED, command=command, output=result.output,
                                attempts=tuple(attempts), under_load=under_load)
        if not result.passed:
            # A timeout or a crash of the harness, not a test failure.
            attempts.append(f"{label} did not finish: {result.output.strip()[-200:]}")
            continue
        attempts.append(f"passed {label}")
    return Reproduction(ReproStatus.NOT_REPRODUCED, command=plan.command(plan.repro_loops),
                        attempts=tuple(attempts))


def _job_timeout(command: str) -> int:
    """The ``--timeout`` seconds a runtest command passes, or 0."""
    words = command.split()
    for index, word in enumerate(words[:-1]):
        if word == "--timeout" and words[index + 1].isdigit():
            return int(words[index + 1])
    return 0


def _wrong_argument(output: str) -> str:
    """runtest's complaint about an option this branch does not have, or ""."""
    for line in output.splitlines():
        if "Wrong argument" in line:
            return line.strip()[:200]
    return ""


def _remaining(deadline: float) -> int:
    return max(1, int(deadline - time.monotonic()))


def _runs_solo(repo_dir: str, test: FailedTest) -> bool:
    try:
        return "run_solo" in (Path(repo_dir) / test.file).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False


def _runtest_supports(repo_dir: str, option: str) -> bool:
    try:
        helper = (Path(repo_dir) / "tests" / "test_helper.tcl").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return f"{{{option}}}" in helper


def _in_moduleapi(test_file: str) -> bool:
    return test_file.startswith("tests/unit/moduleapi/")


def _in_main_suite(test_file: str) -> bool:
    return (
        test_file.startswith(("tests/unit/", "tests/integration/"))
        and test_file.endswith(".tcl")
        and ".." not in Path(test_file).parts
    )
