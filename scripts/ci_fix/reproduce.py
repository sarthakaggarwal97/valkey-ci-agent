"""Reproduce a failing Valkey Tcl test before anyone tries to fix it.

A fix is only evidence of anything if the failure was seen first. For a Tcl
test, code (not the agent) builds the reproduction command from what the CI
job ran. Inside the copied job itself (``scripts/ci_fix/job.sh``) that is the
job's own ``make`` and ``runtest`` commands (:class:`JobCommands`). Anywhere
else it follows Valkey's own workflows (``.github/workflows/daily.yml``):

- the build flags come from the job name (``test-valgrind-*`` builds with
  ``make valgrind``, ``test-sanitizer-address`` with ``SANITIZER=address``,
  ``*-tls`` with ``BUILD_TLS=yes``, ...);
- ``runtest`` runs only the failing tests (``--single <unit> --only <name>``)
  many times in one invocation (``--loops N --fastfail``). With several test
  clients the loop copies run in parallel, which is the load that exposes
  most timing flakes. Scheduled runs pass ``--accurate``; PR runs do not.

A flaky failure may not show up in a few loops on an idle runner, so a second
attempt runs the same loops while every core is busy. A failure that still
does not reproduce is reported as such: the fix can still be prepared from
the log, but it cannot be shown to fix anything, and the report says so.
"""

from __future__ import annotations

import os
import re
import shlex
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from scripts.ci_fix.models import FailedTest
from scripts.common.valkey_ci import FAILURE_MARKERS, ResultLine, called_job_name, parse_result_line

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
_OK_LINE = "[ok]:"
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
class JobCommands:
    """The build and test commands of the CI job the fix is prepared in.

    The copied job's fix mode passes them (``scripts/valkey_workflows/normalize.py``):
    its ``make`` step, and its ``runtest`` and ``runtest-moduleapi`` lines
    without the job's own test selection.
    """

    build: str
    runtest: str
    moduleapi: str = ""

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> JobCommands | None:
        build, runtest = env.get("CI_FIX_BUILD", "").strip(), env.get("CI_FIX_RUNTEST", "").strip()
        if not build or not runtest:
            return None
        return cls(build=build, runtest=runtest, moduleapi=env.get("CI_FIX_RUNTEST_MODULEAPI", "").strip())


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
        elif _OK_LINE in line:
            passed.append(line.strip())
    return "\n".join(failures[:_MAX_FAILURE_LINES] + passed[:_MAX_OK_LINES])


def ran_tests(report: str, tests: tuple[FailedTest, ...]) -> bool:
    """Whether ``report`` shows every named test in ``tests`` running.

    ``--only`` selects nothing when a test was renamed or tagged out, and
    runtest then passes without running it: that is no evidence at all.
    """
    results = [result for result in map(parse_result_line, report.splitlines()) if result]
    finished = [result for result in results if result.status in ("ok", "err")]
    if any(test.whole_file for test in tests):
        return bool(finished)
    return all(any(_is(result, test) for result in finished) for test in tests)


def _is(result: ResultLine, test: FailedTest) -> bool:
    """Whether a result line is about exactly ``test``, not a renamed or sibling one."""
    if test.whole_file:
        return result.file == test.file
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
    job_name: str,
    tests: tuple[FailedTest, ...],
    *,
    scheduled: bool,
    job: JobCommands | None = None,
) -> ReproPlan | None:
    """The reproduction plan for ``tests`` in ``job_name``, or None.

    Only tests in the main Tcl suite are planned (sentinel and the legacy
    cluster suite use other harnesses); None when none is, or the checkout
    has no ``runtest``. With the ``job``'s own commands, they are used as
    they are; otherwise the build and options are derived from the job name.
    """
    tests = tuple(test for test in tests if _in_main_suite(test.file))
    if not tests or not (Path(repo_dir) / "runtest").is_file():
        return None
    if job is not None:
        return _job_plan(repo_dir, job, tests)

    name = called_job_name(job_name).lower()
    make_vars: list[str] = []
    runtest: list[str] = []
    notes: list[str] = []
    setup: list[str] = []
    valgrind = "valgrind" in name

    if "no-malloc-usable-size" in name:
        make_vars += ["MALLOC=libc", "CFLAGS=-DNO_MALLOC_USABLE_SIZE"] if not valgrind else ["CFLAGS=-DNO_MALLOC_USABLE_SIZE"]
    elif "libc-malloc" in name:
        make_vars.append("MALLOC=libc")
    if valgrind:
        make_target = "valgrind"
        runtest += ["--valgrind", "--no-latency", "--clients", "1"]
    else:
        make_target = ""
    runtest += ["--timeout", str(_RUNTEST_TIMEOUT_S)]
    if "sanitizer-address" in name or "force-defrag" in name:
        make_vars += ["OPT=-O3", "SANITIZER=address"]
        if "force-defrag" in name:
            make_vars += ["DEBUG_FORCE_DEFRAG=yes", "USE_JEMALLOC=no"]
    elif "sanitizer-undefined" in name:
        make_vars += ["OPT=-O3", "SANITIZER=undefined", "LUA_DEBUG=yes"]
    if "large-memory" in name:
        runtest += ["--large-memory", "--tags", "large-memory", "--clients", "1"]
    if "tls-module" in name:
        make_vars.append("BUILD_TLS=module")
        setup.append("./utils/gen-test-certs.sh")
        if "no-tls" not in name:
            runtest.append("--tls-module")
    elif "tls" in name:
        make_vars.append("BUILD_TLS=yes")
        setup.append("./utils/gen-test-certs.sh")
        if "no-tls" not in name:
            runtest.append("--tls")
    if "io-threads" in name:
        runtest.append("--io-threads")
    if "reply-schemas" in name:
        make_vars.append("SERVER_CFLAGS=-DLOG_REQ_RES")
        runtest += ["--log-req-res", "--no-latency", "--force-resp3"]
    if "32bit" in name:
        make_target = "32bit"
    if "macos" in name:
        runtest += ["--clients", "1", "--no-latency"]
    for platform in ("macos", "freebsd", "alpine", "almalinux", "centos", "fedora", "rpm-distros", "s390x", "arm"):
        if platform in name:
            notes.append(f"The job ran on {platform}; this runner is Ubuntu x86-64, so the closest build is used.")
            break
    # Scheduled runs pass --accurate; the io-threads jobs pass it on PRs too.
    if (scheduled or "io-threads" in name) and "valgrind" not in name:
        runtest.append("--accurate")
    if "--clients" not in runtest and any(_runs_solo(repo_dir, test) for test in tests):
        # Under --loops, runtest runs a run_solo block inline, so with several
        # clients its copies would run side by side: a race of the bot's own
        # making, which no fix could pass.
        runtest += ["--clients", "1"]
        notes.append("A planned file uses run_solo, so its loops run one at a time.")

    build = " ".join(
        ["make", "-j\"$(nproc)\"", *([make_target] if make_target else []), *map(shlex.quote, make_vars)]
    )
    if setup:
        build = " && ".join([build, *setup])
    selection = _selection(repo_dir, tests, notes)
    command = " ".join(["./runtest", *selection, "--loops", LOOPS, "--dump-logs", *runtest])
    return ReproPlan(build=build, test=command, valgrind=valgrind, notes=tuple(notes), tests=tests)


_MAKE_LINE_RE = re.compile(r"^(\s*make)(?=\s)(?!.*\s-j)", re.MULTILINE)
# A line the job's own script continued on the next one.
_CONTINUATION_RE = re.compile(r"\s*\\\n\s*")


def _job_plan(repo_dir: str, job: JobCommands, tests: tuple[FailedTest, ...]) -> ReproPlan | None:
    """The plan that rebuilds and runs ``tests`` with the job's own commands.

    The module API tests run through ``runtest-moduleapi``, which builds the
    test modules; a plan runs one suite, the first test's.
    """
    moduleapi = bool(job.moduleapi) and _in_moduleapi(tests[0].file)
    tests = tuple(test for test in tests if _in_moduleapi(test.file) == moduleapi)
    base = job.moduleapi if moduleapi else job.runtest
    notes: list[str] = []
    options = ["--timeout", str(_RUNTEST_TIMEOUT_S)]
    if "--clients" not in base.split() and any(_runs_solo(repo_dir, test) for test in tests):
        options += ["--clients", "1"]
        notes.append("A planned file uses run_solo, so its loops run one at a time.")
    selection = _selection(repo_dir, tests, notes)
    # The job's build as one line that stops at its first failure, on every
    # core. One line because every report quotes the command it ran.
    build = " && ".join(
        _MAKE_LINE_RE.sub(r'\1 -j"$(nproc)"', line.strip())
        for line in _CONTINUATION_RE.sub(" ", job.build).splitlines() if line.strip()
    )
    command = " ".join([base, *selection, "--loops", LOOPS, *options])
    return ReproPlan(build=build, test=command, valgrind="--valgrind" in base.split(),
                     notes=tuple(notes), tests=tests)


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
