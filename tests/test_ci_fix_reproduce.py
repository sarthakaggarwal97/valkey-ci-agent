from __future__ import annotations

import time
from pathlib import Path

import pytest

from scripts.ci_fix.models import FailedTest
from scripts.ci_fix.reproduce import (
    LOOPS,
    REPRO_LOOPS,
    JobCommands,
    ReproPlan,
    ReproStatus,
    TestRun,
    failure_report,
    plan_for,
    ran_tests,
    reproduce,
    shows_failure_of,
)

TEST = FailedTest("INCREX BYFLOAT overflow preserves existing TTL", "tests/unit/type/incr.tcl")


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / "runtest").write_text("#!/bin/sh\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_helper.tcl").write_text("} elseif {$opt eq {--fastfail}} {\n", encoding="utf-8")
    return str(tmp_path)


def test_a_plain_job_builds_once_and_loops_only_the_failing_test(checkout):
    plan = plan_for(checkout, "test-ubuntu-jemalloc", (TEST,), scheduled=True)

    assert plan.build == 'make -j"$(nproc)"'
    assert plan.command(20) == (
        'make -j"$(nproc)" && ./runtest --single unit/type/incr '
        "--only 'INCREX BYFLOAT overflow preserves existing TTL' "
        "--fastfail --loops 20 --dump-logs --timeout 600 --accurate"
    )


@pytest.mark.parametrize(("job", "build", "flags"), [
    ("run-daily-for-release-branches (8.1) / test-valgrind-test (unit)",
     'make -j"$(nproc)" valgrind', "--valgrind --no-latency --clients 1 --timeout 600"),
    ("test-valgrind-no-malloc-usable-size-test (unit)",
     'make -j"$(nproc)" valgrind CFLAGS=-DNO_MALLOC_USABLE_SIZE', "--clients 1 --timeout 600"),
    ("test-sanitizer-address (clang)", 'make -j"$(nproc)" OPT=-O3 SANITIZER=address', "--accurate"),
    ("test-sanitizer-undefined (gcc)",
     'make -j"$(nproc)" OPT=-O3 SANITIZER=undefined LUA_DEBUG=yes', "--accurate"),
    ("test-ubuntu-tls-io-threads",
     'make -j"$(nproc)" BUILD_TLS=yes && ./utils/gen-test-certs.sh', "--tls --io-threads --accurate"),
    ("test-ubuntu-tls-no-tls", 'make -j"$(nproc)" BUILD_TLS=yes && ./utils/gen-test-certs.sh', "--accurate"),
    ("test-ubuntu-libc-malloc", 'make -j"$(nproc)" MALLOC=libc', "--accurate"),
])
def test_the_build_and_flags_follow_the_job_name(checkout, job, build, flags):
    plan = plan_for(checkout, job, (TEST,), scheduled=True)

    assert plan.build == build
    assert plan.test.endswith(flags)
    assert ("--tls" in plan.test) == ("tls" in job and "no-tls" not in job)
    assert plan.valgrind == ("valgrind" in job)


def test_a_pr_run_does_not_use_accurate(checkout):
    assert "--accurate" not in plan_for(checkout, "test-ubuntu-jemalloc", (TEST,), scheduled=False).test


def test_another_platform_uses_the_closest_build_and_says_so(checkout):
    plan = plan_for(checkout, "test-macos-latest", (TEST,), scheduled=True)

    assert plan.build == 'make -j"$(nproc)"'
    assert "macos" in plan.notes[0]


def test_a_timed_out_client_without_a_test_name_runs_its_whole_file(checkout):
    plan = plan_for(checkout, "test-ubuntu-jemalloc", (FailedTest("", TEST.file),), scheduled=False)

    assert "--only" not in plan.test and "--single unit/type/incr" in plan.test
    assert "whole files run" in plan.notes[0]


@pytest.mark.parametrize("tests", [
    (),
    (FailedTest("failover", "tests/sentinel/tests/05-manual.tcl"),),  # another harness
    (FailedTest("x", "tests/unit/../../../../tmp/e.tcl"),),  # leaves the suite
])
def test_only_main_suite_tests_are_planned(checkout, tests):
    assert plan_for(checkout, "test-ubuntu-jemalloc", tests, scheduled=True) is None

    plan = plan_for(checkout, "test-ubuntu-jemalloc", (TEST, *tests), scheduled=True)
    assert plan.tests == (TEST,) and "sentinel" not in plan.test and "../" not in plan.test


def test_there_is_no_plan_without_runtest(tmp_path):
    assert plan_for(str(tmp_path / "missing"), "test-ubuntu-jemalloc", (TEST,), scheduled=True) is None


# --- reproduce ---------------------------------------------------------------

PLAN = ReproPlan(build="make", test=f"./runtest --loops {LOOPS}", tests=(TEST,))
PASS = TestRun(passed=True, output="all passed", report=f"[ok]: {TEST.name} (5 ms)")
FAIL = TestRun(passed=False, output="", report=f"[err]: {TEST.name} in {TEST.file}")
BROKEN = TestRun(passed=False, output="tclsh: command not found")


def _runner(*results):
    calls = []
    queue = iter(results)

    def run(command, timeout):
        calls.append(command)
        return next(queue)

    run.calls = calls
    return run


def _deadline(seconds=3600):
    return time.monotonic() + seconds


def test_a_failure_on_two_plain_runs_is_deterministic():
    run = _runner(FAIL, FAIL)
    result = reproduce(PLAN, run, deadline=_deadline())

    assert result.status is ReproStatus.REPRODUCED and result.deterministic
    assert run.calls == ["make && ./runtest --loops 1"] * 2
    assert PLAN.verify_loops(result.deterministic) == 10


def test_one_failure_then_a_pass_is_a_flake():
    result = reproduce(PLAN, _runner(FAIL, PASS), deadline=_deadline())

    assert result.reproduced and not result.deterministic
    assert PLAN.verify_loops(result.deterministic) == 40


def test_a_flaky_failure_reproduces_under_loops():
    run = _runner(PASS, FAIL)
    result = reproduce(PLAN, run, deadline=_deadline())

    assert result.status is ReproStatus.REPRODUCED and not result.deterministic
    assert run.calls[1] == f"make && ./runtest --loops {REPRO_LOOPS}"
    assert f"failed within {REPRO_LOOPS} loops" in result.describe()
    assert "intermittently" in result.describe()


def test_a_timing_flake_reproduces_with_every_core_busy():
    run = _runner(PASS, PASS, FAIL)
    result = reproduce(PLAN, run, deadline=_deadline())

    assert result.reproduced
    assert "while :; do :; done" in run.calls[2] and run.calls[1] in run.calls[2]
    assert "with every core busy" in result.describe()


def test_a_failure_that_never_shows_is_not_reproduced():
    result = reproduce(PLAN, _runner(PASS, PASS, PASS), deadline=_deadline())

    assert result.status is ReproStatus.NOT_REPRODUCED
    assert result.describe().startswith("Did not reproduce")


def test_a_build_break_is_not_a_reproduction():
    result = reproduce(PLAN, _runner(BROKEN), deadline=_deadline())

    assert result.status is ReproStatus.UNRUNNABLE
    assert "tclsh" in result.output


def test_looped_attempts_stop_at_the_deadline():
    run = _runner(PASS)
    result = reproduce(PLAN, run, deadline=_deadline(60))

    assert result.status is ReproStatus.NOT_REPRODUCED
    assert len(run.calls) == 1 and "no time left" in result.describe()




HARNESS_EXCEPTION = """\
# Failed opening Unix socket: unix socket path too long (289), must be under 104
[exception]: Executing test client: error flushing "sock820d74890": broken pipe.
    invoked from within
"send_data_packet $::test_server_fd done "$__testname""
"""


def test_a_harness_exception_outside_the_test_is_not_a_reproduction():
    report = failure_report(HARNESS_EXCEPTION)

    assert not shows_failure_of(report, (TEST,))
    run = _runner(TestRun(False, HARNESS_EXCEPTION, report))
    assert reproduce(PLAN, run, deadline=_deadline()).status is ReproStatus.UNRUNNABLE


def test_an_exception_inside_the_test_is_a_reproduction():
    output = (
        "[exception]: Executing test client: I/O error reading reply.\n"
        f'"test "{TEST.name}" {{\n'
    )
    assert shows_failure_of(failure_report(output), (TEST,))


def test_only_the_planned_tests_failing_counts():
    other = "[err]: Some other test in tests/unit/type/incr.tcl"
    mine = f"[err]: {TEST.name} in {TEST.file}"
    assert not shows_failure_of(failure_report(other), (TEST,))
    assert shows_failure_of(failure_report(mine), (TEST,))
    assert shows_failure_of(failure_report(other), (FailedTest("", TEST.file),))
    assert shows_failure_of("*** [err]: Valgrind error in tests/unit/type/incr.tcl", (TEST,))
    assert not shows_failure_of(failure_report("make: *** [Makefile:6: all] Error 2"), (TEST,))


def test_a_pass_that_ran_no_failing_test_is_not_evidence():
    ok_other = "[ok]: Some other test (3 ms)"

    result = reproduce(PLAN, _runner(TestRun(True, "", ok_other)), deadline=_deadline())

    assert result.status is ReproStatus.UNRUNNABLE
    assert "without running the failing test" in result.describe()
    assert ran_tests(f"[ok]: {TEST.name} (12 ms)", (TEST,))
    assert ran_tests(ok_other, (FailedTest("", TEST.file),))
    assert not ran_tests("", (FailedTest("", TEST.file),))


def test_runtest_reports_a_hang_well_inside_the_reproduction_budget(checkout):
    from scripts.ci_fix.engine import REPRO_BUDGET_S

    for job in ("test-ubuntu-jemalloc", "test-valgrind-test (unit)"):
        test = plan_for(checkout, job, (TEST,), scheduled=True).test
        watchdog = int(test.split("--timeout ", 1)[1].split()[0])
        assert watchdog < REPRO_BUDGET_S - 5 * 60, job


def test_a_renamed_or_sibling_test_is_not_the_failing_test():
    renamed = f"[ok]: {TEST.name} (deflaked) (12 ms)"
    sibling = f"[err]: {TEST.name} again in {TEST.file}"

    assert not ran_tests(renamed, (TEST,))
    assert not shows_failure_of(sibling, (TEST,))
    assert shows_failure_of(f"[TIMEOUT]: {TEST.name} in {TEST.file}", (TEST,))


def test_a_branch_without_fastfail_runs_its_loops_to_the_end(checkout):
    (Path(checkout) / "tests" / "test_helper.tcl").write_text("} elseif {$opt eq {--loops}} {\n", encoding="utf-8")

    plan = plan_for(checkout, "test-ubuntu-jemalloc", (TEST,), scheduled=False)

    assert "--fastfail" not in plan.test
    assert "no --fastfail" in " ".join(plan.notes)


def test_a_rejected_option_is_reported_as_the_reason():
    broken = TestRun(False, "Wrong argument: --fastfail\n")

    result = reproduce(PLAN, _runner(broken), deadline=_deadline())

    assert result.status is ReproStatus.UNRUNNABLE
    assert "Wrong argument: --fastfail" in result.describe()


def test_an_ok_line_from_an_earlier_loop_does_not_make_a_harness_exception_a_reproduction():
    report = failure_report(f"[ok]: {TEST.name} (12 ms)\n" + HARNESS_EXCEPTION)

    assert not shows_failure_of(report, (TEST,))


def test_a_flake_seen_only_under_load_is_verified_under_load():
    result = reproduce(PLAN, _runner(PASS, PASS, FAIL), deadline=_deadline())

    assert result.under_load
    assert "while :; do :; done" in PLAN.command(40, under_load=result.under_load)


def test_a_test_name_with_braces_is_quoted_not_formatted(checkout):
    braced = FailedTest("HSET with {tag} key and {loops}", TEST.file)

    command = plan_for(checkout, "test-ubuntu-jemalloc", (braced,), scheduled=False).command(7)

    assert "--only 'HSET with {tag} key and {loops}'" in command
    assert "--loops 7" in command


def test_a_file_with_run_solo_loops_one_client_at_a_time(checkout):
    (Path(checkout) / "tests" / "unit" / "type").mkdir(parents=True)
    (Path(checkout) / TEST.file).write_text('run_solo {defrag} {\n    test "x" {}\n}\n', encoding="utf-8")

    plan = plan_for(checkout, "test-ubuntu-jemalloc", (TEST,), scheduled=False)

    assert plan.test.endswith("--clients 1")
    assert "run_solo" in " ".join(plan.notes)


def test_io_threads_jobs_use_accurate_even_on_a_pr(checkout):
    assert "--accurate" in plan_for(checkout, "test-ubuntu-io-threads", (TEST,), scheduled=False).test


def test_valgrind_verification_stays_short_even_after_a_deterministic_failure():
    valgrind = ReproPlan(build="make valgrind", test=f"./runtest --loops {LOOPS}", valgrind=True, tests=(TEST,))

    assert valgrind.verify_loops(True) == valgrind.verify_loops(False) == 4


# --- inside the copied job: its own commands ------------------------------------

JOB = JobCommands(
    build="export CC='clang'\nmake all-with-unit-tests SANITIZER=address \\\n  USE_LIBBACKTRACE=yes",
    runtest="./runtest --accurate --verbose --dump-logs --tls",
    moduleapi="CFLAGS='-Werror' ./runtest-moduleapi --verbose --dump-logs",
)


def test_in_the_job_its_own_build_and_runtest_run_the_failing_test(checkout):
    plan = plan_for(checkout, "test-sanitizer-address (clang)", (TEST,), scheduled=True, job=JOB)

    # One line: every report quotes the command, and a code span holds no newline.
    assert plan.build == (
        "export CC='clang' && make -j\"$(nproc)\" all-with-unit-tests SANITIZER=address USE_LIBBACKTRACE=yes"
    )
    assert plan.test.replace(LOOPS, "20") == (
        "./runtest --accurate --verbose --dump-logs --tls --single unit/type/incr "
        "--only 'INCREX BYFLOAT overflow preserves existing TTL' --fastfail --loops 20 --timeout 600"
    )
    assert not plan.valgrind and plan.notes == ()


def test_a_jobs_multi_line_build_becomes_one_command(checkout):
    """A report quotes the command in a code span, which holds no newline."""
    job = JobCommands(
        build="apt-get update && apt-get install -y make gcc-13\nupdate-alternatives --install x\nmake CC=gcc",
        runtest="./runtest --dump-logs",
    )

    plan = plan_for(checkout, "test-rpm-distros (fedora)", (TEST,), scheduled=True, job=job)

    assert plan.build == (
        "apt-get update && apt-get install -y make gcc-13 && update-alternatives --install x"
        " && make -j\"$(nproc)\" CC=gcc"
    )
    assert "\n" not in plan.command(3) and "\n" not in plan.command(3, under_load=True)


def test_in_the_job_a_module_api_test_runs_through_runtest_moduleapi(checkout):
    module = FailedTest("blocking", "tests/unit/moduleapi/blockonkeys.tcl")

    plan = plan_for(checkout, "test-ubuntu-jemalloc", (module, TEST), scheduled=True, job=JOB)

    assert plan.test.startswith("CFLAGS='-Werror' ./runtest-moduleapi --verbose --dump-logs --single unit/moduleapi/")
    assert plan.tests == (module,)


def test_in_a_valgrind_job_its_own_options_are_kept(checkout):
    job = JobCommands(build="make valgrind", runtest="./runtest --valgrind --no-latency --clients 1 --dump-logs")
    (Path(checkout) / "tests" / "unit" / "type").mkdir(parents=True)
    (Path(checkout) / TEST.file).write_text("run_solo {x} {}\n", encoding="utf-8")

    plan = plan_for(checkout, "test-valgrind-test (unit)", (TEST,), scheduled=True, job=job)

    assert plan.valgrind and plan.test.count("--clients") == 1


def test_the_job_commands_come_from_its_fix_step():
    env = {"CI_FIX_BUILD": "make", "CI_FIX_RUNTEST": "./runtest --dump-logs", "CI_FIX_RUNTEST_MODULEAPI": ""}

    assert JobCommands.from_env(env) == JobCommands(build="make", runtest="./runtest --dump-logs")
    assert JobCommands.from_env({}) is None
