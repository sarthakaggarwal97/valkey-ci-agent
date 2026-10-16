from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.common.valkey_ci import (
    FILE_TIMEOUT_TEST_NAME,
    Source,
    called_job_name,
    checked_out_sha,
    failed_tests_in_log,
    parse_result_line,
    source_for_run,
    source_for_workflow,
)
from scripts.common.valkey_ci import (
    tested_branch as branch_tested,
)

WEEKLY_JOB = "run-daily-for-release-branches (8.1) / test-valgrind-test (unit)"


@pytest.mark.parametrize(("event", "path", "expected"), [
    ("schedule", ".github/workflows/daily.yml", Source.DAILY),
    ("schedule", ".github/workflows/weekly.yml", Source.WEEKLY),
    ("pull_request", ".github/workflows/daily.yml", None),
    ("schedule", ".github/workflows/ci.yml", None),
])
def test_a_run_is_daily_or_weekly_only_when_scheduled(event, path, expected):
    assert source_for_run(SimpleNamespace(event=event, path=path)) is expected


def test_workflow_names_parse():
    assert source_for_workflow("weekly") is Source.WEEKLY
    assert source_for_workflow("daily.yml") is Source.DAILY
    with pytest.raises(ValueError):
        source_for_workflow("monthly")


def test_a_weekly_job_names_its_release_branch():
    run = SimpleNamespace(head_branch="unstable")
    assert branch_tested(Source.WEEKLY, run, WEEKLY_JOB) == "8.1"
    assert branch_tested(Source.WEEKLY, run, "test-valgrind-test (unit)") == ""
    assert branch_tested(Source.DAILY, run, "test-valgrind-test (unit)") == "unstable"
    assert called_job_name(WEEKLY_JOB) == "test-valgrind-test (unit)"
    assert called_job_name("test-ubuntu-jemalloc") == "test-ubuntu-jemalloc"


def test_the_checked_out_commit_is_read_from_the_job_log():
    sha = "7e6e5e1712bf0581913eef6724423d72f51ba6ce"
    other = "b" * 40
    log = (
        "2026-10-11T06:13:15.9495435Z Syncing repository: ianlancetaylor/libbacktrace\n"
        "2026-10-11T06:13:17.6919192Z [command]/usr/bin/git log -1 --format=%H\n"
        f"2026-10-11T06:13:17.6939064Z {other}\n"
        "2026-10-11T06:13:17.8244890Z Syncing repository: valkey-io/valkey\n"
        "2026-10-11T06:13:18.4317013Z [command]/usr/bin/git log -1 --format=%H\n"
        f"2026-10-11T06:13:18.4337903Z {sha}\n"
    )
    # Whatever the order, only the named repository's checkout counts.
    assert checked_out_sha(log, "valkey-io/valkey") == sha
    assert checked_out_sha(log, "ianlancetaylor/libbacktrace") == other
    assert checked_out_sha(log, "o/other") == ""
    assert checked_out_sha("no checkout here", "valkey-io/valkey") == ""


def test_failed_tests_are_read_from_a_job_log_once_each():
    log = (
        "2026-10-15T01:33:46.2219673Z [err]: INCREX BYFLOAT overflow [curr_val, 0] in tests/unit/type/incr.tcl\n"
        "Expected '-1' to be equal to '60'\n"
        "[TIMEOUT]: clients state report follows. in tests/unit/other.tcl\n"
        "[err]: pid:74389 in tests/unit/dump.tcl\n"
        "*** [err]: INCREX BYFLOAT overflow [curr_val, 0] in tests/unit/type/incr.tcl\n"
    )
    assert failed_tests_in_log(log) == [
        ("INCREX BYFLOAT overflow [curr_val, 0]", "tests/unit/type/incr.tcl"),
        ("clients state report follows.", "tests/unit/other.tcl"),
        # A client that timed out outside a test is keyed by its file, as the
        # detector files it.
        (FILE_TIMEOUT_TEST_NAME, "tests/unit/dump.tcl"),
    ]


@pytest.mark.parametrize(("line", "expected"), [
    ("2026-10-15T00:18:04.03Z [ok]: First server should be a replica (1061 ms)",
     ("ok", "First server should be a replica", "")),
    ("[err]: INCREX overflow [curr_val, 0] in tests/unit/type/incr.tcl",
     ("err", "INCREX overflow [curr_val, 0]", "tests/unit/type/incr.tcl")),
    ("*** [TIMEOUT]: slow test in tests/unit/a.tcl", ("TIMEOUT", "slow test", "tests/unit/a.tcl")),
    ("[exception]: Executing test client: broken pipe", ("exception", "", "")),
])
def test_runtest_result_lines_parse(line, expected):
    result = parse_result_line(line)
    assert (result.status, result.name, result.file) == expected
    assert parse_result_line("make: *** [all] Error 2") is None
