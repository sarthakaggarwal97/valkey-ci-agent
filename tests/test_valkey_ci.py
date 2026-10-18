from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.common.valkey_ci import (
    FILE_TIMEOUT_TEST_NAME,
    Source,
    failed_tests_in_log,
    parse_result_line,
    source_for_run,
)


@pytest.mark.parametrize(("event", "path", "expected"), [
    ("schedule", ".github/workflows/daily.yml", Source.DAILY),
    ("schedule", ".github/workflows/weekly.yml", None),
    ("pull_request", ".github/workflows/daily.yml", None),
    ("schedule", ".github/workflows/ci.yml", None),
])
def test_a_run_is_daily_only_when_scheduled(event, path, expected):
    assert source_for_run(SimpleNamespace(event=event, path=path)) is expected


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
