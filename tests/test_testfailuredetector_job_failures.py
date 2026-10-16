"""Tests for Daily job failures the failures artifact does not record."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.common.valkey_ci import FILE_TIMEOUT_TEST_NAME
from scripts.test_failure_detector import issue_renderer
from scripts.test_failure_detector.job_failures import (
    UNBRANCHED,
    JobFailure,
    find_unrecorded_failures,
    merge_failures,
    normalize_job_name,
    split_by_branch,
)
from scripts.test_failure_detector.parse_failures import JobReference, UniqueFailure


def _job(job_id, name, conclusion="failure", step="test"):
    return SimpleNamespace(id=job_id, name=name, conclusion=conclusion, html_url=f"https://job/{job_id}",
                           steps=[SimpleNamespace(name="make", conclusion="success"),
                                  SimpleNamespace(name=step, conclusion=conclusion)])


def _find(jobs, logs, recorded=None, ignored=()):
    client = MagicMock()
    client.download_job_log.side_effect = lambda _repo, job_id: logs.get(job_id, "")
    result = find_unrecorded_failures(client, "valkey-io/valkey", recorded or {}, jobs=jobs, ignored_jobs=ignored)
    return result


_SUMMARY = (
    "2026-09-22T00:56:01.1692807Z Test Summary: 5821 passed, 1 failed\n"
    "2026-09-22T00:56:01.1692908Z !!! WARNING The following tests failed:\n"
    "2026-09-22T00:56:01.1692920Z \x1b[31m*** [TIMEOUT]: Throttling tears down in tests/integration/throttle-repl.tcl\x1b[0m\n"
)


def test_a_timeout_in_the_log_becomes_a_test_failure():
    tests, jobs = _find([_job(1, "test-ubuntu-32bit")], {1: _SUMMARY})
    assert jobs == []
    assert [(t.test_name, t.test_file) for t in tests] == [
        ("Throttling tears down", "tests/integration/throttle-repl.tcl")]
    assert tests[0].error == "[TIMEOUT]: Throttling tears down in tests/integration/throttle-repl.tcl"
    assert [ref.job for ref in tests[0].jobs] == ["test-ubuntu-32bit"]


@pytest.mark.parametrize("state", [
    "pid:74389", "port 21581", "ASSIGNED: sock55d0e3c0b890 (unit/type/list)", "SLEEPING, no more units to assign",
])
def test_a_client_timeout_gets_a_stable_name_for_its_file(state):
    """Client states from test_helper.tcl change every run; keying on them would open a new issue daily."""
    log = f"*** [TIMEOUT]: {state} in tests/integration/repl-compression.tcl\n"
    tests, _jobs = _find([_job(1, "test-valgrind-test (integration-type)")], {1: log})
    assert (tests[0].test_name, tests[0].test_file) == (FILE_TIMEOUT_TEST_NAME, "tests/integration/repl-compression.tcl")


def test_a_hung_test_keeps_its_own_name():
    log = "*** [TIMEOUT]: Throttling tears down in tests/integration/throttle-repl.tcl\n"
    tests, _jobs = _find([_job(1, "test-ubuntu-32bit")], {1: log})
    assert tests[0].test_name == "Throttling tears down"


def test_a_failure_without_a_test_becomes_a_job_failure():
    log = "*** [err]: Valgrind error: ==11258== Memcheck, a memory error detector\n"
    tests, jobs = _find([_job(1, "test-valgrind-test (unit)")], {1: log})
    assert tests == []
    assert jobs == [JobFailure(job="test-valgrind-test (unit)", url="https://job/1", step="test",
                               summary=("[err]: Valgrind error: ==11258== Memcheck, a memory error detector",),
                               jobs=[JobReference("test-valgrind-test (unit)", "https://job/1")])]


def test_a_failure_with_no_summary_at_all_is_a_job_failure():
    _tests, jobs = _find([_job(1, "test-freebsd", step="make")], {1: "error: implicit declaration\n"})
    assert [(j.job, j.step, j.summary) for j in jobs] == [("test-freebsd", "make", ())]


def test_successful_and_ignored_jobs_are_skipped():
    jobs = [_job(2, "test-ubuntu-jemalloc", conclusion="success"), _job(3, "consolidate-test-failures")]
    tests, failures = _find(jobs, {2: _SUMMARY, 3: _SUMMARY}, ignored=("consolidate-test-failures*",))
    assert (tests, failures) == ([], [])


_RECORDED = {"test-sanitizer-undefined-clang": {"valkey": [{"test_name": "x", "test_file": "tests/unit/x.tcl"}]}}


def test_a_recorded_job_still_reports_a_failure_the_artifact_does_not_hold():
    """One assertion in the artifact does not explain a timeout in the same job."""
    tests, failures = _find([_job(1, "test-sanitizer-undefined (clang)")], {1: _SUMMARY}, _RECORDED)
    assert [(t.test_name, t.test_file) for t in tests] == [("Throttling tears down", "tests/integration/throttle-repl.tcl")]
    assert failures == []


def test_a_recorded_job_reports_an_unattributed_summary_line():
    log = "*** [err]: x in tests/unit/x.tcl\n*** [err]: Sanitizer error: runtime error: shift exponent\n"
    tests, failures = _find([_job(1, "test-sanitizer-undefined (clang)")], {1: log}, _RECORDED)
    assert [(t.test_name, t.test_file) for t in tests] == [("x", "tests/unit/x.tcl")]  # merged with the artifact later
    assert [f.summary for f in failures] == [("[err]: Sanitizer error: runtime error: shift exponent",)]


def test_a_recorded_job_without_a_summary_is_explained_by_the_artifact():
    """--fastfail exits before the summary: the recorded assertion is the failure."""
    tests, failures = _find([_job(1, "test-sanitizer-undefined (clang)")], {1: "(Fast fail: test will exit now)\n"},
                            _RECORDED)
    assert (tests, failures) == ([], [])


def test_an_empty_artifact_entry_does_not_count_as_recorded():
    tests, _ = _find([_job(1, "test-ubuntu-32bit")], {1: _SUMMARY}, {"test-ubuntu-32bit": {"valkey": []}})
    assert len(tests) == 1


def test_normalized_job_names_match_artifact_keys():
    assert normalize_job_name("test-valgrind-test (unit)") == normalize_job_name("test-valgrind-test-unit")


def test_merge_adds_new_environments_to_an_existing_failure():
    recorded = [UniqueFailure("t", "tests/a.tcl", "e", [JobReference("job-a")])]
    extra = [UniqueFailure("t", "tests/a.tcl", "e2", [JobReference("job-b")]),
             UniqueFailure("u", "tests/b.tcl", "e3", [JobReference("job-c")])]
    merged = merge_failures(recorded, extra)
    assert [ref.job for ref in merged[0].jobs] == ["job-a", "job-b"]
    assert [(f.test_name, f.test_file) for f in merged] == [("t", "tests/a.tcl"), ("u", "tests/b.tcl")]


def test_job_failure_issue_carries_a_parseable_job_marker():
    failure = JobFailure(job="test-valgrind-test (unit)", url="https://job/1", step="test",
                         summary=("[err]: Valgrind error",))
    content = issue_renderer.job_renderer_for(failure).render("<!-- marker -->", 1)
    assert content.title == "[JOB-FAILURE] test-valgrind-test (unit) in Daily"
    assert content.labels == (issue_renderer.LABEL_NAME,)
    assert issue_renderer.parse_job_marker(content.body) == "test-valgrind-test (unit)"
    assert "[err]: Valgrind error" in content.body
    assert issue_renderer.parse_job_marker("no marker here") == ""
    # Distinct jobs never share an issue.
    other = JobFailure(job="test-valgrind-test (cluster)", url="", step="")
    assert issue_renderer.job_fingerprint_for(failure) != issue_renderer.job_fingerprint_for(other)



def test_matrix_twins_failing_the_same_way_are_one_failure():
    """Daily runs each Valgrind leg twice; one leak must not become two issues."""
    def leak(pid):
        return f"*** [err]: Valgrind error: =={pid}== Memcheck, a memory error detector\n"

    jobs = [_job(1, "test-valgrind-test (unit)"), _job(2, "test-valgrind-no-malloc-usable-size-test (unit)"),
            _job(3, "test-valgrind-test (cluster)")]
    _tests, failures = _find(jobs, {1: leak(35021), 2: leak(34715), 3: leak(22231)})
    assert [[ref.job for ref in f.jobs] for f in failures] == [
        ["test-valgrind-test (unit)", "test-valgrind-no-malloc-usable-size-test (unit)"],
        ["test-valgrind-test (cluster)"],
    ]
    assert len({issue_renderer.job_fingerprint_for(f) for f in failures}) == 2


def test_failures_without_a_summary_are_never_merged():
    _tests, failures = _find([_job(1, "test-freebsd", step="make"), _job(2, "test-s390x", step="make")],
                             {1: "boom\n", 2: "boom\n"})
    assert [f.job for f in failures] == ["test-freebsd", "test-s390x"]
    assert len({issue_renderer.job_fingerprint_for(f) for f in failures}) == 2


@pytest.mark.parametrize("step", ["Install EPEL", "Set up job", "Run actions/checkout@de0f", "Upload test failures",
                                  "Initialize containers", "testprep"])
def test_a_setup_step_failure_without_a_summary_is_not_reported(step):
    """A mirror or runner outage is not the repository's failure."""
    _tests, failures = _find([_job(1, "test-almalinux9-tls-module-no-tls", step=step)],
                             {1: "Error: Failed to download metadata for repo 'extras'\n"})
    assert failures == []


def test_a_setup_step_failure_that_printed_a_summary_is_still_reported():
    _tests, failures = _find([_job(1, "test-ubuntu-jemalloc", step="Install gtest")],
                             {1: "*** [err]: something the tests printed\n"})
    assert [f.job for f in failures] == ["test-ubuntu-jemalloc"]


def test_a_merged_failure_lists_every_job_on_its_issue():
    failure = JobFailure(job="a (unit)", url="https://job/1", step="test", summary=("[err]: x",),
                         jobs=[JobReference("a (unit)", "https://job/1"),
                               JobReference("b (unit)", "https://job/2")])
    content = issue_renderer.job_renderer_for(failure).render("<!-- m -->", 1)
    assert "`a (unit)`: [job log](https://job/1)" in content.body and "`b (unit)`: [job log](https://job/2)" in content.body
    assert "[`b (unit)`](https://job/2)" in content.comment
    assert issue_renderer.parse_job_marker(content.body) == "a (unit)"


# --- Weekly: one failure, and one issue, per release branch -------------------

WEEKLY_JOB = "run-daily-for-release-branches (8.1) / test-valgrind-test (unit)"


def test_failures_split_into_one_per_release_branch():
    refs = [
        JobReference("run-daily-for-release-branches (7.2) / test-a", "u1"),
        JobReference("run-daily-for-release-branches (8.1) / test-a", "u2"),
        JobReference("run-daily-for-release-branches (7.2) / test-b", "u3"),
    ]
    tests = split_by_branch([UniqueFailure("t", "tests/unit/a.tcl", jobs=list(refs))])
    assert [(f.branch, [r.url for r in f.jobs]) for f in tests] == [("7.2", ["u1", "u3"]), ("8.1", ["u2"])]

    jobs = split_by_branch([JobFailure(job=refs[0].job, url="u1", jobs=list(refs[:2]))])
    assert [(f.branch, f.job, f.url) for f in jobs] == [("7.2", refs[0].job, "u1"), ("8.1", refs[1].job, "u2")]


def test_weekly_issues_are_separate_from_daily_and_per_branch():
    daily = UniqueFailure("t", "tests/unit/a.tcl")
    weekly = UniqueFailure("t", "tests/unit/a.tcl", branch="8.1")
    other = UniqueFailure("t", "tests/unit/a.tcl", branch="7.2")

    fingerprints = {issue_renderer.fingerprint_for(f) for f in (daily, weekly, other)}
    assert len(fingerprints) == 3
    assert issue_renderer.title_for(daily) == "[TEST-FAILURE] t in tests/unit/a.tcl"
    assert issue_renderer.title_for(weekly) == "[TEST-FAILURE] t in tests/unit/a.tcl (8.1)"
    body = issue_renderer.renderer_for(weekly).render("<!-- m -->", 1).body
    assert "- Release branch: `8.1` (Weekly)" in body

    job = JobFailure(job=WEEKLY_JOB, url="u", branch="8.1")
    assert issue_renderer.job_title_for(job) == f"[JOB-FAILURE] {WEEKLY_JOB} in Weekly"
    assert issue_renderer.job_fingerprint_for(job) != issue_renderer.job_fingerprint_for(
        JobFailure(job=WEEKLY_JOB, url="u")
    )


def test_daily_fingerprints_are_unchanged():
    # Existing Daily issues must keep matching.
    failure = UniqueFailure("t", "tests/unit/a.tcl")
    from scripts.common.incidents import compute_fingerprint

    assert issue_renderer.fingerprint_for(failure) == compute_fingerprint(
        namespace=(issue_renderer.MARKER_NAMESPACE, "t", "tests/unit/a.tcl"), shapes=(),
    )


def test_a_weekly_job_outside_the_branch_calls_stays_a_weekly_failure():
    job = JobFailure(job="determine-release-branches", url="u",
                     jobs=[JobReference("determine-release-branches", "u")])

    (split,) = split_by_branch([job])

    assert split.branch == UNBRANCHED
    assert issue_renderer.job_title_for(split) == "[JOB-FAILURE] determine-release-branches in Weekly"


def test_a_weekly_failure_without_jobs_is_not_filed_under_daily():
    (split,) = split_by_branch([UniqueFailure("t", "tests/unit/a.tcl")])

    assert split.branch == UNBRANCHED
