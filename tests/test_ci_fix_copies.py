from __future__ import annotations

import pytest

from scripts.ci_fix import copies
from scripts.ci_fix.models import FailedTest, Failure, FixPolicy, FixTarget, FixTask, TargetKind
from scripts.common.valkey_ci import Source

MANIFEST = {
    "9.0/ci": {
        "jobs": {"test-ubuntu-latest": ["test-ubuntu-latest"]},
        "fixable": {"test-ubuntu-latest": ""},
    },
    "unstable/daily": {
        "jobs": {
            "test-ubuntu-jemalloc": ["test-ubuntu-jemalloc"],
            "test-valgrind-test": ["test-valgrind-test (${{ matrix.shard }})"],
            "test-sanitizer-address": ["test-sanitizer-address"],
            "test-macos-latest": ["test-macos-latest"],
        },
        "targeted": ["test-valgrind-test"],
        "fixable": {"test-ubuntu-jemalloc": "", "test-valgrind-test": "", "test-sanitizer-address": "compiler"},
    },
}
TEST = FailedTest("INCREX BYFLOAT overflow", "tests/unit/type/incr.tcl")


def _task(job_name: str, *, source=Source.DAILY, kind=TargetKind.DRAFT_PR, repo="valkey-io/valkey") -> FixTask:
    failure = Failure(
        repo_full_name=repo, run_id=1, head_sha="f" * 40, job_id=2, job_name=job_name,
        issue_number=7, source=source, workflow="ci" if source is None else "",
    )
    target = FixTarget(kind=kind, repo_full_name=repo, expected_sha="b" * 40, base_branch="9.0")
    return FixTask(failure, target, FixPolicy.GENERAL)


@pytest.mark.parametrize(("name", "job"), [
    ("test-ubuntu-jemalloc", "test-ubuntu-jemalloc"),
    ("test-valgrind-test (unit)", "test-valgrind-test"),
    ("test-sanitizer-address (gcc)", "test-sanitizer-address"),  # GitHub's matrix suffix
    ("run-daily-for-release-branches (8.1) / test-ubuntu-jemalloc", "test-ubuntu-jemalloc"),  # Weekly
])
def test_a_failed_job_maps_to_the_copied_job_that_hosts_its_fix(name, job):
    copy = copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, name)

    assert copy is not None and copy.job == job


def test_a_job_without_a_fix_mode_or_a_copy_has_none():
    assert copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, "test-macos-latest") is None
    assert copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, "test-something-new") is None
    assert copies.copy_job_for(MANIFEST, "9.9", "daily", "test-ubuntu-jemalloc") is None


def test_a_scheduled_failure_is_fixed_in_unstables_daily_and_a_prs_in_its_base_branchs_workflow():
    assert copies.copy_for(MANIFEST, _task("test-ubuntu-jemalloc")).job == "test-ubuntu-jemalloc"
    pr = copies.copy_for(MANIFEST, _task("test-ubuntu-latest", source=None, kind=TargetKind.UPDATE_PR))
    assert (pr.branch, pr.workflow, pr.job) == ("9.0", "ci", "test-ubuntu-latest")


def test_a_suggestion_or_another_repositorys_failure_has_no_copy():
    assert copies.copy_for(MANIFEST, _task("test-ubuntu-latest", source=None, kind=TargetKind.SUGGEST)) is None
    assert copies.copy_for(MANIFEST, _task("test-ubuntu-jemalloc", repo="valkey-io/valkey-search")) is None


def test_the_copy_is_called_at_the_commit_the_fix_is_prepared_on():
    copy = copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, "test-ubuntu-jemalloc")

    assert copies.fix_inputs(copy, _task("test-ubuntu-jemalloc"), (TEST,)) == {
        "branch": "unstable", "workflow": "daily", "job": "test-ubuntu-jemalloc",
        "use_repo": "valkey-io/valkey", "use_git_ref": "b" * 40, "fix_matrix": "",
        "valgrind_test": "", "accurate": "true",
    }


def test_a_matrix_job_names_the_entry_that_failed():
    name = "run-daily-for-release-branches (8.1) / test-sanitizer-address (clang)"
    copy = copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, name)

    assert copies.fix_inputs(copy, _task(name), (TEST,))["fix_matrix"] == "clang"
    assert copies.fix_inputs(copy, _task("test-sanitizer-address"), (TEST,)) is None  # which entry is unknown


def test_a_targeted_valgrind_job_collapses_to_one_shard_or_is_not_used():
    copy = copies.copy_job_for(MANIFEST, *copies.SCHEDULED_WORKFLOW, "test-valgrind-test (unit)")

    assert copies.fix_inputs(copy, _task("test-valgrind-test (unit)"), (TEST,))["valgrind_test"] == "unit/type/incr"
    assert copies.fix_inputs(copy, _task("test-valgrind-test (unit)"), ()) is None


def test_every_committed_daily_copy_hosts_the_common_ubuntu_jobs():
    manifest = copies.load_manifest()

    for job in ("test-ubuntu-jemalloc", "test-sanitizer-address", "test-valgrind-test"):
        assert job in manifest["unstable/daily"]["fixable"], job
