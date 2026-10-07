"""Tests for verifying a fix by rerunning the failing job in the Daily workflow."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix.daily_verify import (
    DailyPlan,
    DailyResult,
    dispatch_daily,
    evaluate_daily_run,
    plan_daily_run,
)

# A trimmed copy of the shape of valkey's daily.yml: dispatch inputs, jobs
# gated by skipjobs tokens, steps gated by skiptests tokens.
_DAILY = """
on:
  workflow_dispatch:
    inputs:
      skipjobs:
        default: "valgrind,sanitizer,ubuntu,rpm-distros,freebsd"
      skiptests:
        default: "valkey,modules,sentinel,unittest"
      test_args:
        default: ""
      valgrind_test:
        default: ""
      use_repo:
        default: "valkey-io/valkey"
      use_git_ref:
        default: "unstable"
  schedule:
    - cron: "0 0 * * *"
jobs:
  test-ubuntu-jemalloc:
    if: (github.event_name == 'workflow_dispatch' || github.event_name == 'schedule') && !contains(github.event.inputs.skipjobs, 'ubuntu')
    runs-on: ubuntu-latest
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest ${{github.event.inputs.test_args}}
      - name: module api test
        if: true && !contains(github.event.inputs.skiptests, 'modules')
        run: ./runtest-moduleapi ${{github.event.inputs.test_args}}
  test-valgrind-test:
    name: test-valgrind-test (${{ matrix.shard }})
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'valgrind') && !contains(github.event.inputs.skiptests, 'valkey')
    strategy:
      matrix:
        shard: ${{ fromJSON(inputs.valgrind_test && '["targeted"]' || '["unit"]') }}
    runs-on: ubuntu-latest
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        env:
          VALGRIND_TEST: ${{ inputs.valgrind_test }}
        run: ./runtest --valgrind ${{github.event.inputs.test_args}}
  test-valgrind-misc:
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'valgrind') && !(contains(github.event.inputs.skiptests, 'modules') && contains(github.event.inputs.skiptests, 'unittest'))
    runs-on: ubuntu-latest
    steps:
      - name: module api test
        if: true && !contains(github.event.inputs.skiptests, 'modules')
        run: ./runtest-moduleapi --valgrind
      - name: unittest
        if: true && !contains(github.event.inputs.skiptests, 'unittest')
        run: make valgrind-unit
  test-rpm-distros:
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'rpm-distros')
    strategy:
      matrix:
        include:
          - name: test-almalinux8-jemalloc
            container: almalinux:8
    name: ${{ matrix.name }}
    runs-on: ubuntu-latest
    container: ${{ matrix.container }}
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest ${{github.event.inputs.test_args}}
  test-sanitizer-large-memory:
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'sanitizer') && !contains(github.event.inputs.skiptests, 'large-memory')
    runs-on: ubuntu-latest
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest --large-memory ${{github.event.inputs.test_args}}
  test-freebsd:
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'freebsd')
    runs-on: ubuntu-latest
    steps:
      - run: ./runtest --single unit/keyspace
  notify:
    if: github.event_name == 'schedule'
    runs-on: ubuntu-latest
    steps:
      - run: echo
"""


def _plan(job, test_file="", test_name="", loops=20):
    return plan_daily_run(_DAILY, job_name=job, test_file=test_file, test_name=test_name, loops=loops)


def test_a_test_failure_reruns_only_its_job_suite_and_file():
    plan = _plan("test-ubuntu-jemalloc", "tests/integration/rdb.tcl", "TTL expiration")
    assert isinstance(plan, DailyPlan)
    assert plan.inputs == {
        "skipjobs": "freebsd,rpm-distros,sanitizer,valgrind",
        "skiptests": "modules,sentinel,unittest",
        "test_args": "--single integration/rdb --loops 20 --fastfail",
    }
    assert (plan.job, plan.test_name) == ("test-ubuntu-jemalloc", "TTL expiration")


def test_a_matrix_named_container_job_keeps_its_display_name():
    plan = _plan("test-almalinux8-jemalloc", "tests/unit/type/list.tcl", "LPOS")
    assert isinstance(plan, DailyPlan)
    assert (plan.job, plan.job_id) == ("test-almalinux8-jemalloc", "test-rpm-distros")
    assert "rpm-distros" not in plan.inputs["skipjobs"]


def test_valgrind_uses_the_targeted_shard_with_fewer_loops():
    plan = _plan("test-valgrind-test (unit)", "tests/unit/type/zset.tcl", "ZADD", loops=50)
    assert isinstance(plan, DailyPlan)
    assert plan.inputs["valgrind_test"] == "unit/type/zset"
    assert plan.inputs["test_args"] == "--loops 5 --fastfail"
    # The shard is renamed to "targeted", so the job is matched by its key.
    assert (plan.job, plan.job_id) == ("", "test-valgrind-test")
    assert plan.describe() == "`test-valgrind-test (targeted)` with `--single unit/type/zset --loops 5 --fastfail`"


def test_a_job_that_cannot_select_a_test_reruns_whole():
    plan = _plan("test-freebsd", "tests/unit/keyspace.tcl", "KEYS")
    assert isinstance(plan, DailyPlan)
    # No test_args, and every suite the job does not gate is skipped (its own
    # test step has no gate, so nothing it runs is affected).
    assert (plan.inputs["test_args"], plan.inputs["skiptests"]) == ("", "modules,sentinel,unittest,valkey")
    # Judged by the job's result: not every harness prints "[ok]: <name>".
    assert plan.test_name == ""
    assert plan.describe() == "`test-freebsd` (whole job)"


def test_a_job_level_failure_reruns_the_whole_job():
    plan = _plan("test-ubuntu-jemalloc")
    assert isinstance(plan, DailyPlan)
    assert (plan.inputs["test_args"], plan.test_name) == ("", "")


@pytest.mark.parametrize("job, reason", [
    ("notify", "does not run when daily.yml is dispatched"),
    ("missing-job", "is not in daily.yml"),
])
def test_unplannable_jobs_are_refused(job, reason):
    assert reason in _plan(job, "tests/unit/x.tcl", "x")


def test_a_workflow_without_dispatch_inputs_is_refused():
    assert "no workflow_dispatch inputs" in plan_daily_run("jobs: {}\n", job_name="x")


def test_a_kept_token_inside_a_skipped_one_is_refused():
    """contains() is a substring match: keeping 'tls' while skipping 'tls-module' is impossible."""
    workflow = _DAILY.replace("'ubuntu')", "'tls')").replace(
        'default: "valgrind,sanitizer,ubuntu,rpm-distros,freebsd"', 'default: "tls-module,ubuntu"')
    assert "cannot be selected on its own" in plan_daily_run(workflow, job_name="test-ubuntu-jemalloc")


def test_dispatch_uses_the_run_id_api_and_pins_the_commit():
    requester = MagicMock()
    requester.requestJsonAndCheck.return_value = (
        {}, {"workflow_run_id": 42, "html_url": "https://github.com/o/r/actions/runs/42"})
    gh = MagicMock()
    gh.get_repo.return_value._requester = requester
    plan = DailyPlan(job="j", job_id="j", inputs={"skipjobs": "a", "skiptests": "", "test_args": ""})

    run_id, url = dispatch_daily(gh, "o/r", ref="unstable", plan=plan, sha="f" * 40)

    assert (run_id, url) == (42, "https://github.com/o/r/actions/runs/42")
    verb, path = requester.requestJsonAndCheck.call_args.args
    kwargs = requester.requestJsonAndCheck.call_args.kwargs
    assert (verb, path) == ("POST", "/repos/o/r/actions/workflows/daily.yml/dispatches")
    assert kwargs["input"] == {"ref": "unstable", "inputs": {
        "skipjobs": "a", "skiptests": "", "test_args": "", "use_repo": "o/r", "use_git_ref": "f" * 40}}
    assert kwargs["headers"] == {"X-GitHub-Api-Version": "2026-03-10"}


def test_dispatch_without_a_run_id_fails_loudly():
    gh = MagicMock()
    gh.get_repo.return_value._requester.requestJsonAndCheck.return_value = ({}, None)
    with pytest.raises(RuntimeError, match="no run id"):
        dispatch_daily(gh, "o/r", ref="unstable", plan=DailyPlan("j", "j", {}), sha="f" * 40)


# --- evaluation ----------------------------------------------------------------------

def _gh_with_jobs(*jobs, status="completed"):
    run = SimpleNamespace(html_url="https://run/1", status=status, jobs=lambda: list(jobs))
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow_run.return_value = run
    return gh


def _job(name="test-ubuntu-jemalloc", *, conclusion="success", status="completed"):
    return SimpleNamespace(id=7, name=name, status=status, conclusion=conclusion, html_url="https://job/7")


def _evaluate(log, *jobs, test_name="TTL expiration", job="test-ubuntu-jemalloc", job_id=None):
    client = MagicMock()
    client.download_job_log.return_value = log
    plan = DailyPlan(job=job, job_id=job_id or job, inputs={}, test_name=test_name)
    return evaluate_daily_run(_gh_with_jobs(*jobs), client, "o/r", 1, plan)


def test_a_green_job_that_ran_the_test_passes():
    log = "2026-10-01T00:00:00Z \x1b[32m[ok]\x1b[0m: TTL expiration (12 ms)\n" * 3
    result = _evaluate(log, _job())
    assert result is not None and result.verified
    assert (result.state, result.detail) == ("passed", "the test passed 3 time(s)")


def test_a_green_job_that_never_ran_the_test_proves_nothing():
    result = _evaluate("[ok]: Something else (1 ms)\n", _job())
    assert result is not None
    assert (result.state, result.verified) == ("not-run", False)


def test_a_skipped_test_is_reported_as_skipped_and_never_verifies():
    result = _evaluate("[skip]: TTL expiration\n", _job())
    assert result is not None
    assert (result.state, result.verified) == ("skipped", False)


def test_a_cancelled_job_is_cancelled_not_failed():
    result = _evaluate("[err]: TTL expiration in tests/integration/rdb.tcl\n", _job(conclusion="cancelled"))
    assert result is not None
    assert (result.state, result.verified, result.test_failures) == ("cancelled", False, 0)


def test_a_run_cancelled_before_the_job_started_is_cancelled():
    run = SimpleNamespace(html_url="https://run/1", status="completed", conclusion="cancelled", jobs=lambda: [])
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow_run.return_value = run
    plan = DailyPlan(job="j", job_id="j", inputs={})
    result = evaluate_daily_run(gh, MagicMock(), "o/r", 1, plan)
    assert result is not None and result.state == "cancelled"


def test_reproduction_needs_the_tests_own_failure_for_a_test_plan():
    test_plan = DailyPlan(job="j", job_id="j", inputs={}, test_name="TTL expiration")
    job_plan = DailyPlan(job="j", job_id="j", inputs={})
    other_reason = DailyResult("failed", "u", test_failures=0)
    own_failure = DailyResult("failed", "u", test_failures=2)
    assert not other_reason.reproduces(test_plan)
    assert own_failure.reproduces(test_plan)
    assert other_reason.reproduces(job_plan)
    assert not DailyResult("passed", "u").reproduces(job_plan)


def test_a_failed_job_counts_the_tests_own_failures():
    log = "[ok]: TTL expiration (1 ms)\n[err]: TTL expiration in tests/integration/rdb.tcl\n"
    result = _evaluate(log, _job(conclusion="failure"))
    assert result is not None
    assert (result.state, result.test_failures) == ("failed", 1)


def test_a_job_failing_for_another_reason_is_never_credited():
    result = _evaluate("[ok]: TTL expiration (1 ms)\n", _job(conclusion="failure"))
    assert result is not None
    assert (result.state, result.test_failures) == ("failed", 0)
    assert "without the test failing" in result.detail


def test_an_unfinished_job_is_not_evaluated_yet():
    assert _evaluate("", _job(status="in_progress", conclusion="")) is None


def test_a_targeted_valgrind_leg_is_found_by_its_key():
    log = "[ok]: TTL expiration (1 ms)\n"
    result = _evaluate(log, _job("test-valgrind-test (targeted)"), _job("test-valgrind-no-malloc (targeted)"),
                       job="", job_id="test-valgrind-test")
    assert result is not None and result.state == "passed"



@pytest.mark.parametrize("test_file", [
    "tests/unit/x$(id).tcl", "tests/unit/a;b.tcl", "tests/../etc.tcl", "tests/unit/x.tcl --loop", "src/x.tcl",
])
def test_a_test_path_that_is_not_plain_is_refused(test_file):
    """The path reaches the workflow's shell through test_args, so it must be inert."""
    assert "is not a test file path" in _plan("test-ubuntu-jemalloc", test_file, "t")


def test_a_skiptests_token_that_gates_the_job_itself_is_kept():
    plan = _plan("test-sanitizer-large-memory", "tests/unit/type/list.tcl", "LPOS")
    assert isinstance(plan, DailyPlan)
    assert "large-memory" not in plan.inputs["skiptests"]
    assert plan.inputs["test_args"] == "--single unit/type/list --loops 20 --fastfail"


def test_a_whole_job_rerun_is_judged_by_the_job_result_alone():
    client = MagicMock()
    plan = DailyPlan(job="test-freebsd", job_id="test-freebsd", inputs={})
    failed = evaluate_daily_run(_gh_with_jobs(_job("test-freebsd", conclusion="failure")), client, "o/r", 1, plan)
    assert failed is not None and (failed.state, failed.verified) == ("failed", False)
    passed = evaluate_daily_run(_gh_with_jobs(_job("test-freebsd")), client, "o/r", 1, plan)
    assert passed is not None and (passed.state, passed.verified) == ("passed", True)
    client.download_job_log.assert_not_called()



def test_a_whole_job_rerun_skips_sibling_jobs_sharing_its_token():
    """Job-level valgrind failure: the misc jobs gated by other suites stay off."""
    plan = _plan("test-valgrind-test (unit)")
    assert isinstance(plan, DailyPlan)
    assert plan.inputs["skiptests"] == "modules,sentinel,unittest"  # misc needs modules or unittest
    assert "valkey" not in plan.inputs["skiptests"]                  # the target job's own suite


SHELL_GATED = """
on:
  workflow_dispatch:
    inputs:
      skipjobs: {default: "32bit"}
      skiptests: {default: "valkey,modules"}
      test_args: {default: ""}
      use_repo: {default: ""}
      use_git_ref: {default: ""}
jobs:
  test-alpine-32bit:
    if: github.event_name == 'workflow_dispatch' && !contains(github.event.inputs.skipjobs, '32bit')
    runs-on: ubuntu-latest
    steps:
      - name: Run 32-bit Alpine tests
        env:
          SKIPTESTS: ${{ github.event.inputs.skiptests }}
          TEST_ARGS: ${{ github.event.inputs.test_args }}
        run: |
          docker run i386/alpine sh -euxc '
            case "${SKIPTESTS:-}" in
              *valkey*) ;;
              *) ./runtest ${TEST_ARGS:-} ;;
            esac
            case "${SKIPTESTS:-}" in
              *modules*) ;;
              *) ./runtest-moduleapi ${TEST_ARGS:-} ;;
            esac
          '
"""


def test_suites_gated_in_shell_can_still_target_one_test():
    plan = plan_daily_run(SHELL_GATED, job_name="test-alpine-32bit", test_file="tests/integration/rdb.tcl",
                          test_name="TTL expiration", loops=20)
    assert isinstance(plan, DailyPlan)
    assert plan.inputs["skiptests"] == "modules"
    assert plan.inputs["test_args"] == "--single integration/rdb --loops 20 --fastfail"
    assert plan.test_name == "TTL expiration"


MACOS = """
on:
  workflow_dispatch:
    inputs:
      skipjobs: {default: "macos"}
      skiptests: {default: "valkey,modules"}
      test_args: {default: ""}
      use_repo: {default: ""}
      use_git_ref: {default: ""}
jobs:
  test-macos-latest:
    if: github.event_name == 'workflow_dispatch' && !contains(github.event.inputs.skipjobs, 'macos') && !(contains(github.event.inputs.skiptests, 'valkey') && contains(github.event.inputs.skiptests, 'modules'))
    runs-on: macos-latest
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest ${{github.event.inputs.test_args}}
      - name: module api test
        if: true && !contains(github.event.inputs.skiptests, 'modules')
        run: ./runtest-moduleapi ${{github.event.inputs.test_args}}
"""


def test_a_module_test_on_macos_runs_through_the_module_step_only():
    """The job's if: names both suites, but each also gates a step, so the other is skipped."""
    plan = plan_daily_run(MACOS, job_name="test-macos-latest", test_file="tests/unit/moduleapi/blockonkeys.tcl",
                          test_name="x", loops=20)
    assert isinstance(plan, DailyPlan)
    assert plan.inputs["skiptests"] == "valkey"


def test_a_timeout_of_the_target_test_counts_as_its_failure():
    log = "*** [TIMEOUT]: TTL expiration in tests/integration/rdb.tcl\n"
    result = _evaluate(log, _job(conclusion="failure"))
    assert result is not None
    assert (result.state, result.test_failures) == ("failed", 1)


def test_a_sibling_tests_ok_line_does_not_count_for_the_target():
    log = "[ok]: TTL expiration (forkless) (12 ms)\nWaiting... [ok]: TTL expiration (3 ms)\n"
    result = _evaluate(log, _job())
    assert result is not None and result.detail == "the test passed 1 time(s)"
    only_sibling = _evaluate("[ok]: TTL expiration (forkless) (12 ms)\n", _job())
    assert only_sibling is not None and only_sibling.state == "not-run"


_GATED = _DAILY.replace('''  test-freebsd:''', '''  test-ubuntu-arm:
    if: (github.event_name == 'workflow_dispatch') && (!contains(github.event.inputs.skipjobs, 'ubuntu') || !contains(github.event.inputs.skipjobs, 'arm'))
    runs-on: ubuntu-24.04-arm
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest ${{github.event.inputs.test_args}}
      - name: sentinel tests
        if: true && !contains(github.event.inputs.skiptests, 'sentinel')
        run: ./runtest-sentinel ${{github.event.inputs.cluster_test_args}}
  test-shards:
    name: test-shards (${{ matrix.shard }})
    if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'shards') && !contains(github.event.inputs.skiptests, 'valkey')
    strategy:
      matrix:
        shard: ${{ fromJSON(inputs.valgrind_test && '["targeted"]' || '["unit", "integration-type"]') }}
    runs-on: ubuntu-latest
    steps:
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        env:
          VALGRIND_TEST: ${{ inputs.valgrind_test }}
        run: |
          case "${{ matrix.shard }}" in
            unit) shard_args=(--single tests/unit) ;;
            integration-type) shard_args=(--single tests/integration --single tests/unit/type) ;;
            targeted) shard_args=(--single "$VALGRIND_TEST") ;;
          esac
          ./runtest --valgrind "${shard_args[@]}" ${{github.event.inputs.test_args}}
  collect:
    needs: [test-ubuntu-jemalloc]
    if: always() && github.event_name == 'workflow_dispatch'
    runs-on: ubuntu-latest
    steps:
      - run: echo
  test-freebsd:''').replace('''      valgrind_test:
        default: ""''', '''      valgrind_test:
        default: ""
      cluster_test_args:
        default: ""''').replace('"valgrind,sanitizer,ubuntu,rpm-distros,freebsd"', '"valgrind,sanitizer,ubuntu,arm,shards,rpm-distros,freebsd"')


def test_an_or_gated_job_keeps_only_the_token_no_other_job_needs():
    """Keeping 'ubuntu' for test-ubuntu-arm would also start test-ubuntu-jemalloc."""
    plan = plan_daily_run(_GATED, job_name="test-ubuntu-arm", test_file="tests/unit/type/list.tcl", test_name="x")
    skipjobs = plan.inputs["skipjobs"].split(",")
    assert "ubuntu" in skipjobs and "arm" not in skipjobs


def test_a_plan_that_would_not_start_its_job_is_refused():
    unreachable = _GATED.replace(
        "!contains(github.event.inputs.skipjobs, 'freebsd')",
        "!contains(github.event.inputs.skipjobs, 'freebsd') && github.event_name == 'schedule'")
    reason = plan_daily_run(unreachable, job_name="test-freebsd")
    assert isinstance(reason, str) and "would not run" in reason


def test_a_sentinel_test_runs_only_the_sentinel_suite_and_that_file():
    plan = plan_daily_run(_GATED, job_name="test-ubuntu-arm", test_file="tests/sentinel/tests/00-base.tcl",
                          test_name="Sentinel is able to failover")
    assert plan.inputs["cluster_test_args"] == "--single 00-base"
    assert "sentinel" not in plan.inputs["skiptests"] and "valkey" in plan.inputs["skiptests"]
    # The sentinel harness prints no "[ok]: <name>" lines: the job decides.
    assert (plan.test_name, plan.job) == ("", "test-ubuntu-arm")
    assert plan.describe() == "`test-ubuntu-arm` with `--single 00-base`"


def test_a_whole_shard_reruns_as_the_one_targeted_leg_with_the_shard_selection():
    plan = plan_daily_run(_GATED, job_name="test-shards (integration-type)")
    assert (plan.inputs["valgrind_test"], plan.inputs["test_args"]) == (
        "tests/integration", "--single tests/unit/type")
    assert (plan.job, plan.job_id, plan.test_name) == ("", "test-shards", "")
    assert plan.describe() == "`test-shards (targeted)` with `--single tests/integration --single tests/unit/type`"


def test_a_job_that_only_collects_results_is_not_rerun():
    reason = plan_daily_run(_GATED, job_name="collect")
    assert isinstance(reason, str) and "only collects" in reason


def test_a_target_job_condition_that_cannot_be_modelled_is_refused():
    workflow = _DAILY.replace(
        "if: (github.event_name == 'workflow_dispatch' || github.event_name == 'schedule') && "
        "!contains(github.event.inputs.skipjobs, 'ubuntu')",
        "if: fromJSON('true') && !contains(github.event.inputs.skipjobs, 'ubuntu')",
    )
    assert workflow != _DAILY
    plan = plan_daily_run(workflow, job_name="test-ubuntu-jemalloc", test_file="tests/unit/x.tcl",
                          test_name="t", loops=20)
    assert isinstance(plan, str) and "cannot tell whether job 'test-ubuntu-jemalloc' runs" in plan


def test_another_jobs_unmodelled_condition_only_counts_it_as_started(caplog):
    workflow = _DAILY.replace(
        "if: (github.event_name == 'workflow_dispatch') && !contains(github.event.inputs.skipjobs, 'rpm-distros')",
        "if: fromJSON('false')",
    )
    assert workflow != _DAILY
    with caplog.at_level("INFO"):
        plan = plan_daily_run(workflow, job_name="test-ubuntu-jemalloc", test_file="tests/unit/x.tcl",
                              test_name="t", loops=20)
    assert isinstance(plan, DailyPlan)
    assert "also starts: test-rpm-distros" in caplog.text
