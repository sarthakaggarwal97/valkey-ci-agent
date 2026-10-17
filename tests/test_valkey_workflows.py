from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from scripts.ci_fix import copies as copies_mod
from scripts.valkey_workflows import normalize as normalize_mod
from scripts.valkey_workflows import sync as sync_mod

DAILY = """\
name: Daily
on:
  schedule:
    - cron: "0 0 * * *"
  workflow_dispatch:
    inputs:
      skiptests:
        default: ""
concurrency:
  group: daily-${{ github.ref }}
jobs:
  test-ubuntu-jemalloc:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule' && !contains(github.event.inputs.skipjobs, 'ubuntu')
    timeout-minutes: 1440
    steps:
      - name: prep
        if: github.event_name == 'workflow_dispatch' || github.event_name == 'workflow_call'
        run: echo "GITHUB_HEAD_REF=${{github.event.inputs.use_git_ref}}" >> $GITHUB_ENV
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          path: libbacktrace
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ${{ github.event.inputs.use_repo || github.repository }}
      - name: make
        run: make
      - name: test
        if: true && !contains(github.event.inputs.skiptests, 'valkey')
        run: ./runtest ${{ github.event_name != 'pull_request' && '--accurate' || '' }} --dump-logs ${{github.event.inputs.test_args}}
      - name: backward compatibility tests with OSS Redis 7.0
        run: ./runtest --tags compatible-redis
  notify-about-job-results:
    needs: [test-ubuntu-jemalloc]
    runs-on: ubuntu-latest
    steps:
      - run: echo done
"""

CI = """\
name: CI
on: [push, pull_request]
jobs:
  test-ubuntu-latest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
      - name: make
        run: make SERVER_CFLAGS='-Werror'
      - name: test
        run: |
          sudo apt-get install tcl8.6 tclx
          ./runtest --verbose --other-server-path tests/tmp/old/bin/valkey-server --tags -slow \\
            --dump-logs
      - name: module api test
        run: CFLAGS='-Werror' ./runtest-moduleapi --verbose
"""


def _load(text: str) -> dict:
    return yaml.safe_load(text)


def test_a_copy_runs_only_the_named_job_from_explicit_inputs():
    copy = normalize_mod.normalize(DAILY, origin="valkey-io/valkey@abc (unstable):daily.yml")
    workflow = _load(copy.text)

    assert list(workflow["on"]) == ["workflow_call"]
    assert workflow["permissions"] == {"contents": "read", "id-token": "write"}
    assert list(workflow["on"]["workflow_call"]["secrets"]) == ["AWS_ROLE_ARN"]
    assert "concurrency" not in workflow
    assert list(workflow["jobs"]) == ["test-ubuntu-jemalloc"]  # the aggregator is gone
    job = workflow["jobs"]["test-ubuntu-jemalloc"]
    assert job["if"] == "inputs.job == 'test-ubuntu-jemalloc'"
    assert job["timeout-minutes"] == normalize_mod.MAX_JOB_MINUTES
    assert "github.event" not in copy.text
    assert copy.jobs == {"test-ubuntu-jemalloc": ["test-ubuntu-jemalloc"]}


def _steps(source: str, job: str) -> dict:
    steps = _load(normalize_mod.normalize(source, origin="x").text)["jobs"][job]["steps"]
    return {step.get("name") or step.get("uses"): step for step in steps}


def test_only_the_main_checkout_takes_the_ref():
    steps = _load(normalize_mod.normalize(DAILY, origin="x").text)["jobs"]["test-ubuntu-jemalloc"]["steps"]

    main = next(i for i, step in enumerate(steps) if (step.get("with") or {}).get("ref"))
    assert steps[main]["with"]["repository"] == "${{ inputs.use_repo }}"
    assert steps[main]["with"]["ref"] == "${{ inputs.use_git_ref }}"
    assert steps[1]["with"]["repository"] == "ianlancetaylor/libbacktrace"
    assert steps[0]["if"] is True  # a setup step for dispatched runs always runs


def test_test_steps_are_gated_by_suite_and_accurate_is_an_input():
    steps = _steps(DAILY, "test-ubuntu-jemalloc")

    assert steps["test"]["if"] == "(true && !contains(inputs.skiptests, 'valkey')) && inputs.fix == ''"
    assert "${{ inputs.accurate && '--accurate' || '' }}" in steps["test"]["run"]
    assert steps["backward compatibility tests with OSS Redis 7.0"]["if"] == "(inputs.compat) && inputs.fix == ''"


def test_in_fix_mode_the_job_runs_up_to_its_tests_and_hands_over_to_the_agent():
    copy = normalize_mod.normalize(DAILY, origin="x")
    steps = _steps(DAILY, "test-ubuntu-jemalloc")
    names = list(steps)

    assert copy.fixable == {"test-ubuntu-jemalloc": ""}
    assert "if" not in steps["make"]  # the job's own build runs in fix mode too
    fix = names[names.index("CI fix: check out the agent"):]
    assert [step["name"] for step in normalize_mod.fix_steps("x", {})] == fix
    assert all(steps[name]["if"] == "inputs.fix != ''" for name in fix[:-1])
    assert steps[fix[-1]]["if"] == "always() && inputs.fix != ''"
    env = steps["CI fix: prepare the fix in this job"]["env"]
    assert env["CI_FIX_BUILD"] == "make"
    # Without the job's own selection; GitHub evaluates the expression.
    assert env["CI_FIX_RUNTEST"] == "./runtest ${{ inputs.accurate && '--accurate' || '' }} --dump-logs"


_MATRIX = DAILY.replace(
    "    timeout-minutes: 1440\n",
    "    timeout-minutes: 1440\n    strategy:\n      matrix:\n        compiler: [gcc, clang]\n"
    "    env:\n      CC: ${{ matrix.compiler }}\n",
)


def test_a_matrix_job_prepares_the_fix_in_the_entry_that_failed_only():
    copy = normalize_mod.normalize(_MATRIX, origin="x")
    steps = _steps(_MATRIX, "test-ubuntu-jemalloc")

    assert copy.fixable == {"test-ubuntu-jemalloc": "compiler"}
    assert steps["make"]["if"] == "inputs.fix == '' || matrix.compiler == inputs.fix_matrix"
    assert steps["CI fix: prepare the fix in this job"]["if"] == (
        "inputs.fix != '' && matrix.compiler == inputs.fix_matrix"
    )
    # Its environment reaches the rebuilds, which run with a clean one.
    assert steps["CI fix: prepare the fix in this job"]["env"]["CI_FIX_BUILD"] == (
        "export CC='${{ matrix.compiler }}'\nmake"
    )


def test_a_container_job_installs_the_agents_tools_and_names_its_commit():
    source = DAILY.replace("    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    container: alpine:latest\n")
    steps = _steps(source, "test-ubuntu-jemalloc")

    assert normalize_mod.normalize(source, origin="x").fixable == {"test-ubuntu-jemalloc": ""}
    assert steps["CI fix: set up the agent"]["with"]["system-packages"] == "true"
    env = steps["CI fix: prepare the fix in this job"]["env"]
    # Without git in the container, the checkout is an archive of this commit.
    assert env["CI_FIX_REPOSITORY"] == "${{ inputs.use_repo }}"
    assert env["CI_FIX_COMMIT"] == "${{ inputs.use_git_ref }}"


def test_a_macos_job_has_a_fix_mode_on_the_runners_own_tools():
    source = DAILY.replace("    runs-on: ubuntu-latest\n", "    runs-on: macos-latest\n")
    steps = _steps(source, "test-ubuntu-jemalloc")

    assert normalize_mod.normalize(source, origin="x").fixable == {"test-ubuntu-jemalloc": ""}
    assert "system-packages" not in steps["CI fix: set up the agent"]["with"]
    assert "CI_FIX_COMMIT" not in steps["CI fix: prepare the fix in this job"]["env"]


_NAMED_MATRIX = DAILY.replace(
    "  test-ubuntu-jemalloc:\n    runs-on: ubuntu-latest\n",
    "  test-rpm-distros:\n    name: ${{ matrix.name }}\n    runs-on: ubuntu-latest\n"
    "    container: ${{ matrix.container }}\n    strategy:\n      matrix:\n        include:\n"
    "          - {name: test-almalinux8, container: 'almalinux:8', install_epel: true}\n"
    "          - {name: test-fedoralatest, container: 'fedora:latest'}\n",
).replace("      - name: make\n", "      - name: Install EPEL\n        if: matrix.install_epel\n"
          "        run: dnf -y install epel-release\n      - name: make\n")


def test_a_matrix_named_by_its_entries_selects_the_entry_by_the_failed_jobs_name():
    copy = normalize_mod.normalize(_NAMED_MATRIX, origin="x")
    steps = _steps(_NAMED_MATRIX, "test-rpm-distros")

    assert copy.fixable == {"test-rpm-distros": "name"}
    assert copy.jobs == {"test-rpm-distros": ["test-almalinux8", "test-fedoralatest"]}
    # The entry's own conditions still hold in fix mode: && binds tighter than ||.
    assert steps["Install EPEL"]["if"] == (
        "(matrix.install_epel) && (inputs.fix == '' || matrix.name == inputs.fix_matrix)"
    )
    assert steps["test"]["if"].endswith("inputs.fix == '') && "
                                        "(inputs.fix == '' || matrix.name == inputs.fix_matrix)")
    assert steps["CI fix: prepare the fix in this job"]["if"] == (
        "inputs.fix != '' && matrix.name == inputs.fix_matrix"
    )
    manifest = {"unstable/daily": {"jobs": copy.jobs, "fixable": copy.fixable}}
    found = copies_mod.copy_job_for(manifest, "unstable", "daily", "test-fedoralatest")
    assert found is not None and (found.job, found.matrix_key) == ("test-rpm-distros", "name")


@pytest.mark.parametrize("change", [
    ("    runs-on: ubuntu-latest\n", "    runs-on: windows-latest\n"),
    ("      - name: make\n        run: make\n", ""),
])
def test_a_job_the_agent_cannot_run_in_has_no_fix_mode(change):
    source = DAILY.replace(*change)

    assert normalize_mod.normalize(source, origin="x").fixable == {}
    assert "CI fix: prepare the fix in this job" not in _steps(source, "test-ubuntu-jemalloc")


def test_ci_test_commands_receive_the_test_selection():
    steps = {step.get("name"): step for step in
             _load(normalize_mod.normalize(CI, origin="x").text)["jobs"]["test-ubuntu-latest"]["steps"]}

    assert steps["test"]["if"] == "(!contains(inputs.skiptests, 'valkey')) && inputs.fix == ''"
    # The argument ends the runtest command, after its continuation line.
    assert steps["test"]["run"].splitlines()[-1].strip() == "--dump-logs ${{ inputs.test_args }}"
    assert steps["module api test"]["run"].endswith("${{ inputs.test_args }}")
    assert steps["module api test"]["if"] == "(!contains(inputs.skiptests, 'modules')) && inputs.fix == ''"


def test_the_branches_weekly_tests_are_synced_newest_first():
    names = ["unstable", "7.0", "7.2", "8.0", "9.1", "feature-x", "9.0"]

    assert sync_mod.branches(names) == ["unstable", "9.1", "9.0", "8.0", "7.2"]


def test_sync_writes_copies_a_router_and_a_manifest_and_drops_stale_copies(tmp_path):
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "valkey-copy-6-2-daily.yml").write_text("stale", encoding="utf-8")

    sync_mod.sync(["unstable", "9.0"], lambda branch: (f"sha-{branch}", {"daily": DAILY, "ci": CI}), root=tmp_path)

    names = sorted(path.name for path in workflows.iterdir())
    assert names == sorted([
        "valkey-copy-unstable-daily.yml", "valkey-copy-unstable-ci.yml",
        "valkey-copy-9-0-daily.yml", "valkey-copy-9-0-ci.yml", sync_mod.ROUTER,
    ])
    manifest = json.loads((tmp_path / sync_mod.MANIFEST).read_text())
    assert manifest["9.0/daily"]["source_sha"] == "sha-9.0"
    assert manifest["9.0/daily"]["fixable"] == {"test-ubuntu-jemalloc": ""}
    router = _load((workflows / sync_mod.ROUTER).read_text())
    assert router["jobs"]["copy-9-0-daily"]["uses"] == "./.github/workflows/valkey-copy-9-0-daily.yml"
    assert router["jobs"]["copy-9-0-daily"]["if"] == "inputs.branch == '9.0' && inputs.workflow == 'daily'"


def test_the_sync_names_every_job_that_lost_its_copy_or_fix_mode():
    before = {"unstable/daily": {"jobs": {"a": ["a"], "b": ["b"], "c": ["c"]}, "fixable": {"a": "", "b": ""}}}
    after = {"unstable/daily": {"jobs": {"a": ["a"], "b": ["b"]}, "fixable": {"a": ""}}}

    assert sync_mod.lost_jobs(before, after) == [
        "`unstable/daily` `b`: lost its fix mode",
        "`unstable/daily` `c`: no longer copied",
    ]
    assert sync_mod.lost_jobs(after, after) == []


# --- the copies committed in this repository -----------------------------------

_MANIFEST = json.loads(Path(sync_mod.MANIFEST).read_text(encoding="utf-8"))


def test_every_committed_copy_is_callable_only_and_runs_one_named_job():
    for entry in _MANIFEST.values():
        text = (sync_mod.WORKFLOW_DIR / entry["file"]).read_text(encoding="utf-8")
        workflow = _load(text)
        assert list(workflow["on"]) == ["workflow_call"], entry["file"]
        assert workflow["permissions"] == normalize_mod.PERMISSIONS, entry["file"]
        assert "github.event" not in text, entry["file"]
        assert set(workflow["jobs"]) == set(entry["jobs"]), entry["file"]
        for job_id, job in workflow["jobs"].items():
            assert job["if"] == f"inputs.job == '{job_id}'", (entry["file"], job_id)


def test_valkeys_container_and_macos_test_jobs_have_a_fix_mode():
    fixable = _MANIFEST["unstable/daily"]["fixable"]

    for job in ("test-alpine-jemalloc", "test-ubuntu-jemalloc-fortify", "test-macos-latest"):
        assert fixable[job] == "", job
    for job in ("test-rpm-distros-jemalloc", "test-rpm-distros-tls-module"):
        assert fixable[job] == "name", job


def test_the_router_calls_every_copy_and_no_other():
    router = _load((sync_mod.WORKFLOW_DIR / sync_mod.ROUTER).read_text(encoding="utf-8"))
    called = {job["uses"].rsplit("/", 1)[-1] for job in router["jobs"].values()}

    assert called == {entry["file"] for entry in _MANIFEST.values()}
    assert {path.name for path in sync_mod.WORKFLOW_DIR.glob("valkey-copy-*.yml")} == called | {sync_mod.ROUTER}


def test_a_test_step_that_also_installs_keeps_that_setup_in_fix_mode():
    copy = normalize_mod.normalize(CI, origin="x")
    steps = _load(copy.text)["jobs"]["test-ubuntu-latest"]["steps"]
    names = [step.get("name") or step.get("uses") for step in steps]
    by_name = dict(zip(names, steps))

    assert copy.fixable == {"test-ubuntu-latest": ""}
    # The install is a step of its own, which fix mode runs, before the tests.
    assert names.index("test setup") < names.index("test")
    assert by_name["test setup"]["run"] == "sudo apt-get install tcl8.6 tclx"
    assert "inputs.fix" not in str(by_name["test setup"].get("if") or "")
    assert "apt-get" not in by_name["test"]["run"]
    # The agent runs its own tests: not the job's selection, and not against
    # another server the fix mode never downloads.
    env = by_name["CI fix: prepare the fix in this job"]["env"]
    assert env["CI_FIX_RUNTEST"] == "./runtest --verbose --tags -slow --dump-logs"


def test_a_shell_variable_the_test_command_uses_stays_with_it():
    source = CI.replace(
        "          sudo apt-get install tcl8.6 tclx\n",
        "          shard=unit\n          sudo apt-get install tcl8.6 tclx\n",
    )
    steps = {step.get("name"): step for step in
             _load(normalize_mod.normalize(source, origin="x").text)["jobs"]["test-ubuntu-latest"]["steps"]}

    assert steps["test setup"]["run"] == "sudo apt-get install tcl8.6 tclx"
    assert steps["test"]["run"].splitlines()[0].strip() == "shard=unit"


_INSTALLS = re.compile(r"\b(?:apt-get|yum|dnf) install\b|\bapk add\b|\bbrew install\b|\bpip install\b")


def _skipped_in_fix_mode(step: dict) -> bool:
    """Whether fix mode skips ``step``: its condition requires ``inputs.fix == ''``.

    A matrix job's steps are also conditional on ``inputs.fix``, but as an
    alternative (run in test mode, or in the entry that prepares the fix), so
    only the conjunction means skipped.
    """
    condition = str(step.get("if") or "")
    return condition == normalize_mod._NOT_FIX or f"&& {normalize_mod._NOT_FIX}" in condition


def test_no_fixable_job_leaves_its_dependency_install_behind_in_fix_mode():
    """A job the agent fixes in must still install what its own tests need."""
    offenders = []
    for key, entry in _MANIFEST.items():
        workflow = _load((sync_mod.WORKFLOW_DIR / entry["file"]).read_text(encoding="utf-8"))
        for job_id in entry["fixable"]:
            for step in workflow["jobs"][job_id]["steps"]:
                if _skipped_in_fix_mode(step) and _INSTALLS.search(str(step.get("run") or "")):
                    offenders.append(f"{key} {job_id}: {step.get('name')}")

    assert offenders == []


def test_no_fixable_jobs_test_command_names_a_fixture_fix_mode_skips():
    """``--other-server-path`` points into tests/tmp, which only a skipped step fills."""
    offenders = []
    for key, entry in _MANIFEST.items():
        workflow = _load((sync_mod.WORKFLOW_DIR / entry["file"]).read_text(encoding="utf-8"))
        for job_id in entry["fixable"]:
            for step in workflow["jobs"][job_id]["steps"]:
                env = step.get("env") or {}
                for name in ("CI_FIX_RUNTEST", "CI_FIX_RUNTEST_MODULEAPI"):
                    if "--other-server-path" in str(env.get(name) or "") or "--only" in str(env.get(name) or ""):
                        offenders.append(f"{key} {job_id}: {name}")

    assert offenders == []
