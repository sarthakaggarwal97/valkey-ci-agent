from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

_USE_RE = re.compile(r"uses:\s*([^@\s]+)@([^#\s]+)")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_CLAUDE_CODE_INSTALL_RE = re.compile(r"npm install -g @anthropic-ai/claude-code(?:@([^\s]+))?")
_PIP_INSTALL_RE = r"(?:python -m )?pip install"
_OLD_REQUIREMENTS_INSTALL_RE = re.compile(rf"{_PIP_INSTALL_RE} (?:-r requirements\.txt|pyyaml)")
_DIRECT_AGENT_SETUP_RE = re.compile(
    rf"actions/setup-python@|{_PIP_INSTALL_RE} \.|{_PIP_INSTALL_RE} -e|npm install -g @anthropic-ai/claude-code"
)
_EXPECTED_CLAUDE_CODE_VERSION = "2.1.175"
_TRUSTED_REUSABLE_WORKFLOW_REFS = {
    # Qualification is an organization-owned workflow, not a third-party
    # action. It intentionally follows the automation repository's protected
    # main branch so the two release components can evolve together without a
    # tag-management bootstrap cycle. The called workflow pins all of its
    # implementation checkouts to github.workflow_sha, the exact commit this
    # reference resolved to. It receives no secrets, and candidate-code jobs
    # are constrained to contents:read. Pinning this reference to a SHA would
    # add no security boundary: the production artifacts themselves are built
    # by build-release.yml in that same repository at its own protected refs,
    # so an attacker who controls valkey-release-automation main already
    # controls the release outputs regardless of how qualification is called.
    "valkey-io/valkey-release-automation/.github/workflows/qualify-release.yml@main",
}


def _workflow_files():
    return sorted(Path(".github/workflows").glob("*.yml"))


def _action_files():
    action_dir = Path(".github/actions")
    if not action_dir.exists():
        return []
    return sorted([*action_dir.glob("**/*.yml"), *action_dir.glob("**/*.yaml")])


def _automation_yaml_files():
    return [*_workflow_files(), *_action_files()]


def test_automation_yaml_files_parse():
    for path in _automation_yaml_files():
        assert yaml.safe_load(path.read_text(encoding="utf-8")) is not None


def test_external_actions_are_pinned_to_shas():
    offenders = []
    for path in _automation_yaml_files():
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            match = _USE_RE.search(line)
            if not match:
                continue
            action, ref = match.groups()
            if action.startswith("./"):
                continue
            if f"{action}@{ref}" in _TRUSTED_REUSABLE_WORKFLOW_REFS:
                continue
            if not _SHA_RE.fullmatch(ref):
                offenders.append(f"{path}:{line_no}: {action}@{ref}")

    assert offenders == []


def test_claude_code_install_is_version_pinned_consistently():
    offenders = []
    for path in _automation_yaml_files():
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            match = _CLAUDE_CODE_INSTALL_RE.search(line)
            if not match:
                continue
            version = match.group(1)
            if version != _EXPECTED_CLAUDE_CODE_VERSION:
                offenders.append(
                    f"{path}:{line_no}: expected claude-code@{_EXPECTED_CLAUDE_CODE_VERSION}, got {version or 'latest'}"
                )

    assert offenders == []


def test_workflows_install_project_metadata_not_duplicate_requirements():
    offenders = []
    for path in _automation_yaml_files():
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _OLD_REQUIREMENTS_INSTALL_RE.search(line):
                offenders.append(f"{path}:{line_no}: {line.strip()}")

    assert offenders == []


def test_workflows_use_shared_agent_setup_action():
    offenders = []
    for path in _workflow_files():
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _DIRECT_AGENT_SETUP_RE.search(line):
                offenders.append(f"{path}:{line_no}: {line.strip()}")

    assert offenders == []


def test_github_app_tokens_request_explicit_permissions():
    offenders = []
    token_action = "uses: actions/create-github-app-token@"
    for path in _workflow_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_no, line in enumerate(lines, start=1):
            if token_action not in line:
                continue
            block = []
            for later in lines[line_no:]:
                if later.startswith("      - name: "):
                    break
                block.append(later)
            if not any("permission-" in later for later in block):
                offenders.append(f"{path}:{line_no}: create-github-app-token has no explicit permissions")

    assert offenders == []


def test_push_capable_app_tokens_can_update_workflows():
    """Push tokens need workflows:write for commits touching .github/workflows."""
    required_steps = {
        ".github/workflows/backport.yml": "Generate GitHub App token",
        ".github/workflows/backport-poll.yml": "Generate publication token",
        ".github/workflows/backport-sweep.yml": "Generate publication token",
        ".github/workflows/manual-revert-commit.yml": "Generate GitHub App token",
    }
    offenders = []
    for workflow, step_name in required_steps.items():
        path = Path(workflow)
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_no, line in enumerate(lines, start=1):
            if line.strip() != f"- name: {step_name}":
                continue
            block = []
            for later in lines[line_no:]:
                if later.startswith("      - name: "):
                    break
                block.append(later)
            if not any("permission-workflows: write" in later for later in block):
                offenders.append(f"{path}:{line_no}: {step_name} lacks permission-workflows: write")
            break
        else:
            offenders.append(f"{path}: missing token step {step_name!r}")

    assert offenders == []


def test_backport_workflows_refresh_credentials_after_validation():
    for filename, job_name in (
        ("backport-sweep.yml", "sweep"),
        ("backport-poll.yml", "poll"),
    ):
        text = (Path(".github/workflows") / filename).read_text(encoding="utf-8")
        assert (
            text.index("- name: Generate preparation token")
            < text.index("- name: Prepare backport sweep")
            < text.index("- name: Generate publication token")
            < text.index("- name: Publish backport sweep")
            < text.index("- name: Clean up prepared sweep")
        )
        assert "TARGET_TOKEN: ${{ steps.prepare-token.outputs.token }}" in text
        assert "TARGET_TOKEN: ${{ steps.publish-token.outputs.token }}" in text
        assert "--target-token" not in text

        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        job = workflow["jobs"][job_name]
        aws_step = next(
            step
            for step in job["steps"]
            if step.get("name") == "Configure AWS credentials"
        )
        assert int(aws_step["with"]["role-duration-seconds"]) >= (
            int(job["timeout-minutes"]) * 60
        )

    poll = (Path(".github/workflows") / "backport-poll.yml").read_text()
    assert 'cron: "0 * * * *"' in poll
    assert "'1800'" in poll and "'3300'" in poll
    assert "poll_started=${SECONDS}" in poll
    assert 'sleep "${wait}"' in poll
    assert "if action=$(poll_once); then" in poll
    assert 'echo "had_error=${poll_had_error}"' in poll
    assert "steps.poll.outputs.had_error == 'true'" in poll


def test_backport_branch_mutations_share_one_concurrency_group():
    groups = []
    for filename, job_name in (
        ("backport-poll.yml", "poll"),
        ("backport-sweep.yml", "sweep"),
        ("backport-ci-followup.yml", "follow-up"),
    ):
        workflow = yaml.load(
            (Path(".github/workflows") / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        concurrency = workflow["jobs"][job_name]["concurrency"]
        groups.append(concurrency["group"])
        assert concurrency["cancel-in-progress"] == "false"
        assert concurrency["queue"] == "max"

    assert groups == [
        "backport-branch-mutation-${{ matrix.repo }}-${{ matrix.branch }}",
        "backport-branch-mutation-${{ matrix.repo }}-${{ matrix.branch }}",
        "backport-branch-mutation-${{ matrix.repo }}-${{ matrix.branch }}",
    ]


def test_ci_fix_concurrency_groups_never_drop_a_queued_run():
    expected_groups = {
        "ci-fix.yml": "ci-fix-${{ inputs.repo }}-${{ inputs.pr }}",
        "backport-ci-followup.yml": (
            "backport-branch-mutation-${{ matrix.repo }}-${{ matrix.branch }}"
        ),
        # The whole run of one issue is the unit, not a job of it.
        "ci-fix-issues.yml": "ci-fix-issue-${{ inputs.issue }}",
    }
    for filename, job_name in (
        ("ci-fix.yml", "ci-fix"), ("backport-ci-followup.yml", "follow-up"), ("ci-fix-issues.yml", None),
    ):
        workflow = yaml.load(
            (Path(".github/workflows") / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        concurrency = workflow["jobs"][job_name]["concurrency"] if job_name else workflow["concurrency"]
        assert concurrency == {
            "group": expected_groups[filename],
            "cancel-in-progress": "false",
            "queue": "max",
        }, filename


def test_the_comment_poller_dispatches_with_a_separate_token():
    workflow = yaml.load(Path(".github/workflows/ci-fix-comment-poll.yml").read_text(encoding="utf-8"),
                         Loader=yaml.BaseLoader)
    steps = {step.get("name"): step for step in next(iter(workflow["jobs"].values()))["steps"]}
    target, dispatch = steps["Generate App token"]["with"], steps["Generate dispatch token"]["with"]
    assert target["repositories"] == "valkey" and "permission-actions" not in target
    assert dispatch["repositories"] == "valkey-ci-agent" and dispatch["permission-actions"] == "write"
    assert set(k for k in dispatch if k.startswith("permission-")) == {"permission-actions", "permission-metadata"}


def test_the_fix_runs_start_only_from_their_front_doors():
    # The poller and the backport follow-up keep main's hourly schedules; the
    # runs they start are dispatched, and the issue dispatcher has its switch.
    for filename, triggers in (
        ("ci-fix-comment-poll.yml", {"schedule", "workflow_dispatch"}),
        ("backport-ci-followup.yml", {"schedule", "workflow_dispatch"}),
        ("ci-fix.yml", {"workflow_dispatch"}),
        ("ci-fix-issues.yml", {"workflow_dispatch"}),
    ):
        workflow = yaml.load(
            (Path(".github/workflows") / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        assert set(workflow["on"]) == triggers, filename


def test_the_issue_dispatcher_schedule_runs_until_switched_off_and_never_writes_valkey():
    workflow = yaml.load(Path(".github/workflows/ci-fix-issue-dispatch.yml").read_text(encoding="utf-8"),
                         Loader=yaml.BaseLoader)
    job = workflow["jobs"]["dispatch"]
    assert "vars.CI_FIX_ISSUES_AUTO_DISPATCH != 'false'" in job["if"]
    assert "github.event_name == 'workflow_dispatch'" in job["if"]
    steps = {step.get("name"): step for step in job["steps"]}
    token = steps["Generate target repository token"]["with"]
    assert token["repositories"] == "valkey"
    assert {k: v for k, v in token.items() if k.startswith("permission-")} == {
        "permission-issues": "read", "permission-pull-requests": "read", "permission-metadata": "read",
    }


def test_ci_fix_issue_runs_are_named_for_the_dispatcher():
    from scripts.ci_fix.issue_dispatch import RUN_NAME

    workflow = yaml.load(Path(".github/workflows/ci-fix-issues.yml").read_text(encoding="utf-8"),
                         Loader=yaml.BaseLoader)
    assert RUN_NAME.match(workflow["run-name"].replace("${{ inputs.issue }}", "123"))


# --- ci-fix-run.yml: every fix is plan -> fix-in-job | fix-local -> publish

_RUN = Path(".github/workflows/ci-fix-run.yml")


def _run_jobs():
    return yaml.safe_load(_RUN.read_text(encoding="utf-8"))["jobs"]


def test_every_fix_front_door_runs_the_shared_fix():
    from scripts.ci_fix.models import FrontDoor

    doors = set()
    for filename, job_id in (("ci-fix.yml", "ci-fix"), ("backport-ci-followup.yml", "follow-up"),
                             ("ci-fix-issues.yml", "fix")):
        job = yaml.safe_load((Path(".github/workflows") / filename).read_text(encoding="utf-8"))["jobs"][job_id]
        assert job["uses"] == f"./{_RUN.as_posix()}", filename
        assert job["permissions"] == {"contents": "read", "id-token": "write"}, filename
        # Exactly what the run declares, not every secret of the repository.
        assert job["secrets"] == {
            name: f"${{{{ secrets.{name} }}}}"
            for name in ("VALKEYRIE_BOT_APP_ID", "VALKEYRIE_BOT_PRIVATE_KEY", "AWS_ROLE_ARN")
        }, filename
        doors.add(job["with"]["front_door"])
    assert doors == {door.value for door in FrontDoor}


def test_only_the_plan_and_publish_jobs_hold_a_github_token():
    jobs = _run_jobs()
    for job_id, command in (("plan", "python -m scripts.ci_fix.run plan"),
                            ("publish", "python -m scripts.ci_fix.run publish")):
        steps = jobs[job_id]["steps"]
        tokens = [step for step in steps if "actions/create-github-app-token@" in step.get("uses", "")]
        plan = job_id == "plan"
        assert [token["id"] for token in tokens] == (["target-token"] if plan else ["target-token", "ci-token"])
        permissions = {k: v for k, v in tokens[0]["with"].items() if k.startswith("permission-")}
        # The plan only reads and answers (a comment, a reaction); publish pushes and opens PRs.
        assert permissions == {
            "permission-actions": "read", "permission-issues": "write", "permission-metadata": "read",
            "permission-contents": "read" if plan else "write",
            "permission-pull-requests": "read" if plan else "write",
            # The PR command's gate checks team membership.
            **({"permission-members": "read"} if plan else {}),
        }, job_id
        if not plan:
            # Publish mints exactly one of its two tokens: workflows:write only
            # for a validated upstream port that may change .github/.
            plain, ci = tokens
            assert plain["if"] == (
                "needs.plan.outputs.ci_changes != 'true' || "
                "steps.publication-scope.outputs.workflows != 'true'"
            )
            assert ci["if"] == (
                "needs.plan.outputs.ci_changes == 'true' && "
                "steps.publication-scope.outputs.workflows == 'true'"
            )
            ci_permissions = {k: v for k, v in ci["with"].items() if k.startswith("permission-")}
            assert ci_permissions == {**permissions, "permission-workflows": "write"}
            assert {k: v for k, v in ci["with"].items() if not k.startswith("permission-")} == {
                k: v for k, v in plain["with"].items() if not k.startswith("permission-")
            }
        assert not any("configure-aws-credentials@" in step.get("uses", "") for step in steps), job_id
        assert "\n".join(step.get("run", "") for step in steps).count(command) == 1, job_id
        if job_id == "publish":
            scope_step = next(
                step
                for step in steps
                if "python -m scripts.ci_fix.run publication-scope" in step.get("run", "")
            )
            assert scope_step["id"] == "publication-scope"
            assert "TARGET_TOKEN" not in scope_step.get("env", {})
            publish_step = next(step for step in steps if command in step.get("run", ""))
            assert publish_step["env"]["CI_FIX_ISSUES_PUBLISH"] == "${{ vars.CI_FIX_ISSUES_PUBLISH }}"
            assert publish_step["env"]["TARGET_TOKEN"] == (
                "${{ steps.ci-token.outputs.token || steps.target-token.outputs.token }}"
            )
        assert jobs[job_id]["permissions"] == {"contents": "read"}, job_id
        assert jobs[job_id]["runs-on"] == "ubuntu-24.04", job_id
    assert _RUN.read_text(encoding="utf-8").count("actions/create-github-app-token@") == 3
    assert jobs["plan"]["outputs"]["ci_changes"] == "${{ steps.plan.outputs.ci_changes }}"


_FIX_JOBS = ("fix-in-job", "fix-local")
_CI_FIX_STEPS = "CI fix: check out the agent"


def _named_step(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


def test_the_fix_runs_in_the_failed_job_or_on_the_bots_runner():
    from scripts.ci_fix.engine import AWS_SESSION_S

    jobs = _run_jobs()
    assert "fix-in-container" not in jobs
    assert all(not jobs[job_id].get("container") for job_id in _FIX_JOBS)
    in_job, local = (jobs[job_id] for job_id in _FIX_JOBS)
    assert "needs.plan.outputs.container == ''" in in_job["if"] and "container" not in in_job
    assert "env" not in in_job
    assert in_job["runs-on"] == "${{ needs.plan.outputs.runs_on }}"
    assert in_job["timeout-minutes"] == "${{ fromJSON(needs.plan.outputs.timeout_minutes) }}"
    download = _named_step(in_job, "CI fix: download the plan")
    assert download["name"] == "CI fix: download the plan"
    create = _named_step(in_job, "CI fix: create the isolated checkout")
    assert "cd .ci-fix-agent" in create["run"]
    assert "-m scripts.ci_fix.prepare_job_workspace" in create["run"]
    assert 'create --plan "${RUNNER_TEMP}/ci-fix-plan"' in create["run"]
    assert '--state-file "${RUNNER_TEMP}/ci-fix-sandbox-state"' in create["run"]
    assert "actions/checkout@" not in create["run"]
    assert "actions/checkout@" not in str(in_job["steps"][:1])
    assert "Check out the commit to fix" not in {
        step.get("name") for step in in_job["steps"]
    }
    assert "Check out the job's other repository" not in {
        step.get("name") for step in in_job["steps"]
    }
    replay = _named_step(in_job, "CI fix: run the job setup without credentials")
    assert replay["run"] == "bash .ci-fix-agent/scripts/ci_fix/replay-job-setup.sh"
    seal = _named_step(in_job, "CI fix: seal the checkout before credentials")
    assert "env" not in seal
    run_fix = _named_step(in_job, "CI fix: prepare the fix in this job")
    assert not {"CI_FIX_IN_JOB", "CI_FIX_REPOSITORY", "CI_FIX_COMMIT"} & set(
        run_fix["env"]
    )
    assert run_fix["run"] == 'bash "${RUNNER_TEMP}/ci-fix-control/job.sh" run'
    assert "CI_FIX_IN_JOB" not in str(local)
    assert _named_step(local, "CI fix: create the isolated checkout")["run"] == create["run"]
    assert local["runs-on"] == "ubuntu-24.04"
    assert local["timeout-minutes"] * 60 > AWS_SESSION_S
    for job_id in _FIX_JOBS:
        assert jobs[job_id]["permissions"] == {"contents": "read", "id-token": "write"}, job_id
    assert jobs["publish"]["needs"] == ["plan", *_FIX_JOBS]


def test_every_fix_job_seals_before_credentials_and_cleans_up_afterward():
    from scripts.ci_fix.engine import AWS_SESSION_S

    jobs = _run_jobs()
    for job_id in _FIX_JOBS:
        job = jobs[job_id]
        names = [step.get("name") for step in job["steps"]]
        seal = names.index("CI fix: seal the checkout before credentials")
        aws = names.index("CI fix: configure AWS credentials")
        run_fix = names.index("CI fix: prepare the fix in this job")
        cleanup = names.index("CI fix: clean up the isolated account")
        upload = names.index("CI fix: upload the result")
        assert seal < aws < run_fix < cleanup < upload, job_id
        assert job["steps"][aws]["with"]["role-duration-seconds"] == AWS_SESSION_S
        assert job["steps"][cleanup]["if"] == "always()"
        assert job["steps"][upload]["if"] == "always()"


def test_the_job_setup_and_dependencies_run_before_any_credential_and_the_agent_apart():
    jobs = _run_jobs()
    names = [step.get("name") for step in jobs["fix-in-job"]["steps"]]
    download = names.index("CI fix: download the plan")
    checkout = names.index(_CI_FIX_STEPS)
    setup_agent = names.index("CI fix: set up the agent")
    create = names.index("CI fix: create the isolated checkout")
    bootstrap = names.index("CI fix: install code-owned target dependencies")
    replay = names.index("CI fix: run the job setup without credentials")
    seal = names.index("CI fix: seal the checkout before credentials")
    aws = names.index("CI fix: configure AWS credentials")
    assert (
        download
        < checkout
        < setup_agent
        < create
        < bootstrap
        < replay
        < seal
        < aws
    )
    local_names = [step.get("name") for step in jobs["fix-local"]["steps"]]
    assert (
        local_names.index("CI fix: create the isolated checkout")
        < local_names.index("CI fix: install code-owned target dependencies")
        < local_names.index("CI fix: seal the checkout before credentials")
        < local_names.index("CI fix: configure AWS credentials")
    )
    assert {
        _named_step(jobs[job_id], "CI fix: install code-owned target dependencies")["run"]
        for job_id in _FIX_JOBS
    } == {"bash .ci-fix-agent/scripts/ci_fix/bootstrap-job.sh"}


def test_runner_control_state_is_sealed_before_disposable_accounts():
    for path, account_creation in (
        (
            "scripts/ci_fix/replay-job-setup.sh",
            'account_command create --state-file "${account_state}" --user "${SETUP_USER}"',
        ),
        ("scripts/ci_fix/job.sh", 'create_account "${COMMAND_USER}"'),
    ):
        script = Path(path).read_text(encoding="utf-8")
        assert '"${python_bin}" -m scripts.ci_fix.accounts "$@"' in script
        assert 'account_command create --state-file "${account_state}"' in script
        resolved = script.index('runner_temp="$(cd -- "${RUNNER_TEMP}" && pwd -P)"')
        root_refusal = script.index('"${runner_temp}" == /')
        private = script.index('as_root chmod go-rwx "${runner_temp}"')
        account = script.index(account_creation)

        assert resolved < root_refusal < private < account


def test_code_owned_bootstrap_is_hermetic_and_bounded():
    script = Path("scripts/ci_fix/bootstrap-job.sh").read_text(encoding="utf-8")

    assert "/usr/bin/env -i" in script
    assert script.count("/usr/bin/timeout --kill-after=5s 300s") == 2
    assert "GITHUB_" not in script and "needs.plan.outputs" not in script


def test_setup_scripts_do_not_grant_runner_parent_search_permissions():
    scripts = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in (
            "scripts/ci_fix/job.sh",
            "scripts/ci_fix/replay-job-setup.sh",
            "scripts/ci_fix/bootstrap-job.sh",
        )
    )
    assert not re.search(r"\bsetfacl\b[^\n]*(?:-m\b|--modify\b)", scripts)
    assert not re.search(r"\bchmod\b[^\n]*\+a\b", scripts)


@pytest.mark.parametrize("nested", (False, True))
def test_target_setup_refuses_redirected_runner_temp_before_chmod(
    tmp_path,
    nested,
):
    workspace = tmp_path / "runner-work" / "repository" / "repository"
    workspace.mkdir(parents=True)
    redirected = tmp_path / "root-link"
    redirected.symlink_to("/", target_is_directory=True)
    runner_temp = redirected / "tmp" if nested else redirected

    result = subprocess.run(
        ["bash", "scripts/ci_fix/replay-job-setup.sh"],
        env={
            **os.environ,
            "GITHUB_WORKSPACE": str(workspace),
            "RUNNER_TEMP": str(runner_temp),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "runner temp directory is not a safe physical directory" in result.stdout


def test_the_isolation_tests_also_run_as_root_in_a_container():
    job = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))["jobs"]["isolation-as-root"]
    setup = next(step for step in job["steps"] if step.get("uses") == "./.github/actions/setup-agent")
    steps = yaml.safe_load(Path(".github/actions/setup-agent/action.yml").read_text(encoding="utf-8"))["runs"]["steps"]

    assert job["container"] and setup["with"]["system-packages"] == "true"
    assert job["container"] != "alpine:latest"  # pinned, so the test does not drift with Alpine
    assert any("pytest" in step.get("run", "") and "tests/test_isolation.py" in step["run"] for step in job["steps"])
    assert steps[0]["name"] == "Install Python and Node from the system's packages"
    assert steps[0]["shell"] == "sh" and steps[0]["if"] == "${{ inputs['system-packages'] == 'true' }}"
    assert steps[1]["uses"].startswith("actions/setup-python@")
    assert steps[1]["if"] == "${{ inputs['system-packages'] != 'true' }}"


def test_a_rerun_of_a_fix_replaces_its_artifacts():
    """An artifact name is unique per run: re-running a failed fix run would fail on its upload."""
    uploads = [
        step for job in _run_jobs().values() for step in job.get("steps") or []
        if "actions/upload-artifact@" in step.get("uses", "")
    ]
    assert len(uploads) == 3
    assert all(step["with"].get("overwrite") is True for step in uploads)
