from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

_USE_RE = re.compile(r"uses:\s*([^@\s]+)@([^#\s]+)")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_STEP_START_RE = re.compile(r"^\s+- name: ")
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


def _step_block(lines: list[str], line_no: int) -> list[str]:
    """The lines after ``lines[line_no - 1]`` up to the next step."""
    block = []
    for later in lines[line_no:]:
        if _STEP_START_RE.match(later):
            break
        block.append(later)
    return block


def test_github_app_tokens_request_explicit_permissions():
    offenders = []
    token_action = "uses: actions/create-github-app-token@"
    for path in _automation_yaml_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_no, line in enumerate(lines, start=1):
            if token_action not in line:
                continue
            block = _step_block(lines, line_no)
            if not any("permission-" in later for later in block):
                offenders.append(f"{path}:{line_no}: create-github-app-token has no explicit permissions")

    assert offenders == []


def test_push_capable_app_tokens_can_update_workflows():
    """Push tokens need workflows:write for commits touching .github/workflows."""
    required_steps = {
        ".github/workflows/backport.yml": "Generate GitHub App token",
        ".github/actions/backport-sweep/action.yml": "Generate publication token",
        ".github/workflows/manual-revert-commit.yml": "Generate GitHub App token",
    }
    offenders = []
    for workflow, step_name in required_steps.items():
        path = Path(workflow)
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_no, line in enumerate(lines, start=1):
            if line.strip() != f"- name: {step_name}":
                continue
            block = _step_block(lines, line_no)
            if not any("permission-workflows: write" in later for later in block):
                offenders.append(f"{path}:{line_no}: {step_name} lacks permission-workflows: write")
            break
        else:
            offenders.append(f"{path}: missing token step {step_name!r}")

    assert offenders == []


def test_backport_workflows_refresh_credentials_after_validation():
    action = Path(".github/actions/backport-sweep/action.yml").read_text(encoding="utf-8")
    assert (
        action.index("- name: Generate preparation token")
        < action.index("- name: Prepare backport sweep")
        < action.index("- name: Generate publication token")
        < action.index("- name: Publish backport sweep")
        < action.index("- name: Clean up prepared sweep")
    )
    assert "TARGET_TOKEN: ${{ steps.prepare-token.outputs.token }}" in action
    assert "TARGET_TOKEN: ${{ steps.publish-token.outputs.token }}" in action
    assert "role-duration-seconds: ${{ inputs.role-duration-seconds }}" in action
    assert "--target-token" not in action

    for filename, job_name in (
        ("backport-sweep.yml", "sweep"),
        ("backport-poll.yml", "poll"),
    ):
        text = (Path(".github/workflows") / filename).read_text(encoding="utf-8")
        assert "--target-token" not in text
        job = yaml.load(text, Loader=yaml.BaseLoader)["jobs"][job_name]
        sweep_step = next(
            step for step in job["steps"] if step.get("uses") == "./.github/actions/backport-sweep"
        )
        assert int(sweep_step["with"]["role-duration-seconds"]) >= (
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


def test_ci_fix_rollout_is_manual_only():
    for filename in (
        "ci-fix-comment-poll.yml",
        "ci-fix.yml",
        "backport-ci-followup.yml",
        "ci-fix-issues.yml",
    ):
        workflow = yaml.load(
            (Path(".github/workflows") / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        assert set(workflow["on"]) == {"workflow_dispatch"}, filename


def test_the_issue_dispatcher_schedule_waits_for_its_switch_and_never_writes_valkey():
    workflow = yaml.load(Path(".github/workflows/ci-fix-issue-dispatch.yml").read_text(encoding="utf-8"),
                         Loader=yaml.BaseLoader)
    job = workflow["jobs"]["dispatch"]
    assert "vars.CI_FIX_ISSUES_AUTO_DISPATCH == 'true'" in job["if"]
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


def test_llvm_dependent_jobs_stay_on_ubuntu_24_04():
    jobs = {
        "backport.yml": "backport",
        "backport-sweep.yml": "sweep",
        "backport-poll.yml": "poll",
    }
    for filename, job_name in jobs.items():
        workflow = yaml.load(
            (Path(".github/workflows") / filename).read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )
        assert workflow["jobs"][job_name]["runs-on"] == "ubuntu-24.04", filename


# --- ci-fix-run.yml: every fix is plan -> fix-in-copy | fix-local -> publish

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
        assert job["secrets"] == "inherit", filename
        doors.add(job["with"]["front_door"])
    assert doors == {door.value for door in FrontDoor}


def test_only_the_plan_and_publish_jobs_hold_a_github_token():
    jobs = _run_jobs()
    for job_id, command in (("plan", "python -m scripts.ci_fix.run plan"),
                            ("publish", "python -m scripts.ci_fix.run publish")):
        steps = jobs[job_id]["steps"]
        tokens = [step for step in steps if "actions/create-github-app-token@" in step.get("uses", "")]
        assert len(tokens) == 1 and tokens[0]["id"] == "target-token", job_id
        permissions = {k: v for k, v in tokens[0]["with"].items() if k.startswith("permission-")}
        assert permissions == {
            "permission-actions": "read", "permission-contents": "write", "permission-pull-requests": "write",
            "permission-issues": "write", "permission-metadata": "read",
            # The PR command's gate checks team membership.
            **({"permission-members": "read"} if job_id == "plan" else {}),
        }, job_id
        assert not any("configure-aws-credentials@" in step.get("uses", "") for step in steps), job_id
        assert "\n".join(step.get("run", "") for step in steps).count(command) == 1, job_id
        assert jobs[job_id]["permissions"] == {"contents": "read"}, job_id
        assert jobs[job_id]["runs-on"] == "ubuntu-24.04", job_id
    assert _RUN.read_text(encoding="utf-8").count("actions/create-github-app-token@") == 2


def test_the_fix_runs_in_the_copied_job_or_the_same_steps_on_the_bots_runner():
    from scripts.valkey_workflows.normalize import AWS_SESSION_S, fix_steps
    from scripts.valkey_workflows.sync import ROUTER

    jobs = _run_jobs()
    in_copy = jobs["fix-in-copy"]
    assert in_copy["uses"] == f"./.github/workflows/{ROUTER}"
    assert in_copy["with"]["fix"] == "${{ inputs.key }}"
    assert in_copy["secrets"] == {"AWS_ROLE_ARN": "${{ secrets.AWS_ROLE_ARN }}"}
    local = jobs["fix-local"]
    assert local["steps"] == fix_steps("", {}, install_dependencies=True, key="${{ inputs.key }}")
    assert local["permissions"] == {"contents": "read", "id-token": "write"}
    assert local["runs-on"] == "ubuntu-24.04"
    assert local["timeout-minutes"] * 60 == AWS_SESSION_S


def test_the_fix_steps_install_dependencies_before_any_credential_and_run_the_agent_apart():
    from scripts.valkey_workflows.normalize import fix_steps

    names = [step["name"] for step in fix_steps("", {}, install_dependencies=True, key="k")]
    assert names.index("Install Valkey build and test dependencies") < names.index("CI fix: configure AWS credentials")
    script = Path("scripts/ci_fix/job.sh").read_text(encoding="utf-8")
    assert script.index("CI_FIX_AGENT_USER=") < script.index("python -m scripts.ci_fix.run fix")
    assert "TOKEN" not in script


def _restore_agent(workspace: Path, temp: Path) -> subprocess.CompletedProcess[str]:
    """Run job.sh's own ``restore_agent`` (and ``as_root``) against ``workspace``."""
    script = Path("scripts/ci_fix/job.sh").read_text(encoding="utf-8")
    functions = "".join(
        re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", script, re.MULTILINE | re.DOTALL).group(0)
        for name in ("as_root", "restore_agent")
    )
    program = (
        'set -euo pipefail\nsudo() { "$@"; }\n' + functions
        + f'agent={temp / "ci-fix-agent"}\nhome={workspace / ".ci-fix-agent"}\nrestore_agent\n'
    )
    return subprocess.run(["bash", "-c", program], capture_output=True, text=True, check=False)


def _agent_code(root: Path) -> Path:
    action = root / ".github" / "actions" / "setup-agent" / "action.yml"
    action.parent.mkdir(parents=True)
    action.write_text("name: the real agent setup\n")
    return action


def test_the_agents_code_is_put_back_for_the_jobs_own_cleanup():
    """Its setup is a composite action of this checkout: the post phase reads it from there."""
    script = Path("scripts/ci_fix/job.sh").read_text(encoding="utf-8")

    moved = script.index('mv -- "${home}" "${agent}"')
    assert moved < script.index("trap 'restore_agent") < script.index("python -m scripts.ci_fix.run fix")


@pytest.mark.parametrize("planted", ["directory", "link"])
def test_what_the_agent_left_at_the_agents_path_is_never_what_the_cleanup_reads(tmp_path, planted):
    workspace, temp, outside = tmp_path / "workspace", tmp_path / "temp", tmp_path / "outside"
    workspace.mkdir()
    _agent_code(temp / "ci-fix-agent")
    home = workspace / ".ci-fix-agent"
    if planted == "directory":
        # The account owns the workspace: it can make the path a directory of
        # its own, which a plain move would move the real code into.
        (_agent_code(home)).write_text("name: the agent's own action\n")
    else:
        _agent_code(outside).write_text("name: the agent's own action\n")
        home.symlink_to(outside)

    result = _restore_agent(workspace, temp)

    assert result.returncode == 0, result.stderr
    assert not home.is_symlink()
    assert (home / ".github/actions/setup-agent/action.yml").read_text() == "name: the real agent setup\n"
    assert not (home / "ci-fix-agent").exists()
    if planted == "link":
        # The link went; what it pointed at was not followed.
        assert (outside / ".github/actions/setup-agent/action.yml").exists()


def test_the_agents_code_is_put_back_where_nothing_was_left(tmp_path):
    workspace, temp = tmp_path / "workspace", tmp_path / "temp"
    workspace.mkdir()
    _agent_code(temp / "ci-fix-agent")

    assert _restore_agent(workspace, temp).returncode == 0
    assert (workspace / ".ci-fix-agent/.github/actions/setup-agent/action.yml").is_file()
    assert not (temp / "ci-fix-agent").exists()


def test_a_container_fix_mode_installs_the_agents_tools_from_the_distribution():
    """setup-python's builds do not run on every distribution, and a container has no sudo."""
    steps = yaml.safe_load(Path(".github/actions/setup-agent/action.yml").read_text(encoding="utf-8"))["runs"]["steps"]
    # The first step: the others need bash, which the container may lack.
    assert steps[0]["name"] == "Install Python and Node from the system's packages"
    assert steps[0]["shell"] == "sh" and steps[0]["if"] == "${{ inputs['system-packages'] == 'true' }}"
    assert steps[1]["uses"].startswith("actions/setup-python@")
    assert steps[1]["if"] == "${{ inputs['system-packages'] != 'true' }}"
    for path in Path(".github/workflows").glob("valkey-copy-*.yml"):
        for job_id, job in yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"].items():
            for step in job.get("steps") or []:
                if step.get("name") == "CI fix: set up the agent":
                    in_container = step["with"].get("system-packages") == "true"
                    assert in_container == bool(job.get("container")), (path.name, job_id)


def test_the_isolation_tests_also_run_as_root_in_a_container():
    job = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))["jobs"]["isolation-as-root"]
    setup = next(step for step in job["steps"] if step.get("uses") == "./.github/actions/setup-agent")

    assert job["container"] and setup["with"]["system-packages"] == "true"
    assert any("pytest" in step.get("run", "") and "tests/test_isolation.py" in step["run"] for step in job["steps"])


def _agent_automation_files():
    """The agent's own workflows and actions; the valkey-copy-* files mirror Valkey's."""
    return [path for path in _automation_yaml_files() if not path.name.startswith("valkey-copy-")]


def test_each_action_is_pinned_to_one_sha():
    """The agent's workflows, its actions and the copies' injected steps agree on every pin."""
    pins: dict[str, set[str]] = {}
    sources = [path.read_text(encoding="utf-8") for path in _agent_automation_files()]
    sources.append(Path("scripts/valkey_workflows/normalize.py").read_text(encoding="utf-8"))
    for text in sources:
        for action, ref in _USE_RE.findall(text):
            if not action.startswith("./") and _SHA_RE.fullmatch(ref):
                pins.setdefault(action, set()).add(ref)
        for action, ref in re.findall(r'= "([\w.-]+/[\w.-]+)@([0-9a-f]{40})"', text):
            pins.setdefault(action, set()).add(ref)

    assert {action: refs for action, refs in pins.items() if len(refs) > 1} == {}


def test_agent_workflows_run_javascript_actions_on_node_24():
    missing = [
        path.name
        for path in _agent_automation_files()
        if path.parent.name == "workflows"
        and str((yaml.safe_load(path.read_text(encoding="utf-8")).get("env") or {}).get(
            "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24"
        )).lower() != "true"
    ]

    assert missing == []


@pytest.mark.parametrize(
    "filename",
    ["backport-ci-followup.yml", "backport-mark-done-poll.yml", "backport-poll.yml", "backport-sweep.yml"],
)
def test_backport_matrices_come_from_the_shared_workflow(filename):
    workflow = yaml.safe_load((Path(".github/workflows") / filename).read_text(encoding="utf-8"))
    preflight = workflow["jobs"]["preflight"]

    assert preflight["uses"] == "./.github/workflows/backport-matrix.yml"
    assert preflight["if"] == "github.repository == 'valkey-io/valkey-ci-agent'"
    assert preflight["with"]["repo"] == "${{ inputs.repo || '' }}"

