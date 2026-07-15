from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

_USE_RE = re.compile(r"uses:\s*([^@\s]+)@([^#\s]+)")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_PIP_INSTALL_RE = r"(?:python -m )?pip install"
_OLD_REQUIREMENTS_INSTALL_RE = re.compile(rf"{_PIP_INSTALL_RE} (?:-r requirements\.txt|pyyaml)")
_DIRECT_AGENT_SETUP_RE = re.compile(
    rf"actions/setup-python@|{_PIP_INSTALL_RE} \.|{_PIP_INSTALL_RE} -e|npm (?:install|ci).*claude-code"
)
_EXPECTED_CLAUDE_CODE_VERSION = "2.1.175"
_EXPECTED_CLAUDE_CODE_INTEGRITY = (
    "sha512-x37KEw7T1vz/CLkpLYqa8d6eyS/R1777+HMYJRqYf4e5+OhZwF/+d1LoTs5vFXTr"
    "FCWFjZTbWGZksW/gKpvCTQ=="
)
_AI_WORKFLOWS = {
    "backport.yml",
    "ci-fix.yml",
    "monitor-fuzzer.yml",
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


def test_workflow_expression_delimiters_are_balanced():
    for path in _workflow_files():
        text = path.read_text(encoding="utf-8")
        assert text.count("${{") == text.count("}}"), path


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
            if not _SHA_RE.fullmatch(ref):
                offenders.append(f"{path}:{line_no}: {action}@{ref}")

    assert offenders == []


def test_claude_code_install_is_integrity_locked():
    package = yaml.safe_load(
        Path(".github/actions/setup-agent/claude/package.json").read_text(encoding="utf-8")
    )
    lock = yaml.safe_load(
        Path(".github/actions/setup-agent/claude/package-lock.json").read_text(encoding="utf-8")
    )
    assert package["dependencies"]["@anthropic-ai/claude-code"] == _EXPECTED_CLAUDE_CODE_VERSION
    locked = lock["packages"]["node_modules/@anthropic-ai/claude-code"]
    assert locked["version"] == _EXPECTED_CLAUDE_CODE_VERSION
    assert locked["integrity"] == _EXPECTED_CLAUDE_CODE_INTEGRITY


def test_python_installs_require_hash_locks():
    action = Path(".github/actions/setup-agent/action.yml").read_text(encoding="utf-8")
    assert "pip install --require-hashes -r requirements/runtime.txt" in action
    assert "pip install --require-hashes -r requirements/dev.txt" in action
    for lock_name in ("runtime.txt", "dev.txt"):
        lock = Path("requirements", lock_name).read_text(encoding="utf-8")
        assert "--hash=sha256:" in lock
        assert "requests==2.32.5" in lock
        assert "requests==2.34.2" in lock
        assert "urllib3==2.6.3" in lock
        assert "urllib3==2.7.0" in lock


def test_ci_enforces_dependency_vulnerability_and_license_policy():
    workflow = yaml.safe_load(
        Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    assert workflow["jobs"]["test"]["strategy"]["matrix"]["python-version"] == [
        "3.9",
        "3.11",
    ]
    steps = workflow["jobs"]["test"]["steps"]
    review = next(
        step
        for step in steps
        if step["name"] == "Review dependency and license changes"
    )
    assert review["if"] == "github.event_name == 'pull_request'"
    assert review["uses"] == (
        "actions/dependency-review-action@"
        "2031cfc080254a8a887f58cffee85186f0e49e48"
    )
    assert review["with"]["fail-on-severity"] == "moderate"
    assert review["with"]["license-check"] is True
    denied = str(review["with"]["deny-licenses"])
    assert "AGPL-3.0-only" in denied
    assert "GPL-3.0-only" in denied
    audit = next(
        step for step in steps if step["name"] == "Audit locked runtime dependencies"
    )
    assert audit["run"] == "pip-audit --requirement requirements/runtime.txt"
    audit39 = next(
        step
        for step in steps
        if step["name"] == "Audit Python 3.9 dependencies with reviewed backports"
    )
    assert audit39["if"] == "matrix.python-version == '3.9'"
    assert {
        "PYSEC-2026-2275",
        "PYSEC-2026-141",
        "PYSEC-2026-142",
    } == {
        value
        for value in str(audit39["run"]).split()
        if value.startswith("PYSEC-")
    }


def test_ci_runs_linux_and_macos_kernel_boundary_regressions():
    workflow = yaml.safe_load(
        Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    linux_steps = workflow["jobs"]["test"]["steps"]
    assert any(
        step.get("name") == "Run pytest with coverage"
        for step in linux_steps
    )
    macos = workflow["jobs"]["isolation-macos"]
    assert macos["runs-on"] == "macos-latest"
    rendered = json.dumps(macos, sort_keys=True)
    assert "Provision dedicated verifier identity" in rendered
    assert "real_macos_sandbox_denies_network_and_sibling_files" in rendered
    assert "${{ secrets." not in rendered


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
        ".github/workflows/backport.yml": "Generate publisher token",
        ".github/workflows/backport-candidates.yml": "Generate aggregate publisher token",
        ".github/workflows/ci-fix.yml": "Generate publisher token",
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


def test_manual_backport_workflow_separates_trust_domains():
    workflow = yaml.safe_load(
        Path(".github/workflows/backport.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    assert set(jobs) == {
        "discovery",
        "ai-prepare",
        "validation",
        "ai-repair",
        "repair-validation",
        "publisher",
        "failure-reporter",
        "validation-outcome",
        "aggregate-handoff",
    }

    discovery_text = json.dumps(jobs["discovery"], sort_keys=True)
    ai_text = json.dumps(jobs["ai-prepare"], sort_keys=True)
    validation_text = json.dumps(jobs["validation"], sort_keys=True)
    repair_text = json.dumps(jobs["ai-repair"], sort_keys=True)
    repair_validation_text = json.dumps(
        jobs["repair-validation"],
        sort_keys=True,
    )
    publisher_text = json.dumps(jobs["publisher"], sort_keys=True)

    assert jobs["discovery"]["permissions"] == {"contents": "read"}
    assert '"permission-contents": "read"' in discovery_text
    assert '"permission-contents": "write"' not in discovery_text
    assert "create-github-app-token" not in ai_text
    assert "create-github-app-token" not in validation_text
    assert "AI_GATEWAY_TOKEN" not in validation_text
    assert "create-github-app-token" not in repair_text
    assert "PUBLISH_GITHUB_TOKEN" not in repair_text
    assert "create-github-app-token" not in repair_validation_text
    assert "AI_GATEWAY_TOKEN" not in repair_validation_text
    validation_runs = "\n".join(
        step.get("run", "") for step in jobs["validation"]["steps"]
    )
    repair_runs = "\n".join(
        step.get("run", "") for step in jobs["ai-repair"]["steps"]
    )
    repair_validation_runs = "\n".join(
        step.get("run", "") for step in jobs["repair-validation"]["steps"]
    )
    publisher_runs = "\n".join(
        step.get("run", "") for step in jobs["publisher"]["steps"]
    )
    assert "scripts.backport.phased" in validation_runs
    assert "\n  validate \\" in validation_runs
    assert "\n  repair-validation \\" in repair_runs
    assert "\n  validate \\" in repair_validation_runs
    assert "\n  validate \\" not in publisher_runs
    assert "repair-validation" not in publisher_runs
    assert "needs.repair-validation.outputs.status == 'passed'" in (
        jobs["publisher"]["if"]
    )
    outcome = jobs["validation-outcome"]
    assert outcome["permissions"] == {}
    assert "always()" in outcome["if"]
    outcome_runs = "\n".join(
        step.get("run", "") for step in outcome["steps"]
    )
    assert "no repaired tree passed validation" in outcome_runs
    assert "PUBLISHER_RESULT" in outcome_runs

    publisher_steps = jobs["publisher"]["steps"]
    preflight = next(
        index
        for index, step in enumerate(publisher_steps)
        if step["name"] == "Verify artifact before minting write token"
    )
    token = next(
        index
        for index, step in enumerate(publisher_steps)
        if step["name"] == "Generate publisher token"
    )
    publish = next(
        index
        for index, step in enumerate(publisher_steps)
        if step["name"] == "Publish validated backport"
    )
    assert preflight < token < publish
    assert publisher_steps[token]["with"]["permission-contents"] == "write"
    assert "PUBLISH_GITHUB_TOKEN" in json.dumps(publisher_steps[publish])

    reporter_steps = jobs["failure-reporter"]["steps"]
    reporter_names = [step["name"] for step in reporter_steps]
    assert reporter_names.index(
        "Verify failure before minting comment token",
    ) < reporter_names.index(
        "Generate comment-only reporter token",
    ) < reporter_names.index("Report backport needs attention")
    reporter_text = json.dumps(jobs["failure-reporter"], sort_keys=True)
    assert '"permission-contents": "write"' not in reporter_text
    assert '"permission-issues": "write"' in reporter_text
    assert "preflight-failure-report" in reporter_text


def test_ci_fix_workflow_separates_ai_validation_and_publication():
    workflow = yaml.safe_load(
        Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    assert set(jobs) == {
        "discovery",
        "ai-prepare",
        "validation-linux",
        "validation-macos",
        "publisher",
        "refusal-reporter",
    }

    for name in ("validation-linux", "validation-macos"):
        rendered = json.dumps(jobs[name], sort_keys=True)
        assert "create-github-app-token" not in rendered
        assert "AI_GATEWAY_TOKEN" not in rendered
        assert "${{ secrets." not in rendered
        runs = "\n".join(step.get("run", "") for step in jobs[name]["steps"])
        assert "scripts.ci_fix.phased validate" in runs
        assert "Verify failing baseline and patched tree" in rendered

    ai_rendered = json.dumps(jobs["ai-prepare"], sort_keys=True)
    assert "create-github-app-token" not in ai_rendered
    assert "PUBLISH_GITHUB_TOKEN" not in ai_rendered

    publisher_steps = jobs["publisher"]["steps"]
    publisher_names = [step["name"] for step in publisher_steps]
    assert publisher_names.index("Verify artifact before minting write token") < (
        publisher_names.index("Generate publisher token")
    ) < publisher_names.index("Publish validated CI fix")
    publisher_rendered = json.dumps(jobs["publisher"], sort_keys=True)
    assert "scripts.ci_fix.phased validate" not in publisher_rendered
    assert '"permission-contents": "write"' in publisher_rendered

    reporter_steps = jobs["refusal-reporter"]["steps"]
    reporter_names = [step["name"] for step in reporter_steps]
    assert reporter_names.index("Verify refusal before minting comment token") < (
        reporter_names.index("Generate comment-only reporter token")
    ) < reporter_names.index("Report refusal")
    reporter_rendered = json.dumps(jobs["refusal-reporter"], sort_keys=True)
    assert '"permission-contents": "write"' not in reporter_rendered


def test_fuzzer_workflow_separates_discovery_ai_and_publication():
    workflow = yaml.safe_load(
        Path(".github/workflows/monitor-fuzzer.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    assert set(jobs) == {"discovery", "analysis", "publisher"}

    discovery = json.dumps(jobs["discovery"], sort_keys=True)
    analysis = json.dumps(jobs["analysis"], sort_keys=True)
    publisher = json.dumps(jobs["publisher"], sort_keys=True)
    assert '"permission-actions": "read"' in discovery
    assert '"permission-issues": "write"' not in discovery
    assert "setup-ai-runtime" not in discovery
    assert "create-github-app-token" not in analysis
    assert "READ_GITHUB_TOKEN" not in analysis
    assert "PUBLISH_GITHUB_TOKEN" not in analysis
    assert "setup-ai-runtime" in analysis

    steps = jobs["publisher"]["steps"]
    control_step = next(
        step for step in steps
        if step["name"] == "Verify analyses before minting issue token"
    )
    publish_step = next(
        step for step in steps
        if step["name"] == "Publish findings and advance durable cursor"
    )
    assert "AI_GATEWAY_TOKEN" in json.dumps(control_step)
    assert "AI_GATEWAY_TOKEN" not in json.dumps(publish_step)
    names = [step["name"] for step in steps]
    assert names.index("Verify analyses before minting issue token") < (
        names.index("Generate issue-only publisher token")
    ) < names.index("Publish findings and advance durable cursor")
    assert '"permission-issues": "write"' in publisher
    assert '"permission-contents": "write"' not in publisher


def test_issue_publishers_are_serialized():
    for name in ("monitor-fuzzer.yml", "test-failure-detector-sweep.yml"):
        workflow = yaml.safe_load(
            Path(".github/workflows", name).read_text(encoding="utf-8")
        )
        concurrency = workflow["concurrency"]
        assert concurrency["cancel-in-progress"] is False
        assert "publisher" in concurrency["group"]


def test_sweep_and_poller_only_delegate_to_phased_candidate_workflow():
    for name in ("backport-sweep.yml", "backport-poll.yml"):
        path = Path(".github/workflows", name)
        text = path.read_text(encoding="utf-8")
        workflow = yaml.safe_load(text)
        assert set(workflow["jobs"]) == {"candidates"}
        job = workflow["jobs"]["candidates"]
        assert job["uses"] == "./.github/workflows/backport-candidates.yml"
        rendered = text
        assert "create-github-app-token" not in rendered
        assert "permission-contents\": \"write" not in rendered
        assert "TARGET_TOKEN" not in rendered
        assert "setup-ai-runtime" not in rendered

    workflow = yaml.safe_load(
        Path(".github/workflows/backport-candidates.yml").read_text(encoding="utf-8")
    )
    assert set(workflow["jobs"]) == {
        "discovery",
        "backport",
        "aggregate-prepare",
        "aggregate-validation",
        "aggregate-publisher",
        "aggregate-outcome",
    }
    discovery = json.dumps(workflow["jobs"]["discovery"], sort_keys=True)
    assert '"permission-contents": "read"' in discovery
    assert '"permission-contents": "write"' not in discovery
    backport = workflow["jobs"]["backport"]
    assert backport["uses"] == "./.github/workflows/backport.yml"
    assert backport["strategy"]["max-parallel"] == 2
    assert backport["with"]["publication_mode"] == "aggregate"

    for name in ("aggregate-prepare", "aggregate-validation"):
        rendered = json.dumps(workflow["jobs"][name], sort_keys=True)
        assert "create-github-app-token" not in rendered
        assert "${{ secrets." not in rendered
        assert '"permission-contents": "write"' not in rendered
    aggregate_publisher = workflow["jobs"]["aggregate-publisher"]
    names = [step["name"] for step in aggregate_publisher["steps"]]
    assert names.index("Verify aggregates before minting write token") < (
        names.index("Generate aggregate publisher token")
    ) < names.index("Publish rolling aggregate PRs")
    rendered_publisher = json.dumps(aggregate_publisher, sort_keys=True)
    assert '"permission-contents": "write"' in rendered_publisher
    assert "scripts.backport.aggregate" in rendered_publisher
    verification = next(
        step["run"] for step in aggregate_publisher["steps"]
        if step["name"] == "Verify aggregates before minting write token"
    )
    assert verification.index("preflight-publish") < verification.index(
        "--admit-publication-key"
    )
    assert "aggregate-validation-index.json" not in verification
    aggregate_prepare = next(
        step["run"]
        for step in workflow["jobs"]["aggregate-prepare"]["steps"]
        if step["name"] == "Prepare rolling aggregate without credentials"
    )
    assert '--max-candidates "${{ inputs.max_candidates }}"' in aggregate_prepare
    reusable = Path(".github/workflows/backport.yml").read_text(encoding="utf-8")
    assert reusable.count("backport-aggregate-result-") == 2
    assert "Upload aggregate failure handoff" in reusable


def test_backport_zero_candidate_limit_is_not_replaced_by_a_default():
    for name in ("backport-sweep.yml", "backport-poll.yml"):
        workflow = yaml.safe_load(
            Path(".github/workflows", name).read_text(encoding="utf-8"),
        )
        candidate_job = workflow["jobs"]["candidates"]
        assert candidate_job["with"]["max_candidates"] == (
            "${{ github.event_name != 'workflow_dispatch' && 2 || "
            "inputs.max_candidates }}"
        )
        description = workflow[True]["workflow_dispatch"]["inputs"][
            "max_candidates"
        ]["description"]
        assert "0 = no caller cap" in description


def test_ai_workflows_use_isolated_gateway_without_aws_credentials():
    forbidden = (
        "AWS_ROLE_ARN",
        "CLAUDE_CODE_USE_BEDROCK",
        "configure-aws-credentials",
        "id-token: write",
        'install-claude: "true"',
    )
    offenders = []
    for name in sorted(_AI_WORKFLOWS):
        path = Path(".github/workflows", name)
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            if marker in text:
                offenders.append(f"{path}: contains forbidden marker {marker!r}")

        workflow = yaml.safe_load(text)
        ai_jobs = []
        for job_name, job in workflow["jobs"].items():
            steps = job.get("steps", [])
            uses = [step.get("uses") for step in steps]
            if "./.github/actions/setup-ai-runtime" not in uses:
                continue
            ai_jobs.append(job_name)
            setup_index = uses.index("./.github/actions/setup-ai-runtime")
            cleanup_index = uses.index("./.github/actions/cleanup-ai-runtime")
            if cleanup_index <= setup_index:
                offenders.append(f"{path}:{job_name}: cleanup does not follow setup")
            cleanup = steps[cleanup_index]
            if cleanup.get("if") != "always()":
                offenders.append(f"{path}:{job_name}: cleanup is not unconditional")
            setup = steps[setup_index].get("with", {})
            if set(setup) != {"upstream-url", "gateway-token"}:
                offenders.append(f"{path}:{job_name}: unexpected AI runtime inputs")
        expected_jobs = (
            {"ai-prepare", "ai-repair"}
            if name == "backport.yml"
            else {"ai-prepare"} if name == "ci-fix.yml" else {"analysis"}
        )
        if set(ai_jobs) != expected_jobs:
            offenders.append(
                f"{path}: expected isolated AI jobs {sorted(expected_jobs)!r}, "
                f"found {ai_jobs!r}"
            )

    assert offenders == []


def test_ai_runtime_runs_adversarial_boundary_probe():
    action = Path(
        ".github/actions/setup-ai-runtime/action.yml",
    ).read_text(encoding="utf-8")

    assert "Probe AI runtime isolation" in action
    assert "AI_RUNTIME_PARENT_SECRET=" in action
    assert "/proc/[0-9]*/environ" in action
    assert 'test ! -e "${HOME}/.aws/credentials"' in action
    assert 'test ! -e "${HOME}/.gitconfig"' in action
    assert "/host-home/.aws/credentials" in action
    assert "/host-home/.gitconfig" in action
    assert "/neighbor-workspace/.github/workflows/ci.yml" in action
    assert "test ! -S /var/run/docker.sock" in action
    assert "! touch /ai-runtime-root-write" in action
    assert "/dev/tcp/169.254.169.254/80" in action
    assert "GET /health HTTP/1.0" in action


def test_ai_workflow_jobs_have_global_and_feature_kill_switches():
    feature_switches = {
        "backport.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "ci-fix.yml": "VALKEY_CI_AGENT_DISABLE_CI_FIX",
        "monitor-fuzzer.yml": "VALKEY_CI_AGENT_DISABLE_FUZZER",
    }
    offenders = []
    for name, feature_switch in feature_switches.items():
        workflow = yaml.safe_load(
            Path(".github/workflows", name).read_text(encoding="utf-8")
        )
        for job_name, job in workflow["jobs"].items():
            condition = str(job.get("if", ""))
            if "VALKEY_CI_AGENT_KILL_SWITCH" not in condition:
                offenders.append(f"{name}:{job_name}: missing global kill switch")
            if feature_switch not in condition:
                offenders.append(
                    f"{name}:{job_name}: missing {feature_switch}"
                )
    assert offenders == []


def test_ai_publishers_reserve_central_publication_budget_before_token():
    publishers = {
        "backport.yml": ("publisher", "Generate publisher token"),
        "ci-fix.yml": ("publisher", "Generate publisher token"),
        "monitor-fuzzer.yml": ("publisher", "Generate issue-only publisher token"),
    }
    for name, (job_name, token_step) in publishers.items():
        workflow = yaml.safe_load(
            Path(".github/workflows", name).read_text(encoding="utf-8")
        )
        steps = workflow["jobs"][job_name]["steps"]
        token_index = next(
            index for index, step in enumerate(steps)
            if step["name"] == token_step
        )
        admission_indices = [
            index for index, step in enumerate(steps)
            if "--admit-publication-key" in step.get("run", "")
        ]
        assert admission_indices
        assert max(admission_indices) < token_index


def test_every_write_capable_app_token_has_pre_token_publication_admission():
    offenders = []
    for path in _workflow_files():
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        for job_name, job in workflow.get("jobs", {}).items():
            steps = job.get("steps", [])
            for index, step in enumerate(steps):
                if "actions/create-github-app-token@" not in step.get("uses", ""):
                    continue
                options = step.get("with", {})
                write_capable = any(
                    key.startswith("permission-") and value == "write"
                    for key, value in options.items()
                )
                if not write_capable:
                    continue
                earlier_runs = "\n".join(
                    candidate.get("run", "")
                    for candidate in steps[:index]
                )
                if "scripts.common.operational_controls" not in earlier_runs:
                    offenders.append(
                        f"{path.name}:{job_name}:{step.get('name')}: "
                        "missing pre-token operational control"
                    )
                if "--admit-publication-key" not in earlier_runs:
                    offenders.append(
                        f"{path.name}:{job_name}:{step.get('name')}: "
                        "missing pre-token publication admission"
                    )
    assert offenders == []


def test_operational_workflows_have_feature_kill_switches():
    switches = {
        "backport-mark-done-poll.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "backport-poll.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "backport-sweep.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "manual-backport.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "manual-revert-commit.yml": "VALKEY_CI_AGENT_DISABLE_BACKPORT",
        "ci-fix-comment-poll.yml": "VALKEY_CI_AGENT_DISABLE_CI_FIX",
        "test-failure-detector-sweep.yml": (
            "VALKEY_CI_AGENT_DISABLE_TEST_FAILURE_DETECTOR"
        ),
        "metadata-reconcile.yml": "VALKEY_CI_AGENT_DISABLE_METADATA_RECONCILER",
    }
    offenders = []
    for name, feature_switch in switches.items():
        workflow = yaml.safe_load(
            Path(".github/workflows", name).read_text(encoding="utf-8")
        )
        for job_name, job in workflow["jobs"].items():
            condition = str(job.get("if", ""))
            if "VALKEY_CI_AGENT_KILL_SWITCH" not in condition:
                offenders.append(f"{name}:{job_name}: missing global kill switch")
            if feature_switch not in condition:
                offenders.append(f"{name}:{job_name}: missing {feature_switch}")
    assert offenders == []
