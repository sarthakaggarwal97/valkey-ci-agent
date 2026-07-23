from __future__ import annotations

import re
from pathlib import Path

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
        ".github/workflows/backport-poll.yml": "Generate GitHub App token",
        ".github/workflows/backport-sweep.yml": "Generate GitHub App token",
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


def test_ci_fix_push_token_cannot_update_workflows():
    """Authored workflow changes are handoff-only and the token enforces it."""
    text = Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    step = text.split("- name: Generate GitHub App token", 1)[1].split(
        "      - name:", 1,
    )[0]
    assert "permission-contents: write" in step
    assert "permission-workflows: write" not in step


def test_ci_fix_agent_verifier_token_has_only_actions_and_metadata():
    text = Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    step = text.split("- name: Generate agent-repo verifier token", 1)[1].split(
        "      - name:", 1,
    )[0]
    requested = {
        line.strip()
        for line in step.splitlines()
        if line.strip().startswith("permission-")
    }

    assert requested == {
        "permission-actions: write",
        "permission-metadata: read",
    }
    assert "permission-contents:" not in step


def test_ci_fix_refreshes_restricted_tokens_and_trusts_only_poller_comment_ids():
    text = Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")

    assert "steps.generate-token.outputs.installation-id" in text
    assert "steps.agent-token.outputs.installation-id" in text
    assert "CI_FIX_APP_PRIVATE_KEY: ${{ secrets.VALKEYRIE_BOT_PRIVATE_KEY }}" in text
    comment_id_line = next(
        line for line in text.splitlines() if "CI_FIX_COMMENT_ID:" in line
    )
    assert "github.actor == 'valkeyrie-ops[bot]'" in comment_id_line
    assert "inputs.comment_id" in comment_id_line
    assert "[ci-fix-comment:" in text


def test_ci_fix_verification_uses_github_hosted_runners():
    ci_fix = yaml.safe_load(
        Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    )
    linux = yaml.safe_load(
        Path(".github/workflows/ci-fix-verify-linux.yml").read_text(
            encoding="utf-8"
        )
    )
    macos = yaml.safe_load(
        Path(".github/workflows/ci-fix-verify-macos.yml").read_text(
            encoding="utf-8"
        )
    )

    assert ci_fix["jobs"]["ci-fix"]["runs-on"] == "ubuntu-latest"
    assert linux["jobs"]["verify-linux"]["runs-on"] == "ubuntu-latest"
    assert macos["jobs"]["verify-macos"]["runs-on"] == "macos-latest"


def test_agent_owned_verifiers_are_no_secret_read_only_jobs():
    for name in ("linux", "macos"):
        path = Path(f".github/workflows/ci-fix-verify-{name}.yml")
        text = path.read_text(encoding="utf-8")
        workflow = yaml.safe_load(text)

        assert workflow["permissions"] == {}
        assert "${{ secrets." not in text
        assert "id-token:" not in text
        assert "GITHUB_TOKEN" not in text
        assert "github.token" not in text
        assert "persist-credentials: false" in text

    macos = Path(".github/workflows/ci-fix-verify-macos.yml").read_text(
        encoding="utf-8"
    )
    assert "base64 -D" in macos
    assert "base64 --decode" not in macos


def test_agent_owned_verifiers_share_sampling_input_contract():
    required = {
        "target_repo",
        "head_sha",
        "patch_b64",
        "verify_command",
        "workdir",
        "container_image",
        "phase",
        "repetition",
        "repetition_count",
        "correlation",
    }
    for name in ("linux", "macos"):
        workflow = yaml.safe_load(
            Path(f".github/workflows/ci-fix-verify-{name}.yml").read_text(
                encoding="utf-8"
            )
        )
        trigger = workflow.get("on", workflow.get(True))
        assert set(trigger["workflow_dispatch"]["inputs"]) == required


def test_linux_verifier_keeps_container_execution_sandboxed():
    text = Path(".github/workflows/ci-fix-verify-linux.yml").read_text(
        encoding="utf-8"
    )

    assert "--network none" in text
    assert "--cap-drop ALL" in text
    assert "--security-opt no-new-privileges" in text
    assert '--user "$(id -u):$(id -g)"' in text
    assert "--entrypoint /bin/sh" in text
    assert "/bin/sh -e -c" in text


def test_agent_owned_verifiers_run_host_recipes_fail_fast():
    for name in ("linux", "macos"):
        text = Path(
            f".github/workflows/ci-fix-verify-{name}.yml"
        ).read_text(encoding="utf-8")

        assert "/bin/bash --noprofile --norc -e -o pipefail -c" in text


def test_ci_fix_controller_has_no_in_process_verification_fallback():
    pipeline = Path("scripts/ci_fix/pipeline.py").read_text(encoding="utf-8")

    assert "run_verification_command" not in pipeline
    assert "run_fix_loop" not in pipeline
