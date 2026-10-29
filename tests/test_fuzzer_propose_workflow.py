"""Contract tests for the fuzzer scenario proposal workflow."""

from __future__ import annotations

from pathlib import Path

import yaml

_WORKFLOW = Path(".github/workflows/fuzzer-propose.yml")


def _workflow() -> dict:
    # BaseLoader keeps the literal `on` key and every scalar as a string.
    return yaml.load(_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _steps() -> list[dict]:
    return _workflow()["jobs"]["propose"]["steps"]


def _step(name: str) -> dict:
    return next(step for step in _steps() if step.get("name") == name)


def test_dispatch_only_and_dry_run_by_default() -> None:
    on = _workflow()["on"]
    assert list(on) == ["workflow_dispatch"]
    inputs = on["workflow_dispatch"]["inputs"]
    assert list(inputs) == ["pr_number", "count", "dry_run", "dispatch_batch"]
    assert inputs["pr_number"]["required"] == "true"
    assert inputs["dry_run"]["default"] == "true"
    assert inputs["dispatch_batch"]["default"] == "false"


def test_job_permissions_are_minimal() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {}
    assert workflow["jobs"]["propose"]["permissions"] == {"contents": "read", "id-token": "write"}


def test_read_token_covers_only_valkey_and_only_reads() -> None:
    step = _step("Generate read token")
    assert step["if"] == "env.DRY_RUN != 'true'"
    with_ = step["with"]
    assert with_["repositories"] == "valkey"
    permissions = {key: value for key, value in with_.items() if key.startswith("permission-")}
    assert permissions == {
        "permission-contents": "read",
        "permission-pull-requests": "read",
        "permission-metadata": "read",
    }


def test_dispatch_token_is_separate_and_only_when_dispatching() -> None:
    step = _step("Generate dispatch token")
    assert step["if"] == "env.DISPATCH == 'true'"
    with_ = step["with"]
    assert with_["repositories"] == "valkey-fuzzer"
    permissions = {key: value for key, value in with_.items() if key.startswith("permission-")}
    assert permissions == {"permission-actions": "write", "permission-metadata": "read"}
    # A dry run never dispatches, whatever dispatch_batch says.
    env = _workflow()["jobs"]["propose"]["env"]
    assert env["DISPATCH"] == "${{ !inputs.dry_run && inputs.dispatch_batch && 'true' || 'false' }}"
    assert _step("Dispatch the PR batch")["if"] == "env.DISPATCH == 'true'"


def test_claude_step_never_sees_the_dispatch_token() -> None:
    names = [step.get("name") for step in _steps()]
    assert names.index("Propose scenarios") < names.index("Generate dispatch token")
    propose = _step("Propose scenarios")
    assert propose["env"]["TARGET_TOKEN"] == "${{ steps.read-token.outputs.token }}"
    assert _step("Dispatch the PR batch")["env"]["TARGET_TOKEN"] == "${{ steps.dispatch-token.outputs.token }}"


def test_inputs_reach_shell_through_env_and_are_checked_first() -> None:
    text = _WORKFLOW.read_text(encoding="utf-8")
    for step in _steps():
        run = step.get("run", "")
        assert "${{ inputs." not in run, step.get("name")
    assert _steps()[0]["name"] == "Check inputs"
    check = _step("Check inputs")["run"]
    assert "^[1-9][0-9]*$" in check and "^([1-9]|10)$" in check
    assert '--pr "${PR_NUMBER}"' in text
    assert "--dry-run" in _step("Propose scenarios")["run"]


def test_fuzzer_is_checked_out_without_credentials_and_installed_apart() -> None:
    checkout = _step("Check out valkey-fuzzer")
    assert checkout["with"]["repository"] == "valkey-io/valkey-fuzzer"
    assert checkout["with"]["persist-credentials"] == "false"
    install = _step("Install valkey-fuzzer")["run"]
    assert 'python -m venv "${RUNNER_TEMP}/fuzzer-venv"' in install


def test_proposals_are_uploaded_even_when_a_step_fails() -> None:
    upload = _step("Upload proposals")
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "fuzzer-proposals"
    assert upload["with"]["name"] == "fuzzer-proposals-${{ github.run_id }}"
