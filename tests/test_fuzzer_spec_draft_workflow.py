"""Contract tests for the fuzzer spec draft workflow."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_WORKFLOW = Path(".github/workflows/fuzzer-spec-draft.yml")
# valkey-fuzzer's TLA+ Model Check workflow pins the same release and digest.
_TLA_TOOLS_VERSION = "v1.7.4"
_TLA_TOOLS_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"


def _workflow() -> dict:
    # BaseLoader keeps the literal `on` key and every scalar as a string.
    return yaml.load(_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _job() -> dict:
    return _workflow()["jobs"]["draft"]


def _steps() -> list[dict]:
    return _job()["steps"]


def _step(name: str) -> dict:
    return next(step for step in _steps() if step.get("name") == name)


def test_dispatch_only_and_dry_run_by_default() -> None:
    on = _workflow()["on"]
    assert list(on) == ["workflow_dispatch"]
    inputs = on["workflow_dispatch"]["inputs"]
    assert list(inputs) == ["pr_number", "dry_run", "dispatch_batch"]
    assert inputs["pr_number"]["required"] == "true"
    assert inputs["dry_run"]["default"] == "true"
    assert inputs["dispatch_batch"]["default"] == "false"


def test_job_permissions_are_minimal() -> None:
    assert _workflow()["permissions"] == {}
    assert _job()["permissions"] == {"contents": "read", "id-token": "write"}


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
    assert _job()["env"]["DISPATCH"] == "${{ !inputs.dry_run && inputs.dispatch_batch && 'true' || 'false' }}"
    assert _step("Dispatch the PR batch")["if"] == "env.DISPATCH == 'true'"
    assert "spec_draft dispatch" in _step("Dispatch the PR batch")["run"]


def test_claude_step_never_sees_the_dispatch_token() -> None:
    names = [step.get("name") for step in _steps()]
    assert names.index("Draft the spec bundle") < names.index("Generate dispatch token")
    assert _step("Draft the spec bundle")["env"]["TARGET_TOKEN"] == "${{ steps.read-token.outputs.token }}"
    assert _step("Dispatch the PR batch")["env"]["TARGET_TOKEN"] == "${{ steps.dispatch-token.outputs.token }}"


def test_inputs_reach_shell_through_env_and_are_checked_first() -> None:
    for step in _steps():
        assert "${{ inputs." not in step.get("run", ""), step.get("name")
    assert _steps()[0]["name"] == "Check inputs"
    assert "^[1-9][0-9]*$" in _step("Check inputs")["run"]
    draft = _step("Draft the spec bundle")["run"]
    assert '--pr "${PR_NUMBER}"' in draft
    assert "--dry-run" in draft
    assert '--tla2tools "${TLA2TOOLS_JAR}"' in draft


def test_fuzzer_is_checked_out_without_credentials_and_installed_apart() -> None:
    checkout = _step("Check out valkey-fuzzer")
    assert checkout["with"]["repository"] == "valkey-io/valkey-fuzzer"
    assert checkout["with"]["persist-credentials"] == "false"
    install = _step("Install valkey-fuzzer")["run"]
    assert 'python -m venv "${RUNNER_TEMP}/fuzzer-venv"' in install


def test_tla_tools_are_the_fuzzers_pinned_and_checksummed_jar() -> None:
    env = _workflow()["env"]
    assert env["TLA_TOOLS_VERSION"] == _TLA_TOOLS_VERSION
    assert env["TLA_TOOLS_SHA256"] == _TLA_TOOLS_SHA256
    download = _step("Download tla2tools.jar")["run"]
    assert "releases/download/${TLA_TOOLS_VERSION}/tla2tools.jar" in download
    assert 'echo "${TLA_TOOLS_SHA256}  ${RUNNER_TEMP}/tla2tools.jar" | sha256sum -c -' in download
    # The digest is checked before the jar is handed to anything.
    assert download.index("sha256sum -c -") < download.index("TLA2TOOLS_JAR=")
    java = _step("Set up Java")
    assert java["uses"].startswith("actions/setup-java@")
    assert re.fullmatch(r"actions/setup-java@[0-9a-f]{40}", java["uses"])
    assert java["with"]["distribution"] == "temurin"
    names = [step.get("name") for step in _steps()]
    assert names.index("Download tla2tools.jar") < names.index("Draft the spec bundle")


def test_aws_credentials_outlast_the_job() -> None:
    aws = _step("Configure AWS credentials")
    assert aws["if"] == "env.DRY_RUN != 'true'"
    assert int(aws["with"]["role-duration-seconds"]) >= int(_job()["timeout-minutes"]) * 60


def test_the_draft_is_uploaded_even_when_a_step_fails() -> None:
    upload = _step("Upload spec draft")
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "fuzzer-spec-draft"
    assert upload["with"]["name"] == "fuzzer-spec-draft-${{ github.run_id }}"
