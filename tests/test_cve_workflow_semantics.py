from __future__ import annotations

import re
from pathlib import Path

import yaml

_WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/cve-scan.yml"
_SHA_RE = re.compile(r"^[^@]+@[0-9a-f]{40}$")


def _workflow() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text())


def _step(job: dict, name: str) -> dict:
    return next(step for step in job["steps"] if step.get("name") == name)


def test_rebuild_job_requires_canonical_main_targets_and_live_mode() -> None:
    expression = _workflow()["jobs"]["rebuild"]["if"]
    for required in (
        "github.repository == 'valkey-io/valkey-ci-agent'",
        "github.ref == 'refs/heads/main'",
        "needs.scan.outputs.rebuild_required == 'true'",
        "needs.scan.outputs.versions != ''",
        "needs.scan.outputs.targets != ''",
        "dry_run != 'true'",
    ):
        assert required in expression


def test_scan_has_no_write_permissions_or_privileged_token() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {"contents": "read"}
    scan = workflow["jobs"]["scan"]
    assert all("create-github-app-token" not in step.get("uses", "") for step in scan["steps"])
    assert all("setup-qemu" not in step.get("uses", "") for step in scan["steps"])


def test_manual_runs_default_to_dry_run() -> None:
    workflow = _workflow()
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["workflow_dispatch"]["inputs"]["dry_run"]["default"] is True


def test_external_actions_are_sha_pinned() -> None:
    offenders = []
    for job_name, job in _workflow()["jobs"].items():
        for step in job.get("steps", []):
            uses = step.get("uses")
            if uses and not uses.startswith("./") and not _SHA_RE.fullmatch(uses):
                offenders.append(f"{job_name}: {uses}")
    assert not offenders


def test_dispatch_sends_correlation_and_exact_target_contract() -> None:
    rebuild = _workflow()["jobs"]["rebuild"]
    dispatch = _step(rebuild, "Dispatch candidate workflow")
    run = dispatch["run"]
    assert "--ref mainline" in run
    assert '--field "correlation_id=${CORRELATION_ID}"' in run
    assert '--field "targeted_findings=${TARGETS}"' in run
    assert dispatch["env"]["CORRELATION_ID"] == "${{ github.run_id }}-${{ github.run_attempt }}"


def test_locate_uses_exact_run_name_and_mainline_not_timestamp() -> None:
    locate = _step(_workflow()["jobs"]["rebuild"], "Locate correlated candidate run")
    run = locate["run"]
    assert 'expected_title="CVE rebuild ${CORRELATION_ID}"' in run
    assert ".displayTitle == $title" in run
    assert "--branch mainline" in run
    assert "dispatch_ts" not in run.lower()
    assert "--created" not in run


def test_watcher_leaves_job_time_for_reporting() -> None:
    rebuild = _workflow()["jobs"]["rebuild"]
    watch = _step(rebuild, "Wait for candidate workflow")
    assert watch["timeout-minutes"] < rebuild["timeout-minutes"]
    assert "--exit-status" in watch["run"]
    assert _step(rebuild, "Capture downstream conclusion")["if"].startswith("always()")


def test_only_dispatch_step_uses_app_token() -> None:
    rebuild = _workflow()["jobs"]["rebuild"]
    dispatch = _step(rebuild, "Dispatch candidate workflow")
    assert dispatch["env"]["GH_TOKEN"] == "${{ steps.token.outputs.token }}"
    for name in (
        "Locate correlated candidate run",
        "Wait for candidate workflow",
        "Capture downstream conclusion",
    ):
        assert _step(rebuild, name)["env"]["GH_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"


def test_slack_reports_promotion_not_dispatch_acceptance() -> None:
    rebuild = _workflow()["jobs"]["rebuild"]
    compose = _step(rebuild, "Compose Slack notification")
    assert "Verified CVE candidates were promoted" in compose["run"]
    assert compose["if"] == "always()"
    notify = _step(rebuild, "Notify Slack")
    assert notify["continue-on-error"] is True
