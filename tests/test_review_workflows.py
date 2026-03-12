"""Regression tests for PR reviewer workflow wiring."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _get_on_block(workflow: dict) -> dict:
    if "on" in workflow:
        return workflow["on"]
    return workflow[True]


def test_review_workflow_checks_out_bot_repository() -> None:
    workflow = _load_yaml(REPO_ROOT / ".github/workflows/review-pr.yml")
    on_block = _get_on_block(workflow)
    inputs = on_block["workflow_call"]["inputs"]

    assert "bot_repository" in inputs
    assert "bot_ref" in inputs

    checkout_step = next(
        step
        for step in workflow["jobs"]["review"]["steps"]
        if step["name"] == "Checkout bot repository"
    )
    assert checkout_step["with"]["repository"] == "${{ inputs.bot_repository }}"
    assert checkout_step["with"]["ref"] == "${{ inputs.bot_ref }}"


def test_example_pr_review_caller_passes_bot_checkout_inputs() -> None:
    workflow = _load_yaml(REPO_ROOT / "examples/pr-review-caller-workflow.yml")

    review_with = workflow["jobs"]["review"]["with"]
    assert review_with["bot_repository"] == "valkey-io/valkey-ci-bot"
    assert review_with["bot_ref"] == "v1"
