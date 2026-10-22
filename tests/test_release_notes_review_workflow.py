from pathlib import Path

import yaml

from scripts.ai.runtime import AGENT_PROFILES

_WATCH = Path(".github/workflows/release-notes-review-watch.yml")
_CUT = Path(".github/workflows/release-notes-cut.yml")
_HANDLER = Path(".github/workflows/release-notes-review.yml")


def _workflow(path: Path) -> dict:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _step(workflow: dict, job: str, name: str) -> dict:
    return next(
        step
        for step in workflow["jobs"][job]["steps"]
        if step.get("name") == name
    )


def test_one_watcher_per_release_pr_replaces_the_hourly_scan() -> None:
    assert not Path(".github/workflows/release-notes-review-poll.yml").exists()
    workflow = _workflow(_WATCH)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert list(workflow["on"]) == ["workflow_dispatch"]
    assert list(inputs) == ["repo", "pr", "stop"]
    assert inputs["stop"]["default"] == "false"

    job = workflow["jobs"]["watch"]
    token = _step(workflow, "watch", "Generate App token")
    run = _step(workflow, "watch", "Watch and dispatch")
    again = _step(workflow, "watch", "Keep watching")

    assert "!inputs.stop" in job["if"]
    assert job["timeout-minutes"] == "65"
    assert job["concurrency"]["group"] == (
        "release-notes-review-watch-${{ inputs.repo }}-${{ inputs.pr }}"
    )
    assert job["concurrency"]["cancel-in-progress"] == "false"
    assert job["permissions"] == {"contents": "read", "actions": "write"}
    assert token["with"]["repositories"].splitlines() == ["${{ inputs.repo }}", "valkey-ci-agent"]
    assert {
        key: value
        for key, value in token["with"].items()
        if key.startswith("permission-")
    } == {
        "permission-actions": "write",
        "permission-members": "read",
        "permission-pull-requests": "write",
        "permission-metadata": "read",
    }
    # Always a sustained watch, including when started by hand to resume one.
    assert run["env"]["RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS"] == "300"
    assert run["env"]["RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS"] == "3300"
    assert "--repo" in run["run"] and "--pr" in run["run"]
    assert again["if"] == "${{ steps.watch.outputs.continue == 'true' }}"
    assert "gh workflow run release-notes-review-watch.yml" in again["run"]


def test_stop_cancels_only_that_prs_watcher() -> None:
    workflow = _workflow(_WATCH)
    job = workflow["jobs"]["stop"]
    cancel = _step(workflow, "stop", "Cancel every live watcher run for this PR")
    assert job["if"].startswith("inputs.stop")
    assert job["permissions"] == {"actions": "write"}
    assert "concurrency" not in job  # a stop must not queue behind the watcher it stops
    assert cancel["env"]["TITLE"] == "Watch ${{ inputs.repo }}#${{ inputs.pr }} reviews"
    assert "gh run cancel" in cancel["run"]


def test_cut_starts_the_watcher_for_the_pr_it_opens() -> None:
    workflow = _workflow(_CUT)
    outputs = workflow["on"]["workflow_call"]["outputs"]
    job = workflow["jobs"]["cut"]
    start = _step(workflow, "cut", "Start the review watcher for the PR")
    assert list(outputs) == ["pr_number", "pr_url"]
    assert job["outputs"]["pr_number"] == "${{ steps.cut.outputs.pr_number }}"
    assert job["permissions"]["actions"] == "write"
    assert _step(workflow, "cut", "Cut release notes")["id"] == "cut"
    assert start["if"] == "${{ !inputs.dry_run && steps.cut.outputs.pr_number != '' }}"
    assert start["env"]["PR"] == "${{ steps.cut.outputs.pr_number }}"
    assert "gh workflow run release-notes-review-watch.yml" in start["run"]


def test_handler_has_only_pr_identity_inputs_and_scoped_write_token() -> None:
    workflow = _workflow(_HANDLER)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    job = workflow["jobs"]["address"]
    token = _step(workflow, "address", "Generate target token")
    run = _step(workflow, "address", "Address review comments")

    assert list(inputs) == ["repo", "pr", "head_sha", "status_comment_id"]
    assert "github.event.sender.id == 284117741" in job["if"]
    assert job["concurrency"]["group"] == (
        "release-notes-review-${{ inputs.repo }}-${{ inputs.pr }}"
    )
    assert job["permissions"] == {
        "contents": "read",
        "id-token": "write",
    }
    assert token["with"]["repositories"] == "${{ inputs.repo }}"
    assert {
        key: value
        for key, value in token["with"].items()
        if key.startswith("permission-")
    } == {
        "permission-members": "read",
        "permission-contents": "write",
        "permission-pull-requests": "write",
        "permission-metadata": "read",
    }
    assert run["run"] == "python3 -m scripts.release_notes.review_handler"


def test_review_editor_uses_one_edit_only_ai_profile() -> None:
    profile = AGENT_PROFILES["release_notes_review_edit_only"]

    assert profile.writes_allowed is True
    assert profile.allowed_tools == "Read,Edit,MultiEdit,Grep,Glob"
    assert profile.disallowed_tools == "Bash,Write"
    assert "release_notes_review_readonly" not in AGENT_PROFILES
