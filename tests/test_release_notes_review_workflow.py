from pathlib import Path

import yaml

from scripts.ai.runtime import AGENT_PROFILES
from scripts.release_notes.review_poll import watch_run_title

_WATCH = Path(".github/workflows/release-notes-review-watch.yml")
_RESTART = Path(".github/workflows/release-notes-review-restart.yml")
_CUT = Path(".github/workflows/release-notes-cut.yml")
_CUT_CALLERS = (
    Path(".github/workflows/release-prepare.yml"),
    Path(".github/workflows/release-notes-cut-advanced.yml"),
)
_HANDLER = Path(".github/workflows/release-notes-review.yml")


def _workflow(path: Path) -> dict:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _step(workflow: dict, job: str, name: str) -> dict:
    return next(
        step
        for step in workflow["jobs"][job]["steps"]
        if step.get("name") == name
    )


def test_every_caller_of_the_cut_grants_the_watcher_start() -> None:
    # A reusable workflow cannot request more than its caller grants; the cut
    # asks for actions:write to start the watcher, so both callers must too.
    for path in _CUT_CALLERS:
        workflow = _workflow(path)
        caller = next(job for job in workflow["jobs"].values() if job.get("uses", "").endswith("release-notes-cut.yml"))
        assert caller["permissions"]["actions"] == "write", path


def test_missing_watchers_are_restarted_on_a_schedule() -> None:
    workflow = _workflow(_RESTART)
    job = workflow["jobs"]["restart"]
    run = _step(workflow, "restart", "Start a watcher for every unwatched release PR")

    assert workflow["on"]["schedule"] == [{"cron": "23 */6 * * *"}]
    # Reads public PRs and dispatches at home: the repository's own token,
    # no App token at all.
    assert job["permissions"] == {"contents": "read", "actions": "write"}
    assert not any("create-github-app-token" in step.get("uses", "") for step in job["steps"])
    assert run["env"]["RELEASE_NOTES_REVIEW_TOKEN"] == "${{ github.token }}"
    assert run["run"].split()[-1] == "--restart-missing"


def test_one_watcher_per_release_pr_replaces_the_hourly_scan() -> None:
    assert not Path(".github/workflows/release-notes-review-poll.yml").exists()
    workflow = _workflow(_WATCH)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert list(workflow["on"]) == ["workflow_dispatch"]
    assert list(inputs) == ["repo", "pr", "stop"]
    assert inputs["stop"]["default"] == "false"

    job = workflow["jobs"]["watch"]
    token = _step(workflow, "watch", "Generate target token")
    agent_token = _step(workflow, "watch", "Generate dispatch token")
    run = _step(workflow, "watch", "Watch and dispatch")

    # review_poll.watch_run_title() finds this PR's runs by this exact title.
    assert workflow["run-name"] == (
        "${{ inputs.stop && 'Stop watching' || 'Watch' }} ${{ inputs.repo }}#${{ inputs.pr }} reviews"
    )
    assert watch_run_title("valkey", 42) == "Watch valkey#42 reviews"
    assert "!inputs.stop" in job["if"]
    assert job["timeout-minutes"] == "65"
    assert job["concurrency"]["group"] == (
        "release-notes-review-watch-${{ inputs.repo }}-${{ inputs.pr }}"
    )
    assert job["concurrency"]["cancel-in-progress"] == "false"
    # The job's own token stays read-only. Two App tokens split the work:
    # the target one can post on the release PR but not touch workflows
    # there; the dispatch one can start workflows here but not touch PRs.
    assert job["permissions"] == {"contents": "read"}
    assert token["with"]["repositories"] == "${{ inputs.repo }}"
    assert {
        key: value
        for key, value in token["with"].items()
        if key.startswith("permission-")
    } == {
        "permission-members": "read",
        "permission-pull-requests": "write",
        "permission-metadata": "read",
    }
    assert agent_token["with"]["repositories"] == "valkey-ci-agent"
    assert {
        key: value
        for key, value in agent_token["with"].items()
        if key.startswith("permission-")
    } == {
        "permission-actions": "write",
        "permission-metadata": "read",
    }
    assert run["env"]["RELEASE_NOTES_REVIEW_TOKEN"] == "${{ steps.token.outputs.token }}"
    assert run["env"]["RELEASE_NOTES_REVIEW_AGENT_TOKEN"] == "${{ steps.agent-token.outputs.token }}"
    # Always a sustained watch, including when started by hand to resume one.
    assert run["env"]["RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS"] == "300"
    assert run["env"]["RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS"] == "3300"
    command = run["run"].replace("\\\n", " ").split()
    assert command[:3] == ["python3", "-m", "scripts.release_notes.review_poll"]
    assert "--repo" in command and "--pr" in command
    assert not {"--start", "--stop"} & set(command)
    assert [step["name"] for step in job["steps"]][-1] == "Watch and dispatch"


def test_stop_cancels_only_that_prs_watcher() -> None:
    workflow = _workflow(_WATCH)
    job = workflow["jobs"]["stop"]
    cancel = _step(workflow, "stop", "Cancel every live watcher run for this PR")
    assert job["if"].startswith("inputs.stop")
    assert job["permissions"] == {"contents": "read", "actions": "write"}
    assert "concurrency" not in job  # a stop must not queue behind the watcher it stops
    assert cancel["env"]["RELEASE_NOTES_REVIEW_TOKEN"] == "${{ github.token }}"
    assert cancel["run"].split()[-1] == "--stop"


def test_cut_starts_the_watcher_for_the_pr_it_opens() -> None:
    workflow = _workflow(_CUT)
    outputs = workflow["on"]["workflow_call"]["outputs"]
    cut = workflow["jobs"]["cut"]
    watch = workflow["jobs"]["watch"]
    start = _step(workflow, "watch", "Start the review watcher for the PR")
    assert list(outputs) == ["pr_number", "pr_url"]
    assert cut["outputs"]["pr_number"] == "${{ steps.cut.outputs.pr_number }}"
    assert _step(workflow, "cut", "Cut release notes")["id"] == "cut"
    # The job that runs Claude keeps no token that could start or cancel
    # workflows; the start is its own job, and a failed start is not a
    # failed cut (the scheduled restart covers it).
    assert cut["permissions"] == {"contents": "read", "id-token": "write"}
    assert "environment" not in watch
    assert watch["needs"] == "cut"
    assert watch["continue-on-error"] == "true"
    assert watch["permissions"] == {"contents": "read", "actions": "write"}
    assert watch["if"] == "${{ !inputs.dry_run && needs.cut.outputs.pr_number != '' }}"
    assert start["env"]["RELEASE_NOTES_REVIEW_PR"] == "${{ needs.cut.outputs.pr_number }}"
    assert start["env"]["RELEASE_NOTES_REVIEW_TOKEN"] == "${{ github.token }}"
    assert start["run"].split()[-1] == "--start"


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
