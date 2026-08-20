from pathlib import Path

import yaml

E2E_AUTOMATION_WORKFLOW = (
    "sarthakaggarwal97/valkey-release-automation/"
    ".github/workflows/qualify-release.yml@e2e/latest-9.1.4-release-automation"
)


def _load(name: str) -> dict:
    path = Path(".github/workflows") / name
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_prepare_accepts_fork_relay_and_opens_dashboard_and_notes_pr() -> None:
    workflow = _load("release-prepare.yml")
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert inputs["dry_run"]["default"] == "false"
    assert inputs["initiator"]["required"] == "true"
    assert list(workflow["jobs"]) == ["authorize-start", "derive", "cut-notes", "tracker"]
    assert workflow["jobs"]["derive"]["needs"] == "authorize-start"
    assert "sarthakaggarwal97" in str(workflow["jobs"]["authorize-start"])
    assert "release_policy.e2e.yml" in str(workflow["jobs"]["derive"])
    assert "secrets.VALKEY_GITHUB_TOKEN" in str(workflow["jobs"]["derive"])
    assert workflow["jobs"]["cut-notes"]["uses"] == "./.github/workflows/release-notes-cut.yml"
    assert workflow["jobs"]["cut-notes"]["secrets"] == "inherit"
    assert workflow["jobs"]["tracker"]["needs"] == "derive"
    assert "sarthakaggarwal97/valkey" in str(workflow["jobs"]["tracker"])
    assert "--trusted-owner sarthakaggarwal97" in str(workflow["jobs"]["tracker"])


def test_publish_waits_for_qualification_before_protected_write() -> None:
    workflow = _load("release-publish.yml")
    jobs = workflow["jobs"]
    assert list(jobs) == ["validate", "qualify", "approval-plan", "publish", "onboard-backports"]
    assert jobs["qualify"]["needs"] == "validate"
    assert jobs["publish"]["needs"] == ["validate", "qualify", "approval-plan"]
    assert "automation_sha" in str(jobs["approval-plan"])
    assert jobs["publish"]["environment"] == "release"
    assert "VALKEY_GITHUB_TOKEN" in str(jobs["publish"])
    assert "VALKEY_GITHUB_TOKEN" in str(jobs["validate"])
    assert "VALKEY_RELEASE_PUBLISH_APP_PRIVATE_KEY" not in str(jobs["publish"])
    assert '"$APPROVER" != "$TRIGGERING_ACTOR"' not in str(jobs["publish"])
    assert "release_policy.e2e.yml" in str(jobs["publish"])
    assert jobs["onboard-backports"]["continue-on-error"] == "true"


def test_publish_qualification_is_exact_and_synchronous() -> None:
    job = _load("release-publish.yml")["jobs"]["qualify"]
    assert job["uses"] == E2E_AUTOMATION_WORKFLOW
    assert job["with"]["version"] == "${{ needs.validate.outputs.version }}"
    assert job["with"]["source_sha"] == "${{ needs.validate.outputs.sha }}"
    assert "automation_repo" not in job["with"]
    assert "automation_ref" not in job["with"]
    assert "github.run_id" in job["with"]["request_id"]
    assert "steps" not in job
    assert "VALKEY_GITHUB_TOKEN" not in str(job)


def test_no_controller_loop_workflows_remain() -> None:
    names = {path.name for path in Path(".github/workflows").glob("release-*.yml")}
    assert "release-reconcile.yml" not in names
    assert "release-adopt.yml" not in names
    assert "release-start.yml" not in names
    assert {"release-prepare.yml", "release-progress.yml", "release-publish.yml"} <= names


def test_progress_watcher_is_fork_scoped_and_serialized() -> None:
    workflow = _load("release-progress.yml")
    assert workflow["on"]["schedule"][0]["cron"] == "*/5 * * * *"
    assert workflow["permissions"] == {"actions": "write", "contents": "read"}
    assert workflow["concurrency"]["group"] == "release-progress"
    job = workflow["jobs"]["sync"]
    rendered = str(job)
    assert "scripts.release.tracker sync" in rendered
    assert "sarthakaggarwal97/valkey-release-automation" in rendered
    assert "--trusted-owner sarthakaggarwal97" in rendered
    assert "secrets.VALKEY_GITHUB_TOKEN" in rendered
    assert "VALKEY_RELEASE_PUBLISH_APP_PRIVATE_KEY" not in rendered
