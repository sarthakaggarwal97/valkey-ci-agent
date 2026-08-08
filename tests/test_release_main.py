"""Tests for the release CLI entrypoint and the workflow contracts."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from scripts.release.main import main
from scripts.release.reconcile import ReleaseControlError, StartResult

_ROOT = Path(__file__).resolve().parents[1]
_POLICY_ARGS = ["--token", "t", "--policy", str(_ROOT / "release_policy.yml"),
                "--repo", "valkey-io/valkey"]


def _start_result(created: bool = True, cut_needed: bool = True) -> StartResult:
    return StartResult(created=created, cut_needed=cut_needed, issue_number=11,
                       issue_url="https://x/issues/11",
                       version="9.1.1", stage="ga", tag="9.1.1")


class TestCLI:
    def test_start_emits_machine_readable_outputs(self, tmp_path: Path,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
        output_file = tmp_path / "out"
        output_file.touch()
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.start_release", return_value=_start_result()):
            code = main([*_POLICY_ARGS, "start", "--branch", "9.1",
                         "--intent", "patch", "--actor", "madolson"])
        assert code == 0
        outputs = dict(
            line.split("=", 1) for line in output_file.read_text().splitlines()
        )
        assert outputs == {
            "version": "9.1.1", "stage": "ga", "tag": "9.1.1",
            "issue_number": "11", "issue_url": "https://x/issues/11",
            "created": "true", "cut_needed": "true",
        }

    def test_refusal_exits_1(self) -> None:
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.start_release",
                   side_effect=ReleaseControlError("refused")):
            code = main([*_POLICY_ARGS, "start", "--branch", "9.1",
                         "--intent", "security", "--actor", "madolson"])
        assert code == 1

    def test_unknown_repo_is_a_usage_error(self) -> None:
        with patch("scripts.release.main.Github"), pytest.raises(SystemExit) as excinfo:
            main(["--token", "t", "--policy", str(_ROOT / "release_policy.yml"),
                  "--repo", "valkey-io/unknown", "reconcile"])
        assert excinfo.value.code == 2

    def test_missing_token_is_a_usage_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("RELEASE_GITHUB_TOKEN", "TARGET_TOKEN", "GITHUB_TOKEN"):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(SystemExit) as excinfo:
            main(["--token", "", "reconcile"])
        assert excinfo.value.code == 2

    def test_reconcile_covers_every_policy_branch_by_default(self) -> None:
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch") as reconcile:
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 0
        branches = [call.args[2] for call in reconcile.call_args_list]
        assert branches == ["7.2", "8.0", "8.1", "9.0", "9.1"]

    def test_one_failing_branch_does_not_skip_the_rest(self) -> None:
        # A deleted branch or transient API failure on one line must not
        # abort reconciliation of the remaining lines until the next cron.
        def _boom_on_first(gh, policy, branch):
            if branch == "7.2":
                raise RuntimeError("branch gone")

        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch",
                   side_effect=_boom_on_first) as reconcile:
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 1  # the failure is reported...
        branches = [call.args[2] for call in reconcile.call_args_list]
        assert branches == ["7.2", "8.0", "8.1", "9.0", "9.1"]  # ...but nothing is skipped

    def test_reconcile_rejects_unconfigured_branch(self) -> None:
        with patch("scripts.release.main.Github"), pytest.raises(SystemExit) as excinfo:
            main([*_POLICY_ARGS, "reconcile", "--branch", "unstable"])
        assert excinfo.value.code == 2

    def test_adopt_passes_through(self) -> None:
        status = MagicMock()
        status.candidate.sha = "a" * 40
        status.ready = False
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.adopt_candidate", return_value=status) as adopt:
            code = main([*_POLICY_ARGS, "adopt", "--branch", "9.1",
                         "--sha", "a" * 40, "--actor", "madolson"])
        assert code == 0
        assert adopt.call_args.kwargs["sha"] == "a" * 40


def _workflow(name: str) -> dict:
    path = _ROOT / ".github" / "workflows" / name
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


class TestWorkflowContracts:
    def test_start_dispatch_exposes_the_release_decision_only(self) -> None:
        workflow = _workflow("release-start.yml")
        inputs = workflow["on"]["workflow_dispatch"]["inputs"]
        assert list(inputs) == ["branch", "intent", "urgency", "dry_run"]
        # security IS a choice: it maps to the intent the controller refuses
        # BEFORE creating anything public, making the embargo path explicit.
        assert inputs["intent"]["options"] == ["rc", "ga", "patch", "security"]
        assert inputs["dry_run"]["default"] == "true"
        assert workflow["permissions"] == {}

    def test_notes_cut_only_chains_when_a_cut_is_needed_and_not_dry(self) -> None:
        cut = _workflow("release-start.yml")["jobs"]["cut-notes"]
        assert cut["uses"] == "./.github/workflows/release-notes-cut.yml"
        assert "cut_needed == 'true'" in cut["if"]
        assert "!inputs.dry_run" in cut["if"]
        assert cut["with"]["dry_run"] == "false"

    def test_authorization_uses_the_triggering_actor(self) -> None:
        # github.actor on a re-run is the ORIGINAL dispatcher; authorizing it
        # would let a repo-write user replay an authorized user's dispatch.
        for name in ("release-start.yml", "release-adopt.yml"):
            jobs = _workflow(name)["jobs"]
            steps = next(iter(jobs.values()))["steps"]
            run_step = steps[-1]
            assert run_step["env"]["ACTOR"] == "${{ github.triggering_actor }}"

    def test_reconcile_is_scheduled_and_locked_down(self) -> None:
        workflow = _workflow("release-reconcile.yml")
        assert "schedule" in workflow["on"]
        assert workflow["permissions"] == {}
        # Cron runs only in the canonical repo; manual dispatch is allowed in
        # forks (against the fork policy registry).
        condition = workflow["jobs"]["reconcile"]["if"]
        assert "github.repository == 'valkey-io/valkey-ci-agent'" in condition
        assert "workflow_dispatch" in condition

    def test_adopt_requires_branch_and_sha(self) -> None:
        inputs = _workflow("release-adopt.yml")["on"]["workflow_dispatch"]["inputs"]
        assert inputs["branch"]["required"] == "true"
        assert inputs["sha"]["required"] == "true"

    def test_publish_gates_the_write_behind_the_release_environment(self) -> None:
        workflow = _workflow("release-publish.yml")
        assert workflow["permissions"] == {}
        jobs = workflow["jobs"]
        # validate has no environment and read-only App permissions.
        assert "environment" not in jobs["validate"]
        # publish is gated and depends on validate's evidence.
        assert jobs["publish"]["environment"] == "release"
        assert jobs["publish"]["needs"] == "validate"
        publish_step = jobs["publish"]["steps"][-1]
        assert "--expected-tag" in publish_step["run"]
        token_step = next(
            step for step in jobs["publish"]["steps"]
            if step.get("id") == "generate-token"
        )
        assert token_step["with"]["permission-contents"] == "write"

    def test_every_release_workflow_supports_fork_operation(self) -> None:
        # Token minting is attempted everywhere (the App secrets exist on the
        # test fork from earlier live testing; continue-on-error tolerates
        # their absence) with an AUTOMATION_PAT last resort, so the whole
        # flow is testable on a fork before touching upstream.
        for name in ("release-start.yml", "release-adopt.yml",
                     "release-reconcile.yml", "release-publish.yml"):
            text = (_ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
            assert "continue-on-error: true" in text, name
            assert ("github.repository_owner != 'valkey-io' && "
                    "(secrets.AUTOMATION_PAT || secrets.VALKEY_GITHUB_TOKEN)") in text, name
            # PAT fallback must be structurally impossible upstream.
            assert "|| secrets.AUTOMATION_PAT }}" not in text, name
            assert "release_policy.fork.yml" in text, name
