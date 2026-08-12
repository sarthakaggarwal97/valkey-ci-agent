"""Tests for the release CLI entrypoint and the workflow contracts."""

from __future__ import annotations

import itertools
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from scripts.common.polling import run_poll_loop
from scripts.release.authorize import NotAuthorizedError
from scripts.release.main import main
from scripts.release.publish import PublishPlan, plan_digest
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

    def test_multiline_value_never_reaches_github_output(
            self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # A newline smuggled into an emitted value (here via the issue URL)
        # would let it forge extra key=value output lines for downstream
        # workflow steps; the run must fail and the forged line must never
        # land in the output file.
        output_file = tmp_path / "out"
        output_file.touch()
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
        result = _start_result()
        forged = StartResult(
            created=result.created, cut_needed=result.cut_needed,
            issue_number=result.issue_number,
            issue_url="https://x/issues/11\nrelease_url=https://evil.example",
            version=result.version, stage=result.stage, tag=result.tag,
        )
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.start_release", return_value=forged):
            code = main([*_POLICY_ARGS, "start", "--branch", "9.1",
                         "--intent", "patch", "--actor", "madolson"])
        assert code == 1
        assert "evil.example" not in output_file.read_text()


class TestReconcilePoll:
    """The env-gated poll loop delegates to the shared run_poll_loop."""

    _BRANCHES = ["7.2", "8.0", "8.1", "9.0", "9.1"]

    @staticmethod
    def _set_poll_env(monkeypatch: pytest.MonkeyPatch,
                      interval: str, duration: str) -> None:
        monkeypatch.setenv("RECONCILE_POLL_INTERVAL_SECONDS", interval)
        monkeypatch.setenv("RECONCILE_POLL_DURATION_SECONDS", duration)

    @staticmethod
    def _controlled_loop(clock_values: list[float]):
        """The real run_poll_loop with an injected clock and no sleeping.

        Cadence behavior itself is covered by tests/test_polling.py; these
        tests verify the reconcile command wires into it correctly. The
        clock repeats its last tick forever, so a loop that legitimately
        reads the clock one extra time never exhausts the iterator.
        """
        ticks = itertools.chain(iter(clock_values),
                                itertools.repeat(clock_values[-1]))

        def fake_loop(poll_once, *, interval_seconds, duration_seconds, logger):
            return run_poll_loop(
                poll_once,
                interval_seconds=interval_seconds,
                duration_seconds=duration_seconds,
                clock=lambda: next(ticks),
                sleep=lambda _s: None,
                logger=logger,
            )

        return fake_loop

    def test_env_knobs_reach_the_shared_loop(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._set_poll_env(monkeypatch, "600", "2400")
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch"), \
             patch("scripts.release.main.run_poll_loop",
                   return_value=[0]) as loop:
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 0
        assert loop.call_args.kwargs["interval_seconds"] == 600
        assert loop.call_args.kwargs["duration_seconds"] == 2400

    def test_failing_pass_continues_but_the_run_exits_nonzero(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Pass 1 fails on every branch, pass 2 is clean: the loop must
        # survive the bad pass (both passes run) yet the run exits 1 via
        # PollLoopError, matching the other pollers, so scheduled-run
        # health stays visible.
        self._set_poll_env(monkeypatch, "10", "15")
        calls = {"n": 0}

        def _fail_first_pass(gh, policy, branch, **kwargs):
            calls["n"] += 1
            if calls["n"] <= len(self._BRANCHES):
                raise RuntimeError("transient API error")

        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch",
                   side_effect=_fail_first_pass) as reconcile, \
             patch("scripts.release.main.run_poll_loop",
                   side_effect=self._controlled_loop([0, 0, 5, 10, 20])):
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 1
        assert reconcile.call_count == 2 * len(self._BRANCHES)

    def test_clean_passes_exit_zero(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._set_poll_env(monkeypatch, "10", "15")
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch") as reconcile, \
             patch("scripts.release.main.run_poll_loop",
                   side_effect=self._controlled_loop([0, 0, 5, 10, 20])):
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 0
        assert reconcile.call_count == 2 * len(self._BRANCHES)

    def test_defaults_are_a_single_pass_without_the_loop(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No env set: exactly today's behavior. The shared loop is not
        # even entered, so manual dispatch and every existing caller are
        # unchanged and a pass failure reports through the plain handler.
        monkeypatch.delenv("RECONCILE_POLL_INTERVAL_SECONDS", raising=False)
        monkeypatch.delenv("RECONCILE_POLL_DURATION_SECONDS", raising=False)
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.reconcile_branch") as reconcile, \
             patch("scripts.release.main.run_poll_loop") as loop:
            code = main([*_POLICY_ARGS, "reconcile"])
        assert code == 0
        assert reconcile.call_count == len(self._BRANCHES)
        loop.assert_not_called()


def _publish_plan(**overrides: object) -> PublishPlan:
    values: "dict[str, object]" = dict(
        tag="9.1.1", sha="a" * 40, prerelease=False, make_latest="true",
        body="notes body", issue_number=11)
    values.update(overrides)
    return PublishPlan(**values)  # type: ignore[arg-type]


class TestPublishCLI:
    _PLAN_ONLY = [*_POLICY_ARGS, "publish", "--branch", "9.1",
                  "--actor", "madolson", "--plan-only"]

    @staticmethod
    def _clear_actions_env(monkeypatch: pytest.MonkeyPatch) -> None:
        for var in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID"):
            monkeypatch.delenv(var, raising=False)

    def test_gate_is_verified_before_planning(self,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
        # plan-only must fail early on an unprotected environment, before
        # any evidence is produced for an approver to act on.
        self._clear_actions_env(monkeypatch)
        order = MagicMock()
        order.plan_publication.return_value = _publish_plan()
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected",
                   order.ensure_environment_protected), \
             patch("scripts.release.main.plan_publication", order.plan_publication), \
             patch("scripts.release.main.post_approval_evidence",
                   order.post_approval_evidence), \
             patch("scripts.release.main.emit_job_summary"):
            code = main(self._PLAN_ONLY)
        assert code == 0
        names = [name for name, _, _ in order.mock_calls]
        assert names.index("ensure_environment_protected") < names.index("plan_publication")
        # GITHUB_REPOSITORY unset: the gate is checked against the upstream
        # repo (fail closed), and no run URL means no evidence comment.
        assert order.ensure_environment_protected.call_args.args[2] == \
            "valkey-io/valkey-ci-agent"
        order.post_approval_evidence.assert_not_called()

    def test_plan_only_emits_plan_and_posts_evidence(
            self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        output_file = tmp_path / "out"
        output_file.touch()
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "o/agent")
        monkeypatch.setenv("GITHUB_RUN_ID", "123")
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.plan_publication",
                   return_value=_publish_plan()), \
             patch("scripts.release.main.post_approval_evidence") as evidence, \
             patch("scripts.release.main.emit_job_summary"):
            code = main(self._PLAN_ONLY)
        assert code == 0
        outputs = dict(
            line.split("=", 1) for line in output_file.read_text().splitlines()
        )
        assert outputs == {"tag": "9.1.1", "sha": "a" * 40, "make_latest": "true",
                           "plan_digest": plan_digest(_publish_plan())}
        assert evidence.call_args.args[3] == \
            "https://github.com/o/agent/actions/runs/123"

    def test_plan_only_threads_the_controller_sha_from_github_sha(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "o/agent")
        monkeypatch.setenv("GITHUB_RUN_ID", "123")
        monkeypatch.setenv("GITHUB_SHA", "f" * 40)
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.plan_publication",
                   return_value=_publish_plan()) as plan, \
             patch("scripts.release.main.post_approval_evidence") as evidence, \
             patch("scripts.release.main.render_plan_summary") as summary, \
             patch("scripts.release.main.emit_job_summary"):
            code = main(self._PLAN_ONLY)
        assert code == 0
        assert plan.call_args.kwargs["controller_sha"] == "f" * 40
        assert summary.call_args.kwargs["controller_sha"] == "f" * 40
        assert evidence.call_args.kwargs["controller_sha"] == "f" * 40

    def test_unattended_skips_the_actor_check_in_planning(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_actions_env(monkeypatch)
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.plan_publication",
                   return_value=_publish_plan()) as plan, \
             patch("scripts.release.main.emit_job_summary"):
            code = main([*self._PLAN_ONLY, "--unattended"])
        assert code == 0
        assert plan.call_args.kwargs["skip_authorization"] is True

    def test_execute_without_bindings_is_a_usage_error(self) -> None:
        # An empty binding means the approver's evidence never reached this
        # job; proceeding unbound would defeat the gate.
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             pytest.raises(SystemExit) as excinfo:
            main([*_POLICY_ARGS, "publish", "--branch", "9.1",
                  "--actor", "madolson"])
        assert excinfo.value.code == 2

    @pytest.mark.parametrize("partial_binding", [
        ["--expected-tag", "9.1.1"],
        ["--expected-sha", "a" * 40],
    ])
    def test_execute_with_only_one_binding_is_also_a_usage_error(
            self, partial_binding: "list[str]") -> None:
        # Half the evidence is no evidence: a tag without its SHA (or the
        # reverse) must not reach publish_release, where an empty binding
        # silently disables that half of the compare.
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.publish_release") as publish, \
             pytest.raises(SystemExit) as excinfo:
            main([*_POLICY_ARGS, "publish", "--branch", "9.1",
                  "--actor", "madolson", *partial_binding])
        assert excinfo.value.code == 2
        publish.assert_not_called()

    def test_execute_binds_tag_and_sha_and_emits_release_url(
            self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        output_file = tmp_path / "out"
        output_file.touch()
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
        monkeypatch.setenv("GITHUB_SHA", "f" * 40)
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.publish_release",
                   return_value="https://x/releases/9.1.1") as publish:
            code = main([*_POLICY_ARGS, "publish", "--branch", "9.1",
                         "--actor", "madolson",
                         "--expected-tag", "9.1.1", "--expected-sha", "a" * 40,
                         "--expected-digest", "d" * 64])
        assert code == 0
        kwargs = publish.call_args.kwargs
        assert kwargs["expected_tag"] == "9.1.1"
        assert kwargs["expected_sha"] == "a" * 40
        assert kwargs["expected_digest"] == "d" * 64
        assert kwargs["controller_sha"] == "f" * 40
        assert kwargs["branch"] == "9.1"
        assert kwargs["actor"] == "madolson"
        outputs = dict(
            line.split("=", 1) for line in output_file.read_text().splitlines()
        )
        assert outputs == {"release_url": "https://x/releases/9.1.1"}

    def test_execute_without_a_digest_is_legacy_and_still_publishes(
            self) -> None:
        # The digest binding is optional at the CLI: an approval produced
        # before the digest existed still executes on the tag/SHA binding.
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.publish_release",
                   return_value="https://x/releases/9.1.1") as publish:
            code = main([*_POLICY_ARGS, "publish", "--branch", "9.1",
                         "--actor", "madolson",
                         "--expected-tag", "9.1.1", "--expected-sha", "a" * 40])
        assert code == 0
        assert publish.call_args.kwargs["expected_digest"] == ""

    def test_unprotected_gate_exits_1(self) -> None:
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected",
                   side_effect=ReleaseControlError("no required reviewers")):
            code = main(self._PLAN_ONLY)
        assert code == 1

    def test_unauthorized_actor_exits_1(self,
                                        monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_actions_env(monkeypatch)
        with patch("scripts.release.main.Github"), \
             patch("scripts.release.main.ensure_environment_protected"), \
             patch("scripts.release.main.plan_publication",
                   side_effect=NotAuthorizedError("not a member")):
            code = main(self._PLAN_ONLY)
        assert code == 1


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
        # The gate reads the start job's NORMALIZED dry_run output, not raw
        # inputs: on the trampoline (repository_dispatch) path inputs.* is
        # empty, so `!inputs.dry_run` would wrongly read as "not dry".
        assert "needs.start.outputs.dry_run != 'true'" in cut["if"]
        assert "!inputs.dry_run" not in cut["if"]
        assert cut["with"]["dry_run"] == "false"

    def test_start_normalizes_dry_run_fail_closed_for_both_paths(self) -> None:
        # The normalized expression must default to dry-run semantics only
        # via explicit event checks: a payload without dry_run==true reads
        # false, and the boolean input path is honored verbatim.
        outputs = _workflow("release-start.yml")["jobs"]["start"]["outputs"]
        expr = outputs["dry_run"]
        assert "github.event_name == 'repository_dispatch'" in expr
        assert "client_payload.dry_run == true" in expr
        assert "github.event_name == 'workflow_dispatch'" in expr

    def test_authorization_uses_the_triggering_actor(self) -> None:
        # github.actor on a re-run is the ORIGINAL dispatcher; authorizing it
        # would let a repo-write user replay an authorized user's dispatch.
        # Adopt is direct-dispatch only. Start also accepts the valkey-repo
        # trampoline's forwarded actor, but ONLY on the repository_dispatch
        # event (sending that requires contents:write on this repo, and the
        # forwarded human is still team-checked live) - the direct path must
        # still resolve to triggering_actor, never github.actor.
        adopt_steps = next(iter(_workflow("release-adopt.yml")["jobs"].values()))["steps"]
        assert adopt_steps[-1]["env"]["ACTOR"] == "${{ github.triggering_actor }}"
        start_steps = next(iter(_workflow("release-start.yml")["jobs"].values()))["steps"]
        actor = start_steps[-1]["env"]["ACTOR"]
        assert "github.event_name == 'repository_dispatch'" in actor
        assert "client_payload.actor" in actor
        assert actor.rstrip("} ").endswith("github.triggering_actor")
        assert "github.actor" not in actor.replace("github.triggering_actor", "")

    def test_start_accepts_the_valkey_trampoline_dispatch(self) -> None:
        # The valkey repository hosts a thin relay workflow (template kept
        # at docs/valkey-repo/release-start.yml) so maintainers start
        # releases from the repo where releases happen. It carries no
        # authority: the controller re-verifies the forwarded actor.
        workflow = _workflow("release-start.yml")
        assert workflow["on"]["repository_dispatch"]["types"] == ["release-start"]
        template = Path("docs/valkey-repo/release-start.yml")
        assert template.exists()
        relay = yaml.safe_load(template.read_text())
        assert relay["permissions"] == {}
        (job,) = relay["jobs"].values()
        assert job["permissions"] == {}
        relay_step = job["steps"][-1]
        assert relay_step["env"]["ACTOR"] == "${{ github.triggering_actor }}"
        assert "release-start" in relay_step["run"]

    def test_reconcile_is_scheduled_and_locked_down(self) -> None:
        workflow = _workflow("release-reconcile.yml")
        # Hourly (off the congested :00 tick), long-polling internally:
        # GitHub does not fire high-frequency crons reliably, so cadence
        # comes from the in-run loop, not from `*/10`.
        assert workflow["on"]["schedule"] == [{"cron": "7 * * * *"}]
        assert workflow["permissions"] == {}
        job = workflow["jobs"]["reconcile"]
        # Cron runs in the canonical repo or in a fork that opted in via the
        # RELEASE_POLL_ENABLED repository variable (exactly 'true'); manual
        # dispatch is allowed everywhere (against the fork policy registry).
        condition = job["if"]
        assert "github.repository == 'valkey-io/valkey-ci-agent'" in condition
        assert "vars.RELEASE_POLL_ENABLED == 'true'" in condition
        assert "workflow_dispatch" in condition
        # Scheduled runs poll every 10 minutes; the 2400s cap bounds when
        # the last pass STARTS (passes at t=0,10,20,30,40), leaving the
        # final pass roughly 19 minutes of App-token validity. Dispatch is
        # one immediate pass.
        env = job["steps"][-1]["env"]
        assert env["RECONCILE_POLL_INTERVAL_SECONDS"] == \
            "${{ github.event_name == 'schedule' && '600' || '0' }}"
        assert env["RECONCILE_POLL_DURATION_SECONDS"] == \
            "${{ github.event_name == 'schedule' && '2400' || '0' }}"
        assert job["timeout-minutes"] == "58"
        # The read-only downstream mint had no consumer; it was removed
        # rather than shipped as an unused credential. The write-token
        # scoping stays.
        text = (_ROOT / ".github" / "workflows" /
                "release-reconcile.yml").read_text(encoding="utf-8")
        assert "RELEASE_DOWNSTREAM_READ_TOKEN" not in text
        assert "generate-downstream-read-token" not in text
        assert "RELEASE_DOWNSTREAM_TOKEN" in text

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
        # Token minting is attempted everywhere with an AUTOMATION_PAT last
        # resort, so the whole flow is testable on a fork before touching
        # upstream. A missing App secret is tolerated only on forks:
        # upstream keeps fail-fast semantics at the token step.
        for name in ("release-start.yml", "release-adopt.yml",
                     "release-reconcile.yml", "release-publish.yml"):
            text = (_ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
            assert ("continue-on-error: "
                    "${{ github.repository_owner != 'valkey-io' }}") in text, name
            assert "continue-on-error: true" not in text, name
            assert ("github.repository_owner != 'valkey-io' && "
                    "(secrets.AUTOMATION_PAT || secrets.VALKEY_GITHUB_TOKEN)") in text, name
            # PAT fallback must be structurally impossible upstream.
            assert "|| secrets.AUTOMATION_PAT }}" not in text, name
            assert "release_policy.fork.yml" in text, name
