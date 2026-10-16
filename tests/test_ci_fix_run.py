"""One CI fix as three jobs, whichever front door asked for it."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import run
from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FixCandidate,
    FixOutcome,
    FixPlan,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.reproduce import JobCommands
from scripts.ci_fix.state import load, save

_BOT = "valkeyrie-ops[bot]"
_HEAD = "b" * 40
_CANDIDATE = FixCandidate(action=CandidateAction.PATCH, patch="diff", changed_paths=("src/a.c",))


def _pr_plan(job_name="test-ubuntu-latest") -> FixPlan:
    """A PR command's plan: the PR's CI run, on a bot-owned branch into unstable."""
    task = FixTask(
        Failure(repo_full_name="valkey-io/valkey", run_id=100, head_sha=_HEAD, job_id=22, job_name=job_name,
                base_branch="unstable", pr_number=42, workflow="ci"),
        FixTarget(kind=TargetKind.UPDATE_PR, repo_full_name="valkey-io/valkey", branch="agent/ci-fix/x",
                  expected_sha=_HEAD, base_branch="unstable", pr_number=42),
        FixPolicy.GENERAL,
    )
    return FixPlan(front_door=FrontDoor.PR, task=task, comment_id=7)


@pytest.fixture
def front_door(monkeypatch):
    """The PR front door, its planner, publisher and reporter replaced by records."""
    seen = SimpleNamespace(planned=None, reported=[], published=[])

    def planner(_gh, _artifacts, request, *, bot_login):
        return seen.planned

    def publisher(plan, *, bot_login):
        return "checked publication"

    def reporter(_gh, plan, result, *, bot_login):
        seen.reported.append(result)
        return {"action": result.kind.value}

    monkeypatch.setitem(run._FRONT_DOORS, FrontDoor.PR, (planner, publisher, reporter))
    return seen


def _plan(tmp_path, planned):
    artifacts = SimpleNamespace(download_job_log=lambda _repo, _job: "[err]: TTL expiration in tests/integration/rdb.tcl")
    return run.plan(MagicMock(), artifacts, FrontDoor.PR, {}, bot_login=_BOT, out=tmp_path)


def test_a_failure_a_copied_job_can_host_is_fixed_in_that_job(front_door, tmp_path):
    front_door.planned = _pr_plan()

    outputs = _plan(tmp_path, front_door.planned)

    assert outputs == {
        "mode": "copy", "branch": "unstable", "workflow": "ci", "job": "test-ubuntu-latest",
        "use_repo": "valkey-io/valkey", "use_git_ref": _HEAD, "fix_matrix": "", "valgrind_test": "",
        "accurate": "false",
    }
    # Everything the later jobs need survives the handoff, the job log included.
    assert load(tmp_path / run.PLAN_FILE, FixPlan) == replace(
        front_door.planned, job_log="[err]: TTL expiration in tests/integration/rdb.tcl",
    )


def test_a_failure_no_copy_can_host_is_fixed_on_the_bots_runner(front_door, tmp_path):
    front_door.planned = _pr_plan("test-no-copy-has-this-job")

    assert _plan(tmp_path, front_door.planned) == {"mode": "local"}
    assert (tmp_path / run.PLAN_FILE).exists()


@pytest.mark.parametrize(("final", "outputs"), [
    ({"action": "skipped", "reason": "x"}, {"mode": "done", "action": "skipped"}),
    ({"action": "failed"}, {"mode": "done", "action": "failed"}),
])
def test_a_final_answer_ends_the_run_at_the_plan(front_door, tmp_path, final, outputs):
    front_door.planned = final

    assert _plan(tmp_path, final) == outputs
    assert not (tmp_path / run.PLAN_FILE).exists()


def test_the_fix_job_prepares_from_the_plan_alone_with_the_jobs_own_commands(monkeypatch, tmp_path):
    save(tmp_path / run.PLAN_FILE, replace(_pr_plan(), job_log="the log"))
    monkeypatch.setenv("CI_FIX_BUILD", "make")
    monkeypatch.setenv("CI_FIX_RUNTEST", "./runtest --dump-logs")
    seen = {}

    def prepare_fix(task, artifact_client, **kwargs):
        seen.update(task=task, log=artifact_client.download_job_log("valkey-io/valkey", 22), **kwargs)
        return _CANDIDATE

    monkeypatch.setattr(run, "prepare_fix", prepare_fix)

    outcome = run.fix(tmp_path, tmp_path / "out", repo_dir="/job")

    assert seen == {"task": _pr_plan().task, "log": "the log", "repo_dir": "/job",
                    "job": JobCommands(build="make", runtest="./runtest --dump-logs")}
    assert load(tmp_path / "out" / run.OUTCOME_FILE, FixOutcome) == outcome == FixOutcome(candidate=_CANDIDATE)


def _publish(tmp_path, outcome_dir):
    return run.publish_and_report(
        MagicMock(), tmp_path, outcome_dir, bot_login=_BOT, git_env={}, run_url="https://run",
    )


def test_a_fix_that_crashed_or_never_finished_is_reported_as_failed(front_door, monkeypatch, tmp_path):
    save(tmp_path / run.PLAN_FILE, _pr_plan())
    monkeypatch.setattr(run, "prepare_fix", MagicMock(side_effect=RuntimeError("agent died")))
    run.fix(tmp_path, tmp_path / "out")

    _publish(tmp_path, tmp_path / "out")
    _publish(tmp_path, tmp_path / "nothing")

    crashed, missing = front_door.reported
    assert crashed.kind is ResultKind.FAILED and "agent died" in crashed.summary
    assert missing.kind is ResultKind.FAILED and "https://run" in missing.summary


def test_the_candidate_is_published_through_the_front_doors_publication(front_door, monkeypatch, tmp_path):
    save(tmp_path / run.PLAN_FILE, _pr_plan())
    save(tmp_path / run.OUTCOME_FILE, FixOutcome(candidate=_CANDIDATE))
    seen = {}

    def publish(_gh, task, candidate, *, git_env, publish_func):
        seen.update(task=task, candidate=candidate, publish_func=publish_func)
        return FixResult(kind=ResultKind.OPENED, summary="opened", task=task)

    monkeypatch.setattr(run, "publish", publish)

    assert _publish(tmp_path, tmp_path) == {"action": "opened"}
    assert seen == {"task": _pr_plan().task, "candidate": _CANDIDATE, "publish_func": "checked publication"}


def test_every_front_door_plans_publishes_and_reports():
    assert set(run._FRONT_DOORS) == set(FrontDoor)


def test_the_cli_fails_for_a_failed_publication(monkeypatch):
    class AuthContext:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def env(self):
            return {}

    monkeypatch.setenv("TARGET_TOKEN", "token")
    monkeypatch.setattr(run, "Github", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(run, "GitAuth", lambda *_args, **_kwargs: AuthContext())
    monkeypatch.setattr(run, "publish_and_report", lambda *_args, **_kwargs: {"action": "failed"})

    assert run.main(["publish", "--plan", "plan", "--outcome", "outcome"]) == 1
