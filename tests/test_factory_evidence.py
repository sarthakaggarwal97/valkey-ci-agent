"""The evidence: a test that fails before the fix, checked by code."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from scripts.ci_fix import engine as engine_mod
from scripts.ci_fix.agent import AgentTimeout
from scripts.ci_fix.ci_job import JobCommands
from scripts.ci_fix.models import (
    CandidateAction,
    FailedTest,
    Failure,
    FixPolicy,
    FixTarget,
    FixTask,
    IssueBrief,
    IssueKind,
    TargetKind,
)
from scripts.ci_fix.verification import LOOPS, VerifyPlan
from scripts.factory import evidence as evidence_mod
from scripts.factory.evidence import Evidence, TestCandidate, establish_evidence
from tests.git_helpers import git

_TEST = FailedTest("value check", "tests/unit/value.tcl")
# A test command that reports as runtest does: [err] while value.txt is wrong, [ok] once it is right.
_CHECK = (
    'if test "$(head -1 value.txt)" = right; then echo "[ok]: value check (1 ms)"; '
    'else echo "[err]: value check in tests/unit/value.tcl"; exit 1; fi; : loops=' + LOOPS
)


@pytest.fixture(autouse=True)
def _plain_runs(monkeypatch):
    monkeypatch.setattr(engine_mod, "isolated_user", lambda: "")
    monkeypatch.setattr(evidence_mod, "JOB", JobCommands(build="true", runtest="true"))
    monkeypatch.setattr(
        evidence_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(build="true", test=_CHECK, tests=tests),
    )


def _repo(tmp_path: Path) -> tuple[str, str]:
    repo = tmp_path / f"repo{len(list(tmp_path.glob('repo*')))}"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.com")
    (repo / "value.txt").write_text("before\n", encoding="utf-8")
    (repo / "tests" / "unit").mkdir(parents=True)
    (repo / "tests" / "unit" / "value.tcl").write_text("start_server {} {\n}\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "initial")
    return str(repo), git(repo, "rev-parse", "HEAD")


def _task(head: str) -> FixTask:
    return FixTask(
        Failure("o/r", 0, head, 0, "", base_branch="unstable", issue_number=9),
        FixTarget(TargetKind.DRAFT_PR, "o/r", "o/r", "agent/factory/issue-9-1", head), FixPolicy.GENERAL,
    )


def _brief(**kwargs) -> IssueBrief:
    return IssueBrief(number=9, title="GET returns the old value", body="SET then GET; GET is stale.",
                      label_event_id=1, labelled_by="alice", **kwargs)


def _writing(*edits, action="test", test=_TEST, **fields):
    """An agent that makes each of ``edits`` in turn (a callable on the repo dir), recording its feedback."""
    rounds = iter(edits)
    feedback: list[str] = []

    def agent(task, brief, repo_dir, *, feedback="", timeout=None, guide=""):
        agent.feedback.append(feedback)
        edit = next(rounds)
        if edit is not None:
            edit(Path(repo_dir))
        return TestCandidate(action=action, test=test, summary="shows the stale GET", expects="GET returns the new value", **fields)

    agent.feedback = feedback
    return agent


def _adds_test(repo: Path, body='test {value check} {\n    assert_equal right [r get k]\n}\n') -> None:
    path = repo / "tests" / "unit" / "value.tcl"
    path.write_text(path.read_text() + body, encoding="utf-8")


def _establish(tmp_path, agent, brief=None, **kwargs):
    repo, head = _repo(tmp_path)
    return establish_evidence(
        _task(head), brief or _brief(), repo, head, deadline=time.monotonic() + 3600, agent_func=agent,
        run_test=engine_mod.run_test_command, **kwargs,
    ), repo


def test_a_test_that_fails_every_run_on_its_own_assertion_is_the_evidence(tmp_path):
    agent = _writing(_adds_test)

    evidence, repo = _establish(tmp_path, agent)

    assert isinstance(evidence, Evidence)
    assert evidence.tests == (_TEST,) and evidence.paths == ("tests/unit/value.tcl",)
    assert "+test {value check}" in evidence.patch
    assert evidence.checks[0].startswith("The test `value check` in `tests/unit/value.tcl`, written from the issue, fails on")
    assert "in 3 of 3 runs without the change" in evidence.checks[0]
    assert "# Issue #9: GET returns the old value" in evidence.log
    assert evidence.log.count("# The test's output, run") == 3 and "[err]: value check" in evidence.log
    assert "What the test asserts, and what the server does instead: GET returns the new value" in evidence.log
    assert agent.feedback == [""]


def test_a_test_that_passes_goes_back_with_the_output(tmp_path):
    def fixes_it_too(repo: Path) -> None:
        _adds_test(repo)
        (repo / "value.txt").write_text("right\n", encoding="utf-8")

    def undoes_the_fix(repo: Path) -> None:
        (repo / "value.txt").write_text("before\n", encoding="utf-8")

    agent = _writing(fixes_it_too, undoes_the_fix)

    evidence, _repo_dir = _establish(tmp_path, agent)

    assert isinstance(evidence, Evidence)
    # The first round changed product source: that came back before anything ran.
    assert "you changed `value.txt`" in agent.feedback[1] and "do not fix the bug" in agent.feedback[1]
    assert "accepted on round 2 of 5" in evidence.checks[-1]


def test_a_test_that_passes_on_the_working_commit_demonstrates_nothing(tmp_path):
    repo, _head = _repo(tmp_path)
    (Path(repo) / "value.txt").write_text("right\n", encoding="utf-8")
    git(Path(repo), "commit", "-q", "-am", "right already")
    head = git(Path(repo), "rev-parse", "HEAD")
    agent = _writing(*([_adds_test] + [None] * (evidence_mod.EVIDENCE_ROUNDS - 1)))

    refusal = establish_evidence(_task(head), _brief(), repo, head, deadline=time.monotonic() + 3600,
                                 agent_func=agent, run_test=engine_mod.run_test_command)

    assert refusal.action is CandidateAction.REFUSE and "No test that demonstrates the issue" in refusal.summary
    assert agent.feedback[1].startswith("The test passed on the working commit (run 1 of 3)")
    assert len(agent.feedback) == evidence_mod.EVIDENCE_ROUNDS


def test_a_failure_outside_the_tests_own_assertion_is_not_evidence_for_a_bug(tmp_path, monkeypatch):
    exception = 'echo "[exception]: Executing test client: ERR unknown command"; echo "    test {value check}"; exit 1'
    monkeypatch.setattr(
        evidence_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(build="true", test=exception, tests=tests),
    )
    agent = _writing(_adds_test, _adds_test, _adds_test, _adds_test, _adds_test)

    refusal, _repo_dir = _establish(tmp_path, agent)

    assert refusal.action is CandidateAction.REFUSE
    assert "not on the test's own assertion" in agent.feedback[1]


def test_a_features_test_may_stop_on_the_command_the_feature_adds(tmp_path, monkeypatch):
    exception = 'echo "[exception]: Executing test client: ERR unknown command"; echo "    test {value check}"; exit 1'
    monkeypatch.setattr(
        evidence_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(build="true", test=exception, tests=tests),
    )
    agent = _writing(_adds_test, criteria=("HGETEX returns the value", "HGETEX sets the TTL"))

    evidence, _repo_dir = _establish(tmp_path, agent, brief=_brief(kind=IssueKind.FEATURE))

    assert isinstance(evidence, Evidence)
    assert evidence.checks[1].startswith("Acceptance criteria the tests encode")
    assert "(1) HGETEX returns the value; (2) HGETEX sets the TTL." in evidence.checks[1]
    assert "1. HGETEX returns the value" in evidence.log


def test_a_crash_nobody_expected_stops_the_run_and_a_crash_report_expects_one(tmp_path, monkeypatch):
    crash = (
        'echo "=== VALKEY BUG REPORT START: Cut & paste starting from here ==="; '
        'echo "[exception]: I/O error reading reply"; echo "    test {value check}"; exit 1'
    )
    monkeypatch.setattr(
        evidence_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(build="true", test=crash, tests=tests),
    )
    with pytest.raises(evidence_mod.Stopped, match="crashed the server, which the issue does not report"):
        _establish(tmp_path, _writing(_adds_test))

    evidence, _repo_dir = _establish(tmp_path, _writing(_adds_test), brief=_brief(crash_report=True))
    assert isinstance(evidence, Evidence)


def test_a_broken_build_and_a_missing_or_misnamed_test_go_back_to_the_agent(tmp_path, monkeypatch):
    def not_in_the_file(repo: Path) -> None:
        _adds_test(repo, 'test {another name} {\n}\n')

    def in_the_file(repo: Path) -> None:
        _adds_test(repo)

    agent = _writing(not_in_the_file, in_the_file)

    evidence, _repo_dir = _establish(tmp_path, agent)

    assert isinstance(evidence, Evidence)
    assert "holds no test named exactly `value check`" in agent.feedback[1]


def test_questions_and_a_refusal_end_the_attempt_with_what_the_agent_found(tmp_path):
    asking = _writing(None, action="questions", questions=("Which reply for a missing key?", "Does it propagate?"))
    refusal, _repo_dir = _establish(tmp_path, asking)
    assert refusal.action is CandidateAction.REFUSE
    assert refusal.summary.startswith("The behaviour the issue asks for is not agreed")
    assert "(1) Which reply for a missing key? (2) Does it propagate?" in refusal.summary

    refusing = _writing(None, action="refuse")
    refusal, _repo_dir = _establish(tmp_path, refusing)
    assert refusal.action is CandidateAction.REFUSE and refusal.summary == "shows the stale GET"


def test_a_session_that_runs_out_refuses_and_a_crashed_first_session_fails(tmp_path):
    def timing_out(*_args, **_kwargs):
        raise AgentTimeout("timeout")

    refusal, _repo_dir = _establish(tmp_path, timing_out)
    assert refusal.action is CandidateAction.REFUSE and "whole session" in refusal.summary

    def crashing(*_args, **_kwargs):
        raise RuntimeError("the CLI died")

    with pytest.raises(RuntimeError, match="the CLI died"):
        _establish(tmp_path, crashing)


def test_the_agents_result_is_read_strictly():
    candidate = evidence_mod.candidate_from_payload({
        "action": "TEST", "test": {"file": "tests/unit/x.tcl", "name": "n"}, "summary": "s", "expects": "e",
        "criteria": ["a", 3, "b"], "questions": "not a list", "validation": "v",
    })
    assert candidate == TestCandidate("test", FailedTest("n", "tests/unit/x.tcl"), "s", "e", ("a", "b"), (), "v")
    assert evidence_mod.candidate_from_payload({"action": "refuse", "test": {"file": "", "name": "n"}}).test is None
    with pytest.raises(ValueError, match="unknown evidence action"):
        evidence_mod.candidate_from_payload({"action": "fix"})


def test_a_c_unit_test_is_found_by_its_suite_and_name():
    source = "TEST(SlotStats, OrderBySortsLargeCounters) {\n}\n"
    assert evidence_mod._gtest_named(FailedTest("SlotStats.OrderBySortsLargeCounters", "src/unit/test_x.cpp"), source)
    assert not evidence_mod._gtest_named(FailedTest("SlotStats.Other", "src/unit/test_x.cpp"), source)
    assert not evidence_mod._gtest_named(FailedTest("no dot", "src/unit/test_x.cpp"), source)
