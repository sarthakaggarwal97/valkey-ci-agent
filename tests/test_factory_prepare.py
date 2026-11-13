"""Both phases over one checkout: the test, committed and frozen, then the fix through the engine."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from scripts.ci_fix import engine as engine_mod
from scripts.ci_fix.agent import Review
from scripts.ci_fix.ci_job import JobCommands
from scripts.ci_fix.models import (
    CandidateAction,
    FailedTest,
    Failure,
    FixCandidate,
    FixPlan,
    FixPolicy,
    FixTarget,
    FixTask,
    FrontDoor,
    IssueBrief,
    TargetKind,
)
from scripts.ci_fix.verification import LOOPS, VerifyPlan
from scripts.factory import checks as checks_mod
from scripts.factory import evidence as evidence_mod
from scripts.factory import prepare as prepare_mod
from scripts.factory import review as review_mod
from scripts.factory.evidence import TestCandidate
from tests.git_helpers import git

_TEST = FailedTest("value check", "tests/unit/value.tcl")
_CHECK = (
    'if test "$(head -1 value.txt)" = right; then echo "[ok]: value check (1 ms)"; '
    'else echo "[err]: value check in tests/unit/value.tcl"; exit 1; fi; : loops=' + LOOPS
)


def _source(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "source"
    repo.mkdir()
    git(repo, "init", "-b", "unstable")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.com")
    (repo / "value.txt").write_text("before\n", encoding="utf-8")
    (repo / "tests" / "unit").mkdir(parents=True)
    (repo / "tests" / "unit" / "value.tcl").write_text("start_server {} {\n}\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "initial")
    return repo, git(repo, "rev-parse", "HEAD")


@pytest.fixture
def plain(monkeypatch):
    """Plain subprocesses, a trivial build, and a plan that reads value.txt, in every module."""
    monkeypatch.setattr(engine_mod, "isolated_user", lambda: "")
    job = JobCommands(build="true", runtest="true")
    for module in (evidence_mod, prepare_mod, checks_mod, review_mod):
        monkeypatch.setattr(module, "JOB", job)

    def plan_for(repo_dir, tests, *, scheduled, job=None):
        return VerifyPlan(build="true", test=_CHECK, tests=tests)

    for module in (engine_mod, evidence_mod, checks_mod, review_mod):
        monkeypatch.setattr(module, "plan_for", plan_for)
    monkeypatch.setattr(engine_mod, "guide_for", lambda repo: "")
    monkeypatch.setattr(review_mod, "adversarial_review", lambda *a, **k: Review(True, "covers the issue"))


def _install_clone(monkeypatch, source: Path):
    def fake_clone(repo_full_name, destination, sha, *, pull_number=0):
        shutil.copytree(source, destination)
        git(destination, "checkout", "--detach", sha)
        git(destination, "update-ref", "refs/remotes/origin/unstable", sha)
        git(destination, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/unstable")
        return True

    monkeypatch.setattr(engine_mod, "shallow_clone_at_sha", fake_clone)


def _plan(head: str) -> FixPlan:
    task = FixTask(
        Failure("o/r", 0, head, 0, "", base_branch="unstable", description="Issue #9: stale GET", issue_number=9),
        FixTarget(TargetKind.DRAFT_PR, "o/r", "o/r", "agent/factory/issue-9-1", head), FixPolicy.GENERAL,
    )
    brief = IssueBrief(number=9, title="stale GET", body="GET returns the old value", label_event_id=1, labelled_by="a")
    return FixPlan(front_door=FrontDoor.FACTORY, task=task, brief=brief)


def _test_writer(task, brief, repo_dir, *, feedback="", timeout=None, guide=""):
    path = Path(repo_dir, "tests/unit/value.tcl")
    path.write_text(path.read_text() + "test {value check} {\n    assert_equal right [r get k]\n}\n", encoding="utf-8")
    return TestCandidate("test", _TEST, summary="shows the stale GET", expects="GET returns the new value")


def _fixer(*values):
    rounds = iter(values)
    seen = {"feedback": [], "tasks": []}

    def agent(task, repo_dir, job_log, *, feedback="", **_kwargs):
        seen["feedback"].append(feedback)
        seen["tasks"].append(task)
        seen["log"] = job_log
        value = next(rounds)
        if value == "touch the test":
            path = Path(repo_dir, "tests/unit/value.tcl")
            path.write_text(path.read_text().replace("right", "before"), encoding="utf-8")
        else:
            Path(repo_dir, "value.txt").write_text(f"{value}\n", encoding="utf-8")
        return FixCandidate(
            action=CandidateAction.PATCH, title="Return the new value from GET", root_cause="stale value",
            reasoning="set it", changed_paths=("value.txt",) if value != "touch the test" else ("tests/unit/value.tcl",),
        )

    agent.seen = seen
    return agent


def _prepare(tmp_path, monkeypatch, fixer, test_writer=_test_writer):
    source, head = _source(tmp_path)
    _install_clone(monkeypatch, source)
    plan = _plan(head)
    candidate = prepare_mod.prepare_in(
        plan, run_test=engine_mod.run_test_command, fix_agent=fixer,
        evidence_func=lambda *args, **kwargs: evidence_mod.establish_evidence(*args, agent_func=test_writer, **kwargs),
    )
    return candidate, head, fixer.seen


def test_the_test_is_written_first_then_the_fix_judged_by_it_as_one_patch(plain, tmp_path, monkeypatch):
    fixer = _fixer("right")

    candidate, head, seen = _prepare(tmp_path, monkeypatch, fixer)

    assert candidate.action is CandidateAction.PATCH
    # One patch against the branch tip: the test and the fix.
    assert candidate.changed_paths == ("tests/unit/value.tcl", "value.txt")
    assert "+test {value check}" in candidate.patch and "+right" in candidate.patch and "-before" in candidate.patch
    assert "+test {value check}" in candidate.evidence_patch and "value.txt" not in candidate.evidence_patch
    # The engine was given the test as the failure, frozen, on a working commit that holds it.
    judged = seen["tasks"][0]
    assert judged.failure.failed_tests == (_TEST,) and judged.frozen_paths == ("tests/unit/value.tcl",)
    assert judged.target.expected_sha != head and judged.failure.head_sha == head
    assert "fails on the working commit; the fix must make it pass" in judged.failure.description
    assert seen["log"].startswith("# Evidence\nThe test `value check` in `tests/unit/value.tcl`, written from issue #9")
    # What the report says, evidence first.
    lines = candidate.verification.splitlines()
    assert lines[0].startswith("The test `value check` in `tests/unit/value.tcl`, written from the issue, fails on")
    assert any("passed 40 loop(s)" in line for line in lines)
    assert any("passes in full" in line for line in lines) and any("AddressSanitizer" in line for line in lines)
    assert any(line.startswith("Review: covers the issue") for line in lines)


def test_the_fix_and_its_review_get_the_factorys_guide_not_the_ci_fix_bots(plain, tmp_path, monkeypatch):
    seen = {}

    def fix_func(task, log, **kwargs):
        seen.update(kwargs)
        return FixCandidate(CandidateAction.REFUSE, summary="n")

    # Valkey has a guide, which the engine ends with the repository's own guidelines.
    assert prepare_mod.guide_for("valkey-io/valkey").startswith("# Fixing a Valkey issue from its test")
    assert prepare_mod.guide_for("nobody/nothing") == ""
    source, head = _source(tmp_path)
    _install_clone(monkeypatch, source)
    guides = tmp_path / "guides"
    guides.mkdir()
    (guides / "o__r.md").write_text("# Fixing an o/r issue from its test\n", encoding="utf-8")
    monkeypatch.setattr(prepare_mod, "_GUIDES_DIR", guides)
    prepare_mod.prepare_in(
        _plan(head), run_test=engine_mod.run_test_command, fix_func=fix_func,
        evidence_func=lambda *args, **kwargs: evidence_mod.establish_evidence(*args, agent_func=_test_writer, **kwargs),
    )

    assert seen["guide"] == "# Fixing an o/r issue from its test\n"


def test_a_fix_that_changes_the_test_goes_back_and_the_test_stays(plain, tmp_path, monkeypatch):
    fixer = _fixer("touch the test", "right")

    candidate, _head, seen = _prepare(tmp_path, monkeypatch, fixer)

    assert candidate.action is CandidateAction.PATCH
    assert "holds the test the fix is judged by" in seen["feedback"][1]
    assert "assert_equal right" in candidate.patch and "assert_equal before" not in candidate.patch


def test_without_evidence_there_is_no_fix_and_the_fixer_never_runs(plain, tmp_path, monkeypatch):
    def nothing(task, brief, repo_dir, **_kwargs):
        return TestCandidate("refuse", summary="the issue describes no behaviour a test could check")

    fixer = _fixer("right")
    candidate, _head, seen = _prepare(tmp_path, monkeypatch, fixer, test_writer=nothing)

    assert candidate.action is CandidateAction.REFUSE and "no behaviour a test could check" in candidate.summary
    assert seen["tasks"] == [] and candidate.evidence_patch == ""


def test_a_fix_the_engine_refuses_still_reports_the_test(plain, tmp_path, monkeypatch):
    def refusing(task, repo_dir, job_log, **_kwargs):
        return FixCandidate(CandidateAction.REFUSE, summary="the cause is in a dependency")

    refusing.seen = {"feedback": [], "tasks": []}
    candidate, _head, _seen = _prepare(tmp_path, monkeypatch, refusing)

    assert candidate.action is CandidateAction.REFUSE and candidate.summary == "the cause is in a dependency"
    assert "+test {value check}" in candidate.evidence_patch and candidate.patch == ""
    assert candidate.verification.startswith("The test `value check`")


def test_an_unexpected_crash_stops_the_run_with_nothing_to_publish(plain, tmp_path, monkeypatch):
    def crashing(*_args, **_kwargs):
        raise evidence_mod.Stopped("The test crashed the server, which the issue does not report.")

    source, head = _source(tmp_path)
    _install_clone(monkeypatch, source)
    candidate = prepare_mod.prepare_in(_plan(head), run_test=engine_mod.run_test_command, evidence_func=crashing)

    assert candidate.action is CandidateAction.REFUSE
    assert candidate.summary.startswith("The bot stopped. The test crashed the server") and candidate.evidence_patch == ""


def test_a_working_commit_that_does_not_build_is_a_failed_attempt(plain, tmp_path, monkeypatch):
    monkeypatch.setattr(prepare_mod, "JOB", JobCommands(build="false", runtest="true"))
    source, head = _source(tmp_path)
    _install_clone(monkeypatch, source)

    with pytest.raises(RuntimeError, match="does not build"):
        prepare_mod.prepare_in(_plan(head), run_test=engine_mod.run_test_command)
