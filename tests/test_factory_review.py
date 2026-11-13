"""The adversarial review, with the reviewer's counterexample run by code."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.ci_fix.agent import Counterexample, Review
from scripts.ci_fix.models import CandidateAction, FixCandidate, FixContext
from scripts.ci_fix.verification import LOOPS, TestRun, VerifyPlan
from scripts.factory import review as review_mod

_CANDIDATE = FixCandidate(CandidateAction.PATCH, patch="diff")
_TCL = 'start_server {tags {"x"}} {\n    test {restart keeps it} {\n        assert_equal 1 1\n    }\n}\n'


def _reviewer(monkeypatch, counterexample=None, approved=True):
    seen = {}

    def adversarial_review(candidate, repo_dir, job_log, *, context, timeout):
        seen.update(candidate=candidate, timeout=timeout)
        return Review(approved=approved, reasoning="the fix covers the issue", counterexample=counterexample)

    monkeypatch.setattr(review_mod, "adversarial_review", adversarial_review)
    return seen


def _runner(result: TestRun):
    ran = []

    def run_test(repo_dir, command, timeout):
        ran.append((command, Path(repo_dir, "tests/unit").glob("*.ci-fix-isolated.tcl")))
        # The counterexample file exists while its test runs.
        ran[-1] = (command, [path.name for path in ran[-1][1]])
        return result

    run_test.ran = ran
    return run_test


@pytest.fixture
def repo(tmp_path, monkeypatch):
    (tmp_path / "tests" / "unit").mkdir(parents=True)
    monkeypatch.setattr(
        review_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(
            build="true", test=f"./runtest --single {tests[0].unit} --only {tests[0].name!r} loops={LOOPS}", tests=tests,
        ),
    )
    return tmp_path


def _review(repo, run_test):
    return review_mod.review(_CANDIDATE, str(repo), "log", context=FixContext(), timeout=600, run_test=run_test)


def test_without_a_counterexample_the_reviewers_verdict_stands(repo, monkeypatch):
    _reviewer(monkeypatch, approved=False)
    assert _review(repo, _runner(TestRun(True, ""))) == (False, "the fix covers the issue")


def test_a_counterexample_that_fails_sends_the_fix_back_with_the_output(repo, monkeypatch):
    _reviewer(monkeypatch, Counterexample("restart keeps it", _TCL))
    failing = _runner(TestRun(False, "[err]: restart keeps it in tests/unit/counterexample-restart-keeps-it.ci-fix-isolated.tcl",
                              report="[err]: restart keeps it in tests/unit/counterexample-restart-keeps-it.ci-fix-isolated.tcl"))

    approved, reasoning = _review(repo, failing)

    assert approved is False
    assert reasoning.startswith("The reviewer's counterexample `restart keeps it` fails with the fix.")
    assert "Its output:" in reasoning and reasoning.endswith("The reviewer's reasoning: the fix covers the issue")
    command, files = failing.ran[0]
    assert command == "true && ./runtest --single unit/counterexample-restart-keeps-it.ci-fix-isolated --only 'restart keeps it' loops=1"
    assert files == ["counterexample-restart-keeps-it.ci-fix-isolated.tcl"]
    # And the file is gone: the tree is the candidate.
    assert list((repo / "tests" / "unit").iterdir()) == []


def test_a_counterexample_that_passes_is_noted_and_the_verdict_stands(repo, monkeypatch):
    _reviewer(monkeypatch, Counterexample("restart keeps it", _TCL))
    passing = _runner(TestRun(True, "", report="[ok]: restart keeps it (3 ms)"))

    approved, reasoning = _review(repo, passing)

    assert approved is True
    assert reasoning == "the fix covers the issue The reviewer's counterexample `restart keeps it` passes with the fix."


def test_a_counterexample_that_cannot_run_is_not_counted(repo, monkeypatch):
    _reviewer(monkeypatch, Counterexample("restart keeps it", _TCL), approved=False)
    broken = _runner(TestRun(False, "[exception]: Executing test client: invalid command", report="[exception]: x"))

    approved, reasoning = _review(repo, broken)

    assert approved is False and reasoning.endswith("could not run (a Tcl error, or a server that did not start), and was not counted.")
    assert list((repo / "tests" / "unit").iterdir()) == []
