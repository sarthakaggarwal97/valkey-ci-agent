"""The adversarial review, with the reviewer's counterexample run by code."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.ci_fix.agent import Counterexample, Review
from scripts.ci_fix.models import CandidateAction, FixCandidate, FixContext
from scripts.ci_fix.verification import LOOPS, TestRun, VerifyPlan
from scripts.factory import review as review_mod
from tests.git_helpers import git

_CANDIDATE = FixCandidate(CandidateAction.PATCH, patch="diff")
_TCL = 'start_server {tags {"x"}} {\n    test {restart keeps it} {\n        assert_equal 1 1\n    }\n}\n'
_COUNTEREXAMPLE = Counterexample("restart keeps it", _TCL)
_FAILS = TestRun(False, "[err]: restart keeps it in tests/unit/counterexample-restart-keeps-it.ci-fix-isolated.tcl",
                 report="[err]: restart keeps it in tests/unit/counterexample-restart-keeps-it.ci-fix-isolated.tcl")
_PASSES = TestRun(True, "", report="[ok]: restart keeps it (3 ms)")


def _reviewer(monkeypatch, counterexample=None, approved=True):
    seen = {}

    def adversarial_review(candidate, repo_dir, job_log, *, context, timeout, after_rejection=False):
        seen.update(candidate=candidate, timeout=timeout, after_rejection=after_rejection)
        return Review(approved=approved, reasoning="the fix covers the issue", counterexample=counterexample)

    monkeypatch.setattr(review_mod, "adversarial_review", adversarial_review)
    return seen


def _runner(*results: TestRun):
    """A test runner that answers ``results`` in turn and records what each run saw in the tree."""
    answers = iter(results)
    ran = []

    def run_test(repo_dir, command, timeout):
        # The counterexample file exists while its test runs; the fix may not.
        files = sorted(path.name for path in Path(repo_dir, "tests/unit").glob("*.ci-fix-isolated.tcl"))
        ran.append((command, files, Path(repo_dir, "src/value.c").read_text(encoding="utf-8")))
        return next(answers)

    run_test.ran = ran
    return run_test


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A checkout at the working commit, with the fix in its tree and a new file beside it."""
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "user.email", "test@example.com")
    (tmp_path / "tests" / "unit").mkdir(parents=True)
    (tmp_path / "tests" / "unit" / "value.tcl").write_text("test {value check} {}\n", encoding="utf-8")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "value.c").write_text("base\n", encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-q", "-m", "Test the issue")
    (tmp_path / ".git" / "info" / "exclude").write_text("*.ci-fix-isolated.tcl\n", encoding="utf-8")
    (tmp_path / "src" / "value.c").write_text("fixed\n", encoding="utf-8")
    (tmp_path / "src" / "helper.c").write_text("new\n", encoding="utf-8")
    monkeypatch.setattr(
        review_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(
            build="true", test=f"./runtest --single {tests[0].unit} --only {tests[0].name!r} loops={LOOPS}", tests=tests,
        ),
    )
    return tmp_path


def _review(repo, run_test, rejections=None):
    return review_mod.review(
        _CANDIDATE, str(repo), "log", context=FixContext(), timeout=600, run_test=run_test, rejections=rejections,
    )


def _tree_is_the_candidate(repo: Path) -> None:
    assert (repo / "src" / "value.c").read_text(encoding="utf-8") == "fixed\n"
    assert (repo / "src" / "helper.c").read_text(encoding="utf-8") == "new\n"
    assert list((repo / "tests" / "unit").glob("*.ci-fix-isolated.tcl")) == []


def test_without_a_counterexample_the_reviewers_verdict_stands(repo, monkeypatch):
    _reviewer(monkeypatch, approved=False)
    assert _review(repo, _runner(_PASSES)) == (False, "the fix covers the issue")


def test_a_counterexample_that_fails_with_the_fix_and_passes_without_sends_the_fix_back(repo, monkeypatch):
    _reviewer(monkeypatch, _COUNTEREXAMPLE)
    runner = _runner(_FAILS, _PASSES)
    rejections: list[str] = []

    approved, reasoning = _review(repo, runner, rejections)

    assert approved is False
    assert reasoning.startswith("The reviewer's counterexample `restart keeps it` fails with the fix and passes without it.")
    assert "Its output:" in reasoning and reasoning.endswith("The reviewer's reasoning: the fix covers the issue")
    command = "true && ./runtest --single unit/counterexample-restart-keeps-it.ci-fix-isolated --only 'restart keeps it' loops=1"
    # Once with the fix, once on the working commit alone, the same command both times.
    assert runner.ran == [
        (command, ["counterexample-restart-keeps-it.ci-fix-isolated.tcl"], "fixed\n"),
        (command, ["counterexample-restart-keeps-it.ci-fix-isolated.tcl"], "base\n"),
    ]
    assert rejections == ["restart keeps it"]
    # And the fix is back, and the file is gone: the tree is the candidate.
    _tree_is_the_candidate(repo)


def test_a_counterexample_that_fails_without_the_fix_too_is_dropped(repo, monkeypatch, caplog):
    _reviewer(monkeypatch, _COUNTEREXAMPLE)
    runner = _runner(_FAILS, _FAILS)
    rejections: list[str] = []

    with caplog.at_level("INFO", logger="scripts.factory.review"):
        approved, reasoning = _review(repo, runner, rejections)

    assert approved is True
    assert reasoning == (
        "the fix covers the issue The reviewer's counterexample `restart keeps it` fails without the fix too, "
        "so it is not evidence against the fix, and was not counted."
    )
    assert [run[2] for run in runner.ran] == ["fixed\n", "base\n"]
    assert rejections == []
    assert "fails without the fix too: dropped" in caplog.text
    _tree_is_the_candidate(repo)


def test_a_counterexample_rejects_once_per_run(repo, monkeypatch):
    seen = _reviewer(monkeypatch, _COUNTEREXAMPLE)
    rejections: list[str] = []

    first = _review(repo, _runner(_FAILS, _PASSES), rejections)
    assert first[0] is False and seen["after_rejection"] is False

    # The next round's reviewer is told, and its counterexample, run both ways again, cannot reject alone.
    runner = _runner(_FAILS, _PASSES)
    approved, reasoning = _review(repo, runner, rejections)

    assert seen["after_rejection"] is True
    assert approved is True
    assert reasoning == (
        "the fix covers the issue The reviewer's counterexample `restart keeps it` fails with the fix and passes "
        "without it, but a counterexample has already sent this fix back once in this run, so it does not on its own."
    )
    assert [run[2] for run in runner.ran] == ["fixed\n", "base\n"]
    assert rejections == ["restart keeps it"]
    _tree_is_the_candidate(repo)


def test_a_counterexample_that_passes_is_noted_and_the_verdict_stands(repo, monkeypatch):
    _reviewer(monkeypatch, _COUNTEREXAMPLE)
    passing = _runner(_PASSES)

    approved, reasoning = _review(repo, passing)

    assert approved is True
    assert reasoning == "the fix covers the issue The reviewer's counterexample `restart keeps it` passes with the fix."
    assert len(passing.ran) == 1


def test_a_counterexample_that_cannot_run_is_not_counted(repo, monkeypatch):
    _reviewer(monkeypatch, _COUNTEREXAMPLE, approved=False)
    broken = _runner(TestRun(False, "[exception]: Executing test client: invalid command", report="[exception]: x"))

    approved, reasoning = _review(repo, broken)

    assert approved is False and reasoning.endswith("could not run (a Tcl error, or a server that did not start), and was not counted.")
    assert len(broken.ran) == 1
    _tree_is_the_candidate(repo)
