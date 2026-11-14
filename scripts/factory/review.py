"""The adversarial review of a fix written from an issue, with its counterexample run.

A second session reviews the fix with the issue and the test's output in
front of it (:func:`scripts.ci_fix.agent.adversarial_review`). Its job is to
find the case the change still gets wrong, and it may write that case as a
Tcl test. The session cannot run anything, so this module does: the test is
written beside the fix's test as a file the tree ignores, run once with the
fix, and removed. A counterexample that fails with the fix is run once more
without it, on the working commit (the base and the frozen evidence test):
one that fails there too says nothing about the fix, and is dropped. One that
passes without the fix and fails with it sends the change back with the
reviewer's reasoning, once per run: after that the reviewer is asked to
approve or name a defect in the diff, and a further counterexample is run and
noted but cannot send the change back alone. One that passes, or cannot run,
leaves the reviewer's own verdict to stand, and is noted.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from scripts.ci_fix.agent import Counterexample, adversarial_review
from scripts.ci_fix.engine import TestFunc, restore, run_test_command, tree_patch
from scripts.ci_fix.models import FailedTest, FixCandidate, FixContext
from scripts.ci_fix.verification import ISOLATED_COPY_PATTERN, TestRun, plan_for
from scripts.common.proc import run_git
from scripts.factory.evidence import JOB

logger = logging.getLogger(__name__)

# Where a counterexample is written: a file of its own in the main suite, with
# the suffix the tree already ignores (scripts/ci_fix/engine.py).
_SUFFIX = ISOLATED_COPY_PATTERN.rsplit("*", 1)[1]  # ".ci-fix-isolated.tcl"
_DIRECTORY = "tests/unit"


def review(
    candidate: FixCandidate, repo_dir: str, job_log: str, *, context: FixContext, timeout: int,
    run_test: TestFunc = run_test_command, rejections: list[str] | None = None,
) -> tuple[bool, str]:
    """The adversarial review's verdict on ``candidate``, its counterexample run first.

    ``rejections`` holds the names of the counterexamples that sent a fix back
    in this run, shared by the run's rounds: once it holds one, no further
    counterexample rejects on its own. None keeps no count across calls.
    """
    if rejections is None:
        rejections = []
    started = time.monotonic()
    verdict = adversarial_review(
        candidate, repo_dir, job_log, context=context, timeout=timeout, after_rejection=bool(rejections),
    )
    if verdict.counterexample is None:
        return verdict.approved, verdict.reasoning
    name = verdict.counterexample.name
    left = max(60, timeout - int(time.monotonic() - started))
    rejects, outcome = run_counterexample(repo_dir, verdict.counterexample, run_test, left)
    if rejects and not rejections:
        rejections.append(name)
        return False, f"The reviewer's counterexample `{name}` {outcome} {verdict.reasoning}"
    if rejects:
        outcome = (
            "fails with the fix and passes without it, but a counterexample has already sent this fix back "
            "once in this run, so it does not on its own."
        )
    return verdict.approved, f"{verdict.reasoning} The reviewer's counterexample `{name}` {outcome}"


def run_counterexample(
    repo_dir: str, counterexample: Counterexample, run_test: TestFunc, timeout: int,
) -> tuple[bool, str]:
    """Run the reviewer's test with the fix, and without it when it fails.

    Whether it is evidence against the fix, and how it went, as a clause
    after the test's name.
    """
    started = time.monotonic()
    path = Path(repo_dir, _DIRECTORY, _slug(counterexample.name) + _SUFFIX)
    try:
        path.write_text(counterexample.tcl, encoding="utf-8")
        plan = plan_for(repo_dir, (FailedTest(counterexample.name, path.relative_to(repo_dir).as_posix()),),
                        scheduled=False, job=JOB)
        if plan is None:
            return False, "could not be run here, and was not counted."
        result = run_test(repo_dir, plan.command(1), timeout)
        if result.timed_out:
            return False, "did not finish in the time left, and was not counted."
        if result.passed and plan.ran(result):
            return False, "passes with the fix."
        if not plan.failed(result):
            return False, "could not run (a Tcl error, or a server that did not start), and was not counted."
        output = result.output.strip()
        without = _without_fix(repo_dir, run_test, plan.command(1), max(60, timeout - int(time.monotonic() - started)))
        if without.timed_out:
            return False, "fails with the fix, but its run without the fix did not finish in the time left, and was not counted."
        if plan.failed(without):
            logger.info(
                "The reviewer's counterexample `%s` fails without the fix too: dropped, it is not evidence against the fix",
                counterexample.name,
            )
            return False, "fails without the fix too, so it is not evidence against the fix, and was not counted."
        return True, f"fails with the fix and passes without it.\n\nIts output:\n{output}\n\nThe reviewer's reasoning:"
    finally:
        path.unlink(missing_ok=True)


def _without_fix(repo_dir: str, run_test: TestFunc, command: str, timeout: int) -> TestRun:
    """``command`` run on the working commit alone, the fix taken out of the tree and put back after."""
    fix = tree_patch(repo_dir)
    restore(repo_dir)
    try:
        return run_test(repo_dir, command, timeout)
    finally:
        if fix:
            run_git(repo_dir, "apply", "--whitespace=nowarn", "-", input=fix)


def _slug(name: str) -> str:
    return "counterexample-" + (re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40] or "test")
