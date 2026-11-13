"""The adversarial review of a fix written from an issue, with its counterexample run.

A second session reviews the fix with the issue and the test's output in
front of it (:func:`scripts.ci_fix.agent.adversarial_review`). Its job is to
find the case the change still gets wrong, and it may write that case as a
Tcl test. The session cannot run anything, so this module does: the test is
written beside the fix's test as a file the tree ignores, run once with the
fix, and removed. A counterexample that fails sends the change back with the
reviewer's reasoning; one that passes, or cannot run, leaves the reviewer's
own verdict to stand, and is noted.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from scripts.ci_fix.agent import Counterexample, adversarial_review
from scripts.ci_fix.engine import TestFunc, run_test_command
from scripts.ci_fix.models import FailedTest, FixCandidate, FixContext
from scripts.ci_fix.verification import ISOLATED_COPY_PATTERN, plan_for
from scripts.factory.evidence import JOB

logger = logging.getLogger(__name__)

# Where a counterexample is written: a file of its own in the main suite, with
# the suffix the tree already ignores (scripts/ci_fix/engine.py).
_SUFFIX = ISOLATED_COPY_PATTERN.rsplit("*", 1)[1]  # ".ci-fix-isolated.tcl"
_DIRECTORY = "tests/unit"


def review(
    candidate: FixCandidate, repo_dir: str, job_log: str, *, context: FixContext, timeout: int,
    run_test: TestFunc = run_test_command,
) -> tuple[bool, str]:
    """The adversarial review's verdict on ``candidate``, its counterexample run first."""
    started = time.monotonic()
    verdict = adversarial_review(candidate, repo_dir, job_log, context=context, timeout=timeout)
    if verdict.counterexample is None:
        return verdict.approved, verdict.reasoning
    left = max(60, timeout - int(time.monotonic() - started))
    outcome = run_counterexample(repo_dir, verdict.counterexample, run_test, left)
    if outcome.startswith("fails"):
        return False, f"The reviewer's counterexample `{verdict.counterexample.name}` {outcome} {verdict.reasoning}"
    return verdict.approved, f"{verdict.reasoning} The reviewer's counterexample `{verdict.counterexample.name}` {outcome}"


def run_counterexample(repo_dir: str, counterexample: Counterexample, run_test: TestFunc, timeout: int) -> str:
    """Run the reviewer's test once with the fix: how it went, as a clause after the test's name."""
    path = Path(repo_dir, _DIRECTORY, _slug(counterexample.name) + _SUFFIX)
    try:
        path.write_text(counterexample.tcl, encoding="utf-8")
        plan = plan_for(repo_dir, (FailedTest(counterexample.name, path.relative_to(repo_dir).as_posix()),),
                        scheduled=False, job=JOB)
        if plan is None:
            return "could not be run here, and was not counted."
        result = run_test(repo_dir, plan.command(1), timeout)
        if result.timed_out:
            return "did not finish in the time left, and was not counted."
        if result.passed and plan.ran(result):
            return "passes with the fix."
        if plan.failed(result):
            output = result.output.strip()
            return f"fails with the fix.\n\nIts output:\n{output}\n\nThe reviewer's reasoning:"
        return "could not run (a Tcl error, or a server that did not start), and was not counted."
    finally:
        path.unlink(missing_ok=True)


def _slug(name: str) -> str:
    return "counterexample-" + (re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40] or "test")
