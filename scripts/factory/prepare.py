"""Prepare a fix for a labelled issue: the evidence first, then the change.

Both phases run in one checkout of the default branch's tip, a clone made
here (there is no CI job to prepare the fix in), built once by code before
any agent runs so that each phase verifies against an incremental build.

1. **Evidence** (:mod:`scripts.factory.evidence`): a test that fails on the
   working commit, ``EVIDENCE_RUNS`` times, on its own assertion. Code commits
   it in the checkout, so the working commit the fix is prepared on holds it,
   and freezes its files: the fix may not change them.
2. **The fix**: the CI fix engine (:func:`scripts.ci_fix.engine.prepare_fix`),
   as for any failure, with the test's output as the failure's log, the test
   as the failing test, the adversarial review (:mod:`scripts.factory.review`)
   and the further checks (:mod:`scripts.factory.checks`).

The candidate that comes out carries the test and the fix as one patch against
the branch tip, for one commit, and the test alone as its evidence.
"""

from __future__ import annotations

import logging
import time
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Callable

from scripts.ci_fix import engine
from scripts.ci_fix.agent import run_fix_agent
from scripts.ci_fix.ci_job import JobCommands
from scripts.ci_fix.engine import PREPARE_BUDGET_S, AgentFunc, TestFunc, checkout, keep_existing, run_test_command
from scripts.ci_fix.models import CandidateAction, FixCandidate, FixPlan
from scripts.common.proc import BOT_EMAIL, BOT_NAME, git_output, git_paths, run_git
from scripts.factory import checks, review
from scripts.factory.evidence import JOB, Evidence, Stopped, establish_evidence

logger = logging.getLogger(__name__)

# Of the preparation's budget, what the evidence may take at most: the fix
# needs the rest, its own rounds and its checks included.
EVIDENCE_BUDGET_S = 90 * 60
# The test commit's subject in the checkout; the published commit is one, test and fix together.
_TEST_COMMIT = "Test the issue"
# Where the bot's guide for fixing each repository's issues lives.
_GUIDES_DIR = Path(__file__).resolve().parent / "guides"


def guide_for(repo_full_name: str) -> str:
    """The bot's policy for a fix made from one of ``repo_full_name``'s issues, or "" when it has none."""
    try:
        return (_GUIDES_DIR / f"{repo_full_name.replace('/', '__')}.md").read_text(encoding="utf-8")
    except OSError:
        return ""


def prepare(plan: FixPlan, repo_dir: str = "", job: JobCommands | None = None) -> FixCandidate:
    """The candidate for ``plan``: the test and the fix, or the refusal to make.

    ``repo_dir`` and ``job`` are what every preparer takes (a CI job's own
    checkout and commands); an issue's fix has neither, and clones.
    """
    return prepare_in(plan, repo_dir, run_test=run_test_command)


def prepare_in(
    plan: FixPlan,
    repo_dir: str = "",
    *,
    run_test: TestFunc,
    evidence_func: Callable[..., Evidence | FixCandidate] = establish_evidence,
    fix_func: Callable[..., FixCandidate] = engine.prepare_fix,
    fix_agent: AgentFunc = run_fix_agent,
) -> FixCandidate:
    """``prepare`` with its test runner, its phases and the fix agent replaceable."""
    brief = plan.brief
    if brief is None:
        raise ValueError("a factory plan carries its issue brief")
    task = plan.task
    with checkout(task, repo_dir) as (repo_dir, head):
        started = time.monotonic()
        deadline = started + PREPARE_BUDGET_S
        _build_once(repo_dir, run_test, deadline)
        # The repository's own guidelines, for the test; the bot's policy for
        # a fix from an issue, then the guidelines, for the fix and its review.
        guidelines = engine.repository_guidelines(repo_dir)
        try:
            evidence = evidence_func(
                task, brief, repo_dir, head, deadline=min(started + EVIDENCE_BUDGET_S, deadline),
                guide=guidelines, run_test=run_test,
            )
        except Stopped as exc:
            logger.warning("Issue #%s: %s", brief.number, exc)
            return FixCandidate(action=CandidateAction.REFUSE, summary=f"The bot stopped. {exc}")
        if isinstance(evidence, FixCandidate):
            return evidence
        test_sha = _commit(repo_dir, evidence)
        judged = replace(
            task,
            failure=replace(
                task.failure,
                failed_tests=evidence.tests,
                description=(
                    f"{task.failure.description} A test written from the issue, {_named(evidence)}, fails on "
                    "the working commit; the fix must make it pass."
                ),
            ),
            target=replace(task.target, expected_sha=test_sha),
            frozen_paths=evidence.paths,
        )
        fixed = fix_func(
            judged, evidence.log, agent_func=fix_agent, review_func=partial(review.review, run_test=run_test),
            test_func=run_test, repo_dir=repo_dir, job=JOB, check_func=checks.gate(evidence),
            budget_s=max(60, int(deadline - time.monotonic())), guide=guide_for(task.failure.repo_full_name),
        )
        verification = "\n".join([*evidence.checks, *fixed.verification.splitlines()])
        if fixed.action is not CandidateAction.PATCH:
            # A refusal: the test is still worth having, and is reported with it.
            return replace(fixed, evidence_patch=evidence.patch, verification=verification)
        paths = tuple(sorted({*evidence.paths, *fixed.changed_paths}))
        return replace(
            fixed, patch=_combined_patch(repo_dir, head, paths), changed_paths=paths, evidence_patch=evidence.patch,
            verification=verification,
        )


def _named(evidence: Evidence) -> str:
    return ", ".join(f"`{test.name}` in `{test.file}`" if test.name else f"`{test.file}`" for test in evidence.tests)


def _build_once(repo_dir: str, run_test: TestFunc, deadline: float) -> None:
    """Build the clean checkout, and take that build as the checkout's own.

    The agent's rounds then rebuild only what changed, and what the agent
    leaves among the ignored files is removed before each verification
    (:func:`scripts.ci_fix.engine.remove_ignored`), as in a CI job's checkout.
    """
    result = run_test(repo_dir, JOB.build, max(60, int(deadline - time.monotonic())))
    if not result.passed:
        raise RuntimeError(f"The working commit does not build: `{JOB.build}`.\n{result.output[-4_000:]}")
    keep_existing(repo_dir)


def _commit(repo_dir: str, evidence: Evidence) -> str:
    """Commit the test in the checkout, as the workflow user; the SHA the fix is prepared on."""
    run_git(repo_dir, "add", "--", *evidence.paths)
    run_git(
        repo_dir, "-c", f"user.name={BOT_NAME}", "-c", f"user.email={BOT_EMAIL}", "commit", "--quiet", "--no-verify",
        "-m", _TEST_COMMIT,
    )
    return git_output(repo_dir, "rev-parse", "HEAD").strip()


def _combined_patch(repo_dir: str, head: str, paths: tuple[str, ...]) -> str:
    """The test commit and the fix in the tree as one patch against ``head``, the branch tip."""
    untracked = git_paths(repo_dir, "ls-files", "--others", "--exclude-standard", "-z", "--", *paths)
    if untracked:
        run_git(repo_dir, "add", "--intent-to-add", "--", *untracked)
    return run_git(
        repo_dir, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--binary", head, "--", *paths,
    ).stdout
