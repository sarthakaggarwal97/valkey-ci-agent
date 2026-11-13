"""What a fix has to pass besides its own test, before the review sees it.

The engine shows that the test the fix is judged by passes with the fix, in a
loop. These checks are the rest of what a reviewer would otherwise find out
on the pull request, in the order of their cost: the change's scope, every
changed test file in full, the formatting and generated-file checks Valkey's
CI runs, and the test once under AddressSanitizer. A check that fails sends
the change back to the agent with the output, like a failing test; one that
passes is recorded for the report. Every check leaves the tree as it found
it: the candidate is the tree.
"""

from __future__ import annotations

import logging
import re
import shlex
import shutil
import time
from dataclasses import replace
from pathlib import Path

from scripts.ci_fix.ci_job import JobCommands
from scripts.ci_fix.engine import CheckFunc, TestFunc, changed_lines, is_test_path
from scripts.ci_fix.models import FailedTest, FixCandidate
from scripts.ci_fix.verification import UNIT_TEST_BINARY, plan_for
from scripts.factory.evidence import JOB, Evidence

logger = logging.getLogger(__name__)

# A fix small enough to review in one sitting: its added and removed lines,
# the test apart. The median fix in the issues surveyed changed under 60.
MAX_FIX_LINES = 400
# Valkey's CI formats with this version; another version formats differently.
CLANG_FORMAT = "clang-format-18"
# Where a new command is declared: a new one is a TSC decision, not a fix.
_COMMAND_DEFINITIONS = "src/commands/"
# Valkey's CI: `make commands.def` must change nothing.
_GENERATED_FROM_COMMANDS = "src/commands.def"
# Text that must never leave the fix job: a credential the model repeated or invented.
_SECRET_RE = re.compile(
    r"ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{22,}|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r"|xox[abprs]-[A-Za-z0-9-]{10,}|sk-[A-Za-z0-9]{32,}"
)
_C_SOURCE_SUFFIXES = (".c", ".h", ".cpp", ".hpp")
# Valkey's CI builds the sanitizer job with these; the unit tests are built in when the evidence is one.
_ASAN_BUILD = "make SANITIZER=address"
_ASAN_UNIT_BUILD = "make all-with-unit-tests SANITIZER=address"
_GTEST_RE = re.compile(r"\bTEST(?:_F|_P)?\(\s*(\w+)\s*,")


def gate(evidence: Evidence) -> CheckFunc:
    """The checks for a fix judged by ``evidence``, as the engine calls them."""

    def check(candidate: FixCandidate, repo_dir: str, run_test: TestFunc, deadline: float, checks: list[str]) -> str:
        for step in (_scope, _secrets):
            failed = step(candidate)
            if failed:
                return failed
        for run in (_whole_test_files, _formatting, _generated, _sanitizer):
            failed = run(evidence, candidate, repo_dir, run_test, deadline, checks)
            if failed:
                return failed
        return ""

    return check


def _scope(candidate: FixCandidate) -> str:
    """Feedback when the change reaches where a fix from an issue may not, or is too large to review."""
    files = changed_lines(candidate.patch)
    workflow = [path for path in files if path.startswith(".github/")]
    if workflow:
        return (
            "The change touches " + ", ".join(f"`{path}`" for path in workflow) + ": a workflow or action "
            "change runs with the repository's own privileges, and a person applies it. Fix the issue in the code."
        )
    vendored = [path for path in files if path.startswith("deps/")]
    if vendored:
        return (
            "The change touches " + ", ".join(f"`{path}`" for path in vendored) + ", vendored from another "
            "project, which Valkey takes only as an update from that project. Change Valkey's own code, or say in "
            "`validation` which project needs the fix and refuse."
        )
    added_commands = [
        path for path in _added_files(candidate.patch) if path.startswith(_COMMAND_DEFINITIONS) and path.endswith(".json")
    ]
    if added_commands:
        return (
            "The change adds " + ", ".join(f"`{path}`" for path in added_commands) + ": a new command is a "
            "decision for the project's TSC, not a fix. Refuse, and say what the issue needs a decision on."
        )
    lines = sum(
        1 for line in candidate.patch.splitlines()
        if (line.startswith("+") or line.startswith("-")) and not line.startswith(("+++", "---"))
    )
    if lines > MAX_FIX_LINES:
        return (
            f"The change adds and removes {lines} lines, more than a reviewer reads in one sitting "
            f"({MAX_FIX_LINES}). Make the smallest change that removes the cause."
        )
    return ""


def _added_files(patch: str) -> list[str]:
    added, previous = [], ""
    for line in patch.splitlines():
        if line.startswith("--- "):
            previous = line[4:]
        elif line.startswith("+++ ") and previous == "/dev/null":
            added.append(line[6:] if line.startswith("+++ b/") else line[4:])
    return added


def _secrets(candidate: FixCandidate) -> str:
    found = _SECRET_RE.search(candidate.patch + "\n" + candidate.validation + "\n" + candidate.reasoning)
    if found is None:
        return ""
    return (
        f"The change holds what looks like a credential (`{found.group(0)[:12]}...`). Nothing like it may leave "
        "this job: remove it."
    )


def _whole_test_files(
    evidence: Evidence, candidate: FixCandidate, repo_dir: str, run_test: TestFunc, deadline: float,
    checks: list[str],
) -> str:
    """Every test file the test or the fix changed has to pass in full, once."""
    files = sorted({*evidence.paths, *(path for path in changed_lines(candidate.patch) if is_test_path(path))})
    for file in files:
        command = _whole_file_command(evidence, repo_dir, file)
        if command is None:
            checks.append(f"`{file}` changed, but nothing here runs it whole.")
            continue
        result = run_test(repo_dir, command, _timeout(deadline))
        if result.timed_out:
            raise RuntimeError(f"`{command}` did not finish in the time left ({result.output}).")
        if not result.passed:
            return (
                f"With your change, `{file}` does not pass in full: `{command}`. Another test in the file "
                f"fails, or the file no longer runs.\n\nIts output:\n{result.output}"
            )
        checks.append(f"With the fix, `{file}` passes in full: `{command}`.")
    return ""


def _whole_file_command(evidence: Evidence, repo_dir: str, file: str) -> str | None:
    if file.startswith("tests/") and file.endswith(".tcl"):
        # A whole-file plan: the file once, as the fallback of a test that cannot run alone runs it.
        plan = plan_for(repo_dir, (FailedTest("", file),), scheduled=False, job=JOB)
        return plan.command(1) if plan is not None else None
    if file.startswith("src/unit/") and file.endswith(_C_SOURCE_SUFFIXES):
        try:
            suites = sorted(set(_GTEST_RE.findall(Path(repo_dir, file).read_text(encoding="utf-8", errors="replace"))))
        except OSError:
            return None
        if not suites:
            return None
        names = ":".join(f"{suite}.*" for suite in suites)
        return f"{evidence.plan.build} && {UNIT_TEST_BINARY} --gtest_filter={shlex.quote(names)}"
    return None


def _formatting(
    evidence: Evidence, candidate: FixCandidate, repo_dir: str, run_test: TestFunc, deadline: float,
    checks: list[str],
) -> str:
    """Valkey's clang-format check, on the C files the fix changed."""
    files = sorted(
        path for path in changed_lines(candidate.patch)
        if path.startswith("src/") and path.endswith(_C_SOURCE_SUFFIXES) and Path(repo_dir, path).is_file()
    )
    if not files:
        return ""
    if shutil.which(CLANG_FORMAT) is None:
        checks.append(f"{CLANG_FORMAT} is not installed here, so the pull request's own CI checks the formatting.")
        return ""
    command = f"{CLANG_FORMAT} --dry-run --Werror " + " ".join(shlex.quote(path) for path in files)
    result = run_test(repo_dir, command, _timeout(deadline))
    if result.timed_out:
        raise RuntimeError(f"`{command}` did not finish in the time left.")
    if not result.passed:
        return (
            f"`{CLANG_FORMAT}` wants the changed C files formatted differently. Format them as it says, and "
            f"nothing else.\n\nIts output:\n{result.output}"
        )
    checks.append(f"`{CLANG_FORMAT}` accepts the formatting of " + ", ".join(f"`{path}`" for path in files) + ".")
    return ""


def _generated(
    evidence: Evidence, candidate: FixCandidate, repo_dir: str, run_test: TestFunc, deadline: float,
    checks: list[str],
) -> str:
    """Valkey's CI check that ``commands.def`` is what the command definitions generate."""
    if not any(path.startswith(_COMMAND_DEFINITIONS) or path == _GENERATED_FROM_COMMANDS
               for path in changed_lines(candidate.patch)):
        return ""
    generated = Path(repo_dir, _GENERATED_FROM_COMMANDS)
    before = generated.read_bytes() if generated.is_file() else None
    command = f"touch {_COMMAND_DEFINITIONS}*.json && make commands.def"
    try:
        result = run_test(repo_dir, command, _timeout(deadline))
        if result.timed_out:
            raise RuntimeError(f"`{command}` did not finish in the time left.")
        after = generated.read_bytes() if generated.is_file() else None
    finally:
        # The tree is the candidate: what make wrote is not part of it.
        if before is not None:
            generated.write_bytes(before)
    if not result.passed:
        return f"`{command}` fails with your change.\n\nIts output:\n{result.output}"
    if after != before:
        return (
            f"`{_GENERATED_FROM_COMMANDS}` is not what `make commands.def` generates from your change to the "
            "command definitions. Regenerate it and include it in the change."
        )
    checks.append(f"`make commands.def` regenerates `{_GENERATED_FROM_COMMANDS}` unchanged.")
    return ""


def _sanitizer(
    evidence: Evidence, candidate: FixCandidate, repo_dir: str, run_test: TestFunc, deadline: float,
    checks: list[str],
) -> str:
    """The test once with the fix built under AddressSanitizer, as Valkey's CI builds it."""
    unit = any(test.is_unit_test for test in evidence.tests)
    job = JobCommands(build=_ASAN_UNIT_BUILD if unit else _ASAN_BUILD, runtest=JOB.runtest, moduleapi=JOB.moduleapi)
    plan = plan_for(repo_dir, evidence.tests, scheduled=False, job=job)
    if plan is None:
        checks.append("The test could not be planned under AddressSanitizer.")
        return ""
    plan = replace(plan, single_run=True)
    command = plan.command(1)
    result = run_test(repo_dir, command, _timeout(deadline))
    if result.timed_out:
        raise RuntimeError(f"`{command}` did not finish in the time left ({result.output}).")
    if not (result.passed and plan.ran(result)):
        return (
            f"With your change built under AddressSanitizer, the test does not pass: `{plan.quoted(1)}`. "
            "A sanitizer report is a defect in the change or one it uncovers; read it before you change "
            f"anything.\n\nIts output:\n{result.output}"
        )
    checks.append(f"With the fix built under AddressSanitizer, the test passes: `{plan.quoted(1)}`.")
    return ""


def _timeout(deadline: float) -> int:
    return max(60, int(deadline - time.monotonic()))
