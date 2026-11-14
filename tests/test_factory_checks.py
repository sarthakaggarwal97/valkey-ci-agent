"""What a fix from an issue has to pass besides its own test."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import pytest

from scripts.ci_fix.models import CandidateAction, FailedTest, FixCandidate
from scripts.ci_fix.verification import LOOPS, TestRun, VerifyPlan
from scripts.factory import checks as checks_mod
from scripts.factory.evidence import Evidence

_TEST = FailedTest("value check", "tests/unit/value.tcl")
_PLAN = VerifyPlan(build="true", test=f"true loops={LOOPS}", tests=(_TEST,))


def _evidence(paths=("tests/unit/value.tcl",), tests=(_TEST,)) -> Evidence:
    return Evidence(tests=tests, patch="diff", paths=paths, plan=_PLAN, log="log", checks=())


def _candidate(patch: str, **fields) -> FixCandidate:
    return FixCandidate(CandidateAction.PATCH, patch=patch, **fields)


def _diff(path: str, added: int = 1, new: bool = False) -> str:
    header = f"diff --git a/{path} b/{path}\n--- {'/dev/null' if new else 'a/' + path}\n+++ b/{path}\n@@ -0,0 +1 @@\n"
    return header + "".join(f"+line {index}\n" for index in range(added))


def _runner(results: dict[str, TestRun] | None = None, default_passed: bool = True):
    """A test runner answering from ``results`` by a substring of the command, recording what ran."""
    ran: list[str] = []

    def run_test(repo_dir: str, command: str, timeout: int) -> TestRun:
        ran.append(command)
        for needle, result in (results or {}).items():
            if needle in command:
                return result
        return TestRun(passed=default_passed, output="ok" if default_passed else "boom", report="[ok]: value check (1 ms)\n[ok]: Alpha.One (1 ms)")

    run_test.ran = ran
    return run_test


@pytest.fixture
def repo(tmp_path, monkeypatch):
    (tmp_path / "tests" / "unit").mkdir(parents=True)
    (tmp_path / "tests" / "unit" / "value.tcl").write_text("start_server {} {}\n", encoding="utf-8")
    (tmp_path / "src").mkdir()
    monkeypatch.setattr(
        checks_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(
            build=job.build if job and "SANITIZER" in job.build else "true", test=f"./runtest --single {tests[0].unit} loops={LOOPS}", tests=tests,
        ),
    )
    return tmp_path


def _gate(evidence, candidate, repo, run_test, checks=None):
    checks = [] if checks is None else checks
    failed = checks_mod.gate(evidence)(candidate, str(repo), run_test, 1e12, checks)
    return failed, checks


@pytest.mark.parametrize("patch, said", [
    (_diff(".github/workflows/ci.yml"), "a workflow or action change"),
    (_diff("deps/jemalloc/x.c"), "vendored from another project"),
    (_diff("src/commands/newcmd.json", new=True), "a new command is a decision"),
    (_diff("src/t_string.c", added=checks_mod.MAX_FIX_LINES + 1), "more than a reviewer reads"),
])
def test_a_change_outside_a_fixs_scope_goes_back_before_anything_runs(repo, patch, said):
    run_test = _runner()
    failed, checks = _gate(_evidence(), _candidate(patch), repo, run_test)
    assert said in failed and run_test.ran == [] and checks == []


def test_a_changed_command_definition_is_allowed_but_a_new_one_is_not(repo):
    allowed = _candidate(_diff("src/commands/xackdel.json"))
    assert checks_mod._scope(allowed) == ""


def test_what_looks_like_a_credential_never_leaves_the_job(repo):
    candidate = _candidate(_diff("src/x.c").replace("+line 0", "+token = ghp_" + "a" * 36))
    failed, _checks = _gate(_evidence(), candidate, repo, _runner())
    assert "looks like a credential" in failed


def test_every_changed_test_file_runs_in_full_and_a_failing_one_goes_back(repo):
    (repo / "tests" / "unit" / "other.tcl").write_text("start_server {} {}\n", encoding="utf-8")
    candidate = _candidate(_diff("src/x.c") + _diff("tests/unit/other.tcl"))
    run_test = _runner()

    failed, checks = _gate(_evidence(), candidate, repo, run_test)

    assert failed == ""
    whole = [command for command in run_test.ran if "--single" in command]
    assert whole[:2] == ["true && ./runtest --single unit/other loops=1", "true && ./runtest --single unit/value loops=1"]
    assert checks[0] == "With the fix, `tests/unit/other.tcl` passes in full: `true && ./runtest --single unit/other loops=1`."

    failing = _runner({"unit/value": TestRun(passed=False, output="[err]: neighbour in tests/unit/value.tcl")})
    failed, _checks = _gate(_evidence(), _candidate(_diff("src/x.c")), repo, failing)
    assert failed.startswith("With your change, `tests/unit/value.tcl` does not pass in full")


def test_a_c_unit_test_file_runs_its_suites(repo):
    (repo / "src" / "unit").mkdir()
    (repo / "src" / "unit" / "test_x.cpp").write_text("TEST(Alpha, One) {}\nTEST_F(Beta, Two) {}\n", encoding="utf-8")
    evidence = _evidence(paths=("src/unit/test_x.cpp",), tests=(FailedTest("Alpha.One", "src/unit/test_x.cpp"),))
    run_test = _runner()

    failed, checks = _gate(evidence, _candidate(_diff("src/x.c")), repo, run_test)

    assert failed == ""
    assert run_test.ran[0] == "true && ./src/unit/valkey-unit-gtests --gtest_filter='Alpha.*:Beta.*'"


def test_formatting_is_checked_with_valkeys_clang_format_on_the_changed_lines_only(repo, monkeypatch):
    (repo / "src" / "x.c").write_text("int x;\n", encoding="utf-8")
    candidate = _candidate(_diff("src/x.c"))

    monkeypatch.setattr(shutil, "which", lambda name: None)
    failed, checks = _gate(_evidence(), candidate, repo, _runner())
    assert failed == "" and any("is not installed here" in check for check in checks)

    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/" + name)
    run_test = _runner()
    failed, checks = _gate(_evidence(), candidate, repo, run_test)
    assert failed == "" and "clang-format-18 --dry-run --Werror --lines=1:1 src/x.c" in run_test.ran
    assert any("accepts the formatting of the changed lines of `src/x.c`" in check for check in checks)

    # Each file gets its own ranges, one per run of consecutive changed lines; a deletion marks the line in its place.
    (repo / "src" / "y.h").write_text("int y;\n", encoding="utf-8")
    hunks = (
        "diff --git a/src/y.h b/src/y.h\n--- a/src/y.h\n+++ b/src/y.h\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n"
        "@@ -10,2 +10,3 @@\n j\n+K\n+L\n m\n@@ -20,2 +21,1 @@\n t\n-u\n"
    )
    run_test = _runner()
    _gate(_evidence(), _candidate(_diff("src/x.c") + hunks), repo, run_test)
    assert (
        "clang-format-18 --dry-run --Werror --lines=1:1 src/x.c"
        " && clang-format-18 --dry-run --Werror --lines=2:2 --lines=11:12 --lines=22:22 src/y.h"
    ) in run_test.ran

    misformatted = _runner({"clang-format-18": TestRun(passed=False, output="x.c:1:1: error: code should be clang-formatted")})
    failed, _checks = _gate(_evidence(), candidate, repo, misformatted)
    assert failed.startswith("`clang-format-18` wants the lines you changed formatted differently")


@pytest.mark.skipif(shutil.which("clang-format-18") is None and shutil.which("clang-format") is None,
                    reason="needs clang-format")
def test_a_violation_the_fix_did_not_cause_passes_and_one_in_its_lines_fails(repo, monkeypatch):
    """An older working commit can carry lines the current rules would format differently (server.h did)."""
    from scripts.ci_fix import engine as engine_mod

    monkeypatch.setattr(checks_mod, "CLANG_FORMAT", shutil.which("clang-format-18") and "clang-format-18" or "clang-format")
    monkeypatch.setattr(engine_mod, "isolated_user", lambda: "")
    (repo / ".clang-format").write_text("BasedOnStyle: LLVM\n", encoding="utf-8")
    (repo / "src" / "x.c").write_text("int  old = 1;\nint added = 2;\n", encoding="utf-8")
    # A patch that adds line 2; line 1's two spaces were there before.
    patch = "diff --git a/src/x.c b/src/x.c\n--- a/src/x.c\n+++ b/src/x.c\n@@ -1,1 +1,2 @@\n int  old = 1;\n+int added = 2;\n"
    checks: list[str] = []

    deadline = time.monotonic() + 600
    assert checks_mod._formatting(_evidence(), _candidate(patch), str(repo), engine_mod.run_test_command, deadline, checks) == ""
    assert any("accepts the formatting of the changed lines" in check for check in checks)

    (repo / "src" / "x.c").write_text("int  old = 1;\nint   added = 2;\n", encoding="utf-8")
    failed = checks_mod._formatting(_evidence(), _candidate(patch), str(repo), engine_mod.run_test_command, deadline, [])
    assert failed.startswith("`clang-format") and "x.c:2:" in failed


def test_a_changed_command_definition_must_regenerate_commands_def_unchanged(repo):
    generated = repo / "src" / "commands.def"
    generated.write_text("before\n", encoding="utf-8")
    candidate = _candidate(_diff("src/commands/xackdel.json"))

    def regenerates(repo_dir: str, command: str, timeout: int) -> TestRun:
        if "make commands.def" in command:
            generated.write_text("after\n", encoding="utf-8")
        return TestRun(passed=True, output="", report="[ok]: value check (1 ms)\n[ok]: Alpha.One (1 ms)")

    failed, _checks = _gate(_evidence(), candidate, repo, regenerates)

    assert "is not what `make commands.def` generates" in failed
    assert generated.read_text(encoding="utf-8") == "before\n"  # the tree is the candidate

    failed, checks = _gate(_evidence(), candidate, repo, _runner())
    assert failed == "" and any("regenerates `src/commands.def` unchanged" in check for check in checks)


def test_the_test_runs_once_under_address_sanitizer_last(repo):
    run_test = _runner()
    failed, checks = _gate(_evidence(), _candidate(_diff("src/x.c")), repo, run_test)

    assert failed == ""
    assert run_test.ran[-1] == "make SANITIZER=address && ./runtest --single unit/value loops=1"
    assert checks[-1].startswith("With the fix built under AddressSanitizer, the test passes")

    leaking = _runner({"SANITIZER": TestRun(passed=False, output="ERROR: LeakSanitizer", report="Sanitizer error")})
    failed, _checks = _gate(_evidence(), _candidate(_diff("src/x.c")), repo, leaking)
    assert failed.startswith("With your change built under AddressSanitizer, the test does not pass")

    unit = _evidence(paths=("src/unit/test_x.cpp",), tests=(FailedTest("Alpha.One", "src/unit/test_x.cpp"),))
    (repo / "src" / "unit").mkdir()
    (repo / "src" / "unit" / "test_x.cpp").write_text("TEST(Alpha, One) {}\n", encoding="utf-8")
    run_test = _runner()
    _gate(unit, _candidate(_diff("src/x.c")), repo, run_test)
    assert run_test.ran[-1].startswith("make all-with-unit-tests SANITIZER=address && ")


def test_under_the_sanitizer_a_file_whose_code_outside_the_test_breaks_with_only_runs_whole(repo, monkeypatch):
    """Code between tests that waits on a skipped test fails after the test passed: the plan's fallbacks run."""
    whole = VerifyPlan(
        build="make SANITIZER=address", test=f"./runtest --single unit/value --loops {LOOPS}", tests=(_TEST,),
        single_run=True, attributed=True,
    )
    monkeypatch.setattr(
        checks_mod, "plan_for",
        lambda repo_dir, tests, *, scheduled, job=None: VerifyPlan(
            build="make SANITIZER=address", test=f"./runtest --single unit/value --only 'value check' --loops {LOOPS}",
            tests=tests, fallback=whole,
        ) if tests[0].name else VerifyPlan(build="true", test=f"./runtest --single unit/value loops={LOOPS}", tests=tests),
    )
    broken_outside = TestRun(
        passed=False, output="[ok]: value check (1 ms)\n[exception]: Executing test client: I/O error reading reply",
        report="[ok]: value check (1 ms)",
    )
    run_test = _runner({"--only": broken_outside})

    failed, checks = _gate(_evidence(), _candidate(_diff("src/x.c")), repo, run_test)

    assert failed == ""
    assert run_test.ran[-2:] == [
        "make SANITIZER=address && ./runtest --single unit/value --only 'value check' --loops 1",
        "make SANITIZER=address && ./runtest --single unit/value --loops 1",
    ]
    assert checks[-1] == (
        "With the fix built under AddressSanitizer, the test passes: "
        "`make SANITIZER=address && ./runtest --single unit/value --loops 1`."
    )

    # The test itself failing is the fix's failure: nothing wider runs.
    own_failure = TestRun(passed=False, output="[err]: value check in tests/unit/value.tcl", report="[err]: value check in tests/unit/value.tcl")
    run_test = _runner({"--only": own_failure})
    failed, _checks = _gate(_evidence(), _candidate(_diff("src/x.c")), repo, run_test)
    assert failed.startswith("With your change built under AddressSanitizer, the test does not pass")
    assert "--only 'value check'" in failed and sum("SANITIZER" in command for command in run_test.ran) == 1


def test_a_check_that_runs_out_of_time_is_a_failed_attempt(repo):
    timed_out = _runner({"--single": TestRun(passed=False, output="timed out", timed_out=True)})
    with pytest.raises(RuntimeError, match="did not finish in the time left"):
        _gate(_evidence(), _candidate(_diff("src/x.c")), repo, timed_out)
