"""Real-Git tests for generated-file regeneration, convergence, and rollback."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.backport import sweep_validation
from scripts.backport.missing_test_adaptation import MissingTestAdaptationResult
from scripts.backport.models import BackportCandidate, ResolutionResult
from scripts.backport.registry import GeneratedFileRule
from scripts.backport.sweep_validation import (
    ValidationOutcome,
    adapt_added_tests_for_target,
    prepare_generated_files,
    repair_validation_failure_with_claude,
    validate_branch_with_optional_repair,
)
from scripts.backport.validation import UNMAPPED_TEST_PATHS_PREFIX


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_repo(repo: Path) -> None:
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "src/unit").mkdir(parents=True)
    (repo / "src/unit/test_example.c").write_text("int test_new(void) {}\n", encoding="utf-8")
    (repo / "src/unit/test_files.h").write_text("stale\n", encoding="utf-8")
    (repo / "generate.py").write_text(
        "from pathlib import Path\n"
        "Path('src/unit/test_files.h').write_text('generated\\n')\n",
        encoding="utf-8",
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "candidate")


def test_generated_output_is_amended_into_candidate_and_converges(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    starting_count = _git(tmp_path, "rev-list", "--count", "HEAD")

    outcome = prepare_generated_files(
        str(tmp_path),
        ("src/unit/test_example.c",),
        [
            GeneratedFileRule(
                paths=("src/unit/*.c",),
                command="python3 generate.py",
                outputs=("src/unit/test_files.h",),
            )
        ],
    )

    assert outcome.ok is True
    assert outcome.generated_paths == ("src/unit/test_files.h",)
    assert outcome.amended_commit_sha == _git(tmp_path, "rev-parse", "HEAD")
    assert _git(tmp_path, "rev-list", "--count", "HEAD") == starting_count
    assert (tmp_path / "src/unit/test_files.h").read_text() == "generated\n"
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_generated_command_fails_closed_on_unexpected_path(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    (tmp_path / "unexpected.txt").write_text("before\n", encoding="utf-8")
    _git(tmp_path, "add", "unexpected.txt")
    _git(tmp_path, "commit", "-q", "-m", "track unexpected")
    (tmp_path / "generate.py").write_text(
        "from pathlib import Path\n"
        "Path('src/unit/test_files.h').write_text('generated\\n')\n"
        "Path('unexpected.txt').write_text('changed\\n')\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", "generate.py")
    _git(tmp_path, "commit", "-q", "-m", "bad generator")

    outcome = prepare_generated_files(
        str(tmp_path),
        ("src/unit/test_example.c",),
        [
            GeneratedFileRule(
                paths=("src/unit/*.c",),
                command="python3 generate.py",
                outputs=("src/unit/test_files.h",),
            )
        ],
    )

    assert outcome.ok is False
    assert "unexpected path" in outcome.output
    assert (tmp_path / "src/unit/test_files.h").read_text() == "stale\n"
    assert (tmp_path / "unexpected.txt").read_text() == "before\n"
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_generated_rule_rejects_partially_untracked_outputs(tmp_path: Path) -> None:
    """One tracked output and one missing one is a misconfiguration, not a skip.

    The generator demonstrably belongs on this branch, so a typo'd or renamed
    second output would silently shrink the allowlist the generator is held to.
    """
    _init_repo(tmp_path)

    outcome = prepare_generated_files(
        str(tmp_path),
        ("src/unit/test_example.c",),
        [
            GeneratedFileRule(
                paths=("src/unit/*.c",),
                command="python3 generate.py",
                outputs=("src/unit/test_files.h", "src/unit/not-on-target.h"),
            )
        ],
    )

    assert outcome.ok is False
    assert "not tracked on the target branch" in outcome.output
    assert "src/unit/not-on-target.h" in outcome.output
    assert (tmp_path / "src/unit/test_files.h").read_text() == "stale\n"
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_generated_rule_with_no_tracked_outputs_is_skipped(tmp_path: Path) -> None:
    """A generator that does not exist on this branch must not fail the candidate.

    Registry rules are shared across every release line, and Valkey's unit-test
    header generator is absent from 7.2 and 9.1, so a rule with nothing tracked
    is inapplicable rather than broken.
    """
    _init_repo(tmp_path)

    outcome = prepare_generated_files(
        str(tmp_path),
        ("src/unit/test_example.c",),
        [
            GeneratedFileRule(
                paths=("src/unit/*.c",),
                command="python3 generate.py",
                outputs=("src/unit/not-on-target.h",),
            )
        ],
    )

    assert outcome.ok is True
    assert outcome.generated_paths == ()
    assert outcome.amended_commit_sha == ""
    assert (tmp_path / "src/unit/test_files.h").read_text() == "stale\n"
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_successful_repair_regenerates_and_revalidates_final_tree(monkeypatch) -> None:
    generated_calls: list[tuple[str, ...]] = []
    validation_calls: list[str] = []

    def fake_prepare(_repo_dir, changed_paths, _rules, **_kwargs):
        generated_calls.append(changed_paths)
        if len(generated_calls) == 1:
            return ValidationOutcome(True, "")
        return ValidationOutcome(
            True,
            "",
            generated_paths=("src/unit/test_files.h",),
            amended_commit_sha="c" * 40,
        )

    def fake_validate(*_args, **_kwargs):
        validation_calls.append("validate")
        if len(validation_calls) == 1:
            return False, "initial failure"
        return True, "final tree passed"

    monkeypatch.setattr(sweep_validation, "prepare_generated_files", fake_prepare)
    monkeypatch.setattr(sweep_validation, "validate_backport_branch", fake_validate)
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: ValidationOutcome(
            True,
            "repaired tree passed",
            ai_summary="updated generator input",
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: ("src/unit/test_example.c", "generate.py"),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "b" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "9.0",
        ["make"],
        [],
        repair=True,
        generated_file_rules=[object()],
    )

    assert outcome.ok is True
    assert outcome.output == "final tree passed"
    assert outcome.generated_paths == ("src/unit/test_files.h",)
    assert outcome.amended_commit_sha == "c" * 40
    assert generated_calls == [
        ("src/unit/test_example.c", "generate.py"),
        ("src/unit/test_example.c", "generate.py"),
    ]
    assert len(validation_calls) == 2


def test_failed_post_repair_generation_rolls_back_exact_repair_base(monkeypatch) -> None:
    generated_calls = 0
    git_calls: list[tuple[str, ...]] = []

    def fake_prepare(*_args, **_kwargs):
        nonlocal generated_calls
        generated_calls += 1
        if generated_calls == 1:
            return ValidationOutcome(True, "")
        return ValidationOutcome(False, "generated-file command did not converge")

    monkeypatch.setattr(sweep_validation, "prepare_generated_files", fake_prepare)
    monkeypatch.setattr(
        sweep_validation,
        "validate_backport_branch",
        lambda *_args, **_kwargs: (False, "initial failure"),
    )
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: ValidationOutcome(True, "repaired tree passed"),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: ("generate.py",),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "b" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "9.0",
        ["make"],
        [],
        repair=True,
        generated_file_rules=[object()],
        run_git=lambda _repo_dir, *args, **_kwargs: git_calls.append(args),
    )

    assert outcome.ok is False
    assert "did not converge" in outcome.output
    assert git_calls == [("reset", "--hard", "b" * 40)]


def test_optional_repair_routes_unmapped_added_test_to_native_adaptation(
    monkeypatch,
) -> None:
    candidate = BackportCandidate(
        source_pr_number=42,
        source_pr_title="Fix networking regression",
        source_pr_url="https://example.test/pull/42",
        target_branch="8.0",
    )
    initial_output = (
        f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]\n'
        "target branch does not build this test suffix"
    )
    adaptation_calls: list[tuple[str, BackportCandidate | None]] = []
    repair_calls: list[str] = []

    monkeypatch.setattr(
        sweep_validation,
        "prepare_generated_files",
        lambda *_args, **_kwargs: ValidationOutcome(True, ""),
    )
    monkeypatch.setattr(
        sweep_validation,
        "validate_backport_branch",
        lambda *_args, **_kwargs: (False, initial_output),
    )
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: (
            repair_calls.append("repair")
            or ValidationOutcome(False, "repair refused")
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "adapt_added_tests_for_target",
        lambda _repo, _branch, _commands, _rules, output, **kwargs: (
            adaptation_calls.append((output, kwargs["candidate"]))
            or ValidationOutcome(
                True,
                "branch-native test passed",
                amended_commit_sha="c" * 40,
            )
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: ("src/unit/test_networking.cpp",),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "b" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "8.0",
        ["make"],
        [],
        repair=True,
        candidate=candidate,
    )

    assert outcome.ok is True
    assert outcome.amended_commit_sha == "c" * 40
    assert len(adaptation_calls) == 1
    assert UNMAPPED_TEST_PATHS_PREFIX in adaptation_calls[0][0]
    assert adaptation_calls[0][1] is candidate
    assert repair_calls == []


def test_partial_source_repair_combines_with_native_test_adaptation(
    monkeypatch,
) -> None:
    candidate = BackportCandidate(
        source_pr_number=42,
        source_pr_title="Fix networking regression",
        source_pr_url="https://example.test/pull/42",
        target_branch="8.0",
    )
    source_resolution = ResolutionResult(
        path="src/networking.c",
        resolved_content="fixed source\n",
        resolution_summary="adapted source API",
    )
    test_resolution = ResolutionResult(
        path="tests/unit/networking.tcl",
        resolved_content="test regression {}\n",
        resolution_summary="ported test",
    )
    marker = f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]'

    monkeypatch.setattr(
        sweep_validation,
        "prepare_generated_files",
        lambda *_args, **_kwargs: ValidationOutcome(True, ""),
    )
    monkeypatch.setattr(
        sweep_validation,
        "validate_backport_branch",
        lambda *_args, **_kwargs: (False, "source compile failed"),
    )
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: ValidationOutcome(
            False,
            f"{marker}\nsource repair exposed unsupported test",
            resolutions=(source_resolution,),
            ai_summary="adapted source API",
            amended_commit_sha="b" * 40,
            partial_repair=True,
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "adapt_added_tests_for_target",
        lambda *_args, **_kwargs: ValidationOutcome(
            True,
            "combined candidate passed",
            resolutions=(test_resolution,),
            ai_summary="ported test",
            amended_commit_sha="c" * 40,
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: (
            "src/networking.c",
            "src/unit/test_networking.cpp",
        ),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "a" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "8.0",
        ["make"],
        [],
        repair=True,
        candidate=candidate,
    )

    assert outcome.ok is True
    assert outcome.output == "combined candidate passed"
    assert outcome.resolutions == (source_resolution, test_resolution)
    assert outcome.ai_summary == "adapted source API; ported test"
    assert outcome.amended_commit_sha == "c" * 40


def test_partial_source_repair_carries_unmapped_paths_outside_truncated_output(
    monkeypatch,
) -> None:
    path = "src/unit/test_networking.cpp"
    adaptation_outputs: list[str] = []

    monkeypatch.setattr(
        sweep_validation,
        "prepare_generated_files",
        lambda *_args, **_kwargs: ValidationOutcome(True, ""),
    )
    monkeypatch.setattr(
        sweep_validation,
        "validate_backport_branch",
        lambda *_args, **_kwargs: (False, "source compile failed"),
    )
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: ValidationOutcome(
            False,
            "tail without the machine marker",
            partial_repair=True,
            unmapped_test_paths=(path,),
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "adapt_added_tests_for_target",
        lambda _repo, _branch, _commands, _rules, output, **_kwargs: (
            adaptation_outputs.append(output)
            or ValidationOutcome(
                True,
                "combined candidate passed",
                amended_commit_sha="c" * 40,
            )
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: ("src/networking.c", path),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "a" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "8.0",
        ["make"],
        [],
        repair=True,
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="8.0",
        ),
    )

    assert outcome.ok is True
    assert len(adaptation_outputs) == 1
    assert f'{UNMAPPED_TEST_PATHS_PREFIX}["{path}"]' in adaptation_outputs[0]


def test_failed_test_adaptation_rolls_back_retained_source_repair(
    monkeypatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []
    marker = f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]'

    monkeypatch.setattr(
        sweep_validation,
        "prepare_generated_files",
        lambda *_args, **_kwargs: ValidationOutcome(True, ""),
    )
    monkeypatch.setattr(
        sweep_validation,
        "validate_backport_branch",
        lambda *_args, **_kwargs: (False, "source compile failed"),
    )
    monkeypatch.setattr(
        sweep_validation,
        "repair_validation_failure_with_claude",
        lambda *_args, **_kwargs: ValidationOutcome(
            False,
            marker,
            partial_repair=True,
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "adapt_added_tests_for_target",
        lambda *_args, **_kwargs: ValidationOutcome(
            False,
            "no safe branch-native adaptation",
        ),
    )
    monkeypatch.setattr(
        sweep_validation,
        "changed_paths_since_base",
        lambda *_args: ("src/unit/test_networking.cpp",),
    )
    monkeypatch.setattr(sweep_validation, "head_sha", lambda *_args: "a" * 40)

    outcome = validate_branch_with_optional_repair(
        "/repo",
        "8.0",
        ["make"],
        [],
        repair=True,
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="8.0",
        ),
        run_git=lambda _repo, *args, **_kwargs: git_calls.append(args),
    )

    assert outcome.ok is False
    assert outcome.output == "no safe branch-native adaptation"
    assert git_calls == [("reset", "--hard", "a" * 40)]


def test_cleanly_added_test_is_ported_to_branch_native_test(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    native = tmp_path / "tests/unit/networking.tcl"
    native.parent.mkdir(parents=True)
    native.write_text("test existing {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "target branch")
    base_sha = _git(tmp_path, "rev-parse", "HEAD")

    added = tmp_path / "src/unit/test_networking.cpp"
    added.parent.mkdir(parents=True)
    added.write_text("TEST(Networking, Regression) {}\n", encoding="utf-8")
    companion = tmp_path / "src/unit/custom_matchers.hpp"
    companion.write_text("// shared C++ harness helper\n", encoding="utf-8")
    native_added = tmp_path / "src/unit/test_quicklist.c"
    native_added.write_text("void test_quicklist(void) {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "clean upstream test")

    def adapt(repo_dir, _candidate, sources, **kwargs):
        assert sources == {
            "src/unit/custom_matchers.hpp": "// shared C++ harness helper\n",
            "src/unit/test_networking.cpp": "TEST(Networking, Regression) {}\n",
        }
        assert kwargs["excluded_test_paths"] == (
            "src/unit/test_networking.cpp",
            "src/unit/custom_matchers.hpp",
        )
        Path(repo_dir, "tests/unit/networking.tcl").write_text(
            "test existing {}\ntest regression {}\n",
            encoding="utf-8",
        )
        _git(Path(repo_dir), "add", "tests/unit/networking.tcl")
        return MissingTestAdaptationResult(
            adapted_paths=["tests/unit/networking.tcl"],
            summary=(
                "ported target-missing test coverage to: "
                "tests/unit/networking.tcl"
            ),
        )

    def validate(repo_dir, *_args, **_kwargs):
        assert not Path(repo_dir, "src/unit/test_networking.cpp").exists()
        assert not Path(repo_dir, "src/unit/custom_matchers.hpp").exists()
        assert Path(repo_dir, "src/unit/test_quicklist.c").is_file()
        assert "test regression" in Path(
            repo_dir,
            "tests/unit/networking.tcl",
        ).read_text(encoding="utf-8")
        return True, "branch-native test passed"

    outcome = adapt_added_tests_for_target(
        str(tmp_path),
        "9.0",
        ["make"],
        [],
        f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]\n'
        "new test harness is unavailable",
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="9.0",
        ),
        language="c",
        test_path_patterns=None,
        base_ref=base_sha,
        adapt_missing_tests_func=adapt,
        validate_func=validate,
    )

    assert outcome.ok is True
    assert outcome.output == "branch-native test passed"
    assert outcome.amended_commit_sha == _git(tmp_path, "rev-parse", "HEAD")
    assert not added.exists()
    assert not companion.exists()
    assert native_added.exists()
    assert _git(tmp_path, "log", "-1", "--format=%s") == (
        "Adapt tests for target branch"
    )
    assert "Signed-off-by: Test <test@example.com>" in _git(
        tmp_path,
        "log",
        "-1",
        "--format=%B",
    )


def test_added_unsafe_test_harness_companion_fails_closed(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    native = tmp_path / "tests/unit/networking.tcl"
    native.parent.mkdir(parents=True)
    native.write_text("test existing {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "target branch")
    base_sha = _git(tmp_path, "rev-parse", "HEAD")

    unit_dir = tmp_path / "src/unit"
    unit_dir.mkdir(parents=True)
    added = unit_dir / "test_networking.cpp"
    added.write_text("TEST(Networking, Regression) {}\n", encoding="utf-8")
    companion = unit_dir / "custom_matchers.hpp"
    companion.symlink_to("/tmp/external-test-helper")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "upstream C++ test")
    candidate_head = _git(tmp_path, "rev-parse", "HEAD")

    outcome = adapt_added_tests_for_target(
        str(tmp_path),
        "9.0",
        ["make"],
        [],
        f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]\n'
        "new test harness is unavailable",
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="9.0",
        ),
        language="c",
        test_path_patterns=None,
        base_ref=base_sha,
        adapt_missing_tests_func=lambda *_args, **_kwargs: (
            pytest.fail("unsafe companion must fail before adaptation")
        ),
    )

    assert outcome.ok is False
    assert "cannot safely inspect" in outcome.output
    assert "src/unit/custom_matchers.hpp" in outcome.output
    assert _git(tmp_path, "rev-parse", "HEAD") == candidate_head
    assert added.exists()
    assert companion.is_symlink()


def test_replaced_test_is_restored_and_adapted_in_branch_native_format(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    native = tmp_path / "src/unit/test_networking.c"
    native.parent.mkdir(parents=True)
    native.write_text("void test_existing(void) {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "target branch")
    base_sha = _git(tmp_path, "rev-parse", "HEAD")

    native.unlink()
    added = tmp_path / "src/unit/test_networking.cpp"
    added.write_text("TEST(Networking, Regression) {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "replace C test with C++ test")

    def adapt(repo_dir, _candidate, sources, **_kwargs):
        restored = Path(repo_dir, "src/unit/test_networking.c")
        assert restored.read_text(encoding="utf-8") == (
            "void test_existing(void) {}\n"
        )
        assert sources == {
            "src/unit/test_networking.cpp": "TEST(Networking, Regression) {}\n"
        }
        restored.write_text(
            "void test_existing(void) {}\n"
            "void test_regression(void) {}\n",
            encoding="utf-8",
        )
        _git(Path(repo_dir), "add", "src/unit/test_networking.c")
        return MissingTestAdaptationResult(
            adapted_paths=["src/unit/test_networking.c"],
            summary=(
                "ported target-missing test coverage to: "
                "src/unit/test_networking.c"
            ),
        )

    outcome = adapt_added_tests_for_target(
        str(tmp_path),
        "9.0",
        ["make"],
        [],
        f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]\n'
        "new test harness is unavailable",
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="9.0",
        ),
        language="c",
        test_path_patterns=None,
        base_ref=base_sha,
        adapt_missing_tests_func=adapt,
        validate_func=lambda *_args, **_kwargs: (
            True,
            "branch-native test passed",
        ),
    )

    assert outcome.ok is True
    assert not added.exists()
    assert "test_regression" in native.read_text(encoding="utf-8")
    assert _git(tmp_path, "diff", "--name-status", f"{base_sha}...HEAD") == (
        "M\tsrc/unit/test_networking.c"
    )


def test_cleanly_added_test_without_native_adaptation_fails_closed(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("target\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "target branch")
    base_sha = _git(tmp_path, "rev-parse", "HEAD")
    added = tmp_path / "src/unit/test_networking.cpp"
    added.parent.mkdir(parents=True)
    added.write_text("TEST(Networking, Regression) {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "clean upstream test")
    candidate_head = _git(tmp_path, "rev-parse", "HEAD")

    outcome = adapt_added_tests_for_target(
        str(tmp_path),
        "9.0",
        ["make"],
        [],
        f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]\n'
        "new test harness is unavailable",
        candidate=BackportCandidate(
            source_pr_number=42,
            source_pr_title="Fix networking regression",
            source_pr_url="https://example.test/pull/42",
            target_branch="9.0",
        ),
        language="c",
        test_path_patterns=None,
        base_ref=base_sha,
        adapt_missing_tests_func=lambda *_args, **_kwargs: MissingTestAdaptationResult(
            summary="test adaptation not applied: no branch-native test changes",
        ),
    )

    assert outcome.ok is False
    assert "no branch-native test changes" in outcome.output
    assert _git(tmp_path, "rev-parse", "HEAD") == candidate_head
    assert added.exists()


def test_validation_repair_cannot_delete_changed_test_coverage(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    test_path = tmp_path / "tests/unit/networking.tcl"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("test regression {}\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "candidate")

    def remove_test(_profile, _prompt, *, cwd):
        Path(cwd, "tests/unit/networking.tcl").unlink()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    outcome = repair_validation_failure_with_claude(
        str(tmp_path),
        "8.0",
        ["make"],
        [],
        "test failed",
        run_agent_func=remove_test,
        changed_paths_func=lambda *_args: ("tests/unit/networking.tcl",),
        changed_paths_since_base_func=lambda *_args: (
            "tests/unit/networking.tcl",
        ),
    )

    assert outcome.ok is False
    assert "removed changed test coverage" in outcome.output
    assert test_path.read_text(encoding="utf-8") == "test regression {}\n"
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_validation_repair_carries_unmapped_paths_without_retained_edits(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    unsupported_test = tmp_path / "src/unit/test_networking.cpp"
    unsupported_test.parent.mkdir(parents=True)
    unsupported_test.write_text(
        "TEST(Networking, OriginalIntent) {}\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "candidate")
    candidate_head = _git(tmp_path, "rev-parse", "HEAD")

    def rewrite_test(_profile, _prompt, *, cwd):
        Path(cwd, "src/unit/test_networking.cpp").write_text(
            "TEST(Networking, GenericRepairRewrite) {}\n",
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    marker = f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]'
    validation_log = tmp_path.with_suffix(".validation.log")

    def validate_after_repair(*_args, **kwargs):
        Path(kwargs["log_path"]).write_text(marker, encoding="utf-8")
        return False, "tail without the machine marker"

    outcome = repair_validation_failure_with_claude(
        str(tmp_path),
        "8.0",
        ["make"],
        [],
        "source compile failed",
        run_agent_func=rewrite_test,
        validate_func=validate_after_repair,
        validation_log_path=str(validation_log),
        changed_paths_func=lambda *_args: ("src/unit/test_networking.cpp",),
        changed_paths_since_base_func=lambda *_args: (
            "src/unit/test_networking.cpp",
        ),
    )

    assert outcome.ok is False
    assert outcome.partial_repair is False
    assert outcome.unmapped_test_paths == ("src/unit/test_networking.cpp",)
    assert _git(tmp_path, "rev-parse", "HEAD") == candidate_head
    assert unsupported_test.read_text(encoding="utf-8") == (
        "TEST(Networking, OriginalIntent) {}\n"
    )
    assert _git(tmp_path, "status", "--porcelain") == ""


def test_validation_repair_retains_commit_when_only_unmapped_test_remains(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    source = tmp_path / "src/networking.c"
    source.parent.mkdir(parents=True)
    source.write_text("old API\n", encoding="utf-8")
    unsupported_test = tmp_path / "src/unit/test_networking.cpp"
    unsupported_test.parent.mkdir()
    unsupported_test.write_text(
        "TEST(Networking, OriginalIntent) {}\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "candidate")
    candidate_head = _git(tmp_path, "rev-parse", "HEAD")

    def repair_source(_profile, _prompt, *, cwd):
        Path(cwd, "src/networking.c").write_text(
            "target branch API\n",
            encoding="utf-8",
        )
        Path(cwd, "src/unit/test_networking.cpp").write_text(
            "TEST(Networking, GenericRepairRewrite) {}\n",
            encoding="utf-8",
        )
        return SimpleNamespace(
            returncode=0,
            stdout='{"type":"result","result":"adapted source API"}\n',
            stderr="",
        )

    marker = f'{UNMAPPED_TEST_PATHS_PREFIX}["src/unit/test_networking.cpp"]'
    validation_log = tmp_path.with_suffix(".validation.log")

    def validate_after_repair(*_args, **kwargs):
        Path(kwargs["log_path"]).write_text(
            f"{marker}\nunsupported test remains",
            encoding="utf-8",
        )
        return False, "tail without the machine marker"

    outcome = repair_validation_failure_with_claude(
        str(tmp_path),
        "8.0",
        ["make"],
        [],
        "source compile failed",
        run_agent_func=repair_source,
        validate_func=validate_after_repair,
        validation_log_path=str(validation_log),
        changed_paths_func=lambda *_args: (
            "src/networking.c",
            "src/unit/test_networking.cpp",
        ),
        changed_paths_since_base_func=lambda *_args: (
            "src/networking.c",
            "src/unit/test_networking.cpp",
        ),
    )

    assert outcome.ok is False
    assert outcome.partial_repair is True
    assert outcome.unmapped_test_paths == ("src/unit/test_networking.cpp",)
    assert outcome.ai_summary == "adapted source API"
    assert outcome.amended_commit_sha == _git(tmp_path, "rev-parse", "HEAD")
    assert outcome.amended_commit_sha != candidate_head
    assert source.read_text(encoding="utf-8") == "target branch API\n"
    assert unsupported_test.read_text(encoding="utf-8") == (
        "TEST(Networking, OriginalIntent) {}\n"
    )
    assert [resolution.path for resolution in outcome.resolutions] == [
        "src/networking.c"
    ]
    assert "Signed-off-by: Test <test@example.com>" in _git(
        tmp_path,
        "log",
        "-1",
        "--format=%B",
    )
