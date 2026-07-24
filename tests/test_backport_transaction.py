"""Real-Git tests for transactional backport validation."""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.backport.models import BackportCandidate, CandidateResult
from scripts.backport.transaction import (
    apply_candidate_transaction,
    validate_baseline,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_repo(path: Path) -> None:
    path.mkdir()
    _git(path, "init", "-q", "-b", "release")
    _git(path, "config", "user.name", "Test")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "commit.gpgsign", "false")
    (path / ".gitignore").write_text("build/\n", encoding="utf-8")
    (path / "tracked.txt").write_text("base\n", encoding="utf-8")
    (path / "prior.txt").write_text("prior\n", encoding="utf-8")
    _git(path, "add", ".gitignore", "tracked.txt", "prior.txt")
    _git(path, "commit", "-q", "-m", "base")


def _candidate() -> BackportCandidate:
    return BackportCandidate(
        source_pr_number=42,
        source_pr_title="Transactional candidate",
        source_pr_url="https://github.com/example/repo/pull/42",
        target_branch="release",
        merge_commit_sha="source",
    )


def _apply_tracked_change(
    workspace: str,
    candidate: BackportCandidate,
    *_args,
    **_kwargs,
) -> CandidateResult:
    repo = Path(workspace)
    (repo / "tracked.txt").write_text("candidate\n", encoding="utf-8")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-q", "-m", "candidate")
    return CandidateResult(
        candidate.source_pr_number,
        candidate.source_pr_title,
        "applied",
        applied_commits=["source"],
    )


def test_failed_candidate_discards_all_workspace_artifacts(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    starting_head = _git(repo, "rev-parse", "HEAD")
    (repo / "build").mkdir()
    (repo / "build" / "baseline.bin").write_bytes(b"baseline")
    (repo / "scratch.txt").write_text("keep\n", encoding="utf-8")

    def validate(workspace, *_args, **kwargs):
        assert kwargs["repair_paths"] == ("tracked.txt",)
        path = Path(workspace)
        (path / "tracked.txt").write_text("validation mutation\n", encoding="utf-8")
        (path / "build").mkdir(exist_ok=True)
        (path / "build" / "candidate.bin").write_bytes(b"candidate")
        (path / "leaked.txt").write_text("candidate\n", encoding="utf-8")
        return False, "compiler error"

    result = apply_candidate_transaction(
        str(repo),
        _candidate(),
        "example/repo",
        {},
        target_branch="release",
        setup_commands=[],
        test_commands=["make"],
        validation_rules=[],
        repair_validation_failures=True,
        run_commands=lambda *_args, **_kwargs: (True, ""),
        apply_func=_apply_tracked_change,
        validate_func=validate,
    )

    assert result.outcome == "skipped-validation-failed"
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "base\n"
    assert (repo / "build" / "baseline.bin").read_bytes() == b"baseline"
    assert (repo / "scratch.txt").read_text(encoding="utf-8") == "keep\n"
    assert not (repo / "build" / "candidate.bin").exists()
    assert not (repo / "leaked.txt").exists()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_green_candidate_is_promoted_without_validation_artifacts(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    def validate(workspace, *_args, **_kwargs):
        path = Path(workspace)
        (path / "build").mkdir()
        (path / "build" / "candidate.bin").write_bytes(b"candidate")
        (path / "temporary.txt").write_text("temporary\n", encoding="utf-8")
        return True, ""

    result = apply_candidate_transaction(
        str(repo),
        _candidate(),
        "example/repo",
        {},
        target_branch="release",
        setup_commands=[],
        test_commands=["make"],
        validation_rules=[],
        repair_validation_failures=False,
        run_commands=lambda *_args, **_kwargs: (True, ""),
        apply_func=_apply_tracked_change,
        validate_func=validate,
    )

    assert result.outcome == "applied"
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "candidate\n"
    assert not (repo / "build").exists()
    assert not (repo / "temporary.txt").exists()
    assert _git(repo, "log", "-1", "--format=%s") == "candidate"


def test_validation_repair_is_recorded_and_promoted(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    def validate(workspace, *_args, **kwargs):
        assert kwargs["repair_paths"] == ("tracked.txt",)
        path = Path(workspace)
        (path / "tracked.txt").write_text("candidate repaired\n", encoding="utf-8")
        _git(path, "add", "tracked.txt")
        _git(path, "commit", "-q", "-m", "repair")
        return True, ""

    result = apply_candidate_transaction(
        str(repo),
        _candidate(),
        "example/repo",
        {},
        target_branch="release",
        setup_commands=[],
        test_commands=["make"],
        validation_rules=[],
        repair_validation_failures=True,
        run_commands=lambda *_args, **_kwargs: (True, ""),
        apply_func=_apply_tracked_change,
        validate_func=validate,
    )

    assert result.outcome == "applied"
    assert result.validation_repaired is True
    assert result.validation_repair_commit_sha == _git(repo, "rev-parse", "HEAD")
    assert result.validation_repair_paths == ["tracked.txt"]
    assert result.resolved_by_ai is True
    assert "validation repaired by Claude Code: tracked.txt" in result.detail
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == (
        "candidate repaired\n"
    )


def test_repair_outside_candidate_paths_is_not_promoted(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    starting_head = _git(repo, "rev-parse", "HEAD")

    def validate(workspace, *_args, **_kwargs):
        path = Path(workspace)
        (path / "prior.txt").write_text("silently changed\n", encoding="utf-8")
        _git(path, "add", "prior.txt")
        _git(path, "commit", "-q", "-m", "unsafe repair")
        return True, ""

    result = apply_candidate_transaction(
        str(repo),
        _candidate(),
        "example/repo",
        {},
        target_branch="release",
        setup_commands=[],
        test_commands=["make"],
        validation_rules=[],
        repair_validation_failures=True,
        run_commands=lambda *_args, **_kwargs: (True, ""),
        apply_func=_apply_tracked_change,
        validate_func=validate,
    )

    assert result.outcome == "skipped-validation-failed"
    assert "outside the current candidate" in result.detail
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert (repo / "prior.txt").read_text(encoding="utf-8") == "prior\n"


def test_baseline_failure_is_isolated_from_canonical_checkout(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    def validate(workspace, *_args, **_kwargs):
        path = Path(workspace)
        (path / "build").mkdir()
        (path / "build" / "baseline.bin").write_bytes(b"failed")
        return False, "target branch is already red"

    result = validate_baseline(
        str(repo),
        "release",
        [],
        ["make"],
        [],
        run_commands=lambda *_args, **_kwargs: (True, ""),
        validate_func=validate,
    )

    assert result.ok is False
    assert result.phase == "validation"
    assert result.output == "target branch is already red"
    assert not (repo / "build").exists()


def test_baseline_rejects_setup_that_mutates_tracked_files(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    def setup(workspace, _commands):
        path = Path(workspace)
        (path / "tracked.txt").write_text("generated\n", encoding="utf-8")
        return True, ""

    result = validate_baseline(
        str(repo),
        "release",
        ["./configure"],
        ["make"],
        [],
        run_commands=setup,
        validate_func=lambda *_args, **_kwargs: (True, ""),
    )

    assert result.ok is False
    assert result.phase == "cleanliness"
    assert result.output == "validation setup modified tracked file(s): tracked.txt"
    assert (repo / "tracked.txt").read_text(encoding="utf-8") == "base\n"
