"""Real-Git tests for the shared manual/sweep application engine."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts.backport import application
from scripts.backport.application import apply_candidate
from scripts.backport.models import (
    BackportCandidate,
    ResolutionResult,
)
from scripts.backport.source_change import SourceChangePlan
from scripts.backport.sweep_git import list_applied_prs_on_branch


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
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.name", "Test")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "commit.gpgsign", "false")


def _commit(repo: Path, path: str, content: str, message: str) -> str:
    destination = repo / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    _git(repo, "add", "--", path)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _candidate(
    *,
    merge_commit_sha: str | None,
    commit_shas: list[str],
) -> BackportCandidate:
    return BackportCandidate(
        source_pr_number=42,
        source_pr_title="Apply the complete source change",
        source_pr_url="https://github.com/example/repo/pull/42",
        target_branch="release",
        merge_commit_sha=merge_commit_sha,
        commit_shas=commit_shas,
    )


def test_rebase_merged_pull_request_fails_closed_end_to_end(
    tmp_path: Path,
) -> None:
    """The merge SHA of a rebase merge is only the final rewritten commit.
    The old engine cherry-picked just that SHA, silently losing 'source
    one'. Replaying rebased series is not supported, so the candidate must
    error loudly with a clean worktree."""
    source = tmp_path / "source"
    _init_repo(source)
    _commit(source, "base.txt", "base\n", "base")
    base = _git(source, "rev-parse", "HEAD")
    _git(source, "branch", "release", base)

    _git(source, "checkout", "-q", "-b", "pull-request")
    first = _commit(source, "one.txt", "one\n", "source one")
    second = _commit(source, "two.txt", "two\n", "source two")

    _git(source, "checkout", "-q", "main")
    _commit(source, "main.txt", "main advanced\n", "advance main")
    _git(source, "checkout", "-q", "-b", "rebased")
    _git(source, "cherry-pick", first, second)
    rebased_tip = _git(source, "rev-parse", "HEAD")

    remote = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-q", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(source, "remote", "add", "origin", str(remote))
    _git(source, "push", "-q", "origin", "release")
    _git(source, "push", "-q", "origin", "rebased:main")
    _git(
        source,
        "push",
        "-q",
        "origin",
        "pull-request:refs/pull/42/head",
    )

    worktree = tmp_path / "worktree"
    subprocess.run(
        [
            "git",
            "clone",
            "-q",
            "--branch",
            "release",
            str(remote),
            str(worktree),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(worktree, "config", "user.name", "Test")
    _git(worktree, "config", "user.email", "test@example.com")

    result = apply_candidate(
        str(worktree),
        _candidate(
            merge_commit_sha=rebased_tip,
            commit_shas=[first, second],
        ),
        "example/repo",
        dict(os.environ),
    )

    assert result.outcome == "error"
    assert "rebase-merged" in result.detail
    assert result.applied_commits == []
    assert not (worktree / "one.txt").exists()
    assert not (worktree / "two.txt").exists()
    assert _git(worktree, "rev-list", "--count", "origin/release..HEAD") == "0"
    assert _git(worktree, "status", "--porcelain") == ""


def test_conflict_limit_rolls_back_without_invoking_resolver(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "one.txt", "base one\n", "base one")
    _commit(repo, "two.txt", "base two\n", "base two")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    source_sha = _commit(repo, "one.txt", "source one\n", "source one")
    (repo / "two.txt").write_text("source two\n", encoding="utf-8")
    _git(repo, "add", "two.txt")
    _git(repo, "commit", "--amend", "-q", "--no-edit")
    source_sha = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "release", base)
    _commit(repo, "one.txt", "release one\n", "release one")
    (repo / "two.txt").write_text("release two\n", encoding="utf-8")
    _git(repo, "add", "two.txt")
    _git(repo, "commit", "--amend", "-q", "--no-edit")
    starting_head = _git(repo, "rev-parse", "HEAD")
    resolver = MagicMock()

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[source_sha]),
        "example/repo",
        dict(os.environ),
        max_conflicting_files=1,
        resolve_conflicts=resolver,
        source_plan=SourceChangePlan(
            strategy="single",
            commits=(source_sha,),
            merge_commit_sha=None,
            source_commits=(source_sha,),
            aggregate_patch_id="test-patch",
        ),
    )

    assert result.outcome == "skipped-conflict"
    assert "Too many conflicting files (2 >" in result.detail
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert _git(repo, "status", "--porcelain") == ""
    resolver.assert_not_called()


def test_hard_cherry_pick_failure_is_not_reported_as_empty(
    tmp_path: Path,
) -> None:
    """A cherry-pick that fails without conflicts and is NOT empty must be an
    error, never 'already applied' — misclassifying it would silently drop
    the commit while the sweep reports the PR as applied."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    source_sha = _commit(repo, "data.txt", "source\n", "source change")

    _git(repo, "checkout", "-q", "-b", "release", base)
    starting_head = _git(repo, "rev-parse", "HEAD")
    (repo / "data.txt").write_text("untracked\n", encoding="utf-8")

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[source_sha]),
        "example/repo",
        dict(os.environ),
        source_plan=SourceChangePlan(
            strategy="single",
            commits=(source_sha,),
            merge_commit_sha=None,
            source_commits=(source_sha,),
            aggregate_patch_id="test-patch",
        ),
    )

    assert result.outcome == "error"
    assert "cherry-pick failed" in result.detail
    assert result.applied_commits == []
    assert _git(repo, "rev-parse", "HEAD") == starting_head


def test_failed_abort_falls_back_to_hard_reset(tmp_path: Path) -> None:
    """When ``cherry-pick --abort`` itself fails, the helper must reset the
    tree before any caller continues — an empty/no-op path returning success
    on a dirty tree would poison every later commit in the run."""
    calls: list[list[str]] = []

    def run_process(cmd, **_kwargs):
        calls.append(cmd)
        if cmd[:3] == ["git", "cherry-pick", "--abort"]:
            return subprocess.CompletedProcess(
                cmd, 128, stdout="",
                stderr="fatal: no cherry-pick or revert in progress",
            )
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    assert application._abort_cherry_pick(str(tmp_path), run_process) is True

    assert ["git", "cherry-pick", "--abort"] in calls
    assert ["git", "reset", "--hard", "HEAD"] in calls


def test_abort_and_reset_both_failing_reports_unclean(tmp_path: Path) -> None:
    """If the fallback reset fails too, the tree state is unknown; callers
    must see False and turn the candidate into an error rather than
    continuing or reporting skipped-existing."""

    def run_process(cmd, **_kwargs):
        return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="boom")

    assert application._abort_cherry_pick(str(tmp_path), run_process) is False


def test_successful_abort_does_not_reset(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run_process(cmd, **_kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    assert application._abort_cherry_pick(str(tmp_path), run_process) is True

    assert calls == [["git", "cherry-pick", "--abort"]]
