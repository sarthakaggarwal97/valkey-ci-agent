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


def _series_plan(*commits: str) -> SourceChangePlan:
    return SourceChangePlan(
        strategy="series",
        commits=commits,
        merge_commit_sha=None,
        source_commits=commits,
        aggregate_patch_id="test-patch",
    )


def test_fetches_and_applies_every_commit_from_rebase_merge(
    tmp_path: Path,
) -> None:
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

    assert result.outcome == "applied"
    assert result.applied_commits == [first, second]
    assert (worktree / "one.txt").read_text(encoding="utf-8") == "one\n"
    assert (worktree / "two.txt").read_text(encoding="utf-8") == "two\n"
    assert _git(worktree, "rev-list", "--count", "origin/release..HEAD") == "2"
    assert _git(
        worktree,
        "log",
        "--reverse",
        "--format=%s",
        "origin/release..HEAD",
    ).splitlines() == [
        "source one (#42)",
        "source two (#42)",
    ]
    applied = list_applied_prs_on_branch(
        str(worktree),
        "release",
        "HEAD",
    )
    assert [item.source_pr_number for item in applied] == [42]
    assert applied[0].source_pr_title in {"source one", "source two"}


def test_series_identity_annotation_preserves_commit_body(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    source_sha = _commit(repo, "feature.txt", "feature\n", "source feature")
    _git(
        repo,
        "commit",
        "--amend",
        "-q",
        "--author",
        "Contributor <contributor@example.com>",
        "-m",
        "source feature",
        "-m",
        "Preserve this explanation.\n\nSigned-off-by: Test <test@example.com>",
    )
    source_sha = _git(repo, "rev-parse", "HEAD")
    follow_up = _commit(repo, "follow-up.txt", "follow-up\n", "follow up")

    _git(repo, "checkout", "-q", "-b", "release", base)
    result = apply_candidate(
        str(repo),
        _candidate(
            merge_commit_sha=None,
            commit_shas=[source_sha, follow_up],
        ),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(source_sha, follow_up),
    )

    assert result.outcome == "applied"
    first_message = _git(repo, "show", "-s", "--format=%B", "HEAD^")
    assert first_message.startswith("source feature (#42)\n")
    assert "Preserve this explanation." in first_message
    assert "Signed-off-by: Test <test@example.com>" in first_message
    assert f"(cherry picked from commit {source_sha})" in first_message
    assert (
        _git(repo, "show", "-s", "--format=%an <%ae>", "HEAD^")
        == "Contributor <contributor@example.com>"
    )
    second_message = _git(repo, "show", "-s", "--format=%B", "HEAD")
    assert second_message.startswith("follow up (#42)\n")
    assert f"(cherry picked from commit {follow_up})" in second_message


def test_series_identity_failure_rolls_back_every_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "source one")
    second = _commit(repo, "two.txt", "two\n", "source two")
    _git(repo, "checkout", "-q", "-b", "release", base)
    starting_head = _git(repo, "rev-parse", "HEAD")

    def fail_identity(*_args, **_kwargs):
        raise RuntimeError("identity write failed")

    monkeypatch.setattr(
        application,
        "_append_source_pr_to_head_subject",
        fail_identity,
    )
    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[first, second]),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(first, second),
    )

    assert result.outcome == "error"
    assert "could not record source PR identity" in result.detail
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert not (repo / "one.txt").exists()
    assert not (repo / "two.txt").exists()


def test_resolves_conflict_then_continues_remaining_source_commits(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "shared.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "shared.txt", "source\n", "source conflict")
    second = _commit(repo, "later.txt", "later\n", "source follow-up")

    _git(repo, "checkout", "-q", "-b", "release", base)
    _commit(repo, "shared.txt", "release\n", "release divergence")

    def resolve(*_args, **_kwargs):
        return [
            ResolutionResult(
                path="shared.txt",
                resolved_content="resolved\n",
                resolution_summary="preserved source intent",
            )
        ]

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[first, second]),
        "example/repo",
        dict(os.environ),
        resolve_conflicts=resolve,
        source_plan=_series_plan(first, second),
    )

    assert result.outcome == "applied"
    assert result.applied_commits == [first, second]
    assert result.conflicting_commit_sha == first
    assert (repo / "shared.txt").read_text(encoding="utf-8") == "resolved\n"
    assert (repo / "later.txt").read_text(encoding="utf-8") == "later\n"
    assert _git(repo, "status", "--porcelain") == ""


def test_empty_source_commit_does_not_block_later_commit(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "source one")
    second = _commit(repo, "two.txt", "two\n", "source two")

    _git(repo, "checkout", "-q", "-b", "release", base)
    _git(repo, "cherry-pick", first)

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[first, second]),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(first, second),
    )

    assert result.outcome == "applied"
    assert result.applied_commits == [second]
    assert (repo / "two.txt").read_text(encoding="utf-8") == "two\n"
    assert "partial source series:" in result.detail
    assert _git(repo, "status", "--porcelain") == ""


def test_unresolved_later_commit_rolls_back_entire_source_series(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "shared.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "first.txt", "first\n", "source clean commit")
    second = _commit(repo, "shared.txt", "source\n", "source conflict")

    _git(repo, "checkout", "-q", "-b", "release", base)
    _commit(repo, "shared.txt", "release\n", "release divergence")
    starting_head = _git(repo, "rev-parse", "HEAD")

    def unresolved(*_args, **_kwargs):
        return [
            ResolutionResult(
                path="shared.txt",
                resolved_content=None,
                resolution_summary="needs a maintainer",
            )
        ]

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[first, second]),
        "example/repo",
        dict(os.environ),
        resolve_conflicts=unresolved,
        source_plan=_series_plan(first, second),
    )

    assert result.outcome == "skipped-conflict"
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert not (repo / "first.txt").exists()
    assert _git(repo, "status", "--porcelain") == ""


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


def test_untracked_file_collision_mid_series_rolls_back_cleanly(
    tmp_path: Path,
) -> None:
    """A cherry-pick that fails before creating sequencer state (untracked
    file would be overwritten) makes ``cherry-pick --abort`` itself fail.
    The candidate must still report an error and leave HEAD at the start,
    with earlier series commits rolled back."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "source one")
    second = _commit(repo, "collide.txt", "source\n", "source two")

    _git(repo, "checkout", "-q", "-b", "release", base)
    starting_head = _git(repo, "rev-parse", "HEAD")
    # Untracked file that the second cherry-pick would overwrite.
    (repo / "collide.txt").write_text("untracked local\n", encoding="utf-8")

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[first, second]),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(first, second),
    )

    assert result.outcome == "error"
    assert "cherry-pick failed" in result.detail
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert not (repo / "one.txt").exists()
    # The untracked file is the user's; rollback must not delete it.
    assert (repo / "collide.txt").read_text(encoding="utf-8") == "untracked local\n"


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


def test_series_identity_appends_even_when_subject_references_pr_mid_text(
    tmp_path: Path,
) -> None:
    """Only a *trailing* (#N) marks a commit as belonging to PR N. A subject
    that merely references (#N) mid-text (e.g. a revert of the PR's earlier
    attempt) must still be amended, or downstream dedup and mark-done will
    key off the wrong signal."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    source_sha = _commit(
        repo,
        "one.txt",
        "one\n",
        'Revert "broken attempt (#42)" and redo',
    )
    _git(repo, "checkout", "-q", "-b", "release", base)

    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[source_sha]),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(source_sha),
    )

    assert result.outcome == "applied"
    subject = _git(repo, "show", "-s", "--format=%s", "HEAD")
    assert subject == 'Revert "broken attempt (#42)" and redo (#42)'


def test_single_commit_series_identity_failure_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    source_sha = _commit(repo, "one.txt", "one\n", "source one")
    _git(repo, "checkout", "-q", "-b", "release", base)
    starting_head = _git(repo, "rev-parse", "HEAD")

    def fail_identity(*_args, **_kwargs):
        raise RuntimeError("identity write failed")

    monkeypatch.setattr(
        application,
        "_append_source_pr_to_head_subject",
        fail_identity,
    )
    result = apply_candidate(
        str(repo),
        _candidate(merge_commit_sha=None, commit_shas=[source_sha]),
        "example/repo",
        dict(os.environ),
        source_plan=_series_plan(source_sha),
    )

    assert result.outcome == "error"
    assert "could not record source PR identity" in result.detail
    assert _git(repo, "rev-parse", "HEAD") == starting_head
    assert not (repo / "one.txt").exists()


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

    application._abort_cherry_pick(str(tmp_path), run_process)

    assert ["git", "cherry-pick", "--abort"] in calls
    assert ["git", "reset", "--hard", "HEAD"] in calls


def test_successful_abort_does_not_reset(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def run_process(cmd, **_kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    application._abort_cherry_pick(str(tmp_path), run_process)

    assert calls == [["git", "cherry-pick", "--abort"]]
