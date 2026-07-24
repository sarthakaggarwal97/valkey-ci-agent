"""Real-Git contract tests for resolving merged pull-request histories."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.backport.source_change import (
    SourceChangeError,
    plan_source_change,
    prepare_source_change,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit(repo: Path, path: str, content: str, message: str) -> str:
    destination = repo / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    _git(repo, "add", "--", path)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def history(tmp_path: Path) -> tuple[Path, str, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "base.txt", "base\n", "base")
    base = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "one")
    second = _commit(repo, "two.txt", "two\n", "two")
    return repo, base, first, second


def test_plans_real_merge_commit(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, second = history
    _git(repo, "checkout", "-q", "-b", "merge-target", base)
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge source", "source")
    merge_sha = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), merge_sha, [first, second])

    assert plan.strategy == "merge"
    assert plan.commits == (merge_sha,)
    assert plan.aggregate_patch_id


def test_plans_matching_squash_as_one_aggregate_commit(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, second = history
    _git(repo, "checkout", "-q", "-b", "squash-target", base)
    _git(repo, "merge", "-q", "--squash", "source")
    _git(repo, "commit", "-q", "-m", "squash source")
    squash_sha = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), squash_sha, [first, second])

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)
    assert plan.source_commits == (first, second)


def test_squash_ignores_target_updates_merged_into_source(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "base.txt", "base\n", "base")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "one")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "main update\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    _git(repo, "merge", "-q", "--no-ff", "-m", "sync main", "main")
    sync = _git(repo, "rev-parse", "HEAD")
    second = _commit(repo, "two.txt", "two\n", "two")

    _git(repo, "checkout", "-q", "main")
    _git(repo, "merge", "-q", "--squash", "source")
    _git(repo, "commit", "-q", "-m", "squash source")
    squash_sha = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), squash_sha, [first, sync, second])

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)


def test_rebase_series_skips_source_branch_sync_merge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "base.txt", "base\n", "base")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "source one")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "main update\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    _git(repo, "merge", "-q", "--no-ff", "-m", "sync main", "main")
    sync = _git(repo, "rev-parse", "HEAD")
    second = _commit(repo, "two.txt", "two\n", "source two")

    _git(repo, "checkout", "-q", "-b", "rebased", "main")
    _git(repo, "cherry-pick", first, second)
    rebased_tip = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(
        str(repo),
        rebased_tip,
        [first, sync, second],
    )

    assert plan.strategy == "series"
    assert plan.commits == (first, second)
    assert plan.source_commits == (first, sync, second)
    assert sync not in plan.commits

    _git(repo, "checkout", "-q", "-b", "backport", "main")
    _git(repo, "cherry-pick", *plan.commits)
    assert (repo / "one.txt").read_text(encoding="utf-8") == "one\n"
    assert (repo / "two.txt").read_text(encoding="utf-8") == "two\n"
    assert (repo / "main.txt").read_text(encoding="utf-8") == "main update\n"


def test_multi_commit_rebase_uses_complete_source_series(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, second = history
    _git(repo, "branch", "rebased", "source")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "base.txt", "base advanced\n", "advance base")
    _git(repo, "checkout", "-q", "rebased")
    _git(repo, "rebase", "-q", "main")
    rebased_tip = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), rebased_tip, [first, second])

    assert plan.strategy == "series"
    assert plan.commits == (first, second)
    assert plan.merge_commit_sha == rebased_tip


def test_fast_forward_rebase_tip_still_uses_all_source_commits(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, second = history

    plan = plan_source_change(str(repo), second, [first, second])

    assert plan.strategy == "series"
    assert plan.commits == (first, second)


def test_rebase_does_not_lose_earlier_whitespace_only_commit(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "value.txt", "value\n", "base")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "value.txt", "value \n", "preserve whitespace change")
    second = _commit(repo, "feature.txt", "feature\n", "add feature")

    _git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "advance\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    _git(repo, "rebase", "-q", "main")
    rebased_tip = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), rebased_tip, [first, second])

    assert plan.strategy == "series"
    assert plan.commits == (first, second)


def test_single_commit_pull_request_uses_authoritative_merge_sha(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, _second = history
    _git(repo, "checkout", "-q", "-b", "single-target", base)
    _git(repo, "cherry-pick", first)
    merged_sha = _git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), merged_sha, [first])

    assert plan.strategy == "single"
    assert plan.commits == (merged_sha,)


def test_no_merge_sha_uses_complete_ordered_series(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, second = history

    plan = plan_source_change(str(repo), None, [first, second])

    assert plan.strategy == "series"
    assert plan.commits == (first, second)


def test_disconnected_source_history_is_refused(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    base = _commit(repo, "base.txt", "base\n", "base")
    _git(repo, "checkout", "-q", "-b", "left", base)
    left = _commit(repo, "left.txt", "left\n", "left")
    _git(repo, "checkout", "-q", "-b", "right", base)
    right = _commit(repo, "right.txt", "right\n", "right")

    with pytest.raises(SourceChangeError, match="disconnected or out of order"):
        plan_source_change(str(repo), None, [left, right])


def test_ambiguous_merge_base_explains_classification_failure(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    base = _commit(repo, "base.txt", "base\n", "base")

    _git(repo, "checkout", "-q", "-b", "left", base)
    left = _commit(repo, "left.txt", "left\n", "left")
    _git(repo, "checkout", "-q", "-b", "right", base)
    right = _commit(repo, "right.txt", "right\n", "right")

    _git(repo, "checkout", "-q", "left")
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge right", right)
    left_merge = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "right")
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge left", left)
    right_merge = _git(repo, "rev-parse", "HEAD")

    _git(repo, "checkout", "-q", left_merge)
    merged_sha = _commit(repo, "result.txt", "result\n", "merged result")

    with pytest.raises(
        SourceChangeError,
        match="ambiguous merge base.*cannot safely classify",
    ):
        plan_source_change(
            str(repo),
            merged_sha,
            [right, right_merge],
        )


def test_missing_source_identity_is_refused(history: tuple[Path, str, str, str]) -> None:
    repo, _base, _first, _second = history

    with pytest.raises(SourceChangeError, match="neither a merge SHA nor source commits"):
        plan_source_change(str(repo), None, [])


def test_incomplete_source_commit_page_is_refused_before_fetch(
    tmp_path: Path,
) -> None:
    with pytest.raises(SourceChangeError, match="commit list.*incomplete"):
        prepare_source_change(
            str(tmp_path),
            42,
            "a" * 40,
            ["b" * 40],
            source_commits_complete=False,
        )


def test_series_sync_merge_with_manual_content_fails_closed(
    tmp_path: Path,
) -> None:
    """A sync merge that resolved conflicts by hand is not replayable.

    Skipping the merge would silently drop the resolution content — the exact
    silent-loss class this module exists to prevent — so planning must refuse.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "shared.txt", "base\n", "base")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "shared.txt", "source\n", "source one")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "shared.txt", "main\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    merge = subprocess.run(
        ["git", "merge", "--no-ff", "-m", "sync main", "main"],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    assert merge.returncode != 0  # conflict on shared.txt
    (repo / "shared.txt").write_text("resolved by hand\n", encoding="utf-8")
    _git(repo, "add", "--", "shared.txt")
    _git(repo, "commit", "-q", "--no-edit")
    sync = _git(repo, "rev-parse", "HEAD")
    second = _commit(repo, "two.txt", "two\n", "source two")

    with pytest.raises(SourceChangeError, match="carries changes of its own"):
        plan_source_change(str(repo), None, [first, sync, second])


def test_series_evil_merge_with_extra_content_fails_closed(
    tmp_path: Path,
) -> None:
    """A conflict-free merge with a change smuggled in is not replayable."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    _commit(repo, "base.txt", "base\n", "base")

    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "source one")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "main\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    _git(repo, "merge", "-q", "--no-ff", "--no-commit", "main")
    (repo / "evil.txt").write_text("smuggled\n", encoding="utf-8")
    _git(repo, "add", "--", "evil.txt")
    _git(repo, "commit", "-q", "-m", "sync main")
    sync = _git(repo, "rev-parse", "HEAD")
    second = _commit(repo, "two.txt", "two\n", "source two")

    with pytest.raises(SourceChangeError, match="carries changes of its own"):
        plan_source_change(str(repo), None, [first, sync, second])
