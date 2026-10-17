"""Real-Git contract tests for planning merged pull-request histories."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.backport.source_plan import (
    SourceChangeError,
    SourceChangePlan,
    plan_source_change,
    prepare_source_change,
)
from tests.git_helpers import git, init_repo


def _commit(repo: Path, path: str, content: str, message: str) -> str:
    destination = repo / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    git(repo, "add", "--", path)
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def history(tmp_path: Path) -> tuple[Path, str, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")
    base = git(repo, "rev-parse", "HEAD")

    git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "one")
    second = _commit(repo, "two.txt", "two\n", "two")
    return repo, base, first, second


def test_plans_real_merge_commit(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, second = history
    git(repo, "checkout", "-q", "-b", "merge-target", base)
    git(repo, "merge", "-q", "--no-ff", "-m", "merge source", "source")
    merge_sha = git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), merge_sha, [first, second])

    assert plan.strategy == "merge"
    assert plan.commits == (merge_sha,)
    assert plan.aggregate_patch_id


def test_plans_matching_squash_as_one_aggregate_commit(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, second = history
    git(repo, "checkout", "-q", "-b", "squash-target", base)
    git(repo, "merge", "-q", "--squash", "source")
    git(repo, "commit", "-q", "-m", "squash source")
    squash_sha = git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), squash_sha, [first, second])

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)
    assert plan.source_commits == (first, second)


def test_squash_ignores_target_updates_merged_into_source(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    _commit(repo, "base.txt", "base\n", "base")

    git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "one.txt", "one\n", "one")
    git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "main update\n", "advance main")
    git(repo, "checkout", "-q", "source")
    git(repo, "merge", "-q", "--no-ff", "-m", "sync main", "main")
    sync = git(repo, "rev-parse", "HEAD")
    second = _commit(repo, "two.txt", "two\n", "two")

    git(repo, "checkout", "-q", "main")
    git(repo, "merge", "-q", "--squash", "source")
    git(repo, "commit", "-q", "-m", "squash source")
    squash_sha = git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), squash_sha, [first, sync, second])

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)


def test_squash_ignores_adjacent_target_update_context(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    _commit(repo, "settings.mk", "BUILD_TLS\nBUILD_RDMA\n", "base")
    base = git(repo, "rev-parse", "HEAD")

    git(repo, "checkout", "-q", "-b", "source")
    first = _commit(
        repo,
        "settings.mk",
        "BUILD_TLS\nBUILD_RDMA\nBUILD_ZSTD\n",
        "add zstd setting",
    )
    second = _commit(
        repo,
        "settings.mk",
        "BUILD_TLS\nBUILD_RDMA\nBUILD_ZSTD\nZSTD_PREFIX\n",
        "add zstd prefix",
    )

    git(repo, "checkout", "-q", "main")
    _commit(
        repo,
        "settings.mk",
        "BUILD_TLS\nOPENSSL_PREFIX\nBUILD_RDMA\n",
        "advance target beside source change",
    )
    git(repo, "merge", "-q", "--squash", "source")
    git(repo, "commit", "-q", "-m", "squash source")
    squash_sha = git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), squash_sha, [first, second])

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)
    assert plan.source_commits == (first, second)
    assert git(repo, "merge-base", squash_sha, second) == base


def test_multi_commit_rebase_merge_is_refused(
    history: tuple[Path, str, str, str],
) -> None:
    """A rebase merge's SHA is only the final rewritten commit. Picking it
    would silently drop the earlier commits, so planning must refuse."""
    repo, base, first, second = history
    git(repo, "branch", "rebased", "source")
    git(repo, "checkout", "-q", "main")
    _commit(repo, "base.txt", "base advanced\n", "advance base")
    git(repo, "checkout", "-q", "rebased")
    git(repo, "rebase", "-q", "main")
    rebased_tip = git(repo, "rev-parse", "HEAD")

    with pytest.raises(SourceChangeError, match="rebase-merged"):
        plan_source_change(str(repo), rebased_tip, [first, second])


def test_fast_forward_rebase_merge_is_refused(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, second = history

    with pytest.raises(SourceChangeError, match="rebase-merged"):
        plan_source_change(str(repo), second, [first, second])


def test_rebase_with_whitespace_only_commit_is_refused(tmp_path: Path) -> None:
    """The whitespace-only first commit is exactly what the old code lost:
    picking only the rebased tip drops it silently. Planning refuses now."""
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    _commit(repo, "value.txt", "value\n", "base")

    git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "value.txt", "value \n", "preserve whitespace change")
    second = _commit(repo, "feature.txt", "feature\n", "add feature")

    git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "advance\n", "advance main")
    git(repo, "checkout", "-q", "source")
    git(repo, "rebase", "-q", "main")
    rebased_tip = git(repo, "rev-parse", "HEAD")

    with pytest.raises(SourceChangeError, match="rebase-merged"):
        plan_source_change(str(repo), rebased_tip, [first, second])


def test_single_commit_pull_request_uses_authoritative_merge_sha(
    history: tuple[Path, str, str, str],
) -> None:
    repo, base, first, _second = history
    git(repo, "checkout", "-q", "-b", "single-target", base)
    git(repo, "cherry-pick", first)
    merged_sha = git(repo, "rev-parse", "HEAD")

    plan = plan_source_change(str(repo), merged_sha, [first])

    assert plan.strategy == "single"
    assert plan.commits == (merged_sha,)


def test_no_merge_sha_multi_commit_is_refused(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, second = history

    with pytest.raises(SourceChangeError, match="rebase-merged"):
        plan_source_change(str(repo), None, [first, second])


def test_no_merge_sha_single_commit_plans_single(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, _second = history

    plan = plan_source_change(str(repo), None, [first])

    assert plan.strategy == "single"
    assert plan.commits == (first,)


def test_disconnected_multi_commit_history_is_refused(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    base = _commit(repo, "base.txt", "base\n", "base")
    git(repo, "checkout", "-q", "-b", "left", base)
    left = _commit(repo, "left.txt", "left\n", "left")
    git(repo, "checkout", "-q", "-b", "right", base)
    right = _commit(repo, "right.txt", "right\n", "right")

    with pytest.raises(SourceChangeError, match="rebase-merged"):
        plan_source_change(str(repo), None, [left, right])


def test_ambiguous_merge_base_explains_classification_failure(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    init_repo(repo)
    base = _commit(repo, "base.txt", "base\n", "base")

    git(repo, "checkout", "-q", "-b", "left", base)
    left = _commit(repo, "left.txt", "left\n", "left")
    git(repo, "checkout", "-q", "-b", "right", base)
    right = _commit(repo, "right.txt", "right\n", "right")

    git(repo, "checkout", "-q", "left")
    git(repo, "merge", "-q", "--no-ff", "-m", "merge right", right)
    left_merge = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "-q", "right")
    git(repo, "merge", "-q", "--no-ff", "-m", "merge left", left)
    right_merge = git(repo, "rev-parse", "HEAD")

    git(repo, "checkout", "-q", left_merge)
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


def test_empty_source_list_cannot_classify_one_parent_merge(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, _first, second = history

    with pytest.raises(SourceChangeError, match="source commit list is empty"):
        plan_source_change(str(repo), second, [])


def test_source_plan_requires_one_authoritative_commit() -> None:
    with pytest.raises(ValueError, match="exactly one authoritative commit"):
        SourceChangePlan(
            strategy="squash",
            commits=("first", "second"),
            merge_commit_sha="squash",
            source_commits=("first", "second"),
            aggregate_patch_id="patch",
        )


def test_incomplete_commit_page_still_classifies_squash(
    tmp_path: Path,
) -> None:
    """A squash of a PR whose commit listing paged out must still classify:
    the aggregate patch identity only needs the PR head tip, which is
    fetched from the authoritative ref."""
    remote = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-q", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    source = tmp_path / "source"
    source.mkdir()
    init_repo(source)
    _commit(source, "base.txt", "base\n", "base")

    git(source, "checkout", "-q", "-b", "pull-request")
    first = _commit(source, "one.txt", "one\n", "one")
    second = _commit(source, "two.txt", "two\n", "two")

    git(source, "checkout", "-q", "main")
    git(source, "merge", "-q", "--squash", "pull-request")
    git(source, "commit", "-q", "-m", "squash")
    squash_sha = git(source, "rev-parse", "HEAD")

    git(source, "remote", "add", "origin", str(remote))
    git(source, "push", "-q", "origin", "main")
    git(source, "push", "-q", "origin", "pull-request:refs/pull/42/head")

    worktree = tmp_path / "worktree"
    subprocess.run(
        ["git", "clone", "-q", str(remote), str(worktree)],
        check=True,
        capture_output=True,
        text=True,
    )

    # Simulate an incomplete page that already contains the tip out of order.
    # The fetched PR head must still become the final source commit.
    plan = prepare_source_change(
        str(worktree),
        42,
        squash_sha,
        [second, first],
        source_commits_complete=False,
    )

    assert plan.strategy == "squash"
    assert plan.commits == (squash_sha,)
    assert plan.source_commits == (first, second)


def test_duplicate_source_commits_are_refused(
    history: tuple[Path, str, str, str],
) -> None:
    repo, _base, first, second = history

    with pytest.raises(SourceChangeError, match="duplicate SHAs"):
        plan_source_change(str(repo), None, [first, second, first])


def test_source_commit_absent_from_pr_head_fails_after_fetch(
    tmp_path: Path,
) -> None:
    """A commit the API listed but the PR head ref does not contain must fail
    loudly after the fetch, not silently plan around it."""
    remote = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", "-q", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    source = tmp_path / "source"
    source.mkdir()
    init_repo(source)
    _commit(source, "base.txt", "base\n", "base")
    git(source, "remote", "add", "origin", str(remote))
    git(source, "push", "-q", "origin", "main:refs/pull/42/head")

    worktree = tmp_path / "worktree"
    subprocess.run(
        ["git", "clone", "-q", str(remote), str(worktree)],
        check=True,
        capture_output=True,
        text=True,
    )

    phantom = "d" * 40
    with pytest.raises(
        SourceChangeError,
        match=f"PR head does not contain source commit\\(s\\): {phantom}",
    ):
        prepare_source_change(
            str(worktree),
            42,
            None,
            [phantom],
        )
