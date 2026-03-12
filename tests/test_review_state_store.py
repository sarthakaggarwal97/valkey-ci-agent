"""Tests for PR reviewer state persistence."""

from __future__ import annotations

from unittest.mock import MagicMock

from github.GithubException import GithubException

from scripts.models import ReviewState
from scripts.review_state_store import ReviewStateStore


def test_review_state_round_trip() -> None:
    store = ReviewStateStore()
    state = ReviewState(
        repo="owner/repo",
        pr_number=12,
        last_reviewed_head_sha="abc123",
        summary_comment_id=9,
        review_comment_ids=[1, 2, 3],
        updated_at="2026-03-12T00:00:00+00:00",
    )

    store.from_dict({"owner/repo#12": {
        "repo": state.repo,
        "pr_number": state.pr_number,
        "last_reviewed_head_sha": state.last_reviewed_head_sha,
        "summary_comment_id": state.summary_comment_id,
        "review_comment_ids": state.review_comment_ids,
        "updated_at": state.updated_at,
    }})

    restored = store.load("owner/repo", 12)

    assert restored == state


def test_save_creates_bot_data_branch_when_missing() -> None:
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_git_ref.side_effect = [
        GithubException(404, {"message": "missing bot-data"}),
        MagicMock(object=MagicMock(sha="base-sha")),
    ]
    gh = MagicMock()
    gh.get_repo.return_value = repo
    store = ReviewStateStore(gh, "owner/repo")

    store.save(
        ReviewState(
            repo="owner/repo",
            pr_number=5,
            last_reviewed_head_sha="abc123",
            summary_comment_id=1,
            review_comment_ids=[],
            updated_at="2026-03-12T00:00:00+00:00",
        )
    )

    repo.create_git_ref.assert_called_once_with(
        ref="refs/heads/bot-data",
        sha="base-sha",
    )


def test_save_does_not_fallback_to_create_on_non_404_lookup_error() -> None:
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_git_ref.return_value = MagicMock()
    repo.get_contents.side_effect = GithubException(500, {"message": "boom"})
    gh = MagicMock()
    gh.get_repo.return_value = repo
    store = ReviewStateStore(gh, "owner/repo")

    store.save(
        ReviewState(
            repo="owner/repo",
            pr_number=5,
            last_reviewed_head_sha="abc123",
            summary_comment_id=1,
            review_comment_ids=[],
            updated_at="2026-03-12T00:00:00+00:00",
        )
    )

    repo.create_file.assert_not_called()
