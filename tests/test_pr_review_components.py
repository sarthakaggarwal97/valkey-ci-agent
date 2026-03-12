"""Tests for reviewer Bedrock-backed components."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.code_reviewer import CodeReviewer
from scripts.config import ReviewerConfig
from scripts.models import (
    ChangedFile,
    DiffScope,
    PullRequestContext,
    ReviewThread,
)
from scripts.pr_summarizer import PRSummarizer
from scripts.review_chat import ReviewChat


def _context() -> PullRequestContext:
    return PullRequestContext(
        repo="owner/repo",
        number=17,
        title="Improve failover logic",
        body="This updates failover behavior.",
        base_sha="base123",
        head_sha="head456",
        author="alice",
        files=[
            ChangedFile(
                path="src/failover.c",
                status="modified",
                additions=8,
                deletions=2,
                patch="@@ -10,2 +10,8 @@\n-old\n+new",
                contents="int failover(void) { return 1; }",
                is_binary=False,
            )
        ],
    )


def test_pr_summarizer_uses_light_model() -> None:
    bedrock = MagicMock()
    bedrock.invoke.return_value = """
    {
      "walkthrough": "Updates failover handling.",
      "file_groups_markdown": "- Core: failover logic",
      "release_notes": "Improves failover handling."
    }
    """
    summarizer = PRSummarizer(bedrock)
    config = ReviewerConfig()

    result = summarizer.summarize(_context(), config)

    assert result.walkthrough == "Updates failover handling."
    kwargs = bedrock.invoke.call_args.kwargs
    assert kwargs["model_id"] == config.models.light_model_id


def test_code_reviewer_uses_heavy_model_and_filters_findings() -> None:
    bedrock = MagicMock()
    bedrock.invoke.return_value = """
    {
      "findings": [
        {
          "path": "src/failover.c",
          "line": 14,
          "severity": "high",
          "body": "This can leave failover state stale after timeout."
        },
        {
          "path": "README.md",
          "line": 1,
          "severity": "low",
          "body": "LGTM"
        }
      ]
    }
    """
    reviewer = CodeReviewer(bedrock)
    config = ReviewerConfig(max_review_comments=5)
    scope = DiffScope(
        base_sha="base123",
        head_sha="head456",
        files=_context().files,
        incremental=False,
    )

    findings = reviewer.review(_context(), scope, config)

    assert len(findings) == 1
    assert findings[0].path == "src/failover.c"
    kwargs = bedrock.invoke.call_args.kwargs
    assert kwargs["model_id"] == config.models.heavy_model_id


def test_review_chat_uses_heavy_model() -> None:
    bedrock = MagicMock()
    bedrock.invoke.return_value = "Add a targeted failover timeout regression test."
    chat = ReviewChat(bedrock)
    config = ReviewerConfig()

    reply = chat.reply(
        _context(),
        ReviewThread(
            comment_id=1,
            path="src/failover.c",
            line=14,
            conversation=["Can you suggest a test?"],
        ),
        "/reviewbot can you suggest a test?",
        config,
    )

    assert "targeted failover timeout regression test" in reply
    kwargs = bedrock.invoke.call_args.kwargs
    assert kwargs["model_id"] == config.models.heavy_model_id


def test_code_reviewer_raises_on_unparseable_response() -> None:
    bedrock = MagicMock()
    bedrock.invoke.return_value = "not json"
    reviewer = CodeReviewer(bedrock)
    config = ReviewerConfig(max_review_comments=5)
    scope = DiffScope(
        base_sha="base123",
        head_sha="head456",
        files=_context().files,
        incremental=False,
    )

    with pytest.raises(ValueError, match="Unparseable review response"):
        reviewer.review(_context(), scope, config)
