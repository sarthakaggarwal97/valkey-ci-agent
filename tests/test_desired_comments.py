from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.common.desired_comments import (
    DesiredComment,
    parse_desired_comment,
    reconcile_desired_comment,
    record_desired_comment,
)


def _desired() -> DesiredComment:
    return DesiredComment(
        key="ci-fix:123:7:pushed:abcdef",
        expected_head_sha="a" * 40,
        body="Validated fix published.",
        reaction_comment_id=55,
        reaction="+1",
    )


def test_records_pending_state_then_reconciles_after_expected_head() -> None:
    created = MagicMock(
        body="",
        user=SimpleNamespace(login="publisher[bot]"),
    )
    issue = MagicMock(number=7)
    issue.get_comments.return_value = []
    issue.create_comment.return_value = created
    desired = _desired()

    comment = record_desired_comment(
        issue,
        desired,
        writer_login="publisher[bot]",
    )
    pending = issue.create_comment.call_args.args[0]
    assert parse_desired_comment(pending) == desired
    assert "pending" in pending

    requester = MagicMock()
    repository = MagicMock(full_name="org/repo", _requester=requester)
    comment.body = pending
    assert reconcile_desired_comment(
        repository,
        comment,
        desired,
        current_head_sha="a" * 40,
    )
    final = comment.edit.call_args.args[0]
    assert parse_desired_comment(final) == desired
    assert "Validated fix published." in final
    requester.requestJsonAndCheck.assert_called_once_with(
        "POST",
        "/repos/org/repo/issues/comments/55/reactions",
        input={"content": "+1"},
    )


def test_pending_state_does_not_publish_for_a_different_head() -> None:
    desired = _desired()
    comment = MagicMock()
    repository = MagicMock(full_name="org/repo")

    assert not reconcile_desired_comment(
        repository,
        comment,
        desired,
        current_head_sha="b" * 40,
    )
    comment.edit.assert_not_called()


def test_record_is_idempotent_for_existing_bot_comment() -> None:
    desired = _desired()
    existing = MagicMock(
        body="",
        user=SimpleNamespace(login="publisher[bot]"),
    )
    issue = MagicMock(number=7)
    issue.get_comments.return_value = [existing]
    first_issue = MagicMock(number=7)
    first_issue.get_comments.return_value = []
    first_issue.create_comment.return_value = existing
    record_desired_comment(
        first_issue,
        desired,
        writer_login="publisher[bot]",
    )
    existing.body = first_issue.create_comment.call_args.args[0]

    result = record_desired_comment(
        issue,
        desired,
        writer_login="publisher[bot]",
    )

    assert result is existing
    issue.create_comment.assert_not_called()
    existing.edit.assert_not_called()
