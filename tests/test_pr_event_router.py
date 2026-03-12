"""Tests for PR reviewer event parsing and routing."""

from __future__ import annotations

import json

from scripts.pr_event_router import PREventRouter, load_event_from_path


def test_load_pull_request_target_event(tmp_path) -> None:
    payload = {
        "repository": {"full_name": "owner/repo"},
        "sender": {"login": "alice"},
        "pull_request": {"number": 42, "body": "desc"},
    }
    path = tmp_path / "event.json"
    path.write_text(json.dumps(payload))

    event = load_event_from_path("pull_request_target", path)

    assert event.repo == "owner/repo"
    assert event.actor == "alice"
    assert event.pr_number == 42
    assert event.body == "desc"
    assert PREventRouter().classify_event(event) == "review"


def test_review_comment_reply_routes_to_chat(tmp_path) -> None:
    payload = {
        "repository": {"full_name": "owner/repo"},
        "sender": {"login": "alice"},
        "pull_request": {"number": 7},
        "comment": {
            "id": 99,
            "body": "Can you explain this?",
            "path": "src/foo.c",
            "line": 18,
            "in_reply_to_id": 55,
        },
    }
    path = tmp_path / "event.json"
    path.write_text(json.dumps(payload))

    event = load_event_from_path("pull_request_review_comment", path)

    assert event.is_review_comment is True
    assert event.comment_id == 99
    assert event.in_reply_to_id == 55
    assert PREventRouter().classify_event(event) == "chat"


def test_issue_comment_without_invocation_is_skipped(tmp_path) -> None:
    payload = {
        "repository": {"full_name": "owner/repo"},
        "sender": {"login": "alice"},
        "issue": {"number": 4, "pull_request": {}},
        "comment": {"id": 3, "body": "plain comment"},
    }
    path = tmp_path / "event.json"
    path.write_text(json.dumps(payload))

    event = load_event_from_path("issue_comment", path)

    assert PREventRouter().classify_event(event) == "skip"
