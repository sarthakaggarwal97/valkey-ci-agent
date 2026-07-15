from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from scripts.ci_fix.dispatch_ledger import (
    LEDGER_MARKER,
    DispatchEvent,
    DispatchLedger,
    format_event_comment,
    parse_event_comment,
)

_WRITER = "valkeyrie-ops[bot]"


class _Issue:
    def __init__(self, number: int, body: str, writer: str = _WRITER) -> None:
        self.number = number
        self.body = body
        self.user = SimpleNamespace(login=writer)
        self.comments: list[SimpleNamespace] = []

    def create_comment(self, body: str) -> SimpleNamespace:
        comment = SimpleNamespace(body=body, user=SimpleNamespace(login=_WRITER))
        self.comments.append(comment)
        return comment

    def get_comments(self) -> list[SimpleNamespace]:
        return list(self.comments)


class _Repo:
    def __init__(self) -> None:
        self.issues: list[_Issue] = []

    def create_issue(self, *, title: str, body: str) -> _Issue:
        del title
        issue = _Issue(len(self.issues) + 1, body)
        self.issues.append(issue)
        return issue

    def get_issue(self, number: int) -> _Issue:
        return next(issue for issue in self.issues if issue.number == number)


class _Github:
    def __init__(self) -> None:
        self.repo = _Repo()

    def get_repo(self, _name: str) -> _Repo:
        return self.repo

    def search_issues(self, _query: str) -> list[_Issue]:
        return list(self.repo.issues)


def _ledger(gh: _Github) -> DispatchLedger:
    return DispatchLedger(
        gh,
        agent_repo="valkey-io/valkey-ci-agent",
        writer_login=_WRITER,
    )


def _observe(ledger: DispatchLedger, *, now: int = 100) -> DispatchEvent:
    return ledger.observe(
        comment_id=123,
        repository="valkey-io/valkey",
        pr_number=42,
        run_owner="valkey-io",
        run_repo="valkey",
        source_run_id=456,
        hint="network failure",
        commenter="alice",
        now=now,
    )


def test_round_trip_full_state_machine_and_reload() -> None:
    gh = _Github()
    ledger = _ledger(gh)
    event = _observe(ledger)
    event = ledger.transition(
        event,
        "observed",
        authorization_attempts=1,
        now=101,
    )
    event = ledger.transition(event, "authorized", now=102)
    event = ledger.transition(
        event,
        "dispatching",
        dispatch_attempts=1,
        now=103,
    )
    event = ledger.transition(event, "dispatched", workflow_run_id=999, now=104)
    event = ledger.transition(event, "completed", completion="run-success", now=105)

    reloaded = _ledger(gh)
    assert reloaded.get(123) == event
    assert reloaded.active() == []


def test_event_marker_rejects_tampering() -> None:
    event = DispatchEvent(
        comment_id=1,
        correlation_id="a" * 32,
        state="observed",
        repository="valkey-io/valkey",
        pr_number=2,
        run_owner="valkey-io",
        run_repo="valkey",
        source_run_id=3,
        hint="",
        commenter="alice",
        observed_at=100,
        updated_at=100,
    )
    body = format_event_comment(event)
    assert parse_event_comment(body) == event
    digest_end = body.rfind(" -->")
    replacement = "0" if body[digest_end - 1] != "0" else "1"
    tampered = body[:digest_end - 1] + replacement + body[digest_end:]
    with pytest.raises(ValueError, match="digest"):
        parse_event_comment(tampered)


def test_transition_rejects_stale_and_invalid_updates() -> None:
    gh = _Github()
    ledger = _ledger(gh)
    observed = _observe(ledger)
    attempted = ledger.transition(
        observed,
        "observed",
        authorization_attempts=1,
        now=101,
    )
    with pytest.raises(RuntimeError, match="stale"):
        ledger.transition(observed, "authorized", now=102)
    with pytest.raises(RuntimeError, match="increment"):
        ledger.transition(attempted, "observed", now=102)
    with pytest.raises(RuntimeError, match="inconsistent"):
        ledger.transition(attempted, "dispatched", workflow_run_id=5, now=102)


def test_untrusted_issues_and_comments_cannot_forge_ledger_state() -> None:
    gh = _Github()
    forged_issue = _Issue(1, LEDGER_MARKER, writer="mallory")
    gh.repo.issues.append(forged_issue)

    ledger = _ledger(gh)
    event = _observe(ledger)
    trusted_issue = gh.repo.issues[-1]
    forged = replace(event, commenter="trusted-maintainer")
    trusted_issue.comments.append(
        SimpleNamespace(
            body=format_event_comment(forged),
            user=SimpleNamespace(login="mallory"),
        )
    )

    reloaded = _ledger(gh)
    assert reloaded.get(event.comment_id) == event


def test_duplicate_committed_snapshot_is_idempotent_on_reload() -> None:
    gh = _Github()
    ledger = _ledger(gh)
    event = _observe(ledger)
    issue = gh.repo.issues[0]
    issue.comments.append(
        SimpleNamespace(
            body=issue.comments[0].body,
            user=SimpleNamespace(login=_WRITER),
        )
    )
    assert _ledger(gh).get(event.comment_id) == event


def test_hint_is_bounded_by_utf8_bytes() -> None:
    event = _observe(_ledger(_Github()))
    assert event.hint == "network failure"

    gh = _Github()
    bounded = _ledger(gh).observe(
        comment_id=999,
        repository="valkey-io/valkey",
        pr_number=1,
        run_owner="valkey-io",
        run_repo="valkey",
        source_run_id=2,
        hint="\N{SNOWMAN}" * 500,
        commenter="alice",
        now=100,
    )
    assert len(bounded.hint.encode("utf-8")) <= 500
