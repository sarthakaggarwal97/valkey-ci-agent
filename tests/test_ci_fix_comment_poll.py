from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.ci_fix import comment_poll
from scripts.ci_fix.comment_poll import (
    _DISPATCH_DISCOVERY_GRACE_SECONDS,
    dispatch_ci_fix,
    lookup_ci_fix_run,
    poll_once,
)
from scripts.ci_fix.dispatch_ledger import DispatchEvent, DispatchLedger

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"
_WRITER = "valkeyrie-ops[bot]"


class _Issue:
    def __init__(self, number: int, body: str = "", *, writer: str = _WRITER) -> None:
        self.number = number
        self.body = body
        self.pull_request = {"url": "..."}
        self.user = SimpleNamespace(login=writer)
        self.comments: list[SimpleNamespace] = []

    def get_comments(self):
        return list(self.comments)

    def create_comment(self, body: str):
        comment = SimpleNamespace(body=body, user=SimpleNamespace(login=_WRITER))
        self.comments.append(comment)
        return comment


class _AgentRepo:
    def __init__(self) -> None:
        self.issues: list[_Issue] = []

    def create_issue(self, *, title: str, body: str):
        del title
        issue = _Issue(len(self.issues) + 1, body)
        self.issues.append(issue)
        return issue

    def get_issue(self, number: int):
        return next(issue for issue in self.issues if issue.number == number)


class _TargetRepo:
    full_name = "valkey-io/valkey"

    def __init__(self, comments) -> None:
        self.comments = list(comments)

    def get_issues_comments(self, **_kwargs):
        return list(self.comments)

    def get_issue(self, number: int):
        comment = next(
            value
            for value in self.comments
            if value.issue_url.rstrip("/").endswith(f"/{number}")
        )
        return SimpleNamespace(
            pull_request={"url": "..."} if comment._is_pr else None,
        )


class _Github:
    def __init__(self, comments) -> None:
        self.target = _TargetRepo(comments)
        self.agent = _AgentRepo()

    def get_repo(self, name: str):
        if name == "valkey-io/valkey":
            return self.target
        if name == "valkey-io/valkey-ci-agent":
            return self.agent
        raise AssertionError(name)

    def search_issues(self, _query: str):
        return list(self.agent.issues)


def _comment(
    *,
    body: str,
    login: str = "alice",
    user_type: str = "User",
    comment_id: int = 1,
    is_pr: bool = True,
):
    reactions: list[str] = []
    return SimpleNamespace(
        id=comment_id,
        body=body,
        user=SimpleNamespace(login=login, type=user_type),
        issue_url="https://api.github.com/repos/valkey-io/valkey/issues/42",
        _is_pr=is_pr,
        reactions=reactions,
        create_reaction=lambda content: reactions.append(content),
    )


def _ledger(gh: _Github) -> DispatchLedger:
    return DispatchLedger(
        gh,
        agent_repo="valkey-io/valkey-ci-agent",
        writer_login=_WRITER,
    )


def _poll(
    gh: _Github,
    ledger: DispatchLedger,
    *,
    authorize=lambda _username: True,
    dispatch=None,
    lookup_run=lambda _event: None,
    react=lambda comment: comment.create_reaction("eyes"),
    now=100,
):
    dispatch = dispatch or MagicMock()
    count = poll_once(
        gh,
        target_repo="valkey-io/valkey",
        lookback_minutes=30,
        ledger=ledger,
        authorize=authorize,
        dispatch=dispatch,
        lookup_run=lookup_run,
        react=react,
        now=now,
    )
    return count, dispatch


def test_valid_command_is_recorded_before_dispatch() -> None:
    comment = _comment(body=f"@valkeyrie-ops fix {_RUN_URL}")
    gh = _Github([comment])
    ledger = _ledger(gh)
    count, dispatch = _poll(gh, ledger)

    assert count == 1
    event = ledger.get(1)
    assert event is not None
    assert event.state == "dispatching"
    assert event.authorization_attempts == 1
    assert event.dispatch_attempts == 1
    dispatch.assert_called_once_with(event)
    assert comment.reactions == ["eyes"]


def test_noncommands_bots_issues_and_unauthorized_users_do_not_dispatch() -> None:
    cases = [
        _comment(body="ordinary comment"),
        _comment(body=f"@valkeyrie-ops fix {_RUN_URL}", user_type="Bot"),
        _comment(body=f"@valkeyrie-ops fix {_RUN_URL}", is_pr=False),
    ]
    for comment in cases:
        gh = _Github([comment])
        ledger = _ledger(gh)
        count, dispatch = _poll(gh, ledger)
        assert count == 0
        dispatch.assert_not_called()

    gh = _Github([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    ledger = _ledger(gh)
    count, dispatch = _poll(gh, ledger, authorize=lambda _username: False)
    assert count == 0
    dispatch.assert_not_called()
    assert ledger.get(1).completion == "unauthorized"


def test_transient_authorization_retries_after_comment_leaves_lookback() -> None:
    gh = _Github([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    ledger = _ledger(gh)
    count, dispatch = _poll(
        gh,
        ledger,
        authorize=lambda _username: None,
        now=100,
    )
    assert count == 0
    dispatch.assert_not_called()
    assert ledger.get(1).state == "observed"

    gh.target.comments = []
    count, dispatch = _poll(gh, ledger, now=161)
    assert count == 1
    dispatch.assert_called_once()
    assert ledger.get(1).state == "dispatching"


def test_failed_dispatch_is_retried_from_ledger_without_source_comment() -> None:
    gh = _Github([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    ledger = _ledger(gh)

    def fail(_event):
        raise RuntimeError("dispatch transport failed")

    count, _dispatch = _poll(gh, ledger, dispatch=fail, now=100)
    assert count == 0
    assert ledger.get(1).state == "dispatching"
    assert ledger.get(1).dispatch_attempts == 1

    gh.target.comments = []
    count, dispatch = _poll(
        gh,
        ledger,
        now=100 + _DISPATCH_DISCOVERY_GRACE_SECONDS + 1,
    )
    assert count == 1
    dispatch.assert_called_once()
    assert ledger.get(1).dispatch_attempts == 2


def test_correlated_run_id_and_completion_are_reconciled() -> None:
    gh = _Github([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    ledger = _ledger(gh)
    _poll(gh, ledger, now=100)
    gh.target.comments = []

    queued = SimpleNamespace(id=900, status="queued", conclusion=None)
    count, _ = _poll(gh, ledger, lookup_run=lambda _event: queued, now=101)
    assert count == 0
    assert ledger.get(1).state == "dispatched"
    assert ledger.get(1).workflow_run_id == 900

    completed = SimpleNamespace(id=900, status="completed", conclusion="success")
    _poll(gh, ledger, lookup_run=lambda _event: completed, now=102)
    assert ledger.get(1).state == "completed"
    assert ledger.get(1).completion == "run-success"


def test_reaction_failure_is_display_only() -> None:
    gh = _Github([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    ledger = _ledger(gh)

    def fail_reaction(_comment):
        raise RuntimeError("reaction failed")

    count, dispatch = _poll(gh, ledger, react=fail_reaction)
    assert count == 1
    dispatch.assert_called_once()


def _event(**updates) -> DispatchEvent:
    values = {
        "comment_id": 1,
        "correlation_id": "a" * 32,
        "state": "dispatching",
        "repository": "valkey-io/valkey",
        "pr_number": 42,
        "run_owner": "valkey-io",
        "run_repo": "valkey",
        "source_run_id": 123,
        "hint": "",
        "commenter": "alice",
        "authorization_attempts": 1,
        "dispatch_attempts": 1,
        "observed_at": 100,
        "updated_at": 101,
    }
    values.update(updates)
    return DispatchEvent(**values)


def test_dispatcher_includes_exact_correlation_input() -> None:
    workflow = MagicMock()
    repo = MagicMock()
    repo.get_workflow.return_value = workflow
    gh = MagicMock()
    gh.get_repo.return_value = repo

    dispatch_ci_fix(
        gh,
        agent_repo="valkey-io/valkey-ci-agent",
        workflow="ci-fix.yml",
        ref="main",
    )(_event())

    ref, inputs = workflow.create_dispatch.call_args.args
    assert ref == "main"
    assert inputs["correlation_id"] == "a" * 32
    assert inputs["comment_id"] == "1"
    assert inputs["run_url"].endswith("/actions/runs/123")


def test_run_lookup_requires_exact_title_actor_event_and_recency() -> None:
    valid = SimpleNamespace(
        id=20,
        display_title=f"CI fix [dispatch:{'a' * 32}]",
        event="workflow_dispatch",
        actor=SimpleNamespace(login=_WRITER),
        created_at=datetime.fromtimestamp(110, tz=timezone.utc),
    )
    wrong_actor = SimpleNamespace(
        id=10,
        display_title=valid.display_title,
        event="workflow_dispatch",
        actor=SimpleNamespace(login="mallory"),
        created_at=valid.created_at,
    )
    wrong_title = SimpleNamespace(
        id=11,
        display_title=f"prefix {valid.display_title}",
        event="workflow_dispatch",
        actor=valid.actor,
        created_at=valid.created_at,
    )
    workflow = MagicMock()
    workflow.get_runs.return_value = [wrong_actor, wrong_title, valid]
    repo = MagicMock()
    repo.get_workflow.return_value = workflow
    gh = MagicMock()
    gh.get_repo.return_value = repo

    lookup = lookup_ci_fix_run(
        gh,
        agent_repo="valkey-io/valkey-ci-agent",
        workflow="ci-fix.yml",
        bot_login=_WRITER,
    )
    assert lookup(_event()) is valid


def test_pull_request_number_uses_passed_repo_and_confirms_pr() -> None:
    comment = _comment(body="x", comment_id=9)
    pr_repo = SimpleNamespace(
        get_issue=lambda _number: SimpleNamespace(pull_request={"url": "..."}),
    )
    issue_repo = SimpleNamespace(
        get_issue=lambda _number: SimpleNamespace(pull_request=None),
    )
    assert comment_poll._pull_request_number(pr_repo, comment) == 42
    assert comment_poll._pull_request_number(issue_repo, comment) is None


def test_bot_login_and_loop_environment_are_bounded(monkeypatch) -> None:
    monkeypatch.delenv("CI_FIX_POLL_BOT_LOGIN", raising=False)
    monkeypatch.setenv("CI_FIX_POLL_APP_SLUG", "valkeyrie-ops")
    assert comment_poll._bot_login() == _WRITER
    monkeypatch.setenv("CI_FIX_POLL_BOT_LOGIN", "custom-bot")
    assert comment_poll._bot_login() == "custom-bot"

    monkeypatch.delenv("CI_FIX_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv(
        "CI_FIX_POLL_DURATION_SECONDS",
        str(comment_poll._MAX_LOOP_SECONDS * 10),
    )
    assert comment_poll._poll_interval_seconds() == 0
    assert comment_poll._poll_duration_seconds() == comment_poll._MAX_LOOP_SECONDS
