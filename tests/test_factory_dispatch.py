"""The label queue: which labelled issues get a run, and when the bot stops itself."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.ci_fix.models import IssuePublication
from scripts.factory import dispatch as dispatch_mod
from scripts.factory import main as main_mod
from tests.test_factory_main import _BOT, _comment, _event, _issue

_NOW = datetime(2026, 11, 13, 6, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _no_linked_pr(monkeypatch):
    monkeypatch.setattr(dispatch_mod, "has_open_linked_pr", lambda issue: False)


def _labelled(number, event_id=None, **kwargs):
    issue = _issue(number=number, timeline=[_event("labeled", event_id=event_id or number * 10)], **kwargs)
    return issue


def _pull(number, *, state="open", merged=False, closed_at=None, branch=None, author=_BOT):
    return SimpleNamespace(
        number=number, state=state, merged_at=_NOW if merged else None, closed_at=closed_at,
        head=SimpleNamespace(ref=branch or f"{main_mod.BRANCH_PREFIX}issue-{number}-1"),
        user=SimpleNamespace(login=author),
    )


def _target(issues, pulls=(), commits=()):
    return SimpleNamespace(
        get_issues=lambda **_kwargs: list(issues),
        get_pulls=lambda **_kwargs: list(pulls),
        get_commits=lambda: [SimpleNamespace(commit=SimpleNamespace(message=message)) for message in commits],
    )


def _workflow(*runs, failed=()):
    def get_runs(status=None):
        if status == "failure":
            return [SimpleNamespace(status="completed", conclusion="failure", display_title=title) for title in failed]
        return [SimpleNamespace(status=status_, display_title=title) for status_, title in runs]

    return SimpleNamespace(get_runs=get_runs)


def _dispatch(target, workflow=None, *, max_runs=2, max_open_prs=5, publication=IssuePublication.COMMENT):
    dispatched = []
    decisions = dispatch_mod.dispatch_labelled(
        target, workflow or _workflow(), bot_login=_BOT, max_runs=max_runs, max_open_prs=max_open_prs,
        publication=publication, dispatch=lambda issue, event_id: dispatched.append((issue, event_id)),
    )
    return dispatched, {d.issue: (d.action, d.reason) for d in decisions}


def test_labelled_issues_are_dispatched_newest_first_with_their_labelling_until_the_cap():
    target = _target([_labelled(9), _labelled(8), _labelled(7)])

    dispatched, decisions = _dispatch(target, _workflow(("in_progress", "Factory issue #5 (label 50)")), max_runs=3)

    assert dispatched == [(9, 90), (8, 80)]
    assert 7 not in decisions


def test_an_issue_with_an_active_run_an_assignee_or_a_fix_in_progress_waits(monkeypatch):
    target = _target([_labelled(9), _labelled(8, assignees=("dave",)), _labelled(7)])
    monkeypatch.setattr(dispatch_mod, "has_open_linked_pr", lambda issue: issue.number == 7)

    dispatched, decisions = _dispatch(target, _workflow(("queued", "Factory issue #9 (label 90)")), max_runs=5)

    assert dispatched == []
    assert decisions[9][1].endswith("is in progress")
    assert decisions[8][1] == "issue #8 is assigned"
    assert decisions[7][1] == "issue #7 already has an open linked pull request"


def test_a_labelling_the_bot_is_done_with_or_failed_twice_at_gets_no_run():
    done = _labelled(9, comments=[_comment(_BOT, "x <!-- valkey-ci-agent:factory label=90 outcome=suggested -->")])
    target = _target([done, _labelled(8)])
    workflow = _workflow(failed=("Factory issue #8 (label 80)", "Factory issue #8 (label 80)", "Factory issue #8 (label 70)"))

    dispatched, decisions = _dispatch(target, workflow, max_runs=5)

    assert dispatched == []
    assert decisions[9][1] == "the bot is done with labelling 90"
    assert decisions[8][1] == "2 attempts at labelling 80 failed"


def test_nothing_is_listed_when_the_cap_is_reached():
    def unlisted(**_kwargs):
        raise AssertionError("listed issues with no free run")

    target = SimpleNamespace(get_issues=unlisted)
    assert _dispatch(target, _workflow(("in_progress", "Factory issue #1")), max_runs=1) == ([], {})


def test_at_the_draft_pr_step_the_bots_pull_requests_can_pause_it():
    issues = [_labelled(9)]
    full = _target(issues, pulls=[_pull(n) for n in range(1, 6)])
    _dispatched, decisions = _dispatch(full, publication=IssuePublication.DRAFT_PR, max_open_prs=5)
    assert decisions[0] == ("paused", "5 factory pull requests are open (cap 5)")

    closed = [_pull(n, state="closed", closed_at=_NOW - timedelta(days=n)) for n in range(1, 4)]
    unmerged = _target(issues, pulls=closed)
    _dispatched, decisions = _dispatch(unmerged, publication=IssuePublication.DRAFT_PR)
    assert decisions[0][1] == "the last 3 factory pull requests (#1, #2, #3) were closed without merging"

    merged = _target(issues, pulls=[_pull(4, state="closed", merged=True, closed_at=_NOW), *closed[1:]],
                     commits=['Revert "Fix the lag (#4)"', "Something else (#9)"])
    _dispatched, decisions = _dispatch(merged, publication=IssuePublication.DRAFT_PR)
    assert decisions[0][1] == "factory pull request #4 was reverted"

    # Someone else's branch with the prefix, and a reverted commit of another PR, do not count.
    others = _target(issues, pulls=[_pull(n, author="mallory") for n in range(1, 9)], commits=['Revert "x (#77)"'])
    dispatched, _decisions = _dispatch(others, publication=IssuePublication.DRAFT_PR)
    assert dispatched == [(9, 90)]


def test_before_pull_requests_are_opened_nothing_pauses_the_bot():
    target = _target([_labelled(9)], pulls=[_pull(n) for n in range(1, 9)])
    dispatched, _decisions = _dispatch(target, publication=IssuePublication.COMMENT, max_open_prs=1)
    assert dispatched == [(9, 90)]


def test_the_run_name_names_the_issue_and_the_labelling():
    assert dispatch_mod.RUN_NAME.match("Factory issue #123 (label 456)").group("issue", "run") == ("123", "456")
    assert dispatch_mod.RUN_NAME.match("Factory issue #123").group("issue", "run") == ("123", None)
    assert dispatch_mod.RUN_NAME.match("CI fix issue #123") is None
