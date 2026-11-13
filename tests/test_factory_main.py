"""The factory's front door: a labelled issue in, a plan or a final result out."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FixCandidate,
    FixPlan,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    IssueBrief,
    IssueKind,
    IssuePublication,
    ResultKind,
    TargetKind,
)
from scripts.factory import main

_BOT = "valkeyrie-ops[bot]"
_NOW = datetime(2026, 11, 13, 6, 0, tzinfo=timezone.utc)
_TIP = "c" * 40


def _event(event, actor="alice", label=main.LABEL, at=_NOW, event_id=777):
    return SimpleNamespace(
        event=event, actor=SimpleNamespace(login=actor), created_at=at, id=event_id,
        raw_data={"label": {"name": label}} if event == "labeled" else {},
    )


def _comment(author, body, at=_NOW - timedelta(hours=1)):
    return SimpleNamespace(user=SimpleNamespace(login=author), body=body, created_at=at)


def _issue(number=9, *, state="open", labels=("bug",), assignees=(), timeline=None, comments=None, body="x", title="t",
           pull_request=None):
    issue = SimpleNamespace(
        number=number, state=state, title=title, body=body, pull_request=pull_request,
        labels=[SimpleNamespace(name=name) for name in labels],
        assignees=[SimpleNamespace(login=login) for login in assignees],
        comments=list(comments or []),
    )
    issue.get_timeline = lambda: list(timeline if timeline is not None else [_event("labeled")])
    issue.get_comments = lambda: list(issue.comments)
    issue.remove_from_labels = MagicMock()
    issue.create_comment = MagicMock(side_effect=lambda body: issue.comments.append(_comment(_BOT, body)))
    return issue


def _repo(issue, *, permissions=None, default_branch="unstable"):
    permissions = {"alice": "write", "bob": "read", "carol": "admin"} if permissions is None else permissions
    repo = SimpleNamespace(default_branch=default_branch)
    repo.get_issue = lambda number: issue
    repo.get_collaborator_permission = lambda login: permissions.get(login, "none")
    repo.get_branch = lambda name: SimpleNamespace(commit=SimpleNamespace(sha=_TIP))
    return repo


def _gh(repo):
    return SimpleNamespace(get_repo=lambda name: repo)


@pytest.fixture(autouse=True)
def _no_linked_pr_and_no_branch(monkeypatch):
    monkeypatch.setattr(main, "has_open_linked_pr", lambda issue: False)
    monkeypatch.setattr(main, "reconcile_draft_pr", lambda gh, task: None)


def _plan(issue, repo=None, push_repo=""):
    repo = repo or _repo(issue)
    return main.prepare_issue(_gh(repo), "o/r", issue_number=issue.number, bot_login=_BOT, push_repo=push_repo)


def test_a_committers_label_plans_a_fix_on_the_default_branch_tip():
    issue = _issue(comments=[_comment("alice", "repro: SET then GET"), _comment("bob", "me too"),
                             _comment("carol", "after the label", at=_NOW + timedelta(minutes=5))])

    plan = _plan(issue, push_repo="forks/valkey")

    assert isinstance(plan, FixPlan) and plan.front_door is FrontDoor.FACTORY
    assert plan.task.failure == Failure(
        repo_full_name="o/r", run_id=0, head_sha=_TIP, job_id=0, job_name="", base_branch="unstable",
        description="Issue #9: t", issue_number=9,
    )
    assert not plan.task.failure.in_ci and plan.task.failure.run_url == ""
    assert plan.task.target == FixTarget(
        kind=TargetKind.DRAFT_PR, repo_full_name="forks/valkey", base_repo_full_name="o/r",
        branch="agent/factory/issue-9-777", expected_sha=_TIP,
    )
    assert plan.task.policy is FixPolicy.GENERAL
    # The brief: only committers' comments, only up to the labelling.
    assert plan.brief is not None
    assert [(c.author, c.body) for c in plan.brief.comments] == [("alice", "repro: SET then GET")]
    assert (plan.brief.kind, plan.brief.crash_report, plan.brief.label_event_id, plan.brief.labelled_by) == (
        IssueKind.BUG, False, 777, "alice",
    )


def test_the_latest_labelling_is_the_request():
    issue = _issue(timeline=[
        _event("labeled", actor="bob", at=_NOW - timedelta(days=2), event_id=1),
        _event("unlabeled", at=_NOW - timedelta(days=1), event_id=2),
        _event("labeled", actor="alice", at=_NOW, event_id=3),
        _event("labeled", actor="bob", label="bug", at=_NOW + timedelta(hours=1), event_id=4),
    ])

    plan = _plan(issue)

    assert isinstance(plan, FixPlan) and plan.brief.label_event_id == 3 and plan.brief.labelled_by == "alice"


@pytest.mark.parametrize("labels, title, body, kind, crash", [
    (("bug",), "[BUG] lag too high", "XINFO...", IssueKind.BUG, False),
    (("enhancement",), "Add HGETEX", "please", IssueKind.FEATURE, False),
    (("enhancement", "bug"), "x", "y", IssueKind.BUG, False),
    (("bug",), "[CRASH] XRANGE aborts", "=== VALKEY BUG REPORT START ===", IssueKind.BUG, True),
    ((), "Server crashes on SET", "it dies", IssueKind.BUG, True),
])
def test_an_issue_is_a_bug_or_a_feature_and_may_report_a_crash(labels, title, body, kind, crash):
    assert main.classify(_issue(labels=labels, title=title, body=body)) == (kind, crash)


def test_a_label_from_someone_without_write_access_is_removed_with_the_reason():
    issue = _issue(timeline=[_event("labeled", actor="bob")])

    result = _plan(issue)

    assert result["action"] == "refused" and "@bob" in result["summary"]
    issue.remove_from_labels.assert_called_once_with(main.LABEL)
    body = issue.create_comment.call_args.args[0]
    assert body.startswith(f"Removed the `{main.LABEL}` label: the label was applied by @bob")
    assert "<!-- valkey-ci-agent:factory label=777 outcome=refused -->" in body


def test_an_assigned_issue_or_one_with_a_fix_in_progress_is_left_alone(monkeypatch):
    assigned = _issue(assignees=("dave",))
    assert "assigned to @dave" in _plan(assigned)["summary"]
    assigned.remove_from_labels.assert_called_once()

    linked = _issue()
    monkeypatch.setattr(main, "has_open_linked_pr", lambda issue: True)
    assert "open pull request already" in _plan(linked)["summary"]
    linked.remove_from_labels.assert_called_once()


@pytest.mark.parametrize("issue, reason", [
    (_issue(state="closed"), "not open"),
    (_issue(pull_request=object()), "is a pull request"),
    (_issue(timeline=[_event("labeled", label="bug")]), f"not labelled {main.LABEL}"),
])
def test_what_is_not_a_request_is_skipped_without_a_word(issue, reason):
    result = _plan(issue)

    assert result["action"] == "skipped" and reason in result["reason"]
    issue.create_comment.assert_not_called()


def test_a_labelling_the_bot_reported_on_is_done_and_a_new_labelling_is_not():
    issue = _issue(comments=[_comment(_BOT, "Opened draft PR #1.\n\n<!-- valkey-ci-agent:factory label=777 outcome=opened -->")])
    assert _plan(issue)["reason"].startswith("the bot is done with the labelling")

    again = _issue(timeline=[_event("labeled", event_id=778)], comments=list(issue.comments))
    assert isinstance(_plan(again), FixPlan)


def test_two_failed_attempts_at_a_labelling_end_it_and_a_stranger_cannot_fake_them():
    failed = "<!-- valkey-ci-agent:factory label=777 outcome=failed -->"
    once = _issue(comments=[_comment(_BOT, failed)])
    assert isinstance(_plan(once), FixPlan)
    twice = _issue(comments=[_comment(_BOT, failed), _comment(_BOT, failed)])
    assert _plan(twice)["action"] == "skipped"
    faked = _issue(comments=[_comment("mallory", "<!-- valkey-ci-agent:factory label=777 outcome=opened -->")])
    assert isinstance(_plan(faked), FixPlan)


def test_a_permission_that_cannot_be_read_is_not_write_access():
    repo = SimpleNamespace(get_collaborator_permission=MagicMock(side_effect=RuntimeError("503")))
    assert main.is_committer(repo, "alice") is False
    assert main.is_committer(_repo(None), "valkeyrie-ops[bot]") is False
    assert main.is_committer(_repo(None), "carol") is True


def test_a_branch_an_earlier_attempt_left_is_recovered_and_reported(monkeypatch):
    issue = _issue()
    recovered = FixResult(kind=ResultKind.OPENED, summary="Opened draft PR #5.", task=MagicMock(), pr_number=5, pr_url="u")
    monkeypatch.setattr(main, "reconcile_draft_pr", lambda gh, task: recovered)

    result = _plan(issue)

    assert result["action"] == "opened" and result["pr"] == 5
    assert "Opened draft PR [#5](u)" in issue.create_comment.call_args.args[0]


def _brief(**kwargs) -> IssueBrief:
    return IssueBrief(number=9, title="t", body="b", label_event_id=777, labelled_by="alice", **kwargs)


def _task() -> FixTask:
    return FixTask(
        Failure("o/r", 0, _TIP, 0, "", base_branch="unstable", issue_number=9),
        FixTarget(TargetKind.DRAFT_PR, "o/r", "o/r", "agent/factory/issue-9-777", _TIP), FixPolicy.GENERAL,
    )


def _report(result: FixResult):
    issue = _issue()
    plan = FixPlan(front_door=FrontDoor.FACTORY, task=_task(), brief=_brief())
    record = main.report_result(_gh(_repo(issue)), plan, result)
    return record, issue.create_comment


def test_an_opened_pull_request_is_reported_on_the_issue_with_what_was_checked():
    candidate = FixCandidate(CandidateAction.PATCH, verification="The test fails 3 of 3.\nWith the fix, it passes.")
    record, comment = _report(FixResult(ResultKind.OPENED, "opened", _task(), candidate, pr_number=12, pr_url="https://pr"))

    body = comment.call_args.args[0]
    assert body.startswith("Opened draft PR [#12](https://pr): a test that demonstrates the issue")
    assert "- The test fails 3 of 3." in body and body.rstrip().endswith("label=777 outcome=opened -->")
    assert record == {
        "repo": "o/r", "issue": 9, "label_event": 777, "branch": "agent/factory/issue-9-777", "action": "opened",
        "summary": "opened", "pr": 12, "pr_url": "https://pr",
    }


def test_a_patch_posted_on_the_issue_carries_the_test_and_the_fix():
    candidate = FixCandidate(
        CandidateAction.PATCH, patch="diff --git a/x b/x\n+fix", evidence_patch="diff --git a/t b/t\n+test",
        root_cause="the cause", reasoning="the fix", verification="checked",
    )
    _record, comment = _report(FixResult(ResultKind.SUGGESTED, "patch", _task(), candidate))

    body = comment.call_args.args[0]
    assert body.startswith("A tested fix, as a patch: the test fails without the change and passes with it.")
    assert "**Root cause:** the cause" in body and "<summary>The test and the fix</summary>" in body
    assert "+fix" in body and body.rstrip().endswith("label=777 outcome=suggested -->")


def test_a_refusal_with_a_test_leaves_the_test_for_whoever_picks_the_issue_up():
    candidate = FixCandidate(CandidateAction.REFUSE, summary="no fix held up", evidence_patch="diff\n+test",
                             verification="The test fails 3 of 3.")
    _record, comment = _report(FixResult(ResultKind.REFUSED, "no fix held up", _task(), candidate))

    body = comment.call_args.args[0]
    assert body.startswith("No fix was opened. No fix held up.")
    assert "<summary>The failing test</summary>" in body and "+test" in body
    assert "**What the bot checked**" in body and "label=777 outcome=refused -->" in body


def test_a_failed_attempt_posts_nothing():
    _record, comment = _report(FixResult(ResultKind.FAILED, "crashed", _task()))
    comment.assert_not_called()


@pytest.mark.parametrize("value, expected", [
    ("", IssuePublication.COMMENT), ("off", IssuePublication.COMMENT), ("comment", IssuePublication.COMMENT),
    ("draft-pr", IssuePublication.DRAFT_PR), ("Draft-PR", IssuePublication.DRAFT_PR),
])
def test_the_rollout_step_decides_how_a_fix_is_published(monkeypatch, value, expected):
    monkeypatch.setenv(main.STEP_ENV, value)
    assert main.publication() is expected


def test_an_unknown_step_is_an_error(monkeypatch):
    monkeypatch.setenv(main.STEP_ENV, "merge")
    with pytest.raises(ValueError, match="FACTORY_STEP must be"):
        main.publication()
