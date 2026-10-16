from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from scripts.ci_fix import issue_dispatch, issues
from tests.test_ci_fix_issues import (
    _BOT,
    _issue,
    _job_issue_body,
    _linked_pr_event,
    _test_issue_body,
    _weekly_issue_body,
)


def _detector_issue(number, **kwargs):
    issue = _issue(number=number, **kwargs)
    issue.get_comments = lambda: list(issue.comments)
    return issue


def _reported(issue, run_id):
    failure = issues.parse_issue(issue)
    issues._terminal_comment(  # the comment the issue fix posts on finishing
        issue, replace(failure, run_id=run_id),
        repo_full_name="o/r", action="refused", summary="did not reproduce",
    )
    issue.comments[-1].user = SimpleNamespace(login=_BOT)


def _workflow(*runs):
    return SimpleNamespace(get_runs=lambda: [SimpleNamespace(status=status, display_title=title)
                                             for status, title in runs])


def _dispatch(listed, workflow, *, max_runs=3):
    dispatched = []
    decisions = issue_dispatch.dispatch_issues(
        SimpleNamespace(get_issues=lambda **_kwargs: listed), workflow,
        repo_full_name="o/r", bot_login=_BOT, max_runs=max_runs, dispatch=dispatched.append,
    )
    return dispatched, {d.issue: d.reason for d in decisions if d.action == "skipped"}


def test_eligible_issues_are_dispatched_newest_first_until_the_cap_counts_running_ones():
    workflow = _workflow(
        ("in_progress", "CI fix issue #9"), ("completed", "CI fix issue #8"), ("queued", "CI fix issue #1"),
    )

    dispatched, skipped = _dispatch([_detector_issue(n) for n in (9, 8, 7, 6)], workflow, max_runs=4)

    assert dispatched == [8, 7]  # two of four already active
    assert "in progress" in skipped[9]


def test_nothing_is_listed_when_the_cap_is_already_reached():
    def unlisted(**_kwargs):
        raise AssertionError("listed issues with no free run")

    decisions = issue_dispatch.dispatch_issues(
        SimpleNamespace(get_issues=unlisted), _workflow(("in_progress", "CI fix issue #9")),
        repo_full_name="o/r", bot_login=_BOT, max_runs=1, dispatch=lambda _n: None,
    )

    assert decisions == []


def test_issues_that_need_no_run_are_skipped_with_their_reason():
    linked = _detector_issue(5)
    linked.timeline.append(_linked_pr_event())
    tried = _detector_issue(4)
    _reported(tried, run_id=100)
    listed = [
        _detector_issue(8, body=_weekly_issue_body()),
        _detector_issue(7, body=_job_issue_body(job="run-daily-for-release-branches (8.1) / test-ubuntu-jemalloc")),
        _detector_issue(6, assignees=("someone",)),
        linked,
        tried,
        _detector_issue(3, author="someone"),
    ]

    dispatched, skipped = _dispatch(listed, _workflow())

    assert dispatched == []
    assert skipped == {
        8: "Weekly failures wait for the Weekly flow",
        7: "Weekly failures wait for the Weekly flow",
        6: "issue #6 is assigned",
        5: "issue #5 already has an open linked pull request",
        4: "the bot already reported an attempt for run 100",
    }  # #3 is not a detector issue at all


def test_a_newer_occurrence_than_the_one_reported_gets_another_run():
    issue = _detector_issue(7, body=_test_issue_body(run_id=200))
    _reported(issue, run_id=100)

    dispatched, _skipped = _dispatch([issue], _workflow())

    assert dispatched == [7]
