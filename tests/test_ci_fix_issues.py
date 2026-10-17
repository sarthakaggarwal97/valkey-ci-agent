"""Focused tests for the detector issue front door."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.ci_fix import issues as issues_mod
from scripts.ci_fix.github_runs import FailedJob
from scripts.ci_fix.issues import IssueFailure, parse_issue
from scripts.ci_fix.models import (
    CandidateAction,
    FailedTest,
    FixCandidate,
    FixPlan,
    FixResult,
    FrontDoor,
    ResultKind,
    TargetKind,
)
from scripts.test_failure_detector import issue_renderer
from scripts.test_failure_detector.job_failures import JobFailure
from scripts.test_failure_detector.parse_failures import JobReference, UniqueFailure

_BOT = "valkeyrie-ops[bot]"
_BASE = "b" * 40
_FAILING = "f" * 40


# The issue flow in one process, the engine stubbed: the plan job's
# validation, then a ``run_fix`` that prepares and publishes (the fix and
# publish jobs), then the report on the issue.
_ENGINE: dict = {}


def _raw(*jobs):
    """The jobs as GitHub lists them: failed, with their names and ids."""
    return tuple(SimpleNamespace(name=job.name, id=job.id, conclusion="failure") for job in jobs)


def run_issue_fix(gh, artifact_client, repo_full_name, *, issue_number, bot_login, git_env, ignored_jobs=()):
    prepared = issues_mod.prepare_issue(
        gh, artifact_client, repo_full_name,
        issue_number=issue_number, bot_login=bot_login, ignored_jobs=ignored_jobs,
    )
    if not isinstance(prepared, FixPlan):
        return prepared
    result = _ENGINE["run_fix"](gh, prepared.task, artifact_client, git_env)
    return issues_mod.report_result(gh, prepared, result)


def _last_key(run_id: int) -> str:
    return f"<!-- {issue_renderer.MARKER_NAMESPACE}:last-key:{run_id} -->"


def _test_issue_body(
    *,
    run_id: int = 100,
    name: str = "TTL expiration",
    file: str = "tests/integration/rdb.tcl",
    occurrences: int = 1,
) -> str:
    failure = UniqueFailure(
        test_name=name,
        test_file=file,
        error="boom",
        jobs=[JobReference("test-ubuntu-jemalloc", "https://job")],
    )
    marker = (
        f"<!-- {issue_renderer.MARKER_NAMESPACE}:"
        f"{issue_renderer.fingerprint_for(failure)} -->"
    )
    body = issue_renderer.renderer_for(failure).render(
        marker,
        occurrences,
    ).body
    return f"{body}\n{_last_key(run_id)}"


def _job_issue_body(
    *,
    run_id: int = 100,
    job: str = "test-valgrind-test (unit)",
) -> str:
    failure = JobFailure(
        job=job,
        url="https://job",
        step="test",
        summary=("[err]: Valgrind error",),
    )
    marker = (
        f"<!-- {issue_renderer.MARKER_NAMESPACE}:"
        f"{issue_renderer.job_fingerprint_for(failure)} -->"
    )
    body = issue_renderer.render_job_issue(failure, marker, 1).body
    return f"{body}\n{_last_key(run_id)}"


def _issue(
    *,
    number: int = 7,
    body: str | None = None,
    state: str = "open",
    author: str = _BOT,
    assignees: tuple[str, ...] = (),
) -> SimpleNamespace:
    issue = SimpleNamespace(
        number=number,
        body=body if body is not None else _test_issue_body(),
        state=state,
        user=SimpleNamespace(login=author),
        assignees=[SimpleNamespace(login=name) for name in assignees],
        timeline=[],
        comments=[],
        created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )
    issue.get_timeline = lambda: list(issue.timeline)

    def create_comment(body: str) -> SimpleNamespace:
        comment = SimpleNamespace(body=body, id=900 + len(issue.comments))
        issue.comments.append(comment)
        return comment

    issue.create_comment = create_comment
    return issue


def _linked_pr_event() -> SimpleNamespace:
    source = SimpleNamespace(pull_request=object(), state="open")
    return SimpleNamespace(
        event="cross-referenced",
        source=SimpleNamespace(issue=source),
    )


def _gh(
    issue: SimpleNamespace,
    *,
    run_attempt: int = 1,
    event: str = "schedule",
    path: str = ".github/workflows/daily.yml",
) -> tuple[MagicMock, MagicMock, SimpleNamespace]:
    run = SimpleNamespace(
        id=100,
        run_attempt=run_attempt,
        event=event,
        path=path,
        head_repository=SimpleNamespace(full_name="o/r"),
        head_branch="unstable",
        head_sha=_FAILING,
    )
    repo = MagicMock()
    repo.get_issue.return_value = issue
    repo.get_workflow_run.return_value = run
    repo.get_pulls.return_value = []

    def get_branch(name: str):
        if name == "unstable":
            return SimpleNamespace(commit=SimpleNamespace(sha=_BASE))
        raise GithubException(404, {"message": "Not Found"})

    repo.get_branch.side_effect = get_branch
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh, repo, run


def _run(
    monkeypatch: pytest.MonkeyPatch,
    issue: SimpleNamespace,
    *,
    jobs: tuple[FailedJob, ...] = (
        FailedJob("test-ubuntu-jemalloc", 22),
    ),
    run_attempt: int = 1,
    result_kind: ResultKind = ResultKind.OPENED,
) -> tuple[dict, list]:
    gh, _repo, _source_run = _gh(issue, run_attempt=run_attempt)
    monkeypatch.setattr(issues_mod, "_failing_jobs", lambda *args, **kwargs: jobs)
    seen = []

    def engine(_gh, task, _artifact_client, _git_env, publish_func=None):
        assert issue.comments == []  # no claim is posted before the long agent run
        seen.append(task)
        return FixResult(
            kind=result_kind,
            summary="Opened draft PR #55 for review and normal CI.",
            task=task,
            pr_number=55 if result_kind is ResultKind.OPENED else 0,
            pr_url="https://github.com/o/r/pull/55"
            if result_kind is ResultKind.OPENED
            else "",
        )

    monkeypatch.setitem(_ENGINE, "run_fix", engine)
    result = run_issue_fix(
        gh,
        MagicMock(),
        "o/r",
        issue_number=7,
        bot_login=_BOT,
        git_env={"GIT_ASKPASS": "x"},
    )
    return result, seen


def test_test_failure_issue_parses_its_run_and_test():
    current = parse_issue(_issue(body=_test_issue_body(run_id=321)))
    assert current is not None
    assert (current.run_id, current.test_name, current.test_file) == (
        321,
        "TTL expiration",
        "tests/integration/rdb.tcl",
    )
    assert current.fingerprint


def test_job_failure_issue_preserves_its_detector_identity():
    failure = JobFailure(
        job="test-valgrind-test (unit)",
        url="https://job",
        step="test",
        summary=("[err]: Valgrind error",),
    )
    parsed = parse_issue(_issue(body=_job_issue_body()))
    assert parsed is not None
    assert parsed.job_name == "test-valgrind-test (unit)"
    assert parsed.fingerprint == issue_renderer.job_fingerprint_for(failure)


def test_non_detector_body_is_not_parsed():
    assert parse_issue(_issue(body="Please fix this test")) is None
    body = _test_issue_body().replace(_last_key(100), "")
    assert parse_issue(_issue(body=body)) is None


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda issue: setattr(issue, "state", "closed"), "not open"),
        (lambda issue: setattr(issue, "assignees", [SimpleNamespace(login="alice")]), "assigned"),
        (lambda issue: issue.timeline.append(_linked_pr_event()), "open linked"),
        (lambda issue: setattr(issue.user, "login", "someone"), "not filed"),
    ],
)
def test_ineligible_issue_is_skipped_without_comment_or_agent(
    monkeypatch,
    mutate,
    reason,
):
    issue = _issue()
    mutate(issue)
    gh, _repo, _run_ = _gh(issue)
    engine = MagicMock()
    monkeypatch.setitem(_ENGINE, "run_fix", engine)

    result = run_issue_fix(
        gh,
        MagicMock(),
        "o/r",
        issue_number=7,
        bot_login=_BOT,
        git_env={},
    )

    assert result["action"] == "skipped"
    assert reason in result["reason"]
    assert issue.comments == []
    engine.assert_not_called()


def test_legacy_detector_issue_remains_eligible_before_cutover():
    issue = _issue(author="github-actions[bot]")
    issue.created_at = datetime(2026, 6, 20, tzinfo=timezone.utc)
    failure, reason = issues_mod.validate_issue(issue, bot_login=_BOT)
    assert failure is not None and reason == ""


def test_ready_fix_uses_exact_job_and_opens_draft_in_target_repo(
    monkeypatch,
):
    issue = _issue(body=_test_issue_body())
    jobs = (
        FailedJob("test-valgrind-test (unit)", 44),
        FailedJob("test-ubuntu-jemalloc", 55),
    )
    result, seen = _run(
        monkeypatch,
        issue,
        jobs=jobs,
        run_attempt=3,
    )

    (task,) = seen
    assert (
        task.failure.run_id,
        task.failure.run_attempt,
        task.failure.head_sha,
        task.failure.job_id,
        task.failure.job_name,
    ) == (100, 3, _FAILING, 44, "test-valgrind-test (unit)")
    assert task.failure.other_failed_jobs == ("test-ubuntu-jemalloc",)
    assert task.failure.issue_number == 7
    assert "source run tested" in task.failure.description.lower()
    assert task.target.kind is TargetKind.DRAFT_PR
    assert task.target.repo_full_name == "o/r"
    assert task.target.base_repo_full_name == "o/r"
    assert task.target.branch == "agent/ci-fix/issue-7-100"
    assert task.target.expected_sha == _BASE
    assert task.failure.base_branch == "unstable"
    assert result["action"] == "opened"
    assert result["pr"] == 55
    assert len(issue.comments) == 1
    assert "Normal pull-request CI and human review are authoritative" in issue.comments[0].body
    assert "verified" not in issue.comments[0].body.lower()


def test_ignored_artifact_job_is_not_selected(monkeypatch):
    ignored = FailedJob("notify-about-job-results", 11)
    selected = FailedJob("test-ubuntu-jemalloc", 22)
    monkeypatch.setattr(
        issues_mod,
        "jobs_for_run_attempt",
        lambda *args, **kwargs: _raw(ignored, selected),
    )
    record = {
        "valkey": [
            {
                "test_name": "TTL expiration",
                "test_file": "tests/integration/rdb.tcl",
                "error": "x",
                "status": "err",
            },
        ],
    }
    artifact = {
        ignored.name: record,
        selected.name: record,
    }
    monkeypatch.setattr(
        issues_mod,
        "download_all_test_failures",
        lambda *args, **kwargs: json.dumps(artifact).encode(),
    )
    monkeypatch.setattr(
        issues_mod,
        "find_unrecorded_failures",
        lambda *args, **kwargs: ([], []),
    )

    jobs = issues_mod._failing_jobs(
        MagicMock(),
        MagicMock(),
        "o/r",
        SimpleNamespace(id=100),
        IssueFailure(
            7,
            100,
            "TTL expiration",
            "tests/integration/rdb.tcl",
        ),
        ("notify-about-job-results*",),
        issues_mod.Source.DAILY,
    )

    assert jobs == (selected,)


def test_log_only_failure_keeps_exact_job_id(monkeypatch):
    selected = FailedJob("test-ubuntu-32bit", 456)
    snapshot = _raw(selected)
    observed_attempts = []
    observed_snapshots = []

    def load_jobs(*args, **kwargs):
        observed_attempts.append(kwargs["run_attempt"])
        return snapshot

    monkeypatch.setattr(
        issues_mod,
        "jobs_for_run_attempt",
        load_jobs,
    )
    monkeypatch.setattr(
        issues_mod,
        "download_all_test_failures",
        lambda *args, **kwargs: None,
    )
    from_log = UniqueFailure(
        "Throttling tears down",
        "tests/integration/throttle-repl.tcl",
        "[TIMEOUT]",
        [JobReference("test-ubuntu-32bit")],
    )
    monkeypatch.setattr(
        issues_mod,
        "find_unrecorded_failures",
        lambda *args, **kwargs: (
            observed_snapshots.append(kwargs["jobs"]) or [from_log],
            [],
        ),
    )

    jobs = issues_mod._failing_jobs(
        MagicMock(),
        MagicMock(),
        "o/r",
        SimpleNamespace(id=100, run_attempt=4),
        IssueFailure(
            7,
            100,
            "Throttling tears down",
            "tests/integration/throttle-repl.tcl",
        ),
        (),
        issues_mod.Source.DAILY,
    )

    assert jobs == (selected,)
    assert jobs[0].id == 456
    assert observed_attempts == [4]
    assert observed_snapshots == [snapshot]


def test_no_matching_job_records_one_terminal_refusal(monkeypatch):
    issue = _issue()
    gh, _repo, _run_ = _gh(issue)
    monkeypatch.setattr(issues_mod, "_failing_jobs", lambda *args, **kwargs: ())
    engine = MagicMock()
    monkeypatch.setitem(_ENGINE, "run_fix", engine)

    result = run_issue_fix(
        gh,
        MagicMock(),
        "o/r",
        issue_number=7,
        bot_login=_BOT,
        git_env={},
    )

    assert result["action"] == "refused"
    assert "no failed job" in result["summary"]
    assert len(issue.comments) == 1
    assert "No draft PR was opened" in issue.comments[0].body
    engine.assert_not_called()


def test_orphan_branch_is_reconciled_before_job_artifacts_are_read(monkeypatch):
    issue = _issue()
    gh, _repo, _run_ = _gh(issue)
    jobs = MagicMock(side_effect=AssertionError("job artifacts should not be read"))
    engine = MagicMock()
    monkeypatch.setattr(issues_mod, "_failing_jobs", jobs)
    monkeypatch.setitem(_ENGINE, "run_fix", engine)

    def reconcile(_gh, task, **_kwargs):
        return FixResult(
            kind=ResultKind.OPENED,
            summary="Recovered draft PR #55.",
            task=task,
            commit_sha="b" * 40,
            pr_number=55,
            pr_url="https://github.com/o/r/pull/55",
        )

    monkeypatch.setattr(issues_mod, "reconcile_draft_pr", reconcile)

    result = run_issue_fix(
        gh,
        MagicMock(),
        "o/r",
        issue_number=7,
        bot_login=_BOT,
        git_env={},
    )

    assert result["action"] == "opened"
    assert result["pr"] == 55
    jobs.assert_not_called()
    engine.assert_not_called()


@pytest.mark.parametrize(
    ("run_kwargs", "reason"),
    [
        ({"event": "pull_request"}, "triggered by pull_request, not the schedule"),
        ({"path": ".github/workflows/other.yml"}, "is not a scheduled run"),
    ],
)
def test_non_daily_source_run_is_refused_with_terminal_comment(
    monkeypatch,
    run_kwargs,
    reason,
):
    issue = _issue()
    gh, _repo, _run_ = _gh(issue, **run_kwargs)
    engine = MagicMock()
    monkeypatch.setitem(_ENGINE, "run_fix", engine)

    result = run_issue_fix(
        gh,
        MagicMock(),
        "o/r",
        issue_number=7,
        bot_login=_BOT,
        git_env={},
    )

    assert result["action"] == "refused"
    assert reason in result["summary"]
    assert len(issue.comments) == 1
    engine.assert_not_called()


def test_job_level_issue_reaches_shared_engine(monkeypatch):
    issue = _issue(body=_job_issue_body())
    result, seen = _run(
        monkeypatch,
        issue,
        jobs=(FailedJob("test-valgrind-test (unit)", 77),),
    )
    task = seen[0]
    assert result["action"] == "opened"
    assert task.failure.job_id == 77
    assert "without a recorded test failure" in task.failure.description


def test_branch_name_is_stable_for_the_exact_detector_occurrence():
    assert issues_mod._branch_name(IssueFailure(7, 100)) == "agent/ci-fix/issue-7-100"


def test_explicit_positive_issue_number_is_required():
    with pytest.raises(ValueError):
        run_issue_fix(
            MagicMock(),
            MagicMock(),
            "o/r",
            issue_number=0,
            bot_login=_BOT,
            git_env={},
        )


# --- Weekly: one issue per release branch, fixed on that branch --------------

_WEEKLY_JOB = "run-daily-for-release-branches (8.1) / test-ubuntu-jemalloc"
_RELEASE_BASE = "c" * 40
_RELEASE_TESTED = "d" * 40


def _weekly_issue_body() -> str:
    failure = UniqueFailure(
        test_name="TTL expiration",
        test_file="tests/integration/rdb.tcl",
        error="boom",
        jobs=[JobReference(_WEEKLY_JOB, "https://job")],
        branch="8.1",
    )
    marker = f"<!-- {issue_renderer.MARKER_NAMESPACE}:{issue_renderer.fingerprint_for(failure)} -->"
    body = issue_renderer.renderer_for(failure).render(marker, 1).body
    return f"{body}\n{_last_key(100)}"


def test_a_weekly_issue_is_fixed_on_the_release_branch_its_job_tested(monkeypatch):
    issue = _issue(body=_weekly_issue_body())
    gh, repo, _run_ = _gh(issue, path=".github/workflows/weekly.yml")
    branches = {"8.1": _RELEASE_BASE}
    repo.get_branch.side_effect = lambda name: (
        SimpleNamespace(commit=SimpleNamespace(sha=branches[name])) if name in branches
        else (_ for _ in ()).throw(GithubException(404, {"message": "Not Found"}))
    )
    selected = []
    monkeypatch.setattr(
        issues_mod, "_failing_jobs",
        lambda *args, **kwargs: selected.append(args[-1]) or (FailedJob(_WEEKLY_JOB, 22),),
    )
    seen = []
    monkeypatch.setitem(_ENGINE, "run_fix", lambda _gh, task, *a, **k: seen.append(task) or FixResult(
        kind=ResultKind.OPENED, summary="opened", task=task, pr_number=55, pr_url="https://pr/55",
    ))
    artifacts = MagicMock()
    artifacts.download_job_log.return_value = (
        "2026-10-11T06:13:15.9495435Z Syncing repository: o/r\n"
        "2026-10-11T06:13:17.6919192Z [command]/usr/bin/git log -1 --format=%H\n"
        f"2026-10-11T06:13:17.6939064Z {_RELEASE_TESTED}\n"
    )

    result = run_issue_fix(gh, artifacts, "o/r", issue_number=7, bot_login=_BOT, git_env={})

    assert result["action"] == "opened"
    assert selected == [issues_mod.Source.WEEKLY]
    task = seen[0]
    assert (task.failure.base_branch, task.target.expected_sha) == ("8.1", _RELEASE_BASE)
    assert task.failure.head_sha == _RELEASE_TESTED
    assert task.failure.source is issues_mod.Source.WEEKLY and task.failure.scheduled
    assert task.failure.failed_tests == (FailedTest("TTL expiration", "tests/integration/rdb.tcl"),)
    assert "Weekly job" in task.failure.description and "`8.1`" in task.failure.description


def test_a_weekly_issue_records_its_release_branch():
    parsed = parse_issue(_issue(body=_weekly_issue_body()))
    assert parsed is not None and parsed.branch == "8.1"
    assert parse_issue(_issue()).branch == ""


def test_a_daily_issue_passes_its_test_and_source_to_the_engine(monkeypatch):
    result, seen = _run(monkeypatch, _issue())
    task = seen[0]
    assert task.failure.source is issues_mod.Source.DAILY
    assert task.failure.failed_tests == (FailedTest("TTL expiration", "tests/integration/rdb.tcl"),)
    assert task.failure.base_branch == "unstable"


def _weekly_gh(issue, branches=None):
    gh, repo, run = _gh(issue, path=".github/workflows/weekly.yml")
    branches = {"8.1": _RELEASE_BASE} if branches is None else branches
    repo.get_branch.side_effect = lambda name: (
        SimpleNamespace(commit=SimpleNamespace(sha=branches[name])) if name in branches
        else (_ for _ in ()).throw(GithubException(404, {"message": "Not Found"}))
    )
    return gh, repo, run


def test_a_weekly_branch_left_by_an_earlier_attempt_is_recovered_before_logs_are_read(monkeypatch):
    issue = _issue(body=_weekly_issue_body())
    gh, _repo, _run_ = _weekly_gh(issue)
    monkeypatch.setattr(issues_mod, "_failing_jobs", MagicMock(side_effect=AssertionError("logs read")))
    seen = []

    def reconcile(_gh, task, **_kwargs):
        seen.append(task.failure.base_branch)
        return FixResult(kind=ResultKind.OPENED, summary="Recovered", task=task, pr_number=55, pr_url="u")

    monkeypatch.setattr(issues_mod, "reconcile_draft_pr", reconcile)

    result = run_issue_fix(gh, MagicMock(), "o/r", issue_number=7, bot_login=_BOT, git_env={})

    assert result["action"] == "opened" and seen == ["8.1"]


def test_a_weekly_issue_for_a_deleted_release_branch_is_refused(monkeypatch):
    issue = _issue(body=_weekly_issue_body())
    gh, _repo, _run_ = _weekly_gh(issue, branches={})
    monkeypatch.setitem(_ENGINE, "run_fix", MagicMock(side_effect=AssertionError("no fix")))

    result = run_issue_fix(gh, MagicMock(), "o/r", issue_number=7, bot_login=_BOT, git_env={})

    assert result["action"] == "refused"
    assert "branch `8.1` no longer exists" in result["summary"]
    assert len(issue.comments) == 1


def test_a_weekly_issue_resolves_to_its_job_through_the_detectors_own_parsing(monkeypatch):
    """Detector output -> rendered issue -> the front door's real job matching."""
    from scripts.test_failure_detector.job_failures import find_unrecorded_failures, split_by_branch

    def job(job_id, branch):
        return SimpleNamespace(
            id=job_id, name=f"run-daily-for-release-branches ({branch}) / test-ubuntu-jemalloc",
            conclusion="failure", html_url=f"https://job/{job_id}",
            steps=[SimpleNamespace(name="test", conclusion="failure")],
        )

    jobs = (job(31, "7.2"), job(32, "8.1"))
    summary = "*** [err]: TTL expiration in tests/integration/rdb.tcl\n"
    artifacts = MagicMock()
    artifacts.download_job_log.side_effect = lambda _repo, _job_id: summary
    tests, _job_failures = find_unrecorded_failures(artifacts, "o/r", {}, jobs=jobs)
    (filed,) = [f for f in split_by_branch(tests) if f.branch == "8.1"]
    marker = f"<!-- {issue_renderer.MARKER_NAMESPACE}:{issue_renderer.fingerprint_for(filed)} -->"
    body = issue_renderer.renderer_for(filed).render(marker, 1).body + "\n" + _last_key(100)
    failure = parse_issue(_issue(body=body))
    monkeypatch.setattr(issues_mod, "jobs_for_run_attempt", lambda *a, **k: jobs)
    _gh_, _repo, run = _weekly_gh(_issue(body=body))

    matched = issues_mod._failing_jobs(MagicMock(), artifacts, "o/r", run, failure, (), issues_mod.Source.WEEKLY)

    assert [j.id for j in matched] == [32]


def test_an_issue_from_a_manually_started_run_is_refused_with_why(monkeypatch):
    issue = _issue()
    gh, _repo, _run_ = _gh(issue, event="workflow_dispatch")
    monkeypatch.setitem(_ENGINE, "run_fix", MagicMock(side_effect=AssertionError("no fix")))

    result = run_issue_fix(gh, MagicMock(), "o/r", issue_number=7, bot_login=_BOT, git_env={})

    assert "Daily run triggered by workflow_dispatch" in result["summary"]


def test_the_issue_comment_lists_what_the_bot_checked(monkeypatch):
    issue = _issue()
    gh, _repo, _run_ = _gh(issue)
    monkeypatch.setattr(issues_mod, "_failing_jobs", lambda *a, **k: (FailedJob("test-ubuntu-jemalloc", 22),))
    checked = "Existing fixes found: `abc123` Deflake (#9) (port candidate)\nDid not reproduce with `./runtest`."
    monkeypatch.setitem(_ENGINE, "run_fix", lambda _gh, task, *a, **k: FixResult(
        kind=ResultKind.REFUSED, summary="no fix", task=task,
        candidate=FixCandidate(action=CandidateAction.REFUSE, verification=checked),
    ))

    run_issue_fix(gh, MagicMock(), "o/r", issue_number=7, bot_login=_BOT, git_env={})

    body = issue.comments[-1].body
    assert "**Checked by the bot:**" in body
    assert "- Existing fixes found: `abc123`" in body and "- Did not reproduce" in body


# --- other open detector issues the same patch might fix --------------------------

def _other_repo(issues):
    repo = SimpleNamespace()
    repo.get_issues = lambda **_kwargs: list(issues)
    return repo


def test_other_open_detector_issues_are_offered_to_the_engine() -> None:
    other = _issue(number=8, body=_test_issue_body(name="INCR fails", file="tests/unit/type/incr.tcl"))
    others = issues_mod._other_open_issues(_other_repo([_issue(number=7), other]), _BOT, exclude=7, branch="")
    assert [(item.number, item.test.name, item.test.file) for item in others] == [
        (8, "INCR fails", "tests/unit/type/incr.tcl"),
    ]


@pytest.mark.parametrize("issue", [
    _issue(number=8, author="someone"),
    _issue(number=8, body="no detector markers here"),
    _issue(number=8, body=_job_issue_body()),
])
def test_issues_without_one_verifiable_test_are_not_offered(issue) -> None:
    assert issues_mod._other_open_issues(_other_repo([issue]), _BOT, exclude=7, branch="") == ()


def test_a_pull_request_returned_by_the_issues_api_is_not_offered() -> None:
    pr = _issue(number=8)
    pr.pull_request = SimpleNamespace(html_url="https://pr")
    assert issues_mod._other_open_issues(_other_repo([pr]), _BOT, exclude=7, branch="") == ()


def test_a_listing_error_leaves_the_patch_claiming_only_its_own_issue() -> None:
    repo = SimpleNamespace()

    def boom(**_kwargs):
        raise RuntimeError("502")

    repo.get_issues = boom
    assert issues_mod._other_open_issues(repo, _BOT, exclude=7, branch="") == ()



def test_only_issues_about_the_same_branch_are_also_fixed_candidates():
    daily = _issue(number=8)
    weekly = _issue(number=9, body=_weekly_issue_body())

    repo = _other_repo([_issue(number=7), daily, weekly])

    assert [o.number for o in issues_mod._other_open_issues(repo, _BOT, exclude=7, branch="")] == [8]
    assert [o.number for o in issues_mod._other_open_issues(repo, _BOT, exclude=7, branch="8.1")] == [9]


# --- the issue's part in the shared run (scripts/ci_fix/run.py) ----------------

def issue_plan(job_name="test-ubuntu-jemalloc"):
    from scripts.ci_fix.models import Failure, FixPolicy, FixTarget, FixTask
    from scripts.common.valkey_ci import Source

    failure = IssueFailure(number=7, run_id=100, test_name="TTL expiration", test_file="tests/integration/rdb.tcl")
    task = FixTask(
        Failure(repo_full_name="valkey-io/valkey", run_id=100, head_sha=_FAILING, job_id=22, job_name=job_name,
                base_branch="unstable", issue_number=7, failed_tests=failure.failed_tests, source=Source.DAILY),
        FixTarget(kind=TargetKind.DRAFT_PR, repo_full_name="valkey-io/valkey", branch="agent/ci-fix/x",
                  expected_sha=_BASE),
        FixPolicy.GENERAL,
    )
    return FixPlan(front_door=FrontDoor.ISSUE, task=task, issue=failure)


def test_an_issue_is_planned_with_the_registrys_ignored_jobs(monkeypatch):
    seen = {}
    monkeypatch.setattr(issues_mod, "prepare_issue", lambda *a, **k: seen.update(a=a, k=k) or issue_plan())

    plan = issues_mod.plan_request(MagicMock(), None, {"repo": "valkey-io/valkey", "issue": 7}, bot_login=_BOT)

    assert plan == issue_plan()
    assert seen["k"]["issue_number"] == 7 and seen["k"]["ignored_jobs"]
