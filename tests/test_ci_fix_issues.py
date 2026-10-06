"""Tests for the Daily issue front door (scripts/ci_fix/issues.py)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import issues as issues_mod
from scripts.ci_fix.issues import (
    ATTEMPT_MARKER,
    IssueFailure,
    attempts_for,
    parse_issue,
    render_attempt,
    select_issue,
)
from scripts.test_failure_detector import issue_renderer
from scripts.test_failure_detector.job_failures import JobFailure
from scripts.test_failure_detector.parse_failures import JobReference, UniqueFailure

_BOT = "valkeyrie-ops[bot]"


def _test_issue_body(run_id=100, name="TTL expiration during forkless bgsave", file="tests/integration/rdb.tcl",
                     occurrences=1):
    failure = UniqueFailure(test_name=name, test_file=file, error="boom",
                            jobs=[JobReference("test-ubuntu-jemalloc", "valkey", "https://job")])
    marker = f"<!-- {issue_renderer.MARKER_NAMESPACE}:{issue_renderer.fingerprint_for(failure)} -->"
    body = issue_renderer.renderer_for(failure).render(marker, occurrences).body
    return body + f"\n<!-- {issue_renderer.MARKER_NAMESPACE}:last-key:{run_id} -->"


def _job_issue_body(run_id=100, job="test-valgrind-test (unit)"):
    failure = JobFailure(job=job, url="https://job", step="test", summary=("[err]: Valgrind error",))
    marker = f"<!-- {issue_renderer.MARKER_NAMESPACE}:{issue_renderer.job_fingerprint_for(failure)} -->"
    body = issue_renderer.job_renderer_for(failure).render(marker, 1).body
    return body + f"\n<!-- {issue_renderer.MARKER_NAMESPACE}:last-key:{run_id} -->"


def _comment(data=None, *, login=_BOT, body=None):
    text = body if body is not None else render_attempt({"v": 1, "state": "refused", "summary": "x", **(data or {})})
    return SimpleNamespace(user=SimpleNamespace(login=login), body=text, edit=MagicMock())


def _issue(number=7, body=None, comments=(), assignees=(), state="open", author=_BOT):
    issue = SimpleNamespace(
        number=number, body=body if body is not None else _test_issue_body(), state=state,
        assignees=list(assignees), user=SimpleNamespace(login=author), comments=list(comments),
        get_timeline=lambda: [], _rawData={},
    )
    issue.get_comments = lambda: list(issue.comments)

    def create_comment(text):
        comment = _comment(body=text)
        comment.id = 900 + len(issue.comments)
        comment.edit = lambda new, c=comment: setattr(c, "body", new)
        issue.comments.append(comment)
        return comment

    issue.create_comment = create_comment
    issue.get_comment = lambda cid: next(c for c in issue.comments if getattr(c, "id", None) == cid)
    return issue


# --- parse_issue ------------------------------------------------------------------

def test_a_test_failure_issue_is_parsed_from_the_detector_body():
    failure = parse_issue(_issue(body=_test_issue_body(run_id=321)))
    assert failure == IssueFailure(number=7, run_id=321, test_name="TTL expiration during forkless bgsave",
                                   test_file="tests/integration/rdb.tcl")


def test_a_job_failure_issue_is_parsed_from_its_job_marker():
    failure = parse_issue(_issue(body=_job_issue_body(job="test-valgrind-test (unit)")))
    assert failure is not None
    assert (failure.job_name, failure.test_name) == ("test-valgrind-test (unit)", "")
    assert failure.describe("x") == "the failure of job `x`, which recorded no test failure"


def test_issues_not_filed_by_the_detector_are_ignored():
    assert parse_issue(_issue(body="Please fix this test\n- Test name: `x`\n- Test file: `y`")) is None
    body = _test_issue_body().replace(f"<!-- {issue_renderer.MARKER_NAMESPACE}:last-key:100 -->", "")
    assert parse_issue(_issue(body=body)) is None


# --- attempts -----------------------------------------------------------------------

def test_attempt_data_round_trips_through_its_comment():
    data = {"v": 1, "run": 100, "state": "pending", "branch": "agent/ci-fix/issue-7-100",
            "plan": {"job": "j", "job_id": "j", "inputs": {}, "test_name": ""},
            "baseline": 1, "candidate": 2, "baseline_url": "https://b", "candidate_url": "https://c"}
    attempts = attempts_for(_issue(comments=[_comment(data)]), _BOT)
    assert len(attempts) == 1
    assert attempts[0].data == {"summary": "x", **data}
    assert attempts[0].state == "pending" and attempts[0].run == 100


def test_attempt_markers_from_other_users_or_garbage_are_ignored():
    forged = _comment({"run": 100}, login="someone")
    garbage = _comment(body=f"<!-- {ATTEMPT_MARKER} not-base64!! -->")
    assert attempts_for(_issue(comments=[forged, garbage]), _BOT) == []


def test_pending_attempt_comment_links_both_runs_and_the_branch():
    text = render_attempt({
        "v": 1, "state": "pending", "branch": "agent/ci-fix/issue-7-100", "run_url": "https://run",
        "plan": {"job": "test-ubuntu-jemalloc", "job_id": "test-ubuntu-jemalloc",
                 "inputs": {"test_args": "--single unit/x --loops 20 --fastfail"}, "test_name": "t"},
        "baseline": 1, "candidate": 2, "baseline_url": "https://b", "candidate_url": "https://c",
    })
    assert "`agent/ci-fix/issue-7-100`" in text
    assert "[without the fix](https://b)" in text and "[with the fix](https://c)" in text
    assert "--single unit/x --loops 20 --fastfail" in text


# --- select_issue -------------------------------------------------------------------

def _gh_with_issues(*issues):
    gh = MagicMock()
    gh.get_repo.return_value.get_issues.return_value = list(issues)
    return gh


def test_the_newest_failure_without_an_attempt_is_selected():
    older = _issue(number=1, body=_test_issue_body(run_id=100))
    newer = _issue(number=2, body=_test_issue_body(run_id=200, name="other"))
    picked = select_issue(_gh_with_issues(older, newer), "o/r", bot_login=_BOT)
    assert not isinstance(picked, str)
    assert picked[1].number == 2


def test_one_attempt_per_occurrence_and_three_in_total():
    tried = _issue(number=1, body=_test_issue_body(run_id=100), comments=[_comment({"run": 100})])
    exhausted = _issue(number=2, body=_test_issue_body(run_id=300, name="x"),
                       comments=[_comment({"run": r}) for r in (1, 2, 3)])
    assert select_issue(_gh_with_issues(tried, exhausted), "o/r", bot_login=_BOT) == "no-actionable-issue"
    # A new occurrence of the first issue makes it eligible again.
    recurred = _issue(number=1, body=_test_issue_body(run_id=101), comments=[_comment({"run": 100})])
    picked = select_issue(_gh_with_issues(recurred), "o/r", bot_login=_BOT)
    assert not isinstance(picked, str) and picked[1].run_id == 101


def test_a_maintainer_may_retry_the_same_occurrence_by_number():
    tried = _issue(number=1, body=_test_issue_body(run_id=100), comments=[_comment({"run": 100})])
    picked = select_issue(_gh_with_issues(tried), "o/r", bot_login=_BOT, issue_number=1)
    assert not isinstance(picked, str)


def test_owned_issues_are_left_alone():
    assigned = _issue(number=1, assignees=[SimpleNamespace(login="alice")])
    linked = _issue(number=2, body=_test_issue_body(run_id=200, name="x"))
    pr_source = SimpleNamespace(pull_request=object(), state="open")
    linked.get_timeline = lambda: [SimpleNamespace(event="cross-referenced", source=SimpleNamespace(issue=pr_source))]
    assert select_issue(_gh_with_issues(assigned, linked), "o/r", bot_login=_BOT) == "no-actionable-issue"


def test_nothing_new_starts_while_a_verification_is_in_flight():
    busy = _issue(number=1, comments=[_comment({"run": 100, "state": "pending"})])
    fresh = _issue(number=2, body=_test_issue_body(run_id=200, name="x"))
    assert select_issue(_gh_with_issues(busy, fresh), "o/r", bot_login=_BOT) == "verification-in-flight"


def test_selection_ignores_pull_requests_in_the_issue_listing():
    pr_like = _issue(number=9)
    pr_like._rawData = {"pull_request": {}}
    assert select_issue(_gh_with_issues(pr_like), "o/r", bot_login=_BOT) == "no-actionable-issue"


# --- prepare ---------------------------------------------------------------------------

import json  # noqa: E402

from scripts.ci_fix.daily_verify import DailyResult  # noqa: E402
from scripts.ci_fix.issues import prepare_issue_fix, publish_issue_fix, reconcile_attempts  # noqa: E402
from scripts.ci_fix.models import (  # noqa: E402
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    Policy,
    Publication,
    to_dict,
)
from scripts.ci_fix.publish import read_state, write_state  # noqa: E402
from scripts.ci_fix.verify.base import FailedJob  # noqa: E402
from tests.test_ci_fix_daily_verify import _DAILY  # noqa: E402

_BASE, _FAILING, _PREVIOUS, _CANDIDATE = "b" * 40, "f" * 40, "p" * 40, "c" * 40


def _prepare_gh():
    repo = MagicMock()
    repo.full_name = "o/r"
    repo.default_branch = "unstable"
    repo.get_workflow_run.return_value = SimpleNamespace(
        id=100, head_branch="unstable", head_sha=_FAILING, workflow_id=5, event="schedule",
        path=".github/workflows/daily.yml", head_repository=SimpleNamespace(full_name="o/r"))
    repo.get_contents.return_value = SimpleNamespace(decoded_content=_DAILY.encode())
    repo.get_branch.return_value = SimpleNamespace(commit=SimpleNamespace(sha=_BASE))
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def _prepare(monkeypatch, tmp_path, *, job="test-ubuntu-jemalloc", outcome=None, gh=None, issue=None,
             occurrences=1):
    failure = IssueFailure(7, 100, "TTL expiration", "tests/integration/rdb.tcl", occurrences=occurrences)
    issue = issue or _issue()
    monkeypatch.setattr(issues_mod, "select_issue", lambda *a, **k: (issue, failure))
    monkeypatch.setattr(issues_mod, "_failing_job", lambda *a, **k: job)
    monkeypatch.setattr(issues_mod, "_previous_daily_sha", lambda *a, **k: _PREVIOUS)
    engine = MagicMock(return_value=outcome or FixOutcome(kind=OutcomeKind.REFUSED, summary="no"))
    monkeypatch.setattr(issues_mod, "run_ci_fix_request", engine)
    state = tmp_path / "state.json"
    result = prepare_issue_fix(gh or _prepare_gh(), MagicMock(), "o/r", bot_login=_BOT, state_path=str(state))
    return result, read_state(str(state)), engine


def test_prepare_builds_a_new_pr_request_on_the_branch_tip(monkeypatch, tmp_path):
    result, state, engine = _prepare(monkeypatch, tmp_path)
    assert result["decision"] == "refused"
    request = engine.call_args.kwargs["request"]
    # Unique per attempt: the claim comment id is the suffix.
    assert (request.head_branch, request.head_sha, request.base_branch) == (
        f"agent/ci-fix/issue-7-100-{state['context']['comment']}", _BASE, "unstable")
    assert (request.policy, request.publication, request.execute) == (Policy.FIX, Publication.NEW_PR, False)
    assert (request.culprit_range, request.failing_sha) == (f"{_PREVIOUS}..{_FAILING}", _FAILING)
    assert request.target == "`TTL expiration` in `tests/integration/rdb.tcl`, as it failed in job `test-ubuntu-jemalloc`"
    assert engine.call_args.kwargs["failed_jobs"] == ("test-ubuntu-jemalloc",)
    assert state["context"]["plan"]["inputs"]["test_args"] == "--single integration/rdb --loops 20 --fastfail"


def test_prepare_refuses_when_the_failing_job_cannot_be_found(monkeypatch, tmp_path):
    result, state, engine = _prepare(monkeypatch, tmp_path, job=None)
    assert result["decision"] == "refused"
    assert "could not find the job" in state["outcome"]["summary"]
    engine.assert_not_called()


def test_prepare_refuses_what_daily_cannot_verify(monkeypatch, tmp_path):
    _result, state, engine = _prepare(monkeypatch, tmp_path, job="notify")
    assert "cannot verify a fix for this in the Daily workflow" in state["outcome"]["summary"]
    engine.assert_not_called()


# --- publish ---------------------------------------------------------------------------

def _ready_state(tmp_path, *, outcome=None):
    from scripts.ci_fix.models import FixRequest

    request = FixRequest("o/r", 0, "o/r", "agent/ci-fix/issue-7-100", _BASE, 100, _BOT,
                         base_branch="unstable", policy=Policy.FIX, publication=Publication.NEW_PR,
                         execute=False, issue_number=7, target="the failure")
    proposal = FixProposal(path=FixPath.AUTHOR, failing_check="TTL expiration", root_cause="race in test",
                           reasoning="wait for the condition", confidence=0.8)
    outcome = outcome or FixOutcome(kind=OutcomeKind.READY, summary="Fix", proposal=proposal, patch="diff",
                                    changed_paths=("tests/integration/rdb.tcl",), failing_run_url="https://run/100")
    plan = {"job": "test-ubuntu-jemalloc", "job_id": "test-ubuntu-jemalloc",
            "inputs": {"skipjobs": "x", "skiptests": "", "test_args": "--single integration/rdb --loops 20 --fastfail"},
            "test_name": "TTL expiration"}
    state = tmp_path / "state.json"
    write_state(str(state), {"context": {"issue": 7, "run": 100, "job": "test-ubuntu-jemalloc", "plan": plan},
                             "request": to_dict(request), "outcome": to_dict(outcome)})
    return state


def _publish_gh(issue):
    repo = MagicMock()
    repo.full_name = "o/r"
    repo.default_branch = "unstable"
    repo.get_issue.return_value = issue
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def test_publish_creates_the_branch_and_dispatches_both_runs(monkeypatch, tmp_path):
    issue = _issue()
    push = MagicMock(return_value=_CANDIDATE)
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", push)
    dispatched = []
    monkeypatch.setattr(issues_mod, "dispatch_daily", lambda _gh, _repo, *, ref, plan, sha: (
        dispatched.append((ref, sha)) or (len(dispatched), f"https://run/{len(dispatched)}")))

    result = publish_issue_fix(_publish_gh(issue), "o/r", git_env={},
                               state_path=str(_ready_state(tmp_path)))

    assert result["action"] == "pending"
    kwargs = push.call_args.kwargs
    assert (kwargs["head_branch"], kwargs["head_sha"], kwargs["create"]) == ("agent/ci-fix/issue-7-100", _BASE, True)
    assert dispatched == [("unstable", _BASE), ("unstable", _CANDIDATE)]
    # No claim id in this state: the checkpoint posts one comment and the
    # final record edits that same comment.
    assert len(issue.comments) == 1
    attempt = attempts_for(issue, _BOT)[0]
    assert attempt.state == "pending"
    assert (attempt.data["baseline"], attempt.data["candidate"], attempt.data["sha"]) == (1, 2, _CANDIDATE)
    assert attempt.data["body"].startswith("Fixes #7")


def test_publish_records_a_refusal_without_pushing(monkeypatch, tmp_path):
    issue = _issue()
    issue.create_comment = MagicMock()
    push = MagicMock()
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", push)
    state = _ready_state(tmp_path, outcome=FixOutcome(kind=OutcomeKind.REFUSED, summary="infrastructure failure"))
    result = publish_issue_fix(_publish_gh(issue), "o/r", git_env={}, state_path=str(state))
    assert result["action"] == "refused"
    push.assert_not_called()
    assert "infrastructure failure" in issue.create_comment.call_args.args[0]


def test_publish_cleans_up_when_dispatch_fails(monkeypatch, tmp_path):
    issue = _issue()
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", MagicMock(return_value=_CANDIDATE))

    def fail(*_a, **_k):
        raise RuntimeError("422 workflow does not accept input")

    deleted = []
    monkeypatch.setattr(issues_mod, "dispatch_daily", fail)
    monkeypatch.setattr(issues_mod, "_delete_branch", lambda _repo, data: deleted.append(data["branch"]))
    result = publish_issue_fix(_publish_gh(issue), "o/r", git_env={},
                               state_path=str(_ready_state(tmp_path)))
    assert result["action"] == "failed"
    assert deleted == ["agent/ci-fix/issue-7-100"]


def test_publish_on_a_closed_issue_records_it_without_pushing(monkeypatch, tmp_path):
    issue = _issue(state="closed")
    push = MagicMock()
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", push)
    result = publish_issue_fix(_publish_gh(issue), "o/r", git_env={},
                               state_path=str(_ready_state(tmp_path)))
    assert result["action"] == "abandoned"
    push.assert_not_called()
    assert "closed before the attempt finished" in issue.comments[-1].body


# --- reconcile -------------------------------------------------------------------------

def _pending(**overrides):
    data = {"v": 1, "run": 100, "state": "pending", "branch": "agent/ci-fix/issue-7-100", "sha": _CANDIDATE,
            "base": "unstable", "baseline": 11, "candidate": 12, "title": "Fix TTL test", "body": "Fixes #7",
            "dispatched_at": int(__import__("time").time()),
            "plan": {"job": "j", "job_id": "j", "inputs": {}, "test_name": "TTL expiration"}}
    data.update(overrides)
    return data


def _reconcile(monkeypatch, *, results, issue_state="open", branch_sha=_CANDIDATE, pending=None):
    comment = _comment(pending or _pending())
    issue = _issue(comments=[comment], state=issue_state)
    monkeypatch.setattr(issues_mod, "_labelled_issues",
                        lambda _repo, *, state, since=None: [issue] if state == issue.state else [])
    def evaluate(_gh, _ac, _repo, run_id, _plan):
        outcome = results[run_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(issues_mod, "evaluate_daily_run", evaluate)
    monkeypatch.setattr(issues_mod, "_branch_sha", lambda *_a: branch_sha)
    deleted, opened = [], []
    monkeypatch.setattr(issues_mod, "_delete_branch", lambda _repo, data: deleted.append(data["branch"]))
    monkeypatch.setattr(issues_mod, "find_existing_pr", lambda *a, **k: None)
    monkeypatch.setattr(issues_mod, "create_pull_from_push_repo",
                        lambda _repo, **kwargs: opened.append(kwargs) or SimpleNamespace(number=55))
    out = reconcile_attempts(MagicMock(), MagicMock(), "o/r", bot_login=_BOT)
    edited = comment.edit.call_args.args[0] if comment.edit.called else ""
    return out, edited, deleted, opened


def _result(state, detail="", failures=0):
    return DailyResult(state, f"https://run/{state}", detail=detail, test_failures=failures)


def test_a_verified_fix_becomes_a_pr_with_both_runs_as_evidence(monkeypatch):
    out, edited, deleted, opened = _reconcile(monkeypatch, results={
        11: _result("failed", "the test failed 2 time(s)", failures=2),
        12: _result("passed", "the test passed 20 time(s)")})
    assert out[0]["action"] == "opened"
    assert opened[0]["head_branch"] == "agent/ci-fix/issue-7-100" and opened[0]["base_branch"] == "unstable"
    assert "Fixes #7" in opened[0]["body"]
    assert "reproduced the failure (the test failed 2 time(s))" in opened[0]["body"]
    assert "the test passed 20 time(s)" in opened[0]["body"]
    assert "opened #55" in edited
    assert deleted == []


def test_a_fix_whose_baseline_did_not_reproduce_still_opens_with_that_evidence(monkeypatch):
    _out, _edited, _deleted, opened = _reconcile(monkeypatch, results={
        11: _result("passed", "the test passed 20 time(s)"), 12: _result("passed", "the test passed 20 time(s)")})
    assert "did not reproduce it" in opened[0]["body"]


def test_a_failed_candidate_is_reported_and_its_branch_deleted(monkeypatch):
    out, edited, deleted, opened = _reconcile(monkeypatch, results={
        11: _result("failed", failures=1), 12: _result("failed", "the test failed 1 time(s)", failures=1)})
    assert out[0]["action"] == "failed"
    assert deleted == ["agent/ci-fix/issue-7-100"]
    assert opened == []
    assert "did not pass verification" in edited


def test_unfinished_runs_are_left_alone(monkeypatch):
    out, edited, deleted, opened = _reconcile(monkeypatch, results={11: None, 12: _result("passed")})
    assert out[0]["action"] == "waiting"
    assert (edited, deleted, opened) == ("", [], [])


def test_a_closed_issue_abandons_the_attempt(monkeypatch):
    out, edited, deleted, _opened = _reconcile(monkeypatch, results={}, issue_state="closed")
    assert out[0]["action"] == "abandoned"
    assert deleted == ["agent/ci-fix/issue-7-100"]
    assert "closed before the attempt finished, so I deleted `agent/ci-fix/issue-7-100`" in edited


def test_a_moved_candidate_branch_is_never_opened(monkeypatch):
    out, _edited, _deleted, opened = _reconcile(
        monkeypatch, results={11: _result("failed", failures=1), 12: _result("passed")}, branch_sha="d" * 40)
    assert out[0]["action"] == "failed"
    assert opened == []


# --- which job to verify ---------------------------------------------------------------

def test_failing_job_maps_the_issue_test_to_a_job_that_failed(monkeypatch):
    monkeypatch.setattr(issues_mod, "failed_jobs_for_run", lambda *a, **k: [
        FailedJob("test-valgrind-test (unit)", "failure", 1), FailedJob("test-sanitizer-address (gcc)", "failure", 2)])
    artifact = {"test-sanitizer-address-gcc": {"valkey": [
        {"test_name": "TTL expiration", "test_file": "tests/integration/rdb.tcl", "error": "x", "status": "err"}]}}
    monkeypatch.setattr(issues_mod, "download_all_test_failures", lambda *a, **k: json.dumps(artifact).encode())
    monkeypatch.setattr(issues_mod, "find_unrecorded_failures", lambda *a, **k: ([], []))
    failure = IssueFailure(7, 100, "TTL expiration", "tests/integration/rdb.tcl")
    run = SimpleNamespace(id=100)
    assert issues_mod._failing_job(MagicMock(), MagicMock(), "o/r", run, failure, ()) == "test-sanitizer-address (gcc)"


def test_failing_job_for_a_job_issue_is_any_job_that_failed_this_way(monkeypatch):
    """The issue names its first job; a twin that failed the same way in this run qualifies."""
    from scripts.test_failure_detector.issue_renderer import job_fingerprint_for
    from scripts.test_failure_detector.job_failures import JobFailure

    leak = JobFailure("test-valgrind-test (unit)", "u1", "test", ("[err]: Valgrind error: ==1== Memcheck",),
                      jobs=[JobReference("test-valgrind-no-malloc-usable-size-test (unit)", "job log", "u2")])
    other = JobFailure("test-valgrind-test (unit)", "u1", "make", (), jobs=[JobReference("test-valgrind-test (unit)",
                                                                                          "job log", "u1")])
    monkeypatch.setattr(issues_mod, "failed_jobs_for_run", lambda *a, **k: [
        FailedJob("test-valgrind-test (unit)", "failure", 1),
        FailedJob("test-valgrind-no-malloc-usable-size-test (unit)", "failure", 2)])
    monkeypatch.setattr(issues_mod, "download_all_test_failures", lambda *a, **k: None)
    run = SimpleNamespace(id=100)

    def pick(groups, fingerprint):
        monkeypatch.setattr(issues_mod, "find_unrecorded_failures", lambda *a, **k: ([], groups))
        issue = IssueFailure(7, 100, job_name="test-valgrind-test (unit)", fingerprint=fingerprint)
        return issues_mod._failing_job(MagicMock(), MagicMock(), "o/r", run, issue, ())

    assert pick([leak], job_fingerprint_for(leak)) == "test-valgrind-no-malloc-usable-size-test (unit)"
    # The issue's own job failed, but differently: that is not this issue's failure.
    assert pick([other], job_fingerprint_for(leak)) is None



# --- trust and robustness --------------------------------------------------------------

from github.GithubException import GithubException  # noqa: E402


def test_issues_opened_by_anyone_but_the_detector_are_ignored():
    """Valkey's issue template labels any user's report test-failure; markers can be forged."""
    forged = _issue(number=1, author="mallory")
    assert select_issue(_gh_with_issues(forged), "o/r", bot_login=_BOT) == "no-actionable-issue"


def test_prepare_claims_the_occurrence_before_anything_can_fail(monkeypatch, tmp_path):
    issue = _issue()
    gh = _prepare_gh()
    gh.get_repo.return_value.get_workflow_run.side_effect = GithubException(404, {"message": "Not Found"})
    result, state, engine = _prepare(monkeypatch, tmp_path, gh=gh, issue=issue)
    assert result["decision"] == "failed"
    assert state["context"]["comment"] == issue.comments[0].id
    claim = attempts_for(issue, _BOT)[0].data
    assert {k: v for k, v in claim.items() if k != "started_at"} == {
        "v": 1, "run": 100, "run_url": "https://github.com/o/r/actions/runs/100", "state": "running"}
    assert isinstance(claim["started_at"], int)
    engine.assert_not_called()


@pytest.mark.parametrize("attribute, value", [
    ("event", "pull_request"),
    ("path", ".github/workflows/ci.yml"),
    ("head_repository", SimpleNamespace(full_name="mallory/valkey")),
])
def test_prepare_refuses_a_run_that_is_not_the_scheduled_daily(monkeypatch, tmp_path, attribute, value):
    gh = _prepare_gh()
    setattr(gh.get_repo.return_value.get_workflow_run.return_value, attribute, value)
    _result, state, engine = _prepare(monkeypatch, tmp_path, gh=gh)
    assert state["outcome"]["summary"] == "run 100 is not a scheduled daily.yml run of o/r"
    engine.assert_not_called()


def test_prepare_claims_before_the_engine_runs(monkeypatch, tmp_path):
    """Order is recorded outside any scope that would swallow a failed assertion."""
    events = []
    issue = _issue()
    post = issue.create_comment
    issue.create_comment = lambda text: events.append("claim") or post(text)
    failure = IssueFailure(7, 100, "TTL expiration", "tests/integration/rdb.tcl")
    monkeypatch.setattr(issues_mod, "select_issue", lambda *a, **k: (issue, failure))
    monkeypatch.setattr(issues_mod, "_failing_job", lambda *a, **k: "test-ubuntu-jemalloc")
    monkeypatch.setattr(issues_mod, "_previous_daily_sha", lambda *a, **k: "")
    monkeypatch.setattr(issues_mod, "run_ci_fix_request",
                        lambda *a, **k: events.append("engine") or FixOutcome(kind=OutcomeKind.REFUSED, summary="no"))

    prepare_issue_fix(_prepare_gh(), MagicMock(), "o/r", bot_login=_BOT, state_path=str(tmp_path / "s.json"))

    assert events == ["claim", "engine"]


def test_publish_replaces_the_claim_instead_of_adding_a_comment(monkeypatch, tmp_path):
    issue = _issue()
    claim = issue.create_comment("claim")
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", MagicMock())
    state = _ready_state(tmp_path, outcome=FixOutcome(kind=OutcomeKind.REFUSED, summary="no safe fix"))
    payload = read_state(str(state))
    payload["context"]["comment"] = claim.id
    write_state(str(state), {key: value for key, value in payload.items() if key != "version"})
    publish_issue_fix(_publish_gh(issue), "o/r", git_env={}, state_path=str(state))
    assert len(issue.comments) == 1
    assert "no safe fix" in claim.body


def test_a_vanished_verification_run_ends_the_attempt(monkeypatch):
    out, edited, deleted, _opened = _reconcile(monkeypatch, results={
        11: GithubException(404, {"message": "Not Found"}), 12: _result("passed")})
    assert out[0]["action"] == "failed"
    assert deleted == ["agent/ci-fix/issue-7-100"]
    assert "no longer exists" in edited


def test_a_verification_that_never_finishes_expires(monkeypatch):
    out, edited, deleted, _opened = _reconcile(
        monkeypatch, results={11: None, 12: None}, pending=_pending(dispatched_at=1))
    assert out[0]["action"] == "failed"
    assert deleted == ["agent/ci-fix/issue-7-100"]
    assert "did not finish in time" in edited


def test_one_attempts_error_does_not_stop_the_others(monkeypatch):
    out, _edited, _deleted, _opened = _reconcile(monkeypatch, results={
        11: RuntimeError("boom"), 12: _result("passed")})
    assert out == [{"issue": 7, "action": "error"}]



def test_publish_records_the_pushed_branch_before_dispatching(monkeypatch, tmp_path):
    """If dispatch never happens, the branch must still be on record for cleanup."""
    issue = _issue()
    claim = issue.create_comment("claim")
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", MagicMock(return_value=_CANDIDATE))
    seen = []

    def dispatch(*_a, **_k):
        seen.append(attempts_for(issue, _BOT)[0].data)
        raise KeyboardInterrupt  # the runner dies mid-publish

    monkeypatch.setattr(issues_mod, "dispatch_daily", dispatch)
    state = _ready_state(tmp_path)
    payload = read_state(str(state))
    payload["context"]["comment"] = claim.id
    write_state(str(state), {key: value for key, value in payload.items() if key != "version"})
    with pytest.raises(KeyboardInterrupt):
        publish_issue_fix(_publish_gh(issue), "o/r", git_env={}, state_path=str(state))
    assert (seen[0]["state"], seen[0]["branch"], seen[0]["sha"]) == ("running", "agent/ci-fix/issue-7-100", _CANDIDATE)


def test_a_running_attempt_from_a_dead_run_is_ended_and_cleaned_up(monkeypatch):
    stale = {"v": 1, "run": 100, "state": "running", "started_at": 1,
             "branch": "agent/ci-fix/issue-7-100-900", "sha": _CANDIDATE}
    out, edited, deleted, _opened = _reconcile(monkeypatch, results={}, pending=stale)
    assert out[0]["action"] == "failed"
    assert deleted == ["agent/ci-fix/issue-7-100-900"]
    assert "stopped before it finished" in edited


def test_a_recent_running_attempt_is_left_alone(monkeypatch):
    fresh = {"v": 1, "run": 100, "state": "running", "started_at": int(__import__("time").time())}
    out, edited, deleted, _opened = _reconcile(monkeypatch, results={}, pending=fresh)
    assert out[0]["action"] == "waiting"
    assert (edited, deleted) == ("", [])


def test_an_issue_whose_bot_pr_is_still_open_is_not_attempted_again():
    """The timeline cross-reference can lag right after reconcile opens the PR."""
    done = _comment({"run": 100, "state": "done", "pr": 55})
    issue = _issue(number=1, body=_test_issue_body(run_id=200), comments=[done])
    gh = _gh_with_issues(issue)
    gh.get_repo.return_value.get_pull.return_value = SimpleNamespace(state="open")
    assert select_issue(gh, "o/r", bot_login=_BOT) == "no-actionable-issue"
    gh.get_repo.return_value.get_pull.return_value = SimpleNamespace(state="closed")
    picked = select_issue(gh, "o/r", bot_login=_BOT)
    assert not isinstance(picked, str) and picked[1].run_id == 200


# --- the only branch-deleting code, exercised for real ---------------------------------

def _ref_repo(sha):
    ref = MagicMock()
    ref.object.sha = sha
    repo = MagicMock()
    repo.get_git_ref.return_value = ref
    return repo, ref


def test_delete_branch_removes_our_unmoved_candidate():
    repo, ref = _ref_repo(_CANDIDATE)
    issues_mod._delete_branch(repo, {"branch": "agent/ci-fix/issue-7-100-900", "sha": _CANDIDATE})
    ref.delete.assert_called_once_with()


def test_delete_branch_keeps_a_branch_someone_pushed_to():
    repo, ref = _ref_repo("d" * 40)
    issues_mod._delete_branch(repo, {"branch": "agent/ci-fix/issue-7-100-900", "sha": _CANDIDATE})
    ref.delete.assert_not_called()


@pytest.mark.parametrize("data", [
    {"branch": "unstable", "sha": _CANDIDATE},
    {"branch": "agent/backport/sweep/9.0", "sha": _CANDIDATE},
    {"branch": "agent/ci-fix/issue-7-100-900"},
])
def test_delete_branch_never_touches_other_refs(data):
    repo, ref = _ref_repo(_CANDIDATE)
    issues_mod._delete_branch(repo, data)
    repo.get_git_ref.assert_not_called()
    ref.delete.assert_not_called()


# --- claim comment fallbacks and isolation ---------------------------------------------

def _missing_claim_issue(status):
    issue = _issue()

    def get_comment(_cid):
        raise GithubException(status, {"message": "x"})

    issue.get_comment = get_comment
    return issue


def test_a_deleted_claim_is_replaced_by_a_new_comment():
    issue = _missing_claim_issue(404)
    issues_mod._record_attempt(issue, 123, "result")
    assert [c.body for c in issue.comments] == ["result"]


def test_a_server_error_on_the_claim_is_not_hidden_by_a_duplicate_comment():
    issue = _missing_claim_issue(500)
    with pytest.raises(GithubException):
        issues_mod._record_attempt(issue, 123, "result")
    assert issue.comments == []


def test_an_error_on_one_issue_does_not_stop_the_next(monkeypatch):
    broken = _issue(number=1, comments=[_comment(_pending(baseline=1, candidate=2))])
    healthy = _issue(number=2, comments=[_comment(_pending())])
    monkeypatch.setattr(issues_mod, "_labelled_issues",
                        lambda _repo, *, state, since=None: [broken, healthy] if state == "open" else [])
    results = {1: RuntimeError("boom"), 2: _result("passed"),
               11: _result("failed", failures=1), 12: _result("passed", "the test passed 20 time(s)")}

    def evaluate(_gh, _ac, _repo, run_id, _plan):
        if isinstance(results[run_id], Exception):
            raise results[run_id]
        return results[run_id]

    monkeypatch.setattr(issues_mod, "evaluate_daily_run", evaluate)
    monkeypatch.setattr(issues_mod, "_branch_sha", lambda *_a: _CANDIDATE)
    monkeypatch.setattr(issues_mod, "find_existing_pr", lambda *a, **k: None)
    monkeypatch.setattr(issues_mod, "create_pull_from_push_repo", lambda _repo, **kw: SimpleNamespace(number=55))
    out = reconcile_attempts(MagicMock(), MagicMock(), "o/r", bot_login=_BOT)
    assert out == [{"issue": 1, "action": "error"},
                   {"issue": 2, "branch": "agent/ci-fix/issue-7-100", "action": "opened", "pr": 55}]


def test_failing_job_finds_a_failure_known_only_from_the_job_logs(monkeypatch):
    monkeypatch.setattr(issues_mod, "failed_jobs_for_run", lambda *a, **k: [FailedJob("test-ubuntu-32bit", "failure", 1)])
    monkeypatch.setattr(issues_mod, "download_all_test_failures", lambda *a, **k: None)
    from_log = UniqueFailure("Throttling tears down", "tests/integration/throttle-repl.tcl", "[TIMEOUT]",
                             [JobReference("test-ubuntu-32bit", "job log")])
    monkeypatch.setattr(issues_mod, "find_unrecorded_failures", lambda *a, **k: ([from_log], []))
    failure = IssueFailure(7, 100, "Throttling tears down", "tests/integration/throttle-repl.tcl")
    job = issues_mod._failing_job(MagicMock(), MagicMock(), "o/r", SimpleNamespace(id=100), failure, ())
    assert job == "test-ubuntu-32bit"


# --- the fix must be for this issue's test ---------------------------------------------

def _ready_for(check):
    proposal = FixProposal(path=FixPath.AUTHOR, failing_check=check, root_cause="race", reasoning="r",
                           confidence=0.8)
    return FixOutcome(kind=OutcomeKind.READY, summary="Fix", proposal=proposal, patch="diff",
                      changed_paths=("tests/integration/rdb.tcl",))


def test_a_fix_for_another_test_is_refused(monkeypatch, tmp_path):
    result, state, _engine = _prepare(monkeypatch, tmp_path, outcome=_ready_for("Replica buffer limit"))
    assert result["decision"] == "refused"
    assert state["outcome"]["summary"] == "the diagnosis addressed 'Replica buffer limit', not the test this issue tracks"
    assert state["outcome"]["patch"] == "diff"  # kept for the record, never pushed


def test_a_fix_for_the_target_test_is_kept(monkeypatch, tmp_path):
    result, _state, _engine = _prepare(monkeypatch, tmp_path, outcome=_ready_for("ttl  expiration (forkless)"))
    assert result["decision"] == "ready"


def test_a_file_timeout_target_is_described_without_a_fake_test_name():
    from scripts.test_failure_detector.job_failures import FILE_TIMEOUT_TEST_NAME

    failure = IssueFailure(7, 1, FILE_TIMEOUT_TEST_NAME, "tests/integration/repl-compression.tcl")
    assert failure.describe("job-x") == (
        "a test client running `tests/integration/repl-compression.tcl` that timed out without "
        "reporting a test name, in job `job-x`")
    assert failure.addressed_by(None) is True


def test_the_fix_pr_is_titled_by_its_test_and_shows_evidence_above_the_footer(monkeypatch):
    proposal = FixProposal(path=FixPath.AUTHOR, failing_check="ZRANGESTORE with zset-max-listpack-entries 0",
                           root_cause="Under clang UBSan, src/t_zset.c:812 shifts a negative value", reasoning="r",
                           confidence=0.8)
    assert issues_mod._pr_title(FixOutcome(kind=OutcomeKind.READY, summary="", proposal=proposal)) == (
        "Fix ZRANGESTORE with zset-max-listpack-entries 0")
    _out, _edited, _deleted, opened = _reconcile(monkeypatch, results={
        11: _result("failed", failures=1), 12: _result("passed", "the test passed 20 time(s)")})
    body = opened[0]["body"]
    assert body.index("**Verification**") < body.index("Opened by valkey-ci-agent")



def test_occurrences_are_read_from_the_issue():
    assert parse_issue(_issue(body=_test_issue_body(run_id=200, name="x"))).occurrences == 1
    assert parse_issue(_issue(body=_test_issue_body(run_id=200, name="x", occurrences=5))).occurrences == 5


def test_a_recurring_failure_gets_no_culprit_candidates(monkeypatch, tmp_path):
    """The commits before the latest recurrence cannot have introduced a failure that already existed."""
    _result, _state, engine = _prepare(monkeypatch, tmp_path, occurrences=3)
    assert engine.call_args.kwargs["request"].culprit_range == ""


def test_the_previous_daily_run_skips_cancelled_runs():
    from scripts.ci_fix.issues import _previous_daily_sha

    runs = [SimpleNamespace(id=100, head_sha=_FAILING, conclusion="failure"),
            SimpleNamespace(id=99, head_sha="x" * 40, conclusion="cancelled"),
            SimpleNamespace(id=98, head_sha="y" * 40, conclusion="skipped"),
            SimpleNamespace(id=97, head_sha=_PREVIOUS, conclusion="success")]
    repo = MagicMock()
    repo.get_workflow.return_value.get_runs.return_value = runs
    run = SimpleNamespace(id=100, workflow_id=5, head_branch="unstable", head_sha=_FAILING)
    assert _previous_daily_sha(repo, run) == _PREVIOUS



def test_a_deleted_claim_still_leaves_one_comment_per_attempt(monkeypatch, tmp_path):
    """The checkpoint re-posts a deleted claim; the final record must edit that comment, not add another."""
    from github import GithubException

    from scripts.common import github_client

    issue = _issue()
    real_get = issue.get_comment

    def get_comment(cid):
        if cid == 999:  # the claim, deleted during prepare
            raise GithubException(404, {"message": "Not Found"}, None)
        return real_get(cid)

    issue.get_comment = get_comment
    monkeypatch.setattr(github_client.time, "sleep", lambda *_: None, raising=False)
    monkeypatch.setattr(issues_mod, "commit_and_push_fix", MagicMock(return_value=_CANDIDATE))
    monkeypatch.setattr(issues_mod, "dispatch_daily", lambda *_a, **_k: (5, "https://run/5"))
    state = _ready_state(tmp_path)
    data = json.loads(state.read_text())
    data["context"].update(comment=999, started_at=1234)
    state.write_text(json.dumps(data))

    assert publish_issue_fix(_publish_gh(issue), "o/r", git_env={}, state_path=str(state))["action"] == "pending"
    assert len(issue.comments) == 1
    assert attempts_for(issue, _BOT)[0].state == "pending"


def test_the_checkpoint_keeps_the_claim_start_time(monkeypatch, tmp_path):
    """A crash right after the push must age from the claim, not look hours old at once."""
    issue = _issue()
    seen = []

    def dispatch(*_a, **_k):
        seen.append(attempts_for(issue, _BOT)[0].data)
        return 5, "https://run/5"

    monkeypatch.setattr(issues_mod, "commit_and_push_fix", MagicMock(return_value=_CANDIDATE))
    monkeypatch.setattr(issues_mod, "dispatch_daily", dispatch)
    state = _ready_state(tmp_path)
    data = json.loads(state.read_text())
    data["context"]["started_at"] = 1234
    state.write_text(json.dumps(data))
    publish_issue_fix(_publish_gh(issue), "o/r", git_env={}, state_path=str(state))
    assert (seen[0]["state"], seen[0]["started_at"], seen[0]["branch"]) == ("running", 1234, "agent/ci-fix/issue-7-100")


def test_a_job_issue_carries_the_detector_fingerprint():
    from scripts.test_failure_detector.issue_renderer import job_fingerprint_for

    failure = JobFailure(job="test-valgrind-test (unit)", url="https://job", step="test",
                         summary=("[err]: Valgrind error",))
    parsed = parse_issue(_issue(body=_job_issue_body(job="test-valgrind-test (unit)")))
    assert (parsed.job_name, parsed.fingerprint) == ("test-valgrind-test (unit)", job_fingerprint_for(failure))
