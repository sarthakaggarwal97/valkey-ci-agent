"""Tests for the automatic backport CI follow-up gate and orchestration."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.backport import ci_followup
from scripts.backport.ci_followup import (
    FollowupTarget,
    find_followup_target,
    prepare_followup,
    publish_followup,
)
from scripts.backport.registry import BranchEntry, RepoEntry
from scripts.ci_fix.models import FixOutcome, OutcomeKind, Policy, Publication, to_dict
from scripts.ci_fix.publish import read_state
from scripts.ci_fix.verify.base import FailedJob

_HEAD = "a" * 40
_BOT = "valkeyrie-ops[bot]"


def _entry(**overrides) -> RepoEntry:
    values = {
        "repo": "valkey-io/valkey",
        "project_owner": "valkey-io",
        "project_owner_type": "organization",
        "language": "c",
        "branches": (BranchEntry(branch="9.0", project_number=18),),
        "automatic_ci_followup": True,
        "ci_followup_ignored_jobs": ("*dco*",),
    }
    values.update(overrides)
    return RepoEntry(**values)


def _pr(*, author: str = _BOT, comments=()):
    return SimpleNamespace(
        number=4226,
        state="open",
        user=SimpleNamespace(login=author),
        base=SimpleNamespace(ref="9.0"),
        head=SimpleNamespace(
            ref="agent/backport/sweep/9.0",
            sha=_HEAD,
            repo=SimpleNamespace(full_name="valkey-io/valkey"),
        ),
        get_issue_comments=lambda: list(comments),
    )


def _run(
    run_id: int = 10,
    *,
    status: str = "completed",
    conclusion: str = "failure",
    workflow_id: int = 100,
):
    return SimpleNamespace(
        id=run_id,
        workflow_id=workflow_id,
        head_sha=_HEAD,
        status=status,
        conclusion=conclusion,
    )


def _record_comment(pr):
    """Capture the claim comment and every in-place edit applied to it.

    ``run_followup`` claims the job ids before the engine runs and then edits that
    one comment, so a test has to see both the claim and the final body.
    """
    claims: list[str] = []
    bodies: list[str] = []

    def create(body: str):
        claims.append(body)
        bodies.append(body)
        return SimpleNamespace(edit=bodies.append)

    pr.create_issue_comment = create
    return claims, bodies


def _gh(runs, pr):
    repo = MagicMock()
    repo.get_workflow_runs.return_value = list(runs)
    repo.get_pull.return_value = pr
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def test_finds_current_head_failure_and_ignores_dco(monkeypatch) -> None:
    pr = _pr()
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [
            FailedJob("DCO", "failure", id=1),
            FailedJob("Reply schema validator", "failure", id=2),
        ],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert reason == "actionable"
    assert target is not None
    assert [job.id for job in target.jobs] == [2]


def test_selects_one_highest_priority_failure(monkeypatch) -> None:
    pr = _pr()
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [
            FailedJob("asan tests", "failure", id=1),
            FailedJob("clang format", "failure", id=2),
            FailedJob("reply schema", "failure", id=3),
        ],
    )

    target, _reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is not None
    assert [job.id for job in target.jobs] == [2]


def test_sanitizer_named_test_is_lower_priority_than_unit_test(monkeypatch) -> None:
    pr = _pr()
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [
            FailedJob("test-sanitizer-address", "failure", id=1),
            FailedJob("unit tests", "failure", id=2),
        ],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert reason == "actionable"
    assert target is not None
    assert [job.id for job in target.jobs] == [2]


def test_next_same_priority_failure_remains_actionable_after_claim(monkeypatch) -> None:
    marker = SimpleNamespace(
        user=SimpleNamespace(login=_BOT),
        body=(
            f"<!-- valkey-ci-agent:auto-ci-followup head={_HEAD} "
            "run=10 job=2 -->"
        ),
    )
    pr = _pr(comments=(marker,))
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [
            FailedJob("clang format", "failure", id=2),
            FailedJob("reply schema", "failure", id=3),
        ],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert reason == "actionable"
    assert target is not None
    assert [job.id for job in target.jobs] == [3]


def test_waits_while_any_current_head_run_is_in_progress(monkeypatch) -> None:
    pr = _pr()
    gh = _gh([_run(10), _run(11, status="in_progress", conclusion="")], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "current-head-ci-running"


def test_checks_all_current_head_runs_before_acting(monkeypatch) -> None:
    pr = _pr()
    runs = [_run(run_id) for run_id in range(1, 31)]
    runs.append(_run(31, status="in_progress", conclusion=""))
    gh = _gh(runs, pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "current-head-ci-running"


def test_refuses_non_bot_owned_sweep_pr(monkeypatch) -> None:
    pr = _pr(author="maintainer")
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "pr-not-bot-owned"


def test_handled_job_marker_prevents_retry(monkeypatch) -> None:
    marker = SimpleNamespace(
        user=SimpleNamespace(login=_BOT),
        body=(
            f"<!-- valkey-ci-agent:auto-ci-followup head={_HEAD} "
            "run=10 job=2 -->"
        )
    )
    pr = _pr(comments=(marker,))
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [FailedJob("Reply schema validator", "failure", id=2)],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "no-unhandled-actionable-failures"


def test_per_pr_attempt_budget_stops_new_head_retries(monkeypatch) -> None:
    comments = tuple(
        SimpleNamespace(
            user=SimpleNamespace(login=_BOT),
            body=(
                "<!-- valkey-ci-agent:auto-ci-followup "
                f"head={digit * 40} run={index} job={index} -->"
            ),
        )
        for index, digit in enumerate(("b", "c", "d"), start=1)
    )
    pr = _pr(comments=comments)
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "attempt-budget-exhausted"
    gh.get_repo.return_value.get_workflow_runs.assert_not_called()


def test_logical_job_marker_prevents_retry_from_twin_event_run(monkeypatch) -> None:
    first_run = _run(10, workflow_id=100)
    key = ci_followup._job_key(first_run, "Reply schema validator")
    marker = SimpleNamespace(
        user=SimpleNamespace(login=_BOT),
        body=(
            f"<!-- valkey-ci-agent:auto-ci-followup head={_HEAD} "
            f"run=10 job=2 key={key} -->"
        ),
    )
    pr = _pr(comments=(marker,))
    twin_run = _run(11, workflow_id=100)
    gh = _gh([twin_run], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [FailedJob("Reply schema validator", "failure", id=22)],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert target is None
    assert reason == "no-unhandled-actionable-failures"


def test_same_job_name_in_different_workflow_remains_actionable(monkeypatch) -> None:
    first_run = _run(10, workflow_id=100)
    key = ci_followup._job_key(first_run, "build")
    marker = SimpleNamespace(
        user=SimpleNamespace(login=_BOT),
        body=(
            f"<!-- valkey-ci-agent:auto-ci-followup head={_HEAD} "
            f"run=10 job=2 key={key} -->"
        ),
    )
    pr = _pr(comments=(marker,))
    other_workflow = _run(11, workflow_id=200)
    gh = _gh([other_workflow], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [FailedJob("build", "failure", id=22)],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert reason == "actionable"
    assert target is not None
    assert [job.id for job in target.jobs] == [22]


def test_ignores_handled_job_marker_from_another_user(monkeypatch) -> None:
    marker = SimpleNamespace(
        user=SimpleNamespace(login="untrusted-user"),
        body=(
            f"<!-- valkey-ci-agent:auto-ci-followup head={_HEAD} "
            "run=10 job=2 -->"
        ),
    )
    pr = _pr(comments=(marker,))
    gh = _gh([_run()], pr)
    monkeypatch.setattr(ci_followup, "find_existing_pr", lambda *_args: pr)
    monkeypatch.setattr(
        ci_followup,
        "failed_jobs_for_run",
        lambda *_args: [FailedJob("Reply schema validator", "failure", id=2)],
    )

    target, reason = find_followup_target(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
    )

    assert reason == "actionable"
    assert target is not None
    assert [job.id for job in target.jobs] == [2]


def _target(pr, *jobs):
    return FollowupTarget(
        pr=pr, run=_run(), head_sha=_HEAD, head_branch="agent/backport/sweep/9.0",
        jobs=jobs or (FailedJob("unit tests", "failure", id=3),),
    )


class _Claims:
    """A PR comment store: the claim posted by prepare, edited by publish."""

    def __init__(self, pr):
        self.bodies: dict[int, list[str]] = {}
        pr.create_issue_comment = self.create
        pr.get_issue_comment = lambda cid: SimpleNamespace(edit=self.bodies[cid].append)

    def create(self, body):
        cid = len(self.bodies) + 1
        self.bodies[cid] = [body]
        return SimpleNamespace(id=cid, edit=self.bodies[cid].append)

    def latest(self, cid=1):
        return self.bodies[cid][-1]


def _prepare(monkeypatch, tmp_path, target, engine):
    if not hasattr(target.pr, "claims"):
        target.pr.claims = _Claims(target.pr)
    monkeypatch.setattr(ci_followup, "find_followup_target", lambda *_a, **_k: (target, "actionable"))
    monkeypatch.setattr(ci_followup, "run_ci_fix_request", engine)
    state = tmp_path / "state.json"
    result = prepare_followup(
        _gh([target.run], target.pr), repo_entry=_entry(), target_branch="9.0", bot_login=_BOT,
        artifact_client=MagicMock(), state_path=str(state),
    )
    return result, state


def test_prepare_runs_the_engine_on_one_job_and_records_its_markers(monkeypatch, tmp_path) -> None:
    target = _target(_pr(), FailedJob("Reply schema validator", "failure", id=2),
                     FailedJob("unit tests", "failure", id=3))
    engine = MagicMock(return_value=FixOutcome(kind=OutcomeKind.REFUSED, summary="timing-dependent"))
    result, state = _prepare(monkeypatch, tmp_path, target, engine)

    assert result["action"] == "prepared"
    assert result["decision"] == "refused"
    request = engine.call_args.kwargs["request"]
    assert (request.head_sha, request.base_branch) == (_HEAD, "9.0")
    assert (request.policy, request.publication) == (Policy.BACKPORT, Publication.PUSH)
    # A test job outranks Daily's whole-suite reply-schemas validator.
    assert request.target == "the failure in job `unit tests`"
    assert engine.call_args.kwargs["failed_jobs"] == ("unit tests",)
    markers = "\n".join(read_state(str(state))["context"]["markers"])
    assert "job=3" in markers and "key=" in markers
    assert "job=2" not in markers


def test_prepare_claims_the_job_on_the_pr_before_the_engine_runs(monkeypatch, tmp_path) -> None:
    """A lost runner or cancelled job must still retire the job id on GitHub."""
    pr = _pr()
    claims = _Claims(pr)
    state = tmp_path / "state.json"

    def explode(*_a, **_k):
        assert "job=3" in claims.latest(), "the markers must be on the PR before the engine runs"
        assert read_state(str(state))["outcome"]["kind"] == "failed"
        raise RuntimeError("engine crashed")

    target = _target(pr)
    target.pr.claims = claims
    result, _ = _prepare(monkeypatch, tmp_path, target, explode)
    assert result["decision"] == "failed"
    assert read_state(str(state))["context"]["comment"] == 1
    assert "diagnosing the failure in job `unit tests`" in claims.latest()


def test_prepare_claim_happens_before_the_engine(monkeypatch, tmp_path) -> None:
    """Order is recorded outside the engine's exception handler, which would swallow an assert."""
    events = []
    pr = _pr()
    claims = _Claims(pr)
    post = pr.create_issue_comment
    pr.create_issue_comment = lambda body: events.append("claim") or post(body)
    target = _target(pr)
    target.pr.claims = claims
    _prepare(monkeypatch, tmp_path, target,
             lambda *_a, **_k: events.append("engine") or FixOutcome(kind=OutcomeKind.REFUSED, summary="no"))
    assert events == ["claim", "engine"]


def test_prepare_without_a_target_records_nothing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(ci_followup, "find_followup_target", lambda *_a, **_k: (None, "current-head-ci-running"))
    state = tmp_path / "state.json"
    result = prepare_followup(
        MagicMock(), repo_entry=_entry(), target_branch="9.0", bot_login=_BOT,
        artifact_client=MagicMock(), state_path=str(state),
    )
    assert result == {"repo": "valkey-io/valkey", "branch": "9.0", "action": "skipped",
                      "reason": "current-head-ci-running"}
    assert not state.exists()


def _publish(monkeypatch, tmp_path, *, outcome, current_pr, publish, target_pr=None):
    """Prepare with ``outcome``, then publish against ``current_pr``."""
    pr = target_pr or _pr()
    claims = _Claims(pr)
    target = _target(pr)
    target.pr.claims = claims
    _prepare(monkeypatch, tmp_path, target, MagicMock(return_value=outcome))
    monkeypatch.setattr(ci_followup, "publish_to_pr", publish)
    # The publish step sees the PR as it is now, with the claim it posted.
    current_pr.get_issue_comment = pr.get_issue_comment
    current_pr.create_issue_comment = pr.create_issue_comment
    result = publish_followup(
        _gh([], current_pr), repo_entry=_entry(), target_branch="9.0", bot_login=_BOT,
        git_env={}, state_path=str(tmp_path / "state.json"),
    )
    assert len(claims.bodies) == 1, "the result replaces the claim instead of adding a comment"
    return result, claims.bodies[1][1:]


_READY = FixOutcome(kind=OutcomeKind.READY, summary="Fix for unit tests", patch="diff",
                    changed_paths=("tests/x.tcl",), verify_backend="local")


def test_publish_pushes_and_posts_the_result_with_markers(monkeypatch, tmp_path) -> None:
    pushed_sha = "b" * 40
    current = _pr()
    current.head.sha = pushed_sha

    def publish(outcome, request, **kwargs):
        assert kwargs["pre_push_check"]() == "the PR head moved from aaaaaaaaaaaa to bbbbbbbbbbbb"
        return FixOutcome(kind=OutcomeKind.PUSHED, summary="Pushed fix for unit tests",
                          commit_sha=pushed_sha, proposal=outcome.proposal)

    result, posted = _publish(monkeypatch, tmp_path, outcome=_READY, current_pr=current, publish=publish)
    assert result["action"] == "pushed"
    assert "head_moved_after_push" not in result
    assert len(posted) == 1
    assert f"pushed `{pushed_sha[:12]}`" in posted[0]
    assert "job=3" in posted[0]


def test_publish_pre_push_check_refuses_a_closed_pr(monkeypatch, tmp_path) -> None:
    closed = _pr()
    closed.state = "closed"

    def publish(outcome, request, **kwargs):
        return FixOutcome(kind=OutcomeKind.REFUSED, summary=kwargs["pre_push_check"]())

    result, posted = _publish(monkeypatch, tmp_path, outcome=_READY, current_pr=closed, publish=publish)
    assert result["action"] == "refused"
    assert "pr-not-open" in posted[0]


def test_publish_keeps_a_pushed_result_when_the_head_moves_after_push(monkeypatch, tmp_path) -> None:
    current = _pr()
    current.head.sha = "c" * 40

    def publish(outcome, request, **kwargs):
        return FixOutcome(kind=OutcomeKind.PUSHED, summary="Pushed", commit_sha="b" * 40)

    result, posted = _publish(monkeypatch, tmp_path, outcome=_READY, current_pr=current, publish=publish)
    assert result["action"] == "pushed"
    assert result["head_moved_after_push"] is True
    assert "fix was pushed as `bbbbbbbbbbbb`" in posted[0]


def test_publish_discards_a_result_reached_on_a_moved_head(monkeypatch, tmp_path) -> None:
    moved = _pr()
    moved.head.sha = "b" * 40
    refused = FixOutcome(kind=OutcomeKind.REFUSED, summary="no safe change")
    result, posted = _publish(monkeypatch, tmp_path, outcome=refused, current_pr=moved,
                              publish=lambda outcome, *_a, **_k: outcome)
    assert result["action"] == "stale"
    assert "was discarded" in posted[0]
    assert "no safe change" not in posted[0]
    assert "job=3" in posted[0]


def test_publish_rejects_state_for_another_branch(monkeypatch, tmp_path) -> None:
    _prepare(monkeypatch, tmp_path, _target(_pr()), MagicMock(return_value=_READY))
    import pytest

    with pytest.raises(ValueError, match="does not match"):
        publish_followup(
            _gh([], _pr()), repo_entry=_entry(), target_branch="8.0", bot_login=_BOT,
            git_env={}, state_path=str(tmp_path / "state.json"),
        )


def test_publish_without_state_skips(tmp_path) -> None:
    result = publish_followup(
        MagicMock(), repo_entry=_entry(), target_branch="9.0", bot_login=_BOT,
        git_env={}, state_path=str(tmp_path / "none.json"),
    )
    assert result["reason"] == "nothing-prepared"
