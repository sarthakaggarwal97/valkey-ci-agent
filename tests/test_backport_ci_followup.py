"""Tests for the automatic backport CI follow-up gate and orchestration."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.backport import ci_followup
from scripts.backport.ci_followup import FollowupTarget, find_followup_target, run_followup
from scripts.backport.registry import BranchEntry, RepoEntry
from scripts.ci_fix.models import FixOutcome, OutcomeKind
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


def _run(run_id: int = 10, *, status: str = "completed", conclusion: str = "failure"):
    return SimpleNamespace(
        id=run_id,
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


def test_only_highest_priority_failure_tier_is_actionable(monkeypatch) -> None:
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
    assert [job.id for job in target.jobs] == [2, 3]


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


def test_run_followup_uses_shared_engine_and_posts_job_markers(monkeypatch) -> None:
    pr = _pr()
    claims, bodies = _record_comment(pr)
    target = FollowupTarget(
        pr=pr,
        run=_run(),
        head_sha=_HEAD,
        head_branch="agent/backport/sweep/9.0",
        jobs=(
            FailedJob("Reply schema validator", "failure", id=2),
            FailedJob("unit tests", "failure", id=3),
        ),
    )
    gh = _gh([target.run], pr)
    monkeypatch.setattr(ci_followup, "find_followup_target", lambda *_args, **_kwargs: (target, "actionable"))
    engine = MagicMock(
        return_value=FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary="timing-dependent; no safe change",
        )
    )
    monkeypatch.setattr(ci_followup, "run_ci_fix_request", engine)

    result = run_followup(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
        git_env={},
        artifact_client=MagicMock(),
    )

    assert result["action"] == "refused"
    request = engine.call_args.kwargs["request"]
    assert request.head_sha == _HEAD
    assert engine.call_args.kwargs["failed_jobs"] == (
        "Reply schema validator",
        "unit tests",
    )
    assert len(claims) == 1
    assert "job=2" in claims[0]
    assert "job=3" in claims[0]
    assert "job=2" in bodies[-1]
    assert "job=3" in bodies[-1]
    assert "timing-dependent; no safe change" in bodies[-1]


def test_run_followup_claims_job_ids_before_the_engine_runs(monkeypatch) -> None:
    """A crash mid-fix must still retire these job ids.

    The markers are the only record that an attempt happened, so writing them
    only after the engine returns would make every scheduled run re-diagnose the
    same failure on the same head forever.
    """
    pr = _pr()
    claims, _bodies = _record_comment(pr)
    target = FollowupTarget(
        pr=pr,
        run=_run(),
        head_sha=_HEAD,
        head_branch="agent/backport/sweep/9.0",
        jobs=(FailedJob("unit tests", "failure", id=3),),
    )
    gh = _gh([target.run], pr)
    monkeypatch.setattr(ci_followup, "find_followup_target", lambda *_args, **_kwargs: (target, "actionable"))

    def explode(*_args, **_kwargs):
        assert claims, "job ids must be claimed before the engine is invoked"
        raise RuntimeError("engine crashed")

    monkeypatch.setattr(ci_followup, "run_ci_fix_request", explode)

    result = run_followup(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
        git_env={},
        artifact_client=MagicMock(),
    )

    assert result["action"] == "failed"
    assert len(claims) == 1
    assert "job=3" in claims[0]


def test_run_followup_does_not_post_result_after_concurrent_head_move(monkeypatch) -> None:
    """A moved head must be reported as discarded, never as a result.

    The engine's verdict was reached against a head that no longer exists, so
    publishing it would advertise a fix for code nobody is running.
    """
    original = _pr()
    moved = _pr()
    moved.head.sha = "b" * 40
    claims, bodies = _record_comment(original)
    target = FollowupTarget(
        pr=original,
        run=_run(),
        head_sha=_HEAD,
        head_branch="agent/backport/sweep/9.0",
        jobs=(FailedJob("unit tests", "failure", id=3),),
    )
    gh = _gh([target.run], moved)
    monkeypatch.setattr(ci_followup, "find_followup_target", lambda *_args, **_kwargs: (target, "actionable"))
    monkeypatch.setattr(
        ci_followup,
        "run_ci_fix_request",
        lambda *_args, **_kwargs: FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary="no safe change",
        ),
    )

    result = run_followup(
        gh,
        repo_entry=_entry(),
        target_branch="9.0",
        bot_login=_BOT,
        git_env={},
        artifact_client=MagicMock(),
    )

    assert result["action"] == "stale"
    assert len(claims) == 1
    assert "was discarded" in bodies[-1]
    assert "no safe change" not in bodies[-1]
    assert "job=3" in bodies[-1]
