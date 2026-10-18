"""Maintainer-facing output for the compact CI-fix result model."""

from __future__ import annotations

from scripts.ci_fix.comment import render_result
from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FailureType,
    FixCandidate,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    ResultKind,
    TargetKind,
)

_SHA = "a" * 40


def _task(kind: TargetKind = TargetKind.SUGGEST) -> FixTask:
    return FixTask(
        failure=Failure(
            repo_full_name="o/r",
            run_id=9,
            head_sha=_SHA,
            job_id=17,
            job_name="test-ubuntu",
            base_branch="unstable",
            pr_number=3,
            description="The zset payload test failed.",
            other_failed_jobs=("lint",),
        ),
        target=FixTarget(
            kind=kind,
            repo_full_name="o/r",
            base_repo_full_name="o/r",
            branch="agent/ci-fix/x",
            expected_sha=_SHA,
        ),
        policy=FixPolicy.GENERAL,
    )


def _candidate(**overrides: object) -> FixCandidate:
    values = {
        "action": CandidateAction.PATCH,
        "summary": "Prepared a fix.",
        "failing_check": "zset payload",
        "root_cause": "The fixture embeds an obsolete payload.",
        "reasoning": "Regenerate the fixture in the branch format.",
        "failure_type": FailureType.DETERMINISTIC,
        "validation": "Ran the focused payload test.",
        "patch": "--- a/t\n+++ b/t\n@@\n-old\n+new\n",
        "changed_paths": ("t",),
    }
    values.update(overrides)
    return FixCandidate(**values)


def test_pushed_result_defers_to_normal_pr_ci() -> None:
    body = render_result(
        FixResult(
            kind=ResultKind.PUSHED,
            summary="pushed",
            task=_task(TargetKind.UPDATE_PR),
            candidate=_candidate(),
            commit_sha="b" * 40,
        )
    )

    assert "Pushed `bbbbbbbbbbbb`" in body
    assert "Root cause" in body
    assert "Agent validation notes" in body
    assert "Normal pull-request CI is now the verification authority" in body
    assert "verification passed" not in body.lower()


def test_suggestion_contains_the_patch_and_bound_head() -> None:
    body = render_result(
        FixResult(
            kind=ResultKind.SUGGESTED,
            summary="suggested",
            task=_task(),
            candidate=_candidate(patch="--- a/t\n+++ b/t\n+```\n"),
        )
    )

    assert "nothing was pushed" in body
    assert f"Prepared for commit `{_SHA[:12]}`" in body
    assert "Proposed patch" in body
    assert "````diff" in body


def test_oversized_patch_is_not_partially_posted() -> None:
    body = render_result(
        FixResult(
            kind=ResultKind.SUGGESTED,
            summary="suggested",
            task=_task(),
            candidate=_candidate(patch="x" * 45_001),
        )
    )

    assert "exceeded the comment-size limit" in body
    assert "x" * 1_000 not in body


def test_refused_and_failed_results_are_plainly_reported() -> None:
    refused = render_result(
        FixResult(
            kind=ResultKind.REFUSED,
            summary="The job is an infrastructure failure.",
            task=_task(),
            candidate=_candidate(action=CandidateAction.REFUSE),
        )
    )
    failed = render_result(
        FixResult(
            kind=ResultKind.FAILED,
            summary="clone failed",
            task=_task(),
        )
    )

    assert refused.startswith("No fix prepared")
    assert "infrastructure failure" in refused
    assert "stopped with an error: clone failed" in failed


def test_what_the_bot_checked_is_listed():
    from scripts.ci_fix.comment import candidate_context
    from scripts.ci_fix.models import CandidateAction, FixCandidate

    candidate = FixCandidate(action=CandidateAction.PATCH, verification="`make test` failed without the patch "
                             "and passed 2 time(s) with it.\nReview: fixes the cause")
    lines = candidate_context(candidate, include_reasoning=True)
    assert lines[:4] == ["**Checked by the bot:**", "",
                         "- `make test` failed without the patch and passed 2 time(s) with it.",
                         "- Review: fixes the cause"]
