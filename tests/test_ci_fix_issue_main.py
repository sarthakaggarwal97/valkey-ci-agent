"""Tests for issue-to-draft-PR orchestration."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.ci_fix import issue_main
from scripts.ci_fix.issue_gate import IssueFixRequest
from scripts.ci_fix.issue_main import IssueFixResult, render_issue_comment, run_issue_fix
from scripts.ci_fix.issue_publish import IssueFixPullRequest
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
    ReviewVerdict,
    RunResult,
)
from scripts.ci_fix.push import IssuePushResult


def _proposal() -> FixProposal:
    return FixProposal(
        path=FixPath.AUTHOR,
        failing_check="flaky test",
        root_cause="state is observed before replication converges",
        reasoning="wait for the causal transition",
        confidence=0.9,
        failing_job_hint="test-valgrind",
        build_command="make",
        verify_command="./runtest --single x",
    )


def _gated(
    *,
    base_sha: str = "a" * 40,
    detector_issue: bool = True,
) -> IssueFixRequest:
    return IssueFixRequest(
        fix_request=FixRequest(
            repo_full_name="valkey-io/valkey",
            pr_number=4149,
            head_repo_full_name="valkey-io/valkey",
            head_branch="unstable",
            head_sha="a" * 40,
            run_id=29132625592,
            requested_by="alice",
        ),
        issue_number=4149,
        detector_issue=detector_issue,
        issue_title="[TEST-FAILURE] flaky test",
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        issue_evidence="evidence",
        base_branch="unstable",
        base_sha=base_sha,
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
    )


def _publication() -> IssueFixPullRequest:
    return IssueFixPullRequest(
        number=5000,
        url="https://github.com/valkey-io/valkey/pull/5000",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
    )


def _run(
    monkeypatch,
    outcome: FixOutcome,
    *,
    gated: IssueFixRequest | None = None,
    execute_func=None,
):
    monkeypatch.setattr(
        issue_main,
        "build_issue_fix_request",
        lambda *a, **k: gated or _gated(),
    )
    create = MagicMock(return_value=_publication())
    result = run_issue_fix(
        MagicMock(),
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=SimpleNamespace(run_url="", hint=""),
        git_env={},
        artifact_client=MagicMock(),
        execute_func=execute_func or MagicMock(return_value=outcome),
        create_pull_func=create,
    )
    return result, create


def test_verified_outcome_opens_draft_pr(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(True, "approved"),
        commit_sha="a" * 40,
        verify_backend="local",
    )
    result, create = _run(monkeypatch, outcome)

    assert result.publication == _publication()
    assert result.verified is True
    assert create.call_args.kwargs["verified"] is True
    assert create.call_args.kwargs["close_issue"] is True


def test_explicit_link_on_ordinary_issue_does_not_auto_close_source(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(True, "approved"),
        commit_sha="a" * 40,
        verify_backend="local",
    )
    result, create = _run(
        monkeypatch,
        outcome,
        gated=_gated(detector_issue=False),
    )

    assert result.publication == _publication()
    assert create.call_args.kwargs["close_issue"] is False


def test_historical_run_opens_draft_without_claiming_current_verification(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(True, "approved"),
        commit_sha="c" * 40,
        verify_backend="local",
    )
    result, create = _run(monkeypatch, outcome, gated=_gated(base_sha="b" * 40))

    assert result.publication == _publication()
    assert result.verified is False
    assert create.call_args.kwargs["verified"] is False
    assert create.call_args.kwargs["verification_sha"] == "a" * 40
    assert "default branch had advanced" in render_issue_comment(result)


def test_publication_race_uses_actual_pushed_base_for_verification(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(True, "approved"),
        commit_sha="c" * 40,
        verify_backend="local",
    )
    publish = MagicMock(
        return_value=IssuePushResult(commit_sha="c" * 40, base_sha="b" * 40)
    )
    monkeypatch.setattr(issue_main, "commit_and_push_issue_fix", publish)

    def execute(_gh, **kwargs):
        kwargs["push_func"](
            "/repo",
            proposal=_proposal(),
            changed_paths=("test.tcl",),
            git_env={},
        )
        return outcome

    result, create = _run(monkeypatch, outcome, execute_func=execute)

    assert result.verified is False
    assert create.call_args.kwargs["verified"] is False
    assert "default branch had advanced" in render_issue_comment(result)


def test_reviewed_handoff_patch_is_published_as_draft(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="baseline did not reproduce",
        proposal=_proposal(),
        run_result=RunResult(True, True, 0, "./runtest", "ok"),
        review=ReviewVerdict(True, "approved"),
        handoff_patch="reviewed patch",
        handoff_paths=("test.tcl",),
    )
    publish_patch = MagicMock(
        return_value=IssuePushResult(commit_sha="b" * 40, base_sha="a" * 40)
    )
    monkeypatch.setattr(issue_main, "commit_and_push_issue_patch", publish_patch)
    result, create = _run(monkeypatch, outcome)

    assert result.publication == _publication()
    assert result.verified is False
    publish_patch.assert_called_once()
    assert publish_patch.call_args.kwargs["expected_paths"] == ("test.tcl",)
    assert create.call_args.kwargs["verified"] is False


def test_handoff_without_reviewed_paths_is_not_published(monkeypatch):
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="baseline did not reproduce",
        proposal=_proposal(),
        review=ReviewVerdict(True, "approved"),
        handoff_patch="reviewed patch",
    )
    publish_patch = MagicMock()
    monkeypatch.setattr(issue_main, "commit_and_push_issue_patch", publish_patch)

    result, create = _run(monkeypatch, outcome)

    assert result.outcome.kind is OutcomeKind.FAILED
    assert "path metadata" in result.outcome.summary
    publish_patch.assert_not_called()
    create.assert_not_called()


def test_refusal_does_not_publish(monkeypatch):
    outcome = FixOutcome(kind=OutcomeKind.REFUSED, summary="no causal fix")
    result, create = _run(monkeypatch, outcome)
    assert result.publication is None
    create.assert_not_called()


def test_issue_comment_distinguishes_verified_and_handoff():
    verified = IssueFixResult(
        FixOutcome(
            kind=OutcomeKind.PUSHED,
            summary="done",
            proposal=_proposal(),
        ),
        publication=_publication(),
        verified=True,
    )
    handoff = IssueFixResult(
        FixOutcome(
            kind=OutcomeKind.HANDOFF,
            summary="handoff",
            proposal=_proposal(),
            run_result=RunResult(True, True, 0, "./runtest", "ok"),
        ),
        publication=_publication(),
        verified=False,
    )

    assert "passed repeated verification" in render_issue_comment(verified)
    assert "did not establish a failing baseline" in render_issue_comment(handoff)
    assert "do not merge" in render_issue_comment(verified)


def test_issue_comment_does_not_overclaim_unrunnable_handoff():
    handoff = IssueFixResult(
        FixOutcome(
            kind=OutcomeKind.HANDOFF,
            summary="verification could not run",
            proposal=_proposal(),
            run_result=RunResult(False, False, -1, "./runtest", "missing tool"),
            review=ReviewVerdict(True, "approved"),
            handoff_patch="reviewed patch",
            handoff_paths=("test.tcl",),
        ),
        publication=_publication(),
        verified=False,
    )

    comment = render_issue_comment(handoff)
    assert "could not produce a reliable post-change test verdict" in comment
    assert "passed repeated post-change runs" not in comment


def test_main_reports_unexpected_pipeline_error(monkeypatch):
    gh = MagicMock()
    monkeypatch.setattr(issue_main, "Github", MagicMock(return_value=gh))
    monkeypatch.setattr(issue_main, "ArtifactClient", MagicMock())
    auth = MagicMock()
    auth.__enter__.return_value.env.return_value = {}
    monkeypatch.setattr(issue_main, "GitAuth", MagicMock(return_value=auth))
    monkeypatch.setattr(
        issue_main,
        "run_issue_fix",
        MagicMock(side_effect=RuntimeError("unexpected")),
    )
    post = MagicMock()
    react = MagicMock()
    monkeypatch.setattr(issue_main, "_post_comment", post)
    monkeypatch.setattr(issue_main, "_react_outcome", react)
    monkeypatch.delenv("CI_FIX_MACOS_AGENT_REPO", raising=False)
    monkeypatch.delenv("CI_FIX_MACOS_TOKEN", raising=False)

    result = issue_main.main(
        [
            "--target-token",
            "token",
            "--repo",
            "valkey-io/valkey",
            "--issue",
            "4149",
            "--commenter",
            "alice",
        ]
    )

    assert result == 1
    assert "internal error" in post.call_args.args[3]
    react.assert_called_once_with(
        gh,
        "valkey-io/valkey",
        0,
        OutcomeKind.FAILED,
    )


def test_issue_workflow_has_budget_for_repeated_verification():
    workflow = Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    assert "inputs.issue > 0 && 55 || 90" in workflow
    assert "format('issue-{0}', inputs.issue)" in workflow
    assert "CI_FIX_DETECTOR_LOGIN:" in workflow
