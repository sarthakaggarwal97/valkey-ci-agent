"""Tests for target-repository-owned exact-environment verification."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from github.GithubException import GithubException

from scripts.ci_fix.verify.base import (
    VerificationPhase,
    VerificationPlan,
    VerifyEnv,
)
from scripts.ci_fix.verify.target_workflow import TargetWorkflowVerifier

_HEAD_SHA = "a" * 40


def _plan(
    *,
    phase: VerificationPhase = VerificationPhase.CANDIDATE,
    repetition: int = 1,
    repetition_count: int = 1,
) -> VerificationPlan:
    return VerificationPlan(
        env=VerifyEnv.TARGET,
        command="an AI-authored command that must not be dispatched",
        job_name="test-freebsd (13.2)",
        head_sha=_HEAD_SHA,
        target_repo="owner/target",
        source_run_id=991,
        phase=phase,
        repetition=repetition,
        repetition_count=repetition_count,
    )


def _gh_with_run(*, conclusion: str, status: str = "completed"):
    captured = {}

    def dispatch(ref, inputs):
        captured["ref"] = ref
        captured["inputs"] = inputs
        run.display_title = f"ci-fix verify [token:{inputs['correlation']}]"
        return True

    run = SimpleNamespace(
        id=17,
        status=status,
        conclusion=conclusion,
        html_url="https://example/run/17",
        name="Target CI Fix Verification",
        display_title="pending",
        created_at=datetime(2999, 1, 1, tzinfo=timezone.utc),
        head_branch="protected-verifier",
        event="workflow_dispatch",
    )
    workflow = MagicMock()
    workflow.create_dispatch.side_effect = dispatch
    workflow.get_runs.return_value = [run]
    repo = MagicMock()
    repo.get_workflow.return_value = workflow
    repo.get_workflow_run.return_value = run
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh, captured, run


def _verifier(gh, *, artifact_client=None):
    return TargetWorkflowVerifier(
        gh,
        repo_full_name="owner/target",
        workflow=".github/workflows/ci-agent-verify.yml",
        ref="protected-verifier",
        timeout=5,
        artifact_client=artifact_client,
    )


def test_dispatches_factual_contract_without_ai_command(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, captured, _run = _gh_with_run(conclusion="success")
    plan = _plan(
        phase=VerificationPhase.BASELINE,
        repetition=2,
        repetition_count=5,
    )

    result = _verifier(gh).verify("/unused", plan, "")

    assert result.verified is True
    assert result.ran is True
    assert result.run_url == "https://example/run/17"
    assert captured["ref"] == "protected-verifier"
    assert captured["inputs"] == {
        "head_sha": _HEAD_SHA,
        "patch_b64": "",
        "failing_run_id": "991",
        "failing_job": "test-freebsd (13.2)",
        "phase": "baseline",
        "repetition": "2",
        "repetition_count": "5",
        "correlation": captured["inputs"]["correlation"],
    }
    assert "verify_command" not in captured["inputs"]


def test_candidate_patch_is_transported_as_base64(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, captured, _run = _gh_with_run(conclusion="success")

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.verified is True
    assert captured["inputs"]["patch_b64"] == "ZGlmZgo="


def test_failed_run_includes_bounded_logs(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, _captured, run = _gh_with_run(conclusion="failure")
    artifacts = MagicMock()
    artifacts.download_run_logs.return_value = {
        "verify/4_Run recipe.txt": b"assertion failed in unit/keyspace\n",
    }

    result = _verifier(gh, artifact_client=artifacts).verify(
        "/unused",
        _plan(),
        "diff\n",
    )

    assert result.verified is False
    assert result.ran is True
    assert "assertion failed" in result.output_tail
    artifacts.download_run_logs.assert_called_once_with("owner/target", run.id)


def test_failed_run_prefers_recipe_log_over_later_noise(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, _captured, _run = _gh_with_run(conclusion="failure")
    artifacts = MagicMock()
    artifacts.download_run_logs.return_value = {
        "verify/4_Run exact verification.txt": b"target assertion failed\n",
        "verify/9_Cleanup.txt": b"noise\n" * 5000,
    }

    result = _verifier(gh, artifact_client=artifacts).verify(
        "/unused",
        _plan(),
        "diff\n",
    )

    assert "target assertion failed" in result.output_tail
    assert "noise" not in result.output_tail


def test_non_verdict_conclusion_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, _captured, _run = _gh_with_run(conclusion="cancelled")

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.verified is False
    assert result.ran is False
    assert "without a test verdict" in result.detail


def test_stale_token_run_is_never_trusted(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.time",
        _advancing_clock(step=10),
    )
    gh, _captured, run = _gh_with_run(conclusion="success")
    run.created_at = datetime(2000, 1, 1, tzinfo=timezone.utc)

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.verified is False
    assert result.ran is False
    assert "did not complete" in result.detail


def test_plan_timeout_caps_target_workflow_wait(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.time",
        _advancing_clock(step=2),
    )
    gh, _captured, _run = _gh_with_run(conclusion="success")
    gh.get_repo.return_value.get_workflow.return_value.get_runs.return_value = []

    result = _verifier(gh).verify(
        "/unused",
        replace(_plan(), timeout_seconds=1),
        "diff\n",
    )

    assert result.ran is False
    assert "within 1s" in result.detail


def test_wrong_ref_or_event_run_is_ignored(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.time",
        _advancing_clock(step=10),
    )
    gh, _captured, run = _gh_with_run(conclusion="success")
    run.head_branch = "candidate-controlled-ref"

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.ran is False
    assert "did not complete" in result.detail


def test_dispatch_failure_is_unavailable(monkeypatch):
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow.return_value.create_dispatch.side_effect = (
        RuntimeError("no Actions permission")
    )

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.verified is False
    assert result.ran is False
    assert "could not dispatch" in result.detail


def test_ambiguous_dispatch_response_is_reconciled_without_retry(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci_fix.verify.target_workflow.time.sleep",
        lambda *_: None,
    )
    gh, captured, run = _gh_with_run(conclusion="success")
    workflow = gh.get_repo.return_value.get_workflow.return_value

    def accepted_then_failed_response(ref, inputs):
        captured["ref"] = ref
        captured["inputs"] = inputs
        run.display_title = f"ci-fix verify [token:{inputs['correlation']}]"
        raise GithubException(502, {"message": "upstream reset"})

    workflow.create_dispatch.side_effect = accepted_then_failed_response

    result = _verifier(gh).verify("/unused", _plan(), "diff\n")

    assert result.verified is True
    assert workflow.create_dispatch.call_count == 1


def test_plan_cannot_redirect_verifier_to_another_repository():
    gh = MagicMock()
    wrong_repo = _plan().__class__(
        **{**_plan().__dict__, "target_repo": "attacker/repo"}
    )

    result = _verifier(gh).verify("/unused", wrong_repo, "diff\n")

    assert result.ran is False
    assert "does not match" in result.detail
    gh.get_repo.assert_not_called()


def test_plan_cannot_route_a_non_target_environment():
    gh = MagicMock()
    wrong_environment = replace(_plan(), env=VerifyEnv.LOCAL)

    result = _verifier(gh).verify("/unused", wrong_environment, "diff\n")

    assert result.ran is False
    assert "cannot run environment" in result.detail
    gh.get_repo.assert_not_called()


def test_plan_requires_full_head_sha():
    gh = MagicMock()
    abbreviated = replace(_plan(), head_sha="abc1234")

    result = _verifier(gh).verify("/unused", abbreviated, "diff\n")

    assert result.ran is False
    assert "missing failed-run identity" in result.detail
    gh.get_repo.assert_not_called()


def _advancing_clock(step: int):
    state = {"now": 1_700_000_000.0}

    def _now():
        state["now"] += step
        return state["now"]

    return _now
