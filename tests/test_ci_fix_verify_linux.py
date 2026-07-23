"""Tests for the agent-owned Linux Actions verifier."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.ci_fix.verify.base import (
    VerificationPhase,
    VerificationPlan,
    VerifyEnv,
)
from scripts.ci_fix.verify.linux import LinuxVerifier

_HEAD_SHA = "a" * 40


def _plan(
    *,
    env: VerifyEnv = VerifyEnv.LOCAL,
    image: str = "",
    phase: VerificationPhase = VerificationPhase.CANDIDATE,
    repetition: int = 1,
    repetition_count: int = 1,
) -> VerificationPlan:
    return VerificationPlan(
        env=env,
        command="make test",
        workdir="src",
        image=image,
        head_sha=_HEAD_SHA,
        target_repo="owner/target",
        phase=phase,
        repetition=repetition,
        repetition_count=repetition_count,
    )


def _github_with_run(
    *,
    conclusion: str = "success",
    step_conclusion: str | None = None,
):
    captured = {}
    verdict_step = SimpleNamespace(
        name="Run targeted verification",
        conclusion=step_conclusion or conclusion,
    )
    job = SimpleNamespace(steps=[verdict_step])
    run = SimpleNamespace(
        id=23,
        status="completed",
        conclusion=conclusion,
        html_url="https://example/run/23",
        name="CI Fix Verify Linux",
        display_title="pending",
        created_at=datetime(2999, 1, 1, tzinfo=timezone.utc),
        head_branch="main",
        event="workflow_dispatch",
    )
    run.jobs = lambda: [job]

    def dispatch(ref, inputs):
        captured["ref"] = ref
        captured["inputs"] = inputs
        run.display_title = f"verify-linux [token:{inputs['correlation']}]"
        return True

    workflow = MagicMock()
    workflow.create_dispatch.side_effect = dispatch
    workflow.get_runs.return_value = [run]
    repo = MagicMock()
    repo.get_workflow.return_value = workflow
    repo.get_workflow_run.return_value = run
    github = MagicMock()
    github.get_repo.return_value = repo
    return github, captured


def _verifier(github):
    return LinuxVerifier(
        github,
        agent_repo_full_name="owner/agent",
        ref="main",
        timeout=5,
    )


def test_host_baseline_dispatches_shared_contract(monkeypatch):
    monkeypatch.setattr("scripts.ci_fix.verify.linux.time.sleep", lambda *_: None)
    github, captured = _github_with_run()

    result = _verifier(github).verify(
        "/unused",
        _plan(
            phase=VerificationPhase.BASELINE,
            repetition=2,
            repetition_count=3,
        ),
        "",
    )

    assert result.verified is True
    assert result.ran is True
    assert captured["ref"] == "main"
    assert captured["inputs"] == {
        "target_repo": "owner/target",
        "head_sha": _HEAD_SHA,
        "patch_b64": "",
        "verify_command": "make test",
        "workdir": "src",
        "container_image": "",
        "phase": "baseline",
        "repetition": "2",
        "repetition_count": "3",
        "correlation": captured["inputs"]["correlation"],
    }
    github.get_repo.return_value.get_workflow.assert_called_with(
        "ci-fix-verify-linux.yml"
    )


def test_docker_candidate_dispatches_static_image_and_patch(monkeypatch):
    monkeypatch.setattr("scripts.ci_fix.verify.linux.time.sleep", lambda *_: None)
    github, captured = _github_with_run()

    result = _verifier(github).verify(
        "/unused",
        _plan(env=VerifyEnv.DOCKER, image="almalinux:8"),
        "diff\n",
    )

    assert result.verified is True
    assert captured["inputs"]["container_image"] == "almalinux:8"
    assert captured["inputs"]["patch_b64"] == "ZGlmZgo="


def test_linux_verifier_rejects_wrong_environment_without_dispatch():
    github = MagicMock()

    result = _verifier(github).verify(
        "/unused",
        _plan(env=VerifyEnv.MACOS),
        "diff\n",
    )

    assert result.ran is False
    assert "cannot run environment" in result.detail
    github.get_repo.assert_not_called()


def test_docker_plan_requires_an_image():
    github = MagicMock()

    result = _verifier(github).verify(
        "/unused",
        _plan(env=VerifyEnv.DOCKER),
        "diff\n",
    )

    assert result.ran is False
    assert "requires a container image" in result.detail
    github.get_repo.assert_not_called()


def test_cancelled_linux_run_is_unavailable(monkeypatch):
    monkeypatch.setattr("scripts.ci_fix.verify.linux.time.sleep", lambda *_: None)
    github, _captured = _github_with_run(conclusion="cancelled")

    result = _verifier(github).verify("/unused", _plan(), "diff\n")

    assert result.verified is False
    assert result.ran is False
    assert "without a test verdict" in result.detail


def test_linux_setup_failure_is_not_a_test_verdict(monkeypatch):
    monkeypatch.setattr("scripts.ci_fix.verify.linux.time.sleep", lambda *_: None)
    github, _captured = _github_with_run(
        conclusion="failure",
        step_conclusion="skipped",
    )

    result = _verifier(github).verify("/unused", _plan(), "diff\n")

    assert result.verified is False
    assert result.ran is False
    assert "without a test verdict" in result.detail


def test_linux_verifier_rejects_malformed_dispatch_inputs():
    github = MagicMock()
    verifier = _verifier(github)

    invalid_repo = _plan().__class__(
        **{**_plan().__dict__, "target_repo": "../other"}
    )
    invalid_workdir = _plan().__class__(
        **{**_plan().__dict__, "workdir": "../outside"}
    )
    abbreviated_sha = _plan().__class__(
        **{**_plan().__dict__, "head_sha": "abc1234"}
    )

    assert verifier.verify("/unused", invalid_repo, "diff\n").ran is False
    assert verifier.verify("/unused", invalid_workdir, "diff\n").ran is False
    assert verifier.verify("/unused", abbreviated_sha, "diff\n").ran is False
    github.get_repo.assert_not_called()
