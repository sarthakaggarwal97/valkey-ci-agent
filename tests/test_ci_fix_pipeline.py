"""Tests for the push step, comment renderer, and pipeline orchestration."""

from __future__ import annotations

import subprocess
import threading
import time
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.gate import GateRejection, parse_command
from scripts.ci_fix.models import (
    BaselineEvidence,
    BaselineKind,
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    ReviewVerdict,
)
from scripts.ci_fix.pipeline import run_ci_fix
from scripts.ci_fix.push import PushRefused, commit_and_push_fix

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"


def _proposal(path: FixPath = FixPath.AUTHOR) -> FixProposal:
    return FixProposal(
        path=path, failing_check="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11", reasoning="scaffolding fix",
        confidence=0.9, failing_job_hint="test-ubuntu-latest",
        build_command="make", verify_command="./runtest --single x",
    )


def _deterministic_baseline():
    return BaselineEvidence(
        kind=BaselineKind.DETERMINISTIC,
        attempts=3,
        passed=0,
        failed=3,
        detail="clean baseline failed 3/3 run(s)",
    )


# --- push: namespace guard ---

def test_push_refuses_non_namespaced_branch():
    with pytest.raises(PushRefused, match="agent/backport/"):
        commit_and_push_fix(
            "/repo", head_repo_full_name="valkey-io/valkey",
            head_branch="some-contributor-branch", head_sha="abc1234", proposal=_proposal(),
            changed_paths=("test.tcl",), git_env={},
        )


def test_push_refuses_empty_changed_paths():
    with pytest.raises(PushRefused, match="no approved changed paths"):
        commit_and_push_fix(
            "/repo", head_repo_full_name="valkey-io/valkey",
            head_branch="agent/backport/sweep/8.0", head_sha="abc1234", proposal=_proposal(),
            changed_paths=(), git_env={},
        )


def test_push_stages_only_approved_paths(tmp_path, monkeypatch):
    """A stray untracked file from a test must not be committed (P2)."""
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "agent/backport/sweep/8.0", str(repo)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "seed.txt").write_text("seed")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "seed"], check=True, capture_output=True)
    head_sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(remote)], check=True)
    subprocess.run(["git", "-C", str(repo), "push", "origin",
                    "HEAD:agent/backport/sweep/8.0"], check=True, capture_output=True)
    monkeypatch.setattr("scripts.ci_fix.push.github_https_url", lambda _f: str(remote))

    (repo / "test.tcl").write_text("the approved fix")
    (repo / "stray-artifact.log").write_text("test output that must not be committed")

    commit_and_push_fix(
        str(repo), head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/8.0", head_sha=head_sha, proposal=_proposal(),
        changed_paths=("test.tcl",), git_env={},
    )
    committed = subprocess.run(
        ["git", "--git-dir", str(remote), "show", "--name-only", "--format=",
         "refs/heads/agent/backport/sweep/8.0"],
        capture_output=True, text=True, check=True,
    ).stdout
    assert "test.tcl" in committed
    assert "stray-artifact.log" not in committed


def test_push_commits_and_pushes(tmp_path, monkeypatch):
    """A real local repo: the fix is committed (no sign-off) and pushed to a bare remote."""
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)

    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "agent/backport/sweep/8.0", str(repo)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "seed.txt").write_text("seed")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "seed"], check=True, capture_output=True)
    head_sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(remote)], check=True)
    subprocess.run(["git", "-C", str(repo), "push", "origin",
                    "HEAD:agent/backport/sweep/8.0"], check=True, capture_output=True)

    # The push step rewrites origin via github_https_url; point it at the local bare remote.
    monkeypatch.setattr("scripts.ci_fix.push.github_https_url", lambda _full_name: str(remote))

    # The "fix": an edit in the working tree.
    (repo / "test.tcl").write_text("fixed payload")

    commit_sha = commit_and_push_fix(
        str(repo), head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/8.0", head_sha=head_sha, proposal=_proposal(),
        changed_paths=("test.tcl",), git_env={},
    )

    assert len(commit_sha) == 40
    msg = subprocess.run(
        ["git", "--git-dir", str(remote), "log", "-1", "--format=%B",
         "refs/heads/agent/backport/sweep/8.0"],
        capture_output=True, text=True, check=True,
    ).stdout
    assert "Signed-off-by:" not in msg
    assert "NAN score" in msg


def test_push_uses_clean_clone_not_source_git_config(tmp_path, monkeypatch):
    """A credential helper planted in the verified checkout must not run."""
    remote = tmp_path / "remote.git"
    leak = tmp_path / "leak.txt"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)

    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-b", "agent/backport/sweep/8.0", str(repo)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    (repo / "seed.txt").write_text("seed")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "seed"], check=True, capture_output=True)
    head_sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(remote)], check=True)
    subprocess.run(["git", "-C", str(repo), "push", "origin",
                    "HEAD:agent/backport/sweep/8.0"], check=True, capture_output=True)
    monkeypatch.setattr("scripts.ci_fix.push.github_https_url", lambda _full_name: str(remote))

    subprocess.run(
        ["git", "-C", str(repo), "config", "credential.helper",
         f"!sh -c 'echo $GIT_PASSWORD > {leak}; exit 1'"],
        check=True,
    )
    (repo / "test.tcl").write_text("fixed payload")

    commit_and_push_fix(
        str(repo), head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/8.0", head_sha=head_sha, proposal=_proposal(),
        changed_paths=("test.tcl",), git_env={"GIT_PASSWORD": "secret"},
    )

    assert not leak.exists()


# --- comment renderer ---

def test_render_pushed_comment_includes_evidence():
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(approved=True, reasoning="minimal and correct"),
        commit_sha="abcdef1234567890",
        verify_backend="local",
        verification_run_url="https://github.com/o/r/actions/runs/10",
    )
    body = render_comment(outcome)
    assert "NAN score" in body
    assert "abcdef123456" in body
    assert "actions/runs/10" in body
    assert "minimal and correct" in body
    assert "do not merge" in body.lower()


def test_render_pushed_links_failing_and_verification_runs():
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed",
        proposal=_proposal(),
        review=ReviewVerdict(approved=True, reasoning="ok"),
        commit_sha="abcdef1234567890",
        failing_run_url="https://github.com/o/r/actions/runs/9",
        verify_backend="local",
        verification_run_url="https://github.com/o/r/actions/runs/10",
    )
    body = render_comment(outcome)
    assert "actions/runs/9" in body
    assert "actions/runs/10" in body


def test_render_port_comment_shows_targeted_runner_evidence():
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed",
        proposal=_proposal(FixPath.PORT),
        review=ReviewVerdict(
            approved=True,
            reasoning="Commit 9f374e15848d is already merged on unstable",
        ),
        commit_sha="abcdef1234567890",
        verify_backend="local",
        verification_run_url="https://github.com/o/r/actions/runs/10",
        baseline=_deterministic_baseline(),
    )
    body = render_comment(outcome)
    assert "isolated Linux Actions runner" in body
    assert "actions/runs/10" in body
    assert "Port provenance" in body
    assert "upstream fix passed targeted verification" in body


def test_render_refused_comment_explains():
    outcome = FixOutcome(
        kind=OutcomeKind.REFUSED,
        summary="genuinely flaky timing failure; no safe fix",
        other_failing_checks=("other test",),
        failing_run_url="https://github.com/o/r/actions/runs/9",
        verification_run_url="https://github.com/o/r/actions/runs/10",
    )
    body = render_comment(outcome)
    assert "did not push" in body.lower()
    assert "flaky" in body
    assert "other test" in body
    assert "actions/runs/9" in body
    assert "actions/runs/10" in body


def test_render_failed_comment():
    outcome = FixOutcome(kind=OutcomeKind.FAILED, summary="could not clone repo")
    body = render_comment(outcome)
    assert "error" in body.lower()
    assert "could not clone repo" in body


# --- pipeline orchestration ---

def _gh_authorized(pr_head_sha="abc123", run_head_sha="abc123"):
    from types import SimpleNamespace
    membership = SimpleNamespace(state="active")
    team = MagicMock()
    team.get_team_membership.return_value = membership
    org = MagicMock()
    org.get_team_by_slug.return_value = team
    pr = SimpleNamespace(head=SimpleNamespace(
        sha=pr_head_sha, ref="agent/backport/sweep/8.0",
        repo=SimpleNamespace(full_name="valkey-io/valkey")))
    run = SimpleNamespace(head_sha=run_head_sha, head_branch="agent/backport/sweep/8.0",
                          status="completed", conclusion="failure")
    run.jobs = lambda: [SimpleNamespace(name="test-ubuntu-latest", conclusion="failure")]
    repo = MagicMock()
    repo.get_pull.return_value = pr
    repo.get_workflow_run.return_value = run
    gh = MagicMock()
    gh.get_organization.return_value = org
    gh.get_repo.return_value = repo
    return gh


def _artifact_client(logs):
    client = MagicMock()
    client.download_run_logs.return_value = logs
    return client


def _passing_remote_verifier():
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    verifier = MagicMock()

    def verify(_repo, plan, _patch):
        if plan.phase is VerificationPhase.BASELINE:
            return VerificationResult(
                verified=False,
                ran=True,
                detail="clean sample reproduced the failure",
                run_url="https://run/baseline",
            )
        return VerificationResult(
            verified=True,
            ran=True,
            detail="candidate sample passed",
            run_url="https://run/candidate",
        )

    verifier.verify.side_effect = verify
    return verifier


def _run_pipeline(monkeypatch, **overrides):
    from scripts.ci_fix.verify.base import VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha",
                        overrides.get("clone", lambda *a, **k: True))
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline._classify_failing_job",
        overrides.get("classify", lambda *a, **k: JobEnvironment(VerifyEnv.LOCAL)),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.discover_port_candidates",
        overrides.get("discover", lambda *a, **k: ()),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.reset_worktree",
        overrides.get("reset", lambda *a, **k: None),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.apply_port_commit",
        overrides.get("apply_port", lambda *a, **k: ("tests/unit/x.tcl",)),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.build_approved_patch",
        overrides.get("build_patch", lambda *a, **k: "port diff\n"),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.apply_fix",
        overrides.get("apply", lambda *a, **k: (True, ("test.tcl",))),
    )
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline.build_and_review_patch",
        overrides.get("patch_review", lambda *a, **k: _patch_review(True)),
    )
    linux_verifier = (
        overrides["linux_verifier"]
        if "linux_verifier" in overrides
        else _passing_remote_verifier()
    )
    return run_ci_fix(
        _gh_authorized(), command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
        git_env={}, artifact_client=_artifact_client(overrides.get("logs", {"1.txt": b"err"})),
        diagnose_func=overrides.get("diagnose", lambda *a, **k: _proposal()),
        push_func=overrides.get("push", lambda *a, **k: "deadbeef" * 5),
        port_push_func=overrides.get("port_push", lambda *a, **k: "deadbeef" * 5),
        linux_verifier=linux_verifier,
        macos_verifier=overrides.get("macos_verifier"),
        exact_verifier=overrides.get("exact_verifier"),
        history_branches=overrides.get("history_branches", ()),
        verify_runs=overrides.get("verify_runs", 2),
        baseline_runs=overrides.get("baseline_runs", 3),
        flaky_verify_runs=overrides.get("flaky_verify_runs", 10),
        minimum_confidence=overrides.get("minimum_confidence", 0.8),
        protected_paths=overrides.get("protected_paths", (".github/workflows/**",)),
        auto_publish_paths=overrides.get("auto_publish_paths", ("**",)),
        allowed_branch_prefixes=overrides.get(
            "allowed_branch_prefixes", ("agent/backport/",),
        ),
        remote_parallelism=overrides.get("remote_parallelism", 5),
        remote_sample_timeout_seconds=overrides.get(
            "remote_sample_timeout_seconds", 15 * 60,
        ),
        remote_budget_seconds=overrides.get("remote_budget_seconds", 45 * 60),
    )


def test_pipeline_happy_path(monkeypatch):
    outcome = _run_pipeline(monkeypatch)
    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.commit_sha.startswith("deadbeef")


def test_pipeline_gate_rejection(monkeypatch):
    # Non-member: get_team_membership returns pending.
    gh = _gh_authorized()
    gh.get_organization.return_value.get_team_by_slug.return_value\
        .get_team_membership.return_value.state = "pending"
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", lambda *a, **k: True)
    outcome = run_ci_fix(
        gh, command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="stranger",
        git_env={}, artifact_client=_artifact_client({"x": b"y"}),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not an active member" in outcome.summary


def test_pipeline_no_logs(monkeypatch):
    outcome = _run_pipeline(monkeypatch, logs={})
    assert outcome.kind is OutcomeKind.REFUSED
    assert "expired" in outcome.summary


def test_pipeline_clone_failure(monkeypatch):
    outcome = _run_pipeline(monkeypatch, clone=lambda *a, **k: False)
    assert outcome.kind is OutcomeKind.FAILED
    assert "clone" in outcome.summary


def test_pipeline_diagnose_refuses(monkeypatch):
    outcome = _run_pipeline(monkeypatch, diagnose=lambda *a, **k: _proposal(FixPath.REFUSE))
    assert outcome.kind is OutcomeKind.REFUSED


def test_pipeline_candidate_failure_refuses(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    verifier = MagicMock()

    def fail_candidate(_repo, plan, _patch):
        return VerificationResult(
            verified=False,
            ran=True,
            detail=(
                "clean sample failed"
                if plan.phase is VerificationPhase.BASELINE
                else "candidate still failing"
            ),
        )

    verifier.verify.side_effect = fail_candidate
    outcome = _run_pipeline(monkeypatch, linux_verifier=verifier)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "candidate still failing" in outcome.summary


def test_pipeline_port_runs_repeated_linux_actions_verification_before_push(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate
    from scripts.ci_fix.verify.base import VerificationPhase, VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment

    classify = MagicMock(return_value=JobEnvironment(VerifyEnv.LOCAL))
    apply_port = MagicMock(return_value=("tests/unit/x.tcl",))
    verifier = _passing_remote_verifier()
    # The model only sees the short SHA the prompt renders; the discovered
    # candidate carries the full 40-char SHA, as it does in the real flow.
    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": "9f374e15848d"}
    )
    ported = MagicMock(return_value="abc123" * 6)
    candidate = PortCandidate(
        sha=full_sha,
        subject="the historical fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )

    outcome = _run_pipeline(
        monkeypatch, diagnose=lambda *a, **k: proposal,
        classify=classify, port_push=ported,
        discover=lambda *a, **k: (candidate,),
        apply_port=apply_port,
        linux_verifier=verifier,
        baseline_runs=4,
        verify_runs=3,
        remote_parallelism=1,
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.verify_backend == "local"
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.DETERMINISTIC
    assert outcome.review is not None
    assert "already merged on unstable" in outcome.review.reasoning
    assert "3 candidate sample(s)" in outcome.review.reasoning
    # the port push got the full upstream SHA, canonicalized from the short hint
    assert ported.call_args.kwargs["unstable_fix_commit"] == full_sha
    assert ported.call_args.kwargs["source_ref"] == "origin/unstable"
    phases = [call.args[1].phase for call in verifier.verify.call_args_list]
    assert phases == [VerificationPhase.BASELINE] * 4 + [VerificationPhase.CANDIDATE] * 3
    assert [call.args[1].repetition for call in verifier.verify.call_args_list] == [
        1, 2, 3, 4, 1, 2, 3,
    ]
    assert [call.args[2] for call in verifier.verify.call_args_list] == [
        "", "", "", "", "port diff\n", "port diff\n", "port diff\n",
    ]
    assert apply_port.call_count == 1
    assert apply_port.call_args.args[1] == full_sha
    classify.assert_called_once()


def test_pipeline_port_all_green_baseline_is_handoff_only(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": full_sha[:12]}
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="the upstream fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    from scripts.ci_fix.verify.base import VerificationResult

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(
        verified=True,
        ran=True,
        detail="sample passed",
        run_url="https://run/green",
    )
    ported = MagicMock()

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        linux_verifier=verifier,
        verify_runs=2,
        flaky_verify_runs=7,
        remote_parallelism=1,
        port_push=ported,
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.NOT_REPRODUCED
    assert verifier.verify.call_count == 3 + 7
    assert "not established" in outcome.summary
    ported.assert_not_called()


def test_pipeline_port_candidate_failure_never_pushes(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": full_sha[:12]}
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="the upstream fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    ported = MagicMock()
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    verifier = MagicMock()

    def fail_candidate(_repo, plan, _patch):
        return VerificationResult(
            verified=False,
            ran=True,
            detail=(
                "baseline failed"
                if plan.phase is VerificationPhase.BASELINE
                else "trusted port did not pass candidate verification"
            ),
        )

    verifier.verify.side_effect = fail_candidate

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        linux_verifier=verifier,
        remote_parallelism=1,
        port_push=ported,
    )

    assert outcome.kind is OutcomeKind.REFUSED
    assert "did not pass candidate verification" in outcome.summary
    ported.assert_not_called()


def test_pipeline_port_conflict_refuses(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": "9f374e15848d"}
    )
    candidate = PortCandidate(
        sha="9f374e15848d7b070cdd58a071a741c0a59a6c75",
        subject="the upstream fix",
        paths=("tests/unit/x.tcl",),
    )

    def refuse(*_a, **_k):
        raise PushRefused("upstream fix did not cherry-pick cleanly: conflict")

    outcome = _run_pipeline(
        monkeypatch, diagnose=lambda *a, **k: proposal, port_push=refuse,
        discover=lambda *a, **k: (candidate,),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "cherry-pick" in outcome.summary


def test_pipeline_port_apply_conflict_refuses_before_verification(monkeypatch):
    from scripts.ci_fix.apply import PortApplyError
    from scripts.ci_fix.port_discovery import PortCandidate

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": full_sha[:12]}
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="the upstream fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    verifier = _passing_remote_verifier()
    ported = MagicMock()

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        apply_port=MagicMock(
            side_effect=PortApplyError("historical fix did not apply cleanly")
        ),
        linux_verifier=verifier,
        remote_parallelism=1,
        port_push=ported,
    )

    assert outcome.kind is OutcomeKind.REFUSED
    assert "did not apply cleanly" in outcome.summary
    assert all(
        call.args[1].phase.value == "baseline"
        for call in verifier.verify.call_args_list
    )
    ported.assert_not_called()


def test_pipeline_port_refuses_sha_not_in_discovered_candidates(monkeypatch):
    """A PORT SHA the model invents that was not surfaced as a candidate for
    this failure is refused, so PORT cannot bypass verification with an
    unrelated default-branch commit."""
    from scripts.ci_fix.port_discovery import PortCandidate
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": "9f374e15848d"}
    )
    ported = MagicMock(return_value="abc123" * 6)
    # A different commit was discovered; the model's SHA is not in the set.
    other = PortCandidate(
        sha="1111111111111111111111111111111111111111",
        subject="unrelated",
        paths=("tests/unit/other.tcl",),
    )

    outcome = _run_pipeline(
        monkeypatch, diagnose=lambda *a, **k: proposal, port_push=ported,
        discover=lambda *a, **k: (other,),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not among the fixes discovered" in outcome.summary
    ported.assert_not_called()


def test_pipeline_port_refuses_ambiguous_short_sha(monkeypatch):
    """A short SHA that prefixes more than one discovered candidate is
    ambiguous; the port is refused rather than guessing a commit."""
    from scripts.ci_fix.port_discovery import PortCandidate
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": "9f374e"}
    )
    ported = MagicMock(return_value="abc123" * 6)
    cand_a = PortCandidate(
        sha="9f374e15848d7b070cdd58a071a741c0a59a6c75",
        subject="fix a",
        paths=("tests/a",),
    )
    cand_b = PortCandidate(
        sha="9f374eaa00000000000000000000000000000000",
        subject="fix b",
        paths=("tests/b",),
    )

    outcome = _run_pipeline(
        monkeypatch, diagnose=lambda *a, **k: proposal, port_push=ported,
        discover=lambda *a, **k: (cand_a, cand_b),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not among the fixes discovered" in outcome.summary
    ported.assert_not_called()


def test_pipeline_passes_configured_history_branches_to_discovery(monkeypatch):
    seen = {}

    def discover(*_args, **kwargs):
        seen["history_branches"] = kwargs["history_branches"]
        return ()

    outcome = _run_pipeline(
        monkeypatch,
        discover=discover,
        history_branches=("7.2", "8.0"),
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert seen["history_branches"] == ("7.2", "8.0")


def test_pipeline_push_refused(monkeypatch):
    def refuse(*a, **k):
        raise PushRefused("bad branch namespace")
    outcome = _run_pipeline(monkeypatch, push=refuse)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "bad branch namespace" in outcome.summary


def test_verified_authored_product_change_is_handed_off_by_policy(monkeypatch):
    from scripts.ci_fix.review import PatchReview

    pushed = MagicMock()

    outcome = _run_pipeline(
        monkeypatch,
        apply=lambda *a, **k: (True, ("src/server.c",)),
        patch_review=lambda *a, **k: PatchReview(
            ok=True,
            patch="--- a/src/server.c\n+++ b/src/server.c\n",
            review=ReviewVerdict(True, "root cause addressed"),
        ),
        push=pushed,
        auto_publish_paths=("tests/**",),
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert "src/server.c" in outcome.summary
    assert outcome.handoff_patch.startswith("--- a/src/server.c")
    assert outcome.verify_backend == "local"
    pushed.assert_not_called()


def test_historical_workflow_port_is_handed_off_by_policy(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{
            **_proposal(FixPath.PORT).__dict__,
            "unstable_fix_commit": full_sha[:12],
        }
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="Fix release workflow",
        paths=(".github/workflows/daily.yml",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    ported = MagicMock()
    verifier = _passing_remote_verifier()

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        apply_port=lambda *a, **k: (".github/workflows/daily.yml",),
        linux_verifier=verifier,
        remote_parallelism=1,
        port_push=ported,
        protected_paths=(".github/workflows/**",),
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert full_sha in outcome.summary
    assert "protected" in outcome.summary
    assert verifier.verify.call_count == 3 + 2
    ported.assert_not_called()


def test_pipeline_passes_registry_branch_prefix_to_publisher(monkeypatch):
    seen = {}

    def push(*_args, **kwargs):
        seen["prefixes"] = kwargs["allowed_branch_prefixes"]
        return "deadbeef" * 5

    outcome = _run_pipeline(
        monkeypatch,
        push=push,
        allowed_branch_prefixes=("agent/fix/", "agent/backport/"),
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert seen["prefixes"] == ("agent/fix/", "agent/backport/")


# --- backend routing ---

def test_pipeline_docker_dispatches_image_in_every_sample(monkeypatch):
    from scripts.ci_fix.verify.base import VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment
    verifier = _passing_remote_verifier()

    outcome = _run_pipeline(
        monkeypatch,
        classify=lambda *a, **k: JobEnvironment(VerifyEnv.DOCKER, image="almalinux:8"),
        linux_verifier=verifier,
        remote_parallelism=1,
    )
    assert outcome.kind is OutcomeKind.PUSHED
    assert {
        (call.args[1].env, call.args[1].image)
        for call in verifier.verify.call_args_list
    } == {(VerifyEnv.DOCKER, "almalinux:8")}
    assert outcome.verify_backend == "docker:almalinux:8"


def test_pipeline_dispatches_configured_candidate_repetitions(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationPhase

    verifier = _passing_remote_verifier()
    outcome = _run_pipeline(
        monkeypatch,
        linux_verifier=verifier,
        verify_runs=5,
        remote_parallelism=1,
    )
    assert outcome.kind is OutcomeKind.PUSHED
    candidates = [
        call.args[1]
        for call in verifier.verify.call_args_list
        if call.args[1].phase is VerificationPhase.CANDIDATE
    ]
    assert [plan.repetition for plan in candidates] == [1, 2, 3, 4, 5]
    assert all(plan.repetition_count == 5 for plan in candidates)


def test_pipeline_linux_actions_uses_flaky_sampling_policy(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    verifier = MagicMock()

    def mixed_baseline(_repo, plan, _patch):
        verified = (
            plan.phase is VerificationPhase.CANDIDATE
            or plan.repetition == 1
        )
        return VerificationResult(verified=verified, ran=True, detail="sample")

    verifier.verify.side_effect = mixed_baseline

    outcome = _run_pipeline(
        monkeypatch,
        linux_verifier=verifier,
        baseline_runs=7,
        flaky_verify_runs=25,
        remote_parallelism=1,
    )

    assert outcome.kind is OutcomeKind.PUSHED
    plans = [call.args[1] for call in verifier.verify.call_args_list]
    assert sum(plan.phase is VerificationPhase.BASELINE for plan in plans) == 7
    candidates = [
        plan for plan in plans if plan.phase is VerificationPhase.CANDIDATE
    ]
    assert len(candidates) == 25
    assert all(plan.repetition_count == 25 for plan in candidates)


def test_pipeline_refuses_action_below_repository_confidence_threshold(monkeypatch):
    low = _proposal()
    low = low.__class__(**{**low.__dict__, "confidence": 0.74})
    verifier = MagicMock()

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: low,
        linux_verifier=verifier,
        minimum_confidence=0.8,
    )

    assert outcome.kind is OutcomeKind.REFUSED
    assert "0.74" in outcome.summary
    assert "0.80" in outcome.summary
    verifier.verify.assert_not_called()


def test_pipeline_unsupported_env_prepares_reviewed_handoff(monkeypatch):
    from scripts.ci_fix.verify.base import VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment
    outcome = _run_pipeline(
        monkeypatch,
        classify=lambda *a, **k: JobEnvironment(VerifyEnv.UNSUPPORTED, reason="self-hosted arm"),
        reset=lambda *a, **k: None,
        apply=lambda *a, **k: (True, ("tests/arm.tcl",)),
        patch_review=lambda *a, **k: _patch_review(True),
    )
    assert outcome.kind is OutcomeKind.HANDOFF
    assert "self-hosted arm" in outcome.summary
    assert outcome.handoff_patch == "diff\n"


def test_pipeline_refuses_job_not_in_failed_set(monkeypatch):
    # The AI names a job that did not fail in the linked run; refuse.
    def diagnose(*a, **k):
        p = _proposal()
        return p.__class__(**{**p.__dict__, "failing_job_hint": "some-other-job"})
    outcome = _run_pipeline(monkeypatch, diagnose=diagnose)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not among the failed jobs" in outcome.summary


def _macos_pipeline(monkeypatch, verifier, *, apply_ok=True, review_ok=True):
    from scripts.ci_fix.verify.base import VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", lambda *a, **k: True)
    monkeypatch.setattr("scripts.ci_fix.pipeline.reset_worktree", lambda *a, **k: None)
    monkeypatch.setattr("scripts.ci_fix.pipeline._classify_failing_job",
                        lambda *a, **k: JobEnvironment(VerifyEnv.MACOS))
    monkeypatch.setattr("scripts.ci_fix.pipeline.apply_fix",
                        lambda *a, **k: (apply_ok, ("test.tcl",) if apply_ok else ()))
    monkeypatch.setattr("scripts.ci_fix.pipeline.build_and_review_patch",
                        lambda *a, **k: _patch_review(review_ok))
    return run_ci_fix(
        _gh_authorized(), command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
        git_env={}, artifact_client=_artifact_client({"1.txt": b"err"}),
        diagnose_func=lambda *a, **k: _proposal(),
        push_func=lambda *a, **k: "cafe" * 10,
        macos_verifier=verifier,
        auto_publish_paths=("**",),
    )


def _patch_review(ok):
    from scripts.ci_fix.review import PatchReview
    return PatchReview(ok=ok, patch="diff\n",
                       review=ReviewVerdict(ok, "ok" if ok else "weak"),
                       detail="" if ok else "review rejected the fix: weak")


def test_pipeline_macos_green_pushes(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult
    verifier = MagicMock()
    failed = VerificationResult(
        verified=False, ran=True, detail="baseline failed", run_url="https://run/base",
    )
    passed = VerificationResult(
        verified=True, ran=True, detail="ok", run_url="https://run/9",
    )
    verifier.verify.side_effect = [failed] * 3 + [passed] * 2
    outcome = _macos_pipeline(monkeypatch, verifier)
    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.verify_backend == "macos"
    assert outcome.verification_run_url == "https://run/9"
    assert outcome.baseline is not None
    assert outcome.baseline.kind.value == "deterministic"


def test_pipeline_macos_all_green_baseline_is_handoff_only(monkeypatch):
    from scripts.ci_fix.models import BaselineKind
    from scripts.ci_fix.verify.base import VerificationResult

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(
        verified=True,
        ran=True,
        detail="passed",
        run_url="https://run/green",
    )

    outcome = _macos_pipeline(monkeypatch, verifier)

    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.NOT_REPRODUCED
    # Three clean samples plus the stronger default flaky candidate campaign.
    assert verifier.verify.call_count == 3 + 10
    assert "not established" in outcome.summary


def test_pipeline_macos_cleanup_failure_preserves_push(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult, VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment

    calls = {"n": 0}

    def reset_ok_then_fail_on_cleanup(*_a, **_k):
        # Succeed for the loop's own pre-attempt reset; fail only on the final
        # cleanup reset that runs after the loop has already returned PUSHED.
        calls["n"] += 1
        if calls["n"] > 1:
            raise RuntimeError("git reset failed")

    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", lambda *a, **k: True)
    monkeypatch.setattr("scripts.ci_fix.pipeline.reset_worktree", reset_ok_then_fail_on_cleanup)
    monkeypatch.setattr("scripts.ci_fix.pipeline._classify_failing_job",
                        lambda *a, **k: JobEnvironment(VerifyEnv.MACOS))
    monkeypatch.setattr("scripts.ci_fix.pipeline.apply_fix", lambda *a, **k: (True, ("test.tcl",)))
    monkeypatch.setattr("scripts.ci_fix.pipeline.build_and_review_patch",
                        lambda *a, **k: _patch_review(True))
    verifier = MagicMock()
    failed = VerificationResult(
        verified=False, ran=True, detail="baseline failed", run_url="https://run/base",
    )
    passed = VerificationResult(
        verified=True, ran=True, detail="ok", run_url="https://run/9",
    )
    verifier.verify.side_effect = [failed] * 3 + [passed] * 2

    outcome = run_ci_fix(
        _gh_authorized(), command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
        git_env={}, artifact_client=_artifact_client({"1.txt": b"err"}),
        diagnose_func=lambda *a, **k: _proposal(),
        push_func=lambda *a, **k: "cafe" * 10,
        macos_verifier=verifier,
        auto_publish_paths=("**",),
    )
    # A cleanup failure must not mask the successful push.
    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.verification_run_url == "https://run/9"


def test_pipeline_macos_red_refuses(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult
    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(verified=False, ran=True, detail="did not pass", run_url="https://run/9")
    outcome = _macos_pipeline(monkeypatch, verifier)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "did not pass" in outcome.summary
    from scripts.ci_fix.pipeline import _REMOTE_FIX_MAX_ATTEMPTS
    assert verifier.verify.call_count == 3 + 2 * _REMOTE_FIX_MAX_ATTEMPTS


def test_pipeline_macos_red_retries_with_feedback(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult, VerifyEnv
    from scripts.ci_fix.verify.workflow_env import JobEnvironment
    feedback_seen = []

    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", lambda *a, **k: True)
    monkeypatch.setattr("scripts.ci_fix.pipeline.reset_worktree", lambda *a, **k: None)
    monkeypatch.setattr("scripts.ci_fix.pipeline._classify_failing_job",
                        lambda *a, **k: JobEnvironment(VerifyEnv.MACOS))

    def apply(_repo, _proposal, *, feedback=""):
        feedback_seen.append(feedback)
        return True, ("test.tcl",)

    monkeypatch.setattr("scripts.ci_fix.pipeline.apply_fix", apply)
    monkeypatch.setattr("scripts.ci_fix.pipeline.build_and_review_patch",
                        lambda *a, **k: _patch_review(True))
    verifier = MagicMock()
    verifier.verify.side_effect = [
        VerificationResult(
            verified=False, ran=True, detail="baseline failed", run_url="https://run/base",
        ),
        VerificationResult(
            verified=False, ran=True, detail="baseline failed", run_url="https://run/base",
        ),
        VerificationResult(
            verified=False, ran=True, detail="baseline failed", run_url="https://run/base",
        ),
        VerificationResult(
            verified=False, ran=True, detail="did not pass", run_url="https://run/1",
            output_tail="unit/test_vset.c:137:25: error: variable length array folded",
        ),
        VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/1b"),
        VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/2"),
        VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/2"),
    ]

    outcome = run_ci_fix(
        _gh_authorized(), command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
        git_env={}, artifact_client=_artifact_client({"1.txt": b"err"}),
        diagnose_func=lambda *a, **k: _proposal(),
        push_func=lambda *a, **k: "cafe" * 10,
        macos_verifier=verifier,
        auto_publish_paths=("**",),
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert feedback_seen[0] == ""
    assert "unit/test_vset.c:137" in feedback_seen[1]
    assert "https://run/1" in feedback_seen[1]
    assert verifier.verify.call_count == 7


def test_pipeline_macos_unavailable_hands_off_reviewed_patch(monkeypatch):
    outcome = _macos_pipeline(monkeypatch, None)
    assert outcome.kind is OutcomeKind.HANDOFF
    assert "not configured" in outcome.summary
    assert outcome.handoff_patch == "diff\n"


def test_pipeline_port_uses_repeated_target_workflow_before_push(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": full_sha[:12]}
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="the unstable fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    classifier = MagicMock(side_effect=AssertionError("target verifier owns the environment"))
    verifier = MagicMock()
    baseline_failed = VerificationResult(
        verified=False,
        ran=True,
        detail="reproduced",
        run_url="https://run/base",
    )
    candidate_passed = VerificationResult(
        verified=True,
        ran=True,
        detail="fixed",
        run_url="https://run/fixed",
    )
    verifier.verify.side_effect = [baseline_failed] * 2 + [candidate_passed] * 3
    ported = MagicMock(return_value="cafe" * 10)

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        exact_verifier=verifier,
        classify=classifier,
        baseline_runs=2,
        verify_runs=3,
        remote_parallelism=1,
        build_patch=lambda *a, **k: "exact port diff\n",
        port_push=ported,
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.verify_backend == "target-workflow"
    assert outcome.verification_run_url == "https://run/fixed"
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.DETERMINISTIC
    phases = [call.args[1].phase for call in verifier.verify.call_args_list]
    assert phases == [
        VerificationPhase.BASELINE,
        VerificationPhase.BASELINE,
        VerificationPhase.CANDIDATE,
        VerificationPhase.CANDIDATE,
        VerificationPhase.CANDIDATE,
    ]
    patches = [call.args[2] for call in verifier.verify.call_args_list]
    assert patches == ["", "", "exact port diff\n", "exact port diff\n", "exact port diff\n"]
    assert ported.call_args.kwargs["unstable_fix_commit"] == full_sha
    classifier.assert_not_called()


def test_pipeline_port_unavailable_target_baseline_hands_off(monkeypatch):
    from scripts.ci_fix.port_discovery import PortCandidate
    from scripts.ci_fix.verify.base import VerificationResult

    full_sha = "9f374e15848d7b070cdd58a071a741c0a59a6c75"
    proposal = _proposal(FixPath.PORT).__class__(
        **{**_proposal(FixPath.PORT).__dict__, "unstable_fix_commit": full_sha[:12]}
    )
    candidate = PortCandidate(
        sha=full_sha,
        subject="the unstable fix",
        paths=("tests/unit/x.tcl",),
        source_ref="origin/unstable",
        source_branch="unstable",
    )
    verifier = MagicMock(
        **{
            "verify.return_value": VerificationResult(
                verified=False,
                ran=False,
                detail="runner unavailable",
                run_url="https://run/unavailable",
            )
        }
    )
    ported = MagicMock()

    outcome = _run_pipeline(
        monkeypatch,
        diagnose=lambda *a, **k: proposal,
        discover=lambda *a, **k: (candidate,),
        exact_verifier=verifier,
        baseline_runs=1,
        remote_parallelism=1,
        port_push=ported,
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.UNAVAILABLE
    assert outcome.verification_run_url == "https://run/unavailable"
    assert verifier.verify.call_count == 1
    ported.assert_not_called()


def test_pipeline_exact_verifier_is_preferred_and_receives_remote_phases(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationPhase, VerificationResult

    classifier = MagicMock(side_effect=AssertionError("exact verifier bypasses YAML guessing"))
    verifier = MagicMock()
    baseline_failed = VerificationResult(
        verified=False, ran=True, detail="reproduced", run_url="https://run/base",
    )
    candidate_passed = VerificationResult(
        verified=True, ran=True, detail="fixed", run_url="https://run/fixed",
    )
    verifier.verify.side_effect = [baseline_failed] * 2 + [candidate_passed] * 2

    outcome = _run_pipeline(
        monkeypatch,
        exact_verifier=verifier,
        classify=classifier,
        baseline_runs=2,
        verify_runs=2,
        reset=lambda *a, **k: None,
        apply=lambda *a, **k: (True, ("tests/exact.tcl",)),
        patch_review=lambda *a, **k: _patch_review(True),
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.verify_backend == "target-workflow"
    assert outcome.verification_run_url == "https://run/fixed"
    phases = [call.args[1].phase for call in verifier.verify.call_args_list]
    assert phases == [
        VerificationPhase.BASELINE,
        VerificationPhase.BASELINE,
        VerificationPhase.CANDIDATE,
        VerificationPhase.CANDIDATE,
    ]
    assert all(
        call.args[1].source_run_id == 27559908167
        for call in verifier.verify.call_args_list
    )
    classifier.assert_not_called()


def test_pipeline_remote_mixed_baseline_uses_flaky_repetition_policy(monkeypatch):
    from scripts.ci_fix.models import BaselineKind
    from scripts.ci_fix.verify.base import VerificationResult

    verifier = MagicMock()
    failed = VerificationResult(verified=False, ran=True, detail="failed")
    passed = VerificationResult(verified=True, ran=True, detail="passed")
    verifier.verify.side_effect = [failed, passed, failed] + [passed] * 4

    outcome = _run_pipeline(
        monkeypatch,
        exact_verifier=verifier,
        baseline_runs=3,
        verify_runs=1,
        flaky_verify_runs=4,
        reset=lambda *a, **k: None,
        apply=lambda *a, **k: (True, ("tests/flaky.tcl",)),
        patch_review=lambda *a, **k: _patch_review(True),
    )

    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.baseline is not None
    assert outcome.baseline.kind is BaselineKind.FLAKY
    assert verifier.verify.call_count == 7
    candidate_plans = [call.args[1] for call in verifier.verify.call_args_list[3:]]
    assert [plan.repetition for plan in candidate_plans] == [1, 2, 3, 4]
    assert all(plan.repetition_count == 4 for plan in candidate_plans)


def test_pipeline_unavailable_exact_baseline_hands_off(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(
        verified=False,
        ran=False,
        detail="workflow dispatch unavailable",
        run_url="https://run/unavailable",
    )

    outcome = _run_pipeline(
        monkeypatch,
        exact_verifier=verifier,
        reset=lambda *a, **k: None,
        apply=lambda *a, **k: (True, ("tests/exact.tcl",)),
        patch_review=lambda *a, **k: _patch_review(True),
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert "baseline verifier was unavailable" in outcome.summary
    assert outcome.handoff_patch == "diff\n"
    # The unavailable result stops subsequent batches; samples already in the
    # first bounded batch may all have been dispatched concurrently.
    assert verifier.verify.call_count == 3


def test_pipeline_unavailable_exact_candidate_hands_off_with_run_link(monkeypatch):
    from scripts.ci_fix.verify.base import VerificationResult

    baseline_failed = VerificationResult(
        verified=False,
        ran=True,
        detail="reproduced",
        run_url="https://run/base",
    )
    candidate_unavailable = VerificationResult(
        verified=False,
        ran=False,
        detail="runner was cancelled",
        run_url="https://run/cancelled",
    )
    verifier = MagicMock()
    verifier.verify.side_effect = [baseline_failed, candidate_unavailable]

    outcome = _run_pipeline(
        monkeypatch,
        exact_verifier=verifier,
        baseline_runs=1,
        verify_runs=2,
        reset=lambda *a, **k: None,
        apply=lambda *a, **k: (True, ("tests/exact.tcl",)),
        patch_review=lambda *a, **k: _patch_review(True),
    )

    assert outcome.kind is OutcomeKind.HANDOFF
    assert "missing remote evidence" in outcome.summary
    assert outcome.verification_run_url == "https://run/cancelled"
    assert outcome.handoff_patch == "diff\n"


def test_remote_candidate_samples_are_bounded_and_result_order_is_deterministic():
    from scripts.ci_fix.pipeline import _verify_remote_repeatedly
    from scripts.ci_fix.verify.base import (
        VerificationPlan,
        VerificationResult,
        VerifyEnv,
    )

    barrier = threading.Barrier(2)
    lock = threading.Lock()
    state = {"active": 0, "max_active": 0}
    seen = []

    class Verifier:
        def verify(self, _repo, sample_plan, _patch):
            with lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                seen.append((sample_plan.repetition, sample_plan.timeout_seconds))
            barrier.wait(timeout=2)
            # Repetition 2 completes first, but repetition 1 must still define
            # the deterministic first failure returned to retry feedback.
            if sample_plan.repetition == 1:
                time.sleep(0.02)
            with lock:
                state["active"] -= 1
            return VerificationResult(
                verified=False,
                ran=True,
                detail=f"failed repetition {sample_plan.repetition}",
            )

    result = _verify_remote_repeatedly(
        "/repo",
        VerificationPlan(env=VerifyEnv.TARGET, command=""),
        "diff\n",
        verifier=Verifier(),
        runs=4,
        parallelism=2,
        sample_timeout_seconds=37,
        deadline=time.monotonic() + 5,
    )

    assert state["max_active"] == 2
    assert sorted(repetition for repetition, _timeout in seen) == [1, 2]
    assert all(1 <= timeout <= 37 for _repetition, timeout in seen)
    assert result.detail == "failed repetition 1"


def test_remote_candidate_budget_stops_before_next_batch():
    from scripts.ci_fix.pipeline import _verify_remote_repeatedly
    from scripts.ci_fix.verify.base import (
        VerificationPlan,
        VerificationResult,
        VerifyEnv,
    )

    calls = []

    class Verifier:
        def verify(self, _repo, sample_plan, _patch):
            calls.append(sample_plan.repetition)
            time.sleep(0.08)
            return VerificationResult(verified=True, ran=True, detail="passed")

    result = _verify_remote_repeatedly(
        "/repo",
        VerificationPlan(env=VerifyEnv.TARGET, command=""),
        "diff\n",
        verifier=Verifier(),
        runs=5,
        parallelism=2,
        sample_timeout_seconds=30,
        deadline=time.monotonic() + 0.03,
    )

    assert sorted(calls) == [1, 2]
    assert result.ran is False
    assert "budget was exhausted" in result.detail


# --- _classify_failing_job over real workflow files ---

def _write_workflows(tmp_path, files):
    wf = tmp_path / ".github" / "workflows"
    wf.mkdir(parents=True)
    for name, body in files.items():
        (wf / name).write_text(body)
    return tmp_path


def test_classify_finds_job_in_workflow(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job
    from scripts.ci_fix.verify.base import VerifyEnv
    repo = _write_workflows(tmp_path, {
        "ci.yml": "jobs:\n  build-mac:\n    runs-on: macos-latest\n    steps:\n      - run: make\n",
    })
    env = _classify_failing_job(repo, "build-mac")
    assert env.env is VerifyEnv.MACOS


def test_valkey_daily_host_jobs_select_agent_actions_verifiers(tmp_path):
    from scripts.ci_fix.models import FixRequest
    from scripts.ci_fix.pipeline import _plan_verification, _verifier_for_plan
    from scripts.ci_fix.verify.base import VerificationPlan, VerifyEnv

    repo = _write_workflows(tmp_path, {
        "daily.yml": """
jobs:
  test-ubuntu-jemalloc:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ${{ inputs.use_repo || github.repository }}
          ref: ${{ inputs.use_git_ref || github.ref }}
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          ref: b9e40069c0b47a722286b94eb5231f7f05c08713
          path: libbacktrace
      - run: cd libbacktrace && ./configure && make && sudo make install
  test-macos-latest:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
      - name: Install libbacktrace
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd
        with:
          repository: ianlancetaylor/libbacktrace
          ref: b9e40069c0b47a722286b94eb5231f7f05c08713
          path: libbacktrace
""",
    })
    request = FixRequest(
        repo_full_name="valkey-io/valkey",
        pr_number=3988,
        head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/8.0",
        head_sha="a" * 40,
        run_id=123,
        requested_by="maintainer",
    )
    linux_verifier = MagicMock(name="linux-actions-verifier")
    macos_verifier = MagicMock(name="macos-actions-verifier")

    for job, expected_env, expected_verifier in (
        ("test-ubuntu-jemalloc", VerifyEnv.LOCAL, linux_verifier),
        ("test-macos-latest", VerifyEnv.MACOS, macos_verifier),
    ):
        proposal = FixProposal(
            path=FixPath.AUTHOR,
            failing_check="targeted test",
            root_cause="known race",
            reasoning="fix the race",
            confidence=0.9,
            failing_job_hint=job,
            build_command="make",
            verify_command="./runtest --single unit/x",
        )
        plan = _plan_verification(repo, request, proposal, (job,))

        assert isinstance(plan, VerificationPlan)
        assert plan.env is expected_env
        assert _verifier_for_plan(
            plan,
            linux_verifier=linux_verifier,
            macos_verifier=macos_verifier,
            exact_verifier=None,
        ) is expected_verifier


def test_classify_ambiguous_cross_workflow_refuses(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job
    from scripts.ci_fix.verify.base import VerifyEnv
    repo = _write_workflows(tmp_path, {
        "a.yml": "jobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n",
        "b.yml": "jobs:\n  test:\n    runs-on: macos-latest\n    steps:\n      - run: make\n",
    })
    env = _classify_failing_job(repo, "test")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert "multiple workflows" in env.reason


def test_classify_supported_and_exact_only_duplicate_fails_closed(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job
    from scripts.ci_fix.verify.base import VerifyEnv

    repo = _write_workflows(tmp_path, {
        "a.yml": "jobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n",
        "b.yml": (
            "jobs:\n"
            "  test:\n"
            "    runs-on: ubuntu-latest\n"
            "    services:\n"
            "      valkey:\n"
            "        image: valkey/valkey:latest\n"
        ),
    })

    env = _classify_failing_job(repo, "test")

    assert env.env is VerifyEnv.UNSUPPORTED
    assert "matching workflow definition" in env.reason
    assert "services" in env.reason


def test_classify_missing_job_refuses(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job
    from scripts.ci_fix.verify.base import VerifyEnv
    repo = _write_workflows(tmp_path, {
        "ci.yml": "jobs:\n  other:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n",
    })
    assert _classify_failing_job(repo, "nope").env is VerifyEnv.UNSUPPORTED


# --- _match_failed_job ambiguity ---

def test_match_failed_job_exact_and_single_base():
    from scripts.ci_fix.pipeline import _match_failed_job
    assert _match_failed_job("build", ("build", "lint")) == "build"
    # matrix suffix: single base-name match resolves
    assert _match_failed_job("test", ("test (clang)",)) == "test (clang)"


def test_match_failed_job_refuses_ambiguous_matrix():
    from scripts.ci_fix.pipeline import _match_failed_job
    # two matrix legs share the base name "test" -> ambiguous -> None
    assert _match_failed_job("test", ("test (a)", "test (b)")) is None


def test_match_failed_job_none_when_not_failed():
    from scripts.ci_fix.pipeline import _match_failed_job
    assert _match_failed_job("other", ("build",)) is None


def test_read_workflow_safely_skips_symlink_and_oversized(tmp_path):
    from scripts.ci_fix.pipeline import _MAX_WORKFLOW_BYTES, _read_workflow_safely

    good = tmp_path / "ok.yml"
    good.write_text("jobs: {}\n")
    assert _read_workflow_safely(good) == "jobs: {}\n"

    big = tmp_path / "big.yml"
    big.write_text("x" * (_MAX_WORKFLOW_BYTES + 1))
    assert _read_workflow_safely(big) is None

    target = tmp_path / "target.yml"
    target.write_text("jobs: {}\n")
    link = tmp_path / "link.yml"
    link.symlink_to(target)
    assert _read_workflow_safely(link) is None

    assert _read_workflow_safely(tmp_path / "missing.yml") is None


def test_pipeline_linux_verifier_unavailable_hands_off_without_local_fallback(monkeypatch):
    from scripts.ci_fix.review import PatchReview

    patch = "--- a/src/x.c\n+++ b/src/x.c\n"
    outcome = _run_pipeline(
        monkeypatch,
        linux_verifier=None,
        apply=lambda *a, **k: (True, ("src/x.c",)),
        patch_review=lambda *a, **k: PatchReview(
            ok=True,
            patch=patch,
            review=ReviewVerdict(True, "looks sane"),
        ),
    )
    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.handoff_patch == patch
    assert "Linux Actions verification is not configured" in outcome.summary
