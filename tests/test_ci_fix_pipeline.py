"""Tests for the push step, comment renderer, and pipeline orchestration."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.gate import GateRejection, parse_command
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    ReviewVerdict,
    RunResult,
)
from scripts.ci_fix.pipeline import run_ci_fix
from scripts.ci_fix.push import PushRefused, commit_and_push_fix

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"


def _proposal(path: FixPath = FixPath.AUTHOR) -> FixProposal:
    return FixProposal(
        path=path, failing_test="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11", reasoning="scaffolding fix",
        confidence=0.9, build_command="make", test_command="./runtest --single x",
    )


def _passed_run() -> RunResult:
    return RunResult(ran=True, passed=True, exit_code=0,
                     command="make && ./runtest --single x", output_tail="All tests passed")


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


def test_push_commits_with_dco_and_pushes(tmp_path, monkeypatch):
    """A real local repo: the fix is committed with a sign-off and pushed to a bare remote."""
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
    assert "Signed-off-by:" in msg
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
        proposal=_proposal(), run_result=_passed_run(),
        review=ReviewVerdict(approved=True, reasoning="minimal and correct"),
        commit_sha="abcdef1234567890",
    )
    body = render_comment(outcome)
    assert "NAN score" in body
    assert "abcdef123456" in body
    assert "make && ./runtest" in body
    assert "minimal and correct" in body
    assert "do not merge" in body.lower()


def test_render_refused_comment_explains():
    outcome = FixOutcome(
        kind=OutcomeKind.REFUSED,
        summary="genuinely flaky timing failure; no safe fix",
        other_failing_tests=("other test",),
    )
    body = render_comment(outcome)
    assert "did not push" in body.lower()
    assert "flaky" in body
    assert "other test" in body


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
    run = SimpleNamespace(head_sha=run_head_sha, head_branch="agent/backport/sweep/8.0")
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


def _run_pipeline(monkeypatch, **overrides):
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha",
                        overrides.get("clone", lambda *a, **k: True))
    return run_ci_fix(
        _gh_authorized(), command=parse_command(f"@valkeyrie-bot fix {_RUN_URL}"),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
        git_env={}, artifact_client=_artifact_client(overrides.get("logs", {"1.txt": b"err"})),
        diagnose_func=overrides.get("diagnose", lambda *a, **k: _proposal()),
        run_loop_func=overrides.get("loop", lambda *a, **k: _loop_success()),
        push_func=overrides.get("push", lambda *a, **k: "deadbeef" * 5),
    )


def _loop_success():
    from scripts.ci_fix.review import LoopResult
    return LoopResult(success=True, run_result=_passed_run(),
                      review=ReviewVerdict(True, "ok"), changed_paths=("test.tcl",),
                      attempts=1, detail="ok")


def _loop_failure():
    from scripts.ci_fix.review import LoopResult
    return LoopResult(success=False, run_result=None, review=None,
                      changed_paths=(), attempts=3, detail="test still failing")


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
    assert "unavailable" in outcome.summary


def test_pipeline_clone_failure(monkeypatch):
    outcome = _run_pipeline(monkeypatch, clone=lambda *a, **k: False)
    assert outcome.kind is OutcomeKind.FAILED
    assert "clone" in outcome.summary


def test_pipeline_diagnose_refuses(monkeypatch):
    outcome = _run_pipeline(monkeypatch, diagnose=lambda *a, **k: _proposal(FixPath.REFUSE))
    assert outcome.kind is OutcomeKind.REFUSED


def test_pipeline_loop_failure(monkeypatch):
    outcome = _run_pipeline(monkeypatch, loop=lambda *a, **k: _loop_failure())
    assert outcome.kind is OutcomeKind.REFUSED
    assert "still failing" in outcome.summary


def test_pipeline_push_refused(monkeypatch):
    def refuse(*a, **k):
        raise PushRefused("bad branch namespace")
    outcome = _run_pipeline(monkeypatch, push=refuse)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "bad branch namespace" in outcome.summary
