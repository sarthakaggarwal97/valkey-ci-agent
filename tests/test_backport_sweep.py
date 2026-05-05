from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

from scripts.backport import sweep as backport_sweep
from scripts.backport.models import ResolutionResult
from scripts.backport.sweep import ProjectBackportCandidate
from scripts.common.git_auth import GitAuth


def test_git_auth_keeps_askpass_outside_clone_destination(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with GitAuth("token", prefix="test-git-auth-") as git_auth:
        env = git_auth.env()
        askpass = Path(env["GIT_ASKPASS"])
        assert askpass.exists()
        assert askpass.parent != repo_dir
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert env["GIT_PASSWORD"] == "token"
    assert not askpass.exists()


def test_apply_candidate_aborts_empty_cherry_pick(monkeypatch, tmp_path):
    candidate = ProjectBackportCandidate(
        source_pr_number=10,
        source_pr_title="Already applied",
        source_pr_url="https://github.com/valkey-io/valkey/pull/10",
        target_branch="8.1",
        merge_commit_sha="abc123",
    )
    git_calls: list[tuple[str, ...]] = []
    subprocess_calls: list[list[str]] = []

    def fake_run_git(_repo_dir, *args, **_kwargs):
        git_calls.append(args)

    def fake_subprocess_run(cmd, **_kwargs):
        subprocess_calls.append(cmd)
        if cmd[:2] == ["git", "cherry-pick"]:
            return subprocess.CompletedProcess(
                cmd,
                1,
                stdout="",
                stderr="The previous cherry-pick is now empty",
            )
        if cmd[:4] == ["git", "diff", "--name-only", "--diff-filter=U"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:3] == ["git", "cherry-pick", "--abort"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backport_sweep, "_run_git", fake_run_git)
    monkeypatch.setattr(backport_sweep.subprocess, "run", fake_subprocess_run)

    result = backport_sweep._apply_candidate(
        str(tmp_path),
        candidate,
        MagicMock(),
        "valkey-io/valkey",
        {},
    )

    assert result.outcome == "skipped-existing"
    assert result.detail == "already applied or empty cherry-pick"
    assert ("fetch", "origin", "abc123") in git_calls
    assert ["git", "cherry-pick", "--abort"] in subprocess_calls


def test_apply_candidate_skips_noop_conflict_resolution(monkeypatch, tmp_path):
    conflicted_file = tmp_path / "conflict.txt"
    conflicted_file.write_text("target content\n", encoding="utf-8")
    candidate = ProjectBackportCandidate(
        source_pr_number=3317,
        source_pr_title="Fix macOS workflow",
        source_pr_url="https://github.com/valkey-io/valkey/pull/3317",
        target_branch="8.1",
        merge_commit_sha="abc123",
    )
    git_calls: list[tuple[str, ...]] = []
    subprocess_calls: list[list[str]] = []

    def fake_run_git(_repo_dir, *args, **_kwargs):
        git_calls.append(args)

    def fake_subprocess_run(cmd, **_kwargs):
        subprocess_calls.append(cmd)
        if cmd[:2] == ["git", "cherry-pick"] and "--abort" not in cmd:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="conflict")
        if cmd[:4] == ["git", "diff", "--name-only", "--diff-filter=U"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="conflict.txt\n", stderr="")
        if cmd[:2] == ["git", "show"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="target content\n", stderr="")
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:4] == ["git", "diff", "--cached", "--quiet"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:3] == ["git", "cherry-pick", "--abort"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backport_sweep, "_run_git", fake_run_git)
    monkeypatch.setattr(backport_sweep.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        backport_sweep,
        "resolve_conflicts_with_claude",
        lambda *_args, **_kwargs: [
            ResolutionResult(
                path="conflict.txt",
                resolved_content="target content\n",
                resolution_summary="resolved",
            )
        ],
    )

    result = backport_sweep._apply_candidate(
        str(tmp_path),
        candidate,
        MagicMock(),
        "valkey-io/valkey",
        {},
    )

    assert result.outcome == "skipped-existing"
    assert result.detail == "resolution was already satisfied on target branch"
    assert ("add", "conflict.txt") in git_calls
    assert ["git", "commit", "--no-edit"] not in subprocess_calls
    assert ["git", "cherry-pick", "--abort"] in subprocess_calls


def test_apply_candidate_does_not_recreate_target_missing_file(monkeypatch, tmp_path):
    missing_on_target = tmp_path / "src" / "cluster_legacy.c"
    missing_on_target.parent.mkdir()
    missing_on_target.write_text("<<<<<<< HEAD\n=======\nlarge source file\n>>>>>>> source\n", encoding="utf-8")
    candidate = ProjectBackportCandidate(
        source_pr_number=2174,
        source_pr_title="Converge divergent shard-id",
        source_pr_url="https://github.com/valkey-io/valkey/pull/2174",
        target_branch="7.2",
        merge_commit_sha="def456",
    )
    git_calls: list[tuple[str, ...]] = []
    subprocess_calls: list[list[str]] = []

    def fake_run_git(_repo_dir, *args, **_kwargs):
        git_calls.append(args)

    def fake_subprocess_run(cmd, **_kwargs):
        subprocess_calls.append(cmd)
        if cmd[:2] == ["git", "cherry-pick"] and "--abort" not in cmd:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="conflict")
        if cmd[:4] == ["git", "diff", "--name-only", "--diff-filter=U"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="src/cluster_legacy.c\n", stderr="")
        if cmd[:2] == ["git", "show"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="large source file\n", stderr="")
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="missing")
        if cmd[:4] == ["git", "diff", "--cached", "--quiet"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:3] == ["git", "cherry-pick", "--abort"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backport_sweep, "_run_git", fake_run_git)
    monkeypatch.setattr(backport_sweep.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        backport_sweep,
        "resolve_conflicts_with_claude",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not call Claude")),
    )

    result = backport_sweep._apply_candidate(
        str(tmp_path),
        candidate,
        MagicMock(),
        "valkey-io/valkey",
        {},
    )

    assert result.outcome == "skipped-conflict"
    assert result.detail == "target branch lacks conflicted file(s): src/cluster_legacy.c"
    assert ("add", "src/cluster_legacy.c") not in git_calls
    assert missing_on_target.exists()
    assert ["git", "commit", "--no-edit"] not in subprocess_calls
    assert ["git", "cherry-pick", "--abort"] in subprocess_calls


def test_run_test_commands_returns_failure_output(tmp_path):
    ok, output = backport_sweep._run_test_commands(
        str(tmp_path),
        ["printf stdout; printf stderr >&2; exit 3"],
    )

    assert ok is False
    assert "stdout" in output
    assert "stderr" in output
