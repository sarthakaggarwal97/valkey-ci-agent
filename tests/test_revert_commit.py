from __future__ import annotations

import subprocess

import pytest

from scripts.backport import revert_commit as subject


def _git(cwd, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def legacy_branch(tmp_path, monkeypatch):
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    work = tmp_path / "work"
    subprocess.run(
        ["git", "clone", "-q", str(bare), str(work)],
        check=True,
    )
    _git(work, "config", "user.email", "test@example.com")
    _git(work, "config", "user.name", "Test")
    _git(work, "checkout", "-q", "-b", "8.0")
    (work / "base.txt").write_text("base\n", encoding="utf-8")
    _git(work, "add", "base.txt")
    _git(work, "commit", "-q", "-m", "base commit")
    base_sha = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", "8.0")

    branch = "agent/backport/sweep/8.0"
    _git(work, "checkout", "-q", "-b", branch)
    (work / "feature.txt").write_text("feature\n", encoding="utf-8")
    _git(work, "add", "feature.txt")
    _git(work, "commit", "-q", "-m", "backported feature")
    agent_sha = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", branch)

    monkeypatch.setattr(subject, "github_https_url", lambda _repo: str(bare))
    monkeypatch.setattr(subject, "_note_pr", lambda *_args: None)
    return work, base_sha, agent_sha


def test_reverts_only_commit_unique_to_legacy_branch(legacy_branch) -> None:
    work, _base_sha, agent_sha = legacy_branch
    subject.revert_commit(
        "owner/repo",
        "agent/backport/sweep/8.0",
        agent_sha,
        token="",
        base_branch="8.0",
    )

    _git(work, "fetch", "-q", "origin")
    log = _git(
        work,
        "log",
        "--format=%s",
        "origin/agent/backport/sweep/8.0",
    ).splitlines()
    assert log[0] == 'Revert "backported feature"'


def test_refuses_base_commit_and_non_agent_branch(legacy_branch) -> None:
    _work, base_sha, _agent_sha = legacy_branch
    with pytest.raises(RuntimeError, match="not unique"):
        subject.revert_commit(
            "owner/repo",
            "agent/backport/sweep/8.0",
            base_sha,
            token="",
            base_branch="8.0",
        )
    with pytest.raises(ValueError, match="non-namespaced"):
        subject.revert_commit("owner/repo", "8.0", base_sha, token="")
