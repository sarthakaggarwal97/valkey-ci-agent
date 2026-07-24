from __future__ import annotations

import subprocess

import pytest

from scripts.backport import revert_commit as rc
from scripts.backport import sweep_git
from scripts.backport.provenance import (
    ManifestEntry,
    parse_manifest,
    parse_trailers,
    render_manifest,
)
from scripts.backport.provenance_history import (
    AppliedBackport,
    scan_applied_backports,
)


def _git(cwd, *args):
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.fixture
def fork(tmp_path, monkeypatch):
    """A bare remote with a release branch and an agent branch on top of it.

    Layout: base commit on `8.0`; the agent branch `agent/backport/sweep/8.0`
    adds one more commit. github_https_url is redirected to the bare repo so
    revert_commit operates against local git.
    """
    subprocess.run(["git", "config", "--global", "user.email", "t@t.com"], check=True)
    subprocess.run(["git", "config", "--global", "user.name", "t"], check=True)

    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    work = tmp_path / "work"
    subprocess.run(["git", "clone", "-q", str(bare), str(work)], check=True)

    _git(work, "checkout", "-q", "-b", "8.0")
    (work / "base.txt").write_text("base\n")
    _git(work, "add", "base.txt")
    _git(work, "commit", "-q", "-m", "base commit")
    base_sha = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", "8.0")

    _git(work, "checkout", "-q", "-b", "agent/backport/sweep/8.0")
    (work / "feature.txt").write_text("feature\n")
    _git(work, "add", "feature.txt")
    _git(work, "commit", "-q", "-m", "backported feature (#42)")
    agent_sha = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", "agent/backport/sweep/8.0")

    monkeypatch.setattr("scripts.backport.sweep_git.github_https_url", lambda _r: str(bare))
    monkeypatch.setattr(rc, "_note_pr", lambda *a, **k: None)
    return sweep_git, work, base_sha, agent_sha


def _agent_head(work):
    _git(work, "fetch", "-q", "origin")
    return _git(work, "rev-parse", "origin/agent/backport/sweep/8.0")


def test_reverts_agent_commit(fork):
    _, work, _base, agent_sha = fork
    rc.revert_commit("o/r", "agent/backport/sweep/8.0", agent_sha, token="", base_branch="8.0")

    _git(work, "fetch", "-q", "origin")
    subjects = _git(work, "log", "--format=%s", "origin/agent/backport/sweep/8.0").splitlines()
    assert subjects[0] == 'Revert "backported feature (#42)"'
    assert "backported feature (#42)" in subjects
    records = parse_trailers(
        _git(
            work,
            "show",
            "-s",
            "--format=%B",
            "origin/agent/backport/sweep/8.0",
        )
    )
    assert records and all(record.kind == "inverse" for record in records)
    assert scan_applied_backports(
        str(work),
        "origin/8.0..origin/agent/backport/sweep/8.0",
    ) == []


def test_refuses_base_branch_commit(fork):
    _, work, base_sha, _agent = fork
    before = _agent_head(work)

    with pytest.raises(RuntimeError, match="not unique"):
        rc.revert_commit("o/r", "agent/backport/sweep/8.0", base_sha, token="", base_branch="8.0")

    assert _agent_head(work) == before


def test_rejects_non_agent_branch():
    with pytest.raises(ValueError, match="non-namespaced branch"):
        rc.revert_commit("o/r", "8.0", "abc1234", token="")


def test_note_pr_replaces_stale_manifest(monkeypatch):
    class Pull:
        number = 12
        body = (
            "Report\n\n"
            + render_manifest(
                [
                    ManifestEntry(41, "Reverted source"),
                    ManifestEntry(42, "Still applied"),
                ]
            )
        )

        def edit(self, *, body):
            self.body = body

    pull = Pull()
    monkeypatch.setattr(rc, "Github", lambda **_kwargs: object())
    monkeypatch.setattr(rc, "find_existing_pr", lambda *_args: pull)

    rc._note_pr(
        "o/r",
        "fork/r",
        "agent/backport/sweep/8.0",
        "a" * 40,
        "Reverted source",
        "token",
        [AppliedBackport(42, "Still applied")],
    )

    assert parse_manifest(pull.body) == (
        ManifestEntry(42, "Still applied"),
    )
    assert "Reverted `aaaaaaaaaaaa`" in pull.body
