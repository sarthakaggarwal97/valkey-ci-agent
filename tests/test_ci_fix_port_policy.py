from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.ci_fix.port_policy import PortRefused, verify_portable_commit


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, str, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "unstable")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "base.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "base.txt")
    _git(repo, "commit", "-qm", "base")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "fix.txt").write_text("fix\n", encoding="utf-8")
    _git(repo, "add", "fix.txt")
    _git(repo, "commit", "-qm", "fix")
    fix = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-qb", "side", base)
    (repo / "side.txt").write_text("side\n", encoding="utf-8")
    _git(repo, "add", "side.txt")
    _git(repo, "commit", "-qm", "side")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "remote", "add", "origin", str(repo))
    _git(repo, "update-ref", "refs/remotes/origin/unstable", fix)
    _git(repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/unstable")
    return repo, base, fix, side


def test_port_policy_accepts_merged_commit_missing_from_head(
    tmp_path: Path,
) -> None:
    repo, base, fix, _side = _repository(tmp_path)
    verify_portable_commit(str(repo), fix, base)


def test_port_policy_rejects_unmerged_or_already_present_commit(
    tmp_path: Path,
) -> None:
    repo, _base, fix, side = _repository(tmp_path)
    with pytest.raises(PortRefused, match="not reachable"):
        verify_portable_commit(str(repo), side, fix)
    with pytest.raises(PortRefused, match="already present"):
        verify_portable_commit(str(repo), fix, fix)
