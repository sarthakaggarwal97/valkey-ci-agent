"""Small git helpers for tests that build throwaway repositories."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def git(cwd: Path, *args: str, env: dict[str, str] | None = None) -> str:
    """Run git in ``cwd`` and return its stripped output.

    ``env`` adds variables to the inherited environment.
    """
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        env={**os.environ, **env} if env else None,
    ).stdout.strip()


def init_repo(path: Path, *, branch: str = "main") -> Path:
    """Create a repository at ``path`` with a test identity and unsigned commits."""
    path.mkdir(parents=True, exist_ok=True)
    git(path, "init", "-q", "-b", branch)
    git(path, "config", "user.name", "Test")
    git(path, "config", "user.email", "test@example.com")
    git(path, "config", "commit.gpgsign", "false")
    return path


def commit(repo: Path, path: str, content: str, message: str) -> str:
    """Write ``path``, commit it with ``message``, and return the new commit."""
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    git(repo, "add", path)
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")
