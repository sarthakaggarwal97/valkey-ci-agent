"""Shared Git command execution for backport worktrees."""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Mapping
from typing import Callable

logger = logging.getLogger(__name__)

RunProcess = Callable[..., subprocess.CompletedProcess[str]]


def run_git(
    repo_dir: str,
    *args: str,
    env: Mapping[str, str] | None = None,
) -> None:
    """Run Git in *repo_dir*, raising with bounded diagnostics on failure."""

    cmd = ["git", *args]
    logger.debug("Running: %s (cwd=%s)", " ".join(cmd), repo_dir)
    result = subprocess.run(
        cmd,
        cwd=repo_dir,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        return
    logger.error(
        "git %s failed (rc=%d)\nstdout: %s\nstderr: %s",
        args[0],
        result.returncode,
        result.stdout.strip()[-500:] if result.stdout else "",
        result.stderr.strip()[-500:] if result.stderr else "",
    )
    result.check_returncode()


def has_staged_changes(
    repo_dir: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> bool:
    result = run_process(
        ["git", "diff", "--cached", "--quiet"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    raise RuntimeError(
        "could not inspect staged changes: "
        + ((result.stderr or "").strip()[:300] or "git diff failed")
    )


def index_stage_exists(
    repo_dir: str,
    path: str,
    stage: int,
    *,
    run_process: RunProcess = subprocess.run,
) -> bool:
    result = run_process(
        ["git", "cat-file", "-e", f":{stage}:{path}"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def read_index_stage(
    repo_dir: str,
    path: str,
    stage: int,
    *,
    run_process: RunProcess = subprocess.run,
) -> str:
    result = run_process(
        ["git", "show", f":{stage}:{path}"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        errors="replace",
    )
    return result.stdout if result.returncode == 0 else ""
