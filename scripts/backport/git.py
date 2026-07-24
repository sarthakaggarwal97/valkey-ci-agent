"""Shared Git command execution for backport worktrees."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
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


def git_output(repo_dir: str, *args: str) -> str:
    """Run Git and return stripped stdout."""

    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: "
            + ((result.stderr or "").strip()[:300] or "git command failed")
        )
    return result.stdout.strip()


def head_sha(repo_dir: str) -> str:
    return git_output(repo_dir, "rev-parse", "HEAD")


def changed_paths_between(
    repo_dir: str,
    base_ref: str,
    tip_ref: str,
) -> tuple[str, ...]:
    """Return exact path names changed between two committed trees."""

    return _git_paths(
        repo_dir,
        "diff",
        "--name-only",
        "-z",
        base_ref,
        tip_ref,
    )


def tracked_worktree_changes(repo_dir: str) -> tuple[str, ...]:
    """Return staged or unstaged tracked paths that differ from HEAD."""

    return _git_paths(repo_dir, "diff", "--name-only", "-z", "HEAD")


def _git_paths(repo_dir: str, *args: str) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=False,
    )
    if result.returncode != 0:
        stderr = os.fsdecode(result.stderr).strip()
        raise RuntimeError(
            f"git {' '.join(args)} failed: " + (stderr[:300] or "git command failed")
        )
    return tuple(
        sorted(os.fsdecode(value) for value in result.stdout.split(b"\0") if value)
    )


@contextmanager
def detached_worktree(
    repo_dir: str,
    *,
    prefix: str = "backport-worktree-",
) -> Iterator[str]:
    """Yield an isolated worktree at HEAD and remove all of its artifacts."""

    root = Path(tempfile.mkdtemp(prefix=prefix))
    worktree = root / "repo"
    added = False
    try:
        run_git(
            repo_dir,
            "worktree",
            "add",
            "--detach",
            str(worktree),
            "HEAD",
        )
        added = True
        yield str(worktree)
    finally:
        if added:
            removal = subprocess.run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=repo_dir,
                capture_output=True,
                text=True,
            )
            if removal.returncode != 0:
                logger.warning(
                    "Could not remove validation worktree %s: %s",
                    worktree,
                    (removal.stderr or removal.stdout).strip()[:300],
                )
                shutil.rmtree(worktree, ignore_errors=True)
                subprocess.run(
                    ["git", "worktree", "prune"],
                    cwd=repo_dir,
                    capture_output=True,
                    text=True,
                )
        shutil.rmtree(root, ignore_errors=True)


def promote_detached_head(
    repo_dir: str,
    *,
    expected_head: str,
    validated_head: str,
) -> None:
    """Fast-forward the checked-out branch to an isolated validated commit."""

    actual_head = head_sha(repo_dir)
    if actual_head != expected_head:
        raise RuntimeError(
            "backport branch moved during candidate validation: "
            f"expected {expected_head}, found {actual_head}"
        )
    dirty = tracked_worktree_changes(repo_dir)
    if dirty:
        raise RuntimeError(
            "backport branch has tracked workspace changes before promotion: "
            + ", ".join(dirty[:10])
        )
    run_git(repo_dir, "merge", "--ff-only", validated_head)


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
