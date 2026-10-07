"""Git workspace operations for scheduled backport sweeps."""

from __future__ import annotations

import logging
import os
import re
import subprocess
from typing import Any, Callable

from scripts.backport.git_commands import run_git as run_git_default
from scripts.backport.models import CandidateResult
from scripts.backport.sweep_models import DETAIL_ALREADY_ON_SWEEP_BRANCH
from scripts.backport.utils import pr_numbers_from_commit_messages
from scripts.common.git_auth import github_https_url
from scripts.common.identity import BOT_EMAIL, BOT_NAME
from scripts.common.proc import GitCommandError

logger = logging.getLogger(__name__)


RunGit = Callable[..., Any]

BRANCH_PREFIX = "agent/backport/sweep"


def safe_tmp_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "branch"


def clone_target_branch(
    repo_full_name: str,
    target_branch: str,
    dest_dir: str,
    git_env: dict[str, str],
) -> None:
    clone_url = github_https_url(repo_full_name)
    cmd = ["git", "clone", "--branch", target_branch, clone_url, dest_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, env=git_env)
    if result.returncode != 0:
        # The error's message carries git's reason (auth, missing branch).
        raise GitCommandError(result.returncode, cmd, result.stdout, result.stderr)
    run_git_default(dest_dir, "config", "user.name", BOT_NAME)
    run_git_default(dest_dir, "config", "user.email", BOT_EMAIL)


def push_backport_branch(
    repo_dir: str,
    branch: str,
    git_env: dict[str, str],
    *,
    push_repo: str,
    prepared_head: str,
    expected_remote_head: str | None,
    branch_prefix: str = BRANCH_PREFIX,
    run_git: RunGit = run_git_default,
) -> None:
    if not branch.startswith(f"{branch_prefix}/"):
        raise RuntimeError(
            f"Refusing to push to non-namespaced branch: {branch!r}. "
            f"Agent push targets must start with {branch_prefix}/."
        )
    destination = f"refs/heads/{branch}"
    args = [
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "credential.helper=",
        "push",
        f"--force-with-lease={destination}:{expected_remote_head or ''}",
        github_https_url(push_repo),
        f"{prepared_head}:{destination}",
    ]
    run_git(repo_dir, *args, env=git_env)


def list_already_applied(repo_dir: str, base_branch: str, backport_branch: str) -> set[str]:
    return {
        str(result.source_pr_number)
        for result in list_applied_prs_on_branch(repo_dir, base_branch, backport_branch)
    }


def list_applied_prs_on_branch(
    repo_dir: str,
    base_branch: str,
    backport_branch: str,
) -> list[CandidateResult]:
    result = subprocess.run(
        [
            "git",
            "log",
            "--reverse",
            f"origin/{base_branch}..{backport_branch}",
            "--format=%B%x00",
        ],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    applied: list[CandidateResult] = []
    seen: set[int] = set()
    for message in result.stdout.split("\x00"):
        message = message.strip()
        if not message:
            continue
        matched = pr_numbers_from_commit_messages([message])
        if not matched:
            continue
        subject = message.splitlines()[0]
        title = re.sub(r"\s*\(#\d+\)\s*$", "", subject).strip() or subject.strip()
        for pr_number in sorted(matched):
            if pr_number in seen:
                continue
            seen.add(pr_number)
            applied.append(
                CandidateResult(
                    source_pr_number=pr_number,
                    source_pr_title=title,
                    outcome="skipped-existing",
                    detail=DETAIL_ALREADY_ON_SWEEP_BRANCH,
                )
            )
    return applied


RunProcess = Callable[..., subprocess.CompletedProcess[Any]]


def changed_paths_in_index_or_worktree(
    repo_dir: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> tuple[str, ...]:
    """Return staged, unstaged, and untracked paths with exact git path names."""
    return collect_git_paths_z(
        repo_dir,
        (
            ("git", "diff", "--name-only", "-z"),
            ("git", "diff", "--cached", "--name-only", "-z"),
            ("git", "ls-files", "--others", "--exclude-standard", "-z"),
        ),
        run_process=run_process,
    )


def untracked_paths(
    repo_dir: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> tuple[str, ...]:
    """Return non-ignored untracked paths with exact Git path names."""
    return collect_git_paths_z(
        repo_dir,
        (("git", "ls-files", "--others", "--exclude-standard", "-z"),),
        run_process=run_process,
    )


def worktree_changed_paths(
    repo_dir: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> tuple[str, ...]:
    return collect_git_paths_z(
        repo_dir,
        (
            ("git", "diff", "--name-only", "-z", "HEAD"),
            ("git", "ls-files", "--others", "--exclude-standard", "-z"),
        ),
        run_process=run_process,
    )


def collect_git_paths_z(
    repo_dir: str,
    commands: tuple[tuple[str, ...], ...],
    *,
    run_process: RunProcess = subprocess.run,
) -> tuple[str, ...]:
    paths: set[str] = set()
    for command in commands:
        result = run_process(
            list(command),
            cwd=repo_dir,
            capture_output=True,
            text=False,
        )
        if result.returncode != 0:
            stderr = os.fsdecode(result.stderr).strip()
            raise RuntimeError(
                f"could not collect changed paths with {' '.join(command)} "
                f"(exit {result.returncode}): "
                + (stderr[:300] or "git command failed")
            )
        stdout = result.stdout
        parts = stdout.split(b"\0") if isinstance(stdout, bytes) else str(stdout).split("\0")
        paths.update(os.fsdecode(path) for path in parts if path)
    return tuple(sorted(paths))


def branch_has_changes(repo_dir: str, target_branch: str) -> bool:
    return _diff_is_nonempty(repo_dir, f"origin/{target_branch}...HEAD")


def has_changes_since(repo_dir: str, base_ref: str) -> bool:
    """Return whether HEAD's tree differs from ``base_ref``'s tree."""
    return _diff_is_nonempty(repo_dir, base_ref, "HEAD")


def _diff_is_nonempty(repo_dir: str, *revs: str) -> bool:
    result = subprocess.run(
        ["git", "diff", "--quiet", *revs],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    raise RuntimeError(
        f"could not compare {' '.join(revs)}: "
        + (result.stderr.strip()[:300] or "git diff failed")
    )
