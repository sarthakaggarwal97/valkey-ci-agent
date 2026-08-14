"""Git cherry-pick operations for backporting."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Callable

from scripts.backport.git import (
    CHERRY_PICK_GIT_MODE,
    run_git_command,
)
from scripts.backport.models import (
    CherryPickResult,
    ConflictedFile,
    ResolutionResult,
)

logger = logging.getLogger(__name__)

RunGit = Callable[..., subprocess.CompletedProcess[str]]
StageGit = Callable[..., object]


def cherry_pick(
    repo_dir: str,
    target_branch: str,
    merge_commit_sha: str | None,
    commit_shas: list[str],
) -> CherryPickResult:
    logger.info("Checking out target branch %s", target_branch)
    _run_git(repo_dir, "checkout", target_branch)

    if merge_commit_sha:
        return _cherry_pick_merge(repo_dir, target_branch, merge_commit_sha)
    return _cherry_pick_sequential(repo_dir, target_branch, commit_shas)


def _cherry_pick_merge(
    repo_dir: str,
    target_branch: str,
    merge_commit_sha: str,
) -> CherryPickResult:
    logger.info(
        "Cherry-picking merge commit %s onto %s",
        merge_commit_sha,
        target_branch,
    )
    result = cherry_pick_commit(
        repo_dir,
        merge_commit_sha,
        mainline=True,
        run_git=_run_git,
        log=logger,
    )
    if result.returncode != 0:
        logger.warning(
            "Cherry-pick of merge commit %s produced conflicts",
            merge_commit_sha,
        )
        conflicts = _collect_conflicts(repo_dir, target_branch)

        # Empty cherry-pick: the changes already exist on the target branch.
        # Abort and report a no-op so callers can skip creating empty PRs.
        if not conflicts and _is_empty_cherry_pick(result):
            logger.info(
                "No conflicting files — cherry-pick is empty/already applied.",
            )
            logger.debug(
                "Original cherry-pick stderr: %s",
                result.stderr.strip(),
            )
            _run_git(repo_dir, "cherry-pick", "--abort", check=False)
            return CherryPickResult(success=True, applied_commits=[])

        return CherryPickResult(
            success=False,
            conflicting_files=conflicts,
            applied_commits=[],
            conflicting_commit_sha=merge_commit_sha,
        )
    logger.info("Cherry-pick of merge commit %s succeeded", merge_commit_sha)
    return CherryPickResult(
        success=True,
        applied_commits=[merge_commit_sha],
    )


def _cherry_pick_sequential(
    repo_dir: str,
    target_branch: str,
    commit_shas: list[str],
) -> CherryPickResult:
    applied: list[str] = []
    for sha in commit_shas:
        logger.info("Cherry-picking commit %s onto %s", sha, target_branch)
        result = cherry_pick_commit(
            repo_dir,
            sha,
            mainline=False,
            run_git=_run_git,
            log=logger,
        )
        if result.returncode != 0:
            logger.warning(
                "Cherry-pick of commit %s produced conflicts", sha,
            )
            conflicts = _collect_conflicts(repo_dir, target_branch)
            if not conflicts and _is_empty_cherry_pick(result):
                logger.info(
                    "No conflicting files; cherry-pick is empty/already applied.",
                )
                logger.debug(
                    "Original cherry-pick stderr: %s",
                    result.stderr.strip(),
                )
                _run_git(repo_dir, "cherry-pick", "--abort", check=False)
                continue
            return CherryPickResult(
                success=False,
                conflicting_files=conflicts,
                applied_commits=applied,
                conflicting_commit_sha=sha,
            )
        applied.append(sha)
    logger.info("All %d commits cherry-picked cleanly", len(applied))
    return CherryPickResult(success=True, applied_commits=applied)


def _collect_conflicts(repo_dir: str, target_branch: str) -> list[ConflictedFile]:
    result = _run_git(repo_dir, "diff", "--name-only", "--diff-filter=U")
    paths = [p for p in result.stdout.strip().splitlines() if p]
    logger.info("Found %d conflicting file(s): %s", len(paths), paths)

    conflicts: list[ConflictedFile] = []
    for path in paths:
        # build_conflicted_file returns None for binary conflicts. A
        # cherry-pick left with only binary conflicts becomes an empty set
        # and is skipped by the caller.
        conflicted_file = _build_conflicted_file(repo_dir, target_branch, path)
        if conflicted_file is not None:
            conflicts.append(conflicted_file)
    return conflicts


def _build_conflicted_file(
    repo_dir: str,
    target_branch: str,
    file_path: str,
) -> ConflictedFile | None:
    # Target branch version (before cherry-pick)
    target_branch_content = _show_file(repo_dir, target_branch, file_path)

    # Source branch version (the commit being cherry-picked)
    source_branch_content = _show_file(repo_dir, "CHERRY_PICK_HEAD", file_path)

    return build_conflicted_file(
        file_path,
        target_branch_content,
        source_branch_content,
        log=logger,
    )


def _show_file(repo_dir: str, ref: str, file_path: str) -> str:
    result = _run_git(repo_dir, "show", f"{ref}:{file_path}", check=False)
    if result.returncode != 0:
        logger.warning(
            "Could not read %s:%s — %s",
            ref,
            file_path,
            result.stderr.strip(),
        )
        return ""
    return result.stdout


def _is_empty_cherry_pick(result: subprocess.CompletedProcess[str]) -> bool:
    output = f"{result.stdout}\n{result.stderr}".lower()
    return (
        "cherry-pick is now empty" in output
        or "previous cherry-pick is now empty" in output
        or "nothing to commit" in output
        or "patch is empty" in output
    )


def is_non_merge_mainline_error(output: str) -> bool:
    normalized = output.lower()
    return (
        "mainline was specified" in normalized
        and "is not a merge" in normalized
    )


def cherry_pick_commit(
    repo_dir: str,
    commit_sha: str,
    *,
    mainline: bool,
    run_git: RunGit,
    log: logging.Logger,
) -> subprocess.CompletedProcess[str]:
    """Attempt one commit, retrying a non-merge SHA without ``-m``."""
    args = ("cherry-pick", "-m", "1", commit_sha) if mainline else ("cherry-pick", commit_sha)
    result = run_git(repo_dir, *args, check=False)
    if mainline and result.returncode != 0 and is_non_merge_mainline_error(
        f"{result.stdout}\n{result.stderr}"
    ):
        log.info(
            "%s is not a merge commit; retrying cherry-pick without -m",
            commit_sha,
        )
        result = run_git(repo_dir, "cherry-pick", commit_sha, check=False)
    return result


def build_conflicted_file(
    path: str,
    target_content: str,
    source_content: str,
    *,
    log: logging.Logger,
) -> ConflictedFile | None:
    """Build a text conflict model, excluding binary conflicts."""
    if "\x00" in target_content or "\x00" in source_content:
        log.warning("Skipping binary conflict: %s", path)
        return None
    return ConflictedFile(
        path=path,
        target_branch_content=target_content,
        source_branch_content=source_content,
    )


def stage_resolutions(
    repo_dir: str,
    resolution_results: list[ResolutionResult],
    *,
    run_git: StageGit,
) -> None:
    """Write and stage resolved files, rejecting unresolved entries."""
    for result in resolution_results:
        if result.resolved_content is None:
            raise ValueError(f"Cannot apply unresolved conflict for {result.path}")
        file_path = Path(repo_dir, result.path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(result.resolved_content, encoding="utf-8")
        run_git(repo_dir, "add", result.path)


def _run_git(
    repo_dir: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return run_git_command(
        repo_dir,
        *args,
        mode=CHERRY_PICK_GIT_MODE,
        check=check,
        run_process=subprocess.run,
        log=logger,
    )
