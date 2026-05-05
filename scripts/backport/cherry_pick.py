"""Git cherry-pick operations for backporting."""

from __future__ import annotations

import logging
import subprocess

from scripts.backport.models import CherryPickResult, ConflictedFile

logger = logging.getLogger(__name__)


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
    result = _run_git(
        repo_dir, "cherry-pick", "-m", "1", merge_commit_sha, check=False,
    )
    if result.returncode != 0:
        logger.warning(
            "Cherry-pick of merge commit %s produced conflicts",
            merge_commit_sha,
        )
        conflicts = _collect_conflicts(repo_dir, target_branch)

        # Empty cherry-pick: non-zero exit but no unmerged files means
        # the changes already exist on the target branch.  Abort the
        # cherry-pick and retry with --allow-empty so the branch has a
        # commit that can be pushed.
        if not conflicts:
            logger.info(
                "No conflicting files — cherry-pick is empty. "
                "Retrying with --allow-empty.",
            )
            logger.debug(
                "Original cherry-pick stderr: %s",
                result.stderr.strip(),
            )
            _run_git(repo_dir, "cherry-pick", "--abort", check=False)
            retry = _run_git(
                repo_dir, "cherry-pick", "-m", "1", "--allow-empty",
                merge_commit_sha, check=False,
            )
            if retry.returncode == 0:
                logger.info(
                    "Empty cherry-pick of %s succeeded with --allow-empty",
                    merge_commit_sha,
                )
                return CherryPickResult(
                    success=True,
                    applied_commits=[merge_commit_sha],
                )
            # If --allow-empty also fails, fall through to conflict path
            logger.warning(
                "Retry with --allow-empty also failed for %s: %s",
                merge_commit_sha,
                retry.stderr.strip(),
            )
            conflicts = _collect_conflicts(repo_dir, target_branch)

        return CherryPickResult(
            success=False,
            conflicting_files=conflicts,
            applied_commits=[merge_commit_sha],
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
        result = _run_git(repo_dir, "cherry-pick", sha, check=False)
        if result.returncode != 0:
            logger.warning(
                "Cherry-pick of commit %s produced conflicts", sha,
            )
            conflicts = _collect_conflicts(repo_dir, target_branch)
            if not conflicts:
                logger.info(
                    "No conflicting files; cherry-pick is empty. "
                    "Retrying with --allow-empty.",
                )
                logger.debug(
                    "Original cherry-pick stderr: %s",
                    result.stderr.strip(),
                )
                _run_git(repo_dir, "cherry-pick", "--abort", check=False)
                retry = _run_git(
                    repo_dir, "cherry-pick", "--allow-empty", sha, check=False,
                )
                if retry.returncode == 0:
                    logger.info(
                        "Empty cherry-pick of %s succeeded with --allow-empty",
                        sha,
                    )
                    applied.append(sha)
                    continue
                logger.warning(
                    "Retry with --allow-empty also failed for %s: %s",
                    sha,
                    retry.stderr.strip(),
                )
                conflicts = _collect_conflicts(repo_dir, target_branch)
            return CherryPickResult(
                success=False,
                conflicting_files=conflicts,
                applied_commits=applied,
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
        conflicts.append(_build_conflicted_file(repo_dir, target_branch, path))
    return conflicts


def _build_conflicted_file(
    repo_dir: str,
    target_branch: str,
    file_path: str,
) -> ConflictedFile:
    # Target branch version (before cherry-pick)
    target_branch_content = _show_file(repo_dir, target_branch, file_path)

    # Source branch version (the commit being cherry-picked)
    source_branch_content = _show_file(repo_dir, "CHERRY_PICK_HEAD", file_path)

    return ConflictedFile(
        path=file_path,
        target_branch_content=target_branch_content,
        source_branch_content=source_branch_content,
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


def _run_git(
    repo_dir: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    cmd = ["git", *args]
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=repo_dir,
        check=False,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result
