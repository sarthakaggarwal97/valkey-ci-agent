"""Git cherry-pick operations for backporting."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from scripts.backport.models import CherryPickResult, ConflictedFile, ResolutionResult
from scripts.common.proc import (
    GitPathEncodingError,
    decode_git_paths,
    run_git,
    run_git_bytes,
)

logger = logging.getLogger(__name__)


def complete_resolved_cherry_pick(
    repo_dir: str,
    resolutions: list[ResolutionResult],
) -> None:
    """Apply text resolutions and complete the in-progress cherry-pick."""
    root = Path(repo_dir).resolve()
    for resolution in resolutions:
        if resolution.resolved_content is None:
            raise ValueError(
                f"Cannot apply unresolved conflict for {resolution.path}"
            )
        path = (root / resolution.path).resolve()
        if path != root and root not in path.parents:
            raise ValueError(
                f"Conflict resolution path escapes checkout: {resolution.path!r}"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            content = resolution.resolved_content.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ValueError(
                f"Conflict resolution is not valid UTF-8 for {resolution.path}"
            ) from exc
        if b"\0" in content:
            raise ValueError(
                f"Conflict resolution contains binary content for {resolution.path}"
            )
        path.write_bytes(content)
        _run_git(repo_dir, "add", "--", resolution.path)

    _run_git(
        repo_dir,
        "-c",
        "core.editor=true",
        "cherry-pick",
        "--continue",
    )


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
    if result.returncode != 0 and is_non_merge_mainline_error(
        f"{result.stdout}\n{result.stderr}"
    ):
        logger.info(
            "%s is not a merge commit; retrying cherry-pick without -m",
            merge_commit_sha,
        )
        result = _run_git(repo_dir, "cherry-pick", merge_commit_sha, check=False)
    if result.returncode != 0:
        logger.warning(
            "Cherry-pick of merge commit %s produced conflicts",
            merge_commit_sha,
        )
        conflicts, handoff_reason = _collect_conflicts(repo_dir, target_branch)

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
            handoff_reason=handoff_reason,
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
            conflicts, handoff_reason = _collect_conflicts(repo_dir, target_branch)
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
                handoff_reason=handoff_reason,
            )
        applied.append(sha)
    logger.info("All %d commits cherry-picked cleanly", len(applied))
    return CherryPickResult(success=True, applied_commits=applied)


def _collect_conflicts(
    repo_dir: str,
    target_branch: str,
) -> tuple[list[ConflictedFile], str | None]:
    result = run_git_bytes(
        repo_dir,
        "diff",
        "--name-only",
        "-z",
        "--diff-filter=U",
    )
    try:
        paths = decode_git_paths(result.stdout, context="conflicting path list")
    except GitPathEncodingError as exc:
        logger.warning("Conflict requires human handling: %s", exc)
        return [], str(exc)
    logger.info("Found %d conflicting file(s): %s", len(paths), paths)

    conflicts: list[ConflictedFile] = []
    for path in paths:
        try:
            cf = _build_conflicted_file(repo_dir, target_branch, path)
        except ValueError as exc:
            logger.warning("Conflict requires human handling: %s", exc)
            return [], str(exc)
        conflicts.append(cf)
    return conflicts, None


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
    result = run_git_bytes(
        repo_dir,
        "show",
        f"{ref}:{file_path}",
        check=False,
    )
    if result.returncode != 0:
        logger.warning(
            "Could not read %s:%s — %s",
            ref,
            file_path,
            result.stderr.decode("utf-8", errors="backslashreplace").strip(),
        )
        return ""
    if b"\0" in result.stdout:
        raise ValueError(f"binary conflict requires human handling: {file_path}")
    try:
        return result.stdout.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"non-UTF-8 conflict requires human handling: {file_path}"
        ) from exc


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


def _run_git(
    repo_dir: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    logger.debug("Running locked git %s", " ".join(args))
    result = run_git(
        repo_dir,
        *args,
        check=False,
        errors="backslashreplace",
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result
