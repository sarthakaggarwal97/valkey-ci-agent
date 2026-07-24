"""Apply one merged pull request to a local backport branch."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any, Callable

from scripts.backport.conflict_resolver import resolve_conflicts_with_claude
from scripts.backport.git import (
    has_staged_changes,
    index_stage_exists,
    read_index_stage,
)
from scripts.backport.git import (
    run_git as run_git_default,
)
from scripts.backport.missing_test_adaptation import (
    MissingTestAdaptationResult,
    adapt_target_missing_tests_with_claude,
    build_missing_test_context,
    is_test_path,
)
from scripts.backport.models import (
    DETAIL_DROPPED_TARGET_MISSING_TEST_PREFIX,
    DETAIL_EMPTY_ON_TARGET,
    DETAIL_RESOLVED_BY_AI,
    BackportCandidate,
    BackportPRContext,
    CandidateOutcome,
    CandidateResult,
    ConflictedFile,
    ResolutionResult,
)
from scripts.backport.source_change import (
    SourceChangeError,
    SourceChangePlan,
    prepare_source_change,
)
from scripts.backport.sweep_git import changed_paths_in_index_or_worktree
from scripts.backport.validation import select_validation_commands

logger = logging.getLogger(__name__)

RunGit = Callable[..., Any]
RunProcess = Callable[..., subprocess.CompletedProcess[str]]
ResolveConflicts = Callable[..., list[ResolutionResult]]


AdaptMissingTests = Callable[..., MissingTestAdaptationResult]


def _abort_cherry_pick(repo_dir: str, run_git: RunGit) -> None:
    run_git(repo_dir, "cherry-pick", "--abort")


def _empty_skip_reason(
    conflicting_files: list[ConflictedFile],
    resolutions: list[ResolutionResult],
) -> str:
    """A deterministic reason a resolved cherry-pick produced no net change.

    Derived only from provable facts, never from the resolver's prose. When the
    resolution of every conflicted file matched the target branch's existing
    content, the source PR's change does not apply on this branch (the code it
    modifies differs or is absent here), so the cherry-pick is a no-op.
    """
    target_by_path = {cf.path: cf.target_branch_content for cf in conflicting_files}
    matched_target = [
        r.path
        for r in resolutions
        if r.resolved_content is not None and r.path in target_by_path and r.resolved_content == target_by_path[r.path]
    ]
    if matched_target and len(matched_target) == len([r for r in resolutions if r.resolved_content is not None]):
        return (
            "The change does not apply to this branch: resolving the conflict "
            "matched the existing code, so the cherry-pick added nothing."
        )
    return "The cherry-pick produced no net change on this branch, so there is nothing to backport."


def apply_candidate(
    repo_dir: str,
    candidate: BackportCandidate,
    repo_full_name: str,
    git_env: dict[str, str],
    *,
    language: str = "c",
    build_commands: list[str] | None = None,
    validation_rules: list[Any] | None = None,
    max_conflicting_files: int = 100,
    run_git: RunGit = run_git_default,
    resolve_conflicts: ResolveConflicts = resolve_conflicts_with_claude,
    adapt_missing_tests: AdaptMissingTests | None = None,
    run_process: RunProcess = subprocess.run,
    source_plan: SourceChangePlan | None = None,
) -> CandidateResult:
    """Apply a complete candidate, rolling back any partial source series."""

    if adapt_missing_tests is None:
        adapt_missing_tests = adapt_target_missing_tests_with_claude

    try:
        plan = source_plan or prepare_source_change(
            repo_dir,
            candidate.source_pr_number,
            candidate.merge_commit_sha,
            candidate.commit_shas,
            source_commits_complete=candidate.source_commits_complete,
            git_env=git_env,
        )
    except (SourceChangeError, subprocess.CalledProcessError) as exc:
        return CandidateResult(candidate.source_pr_number, candidate.source_pr_title, "error", str(exc))

    logger.info(
        "Applying PR #%d from %s with %s plan (%d commit(s))",
        candidate.source_pr_number,
        repo_full_name,
        plan.strategy,
        len(plan.commits),
    )
    starting_head = (
        _head_sha(repo_dir, run_process=run_process)
        if len(plan.commits) > 1
        else None
    )
    applied_commits: list[str] = []
    all_conflicts: list[ConflictedFile] = []
    all_resolutions: list[ResolutionResult] = []
    conflict_paths_seen: set[str] = set()
    detail_parts: list[str] = []
    last_resolved_sha: str | None = None
    no_change_reason = ""
    adapted_by_ai = False
    first_conflicting_sha: str | None = None

    for index, sha in enumerate(plan.commits):
        command = ["git", "cherry-pick", sha]
        if plan.strategy == "merge" and index == 0:
            command[2:2] = ["-m", "1"]
        result = run_process(
            command,
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            applied_commits.append(sha)
            continue

        conflict_result = run_process(
            ["git", "diff", "--name-only", "--diff-filter=U"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if conflict_result.returncode != 0:
            _abort_and_rollback(repo_dir, starting_head, run_git)
            return _application_result(
                candidate,
                "error",
                "could not inspect cherry-pick conflicts: "
                + ((conflict_result.stderr or "").strip()[:300] or "git diff failed"),
            )

        conflicting_paths = [
            line.strip()
            for line in conflict_result.stdout.splitlines()
            if line.strip()
        ]
        if not conflicting_paths:
            _abort_cherry_pick(repo_dir, run_git)
            if _is_empty_cherry_pick(result):
                continue
            if starting_head is not None:
                run_git(repo_dir, "reset", "--hard", starting_head)
            return _application_result(
                candidate,
                "error",
                f"cherry-pick failed: {(result.stderr or result.stdout).strip()[:500]}",
            )

        logger.info(
            "Found %d conflict(s) while applying %s: %s",
            len(conflicting_paths),
            sha,
            conflicting_paths,
        )
        if first_conflicting_sha is None:
            first_conflicting_sha = sha
        conflicting_files: list[ConflictedFile] = []
        target_missing_paths: set[str] = set()
        target_missing_test_contexts: dict[str, str] = {}
        for path in conflicting_paths:
            target_content = read_index_stage(
                repo_dir,
                path,
                2,
                run_process=run_process,
            )
            source_content = read_index_stage(
                repo_dir,
                path,
                3,
                run_process=run_process,
            )
            if "\x00" in target_content or "\x00" in source_content:
                logger.warning("Skipping binary conflict: %s", path)
                continue
            if not index_stage_exists(
                repo_dir,
                path,
                2,
                run_process=run_process,
            ):
                target_missing_paths.add(path)
                if is_test_path(path):
                    target_missing_test_contexts[path] = build_missing_test_context(
                        repo_dir,
                        path,
                        source_content,
                        run_process=run_process,
                    )
            conflicting_files.append(
                ConflictedFile(
                    path=path,
                    target_branch_content=target_content,
                    source_branch_content=source_content,
                )
            )

        if not conflicting_files:
            _abort_and_rollback(repo_dir, starting_head, run_git)
            return _application_result(
                candidate,
                "skipped-conflict",
                "only binary file conflicts; nothing the resolver can act on",
            )

        all_conflicts.extend(conflicting_files)
        conflict_paths_seen.update(item.path for item in conflicting_files)
        if len(conflict_paths_seen) > max_conflicting_files:
            _abort_and_rollback(repo_dir, starting_head, run_git)
            detail = (
                f"Too many conflicting files ({len(conflict_paths_seen)} > "
                f"max_conflicting_files={max_conflicting_files}). "
                "Refusing to invoke conflict resolver."
            )
            return _application_result(
                candidate,
                "skipped-conflict",
                detail,
                conflicting_files=all_conflicts,
            )

        if target_missing_paths:
            non_test_missing_paths = sorted(
                path
                for path in target_missing_paths
                if not is_test_path(path)
            )
            if non_test_missing_paths:
                _abort_and_rollback(repo_dir, starting_head, run_git)
                paths = ", ".join(non_test_missing_paths)
                return _application_result(
                    candidate,
                    "skipped-conflict",
                    f"target branch lacks conflicted file(s): {paths}",
                    conflicting_files=all_conflicts,
                )

            for path in sorted(target_missing_paths):
                logger.info(
                    "Dropping target-missing test file from cherry-pick: %s",
                    path,
                )
                run_git(
                    repo_dir,
                    "rm",
                    "-f",
                    "--ignore-unmatch",
                    "--",
                    path,
                )
            conflicting_paths = [
                path
                for path in conflicting_paths
                if path not in target_missing_paths
            ]
            conflicting_files = [
                item
                for item in conflicting_files
                if item.path not in target_missing_paths
            ]

        resolutions: list[ResolutionResult] = []
        if conflicting_files:
            pr_context = BackportPRContext(
                source_pr_number=candidate.source_pr_number,
                source_pr_title=candidate.source_pr_title,
                source_pr_url=candidate.source_pr_url,
                source_pr_diff=candidate.source_pr_diff,
                target_branch=candidate.target_branch,
                commits=candidate.commit_shas,
            )
            resolver_validation_commands = select_validation_commands(
                build_commands or [],
                validation_rules or [],
                conflicting_paths,
            )
            worktree_paths = changed_paths_in_index_or_worktree(
                repo_dir,
                run_process=run_process,
            )
            allowed_resolution_paths = sorted(
                set(conflicting_paths) | set(worktree_paths)
            )
            resolutions = resolve_conflicts(
                repo_dir,
                conflicting_files,
                pr_context,
                language=language,
                build_commands=resolver_validation_commands or None,
                allowed_paths=allowed_resolution_paths,
            )
            all_resolutions.extend(resolutions)

        unresolved = [
            resolution
            for resolution in resolutions
            if resolution.resolved_content is None
        ]
        if unresolved:
            _abort_and_rollback(repo_dir, starting_head, run_git)
            details = "; ".join(
                f"{item.path}: {(item.resolution_summary or 'unresolved')[:200]}"
                for item in unresolved
            )
            return _application_result(
                candidate,
                "skipped-conflict",
                f"unresolved - {details}",
                resolutions=all_resolutions,
                conflicting_files=all_conflicts,
            )

        for resolution in resolutions:
            if resolution.resolved_content is None:
                continue
            resolved_path = Path(repo_dir, resolution.path)
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            resolved_path.write_text(
                resolution.resolved_content,
                encoding="utf-8",
            )
            run_git(repo_dir, "add", resolution.path)

        test_adaptation = MissingTestAdaptationResult()
        if target_missing_test_contexts:
            try:
                test_adaptation = adapt_missing_tests(
                    repo_dir,
                    candidate,
                    target_missing_test_contexts,
                    language=language,
                    run_git=run_git,
                    run_process=run_process,
                )
            except Exception as exc:  # noqa: BLE001 - adapter failures fail closed
                test_adaptation = MissingTestAdaptationResult(
                    summary=(
                        "test adaptation failed unexpectedly: "
                        f"{str(exc)[:200]}"
                    ),
                    fatal=True,
                )
            if test_adaptation.fatal:
                _abort_and_rollback(repo_dir, starting_head, run_git)
                return _application_result(
                    candidate,
                    "skipped-conflict",
                    test_adaptation.summary,
                    resolutions=all_resolutions,
                    conflicting_files=all_conflicts,
                )
            adapted_by_ai = adapted_by_ai or bool(
                test_adaptation.adapted_paths
            )

        if resolutions:
            _append_detail(detail_parts, DETAIL_RESOLVED_BY_AI)
        if target_missing_paths:
            paths = ", ".join(sorted(target_missing_paths))
            _append_detail(
                detail_parts,
                f"{DETAIL_DROPPED_TARGET_MISSING_TEST_PREFIX} {paths}",
            )
        if test_adaptation.summary:
            _append_detail(detail_parts, test_adaptation.summary)

        if not has_staged_changes(repo_dir, run_process=run_process):
            _abort_cherry_pick(repo_dir, run_git)
            if target_missing_paths:
                paths = ", ".join(sorted(target_missing_paths))
                no_change_reason = (
                    "Only target-missing test file(s) were absent on this "
                    f"branch: {paths}"
                )
            else:
                no_change_reason = _empty_skip_reason(
                    conflicting_files,
                    resolutions,
                )
            continue

        commit_result = run_process(
            ["git", "-c", "core.editor=true", "cherry-pick", "--continue"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if commit_result.returncode != 0:
            output = f"{commit_result.stdout}\n{commit_result.stderr}"
            if "nothing to commit" in output.lower():
                _abort_cherry_pick(repo_dir, run_git)
                no_change_reason = (
                    "The cherry-pick produced no net change on this branch, "
                    "so there is nothing to backport."
                )
                continue
            _abort_and_rollback(repo_dir, starting_head, run_git)
            return _application_result(
                candidate,
                "skipped-conflict",
                f"commit failed: {output.strip()[:200]}",
                resolutions=all_resolutions,
                conflicting_files=all_conflicts,
            )

        applied_commits.append(sha)
        head_result = run_process(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if head_result.returncode == 0:
            last_resolved_sha = head_result.stdout.strip()

    if not applied_commits:
        detail = (
            DETAIL_EMPTY_ON_TARGET
            if no_change_reason
            else "already applied or empty cherry-pick"
        )
        return _application_result(
            candidate,
            "skipped-existing",
            detail,
            resolutions=all_resolutions,
            resolved_by_ai=bool(all_resolutions),
            skip_reason=no_change_reason,
            conflicting_files=all_conflicts,
        )

    return _application_result(
        candidate,
        "applied",
        "; ".join(detail_parts),
        resolutions=all_resolutions,
        resolved_by_ai=bool(all_resolutions or adapted_by_ai),
        resolved_commit_sha=last_resolved_sha,
        applied_commits=applied_commits,
        conflicting_files=all_conflicts,
        conflicting_commit_sha=first_conflicting_sha,
    )


def _application_result(
    candidate: BackportCandidate,
    outcome: CandidateOutcome,
    detail: str,
    *,
    resolutions: list[ResolutionResult] | None = None,
    resolved_by_ai: bool = False,
    skip_reason: str = "",
    resolved_commit_sha: str | None = None,
    applied_commits: list[str] | None = None,
    conflicting_files: list[ConflictedFile] | None = None,
    conflicting_commit_sha: str | None = None,
) -> CandidateResult:
    return CandidateResult(
        source_pr_number=candidate.source_pr_number,
        source_pr_title=candidate.source_pr_title,
        outcome=outcome,
        detail=detail,
        resolutions=list(resolutions or []),
        resolved_by_ai=resolved_by_ai,
        skip_reason=skip_reason,
        resolved_commit_sha=resolved_commit_sha,
        applied_commits=list(applied_commits or []),
        conflicting_files=list(conflicting_files or []),
        conflicting_commit_sha=conflicting_commit_sha,
    )


def _abort_and_rollback(
    repo_dir: str,
    starting_head: str | None,
    run_git: RunGit,
) -> None:
    _abort_cherry_pick(repo_dir, run_git)
    if starting_head is not None:
        run_git(repo_dir, "reset", "--hard", starting_head)


def _append_detail(parts: list[str], detail: str) -> None:
    if detail and detail not in parts:
        parts.append(detail)


def _head_sha(
    repo_dir: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> str:
    result = run_process(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "could not resolve candidate start: "
            + ((result.stderr or "").strip()[:300] or "git rev-parse failed")
        )
    return result.stdout.strip()


def _is_empty_cherry_pick(result: subprocess.CompletedProcess[str]) -> bool:
    output = f"{result.stdout}\n{result.stderr}".lower()
    return any(
        marker in output
        for marker in (
            "cherry-pick is now empty",
            "previous cherry-pick is now empty",
            "nothing to commit",
            "patch is empty",
        )
    )
