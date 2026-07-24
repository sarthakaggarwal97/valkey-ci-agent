"""Transactional validation and promotion for backport candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from scripts.backport.application import apply_candidate
from scripts.backport.git import (
    changed_paths_between,
    detached_worktree,
    head_sha,
    promote_detached_head,
    tracked_worktree_changes,
)
from scripts.backport.models import (
    DETAIL_VALIDATION_REPAIRED_PREFIX,
    BackportCandidate,
    CandidateResult,
)
from scripts.backport.sweep_validation import (
    run_test_commands,
    validate_backport_branch,
    validate_branch_with_optional_repair,
)

BaselinePhase = Literal["setup", "validation", "cleanliness"]
RunCommands = Callable[..., tuple[bool, str]]
ValidateBaseline = Callable[..., tuple[bool, str]]
ValidateCandidate = Callable[..., tuple[bool, str]]
ApplyCandidate = Callable[..., CandidateResult]


@dataclass(frozen=True)
class BaselineValidationResult:
    ok: bool
    phase: BaselinePhase
    output: str = ""


def validate_baseline(
    repo_dir: str,
    target_branch: str,
    setup_commands: list[str],
    test_commands: list[str],
    validation_rules: list[Any],
    *,
    run_commands: RunCommands = run_test_commands,
    validate_func: ValidateBaseline = validate_backport_branch,
) -> BaselineValidationResult:
    """Validate the pre-candidate branch in a disposable worktree."""

    if not setup_commands and not test_commands and not validation_rules:
        return BaselineValidationResult(True, "validation")

    with detached_worktree(repo_dir, prefix="backport-baseline-") as workspace:
        setup_ok, setup_output = run_commands(workspace, setup_commands)
        if not setup_ok:
            return BaselineValidationResult(False, "setup", setup_output)

        dirty = tracked_worktree_changes(workspace)
        if dirty:
            return BaselineValidationResult(
                False,
                "cleanliness",
                "validation setup modified tracked file(s): "
                + ", ".join(dirty[:10]),
            )

        ok, output = validate_func(
            workspace,
            target_branch,
            test_commands,
            validation_rules,
        )
        dirty = tracked_worktree_changes(workspace)
        if dirty:
            return BaselineValidationResult(
                False,
                "cleanliness",
                "baseline validation modified tracked file(s): "
                + ", ".join(dirty[:10]),
            )
        return BaselineValidationResult(ok, "validation", output)


def apply_candidate_transaction(
    repo_dir: str,
    candidate: BackportCandidate,
    repo_full_name: str,
    git_env: dict[str, str],
    *,
    target_branch: str,
    setup_commands: list[str],
    test_commands: list[str],
    validation_rules: list[Any],
    repair_validation_failures: bool,
    language: str = "c",
    build_commands: list[str] | None = None,
    max_conflicting_files: int = 100,
    run_commands: RunCommands = run_test_commands,
    apply_func: ApplyCandidate = apply_candidate,
    validate_func: ValidateCandidate = validate_branch_with_optional_repair,
) -> CandidateResult:
    """Apply, validate, and atomically promote one candidate."""

    starting_head = head_sha(repo_dir)
    with detached_worktree(
        repo_dir,
        prefix=f"backport-candidate-{candidate.source_pr_number}-",
    ) as workspace:
        setup_ok, setup_output = run_commands(workspace, setup_commands)
        if not setup_ok:
            raise RuntimeError(
                "candidate validation setup failed: "
                + (setup_output[:500] or "setup command failed")
            )
        setup_changes = tracked_worktree_changes(workspace)
        if setup_changes:
            raise RuntimeError(
                "candidate validation setup modified tracked file(s): "
                + ", ".join(setup_changes[:10])
            )

        result = apply_func(
            workspace,
            candidate,
            repo_full_name,
            git_env,
            language=language,
            build_commands=build_commands,
            validation_rules=validation_rules,
            max_conflicting_files=max_conflicting_files,
        )
        if result.outcome != "applied":
            return result

        applied_head = head_sha(workspace)
        candidate_paths = changed_paths_between(
            workspace,
            starting_head,
            applied_head,
        )
        dirty = tracked_worktree_changes(workspace)
        if dirty:
            result.outcome = "error"
            result.detail = (
                "candidate application left tracked workspace changes: "
                + ", ".join(dirty[:10])
            )
            return result

        ok, output = validate_func(
            workspace,
            target_branch,
            test_commands,
            validation_rules,
            repair=repair_validation_failures,
            repair_paths=candidate_paths,
        )
        if not ok:
            result.outcome = "skipped-validation-failed"
            result.detail = output
            return result

        validated_head = head_sha(workspace)
        dirty = tracked_worktree_changes(workspace)
        if dirty:
            result.outcome = "skipped-validation-failed"
            result.detail = (
                "validation modified tracked file(s) without committing them: "
                + ", ".join(dirty[:10])
            )
            return result

        if validated_head != applied_head:
            repair_paths = changed_paths_between(
                workspace,
                applied_head,
                validated_head,
            )
            unexpected = sorted(set(repair_paths) - set(candidate_paths))
            if unexpected:
                result.outcome = "skipped-validation-failed"
                result.detail = (
                    "validation repair modified paths outside the current "
                    "candidate: " + ", ".join(unexpected[:10])
                )
                return result
            result.validation_repaired = True
            result.validation_repair_commit_sha = validated_head
            result.validation_repair_paths = list(repair_paths)
            result.resolved_by_ai = True
            repair_detail = (
                f"{DETAIL_VALIDATION_REPAIRED_PREFIX} "
                + ", ".join(repair_paths)
            )
            result.detail = _append_detail(result.detail, repair_detail)

        promote_detached_head(
            repo_dir,
            expected_head=starting_head,
            validated_head=validated_head,
        )
        return result


def _append_detail(current: str, detail: str) -> str:
    if not current:
        return detail
    if detail in current:
        return current
    return f"{current}; {detail}"
