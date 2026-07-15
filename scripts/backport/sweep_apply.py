"""Apply a single project-board backport candidate to a sweep branch."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from scripts.ai.runtime import AgentRunResult, run_agent
from scripts.backport.cherry_pick import is_non_merge_mainline_error
from scripts.backport.conflict_resolver import resolve_conflicts_with_claude
from scripts.backport.main import _run_git as run_git_default
from scripts.backport.models import BackportPRContext, ConflictedFile, ResolutionResult
from scripts.backport.sweep_git import changed_paths_in_index_or_worktree
from scripts.backport.sweep_models import (
    DETAIL_EMPTY_ON_TARGET,
    DETAIL_RESOLVED_BY_AI,
    CandidateResult,
    ProjectBackportCandidate,
)
from scripts.backport.utils import has_conflict_markers
from scripts.backport.validation import select_validation_commands

logger = logging.getLogger(__name__)

RunGit = Callable[..., Any]
RunProcess = Callable[..., subprocess.CompletedProcess[str]]
ResolveConflicts = Callable[..., list[ResolutionResult]]
RunAgent = Callable[..., AgentRunResult]


@dataclass
class MissingTestAdaptationResult:
    adapted_paths: list[str] = field(default_factory=list)
    summary: str = ""
    fatal: bool = False


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
    candidate: ProjectBackportCandidate,
    repo_full_name: str,
    git_env: dict[str, str],
    *,
    language: str = "c",
    build_commands: list[str] | None = None,
    validation_rules: list[Any] | None = None,
    run_git: RunGit = run_git_default,
    resolve_conflicts: ResolveConflicts = resolve_conflicts_with_claude,
    adapt_missing_tests: AdaptMissingTests | None = None,
    run_process: RunProcess = subprocess.run,
) -> CandidateResult:
    if adapt_missing_tests is None:
        adapt_missing_tests = adapt_target_missing_tests_with_claude

    sha = candidate.merge_commit_sha
    if not sha:
        return CandidateResult(candidate.source_pr_number, candidate.source_pr_title, "error", "no merge SHA")

    try:
        run_git(repo_dir, "fetch", "origin", sha, env=git_env)
        result = run_process(
            ["git", "cherry-pick", "-m", "1", sha],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 and is_non_merge_mainline_error(f"{result.stdout}\n{result.stderr}"):
            logger.info(
                "%s is not a merge commit; retrying cherry-pick without -m",
                sha,
            )
            result = run_process(
                ["git", "cherry-pick", sha],
                cwd=repo_dir,
                capture_output=True,
                text=True,
            )
    except subprocess.CalledProcessError as exc:
        return CandidateResult(candidate.source_pr_number, candidate.source_pr_title, "error", str(exc))

    if result.returncode == 0:
        return CandidateResult(candidate.source_pr_number, candidate.source_pr_title, "applied")

    conflict_result = run_process(
        ["git", "diff", "--name-only", "--diff-filter=U"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    conflicting_paths = [line.strip() for line in conflict_result.stdout.splitlines() if line.strip()]
    if not conflicting_paths:
        _abort_cherry_pick(repo_dir, run_git)
        stderr = result.stderr[:500]
        if "cherry-pick is now empty" in result.stderr or "nothing to commit" in result.stderr:
            return CandidateResult(
                candidate.source_pr_number,
                candidate.source_pr_title,
                "skipped-existing",
                "already applied or empty cherry-pick",
            )
        return CandidateResult(
            candidate.source_pr_number,
            candidate.source_pr_title,
            "error",
            f"cherry-pick failed: {stderr}",
        )

    logger.info("Found %d conflicting file(s): %s", len(conflicting_paths), conflicting_paths)
    conflicting_files = []
    target_missing_paths: set[str] = set()
    target_missing_test_sources: dict[str, str] = {}
    for path in conflicting_paths:
        target_content = read_index_stage(repo_dir, path, 2, run_process=run_process)
        source_content = read_index_stage(repo_dir, path, 3, run_process=run_process)
        # Binary files have no line-level merge, so the resolver can't act on
        # them (git marks binary content with a NUL byte). Skip them rather
        # than feeding them to the resolver. A candidate left with only binary
        # conflicts has no resolvable files and is skipped below.
        if "\x00" in target_content or "\x00" in source_content:
            logger.warning("Skipping binary conflict: %s", path)
            continue
        if not index_stage_exists(repo_dir, path, 2, run_process=run_process):
            target_missing_paths.add(path)
            if is_test_path(path):
                target_missing_test_sources[path] = source_content
        conflicting_files.append(
            ConflictedFile(
                path=path,
                target_branch_content=target_content,
                source_branch_content=source_content,
            )
        )
    if not conflicting_files:
        _abort_cherry_pick(repo_dir, run_git)
        return CandidateResult(
            candidate.source_pr_number,
            candidate.source_pr_title,
            "skipped-conflict",
            "only binary file conflicts; nothing the resolver can act on",
        )
    if target_missing_paths:
        non_test_missing_paths = sorted(path for path in target_missing_paths if not is_test_path(path))
        if non_test_missing_paths:
            _abort_cherry_pick(repo_dir, run_git)
            paths = ", ".join(non_test_missing_paths)
            return CandidateResult(
                candidate.source_pr_number,
                candidate.source_pr_title,
                "skipped-conflict",
                f"target branch lacks conflicted file(s): {paths}",
            )

        for path in sorted(target_missing_paths):
            logger.info("Dropping target-missing test file from cherry-pick: %s", path)
            run_git(repo_dir, "rm", "-f", "--ignore-unmatch", "--", path)
        conflicting_paths = [path for path in conflicting_paths if path not in target_missing_paths]
        conflicting_files = [cf for cf in conflicting_files if cf.path not in target_missing_paths]

    resolutions: list[ResolutionResult] = []
    if conflicting_files:
        pr_context = BackportPRContext(
            source_pr_number=candidate.source_pr_number,
            source_pr_title=candidate.source_pr_title,
            source_pr_url=candidate.source_pr_url,
            source_pr_diff="",
            target_branch=candidate.target_branch,
            commits=candidate.commit_shas,
        )

        resolver_validation_commands = select_validation_commands(
            build_commands or [],
            validation_rules or [],
            conflicting_paths,
        )
        worktree_paths = changed_paths_in_index_or_worktree(repo_dir, run_process=run_process)
        allowed_resolution_paths = sorted(set(conflicting_paths) | set(worktree_paths))
        resolutions = resolve_conflicts(
            repo_dir,
            conflicting_files,
            pr_context,
            language=language,
            build_commands=resolver_validation_commands or None,
            allowed_paths=allowed_resolution_paths,
        )
    unresolved = [r for r in resolutions if r.resolved_content is None]
    if unresolved:
        _abort_cherry_pick(repo_dir, run_git)
        details = "; ".join(f"{r.path}: {(r.resolution_summary or 'unresolved')[:200]}" for r in unresolved)
        return CandidateResult(
            candidate.source_pr_number,
            candidate.source_pr_title,
            "skipped-conflict",
            f"unresolved - {details}",
        )

    for r in resolutions:
        if r.resolved_content is not None:
            resolved_path = Path(repo_dir, r.path)
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            resolved_path.write_text(r.resolved_content, encoding="utf-8")
            run_git(repo_dir, "add", r.path)

    test_adaptation = MissingTestAdaptationResult()
    if target_missing_test_sources:
        test_adaptation = adapt_missing_tests(
            repo_dir,
            candidate,
            target_missing_test_sources,
            language=language,
            run_git=run_git,
            run_process=run_process,
        )
        if test_adaptation.fatal:
            _abort_cherry_pick(repo_dir, run_git)
            return CandidateResult(
                candidate.source_pr_number,
                candidate.source_pr_title,
                "skipped-conflict",
                test_adaptation.summary,
            )

    if not has_staged_changes(repo_dir, run_process=run_process):
        _abort_cherry_pick(repo_dir, run_git)
        if target_missing_paths:
            paths = ", ".join(sorted(target_missing_paths))
            return CandidateResult(
                candidate.source_pr_number,
                candidate.source_pr_title,
                "skipped-existing",
                DETAIL_EMPTY_ON_TARGET,
                skip_reason=(f"Only target-missing test file(s) were absent on this branch: {paths}"),
            )
        return CandidateResult(
            candidate.source_pr_number,
            candidate.source_pr_title,
            "skipped-existing",
            DETAIL_EMPTY_ON_TARGET,
            resolutions=resolutions,
            skip_reason=_empty_skip_reason(conflicting_files, resolutions),
        )

    commit_result = run_process(
        [
            "git",
            "-c",
            "core.editor=true",
            "cherry-pick",
            "--continue",
        ],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if commit_result.returncode != 0:
        stderr_lower = (commit_result.stderr or "").lower()
        stdout_lower = (commit_result.stdout or "").lower()
        if "nothing to commit" in stderr_lower or "nothing to commit" in stdout_lower:
            _abort_cherry_pick(repo_dir, run_git)
            return CandidateResult(
                candidate.source_pr_number,
                candidate.source_pr_title,
                "skipped-existing",
                DETAIL_EMPTY_ON_TARGET,
            )
        _abort_cherry_pick(repo_dir, run_git)
        return CandidateResult(
            candidate.source_pr_number,
            candidate.source_pr_title,
            "skipped-conflict",
            f"commit failed: {(commit_result.stderr or commit_result.stdout).strip()[:200]}",
        )

    # Capture the resolution commit so diff comments can link each file to its
    # native diff in the commit view.
    head_result = run_process(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    resolved_sha = head_result.stdout.strip() if head_result.returncode == 0 else None

    detail_parts = []
    if resolutions:
        detail_parts.append(DETAIL_RESOLVED_BY_AI)
    if target_missing_paths:
        paths = ", ".join(sorted(target_missing_paths))
        detail_parts.append(f"dropped target-missing test file(s): {paths}")
    if test_adaptation.summary:
        detail_parts.append(test_adaptation.summary)
    detail = "; ".join(detail_parts) or ""
    resolved_by_ai = bool(resolutions or test_adaptation.adapted_paths)

    # Carry the per-file resolutions and a durable resolved-by-AI flag so the
    # sweep can post diff comments on the sweep PR and the sweep-PR-body table
    # keeps the "resolved by Claude" record across later runs.
    return CandidateResult(
        candidate.source_pr_number,
        candidate.source_pr_title,
        "applied",
        detail,
        resolutions=resolutions,
        resolved_by_ai=resolved_by_ai,
        resolved_commit_sha=resolved_sha,
    )


def has_staged_changes(repo_dir: str, *, run_process: RunProcess = subprocess.run) -> bool:
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
        "could not inspect staged changes: " + ((result.stderr or "").strip()[:300] or "git diff failed")
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


def is_test_path(path: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    parts = [part.lower() for part in normalized.split("/") if part]
    name = parts[-1] if parts else ""
    if len(parts) >= 3 and parts[0] == "src" and parts[1] == "unit":
        return name.startswith("test_") and name.endswith(".cpp")
    return len(parts) >= 2 and parts[0] == "tests" and name.endswith(".tcl")


def adapt_target_missing_tests_with_claude(
    repo_dir: str,
    candidate: ProjectBackportCandidate,
    missing_test_sources: dict[str, str],
    *,
    language: str,
    run_git: RunGit = run_git_default,
    run_process: RunProcess = subprocess.run,
    run_agent_func: RunAgent = run_agent,
) -> MissingTestAdaptationResult:
    pre_changed_paths = set(changed_paths_in_index_or_worktree(repo_dir, run_process=run_process))
    # Snapshot the exact pre-agent bytes of every already-changed file so any
    # unsuccessful path can restore the worktree to precisely this state,
    # rather than leaving stray agent edits behind for the next candidate.
    pre_snapshots = {path: file_bytes(Path(repo_dir, path)) for path in pre_changed_paths}
    pre_index_entries = index_entries_for_paths(repo_dir, pre_changed_paths, run_process=run_process)
    prompt = build_test_adaptation_prompt(
        repo_dir,
        candidate,
        missing_test_sources,
        language=language,
        run_process=run_process,
    )

    logger.info(
        "Calling Claude Code to adapt %d target-missing test file(s) for PR #%d onto %s...",
        len(missing_test_sources),
        candidate.source_pr_number,
        candidate.target_branch,
    )
    agent_result = run_agent_func("test_adaptation_edit_only", prompt, cwd=repo_dir)
    result_text = extract_agent_result_text(agent_result)
    logger.info(
        "Claude Code test adaptation finished (rc=%d). Result: %s",
        agent_result.returncode,
        result_text[:200] if result_text else "(no result text)",
    )

    post_changed_paths = set(changed_paths_in_index_or_worktree(repo_dir, run_process=run_process))
    new_changed_paths = sorted(post_changed_paths - pre_changed_paths)

    def rollback() -> None:
        _restore_pre_agent_state(
            repo_dir,
            pre_snapshots=pre_snapshots,
            pre_index_entries=pre_index_entries,
            new_paths=new_changed_paths,
            run_git=run_git,
            run_process=run_process,
        )

    if agent_result.returncode != 0:
        rollback()
        detail = agent_result.stderr or result_text or "Claude Code returned non-zero"
        return MissingTestAdaptationResult(
            summary=f"test adaptation not applied: Claude Code failed: {detail[:200]}",
            fatal=True,
        )

    protected_changes = sorted(
        path for path, pre_bytes in pre_snapshots.items() if file_bytes(Path(repo_dir, path)) != pre_bytes
    )
    post_index_entries = index_entries_for_paths(repo_dir, pre_changed_paths, run_process=run_process)
    protected_index_changes = sorted(
        path for path, pre_entries in pre_index_entries.items() if post_index_entries.get(path, ()) != pre_entries
    )
    non_test_changes = sorted(path for path in new_changed_paths if not is_test_path(path))
    if protected_changes or protected_index_changes or non_test_changes:
        rollback()
        details = ", ".join((protected_changes + protected_index_changes + non_test_changes)[:10])
        return MissingTestAdaptationResult(
            summary=f"test adaptation modified paths outside allowed test scope: {details}",
            fatal=True,
        )

    if not new_changed_paths:
        return MissingTestAdaptationResult(
            summary="test adaptation not applied: no branch-native test changes",
        )

    invalid_paths = []
    for path in new_changed_paths:
        file_path = Path(repo_dir, path)
        if not file_path.exists():
            invalid_paths.append(path)
            continue
        content = file_path.read_text(encoding="utf-8", errors="replace")
        if has_conflict_markers(content):
            invalid_paths.append(path)
    if invalid_paths:
        rollback()
        return MissingTestAdaptationResult(
            summary=("test adaptation not applied: invalid generated test path(s): " + ", ".join(invalid_paths[:10])),
            fatal=True,
        )

    for path in new_changed_paths:
        run_git(repo_dir, "add", path)

    return MissingTestAdaptationResult(
        adapted_paths=new_changed_paths,
        summary="ported target-missing test coverage to: " + ", ".join(new_changed_paths),
    )


def build_test_adaptation_prompt(
    repo_dir: str,
    candidate: ProjectBackportCandidate,
    missing_test_sources: dict[str, str],
    *,
    language: str,
    run_process: RunProcess = subprocess.run,
) -> str:
    source_sections = "\n\n".join(
        f"### Missing upstream test file: {path}\n```\n{content[:12000]}\n```"
        for path, content in sorted(missing_test_sources.items())
    )
    existing_tests = "\n".join(f"- {path}" for path in list_existing_test_paths(repo_dir, run_process=run_process))
    return (
        f"You are adapting test coverage for a {language} backport.\n\n"
        f'Source PR #{candidate.source_pr_number}: "{candidate.source_pr_title}"\n'
        f"URL: {candidate.source_pr_url}\n"
        f"Target branch: {candidate.target_branch}\n\n"
        f"The upstream PR changed test file(s) that do not exist on this target "
        f"branch. The cherry-pick has already kept those missing files absent. "
        f"Your task is to decide whether equivalent coverage can be added using "
        f"the target branch's existing test format.\n\n"
        f"Missing upstream test content:\n{source_sections}\n\n"
        f"Existing test files on the target branch include:\n"
        f"{existing_tests or '- (none found)'}\n\n"
        f"CRITICAL constraints:\n"
        f"- Edit or create test files only. Do not edit source, build, workflow, "
        f"or metadata files.\n"
        f"- Prefer modifying an existing test file that matches the target "
        f"branch's conventions. Only create a new test file if the target branch "
        f"already has that kind of test directory and naming pattern.\n"
        f"- Preserve the source PR's test intent only. Do not add unrelated "
        f"coverage or new product behavior.\n"
        f"- Before using a helper, command, macro, fixture, or test harness, "
        f"verify it exists on this target branch.\n"
        f"- Do not recreate the missing upstream file path unless that exact "
        f"test harness already exists on the target branch.\n"
        f"- Do not run `git add`, `git commit`, or any network command.\n"
        f"- If equivalent branch-native coverage is not practical, make no file "
        f"changes and explain that in your final result.\n\n"
        f"Do not wrap output in markdown. Edit files directly when safe."
    )


def list_existing_test_paths(
    repo_dir: str,
    *,
    limit: int = 120,
    run_process: RunProcess = subprocess.run,
) -> list[str]:
    result = run_process(
        ["git", "ls-files"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    paths = [line.strip() for line in result.stdout.splitlines() if line.strip() and is_test_path(line.strip())]
    return paths[:limit]


def extract_agent_result_text(agent_result: AgentRunResult) -> str:
    result_text = ""
    for line in agent_result.stdout.strip().splitlines():
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if event.get("type") == "result" and "result" in event:
            raw_result = event.get("result")
            if isinstance(raw_result, str):
                result_text = raw_result
            elif raw_result is not None:
                result_text = json.dumps(raw_result, sort_keys=True, default=str)
    return result_text


def file_bytes(path: Path) -> bytes | None:
    """Return the file's bytes, or ``None`` if it cannot be read.

    ``None`` is a distinct sentinel from empty bytes so a file that becomes
    unreadable is treated as changed rather than silently matching.
    """
    try:
        return path.read_bytes()
    except OSError:
        return None


def index_entries_for_paths(
    repo_dir: str,
    paths: set[str],
    *,
    run_process: RunProcess = subprocess.run,
) -> dict[str, tuple[str, ...]]:
    entries: dict[str, tuple[str, ...]] = {}
    for path in sorted(paths):
        result = run_process(
            ["git", "ls-files", "--stage", "--", path],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"could not inspect index entry for {path}: "
                + ((result.stderr or "").strip()[:300] or "git ls-files failed")
            )
        entries[path] = tuple(line for line in result.stdout.splitlines() if line)
    return entries


def _restore_pre_agent_state(
    repo_dir: str,
    *,
    pre_snapshots: dict[str, bytes | None],
    pre_index_entries: dict[str, tuple[str, ...]],
    new_paths: list[str],
    run_git: RunGit = run_git_default,
    run_process: RunProcess = subprocess.run,
) -> None:
    """Undo every agent edit, returning the worktree to its pre-agent state.

    Restores the recorded bytes of files that were already changed before the
    agent ran, and removes paths the agent newly touched (checking out tracked
    files, deleting untracked ones). ``cherry-pick --abort`` alone does not
    remove newly created untracked files, so a rejected adaptation must clean
    up after itself to avoid contaminating a later cherry-pick.
    """
    for path, pre_bytes in pre_snapshots.items():
        if pre_bytes is None:
            continue
        file_path = Path(repo_dir, path)
        if file_bytes(file_path) == pre_bytes:
            continue
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(pre_bytes)

    discard_worktree_paths(
        repo_dir,
        new_paths,
        run_git=run_git,
        run_process=run_process,
    )
    restore_index_entries(
        repo_dir,
        pre_index_entries,
        run_git=run_git,
        run_process=run_process,
    )


def restore_index_entries(
    repo_dir: str,
    entries_by_path: dict[str, tuple[str, ...]],
    *,
    run_git: RunGit = run_git_default,
    run_process: RunProcess = subprocess.run,
) -> None:
    for path, entries in entries_by_path.items():
        current_entries = index_entries_for_paths(repo_dir, {path}, run_process=run_process).get(path, ())
        if current_entries:
            run_git(repo_dir, "reset", "-q", "HEAD", "--", path)
        if not entries:
            continue
        result = run_process(
            ["git", "update-index", "--index-info"],
            cwd=repo_dir,
            input="\n".join(entries) + "\n",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"could not restore index entry for {path}: "
                + ((result.stderr or "").strip()[:300] or "git update-index failed")
            )


def discard_worktree_paths(
    repo_dir: str,
    paths: list[str],
    *,
    run_git: RunGit = run_git_default,
    run_process: RunProcess = subprocess.run,
) -> None:
    for path in paths:
        tracked = run_process(
            ["git", "ls-files", "--error-unmatch", "--", path],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if tracked.returncode == 0:
            run_git(repo_dir, "reset", "-q", "HEAD", "--", path)
            run_git(repo_dir, "checkout", "--", path)
        else:
            try:
                Path(repo_dir, path).unlink()
            except OSError:
                pass


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
