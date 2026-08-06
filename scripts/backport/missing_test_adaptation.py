"""Adapt upstream tests that do not exist on an older target branch.

This module runs strictly inside ``candidate_apply``'s candidate
transaction: any ``fatal`` result makes the caller roll the whole
candidate back (worktree, index and untracked files), so no partial
import survives. It therefore keeps no restore machinery of its own.
"""

from __future__ import annotations

import difflib
import fnmatch
import hashlib
import logging
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

from scripts.ai.runtime import AgentRunResult, extract_agent_result_text, run_agent
from scripts.backport.git_commands import (
    index_stage_exists,
    read_index_stage,
)
from scripts.backport.git_commands import (
    run_git as run_git_default,
)
from scripts.backport.models import (
    DETAIL_PORTED_TARGET_MISSING_TEST_PREFIX,
    BackportCandidate,
)
from scripts.backport.utils import has_conflict_markers

logger = logging.getLogger(__name__)

MAX_TEST_CONTEXT_CHARS = 12000
MAX_EXISTING_TEST_PATHS = 120
DEFAULT_TEST_PATH_PATTERNS = (
    "tests/*.tcl",
    "src/unit/test_*.c",
    "src/unit/test_*.cc",
    "src/unit/test_*.cpp",
)

RunGit = Callable[..., Any]
RunProcess = Callable[..., subprocess.CompletedProcess[str]]
RunAgent = Callable[..., AgentRunResult]


@dataclass
class MissingTestAdaptationResult:
    adapted_paths: list[str] = field(default_factory=list)
    summary: str = ""
    fatal: bool = False


@dataclass(frozen=True)
class FileSnapshot:
    state: Literal["file", "absent", "special", "unreadable"]
    digest: str = ""


def is_test_path(
    path: str,
    test_path_patterns: tuple[str, ...] | list[str] | None = None,
) -> bool:
    normalized = path.replace("\\", "/").strip("/").lower()
    patterns = test_path_patterns or DEFAULT_TEST_PATH_PATTERNS
    return any(
        fnmatch.fnmatchcase(normalized, pattern.replace("\\", "/").lower())
        for pattern in patterns
    )


def build_missing_test_context(
    repo_dir: str,
    path: str,
    source_content: str,
    *,
    run_process: RunProcess = subprocess.run,
) -> str:
    if index_stage_exists(repo_dir, path, 1, run_process=run_process):
        base_content = read_index_stage(repo_dir, path, 1, run_process=run_process)
        diff = "".join(
            difflib.unified_diff(
                base_content.splitlines(keepends=True),
                source_content.splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )
        return "Changed upstream test hunk:\n" + (diff or "(no textual diff)")
    return "Full upstream test content for a new missing test file:\n" + source_content


def adapt_target_missing_tests_with_claude(
    repo_dir: str,
    candidate: BackportCandidate,
    missing_test_sources: dict[str, str],
    *,
    language: str,
    test_path_patterns: tuple[str, ...] | list[str] | None = None,
    run_git: RunGit = run_git_default,
    run_process: RunProcess = subprocess.run,
    run_agent_func: RunAgent = run_agent,
) -> MissingTestAdaptationResult:
    existing_test_paths = list_existing_test_paths(
        repo_dir,
        test_path_patterns=test_path_patterns,
        run_process=run_process,
    )
    prompt = build_test_adaptation_prompt(
        candidate,
        missing_test_sources,
        existing_test_paths,
        language=language,
        test_path_patterns=test_path_patterns,
    )

    try:
        with tempfile.TemporaryDirectory(prefix="valkey-test-adaptation-") as temp_dir:
            sandbox_dir = Path(temp_dir, "repo")
            copy_worktree_for_adaptation(repo_dir, sandbox_dir)
            sandbox_before = snapshot_regular_files(sandbox_dir)

            logger.info(
                "Calling Claude Code to adapt %d target-missing test file(s) for PR #%d onto %s...",
                len(missing_test_sources),
                candidate.source_pr_number,
                candidate.target_branch,
            )
            agent_result = run_agent_func("test_adaptation_edit_only", prompt, cwd=str(sandbox_dir))
            result_text = extract_agent_result_text(agent_result.stdout)
            logger.info(
                "Claude Code test adaptation finished (rc=%d). Result: %s",
                agent_result.returncode,
                result_text[:200] if result_text else "(no result text)",
            )

            sandbox_after = snapshot_regular_files(sandbox_dir)
            changed_paths = changed_snapshot_paths(sandbox_before, sandbox_after)

            if agent_result.returncode != 0:
                detail = agent_result.stderr or result_text or "Claude Code returned non-zero"
                return MissingTestAdaptationResult(
                    summary=f"test adaptation not applied: Claude Code failed: {detail[:200]}",
                    fatal=True,
                )

            if not changed_paths:
                return MissingTestAdaptationResult(
                    summary="test adaptation not applied: no branch-native test changes",
                )

            removed_paths = [
                path
                for path in changed_paths
                if path in sandbox_before
                and not Path(sandbox_dir, path).exists()
                and not Path(sandbox_dir, path).is_symlink()
            ]
            if removed_paths:
                return MissingTestAdaptationResult(
                    summary=(
                        "test adaptation not applied: removed existing test "
                        "path(s): " + ", ".join(removed_paths[:10])
                    ),
                    fatal=True,
                )

            invalid_paths = invalid_sandbox_test_paths(
                sandbox_dir,
                changed_paths,
                sandbox_before=sandbox_before,
                existing_test_paths=set(existing_test_paths),
                test_path_patterns=test_path_patterns,
            )
            if invalid_paths:
                return MissingTestAdaptationResult(
                    summary=(
                        "test adaptation not applied: invalid generated test path(s): " + ", ".join(invalid_paths[:10])
                    ),
                    fatal=True,
                )

            unsafe_destination_paths = [
                path
                for path in changed_paths
                if safe_regular_file(Path(repo_dir), path) is None
            ]
            if unsafe_destination_paths:
                return MissingTestAdaptationResult(
                    summary=(
                        "test adaptation not applied: unsafe repository "
                        "test path(s): " + ", ".join(unsafe_destination_paths[:10])
                    ),
                    fatal=True,
                )

            try:
                for path in changed_paths:
                    source = safe_regular_file(sandbox_dir, path)
                    destination = safe_regular_file(Path(repo_dir), path)
                    if source is None or destination is None:
                        raise RuntimeError(f"unsafe test adaptation path: {path}")
                    destination.write_bytes(source.read_bytes())
                    run_git(repo_dir, "add", path)
            except Exception as exc:  # noqa: BLE001 - fatal makes the caller roll back
                return MissingTestAdaptationResult(
                    summary=f"test adaptation import failed: {str(exc)[:200]}",
                    fatal=True,
                )

            return MissingTestAdaptationResult(
                adapted_paths=changed_paths,
                summary=f"{DETAIL_PORTED_TARGET_MISSING_TEST_PREFIX} " + ", ".join(changed_paths),
            )
    except Exception as exc:  # noqa: BLE001 - adaptation infrastructure failures must fail closed
        return MissingTestAdaptationResult(
            summary=f"test adaptation failed unexpectedly: {str(exc)[:200]}",
            fatal=True,
        )


def build_test_adaptation_prompt(
    candidate: BackportCandidate,
    missing_test_sources: dict[str, str],
    existing_test_paths: list[str],
    *,
    language: str,
    test_path_patterns: tuple[str, ...] | list[str] | None = None,
) -> str:
    source_sections = "\n\n".join(
        f"### Missing upstream test file: {path}\n```\n{content[:MAX_TEST_CONTEXT_CHARS]}\n```"
        for path, content in sorted(missing_test_sources.items())
    )
    existing_tests = "\n".join(
        f"- {path}" for path in existing_test_paths[:MAX_EXISTING_TEST_PATHS]
    )
    patterns = "\n".join(
        f"- `{pattern}`"
        for pattern in (test_path_patterns or DEFAULT_TEST_PATH_PATTERNS)
    )
    return (
        f"You are adapting test coverage for a {language} backport.\n\n"
        f'Source PR #{candidate.source_pr_number}: "{candidate.source_pr_title}"\n'
        f"URL: {candidate.source_pr_url}\n"
        f"Target branch: {candidate.target_branch}\n\n"
        f"The upstream PR changed test file(s) that do not exist on this target "
        f"branch. The cherry-pick has already kept those missing files absent. "
        f"Your task is to decide whether equivalent coverage can be added using "
        f"the target branch's existing test format.\n\n"
        f"Missing upstream test context:\n{source_sections}\n\n"
        f"Existing test files on the target branch include:\n"
        f"{existing_tests or '- (none found)'}\n\n"
        f"Test files on this branch match these path patterns:\n{patterns}\n\n"
        f"CRITICAL constraints:\n"
        f"- Edit existing test files only. Do not edit source, build, workflow, "
        f"or metadata files.\n"
        f"- Prefer modifying an existing test file that matches the target "
        f"branch's conventions. Do not create new files.\n"
        f"- Preserve the source PR's test intent only. Do not add unrelated "
        f"coverage or new product behavior.\n"
        f"- Before using a helper, command, macro, fixture, or test harness, "
        f"verify it exists on this target branch.\n"
        f"- Do not recreate the missing upstream file path.\n"
        f"- Do not run `git add`, `git commit`, or any network command.\n"
        f"- If equivalent branch-native coverage is not practical, make no file "
        f"changes and explain that in your final result.\n\n"
        f"Do not wrap output in markdown. Edit files directly when safe."
    )


def list_existing_test_paths(
    repo_dir: str,
    *,
    test_path_patterns: tuple[str, ...] | list[str] | None = None,
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
    return [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() and is_test_path(line.strip(), test_path_patterns)
    ]


def copy_worktree_for_adaptation(repo_dir: str, sandbox_dir: Path) -> None:
    shutil.copytree(
        repo_dir,
        sandbox_dir,
        ignore=_ignore_git_and_symlinks,
    )


def _ignore_git_and_symlinks(directory: str, names: list[str]) -> set[str]:
    """Keep repository symlinks out of the agent's writable sandbox."""
    ignored = {".git"} if ".git" in names else set()
    ignored.update(
        name for name in names if Path(directory, name).is_symlink()
    )
    return ignored


def safe_regular_file(root: Path, relative_path: str) -> Path | None:
    """Return a non-symlink regular file contained by *root*, or ``None``."""
    normalized = Path(*relative_path.replace("\\", "/").split("/"))
    if normalized.is_absolute() or ".." in normalized.parts:
        return None
    candidate = root / normalized
    current = root
    for part in normalized.parts:
        current = current / part
        if current.is_symlink():
            return None
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError):
        return None
    return candidate if candidate.is_file() else None


def snapshot_regular_files(root: Path) -> dict[str, FileSnapshot]:
    snapshots: dict[str, FileSnapshot] = {}
    for path in root.rglob("*"):
        if not path.is_symlink() and path.is_file():
            snapshots[path.relative_to(root).as_posix()] = snapshot_path(path)
    return snapshots


def snapshot_path(path: Path) -> FileSnapshot:
    """Digest a path for change detection without buffering file contents."""
    if not path.exists():
        return FileSnapshot("absent")
    if not path.is_file():
        return FileSnapshot("special")
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return FileSnapshot("file", digest.hexdigest())
    except OSError:
        return FileSnapshot("unreadable")


def changed_snapshot_paths(
    before: dict[str, FileSnapshot],
    after: dict[str, FileSnapshot],
) -> list[str]:
    return sorted(path for path in set(before) | set(after) if before.get(path) != after.get(path))


def invalid_sandbox_test_paths(
    sandbox_dir: Path,
    changed_paths: list[str],
    *,
    sandbox_before: dict[str, FileSnapshot],
    existing_test_paths: set[str],
    test_path_patterns: tuple[str, ...] | list[str] | None = None,
) -> list[str]:
    invalid_paths = []
    for path in changed_paths:
        if (
            path not in sandbox_before
            or path not in existing_test_paths
            or not is_test_path(path, test_path_patterns)
        ):
            invalid_paths.append(path)
            continue
        file_path = safe_regular_file(sandbox_dir, path)
        if file_path is None:
            invalid_paths.append(path)
            continue
        content = file_path.read_text(encoding="utf-8", errors="replace")
        if has_conflict_markers(content):
            invalid_paths.append(path)
    return invalid_paths
