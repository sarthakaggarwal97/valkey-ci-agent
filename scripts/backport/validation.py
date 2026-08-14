"""Path-based validation command selection for backport branches."""

from __future__ import annotations

import subprocess
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from scripts.backport.registry import ValidationRule

RunValidationCommands = Callable[..., tuple[bool, str]]
ChangedPathsSinceBase = Callable[[str, str], Iterable[str]]


def changed_paths_since_base(repo_dir: str, base_ref: str) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_ref}...HEAD"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())


def select_validation_commands(
    base_commands: Iterable[str],
    validation_rules: Iterable["ValidationRule"],
    changed_paths: Iterable[str],
) -> list[str]:
    commands: list[str] = []
    seen: set[str] = set()
    for command in base_commands:
        if command not in seen:
            commands.append(command)
            seen.add(command)

    paths = tuple(changed_paths)
    for rule in validation_rules:
        if not _rule_matches(rule.paths, paths):
            continue
        for command in rule.commands:
            if command not in seen:
                commands.append(command)
                seen.add(command)
    return commands


def validate_branch(
    repo_dir: str,
    base_ref: str,
    base_commands: Iterable[str],
    validation_rules: Iterable["ValidationRule"],
    *,
    log_path: str | None = None,
    run_empty: bool,
    pass_log_path: bool,
    run_commands: RunValidationCommands | None = None,
    changed_paths_func: ChangedPathsSinceBase = changed_paths_since_base,
) -> tuple[bool, str]:
    """Select and run validation commands for either backport path."""
    commands = select_validation_commands(
        base_commands,
        validation_rules,
        changed_paths_func(repo_dir, base_ref),
    )
    if not commands and not run_empty:
        return True, ""
    if run_commands is None:
        from scripts.common.build_validator import run_build_commands

        run_commands = run_build_commands
    if pass_log_path:
        return run_commands(repo_dir, commands, log_path=log_path)
    return run_commands(repo_dir, commands)


def _rule_matches(patterns: Iterable[str], changed_paths: Iterable[str]) -> bool:
    return any(fnmatch(path, pattern) for path in changed_paths for pattern in patterns)
