"""Registry loader for multi-repo backport configuration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from scripts.common.operational_controls import (
    OperationalPolicy,
    parse_operational_policy,
)
from scripts.common.validation_adapter import (
    ValidationAdapter,
    ValidationRule,
    parse_validation_adapter,
)

__all__ = [
    "BranchEntry",
    "OperationalPolicy",
    "Registry",
    "RepoEntry",
    "ValidationRule",
    "ValidationWaiver",
    "load_registry",
]

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_BRANCH_RE = re.compile(r"^(?!-)(?!.*\.\.)(?!.*//)[A-Za-z0-9._/-]+$")
_VALID_OWNER_TYPES = {"organization", "user"}
_SCHEMA_VERSION = 2

_TOP_LEVEL_KEYS = {"schema_version", "repos"}
_REPO_KEYS = {
    "repo",
    "project_owner",
    "project_owner_type",
    "language",
    "branches",
    "push_repo",
    "validation",
    "validation_waiver",
    "repair_validation_failures",
    "backport_label",
    "llm_conflict_label",
    "max_conflicting_files",
    "automation",
}
_BRANCH_KEYS = {"branch", "project_number"}
_WAIVER_KEYS = {"reason", "approved_by", "expires"}


@dataclass(frozen=True)
class BranchEntry:
    branch: str
    project_number: int


@dataclass(frozen=True)
class ValidationWaiver:
    reason: str
    approved_by: str
    expires: date


@dataclass(frozen=True)
class RepoEntry:
    repo: str
    project_owner: str
    project_owner_type: str
    language: str
    branches: tuple[BranchEntry, ...]
    push_repo: str | None = None
    validation: ValidationAdapter | None = None
    validation_waiver: ValidationWaiver | None = None
    repair_validation_failures: bool = False
    backport_label: str = "backport"
    llm_conflict_label: str = "ai-resolved-conflicts"
    max_conflicting_files: int = 100
    automation: OperationalPolicy = OperationalPolicy()

    @property
    def effective_push_repo(self) -> str:
        return self.push_repo or self.repo


@dataclass(frozen=True)
class Registry:
    repos: tuple[RepoEntry, ...]

    def get_repo(self, repo_full_name: str) -> RepoEntry:
        for entry in self.repos:
            if entry.repo == repo_full_name:
                return entry
        raise KeyError(f"Repository '{repo_full_name}' not found in registry")

    def get_branch(self, repo_full_name: str, branch: str) -> tuple[RepoEntry, BranchEntry]:
        repo_entry = self.get_repo(repo_full_name)
        for b in repo_entry.branches:
            if b.branch == branch:
                return repo_entry, b
        raise KeyError(
            f"Branch '{branch}' not found for '{repo_full_name}' in registry"
        )


def load_registry(path: str) -> Registry:
    """Load and validate the registry from a YAML file."""
    text = Path(path).read_text(encoding="utf-8")
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError(f"Registry file must be a YAML mapping, got {type(raw).__name__}")
    return _parse_registry(raw)


def _parse_registry(raw: dict[str, Any]) -> Registry:
    _reject_unknown_keys(raw, _TOP_LEVEL_KEYS, "registry")
    schema_version = raw.get("schema_version")
    if schema_version != _SCHEMA_VERSION:
        raise ValueError(
            f"schema_version must be {_SCHEMA_VERSION}, got {schema_version!r}"
        )

    # repos
    repos_raw = raw.get("repos", [])
    if not isinstance(repos_raw, list) or not repos_raw:
        raise ValueError("repos must be a non-empty list")

    seen_repos: set[str] = set()
    entries: list[RepoEntry] = []
    for i, repo_raw in enumerate(repos_raw):
        entries.append(_parse_repo_entry(repo_raw, i, seen_repos))

    return Registry(
        repos=tuple(entries),
    )


def _parse_repo_entry(raw: Any, index: int, seen_repos: set[str]) -> RepoEntry:
    if not isinstance(raw, dict):
        raise ValueError(f"repos[{index}] must be a mapping")
    _reject_unknown_keys(raw, _REPO_KEYS, f"repos[{index}]")

    repo = raw.get("repo")
    if not isinstance(repo, str) or not _REPO_RE.match(repo):
        raise ValueError(f"repos[{index}].repo must be a valid 'owner/name' string, got {repo!r}")
    if repo in seen_repos:
        raise ValueError(f"Duplicate repo in registry: {repo!r}")
    seen_repos.add(repo)

    project_owner = raw.get("project_owner")
    if not isinstance(project_owner, str) or not project_owner:
        raise ValueError(f"repos[{index}].project_owner is required")

    project_owner_type = raw.get("project_owner_type", "organization")
    if project_owner_type not in _VALID_OWNER_TYPES:
        raise ValueError(
            f"repos[{index}].project_owner_type must be one of {_VALID_OWNER_TYPES}, "
            f"got {project_owner_type!r}"
        )

    language = raw.get("language")
    if not isinstance(language, str) or not language:
        raise ValueError(f"repos[{index}].language is required")

    push_repo = raw.get("push_repo")
    if push_repo is not None:
        if not isinstance(push_repo, str) or not _REPO_RE.match(push_repo):
            raise ValueError(
                f"repos[{index}].push_repo must be a valid 'owner/name' string"
            )
        if push_repo.split("/", 1)[0] == repo.split("/", 1)[0]:
            raise ValueError(
                f"repos[{index}].push_repo must be a different-owner fork; "
                "omit push_repo for direct upstream pushes"
            )

    validation_raw = raw.get("validation")
    validation = (
        parse_validation_adapter(
            validation_raw,
            field=f"repos[{index}].validation",
        )
        if validation_raw is not None
        else None
    )
    validation_waiver = _parse_validation_waiver(
        raw.get("validation_waiver"),
        index,
    )
    repair_validation_failures = raw.get("repair_validation_failures", False)
    if not isinstance(repair_validation_failures, bool):
        raise ValueError(
            f"repos[{index}].repair_validation_failures must be boolean"
        )
    backport_label = raw.get("backport_label", "backport")
    if not isinstance(backport_label, str) or not backport_label.strip():
        raise ValueError(f"repos[{index}].backport_label must be a non-empty string")
    llm_conflict_label = raw.get("llm_conflict_label", "ai-resolved-conflicts")
    if not isinstance(llm_conflict_label, str) or not llm_conflict_label.strip():
        raise ValueError(f"repos[{index}].llm_conflict_label must be a non-empty string")
    if len(backport_label) > 50 or "\n" in backport_label:
        raise ValueError(f"repos[{index}].backport_label must be at most 50 characters without newlines")
    if len(llm_conflict_label) > 50 or "\n" in llm_conflict_label:
        raise ValueError(
            f"repos[{index}].llm_conflict_label must be at most 50 characters without newlines"
        )
    if backport_label.casefold() == llm_conflict_label.casefold():
        raise ValueError(f"repos[{index}] backport and LLM conflict labels must be distinct")
    max_conflicting_files = raw.get("max_conflicting_files", 100)
    if not isinstance(max_conflicting_files, int) or max_conflicting_files < 1:
        raise ValueError(f"repos[{index}].max_conflicting_files must be a positive integer")
    automation = parse_operational_policy(
        raw.get("automation"),
        field=f"repos[{index}].automation",
    )

    if validation is None and validation_waiver is None:
        raise ValueError(
            f"repos[{index}] must define validation or an explicit validation_waiver"
        )
    if validation_waiver is not None and (
        validation is not None or repair_validation_failures
    ):
        raise ValueError(
            f"repos[{index}].validation_waiver cannot be combined with validation commands or repair"
        )

    branches_raw = raw.get("branches", [])
    if not isinstance(branches_raw, list) or not branches_raw:
        raise ValueError(f"repos[{index}].branches must be a non-empty list")

    seen_branches: set[str] = set()
    seen_projects: set[int] = set()
    branches: list[BranchEntry] = []
    for j, b_raw in enumerate(branches_raw):
        branches.append(_parse_branch_entry(b_raw, index, j, seen_branches, seen_projects))

    return RepoEntry(
        repo=repo,
        project_owner=project_owner,
        project_owner_type=project_owner_type,
        language=language,
        push_repo=push_repo,
        validation=validation,
        validation_waiver=validation_waiver,
        repair_validation_failures=repair_validation_failures,
        backport_label=backport_label,
        llm_conflict_label=llm_conflict_label,
        max_conflicting_files=max_conflicting_files,
        automation=automation,
        branches=tuple(branches),
    )


def _parse_validation_waiver(raw: Any, repo_idx: int) -> ValidationWaiver | None:
    if raw is None:
        return None
    field = f"repos[{repo_idx}].validation_waiver"
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be a mapping")
    _reject_unknown_keys(raw, _WAIVER_KEYS, field)
    values: dict[str, str] = {}
    for key in _WAIVER_KEYS:
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field}.{key} must be a non-empty string")
        values[key] = value.strip()
    try:
        expires = date.fromisoformat(values["expires"])
    except ValueError as exc:
        raise ValueError(f"{field}.expires must be an ISO date (YYYY-MM-DD)") from exc
    if expires < date.today():
        raise ValueError(f"{field} expired on {expires.isoformat()}")
    return ValidationWaiver(
        reason=values["reason"],
        approved_by=values["approved_by"],
        expires=expires,
    )


def _reject_unknown_keys(
    value: dict[str, Any],
    allowed: set[str],
    field: str,
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{field} contains unknown key(s): {', '.join(unknown)}")


def _parse_branch_entry(
    raw: Any, repo_idx: int, branch_idx: int,
    seen_branches: set[str], seen_projects: set[int],
) -> BranchEntry:
    if not isinstance(raw, dict):
        raise ValueError(f"repos[{repo_idx}].branches[{branch_idx}] must be a mapping")
    _reject_unknown_keys(
        raw,
        _BRANCH_KEYS,
        f"repos[{repo_idx}].branches[{branch_idx}]",
    )

    branch = raw.get("branch")
    if (
        not isinstance(branch, str)
        or not _BRANCH_RE.fullmatch(branch)
        or branch.endswith(("/", "."))
        or "@{" in branch
        or branch.endswith(".lock")
    ):
        raise ValueError(
            f"repos[{repo_idx}].branches[{branch_idx}].branch must be a safe Git branch name"
        )
    if branch in seen_branches:
        raise ValueError(f"Duplicate branch '{branch}' in repos[{repo_idx}]")
    seen_branches.add(branch)

    project_number = raw.get("project_number")
    if isinstance(project_number, bool) or not isinstance(project_number, int) or project_number < 1:
        raise ValueError(
            f"repos[{repo_idx}].branches[{branch_idx}].project_number must be a positive integer"
        )
    if project_number in seen_projects:
        raise ValueError(
            f"Duplicate project_number {project_number} in repos[{repo_idx}]"
        )
    seen_projects.add(project_number)

    return BranchEntry(branch=str(branch), project_number=project_number)
