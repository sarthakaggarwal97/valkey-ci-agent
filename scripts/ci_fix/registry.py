"""Typed repository registry for the CI-fix control plane.

``repos.yml`` is already the source of truth for Valkey automation.  This
module reads the nested ``ci_fix`` section without coupling the CI-fix engine
to the backport registry's project-board requirements.

The command-line interface is intentionally small and workflow-oriented:

.. code-block:: console

   python -m scripts.ci_fix.registry resolve \
     --registry repos.yml --repo valkey-io/valkey
   python -m scripts.ci_fix.registry poll-matrix --registry repos.yml

Both commands can append validated values to ``GITHUB_OUTPUT``.  Workflows use
those values before minting repository-scoped credentials, so a dispatch input
cannot expand the GitHub App token beyond an explicitly enabled repository.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from scripts.ci_fix.policy import (
    DEFAULT_AUTO_PUBLISH_PATTERNS,
    DEFAULT_PROTECTED_PATTERNS,
)
from scripts.common.git_clone import REPO_RE

_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_REF_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
_WORKFLOW_RE = re.compile(r"^(?:\.github/workflows/)?[A-Za-z0-9_.-]+\.ya?ml$")


@dataclass(frozen=True)
class CiFixRepoConfig:
    """Validated CI-fix policy for one target repository."""

    repo: str
    owner: str
    name: str
    enabled: bool = False
    poll_comments: bool = False
    authorization_org: str = ""
    authorization_team: str = "contributors"
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",)
    history_branches: tuple[str, ...] = ()
    baseline_runs: int = 3
    flaky_verify_runs: int = 10
    remote_parallelism: int = 5
    remote_sample_timeout_minutes: int = 15
    remote_budget_minutes: int = 45
    minimum_confidence: float = 0.8
    verification_workflow: str = ""
    verification_ref: str = ""
    protected_paths: tuple[str, ...] = DEFAULT_PROTECTED_PATTERNS
    auto_publish_paths: tuple[str, ...] = DEFAULT_AUTO_PUBLISH_PATTERNS

    def workflow_outputs(self) -> dict[str, str]:
        """Return newline-safe scalar outputs consumed by GitHub Actions."""
        return {
            "repo": self.repo,
            "owner": self.owner,
            "name": self.name,
            "authorization_org": self.authorization_org,
            "authorization_team": self.authorization_team,
            "allowed_branch_prefixes_json": json.dumps(self.allowed_branch_prefixes),
            "history_branches_json": json.dumps(self.history_branches),
            "baseline_runs": str(self.baseline_runs),
            "flaky_verify_runs": str(self.flaky_verify_runs),
            "remote_parallelism": str(self.remote_parallelism),
            "remote_sample_timeout_minutes": str(self.remote_sample_timeout_minutes),
            "remote_budget_minutes": str(self.remote_budget_minutes),
            "minimum_confidence": str(self.minimum_confidence),
            "verification_workflow": self.verification_workflow,
            "verification_ref": self.verification_ref,
            "protected_paths_json": json.dumps(self.protected_paths),
            "auto_publish_paths_json": json.dumps(self.auto_publish_paths),
        }


@dataclass(frozen=True)
class CiFixRegistry:
    repos: tuple[CiFixRepoConfig, ...]

    def get_repo(self, repo_full_name: str, *, require_enabled: bool = True) -> CiFixRepoConfig:
        for entry in self.repos:
            if entry.repo != repo_full_name:
                continue
            if require_enabled and not entry.enabled:
                raise KeyError(
                    f"Repository {repo_full_name!r} is present in the registry "
                    "but ci_fix is not enabled"
                )
            return entry
        raise KeyError(f"Repository {repo_full_name!r} is not present in the registry")

    def poll_matrix(self) -> dict[str, list[dict[str, str]]]:
        include = []
        for entry in self.repos:
            if not (entry.enabled and entry.poll_comments):
                continue
            include.append(
                {
                    "repo": entry.repo,
                    "repo_slug": entry.repo.replace("/", "-"),
                    "owner": entry.owner,
                    "name": entry.name,
                    "authorization_org": entry.authorization_org,
                    "authorization_team": entry.authorization_team,
                }
            )
        return {"include": include}


def load_ci_fix_registry(path: str) -> CiFixRegistry:
    """Load and validate CI-fix settings from the shared repository registry."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Registry file must be a YAML mapping")
    repos_raw = raw.get("repos")
    if not isinstance(repos_raw, list) or not repos_raw:
        raise ValueError("repos must be a non-empty list")

    seen: set[str] = set()
    entries = tuple(_parse_repo(item, index, seen) for index, item in enumerate(repos_raw))
    return CiFixRegistry(entries)


def _parse_repo(raw: Any, index: int, seen: set[str]) -> CiFixRepoConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"repos[{index}] must be a mapping")
    repo = raw.get("repo")
    if not isinstance(repo, str) or not REPO_RE.fullmatch(repo):
        raise ValueError(f"repos[{index}].repo must be a valid owner/name")
    if repo in seen:
        raise ValueError(f"Duplicate repo in registry: {repo!r}")
    seen.add(repo)
    owner, name = repo.split("/", 1)

    branch_names = _registry_branches(raw.get("branches", []), index)
    settings = raw.get("ci_fix")
    if settings is None:
        settings = {}
    if not isinstance(settings, dict):
        raise ValueError(f"repos[{index}].ci_fix must be a mapping")

    enabled = _bool(settings, "enabled", False, index)
    poll_comments = _bool(settings, "poll_comments", False, index)
    if poll_comments and not enabled:
        raise ValueError(
            f"repos[{index}].ci_fix.poll_comments requires ci_fix.enabled"
        )

    auth_org = _name(
        settings.get("authorization_org", owner),
        f"repos[{index}].ci_fix.authorization_org",
    )
    auth_team = _name(
        settings.get("authorization_team", "contributors"),
        f"repos[{index}].ci_fix.authorization_team",
    )
    branch_prefixes = _string_tuple(
        settings.get("allowed_branch_prefixes", ["agent/backport/"]),
        f"repos[{index}].ci_fix.allowed_branch_prefixes",
        require_nonempty=enabled,
        validator=_valid_ref_fragment,
    )
    history_branches = _string_tuple(
        settings.get("history_branches", branch_names),
        f"repos[{index}].ci_fix.history_branches",
        validator=_valid_ref_fragment,
    )
    baseline_runs = _bounded_int(settings, "baseline_runs", 3, index, 1, 20)
    flaky_verify_runs = _bounded_int(settings, "flaky_verify_runs", 10, index, 2, 100)
    remote_parallelism = _bounded_int(
        settings, "remote_parallelism", 5, index, 1, 10,
    )
    remote_sample_timeout_minutes = _bounded_int(
        settings, "remote_sample_timeout_minutes", 15, index, 1, 60,
    )
    remote_budget_minutes = _bounded_int(
        settings, "remote_budget_minutes", 45, index, 5, 60,
    )
    minimum_confidence = _bounded_float(
        settings, "minimum_confidence", 0.8, index, 0.0, 1.0,
    )

    verification_workflow = _optional_string(
        settings.get("verification_workflow", ""),
        f"repos[{index}].ci_fix.verification_workflow",
    )
    if verification_workflow and not _WORKFLOW_RE.fullmatch(verification_workflow):
        raise ValueError(
            f"repos[{index}].ci_fix.verification_workflow must name a YAML workflow"
        )
    verification_ref = _optional_string(
        settings.get("verification_ref", ""),
        f"repos[{index}].ci_fix.verification_ref",
    )
    if verification_ref and not _valid_ref_fragment(verification_ref):
        raise ValueError(f"repos[{index}].ci_fix.verification_ref is malformed")
    if bool(verification_workflow) != bool(verification_ref):
        raise ValueError(
            f"repos[{index}].ci_fix.verification_workflow and verification_ref "
            "must either both be set or both be empty"
        )

    protected_paths = _string_tuple(
        settings.get("protected_paths", list(DEFAULT_PROTECTED_PATTERNS)),
        f"repos[{index}].ci_fix.protected_paths",
    )
    auto_publish_paths = _string_tuple(
        settings.get("auto_publish_paths", list(DEFAULT_AUTO_PUBLISH_PATTERNS)),
        f"repos[{index}].ci_fix.auto_publish_paths",
    )

    return CiFixRepoConfig(
        repo=repo,
        owner=owner,
        name=name,
        enabled=enabled,
        poll_comments=poll_comments,
        authorization_org=auth_org,
        authorization_team=auth_team,
        allowed_branch_prefixes=branch_prefixes,
        history_branches=history_branches,
        baseline_runs=baseline_runs,
        flaky_verify_runs=flaky_verify_runs,
        remote_parallelism=remote_parallelism,
        remote_sample_timeout_minutes=remote_sample_timeout_minutes,
        remote_budget_minutes=remote_budget_minutes,
        minimum_confidence=minimum_confidence,
        verification_workflow=verification_workflow,
        verification_ref=verification_ref,
        protected_paths=protected_paths,
        auto_publish_paths=auto_publish_paths,
    )


def _registry_branches(raw: Any, repo_index: int) -> list[str]:
    if not isinstance(raw, list):
        raise ValueError(f"repos[{repo_index}].branches must be a list")
    result: list[str] = []
    for branch_index, branch in enumerate(raw):
        if not isinstance(branch, dict):
            raise ValueError(
                f"repos[{repo_index}].branches[{branch_index}] must be a mapping"
            )
        name = branch.get("branch")
        if not isinstance(name, str) or not _valid_ref_fragment(name):
            raise ValueError(
                f"repos[{repo_index}].branches[{branch_index}].branch is malformed"
            )
        result.append(name)
    return result


def _bool(raw: dict[str, Any], key: str, default: bool, repo_index: int) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"repos[{repo_index}].ci_fix.{key} must be a boolean")
    return value


def _bounded_int(
    raw: dict[str, Any], key: str, default: int, repo_index: int,
    minimum: int, maximum: int,
) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(
            f"repos[{repo_index}].ci_fix.{key} must be an integer "
            f"between {minimum} and {maximum}"
        )
    return value


def _bounded_float(
    raw: dict[str, Any], key: str, default: float, repo_index: int,
    minimum: float, maximum: float,
) -> float:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"repos[{repo_index}].ci_fix.{key} must be numeric")
    converted = float(value)
    if not minimum <= converted <= maximum:
        raise ValueError(
            f"repos[{repo_index}].ci_fix.{key} must be between {minimum} and {maximum}"
        )
    return converted


def _name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
        raise ValueError(f"{label} must be a non-empty GitHub name")
    return value


def _optional_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or "\n" in value or "\r" in value:
        raise ValueError(f"{label} must be a string without newlines")
    return value.strip()


def _string_tuple(
    value: Any,
    label: str,
    *,
    require_nonempty: bool = False,
    validator: Any = None,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    items: list[str] = []
    for item_index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip() or "\n" in item or "\r" in item:
            raise ValueError(f"{label}[{item_index}] must be a non-empty string")
        cleaned = item.strip()
        if validator is not None and not validator(cleaned):
            raise ValueError(f"{label}[{item_index}] is malformed")
        if cleaned not in items:
            items.append(cleaned)
    if require_nonempty and not items:
        raise ValueError(f"{label} must not be empty when ci_fix is enabled")
    return tuple(items)


def _valid_ref_fragment(value: str) -> bool:
    return (
        bool(_REF_RE.fullmatch(value))
        and ".." not in value
        and not value.startswith("/")
        and not value.endswith(".")
        and "//" not in value
    )


def _write_outputs(path: str, values: dict[str, str]) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            if "\n" in value or "\r" in value:
                raise ValueError(f"Workflow output {key!r} contains a newline")
            handle.write(f"{key}={value}\n")


def _config_json(entry: CiFixRepoConfig) -> str:
    data = asdict(entry)
    return json.dumps(data, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    resolve = subparsers.add_parser("resolve", help="Resolve one enabled target repository")
    resolve.add_argument("--registry", required=True)
    resolve.add_argument("--repo", required=True)
    resolve.add_argument("--output-file", default="")

    matrix = subparsers.add_parser("poll-matrix", help="Build the comment-poller matrix")
    matrix.add_argument("--registry", required=True)
    matrix.add_argument("--output-file", default="")

    args = parser.parse_args(argv)
    try:
        registry = load_ci_fix_registry(args.registry)
        if args.command == "resolve":
            entry = registry.get_repo(args.repo)
            values = entry.workflow_outputs()
            values["config_json"] = _config_json(entry)
        else:
            matrix_value = registry.poll_matrix()
            values = {
                "matrix": json.dumps(matrix_value, sort_keys=True),
                "has_entries": "true" if matrix_value["include"] else "false",
            }
    except (OSError, KeyError, ValueError, yaml.YAMLError) as exc:
        print(f"ci-fix registry error: {exc}", file=sys.stderr)
        return 2

    if args.output_file:
        _write_outputs(args.output_file, values)
    else:
        for key, value in values.items():
            print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
