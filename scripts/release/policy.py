"""Load the deliberately small release allow-list."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from scripts.release.models import ReleasePolicy


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate mapping keys.

    yaml.safe_load silently keeps the last duplicate, which would let a
    second `authorized_teams:` line replace the reviewed one.
    """


def _construct_mapping_no_duplicates(loader: yaml.SafeLoader, node: yaml.MappingNode) -> dict:
    keys = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node)
        if key in keys:
            raise ValueError(f"duplicate release policy key: {key}")
        keys.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_no_duplicates,
)


def load_policy(path: str | Path) -> ReleasePolicy:
    raw = yaml.load(Path(path).read_text(encoding="utf-8"), Loader=_StrictLoader)
    # bool is an int subclass, so `True == 1`; require a literal integer 1.
    if not isinstance(raw, dict) or type(raw.get("schema_version")) is not int or raw.get("schema_version") != 1:
        raise ValueError("release policy must be a schema_version: 1 mapping")

    allowed = {
        "schema_version",
        "repo",
        "authorized_teams",
        "branches",
        "checks_workflow",
        "required_checks",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown release policy key(s): {', '.join(sorted(unknown))}")

    repo = _nonempty(raw.get("repo"), "repo")
    teams = _strings(raw.get("authorized_teams"), "authorized_teams")
    for team in teams:
        if team.count("/") != 1 or any(not part for part in team.split("/")):
            raise ValueError("every authorized_teams entry must be org/team-slug")
    if len(set(teams)) != len(teams):
        raise ValueError("authorized_teams contains duplicates")
    workflow = _nonempty(raw.get("checks_workflow"), "checks_workflow")
    if "/" in workflow or not workflow.endswith((".yml", ".yaml")):
        raise ValueError("checks_workflow must be a workflow filename")

    branches = _strings(raw.get("branches"), "branches")
    checks = _strings(raw.get("required_checks"), "required_checks")
    if len(set(branches)) != len(branches):
        raise ValueError("branches contains duplicates")
    if len(set(checks)) != len(checks):
        raise ValueError("required_checks contains duplicates")

    return ReleasePolicy(
        repo=repo,
        authorized_teams=teams,
        branches=branches,
        checks_workflow=workflow,
        required_checks=checks,
    )


def validate_branch(policy: ReleasePolicy, branch: str) -> str:
    branch = branch.strip()
    if branch not in policy.branches:
        raise ValueError(f"branch {branch!r} is not releasable; allowed: {', '.join(policy.branches)}")
    return branch


def _nonempty(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _strings(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list")
    result = tuple(_nonempty(item, name) for item in value)
    return result
