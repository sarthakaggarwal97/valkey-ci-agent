"""Read-only live preflight for the backport repository registry."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from typing import Any

from github import Auth, Github

from scripts.backport.registry import Registry, RepoEntry, load_registry
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.common.github_client import retry_github_call

_REQUIRED_STATUS_OPTIONS = {"to be backported", "done"}


@dataclass(frozen=True)
class RegistryPreflightReport:
    repositories: int
    branches: int
    projects: int
    labels: int
    validation_commands: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def preflight_registry(
    registry_path: str,
    token: str,
    *,
    repo_filter: str = "",
    github_client: Any | None = None,
    graphql_client: Any | None = None,
) -> RegistryPreflightReport:
    """Resolve every live dependency required by registered backport flows."""
    if not token or "\n" in token or "\r" in token:
        raise ValueError("an App installation token is required")

    registry = load_registry(registry_path)
    entries = _selected_entries(registry, repo_filter)
    gh = github_client or Github(auth=Auth.Token(token))
    gql = graphql_client or GitHubGraphQLClient(token)

    repositories: set[str] = set()
    branches = 0
    projects = 0
    labels = 0
    validation_commands = 0
    for entry in entries:
        repository = _resolve_repository(gh, entry.repo)
        repositories.add(entry.repo.casefold())
        if entry.effective_push_repo.casefold() != entry.repo.casefold():
            _resolve_repository(gh, entry.effective_push_repo)
            repositories.add(entry.effective_push_repo.casefold())

        for label in (entry.backport_label, entry.llm_conflict_label):
            _resolve_label(repository, entry.repo, label)
            labels += 1

        entry_validation_commands = len(entry.build_commands) + sum(
            len(rule.commands) for rule in entry.validation_rules
        )
        if entry.repair_validation_failures and entry_validation_commands == 0:
            raise RuntimeError(
                f"{entry.repo} enables repair_validation_failures without any build_commands or validation_rules"
            )
        validation_commands += entry_validation_commands

        for branch in entry.branches:
            _resolve_branch(repository, entry.repo, branch.branch)
            branches += 1
            _validate_project(
                gql,
                owner=entry.project_owner,
                owner_type=entry.project_owner_type,
                number=branch.project_number,
            )
            projects += 1

    return RegistryPreflightReport(
        repositories=len(repositories),
        branches=branches,
        projects=projects,
        labels=labels,
        validation_commands=validation_commands,
    )


def _selected_entries(
    registry: Registry,
    repo_filter: str,
) -> tuple[RepoEntry, ...]:
    if not repo_filter:
        return registry.repos
    return (registry.get_repo(repo_filter),)


def _resolve_repository(gh: Any, name: str) -> Any:
    repository = retry_github_call(
        lambda: gh.get_repo(name),
        retries=3,
        description=f"resolve repository {name}",
    )
    resolved_name = str(getattr(repository, "full_name", "") or "")
    if resolved_name.casefold() != name.casefold():
        raise RuntimeError(f"repository {name} resolved as {resolved_name or '<missing>'}")
    if bool(getattr(repository, "archived", False)):
        raise RuntimeError(f"repository {name} is archived")
    if bool(getattr(repository, "disabled", False)):
        raise RuntimeError(f"repository {name} is disabled")
    return repository


def _resolve_label(repository: Any, repository_name: str, label: str) -> None:
    resolved = retry_github_call(
        lambda: repository.get_label(label),
        retries=3,
        description=f"resolve label {repository_name}:{label}",
    )
    resolved_name = str(getattr(resolved, "name", "") or "")
    if resolved_name.casefold() != label.casefold():
        raise RuntimeError(f"label {repository_name}:{label} resolved as {resolved_name or '<missing>'}")


def _resolve_branch(repository: Any, repository_name: str, branch: str) -> None:
    resolved = retry_github_call(
        lambda: repository.get_branch(branch),
        retries=3,
        description=f"resolve branch {repository_name}:{branch}",
    )
    resolved_name = str(getattr(resolved, "name", "") or "")
    if resolved_name and resolved_name != branch:
        raise RuntimeError(f"branch {repository_name}:{branch} resolved as {resolved_name}")


def _validate_project(
    gql: Any,
    *,
    owner: str,
    owner_type: str,
    number: int,
) -> None:
    owner_field = "user" if owner_type == "user" else "organization"
    cursor: str | None = None
    fields: list[dict[str, Any]] = []
    while True:
        data = gql.execute(
            _project_fields_query(owner_field),
            {"owner": owner, "number": number, "cursor": cursor},
        )
        owner_data = data.get(owner_field)
        project = owner_data.get("projectV2") if isinstance(owner_data, dict) else None
        if not isinstance(project, dict) or not project.get("id"):
            raise RuntimeError(f"Project {owner}/{number} not found")
        connection = project.get("fields")
        if not isinstance(connection, dict):
            raise RuntimeError(f"Project {owner}/{number} has no fields")
        nodes = connection.get("nodes")
        if not isinstance(nodes, list):
            raise RuntimeError(f"Project {owner}/{number} fields are malformed")
        fields.extend(node for node in nodes if isinstance(node, dict))
        page_info = connection.get("pageInfo")
        if not isinstance(page_info, dict):
            raise RuntimeError(f"Project {owner}/{number} field pagination is malformed")
        if not page_info.get("hasNextPage"):
            break
        next_cursor = page_info.get("endCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            raise RuntimeError(f"Project {owner}/{number} fields are truncated")
        cursor = next_cursor

    for field in fields:
        if (
            field.get("__typename") != "ProjectV2SingleSelectField"
            or str(field.get("name", "")).strip().casefold() != "status"
        ):
            continue
        options = field.get("options")
        if not isinstance(options, list):
            break
        names = {str(option.get("name", "")).strip().casefold() for option in options if isinstance(option, dict)}
        missing = sorted(_REQUIRED_STATUS_OPTIONS - names)
        if not missing:
            return
        raise RuntimeError(f"Project {owner}/{number} Status field lacks options: " + ", ".join(missing))
    raise RuntimeError(f"Project {owner}/{number} has no usable Status single-select field")


def _project_fields_query(owner_field: str) -> str:
    return f"""
query($owner: String!, $number: Int!, $cursor: String) {{
  {owner_field}(login: $owner) {{
    projectV2(number: $number) {{
      id
      fields(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          __typename
          ... on ProjectV2SingleSelectField {{
            id
            name
            options {{ id name }}
          }}
          ... on ProjectV2FieldCommon {{ id name }}
        }}
      }}
    }}
  }}
}}
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", default="")
    parser.add_argument(
        "--token",
        default=os.environ.get("REGISTRY_PREFLIGHT_GITHUB_TOKEN", ""),
        help="App installation token with read access to repositories, labels, and projects.",
    )
    args = parser.parse_args(argv)
    if not args.token:
        parser.error("--token or REGISTRY_PREFLIGHT_GITHUB_TOKEN is required")
    try:
        report = preflight_registry(
            args.registry,
            args.token,
            repo_filter=args.repo,
        )
    except (KeyError, OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
