"""Live onboarding preflight for the strict repository registry."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Callable

from github import Auth, Github

from scripts.backport.registry import Registry, RepoEntry, load_registry
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.common.github_client import retry_github_call
from scripts.common.proc import NETWORK_ENV, PROCESS_BASICS, filter_env

_INSTALLATION_URL = "https://api.github.com/installation"
_MAX_INSTALLATION_RESPONSE_BYTES = 64 * 1024
_REQUIRED_APP_PERMISSIONS = {
    "actions": "write",
    "contents": "write",
    "issues": "write",
    "members": "read",
    "metadata": "read",
    "organization_projects": "write",
    "pull_requests": "write",
    "workflows": "write",
}
_PERMISSION_RANK = {"read": 1, "write": 2, "admin": 3}
_REQUIRED_STATUS_OPTIONS = {"to be backported", "done"}


@dataclass(frozen=True)
class RegistryPreflightReport:
    repositories: int
    branches: int
    projects: int
    labels: int
    images: int
    permissions: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def preflight_registry(
    registry_path: str,
    token: str,
    *,
    repo_filter: str = "",
    github_client: Any | None = None,
    graphql_client: Any | None = None,
    permission_loader: Callable[[str], dict[str, str]] | None = None,
    image_resolver: Callable[[str], None] | None = None,
) -> RegistryPreflightReport:
    """Resolve every live dependency required to enable registry automation."""
    if not token:
        raise ValueError("an App installation token is required")
    registry = load_registry(registry_path)
    entries = _selected_entries(registry, repo_filter)
    permissions = (permission_loader or load_installation_permissions)(token)
    _validate_app_permissions(permissions)
    gh = github_client or Github(auth=Auth.Token(token))
    gql = graphql_client or GitHubGraphQLClient(token)
    resolve_image = image_resolver or resolve_container_image

    repositories: set[str] = set()
    labels = 0
    branches = 0
    projects = 0
    images: set[str] = set()
    for entry in entries:
        repository = _resolve_repository(gh, entry.repo)
        repositories.add(entry.repo.casefold())
        if entry.effective_push_repo.casefold() != entry.repo.casefold():
            _resolve_repository(gh, entry.effective_push_repo)
            repositories.add(entry.effective_push_repo.casefold())

        for label in (entry.backport_label, entry.llm_conflict_label):
            _resolve_label(repository, entry.repo, label)
            labels += 1

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

        if entry.validation is not None and entry.validation.image not in images:
            resolve_image(entry.validation.image)
            images.add(entry.validation.image)

    return RegistryPreflightReport(
        repositories=len(repositories),
        branches=branches,
        projects=projects,
        labels=labels,
        images=len(images),
        permissions={
            name: permissions[name]
            for name in sorted(_REQUIRED_APP_PERMISSIONS)
        },
    )


def load_installation_permissions(token: str) -> dict[str, str]:
    """Read the effective permission grant of an App installation token."""
    if not token or "\n" in token or "\r" in token:
        raise ValueError("installation token must be non-empty and single-line")
    request = urllib.request.Request(
        _INSTALLATION_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "valkey-ci-agent-registry-preflight",
        },
    )
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read(_MAX_INSTALLATION_RESPONSE_BYTES + 1)
            if len(payload) > _MAX_INSTALLATION_RESPONSE_BYTES:
                raise RuntimeError("installation permission response is oversized")
            try:
                data = json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise RuntimeError(
                    "installation permission response is invalid JSON",
                ) from exc
            if not isinstance(data, dict) or not isinstance(
                data.get("permissions"),
                dict,
            ):
                raise RuntimeError(
                    "installation response has no permissions object",
                )
            permissions = data["permissions"]
            if not all(
                isinstance(name, str) and isinstance(level, str)
                for name, level in permissions.items()
            ):
                raise RuntimeError("installation permissions are malformed")
            return dict(permissions)
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                raise RuntimeError(
                    f"installation permission request failed with HTTP {exc.code}",
                ) from exc
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt == 2:
                raise RuntimeError(
                    "installation permission request failed",
                ) from exc
        time.sleep(2**attempt)
    raise RuntimeError("installation permission request failed") from last_error


def resolve_container_image(image: str) -> None:
    """Resolve a pinned OCI manifest without pulling or running the image."""
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("Docker is required to resolve validation image digests")
    result = subprocess.run(
        [docker, "manifest", "inspect", image],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=filter_env(PROCESS_BASICS + NETWORK_ENV),
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"validation image digest did not resolve: {image}")


def _selected_entries(
    registry: Registry,
    repo_filter: str,
) -> tuple[RepoEntry, ...]:
    if not repo_filter:
        return registry.repos
    entry = registry.get_repo(repo_filter)
    return (entry,)


def _resolve_repository(gh: Any, name: str) -> Any:
    repository = retry_github_call(
        lambda: gh.get_repo(name),
        retries=3,
        description=f"resolve repository {name}",
    )
    resolved_name = str(getattr(repository, "full_name", "") or "")
    if resolved_name.casefold() != name.casefold():
        raise RuntimeError(
            f"repository {name} resolved as {resolved_name or '<missing>'}",
        )
    if bool(getattr(repository, "archived", False)):
        raise RuntimeError(f"repository {name} is archived")
    if bool(getattr(repository, "disabled", False)):
        raise RuntimeError(f"repository {name} is disabled")
    return repository


def _resolve_label(repository: Any, repository_name: str, label: str) -> Any:
    return retry_github_call(
        lambda: repository.get_label(label),
        retries=3,
        description=f"resolve label {repository_name}:{label}",
    )


def _resolve_branch(
    repository: Any,
    repository_name: str,
    branch: str,
) -> Any:
    return retry_github_call(
        lambda: repository.get_branch(branch),
        retries=3,
        description=f"resolve branch {repository_name}:{branch}",
    )


def _validate_app_permissions(permissions: dict[str, str]) -> None:
    missing: list[str] = []
    for name, required in sorted(_REQUIRED_APP_PERMISSIONS.items()):
        actual = permissions.get(name, "")
        if _PERMISSION_RANK.get(actual, 0) < _PERMISSION_RANK[required]:
            missing.append(f"{name}={required} (actual {actual or 'absent'})")
    if missing:
        raise RuntimeError(
            "App installation token lacks required permissions: "
            + ", ".join(missing),
        )


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
        project = (
            owner_data.get("projectV2")
            if isinstance(owner_data, dict)
            else None
        )
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
            raise RuntimeError(
                f"Project {owner}/{number} field pagination is malformed",
            )
        if not page_info.get("hasNextPage"):
            break
        next_cursor = page_info.get("endCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            raise RuntimeError(
                f"Project {owner}/{number} fields are truncated",
            )
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
        names = {
            str(option.get("name", "")).strip().casefold()
            for option in options
            if isinstance(option, dict)
        }
        missing = sorted(_REQUIRED_STATUS_OPTIONS - names)
        if not missing:
            return
        raise RuntimeError(
            f"Project {owner}/{number} Status field lacks options: "
            + ", ".join(missing),
        )
    raise RuntimeError(
        f"Project {owner}/{number} has no usable Status single-select field",
    )


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
        help="App installation token with the complete required permission grant.",
    )
    args = parser.parse_args(argv)
    if not args.token:
        parser.error(
            "--token or REGISTRY_PREFLIGHT_GITHUB_TOKEN is required",
        )
    report = preflight_registry(
        args.registry,
        args.token,
        repo_filter=args.repo,
    )
    print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
