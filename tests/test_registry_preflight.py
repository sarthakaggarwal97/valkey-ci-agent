from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.backport.registry_preflight import preflight_registry

_IMAGE = "gcc@sha256:" + "a" * 64
_PERMISSIONS = {
    "actions": "write",
    "contents": "write",
    "issues": "write",
    "members": "read",
    "metadata": "read",
    "organization_projects": "write",
    "pull_requests": "write",
    "workflows": "write",
}


class _Repository:
    archived = False
    disabled = False

    def __init__(self, name: str) -> None:
        self.full_name = name
        self.branches: list[str] = []
        self.labels: list[str] = []

    def get_branch(self, name: str) -> object:
        self.branches.append(name)
        return SimpleNamespace(name=name)

    def get_label(self, name: str) -> object:
        self.labels.append(name)
        return SimpleNamespace(name=name)


class _Github:
    def __init__(self) -> None:
        self.repositories: dict[str, _Repository] = {}

    def get_repo(self, name: str) -> _Repository:
        return self.repositories.setdefault(name, _Repository(name))


class _GraphQL:
    def __init__(self, *, missing_option: bool = False) -> None:
        self.calls: list[dict[str, object]] = []
        self.missing_option = missing_option

    def execute(
        self,
        _query: str,
        variables: dict[str, object],
    ) -> dict[str, object]:
        self.calls.append(dict(variables))
        cursor = variables["cursor"]
        number = variables["number"]
        if cursor is None:
            fields = {
                "nodes": [{"__typename": "ProjectV2Field", "name": "Title"}],
                "pageInfo": {"hasNextPage": True, "endCursor": f"page-{number}"},
            }
        else:
            options = [{"id": "todo", "name": "To be backported"}]
            if not self.missing_option:
                options.append({"id": "done", "name": "Done"})
            fields = {
                "nodes": [{
                    "__typename": "ProjectV2SingleSelectField",
                    "id": f"status-{number}",
                    "name": "Status",
                    "options": options,
                }],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        return {
            "organization": {
                "projectV2": {
                    "id": f"project-{number}",
                    "fields": fields,
                },
            },
        }


def _registry(tmp_path) -> str:
    path = tmp_path / "repos.yml"
    path.write_text(
        f"""
schema_version: 2
repos:
  - repo: org/repo
    push_repo: fork/repo
    project_owner: org
    project_owner_type: organization
    language: c
    validation:
      adapter: container-argv-v1
      image: "{_IMAGE}"
      platform: linux/amd64
      network: none
      resources:
        cpus: 2
        memory_mb: 1024
        pids: 64
        output_bytes: 65536
        tmpfs_mb: 64
      default_commands: [build]
      commands:
        - id: build
          argv: [make]
          working_directory: "."
          timeout_seconds: 60
          inputs: ["**"]
          expected_artifacts: []
      rules: []
    backport_label: backport
    llm_conflict_label: ai-resolved
    branches:
      - branch: "1.0"
        project_number: 1
      - branch: "2.0"
        project_number: 2
""",
        encoding="utf-8",
    )
    return str(path)


def test_preflight_resolves_every_live_registry_dependency(tmp_path) -> None:
    gh = _Github()
    gql = _GraphQL()
    images: list[str] = []

    report = preflight_registry(
        _registry(tmp_path),
        "installation-token",
        github_client=gh,
        graphql_client=gql,
        permission_loader=lambda _token: dict(_PERMISSIONS),
        image_resolver=images.append,
    )

    assert report.repositories == 2
    assert report.branches == 2
    assert report.projects == 2
    assert report.labels == 2
    assert report.images == 1
    assert report.permissions == _PERMISSIONS
    assert gh.repositories["org/repo"].branches == ["1.0", "2.0"]
    assert gh.repositories["org/repo"].labels == ["backport", "ai-resolved"]
    assert images == [_IMAGE]
    assert [call["number"] for call in gql.calls] == [1, 1, 2, 2]


def test_preflight_rejects_insufficient_app_permission(tmp_path) -> None:
    permissions = dict(_PERMISSIONS)
    permissions["contents"] = "read"
    with pytest.raises(RuntimeError, match="contents=write"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            github_client=_Github(),
            graphql_client=_GraphQL(),
            permission_loader=lambda _token: permissions,
            image_resolver=lambda _image: None,
        )


def test_preflight_rejects_incomplete_project_status_contract(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="lacks options: done"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            github_client=_Github(),
            graphql_client=_GraphQL(missing_option=True),
            permission_loader=lambda _token: dict(_PERMISSIONS),
            image_resolver=lambda _image: None,
        )


def test_preflight_fails_when_image_digest_does_not_resolve(tmp_path) -> None:
    def fail_image(image: str) -> None:
        raise RuntimeError(f"unresolvable {image}")

    with pytest.raises(RuntimeError, match="unresolvable"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            github_client=_Github(),
            graphql_client=_GraphQL(),
            permission_loader=lambda _token: dict(_PERMISSIONS),
            image_resolver=fail_image,
        )


def test_preflight_repo_filter_must_exist(tmp_path) -> None:
    with pytest.raises(KeyError, match="missing/repo"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            repo_filter="missing/repo",
            github_client=_Github(),
            graphql_client=_GraphQL(),
            permission_loader=lambda _token: dict(_PERMISSIONS),
            image_resolver=lambda _image: None,
        )
