from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.backport.registry_preflight import preflight_registry


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
                "nodes": [
                    {
                        "__typename": "ProjectV2SingleSelectField",
                        "id": f"status-{number}",
                        "name": "Status",
                        "options": options,
                    }
                ],
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


def _registry(tmp_path: Path, *, with_validation: bool = True) -> str:
    validation = (
        """
    build_commands:
      - "make test"
    repair_validation_failures: true
"""
        if with_validation
        else """
    repair_validation_failures: true
"""
    )
    path = tmp_path / "repos.yml"
    path.write_text(
        f"""
repos:
  - repo: org/repo
    push_repo: fork/repo
    project_owner: org
    project_owner_type: organization
    language: c
{validation.rstrip()}
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


def test_preflight_resolves_every_live_registry_dependency(tmp_path: Path) -> None:
    gh = _Github()
    gql = _GraphQL()

    report = preflight_registry(
        _registry(tmp_path),
        "installation-token",
        github_client=gh,
        graphql_client=gql,
    )

    assert report.repositories == 2
    assert report.branches == 2
    assert report.projects == 2
    assert report.labels == 2
    assert report.validation_commands == 1
    assert gh.repositories["org/repo"].branches == ["1.0", "2.0"]
    assert gh.repositories["org/repo"].labels == ["backport", "ai-resolved"]
    assert [call["number"] for call in gql.calls] == [1, 1, 2, 2]


def test_preflight_rejects_incomplete_project_status_contract(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="lacks options: done"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            github_client=_Github(),
            graphql_client=_GraphQL(missing_option=True),
        )


def test_preflight_requires_commands_when_validation_repair_is_enabled(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="without any build_commands"):
        preflight_registry(
            _registry(tmp_path, with_validation=False),
            "installation-token",
            github_client=_Github(),
            graphql_client=_GraphQL(),
        )


def test_preflight_repo_filter_must_exist(tmp_path: Path) -> None:
    with pytest.raises(KeyError, match="missing/repo"):
        preflight_registry(
            _registry(tmp_path),
            "installation-token",
            repo_filter="missing/repo",
            github_client=_Github(),
            graphql_client=_GraphQL(),
        )


def test_workflow_mints_least_privilege_token_before_preflight() -> None:
    workflow = Path(".github/workflows/backport-registry-preflight.yml").read_text(encoding="utf-8")

    assert "actions/create-github-app-token@" in workflow
    assert "scripts.backport.registry_preflight" in workflow
    assert workflow.index("actions/create-github-app-token@") < workflow.index("scripts.backport.registry_preflight")
    assert "permission-contents: read" in workflow
    assert "permission-issues: read" in workflow
    assert "permission-organization-projects: read" in workflow
    assert "permission-metadata: read" in workflow
    assert "permission-contents: write" not in workflow
    assert "permission-issues: write" not in workflow
    assert "permission-organization-projects: write" not in workflow
    assert "permission-pull-requests:" not in workflow
    assert "permission-workflows:" not in workflow
    assert "30 8 * * *" in workflow

    for path in (
        ".github/workflows/backport-sweep.yml",
        ".github/workflows/backport-poll.yml",
        ".github/workflows/backport-mark-done-poll.yml",
    ):
        caller = Path(path).read_text(encoding="utf-8")
        assert "uses: ./.github/workflows/backport-registry-preflight.yml" in caller
        assert "needs: registry-preflight" in caller
