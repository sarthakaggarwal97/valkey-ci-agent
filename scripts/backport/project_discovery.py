"""Read-only discovery of backport candidates from a GitHub Project."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_models import ProjectBackportCandidate

logger = logging.getLogger(__name__)

DEFAULT_BRANCH_FIELDS = (
    "Backport Branch",
    "Target Branch",
    "Release Branch",
    "Branch",
    "Version",
    "Release",
    "Folder",
)
DEFAULT_STATUS_FIELD = "Status"
DEFAULT_STATUS_VALUE = "To be backported"


class ProjectBackportDiscovery:
    def __init__(
        self,
        gql: GitHubGraphQLClient,
        *,
        project_owner: str,
        project_number: int,
        source_repo: str,
        project_owner_type: str = "organization",
        status_field: str = DEFAULT_STATUS_FIELD,
        status_value: str = DEFAULT_STATUS_VALUE,
        branch_fields: list[str] | None = None,
        implicit_target_branch: str | None = None,
    ) -> None:
        self._gql = gql
        self._owner = project_owner
        self._number = project_number
        self._owner_type = project_owner_type
        self._source_repo = source_repo
        self._status_field = status_field
        self._status_value = status_value
        self._branch_fields = branch_fields or list(DEFAULT_BRANCH_FIELDS)
        self._implicit_target = implicit_target_branch

    def discover(
        self,
        release_branches: list[str],
    ) -> dict[str, list[ProjectBackportCandidate]]:
        by_branch: dict[str, list[ProjectBackportCandidate]] = {
            branch: [] for branch in release_branches
        }
        for item in self._iter_items():
            candidate = self._candidate_from_item(item, release_branches)
            if candidate:
                by_branch.setdefault(candidate.target_branch, []).append(candidate)
        return by_branch

    def _iter_items(self) -> list[dict[str, Any]]:
        owner_field = "user" if self._owner_type == "user" else "organization"
        query = _project_items_query(owner_field)
        cursor = None
        items: list[dict[str, Any]] = []
        while True:
            data = self._gql.execute(
                query,
                {"owner": self._owner, "number": self._number, "cursor": cursor},
            )
            project = (data.get(owner_field) or {}).get("projectV2")
            if not project:
                raise RuntimeError(f"Project {self._owner}/{self._number} not found")
            page = project.get("items") or {}
            items.extend(
                self._complete_field_values(item)
                for item in (page.get("nodes") or [])
            )
            page_info = page.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return items
            cursor = page_info.get("endCursor")
            if not cursor:
                raise RuntimeError("project items page omitted endCursor")

    def _complete_field_values(self, item: dict[str, Any]) -> dict[str, Any]:
        values = item.get("fieldValues") or {}
        page_info = values.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return item
        item_id = item.get("id")
        cursor = page_info.get("endCursor")
        if not isinstance(item_id, str) or not item_id or not cursor:
            raise RuntimeError(
                "project item field values are truncated without a cursor"
            )
        nodes = list(values.get("nodes") or [])
        while True:
            data = self._gql.execute(
                _project_item_field_values_query(),
                {"id": item_id, "cursor": cursor},
            )
            node = data.get("node") or {}
            page = node.get("fieldValues") or {}
            nodes.extend(page.get("nodes") or [])
            page_info = page.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                raise RuntimeError(
                    "project item field-values page omitted endCursor"
                )
        item["fieldValues"] = {"nodes": nodes}
        return item

    def _candidate_from_item(
        self,
        item: dict[str, Any],
        branches: list[str],
    ) -> ProjectBackportCandidate | None:
        content = item.get("content") or {}
        if content.get("__typename") != "PullRequest" or not content.get("merged"):
            return None

        item_repo = (content.get("repository") or {}).get("nameWithOwner")
        if item_repo and item_repo != self._source_repo:
            logger.debug(
                "Skipping project item PR #%s from %s (target is %s)",
                content.get("number"),
                item_repo,
                self._source_repo,
            )
            return None

        fields = _extract_field_values(item)
        if not _field_has_value(fields, self._status_field, self._status_value):
            return None

        if self._implicit_target is not None:
            target_branch = self._implicit_target
        else:
            matched_branch = _matching_release_branch(
                fields,
                self._branch_fields,
                branches,
            )
            if matched_branch is None:
                return None
            target_branch = matched_branch

        merge_sha = (content.get("mergeCommit") or {}).get("oid")
        return ProjectBackportCandidate(
            source_pr_number=int(content["number"]),
            source_pr_title=str(content.get("title") or ""),
            source_pr_url=str(content.get("url") or ""),
            target_branch=target_branch,
            merge_commit_sha=merge_sha,
            commit_shas=[],
            merged_at=str(content.get("mergedAt") or ""),
        )


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _project_items_query(owner_field: str) -> str:
    return f"""
query($owner: String!, $number: Int!, $cursor: String) {{
  {owner_field}(login: $owner) {{
    projectV2(number: $number) {{
      items(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          id
          content {{
            __typename
            ... on PullRequest {{
              number title url merged mergedAt
              repository {{ nameWithOwner }}
              mergeCommit {{ oid }}
            }}
          }}
          fieldValues(first: 100) {{
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
              __typename
              ... on ProjectV2ItemFieldTextValue {{ text field {{ ... on ProjectV2FieldCommon {{ name }} }} }}
              ... on ProjectV2ItemFieldSingleSelectValue {{ name field {{ ... on ProjectV2FieldCommon {{ name }} }} }}
              ... on ProjectV2ItemFieldNumberValue {{ number field {{ ... on ProjectV2FieldCommon {{ name }} }} }}
              ... on ProjectV2ItemFieldIterationValue {{ title field {{ ... on ProjectV2FieldCommon {{ name }} }} }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""


def _project_item_field_values_query() -> str:
    return """
query($id: ID!, $cursor: String) {
  node(id: $id) {
    ... on ProjectV2Item {
      fieldValues(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
          ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2FieldCommon { name } } }
          ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
          ... on ProjectV2ItemFieldIterationValue { title field { ... on ProjectV2FieldCommon { name } } }
        }
      }
    }
  }
}
"""


def _extract_field_values(item: dict[str, Any]) -> dict[str, list[str]]:
    values: dict[str, list[str]] = defaultdict(list)
    for field_value in (item.get("fieldValues") or {}).get("nodes") or []:
        name = (field_value.get("field") or {}).get("name")
        if not name:
            continue
        values[_normalize(name)].extend(_field_value_strings(field_value))
    return dict(values)


def _field_value_strings(field_value: dict[str, Any]) -> list[str]:
    type_name = field_value.get("__typename")
    if type_name == "ProjectV2ItemFieldTextValue":
        return [str(field_value.get("text") or "")]
    if type_name == "ProjectV2ItemFieldSingleSelectValue":
        return [str(field_value.get("name") or "")]
    if type_name == "ProjectV2ItemFieldNumberValue":
        number = field_value.get("number")
        return [] if number is None else [str(number)]
    if type_name == "ProjectV2ItemFieldIterationValue":
        return [str(field_value.get("title") or "")]
    return []


def _field_has_value(
    fields: dict[str, list[str]],
    field_name: str,
    expected: str,
) -> bool:
    return any(
        _normalize(value) == _normalize(expected)
        for value in fields.get(_normalize(field_name), [])
    )


def _matching_release_branch(
    fields: dict[str, list[str]],
    branch_fields: list[str],
    branches: list[str],
) -> str | None:
    for field_name in branch_fields:
        values = fields.get(_normalize(field_name), [])
        for branch in branches:
            normalized_branch = _normalize(branch)
            if any(
                _normalize(value) == normalized_branch
                or _normalize(value) == f"backport {normalized_branch}"
                for value in values
            ):
                return branch
    return None
