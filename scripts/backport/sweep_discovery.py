"""Complete GitHub Project v2 discovery for scheduled backport candidates."""

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
        query = project_items_query(owner_field)
        cursor: str | None = None
        seen_cursors: set[str] = set()
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
            nodes = page.get("nodes")
            if not isinstance(nodes, list):
                raise RuntimeError(
                    f"Project {self._owner}/{self._number} returned invalid items"
                )
            items.extend(nodes)
            cursor = _next_page_cursor(
                page,
                f"project {self._owner}/{self._number} items",
            )
            if cursor is None:
                return items
            if cursor in seen_cursors:
                raise RuntimeError(
                    f"project {self._owner}/{self._number} items repeated "
                    f"pagination cursor {cursor!r}"
                )
            seen_cursors.add(cursor)

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
                "Skipping project item PR #%s from %s (sweep target is %s)",
                content.get("number"),
                item_repo,
                self._source_repo,
            )
            return None

        self._complete_connection(
            item,
            connection_name="fieldValues",
            owner_id=item.get("id"),
            query=_project_item_field_values_query(),
            label=f"project item for PR #{content.get('number')}",
        )
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
            if not matched_branch:
                return None
            target_branch = matched_branch

        self._complete_connection(
            content,
            connection_name="commits",
            owner_id=content.get("id"),
            query=_pull_request_commits_query(),
            label=f"PR #{content.get('number')} commits",
        )
        commits = [
            node.get("commit", {}).get("oid", "")
            for node in (content.get("commits", {}).get("nodes") or [])
        ]
        commits_page = content.get("commits") or {}
        merge_sha = (content.get("mergeCommit") or {}).get("oid")
        return ProjectBackportCandidate(
            source_pr_number=int(content["number"]),
            source_pr_title=str(content.get("title") or ""),
            source_pr_url=str(content.get("url") or ""),
            target_branch=target_branch,
            merge_commit_sha=merge_sha,
            commit_shas=[sha for sha in commits if sha],
            merged_at=str(content.get("mergedAt") or ""),
            source_commits_complete=not bool(
                (commits_page.get("pageInfo") or {}).get("hasNextPage")
            ),
        )

    def _complete_connection(
        self,
        owner: dict[str, Any],
        *,
        connection_name: str,
        owner_id: object,
        query: str,
        label: str,
    ) -> None:
        connection = owner.get(connection_name)
        if not isinstance(connection, dict):
            raise RuntimeError(f"{label} returned no {connection_name} connection")
        nodes = connection.get("nodes")
        if not isinstance(nodes, list):
            raise RuntimeError(f"{label} returned invalid {connection_name} nodes")

        seen_cursors: set[str] = set()
        while True:
            cursor = _next_page_cursor(connection, label)
            if cursor is None:
                return
            if not isinstance(owner_id, str) or not owner_id:
                raise RuntimeError(
                    f"{label} requires pagination but its GraphQL node ID is missing"
                )
            if cursor in seen_cursors:
                raise RuntimeError(
                    f"{label} repeated pagination cursor {cursor!r}"
                )
            seen_cursors.add(cursor)

            data = self._gql.execute(
                query,
                {"id": owner_id, "cursor": cursor},
            )
            node = data.get("node")
            if not isinstance(node, dict) or node.get("id") != owner_id:
                raise RuntimeError(
                    f"{label} pagination returned the wrong GraphQL node"
                )
            next_page = node.get(connection_name)
            if not isinstance(next_page, dict):
                raise RuntimeError(
                    f"{label} pagination returned no {connection_name} connection"
                )
            next_nodes = next_page.get("nodes")
            if not isinstance(next_nodes, list):
                raise RuntimeError(
                    f"{label} pagination returned invalid {connection_name} nodes"
                )
            nodes.extend(next_nodes)
            connection["pageInfo"] = next_page.get("pageInfo")


def project_items_query(owner_field: str) -> str:
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
              id
              number title url merged mergedAt
              repository {{ nameWithOwner }}
              mergeCommit {{ oid }}
              commits(first: 100) {{
                pageInfo {{ hasNextPage endCursor }}
                nodes {{ commit {{ oid }} }}
              }}
            }}
          }}
          fieldValues(first: 50) {{
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


def _pull_request_commits_query() -> str:
    return """
query($id: ID!, $cursor: String!) {
  node(id: $id) {
    ... on PullRequest {
      id
      commits(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { commit { oid } }
      }
    }
  }
}
"""


def _project_item_field_values_query() -> str:
    return """
query($id: ID!, $cursor: String!) {
  node(id: $id) {
    ... on ProjectV2Item {
      id
      fieldValues(first: 50, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on ProjectV2ItemFieldTextValue {
            text
            field { ... on ProjectV2FieldCommon { name } }
          }
          ... on ProjectV2ItemFieldSingleSelectValue {
            name
            field { ... on ProjectV2FieldCommon { name } }
          }
          ... on ProjectV2ItemFieldNumberValue {
            number
            field { ... on ProjectV2FieldCommon { name } }
          }
          ... on ProjectV2ItemFieldIterationValue {
            title
            field { ... on ProjectV2FieldCommon { name } }
          }
        }
      }
    }
  }
}
"""


def _next_page_cursor(connection: dict[str, Any], label: str) -> str | None:
    page_info = connection.get("pageInfo")
    if not isinstance(page_info, dict):
        raise RuntimeError(f"{label} returned no pagination metadata")
    has_next_page = page_info.get("hasNextPage")
    if has_next_page is False:
        return None
    if has_next_page is not True:
        raise RuntimeError(f"{label} returned invalid hasNextPage metadata")
    cursor = page_info.get("endCursor")
    if not isinstance(cursor, str) or not cursor:
        raise RuntimeError(
            f"{label} reports another page but returned no endCursor"
        )
    return cursor


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


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
