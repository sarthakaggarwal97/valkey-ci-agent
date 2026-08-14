"""Shared ProjectV2 querying and field decoding for backport workflows."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from scripts.backport.utils import normalize_project_value

ProjectQueryKind = Literal["discovery", "status"]


class GraphQLClient(Protocol):
    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        ...


@dataclass
class ProjectV2Data:
    id: str = ""
    fields: list[dict[str, Any]] = field(default_factory=list)
    items: list[dict[str, Any]] = field(default_factory=list)


_DISCOVERY_ITEM_FIELDS = """
          content {
            __typename
            ... on PullRequest {
              number title url merged mergedAt
              repository { nameWithOwner }
              mergeCommit { oid }
              commits(first: 100) { nodes { commit { oid } } }
            }
          }
          fieldValues(first: 50) {
            nodes {
              __typename
              ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
              ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2FieldCommon { name } } }
              ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
              ... on ProjectV2ItemFieldIterationValue { title field { ... on ProjectV2FieldCommon { name } } }
            }
          }"""

_STATUS_PROJECT_FIELDS = """
      id
      fields(first: 100) {
        nodes {
          __typename
          ... on ProjectV2SingleSelectField {
            id
            name
            options { id name }
          }
        }
      }"""

_STATUS_ITEM_FIELDS = """
          id
          content {
            __typename
            ... on PullRequest {
              number
              repository { nameWithOwner }
            }
          }
          fieldValues(first: 50) {
            nodes {
              __typename
              ... on ProjectV2ItemFieldSingleSelectValue {
                name
                field { ... on ProjectV2FieldCommon { name } }
              }
            }
          }"""


def project_owner_field(project_owner_type: str) -> str:
    return "user" if project_owner_type == "user" else "organization"


def build_project_query(owner_field: str, kind: ProjectQueryKind) -> str:
    """Build either legacy query from one pagination/query shell."""
    project_fields = _STATUS_PROJECT_FIELDS if kind == "status" else ""
    item_fields = _STATUS_ITEM_FIELDS if kind == "status" else _DISCOVERY_ITEM_FIELDS
    return f"""
query($owner: String!, $number: Int!, $cursor: String) {{
  {owner_field}(login: $owner) {{
    projectV2(number: $number) {{{project_fields}
      items(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{{item_fields}
        }}
      }}
    }}
  }}
}}
"""


def load_project_v2(
    gql: GraphQLClient,
    *,
    project_owner: str,
    project_number: int,
    project_owner_type: str,
    query: str,
    require_id: bool,
) -> ProjectV2Data:
    """Load every item page while preserving each caller's selected fields."""
    owner_field = project_owner_field(project_owner_type)
    cursor = None
    loaded = ProjectV2Data()
    while True:
        data = gql.execute(
            query,
            {
                "owner": project_owner,
                "number": project_number,
                "cursor": cursor,
            },
        )
        project = (data.get(owner_field) or {}).get("projectV2")
        if not project:
            raise RuntimeError(f"Project {project_owner}/{project_number} not found")

        loaded.id = loaded.id or str(project.get("id") or "")
        if not loaded.fields:
            loaded.fields = (project.get("fields") or {}).get("nodes") or []

        page = project.get("items") or {}
        loaded.items.extend(page.get("nodes") or [])
        page_info = page.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    if require_id and not loaded.id:
        raise RuntimeError(f"Project {project_owner}/{project_number} has no id")
    return loaded


def field_value_strings(field_value: dict[str, Any]) -> list[str]:
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


def extract_field_values(item: dict[str, Any]) -> dict[str, list[str]]:
    values: dict[str, list[str]] = defaultdict(list)
    for field_value in (item.get("fieldValues") or {}).get("nodes") or []:
        name = (field_value.get("field") or {}).get("name")
        if not name:
            continue
        values[normalize_project_value(name)].extend(field_value_strings(field_value))
    return dict(values)


def item_single_select_value(item: dict[str, Any], field_name: str) -> str:
    for field_value in (item.get("fieldValues") or {}).get("nodes") or []:
        if field_value.get("__typename") != "ProjectV2ItemFieldSingleSelectValue":
            continue
        name = (field_value.get("field") or {}).get("name")
        if normalize_project_value(name) == normalize_project_value(field_name):
            return str(field_value.get("name") or "")
    return ""
