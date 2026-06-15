from __future__ import annotations

from scripts.backport.mark_done import (
    mark_backport_items_done,
    parse_backport_source_pr_numbers,
)


def test_parse_sweep_body_uses_only_applied_section() -> None:
    body = """# Backport sweep for 8.1

Automated cherry-picks from PRs marked "To be backported".

## Applied

| Source PR | Title | Detail |
|---|---|---|
| #101 | Good fix |  |
| #102 | Other fix | conflicts resolved |

## Needs attention

| Source PR | Title | Outcome | Reason |
|---|---|---|---|
| #999 | Bad fix | skipped-conflict | conflict |
"""

    assert parse_backport_source_pr_numbers(body) == [101, 102]


def test_parse_manual_body_source_pr_row() -> None:
    body = """## Backport Summary

| Field | Value |
|---|---|
| Source PR | [#123](https://github.com/valkey-io/valkey/pull/123) |
| Target branch | `8.1` |
"""

    assert parse_backport_source_pr_numbers(body) == [123]


def test_parse_head_ref_for_single_pr_backport() -> None:
    assert parse_backport_source_pr_numbers("", head_ref="backport/456-to-8.1") == [456]


def test_mark_backport_items_done_updates_matching_to_be_backported_items() -> None:
    gql = FakeGraphQLClient(
        project_items=[
            _project_item(101, "valkey-io/valkey", "item-101", "To be backported"),
            _project_item(102, "valkey-io/valkey", "item-102", "Done"),
            _project_item(103, "valkey-io/valkey", "item-103", "Needs review"),
            _project_item(104, "valkey-io/valkey-bloom", "item-104", "To be backported"),
        ]
    )

    result = mark_backport_items_done(
        gql,
        project_owner="valkey-io",
        project_number=14,
        source_repo="valkey-io/valkey",
        source_pr_numbers=[101, 102, 103, 104, 105],
    )

    assert result.updated == [101]
    assert result.already_done == [102]
    assert result.missing == [104, 105]
    assert result.skipped == {103: "Status is 'Needs review', not 'To be backported'"}
    assert gql.mutations == [
        {
            "projectId": "project-1",
            "itemId": "item-101",
            "fieldId": "status-field",
            "optionId": "done-option",
        }
    ]


class FakeGraphQLClient:
    def __init__(self, *, project_items: list[dict]) -> None:
        self._project_items = project_items
        self.mutations: list[dict] = []

    def execute(self, query: str, variables: dict) -> dict:
        if "updateProjectV2ItemFieldValue" in query:
            self.mutations.append(dict(variables))
            return {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": variables["itemId"]}}}

        return {
            "organization": {
                "projectV2": {
                    "id": "project-1",
                    "fields": {
                        "nodes": [
                            {
                                "__typename": "ProjectV2SingleSelectField",
                                "id": "status-field",
                                "name": "Status",
                                "options": [
                                    {"id": "todo-option", "name": "To be backported"},
                                    {"id": "done-option", "name": "Done"},
                                ],
                            }
                        ]
                    },
                    "items": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": self._project_items,
                    },
                }
            }
        }


def _project_item(number: int, repo: str, item_id: str, status: str) -> dict:
    return {
        "id": item_id,
        "content": {
            "__typename": "PullRequest",
            "number": number,
            "repository": {"nameWithOwner": repo},
        },
        "fieldValues": {
            "nodes": [
                {
                    "__typename": "ProjectV2ItemFieldSingleSelectValue",
                    "name": status,
                    "field": {"name": "Status"},
                }
            ]
        },
    }
