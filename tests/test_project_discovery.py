from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.backport.project_discovery import (
    ProjectBackportDiscovery,
    _project_items_query,
)


def _project_item(
    *,
    number: int,
    repo: str,
    status: str = "To be backported",
) -> dict:
    return {
        "id": f"ITEM_{number}",
        "content": {
            "__typename": "PullRequest",
            "number": number,
            "title": f"PR {number}",
            "url": f"https://github.com/{repo}/pull/{number}",
            "merged": True,
            "mergedAt": "2026-01-01T00:00:00Z",
            "repository": {"nameWithOwner": repo},
            "mergeCommit": {"oid": f"{number:040x}"},
        },
        "fieldValues": {
            "nodes": [
                {
                    "__typename": "ProjectV2ItemFieldSingleSelectValue",
                    "name": status,
                    "field": {"name": "Status"},
                },
            ],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        },
    }


def _discovery(items: list[dict]) -> ProjectBackportDiscovery:
    discovery = ProjectBackportDiscovery(
        MagicMock(),
        project_owner="valkey-io",
        project_number=1,
        source_repo="valkey-io/valkey",
        implicit_target_branch="9.1",
    )
    discovery._iter_items = lambda: items  # type: ignore[method-assign]
    return discovery


def test_discovery_filters_repository_merge_and_status() -> None:
    other_repo = _project_item(number=2, repo="valkey-io/docs")
    unmerged = _project_item(number=3, repo="valkey-io/valkey")
    unmerged["content"]["merged"] = False
    done = _project_item(
        number=4,
        repo="valkey-io/valkey",
        status="Done",
    )
    result = _discovery(
        [
            _project_item(number=1, repo="valkey-io/valkey"),
            other_repo,
            unmerged,
            done,
        ]
    ).discover(["9.1"])

    assert [item.source_pr_number for item in result["9.1"]] == [1]
    assert result["9.1"][0].commit_shas == []


def test_discovery_paginates_items_and_nested_field_values() -> None:
    item = _project_item(number=10, repo="valkey-io/valkey", status="Ignored")
    item["fieldValues"] = {
        "nodes": [],
        "pageInfo": {"hasNextPage": True, "endCursor": "fields-1"},
    }
    gql = MagicMock()
    gql.execute.side_effect = [
        {
            "organization": {
                "projectV2": {
                    "items": {
                        "nodes": [item],
                        "pageInfo": {
                            "hasNextPage": True,
                            "endCursor": "items-1",
                        },
                    }
                }
            }
        },
        {
            "node": {
                "fieldValues": {
                    "nodes": [
                        {
                            "__typename": "ProjectV2ItemFieldSingleSelectValue",
                            "name": "To be backported",
                            "field": {"name": "Status"},
                        }
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                }
            }
        },
        {
            "organization": {
                "projectV2": {
                    "items": {
                        "nodes": [],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": None,
                        },
                    }
                }
            }
        },
    ]
    discovery = ProjectBackportDiscovery(
        gql,
        project_owner="valkey-io",
        project_number=1,
        source_repo="valkey-io/valkey",
        implicit_target_branch="9.1",
    )

    result = discovery.discover(["9.1"])

    assert [candidate.source_pr_number for candidate in result["9.1"]] == [10]
    assert gql.execute.call_count == 3


def test_discovery_fails_closed_when_pagination_cursor_is_missing() -> None:
    gql = MagicMock()
    gql.execute.return_value = {
        "organization": {
            "projectV2": {
                "items": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": True, "endCursor": None},
                }
            }
        }
    }
    discovery = ProjectBackportDiscovery(
        gql,
        project_owner="valkey-io",
        project_number=1,
        source_repo="valkey-io/valkey",
    )

    with pytest.raises(RuntimeError, match="omitted endCursor"):
        discovery.discover(["9.1"])


def test_project_query_requests_repository_and_pagination() -> None:
    query = _project_items_query("organization")
    assert "repository { nameWithOwner }" in query
    assert "pageInfo { hasNextPage endCursor }" in query
