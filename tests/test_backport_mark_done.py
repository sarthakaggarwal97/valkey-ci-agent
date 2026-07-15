from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import scripts.backport.mark_done as mark_done
from scripts.backport.mark_done import (
    BackportStatusUpdateResult,
    mark_backport_items_done,
    reconcile_project_board,
)
from scripts.backport.provenance import (
    build_provenance,
    provenance_commit_message,
)
from scripts.common.polling import PollLoopError


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
        verified_pr_numbers={101, 102, 103, 104, 105},
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


def test_mark_backport_items_done_gates_on_verified_set() -> None:
    gql = FakeGraphQLClient(
        project_items=[
            _project_item(101, "valkey-io/valkey", "item-101", "To be backported"),
            _project_item(102, "valkey-io/valkey", "item-102", "To be backported"),
        ]
    )

    result = mark_backport_items_done(
        gql,
        project_owner="valkey-io",
        project_number=14,
        source_repo="valkey-io/valkey",
        source_pr_numbers=[101, 102],
        verified_pr_numbers={101},
    )

    assert result.updated == [101]
    assert result.unverified == [102]
    assert [m["itemId"] for m in gql.mutations] == ["item-101"]


def test_reconcile_marks_only_branch_present_items(monkeypatch) -> None:
    gql = FakeGraphQLClient(
        project_items=[
            _project_item(201, "valkey-io/valkey", "item-201", "To be backported"),
            _project_item(202, "valkey-io/valkey", "item-202", "To be backported"),
            _project_item(203, "valkey-io/valkey", "item-203", "Done"),
            _project_item(204, "valkey-io/valkey-bloom", "item-204", "To be backported"),
        ]
    )

    captured: dict = {}

    def fake_verify(
        repo, branch, pr_numbers, *, token="", git_env=None, push_repo=None,
    ):
        captured["repo"] = repo
        captured["branch"] = branch
        captured["pr_numbers"] = set(pr_numbers)
        return {201}  # only 201 actually landed on the branch

    monkeypatch.setattr(mark_done, "verify_prs_on_branch", fake_verify)

    result = reconcile_project_board(
        gql,
        project_owner="valkey-io",
        project_number=14,
        source_repo="valkey-io/valkey",
        target_branch="9.1",
    )

    # Only valkey-io/valkey items still "To be backported" are candidates.
    assert captured["pr_numbers"] == {201, 202}
    assert captured["repo"] == "valkey-io/valkey"
    assert captured["branch"] == "9.1"
    assert result.updated == [201]
    assert result.unverified == [202]
    assert [m["itemId"] for m in gql.mutations] == ["item-201"]


def test_reconcile_no_candidates_is_noop(monkeypatch) -> None:
    gql = FakeGraphQLClient(
        project_items=[_project_item(301, "valkey-io/valkey", "item-301", "Done")]
    )
    monkeypatch.setattr(
        mark_done, "verify_prs_on_branch",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not verify")),
    )

    result = reconcile_project_board(
        gql,
        project_owner="valkey-io",
        project_number=14,
        source_repo="valkey-io/valkey",
        target_branch="9.1",
    )

    assert result == BackportStatusUpdateResult(requested=[])
    assert gql.mutations == []


def test_pr_numbers_from_subjects_ignores_body_only_mentions() -> None:
    from scripts.backport.utils import pr_numbers_from_commit_subjects

    # Each element is a commit *subject*. A (#N) here means that commit is PR N.
    subjects = [
        "Fix a thing (#3801)",
        "Unrelated work without a ref",
        "Another fix (#3920)",
    ]
    assert pr_numbers_from_commit_subjects(subjects) == {3801, 3920}


def test_pr_numbers_from_subjects_uses_trailing_pr_only() -> None:
    from scripts.backport.utils import pr_numbers_from_commit_subjects

    # A revert names the reverted PR mid-subject; only the trailing (#N) is the
    # commit's own PR. Must be 3756 (the revert), not 3544 (what it reverts).
    subjects = ['Revert "IO-Threads redesign cleanup work (#3544)" (#3756)']
    assert pr_numbers_from_commit_subjects(subjects) == {3756}


def _commit(sha: str, *, tree: str, message: str, parents=()):
    return SimpleNamespace(
        sha=sha,
        parents=[SimpleNamespace(sha=value) for value in parents],
        commit=SimpleNamespace(
            message=message,
            tree=SimpleNamespace(sha=tree),
        ),
    )


def _provenance_fixture(*, source_commits=None, comparison_status="ahead"):
    base = "a" * 40
    target = "b" * 40
    attestation = "c" * 40
    tree = "d" * 40
    source = "e" * 40
    source_merge = "f" * 40
    merge = "1" * 40
    source_commits = source_commits or [source]
    provenance = build_provenance(
        repository="valkey-io/valkey",
        target_branch="9.1",
        source_pr_number=3801,
        source_merge_commit=source_merge,
        source_commits=tuple(source_commits),
        base_commit=base,
        target_commit=target,
        patch_sha256="2" * 64,
        patch_id="3" * 40,
        validated_tree=tree,
        prepared_manifest_sha256="4" * 64,
        validated_manifest_sha256="5" * 64,
    )
    target_commit = _commit(target, tree=tree, message="target", parents=(base,))
    attestation_commit = _commit(
        attestation,
        tree=tree,
        message=provenance_commit_message(provenance),
        parents=(target,),
    )
    pull = SimpleNamespace(
        number=9001,
        head=SimpleNamespace(
            ref="agent/backport/3801-to-9.1",
            sha=attestation,
            repo=SimpleNamespace(full_name="valkey-io/valkey"),
        ),
        user=SimpleNamespace(login="publisher[bot]"),
        merged_at="2026-01-01",
        merge_commit_sha=merge,
        get_commits=lambda: [target_commit, attestation_commit],
    )
    source_pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=source_merge,
        get_commits=lambda: [
            _commit(value, tree=tree, message="source")
            for value in source_commits
        ],
    )
    repo = MagicMock()
    repo.get_pulls.return_value = [pull]
    repo.get_pull.return_value = source_pr
    repo.compare.return_value = SimpleNamespace(status=comparison_status)
    github = MagicMock()
    github.get_repo.return_value = repo
    return github, repo, pull


def test_verify_requires_immutable_provenance_and_target_ancestry() -> None:
    github, repo, _pull = _provenance_fixture()
    present = mark_done.verify_prs_on_branch(
        "valkey-io/valkey",
        "9.1",
        {3801, 3920},
        github_client=github,
        publisher_login="publisher[bot]",
    )
    assert present == {3801}
    repo.compare.assert_called_once_with("1" * 40, "9.1")


def test_verify_rejects_source_drift_and_non_ancestor_merge() -> None:
    github, repo, pull = _provenance_fixture(comparison_status="diverged")
    assert mark_done.verify_prs_on_branch(
        "valkey-io/valkey",
        "9.1",
        {3801},
        github_client=github,
        publisher_login="publisher[bot]",
    ) == set()

    github, repo, pull = _provenance_fixture()
    repo.get_pull.return_value.get_commits = lambda: [
        _commit("9" * 40, tree="d" * 40, message="changed source"),
    ]
    assert mark_done.verify_prs_on_branch(
        "valkey-io/valkey",
        "9.1",
        {3801},
        github_client=github,
        publisher_login="publisher[bot]",
    ) == set()


def test_verify_rejects_mutable_applied_table_without_provenance() -> None:
    github, _repo, pull = _provenance_fixture()
    pull.get_commits = lambda: [
        _commit("b" * 40, tree="d" * 40, message="target", parents=("a" * 40,)),
        _commit(
            "c" * 40,
            tree="d" * 40,
            message="## Applied\n\n| Source PR |\n|---|\n| #3801 |",
            parents=("b" * 40,),
        ),
    ]
    assert mark_done.verify_prs_on_branch(
        "valkey-io/valkey",
        "9.1",
        {3801},
        github_client=github,
        publisher_login="publisher[bot]",
    ) == set()


def test_dry_run_reports_without_mutating() -> None:
    gql = FakeGraphQLClient(
        project_items=[
            _project_item(101, "valkey-io/valkey", "item-101", "To be backported"),
        ]
    )

    result = mark_backport_items_done(
        gql,
        project_owner="valkey-io",
        project_number=14,
        source_repo="valkey-io/valkey",
        source_pr_numbers=[101],
        verified_pr_numbers={101},
        dry_run=True,
    )

    assert result.updated == [101]
    assert gql.mutations == []


def test_load_project_paginates_fields_and_each_items_field_values() -> None:
    calls: list[dict] = []

    class PaginatedGraphQL:
        def execute(self, query: str, variables: dict) -> dict:
            calls.append(dict(variables))
            if "query($id: ID!, $cursor: String)" in query and "... on ProjectV2Item {" in query:
                return {
                    "node": {
                        "fieldValues": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{
                                "__typename": "ProjectV2ItemFieldSingleSelectValue",
                                "name": "To be backported",
                                "field": {"name": "Status"},
                            }],
                        }
                    }
                }
            if "query($id: ID!, $cursor: String)" in query:
                return {
                    "node": {
                        "fields": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{
                                "__typename": "ProjectV2SingleSelectField",
                                "id": "status-field",
                                "name": "Status",
                                "options": [{"id": "done", "name": "Done"}],
                            }],
                        }
                    }
                }
            return {
                "organization": {
                    "projectV2": {
                        "id": "project",
                        "fields": {
                            "pageInfo": {"hasNextPage": True, "endCursor": "fields-1"},
                            "nodes": [],
                        },
                        "items": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{
                                "id": "item-1",
                                "content": {
                                    "__typename": "PullRequest",
                                    "number": 42,
                                    "repository": {"nameWithOwner": "valkey-io/valkey"},
                                },
                                "fieldValues": {
                                    "pageInfo": {
                                        "hasNextPage": True,
                                        "endCursor": "values-1",
                                    },
                                    "nodes": [],
                                },
                            }],
                        },
                    }
                }
            }

    project = mark_done._load_project(
        PaginatedGraphQL(),
        project_owner="valkey-io",
        project_number=14,
        project_owner_type="organization",
    )
    assert project["fields"][0]["name"] == "Status"
    assert mark_done._item_single_select_value(
        project["items"][0],
        "Status",
    ) == "To be backported"
    assert {"id": "project", "cursor": "fields-1"} in calls
    assert {"id": "item-1", "cursor": "values-1"} in calls


def test_main_surfaces_sustained_poll_failure(monkeypatch, capsys) -> None:
    class FakeRegistry:
        pass

    monkeypatch.setattr(
        "scripts.backport.registry.load_registry",
        lambda _path: FakeRegistry(),
    )
    monkeypatch.setattr(mark_done, "GitHubGraphQLClient", lambda _token: object())

    def fail_after_success(*_args, **_kwargs):
        raise PollLoopError(
            results=[{"repo": "valkey-io/valkey", "action": "checked"}],
            last_error=RuntimeError("transient graphql failure"),
        )

    monkeypatch.setattr(mark_done, "run_poll_loop_from_args", fail_after_success)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mark_done",
            "--repo", "valkey-io/valkey",
            "--target-branch", "9.1",
            "--target-token", "tok",
        ],
    )

    with pytest.raises(SystemExit) as raised:
        mark_done.main()

    assert raised.value.code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["runs"] == [
        {"repo": "valkey-io/valkey", "action": "checked"},
        {
            "repo": "valkey-io/valkey",
            "target_branch": "9.1",
            "action": "error",
            "error": "transient graphql failure",
        },
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


def _project_item(
    number: int, repo: str, item_id: str, status: str
) -> dict:
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
