from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.common.desired_comments import DesiredComment, record_desired_comment
from scripts.common.metadata_reconciler import (
    build_reconcile_matrix,
    desired_labels_from_body,
    reconcile_repository,
    with_desired_labels,
)
from scripts.common.phase_artifact import ArtifactError


def test_desired_label_marker_round_trip_and_replacement() -> None:
    body = with_desired_labels("body", ["backport", "ai-resolved-conflicts"])
    replaced = with_desired_labels(body, ["backport"])

    assert desired_labels_from_body(replaced) == ("backport",)
    assert replaced.count("valkey-ci-agent:desired-labels:v1") == 1


def test_desired_label_marker_rejects_tampering() -> None:
    body = with_desired_labels("body", ["backport"])
    tampered = body.replace("payload=", "payload=A", 1)

    with pytest.raises(ArtifactError):
        desired_labels_from_body(tampered)


def test_reconciler_updates_only_bot_authored_allowlisted_items() -> None:
    bot_item = MagicMock(
        number=1,
        body=with_desired_labels("body", ["backport"]),
        user=SimpleNamespace(login="publisher[bot]"),
        labels=[],
        pull_request=None,
    )
    user_item = MagicMock(
        number=2,
        body=with_desired_labels("body", ["backport"]),
        user=SimpleNamespace(login="contributor"),
        labels=[],
        pull_request=None,
    )
    repo = MagicMock()
    repo.get_issues.return_value = [bot_item, user_item]
    repo.get_label.return_value = MagicMock()
    gh = MagicMock()
    gh.get_repo.return_value = repo

    result = reconcile_repository(
        gh,
        "org/repo",
        writer_login="publisher[bot]",
        allowed_labels={"backport"},
    )

    assert result.scanned == 2
    assert result.marked == 2
    assert result.updated == 1
    bot_item.add_to_labels.assert_called_once_with("backport")
    user_item.add_to_labels.assert_not_called()


def test_reconciler_creates_missing_label_and_converges_on_retry() -> None:
    item = MagicMock(
        number=1,
        body=with_desired_labels("body", ["backport"]),
        user=SimpleNamespace(login="publisher[bot]"),
        labels=[],
        pull_request=None,
    )
    repo = MagicMock()
    repo.get_issues.return_value = [item]
    repo.get_label.side_effect = GithubException(404, {"message": "not found"})
    gh = MagicMock()
    gh.get_repo.return_value = repo

    result = reconcile_repository(
        gh,
        "org/repo",
        writer_login="publisher[bot]",
        allowed_labels={"backport"},
    )

    assert result.updated == 1
    repo.create_label.assert_called_once()
    item.add_to_labels.assert_called_once_with("backport")


def test_reconciler_rejects_bot_marker_outside_label_policy() -> None:
    item = MagicMock(
        number=1,
        body=with_desired_labels("body", ["security-sensitive"]),
        user=SimpleNamespace(login="publisher[bot]"),
        labels=[],
        pull_request=None,
    )
    repo = MagicMock()
    repo.get_issues.return_value = [item]
    gh = MagicMock()
    gh.get_repo.return_value = repo

    with pytest.raises(RuntimeError, match="outside policy"):
        reconcile_repository(
            gh,
            "org/repo",
            writer_login="publisher[bot]",
            allowed_labels={"backport"},
        )


def test_reconciler_finishes_pending_comment_after_branch_update() -> None:
    desired = DesiredComment(
        key="ci-fix:123:7:pushed:abcdef",
        expected_head_sha="a" * 40,
        body="Validated fix published.",
        reaction_comment_id=55,
        reaction="+1",
    )
    comment = MagicMock(user=SimpleNamespace(login="publisher[bot]"))
    seed_issue = MagicMock(number=7)
    seed_issue.get_comments.return_value = []
    seed_issue.create_comment.return_value = comment
    record_desired_comment(
        seed_issue,
        desired,
        writer_login="publisher[bot]",
    )
    comment.body = seed_issue.create_comment.call_args.args[0]

    item = MagicMock(
        number=7,
        body="user PR body",
        user=SimpleNamespace(login="contributor"),
        labels=[],
        pull_request={"url": "https://api.github.com/repos/org/repo/pulls/7"},
    )
    item.get_comments.return_value = [comment]
    requester = MagicMock()
    repo = MagicMock(
        full_name="org/repo",
        _requester=requester,
    )
    repo.get_issues.return_value = [item]
    repo.get_pull.return_value = SimpleNamespace(
        head=SimpleNamespace(sha="a" * 40),
    )
    gh = MagicMock()
    gh.get_repo.return_value = repo

    result = reconcile_repository(
        gh,
        "org/repo",
        writer_login="publisher[bot]",
        allowed_labels={"backport"},
    )

    assert result.comments_reconciled == 1
    assert "Validated fix published." in comment.edit.call_args.args[0]
    requester.requestJsonAndCheck.assert_called_once()


def test_reconcile_matrix_is_repository_scoped_and_honors_disable_policy(
    tmp_path,
    monkeypatch,
) -> None:
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
schema_version: 2
repos:
  - repo: org/enabled
    project_owner: org
    language: c
    validation_waiver:
      reason: test
      approved_by: test
      expires: "2099-01-01"
    branches:
      - branch: "1.0"
        project_number: 1
  - repo: org/disabled
    project_owner: org
    language: c
    automation:
      enabled: false
    validation_waiver:
      reason: test
      approved_by: test
      expires: "2099-01-01"
    branches:
      - branch: "1.0"
        project_number: 2
""",
        encoding="utf-8",
    )
    monkeypatch.delenv("VALKEY_CI_AGENT_KILL_SWITCH", raising=False)
    monkeypatch.delenv("VALKEY_CI_AGENT_DISABLED_REPOSITORIES", raising=False)

    matrix = build_reconcile_matrix(str(registry))

    assert matrix == {
        "include": [
            {
                "repo": "org/enabled",
                "repo_name": "enabled",
                "registry_backed": True,
            },
            {
                "repo": "valkey-io/valkey-fuzzer",
                "repo_name": "valkey-fuzzer",
                "registry_backed": False,
            },
        ],
    }

    filtered = build_reconcile_matrix(
        str(registry),
        repositories=["org/enabled"],
    )
    assert [item["repo"] for item in filtered["include"]] == ["org/enabled"]

    with pytest.raises(ValueError, match="outside metadata policy"):
        build_reconcile_matrix(
            str(registry),
            repositories=["org/unknown"],
        )
