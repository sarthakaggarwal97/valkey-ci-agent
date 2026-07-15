from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.backport import candidate_matrix
from scripts.backport.sweep_models import ProjectBackportCandidate
from scripts.common.phase_artifact import ArtifactError


def _registry(tmp_path) -> str:
    path = tmp_path / "repos.yml"
    path.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    project_owner_type: organization
    language: c
    validation_waiver:
      reason: Candidate discovery does not execute validation.
      approved_by: test suite
      expires: "2099-01-01"
    branches:
      - branch: release/1.0
        project_number: 7
""",
        encoding="utf-8",
    )
    return str(path)


def test_candidate_matrix_is_bounded_and_skips_duplicates(monkeypatch, tmp_path) -> None:
    candidates = [
        ProjectBackportCandidate(
            source_pr_number=number,
            source_pr_title=f"PR {number}",
            source_pr_url=f"https://github.com/org/repo/pull/{number}",
            target_branch="release/1.0",
            merge_commit_sha=f"{number:040x}",
            merged_at=f"2026-01-{number:02d}T00:00:00Z",
        )
        for number in (1, 2, 3, 4)
    ]

    class FakeDiscovery:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def discover(self, _branches):
            return {"release/1.0": candidates}

    class FakeCreator:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def check_duplicate(self, pr_number: int, _branch: str):
            return "existing" if pr_number == 2 else None

    monkeypatch.setattr(candidate_matrix, "ProjectBackportDiscovery", FakeDiscovery)
    monkeypatch.setattr(candidate_matrix, "BackportPRCreator", FakeCreator)
    monkeypatch.setattr(
        candidate_matrix,
        "_legacy_sweep_pr_numbers",
        lambda *_args: {1},
    )

    matrix = candidate_matrix.build_candidate_matrix(
        _registry(tmp_path),
        "read-token",
        max_candidates=2,
        github_client=SimpleNamespace(),
        graphql_client=SimpleNamespace(),
    )

    assert [entry["source_pr_number"] for entry in matrix["include"]] == [3, 4]
    assert matrix["include"][0]["artifact_suffix"] == "org-repo-release-1.0-pr-3"
    assert matrix["include"][0]["push_repo"] == ""


def test_candidate_matrix_enforces_operational_budget(tmp_path) -> None:
    with pytest.raises(ArtifactError, match="between 0 and 100"):
        candidate_matrix.build_candidate_matrix(
            _registry(tmp_path),
            "read-token",
            max_candidates=101,
            github_client=SimpleNamespace(),
            graphql_client=SimpleNamespace(),
        )


def test_zero_candidate_limit_preserves_unlimited_caller_semantics(
    monkeypatch,
    tmp_path,
) -> None:
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    language: c
    automation:
      max_queue_depth: 32
    validation_waiver:
      reason: Candidate discovery does not execute validation.
      approved_by: test suite
      expires: "2099-01-01"
    branches:
      - branch: release/1.0
        project_number: 7
""",
        encoding="utf-8",
    )
    candidates = [
        ProjectBackportCandidate(
            source_pr_number=number,
            source_pr_title=f"PR {number}",
            source_pr_url=f"https://github.com/org/repo/pull/{number}",
            target_branch="release/1.0",
            merge_commit_sha=f"{number:040x}",
            merged_at=f"2026-01-{number:02d}T00:00:00Z",
        )
        for number in range(1, 13)
    ]

    class FakeDiscovery:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def discover(self, _branches):
            return {"release/1.0": candidates}

    class FakeCreator:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def check_duplicate(self, _pr_number: int, _branch: str):
            return None

    monkeypatch.setattr(candidate_matrix, "ProjectBackportDiscovery", FakeDiscovery)
    monkeypatch.setattr(candidate_matrix, "BackportPRCreator", FakeCreator)
    monkeypatch.setattr(candidate_matrix, "_legacy_sweep_pr_numbers", lambda *_args: set())

    matrix = candidate_matrix.build_candidate_matrix(
        str(registry),
        "read-token",
        max_candidates=0,
        github_client=SimpleNamespace(),
        graphql_client=SimpleNamespace(),
    )

    assert [item["source_pr_number"] for item in matrix["include"]] == list(
        range(1, 13),
    )


def test_candidate_matrix_stops_at_repository_queue_depth(
    monkeypatch,
    tmp_path,
) -> None:
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    language: c
    automation:
      max_queue_depth: 1
    validation_waiver:
      reason: Candidate discovery does not execute validation.
      approved_by: test suite
      expires: "2099-01-01"
    branches:
      - branch: release/1.0
        project_number: 7
""",
        encoding="utf-8",
    )
    candidates = [
        ProjectBackportCandidate(
            source_pr_number=number,
            source_pr_title=f"PR {number}",
            source_pr_url=f"https://github.com/org/repo/pull/{number}",
            target_branch="release/1.0",
            merge_commit_sha=f"{number:040x}",
            merged_at=f"2026-01-{number:02d}T00:00:00Z",
        )
        for number in (1, 2, 3)
    ]

    class FakeDiscovery:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def discover(self, _branches):
            return {"release/1.0": candidates}

    class FakeCreator:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def check_duplicate(self, _pr_number: int, _branch: str):
            return None

    monkeypatch.setattr(candidate_matrix, "ProjectBackportDiscovery", FakeDiscovery)
    monkeypatch.setattr(candidate_matrix, "BackportPRCreator", FakeCreator)
    monkeypatch.setattr(candidate_matrix, "_legacy_sweep_pr_numbers", lambda *_args: set())

    matrix = candidate_matrix.build_candidate_matrix(
        str(registry),
        "read-token",
        max_candidates=10,
        github_client=SimpleNamespace(),
        graphql_client=SimpleNamespace(),
    )

    assert [item["source_pr_number"] for item in matrix["include"]] == [1]


def test_success_cap_does_not_cap_discovery_attempts(
    monkeypatch,
    tmp_path,
) -> None:
    candidates = [
        ProjectBackportCandidate(
            source_pr_number=number,
            source_pr_title=f"PR {number}",
            source_pr_url=f"https://github.com/org/repo/pull/{number}",
            target_branch="release/1.0",
            merge_commit_sha=f"{number:040x}",
        )
        for number in (1, 2, 3)
    ]

    class FakeDiscovery:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def discover(self, _branches):
            return {"release/1.0": candidates}

    class FakeCreator:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def check_duplicate(self, _pr_number: int, _branch: str):
            return None

    monkeypatch.setattr(candidate_matrix, "ProjectBackportDiscovery", FakeDiscovery)
    monkeypatch.setattr(candidate_matrix, "BackportPRCreator", FakeCreator)
    monkeypatch.setattr(candidate_matrix, "_legacy_sweep_pr_numbers", lambda *_args: set())

    matrix = candidate_matrix.build_candidate_matrix(
        _registry(tmp_path),
        "read-token",
        max_candidates=1,
        github_client=SimpleNamespace(),
        graphql_client=SimpleNamespace(),
    )

    assert [item["source_pr_number"] for item in matrix["include"]] == [1, 2, 3]
