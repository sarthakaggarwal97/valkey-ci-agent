from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.backport import aggregate, phased
from scripts.backport.models import ResolutionResult
from scripts.backport.sweep_models import (
    DETAIL_RESOLVED_BY_AI,
    BranchSweepResult,
    CandidateResult,
)
from scripts.common.operational_controls import (
    OperationalPolicy,
    operational_policy_to_dict,
)
from scripts.common.phase_artifact import load_prepared, load_validated, write_json
from scripts.common.proc import git_output, run_git
from scripts.common.publication_manifest import load_publication_manifest
from scripts.common.validation_adapter import (
    ValidationRunResult,
    command_plan_payload,
)

_IMAGE = "gcc@sha256:" + "a" * 64


def _validation_policy() -> dict[str, Any]:
    return {
        "adapter": "container-argv-v1",
        "image": _IMAGE,
        "platform": "linux/amd64",
        "network": "none",
        "resources": {
            "cpus": 1,
            "memory_mb": 512,
            "pids": 64,
            "output_bytes": 65536,
            "tmpfs_mb": 64,
        },
        "default_commands": ["verify"],
        "commands": [
            {
                "id": "verify",
                "argv": ["test", "-f", "base.txt"],
                "working_directory": ".",
                "timeout_seconds": 60,
                "inputs": ["**"],
                "expected_artifacts": [],
            },
        ],
        "rules": [],
    }


def _registry(tmp_path: Path) -> Path:
    path = tmp_path / "repos.yml"
    path.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    language: c
    validation:
      adapter: container-argv-v1
      image: "gcc@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      platform: linux/amd64
      network: none
      resources:
        cpus: 1
        memory_mb: 512
        pids: 64
        output_bytes: 65536
        tmpfs_mb: 64
      default_commands: [verify]
      commands:
        - id: verify
          argv: ["test", "-f", "base.txt"]
          working_directory: "."
          timeout_seconds: 60
          inputs: ["**"]
          expected_artifacts: []
      rules: []
    repair_validation_failures: true
    branches:
      - branch: release/1.0
        project_number: 1
""",
        encoding="utf-8",
    )
    return path


def _repository(tmp_path: Path) -> tuple[Path, str, list[str]]:
    repo = tmp_path / "source"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    run_git(str(repo), "config", "user.name", "Test")
    run_git(str(repo), "config", "user.email", "test@example.com")
    (repo / "base.txt").write_text("base\n", encoding="utf-8")
    run_git(str(repo), "add", "base.txt")
    run_git(str(repo), "commit", "-qm", "base")
    base = git_output(str(repo), "rev-parse", "HEAD").strip()
    run_git(str(repo), "branch", "release/1.0", base)

    sources = []
    for number, filename in ((1, "one.txt"), (2, "two.txt")):
        run_git(str(repo), "checkout", "--detach", base)
        Path(repo, filename).write_text(f"{number}\n", encoding="utf-8")
        run_git(str(repo), "add", filename)
        run_git(str(repo), "commit", "-qm", f"source {number}")
        sources.append(git_output(str(repo), "rev-parse", "HEAD").strip())
    return repo, base, sources


def _descriptor(path: Path, *, base: str, source: str, number: int) -> None:
    write_json(
        path,
        {
            "schema_version": 1,
            "kind": "manual-backport-discovery",
            "repository": "org/repo",
            "push_repository": "org/repo",
            "target_branch": "release/1.0",
            "base_commit": base,
            "source_pr": {
                "number": number,
                "title": f"Source change {number}",
                "url": f"https://github.com/org/repo/pull/{number}",
                "merge_commit": source,
                "commits": [source],
                "diff": "",
            },
            "branch_name": f"agent/backport/{number}-to-release/1.0",
            "policy": {
                "language": "c",
                "validation": _validation_policy(),
                "validation_waiver": None,
                "repair_validation_failures": True,
                "max_conflicting_files": 10,
                "backport_label": "backport",
                "llm_conflict_label": "ai-resolved-conflicts",
                "automation": operational_policy_to_dict(OperationalPolicy()),
            },
        },
    )


def test_rolling_aggregate_combines_validated_candidates_and_publishes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, sources = _repository(tmp_path)
    registry = _registry(tmp_path)
    candidate_root = tmp_path / "candidates"
    candidate_root.mkdir()
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    monkeypatch.setattr(aggregate, "github_https_url", lambda _repo: str(remote))

    validated_contents: list[set[str]] = []

    def fake_validation(repo_dir, adapter, commands, *, log_path=None):
        files = {
            path.name
            for path in Path(repo_dir).iterdir()
            if path.is_file()
        }
        validated_contents.append(files)
        if log_path:
            Path(log_path).write_text("passed", encoding="utf-8")
        return ValidationRunResult(
            True,
            "",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", fake_validation)
    for number, source in enumerate(sources, start=1):
        discovery = tmp_path / f"discovery-{number}.json"
        artifact = candidate_root / f"candidate-{number}"
        _descriptor(discovery, base=base, source=source, number=number)
        assert phased.prepare(
            discovery_path=discovery,
            output_directory=artifact,
        ) == "ready"
        assert phased.validate(
            registry_path=str(registry),
            artifact_directory=artifact,
        ) == "passed"

    capped_root = tmp_path / "capped-aggregate"
    aggregate.prepare(
        registry_path=str(registry),
        candidates_directory=candidate_root,
        output_directory=capped_root,
        max_candidates=1,
    )
    capped = load_prepared(capped_root / "group-001")
    assert capped.changed_paths == ("one.txt",)
    capped_report = phased._load_json(capped.aggregate_path)
    assert [
        item["source_pr_number"] for item in capped_report["candidates"]
    ] == [1]

    aggregate_root = tmp_path / "aggregate"
    index = aggregate.prepare(
        registry_path=str(registry),
        candidates_directory=candidate_root,
        output_directory=aggregate_root,
    )
    assert index["groups"][0]["status"] == "ready"
    group = aggregate_root / "group-001"
    prepared = load_prepared(group)
    assert prepared.aggregate_path is not None
    assert prepared.branch_name == "agent/backport/sweep/release/1.0"
    assert prepared.changed_paths == ("one.txt", "two.txt")

    validation_index = aggregate.validate(
        registry_path=str(registry),
        artifact_directory=aggregate_root,
    )
    assert validation_index["groups"][0]["status"] == "passed"
    validated = load_validated(group)
    assert {"base.txt", "one.txt", "two.txt"} in validated_contents
    repositories_file = tmp_path / "verified-repositories.txt"
    assert aggregate.preflight_publish(
        registry_path=str(registry),
        artifact_directory=aggregate_root,
        repositories_output=repositories_file,
    ) == ("org/repo",)
    assert repositories_file.read_text(encoding="utf-8") == "org/repo\n"

    created: dict[str, Any] = {}

    class Pull:
        number = 99
        html_url = "https://github.com/org/repo/pull/99"
        state = "open"
        draft = False

        def __init__(self, body: str) -> None:
            self.body = body
            self.head = type(
                "Head",
                (),
                {
                    "repo": type(
                        "HeadRepo",
                        (),
                        {"full_name": "org/repo"},
                    )(),
                },
            )()

        def add_to_labels(self, *labels) -> None:
            created["labels"] = labels

        def edit(self, *, title: str | None = None, body: str) -> None:
            if title is not None:
                created["edited"] = title
            self.body = body

    class Repo:
        def get_pulls(self, **_kwargs):
            return created.get("existing_pulls", [])

        def create_pull(self, **kwargs):
            created["pull"] = kwargs
            pull = Pull(kwargs["body"])
            created["pull_object"] = pull
            return pull

    repo = Repo()
    gh = type("GitHub", (), {"get_repo": lambda self, _name: repo})()
    monkeypatch.setattr(aggregate, "Github", lambda **_kwargs: gh)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "valkeyrie-bot[bot]")

    urls = aggregate.publish(
        registry_path=str(registry),
        artifact_directory=aggregate_root,
        token="publisher-token",
    )

    assert urls == ["https://github.com/org/repo/pull/99"]
    body = created["pull"]["body"]
    assert "## Applied" in body
    assert "#1" in body
    assert "#2" in body
    branch = "agent/backport/sweep/release/1.0"
    published = git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert git_output(str(remote), "rev-parse", f"{published}^{{tree}}").strip() == (
        validated.prepared.result_tree
    )
    publication = load_publication_manifest(
        group,
        expected_kind="backport-aggregate-publication",
        final_state_keys=aggregate._PUBLICATION_STATE_KEYS,
        expected_source_file="validated.json",
        expected_source_sha256=validated.manifest_sha256,
    )
    assert publication.final_state["candidate_count"] == 2

    run_git(str(remote), "checkout", "--detach", base)
    (remote / "three.txt").write_text("3\n", encoding="utf-8")
    run_git(str(remote), "add", "three.txt")
    run_git(str(remote), "commit", "-qm", "source 3")
    source_three = git_output(str(remote), "rev-parse", "HEAD").strip()
    third_root = tmp_path / "third-candidate"
    third_root.mkdir()
    discovery_three = tmp_path / "discovery-3.json"
    artifact_three = third_root / "candidate-3"
    _descriptor(discovery_three, base=base, source=source_three, number=3)
    assert phased.prepare(
        discovery_path=discovery_three,
        output_directory=artifact_three,
    ) == "ready"
    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_three,
    ) == "passed"

    rolling_root = tmp_path / "rolling-update"
    rolling_index = aggregate.prepare(
        registry_path=str(registry),
        candidates_directory=third_root,
        output_directory=rolling_root,
    )
    assert rolling_index["groups"][0]["status"] == "ready"
    rolling_group = rolling_root / "group-001"
    assert aggregate.validate(
        registry_path=str(registry),
        artifact_directory=rolling_root,
    )["groups"][0]["status"] == "passed"
    rolling_validated = load_validated(rolling_group)
    report = aggregate._load_report(rolling_validated)
    assert report["prior_source_prs"] == [1, 2]
    assert report["candidates"][0]["source_pr_number"] == 3
    aggregate.preflight_publish(
        registry_path=str(registry),
        artifact_directory=rolling_root,
    )
    created["existing_pulls"] = [created["pull_object"]]
    created["pull_object"].draft = True
    created["pull_object"].node_id = "PR_node_99"
    ready: list[tuple[str, str]] = []
    monkeypatch.setattr(
        aggregate,
        "_mark_ready_for_review",
        lambda token, node_id: ready.append((token, node_id)),
    )

    aggregate.publish(
        registry_path=str(registry),
        artifact_directory=rolling_root,
        token="publisher-token",
    )

    assert "edited" in created
    assert ready == [("publisher-token", "PR_node_99")]
    updated_body = created["pull_object"].body
    assert "#1" in updated_body
    assert "#2" in updated_body
    assert "#3" in updated_body
    updated = git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert git_output(str(remote), "rev-parse", f"{updated}^{{tree}}").strip() == (
        rolling_validated.prepared.result_tree
    )


def test_aggregate_reconciles_ai_resolution_comments() -> None:
    comments = []

    class Comment:
        def __init__(self, body: str) -> None:
            self.body = body
            self.html_url = "https://github.com/org/repo/pull/9#issuecomment-1"
            self.user = type("User", (), {"login": "valkeyrie-bot[bot]"})()

        def edit(self, body: str) -> None:
            self.body = body

        def delete(self) -> None:
            comments.remove(self)

    class Pull:
        html_url = "https://github.com/org/repo/pull/9"

        def get_issue_comments(self):
            return list(comments)

        def create_issue_comment(self, body: str):
            comment = Comment(body)
            comments.append(comment)
            return comment

    resolution = ResolutionResult(
        path="src/value.c",
        resolved_content="resolved",
        resolution_summary="Used the release-branch API.",
        reviewer_diff="-old\n+resolved",
    )
    candidate = CandidateResult(
        source_pr_number=42,
        source_pr_title="Fix value handling",
        outcome="applied",
        detail=DETAIL_RESOLVED_BY_AI,
        resolutions=[resolution],
        resolved_by_ai=True,
        resolved_commit_sha="a" * 40,
    )

    urls = aggregate._reconcile_aggregate_diff_comments(
        Pull(),
        BranchSweepResult(
            target_branch="release/1.0",
            candidates_found=1,
            results=[candidate],
        ),
        branch_applied=[candidate],
        bot_login="valkeyrie-bot[bot]",
    )

    assert urls == {
        42: "https://github.com/org/repo/pull/9#issuecomment-1",
    }
    assert len(comments) == 1
    assert "AI conflict resolution: source PR #42" in comments[0].body
    assert "/commit/" + "a" * 40 in comments[0].body


def test_verified_candidate_failure_is_rendered_as_needs_attention(
    tmp_path: Path,
) -> None:
    discovery = tmp_path / "discovery.json"
    metadata = tmp_path / "metadata.json"
    _descriptor(
        discovery,
        base="a" * 40,
        source="b" * 40,
        number=77,
    )
    descriptor = phased._load_json(discovery)
    write_json(
        metadata,
        {
            "schema_version": 1,
            "kind": "manual-backport-metadata",
            "source_pr": descriptor["source_pr"],
            "had_conflicts": False,
            "applied_commits": ["b" * 40],
            "resolutions": [],
            "reason": "",
        },
    )
    failure = {
        "prepared": SimpleNamespace(
            discovery_path=discovery,
            metadata_path=metadata,
            source_pr_number=77,
            attempt=0,
            manifest_sha256="c" * 64,
            patch_sha256="d" * 64,
        ),
        "failure_kind": "validation-candidate",
        "detail": "compiler error: unavailable release-branch API",
        "source_manifest_sha256": "e" * 64,
    }

    row = aggregate._failure_row(failure)
    result, branch_applied = aggregate._report_results(
        {
            "target_branch": "release/1.0",
            "prior_source_prs": [],
            "candidates": [row],
        },
    )
    body = aggregate.build_pr_body(result, branch_applied=branch_applied)

    assert row["outcome"] == "skipped-validation-failed"
    assert "## Needs attention" in body
    assert "#77" in body
    assert "unavailable release-branch API" in body


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("directory", "group-001", "directories are duplicated"),
        ("target_branch", "release/1.0", "repository and branch groups are duplicated"),
    ],
)
def test_aggregate_index_rejects_duplicate_groups(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    first = {
        "directory": "group-001",
        "status": "passed",
        "repository": "org/repo",
        "target_branch": "release/1.0",
    }
    second = {
        "directory": "group-002",
        "status": "passed",
        "repository": "org/repo",
        "target_branch": "release/2.0",
        field: value,
    }
    path = tmp_path / "aggregate-validation-index.json"
    write_json(
        path,
        {
            "schema_version": 1,
            "kind": "backport-aggregate-validation-index",
            "groups": [first, second],
        },
    )

    with pytest.raises(Exception, match=message):
        aggregate._load_index(
            path,
            kind="backport-aggregate-validation-index",
        )
