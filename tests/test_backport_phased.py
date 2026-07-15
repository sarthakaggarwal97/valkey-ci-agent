from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.backport import phased
from scripts.backport.models import CherryPickResult, ConflictedFile
from scripts.backport.provenance import parse_provenance_commit
from scripts.common.operational_controls import (
    OperationalPolicy,
    operational_policy_to_dict,
)
from scripts.common.phase_artifact import (
    ArtifactError,
    load_prepared,
    load_validated,
    load_validation,
    sha256_file,
    write_json,
)
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
        "default_commands": ["verify-value"],
        "commands": [
            {
                "id": "verify-value",
                "argv": ["test", "-f", "value.txt"],
                "working_directory": ".",
                "timeout_seconds": 60,
                "inputs": ["value.txt"],
                "expected_artifacts": [],
            },
        ],
        "rules": [],
    }


@pytest.fixture(autouse=True)
def _run_typed_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_validation(repo_dir, adapter, commands, *, log_path=None):
        assert Path(repo_dir, "value.txt").is_file()
        if log_path:
            Path(log_path).write_text("typed validation passed", encoding="utf-8")
        return ValidationRunResult(
            True,
            "",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", fake_validation)


def _repository(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "source"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    run_git(str(repo), "config", "user.name", "Test")
    run_git(str(repo), "config", "user.email", "test@example.com")
    (repo / "value.txt").write_text("base\n", encoding="utf-8")
    run_git(str(repo), "add", "value.txt")
    run_git(str(repo), "commit", "-qm", "base")
    base = git_output(str(repo), "rev-parse", "HEAD").strip()
    run_git(str(repo), "branch", "release/1.0", base)
    (repo / "value.txt").write_text("changed\n", encoding="utf-8")
    run_git(str(repo), "commit", "-qam", "source change")
    source = git_output(str(repo), "rev-parse", "HEAD").strip()
    return repo, base, source


def _registry(tmp_path: Path) -> Path:
    path = tmp_path / "repos.yml"
    path.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    project_owner_type: organization
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
      default_commands: [verify-value]
      commands:
        - id: verify-value
          argv: ["test", "-f", "value.txt"]
          working_directory: "."
          timeout_seconds: 60
          inputs: ["value.txt"]
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


def _descriptor(
    path: Path,
    *,
    base: str,
    source: str,
    push_repository: str = "org/repo",
) -> None:
    write_json(
        path,
        {
            "schema_version": 1,
            "kind": "manual-backport-discovery",
            "repository": "org/repo",
            "push_repository": push_repository,
            "target_branch": "release/1.0",
            "base_commit": base,
            "source_pr": {
                "number": 42,
                "title": "Change the value",
                "url": "https://github.com/org/repo/pull/42",
                "merge_commit": source,
                "commits": [source],
                "diff": "",
            },
            "branch_name": "agent/backport/42-to-release/1.0",
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


def test_manual_backport_phases_publish_only_the_validated_tree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))

    validated_values: list[str] = []

    def fake_validation(repo_dir, adapter, commands, *, log_path=None):
        validated_values.append(
            Path(repo_dir, "value.txt").read_text(encoding="utf-8"),
        )
        if log_path:
            Path(log_path).write_text("typed validation passed", encoding="utf-8")
        return ValidationRunResult(
            True,
            "",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", fake_validation)

    assert phased.prepare(
        discovery_path=discovery,
        output_directory=artifact_dir,
    ) == "ready"
    prepared = load_prepared(artifact_dir)
    assert prepared.base_commit == base
    assert prepared.changed_paths == ("value.txt",)

    phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )
    assert validated_values == ["base\n", "changed\n"]
    validated = load_validated(artifact_dir)
    phased.preflight_publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )

    created: dict[str, Any] = {}

    class FakeCreator:
        def __init__(self, _gh: Any, **kwargs: Any) -> None:
            created["constructor"] = kwargs

        def check_duplicate(self, source_pr: int, target: str) -> None:
            created["duplicate_check"] = (source_pr, target)

        def create_backport_pr(
            self,
            context: Any,
            cherry_result: Any,
            resolutions: Any,
            branch: str,
        ) -> str:
            created["create"] = (context, cherry_result, resolutions, branch)
            return "https://github.com/org/repo/pull/99"

    monkeypatch.setattr(phased, "Github", lambda **_kwargs: object())
    monkeypatch.setattr(phased, "BackportPRCreator", FakeCreator)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    url = phased.publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
        token="publisher-token",
    )

    assert url.endswith("/99")
    branch = "agent/backport/42-to-release/1.0"
    published = git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert git_output(str(remote), "rev-parse", f"{published}^{{tree}}").strip() == (
        validated.prepared.result_tree
    )
    message = git_output(str(remote), "show", "-s", "--format=%B", published)
    assert f"Validated-Tree: {validated.prepared.result_tree}" in message
    provenance = parse_provenance_commit(message)
    target_commit = git_output(str(remote), "rev-parse", f"{published}^").strip()
    assert provenance["target_commit"] == target_commit
    assert provenance["base_commit"] == base
    assert provenance["source_pr_number"] == 42
    assert provenance["source_commits"] == [source]
    assert git_output(str(remote), "rev-parse", f"{target_commit}^").strip() == base
    assert created["duplicate_check"] == (42, "release/1.0")
    publication = load_publication_manifest(
        artifact_dir,
        expected_kind="backport-publication",
        final_state_keys=phased._PUBLICATION_STATE_KEYS,
        expected_source_file="validated.json",
        expected_source_sha256=validated.manifest_sha256,
    )
    assert publication.final_state["remote_ref_sha"] == published
    assert publication.final_state["pull_request_number"] == 99


def test_preflight_rejects_registry_policy_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)
    phased.validate(registry_path=str(registry), artifact_directory=artifact_dir)
    registry.write_text(
        registry.read_text(encoding="utf-8").replace(
            'argv: ["test", "-f", "value.txt"]',
            'argv: ["test", "-e", "value.txt"]',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ArtifactError, match="policy changed"):
        phased.preflight_publish(
            registry_path=str(registry),
            artifact_directory=artifact_dir,
        )


def test_validation_stops_when_target_baseline_is_red(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)

    calls: list[str] = []

    def red_baseline(repo_dir, adapter, commands, *, log_path=None):
        calls.append(Path(repo_dir, "value.txt").read_text(encoding="utf-8"))
        if log_path:
            Path(log_path).write_text("baseline red", encoding="utf-8")
        return ValidationRunResult(
            False,
            "target branch is already red",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", red_baseline)
    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    ) == "failed"
    assert calls == ["base\n"]
    failed = load_validation(artifact_dir)
    assert failed.failure_stage == "baseline"
    with pytest.raises(ArtifactError, match="does not have passed status"):
        load_validated(artifact_dir)


def test_failed_backport_is_repaired_once_and_requires_fresh_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    repaired_dir = tmp_path / "repaired"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)

    def candidate_failure(repo_dir, adapter, commands, *, log_path=None):
        value = Path(repo_dir, "value.txt").read_text(encoding="utf-8")
        success = value == "base\n"
        if log_path:
            Path(log_path).write_text(
                "passed" if success else "error: target API differs",
                encoding="utf-8",
            )
        return ValidationRunResult(
            success,
            "" if success else "target API differs",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", candidate_failure)
    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    ) == "failed"
    failed = load_validation(artifact_dir)
    assert failed.failure_stage == "candidate"

    def repair_agent(_profile, _prompt, *, cwd):
        Path(cwd, "value.txt").write_text("repaired\n", encoding="utf-8")
        return type("AgentResult", (), {"returncode": 0})()

    monkeypatch.setattr(phased, "run_agent", repair_agent)
    assert phased.repair_validation_failure(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
        output_directory=repaired_dir,
    ) == "ready"
    repaired = load_prepared(repaired_dir)
    assert repaired.attempt == 1
    assert repaired.parent_prepared_manifest_sha256 == failed.prepared.manifest_sha256
    assert repaired.failed_validation_manifest_sha256 == failed.manifest_sha256

    with pytest.raises(ArtifactError, match="validated.json"):
        phased.preflight_publish(
            registry_path=str(registry),
            artifact_directory=repaired_dir,
        )

    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=repaired_dir,
    ) == "failed"
    with pytest.raises(ArtifactError, match="one attempt"):
        phased.repair_validation_failure(
            registry_path=str(registry),
            artifact_directory=repaired_dir,
            output_directory=tmp_path / "second-repair",
        )

    def repaired_validation(repo_dir, adapter, commands, *, log_path=None):
        if log_path:
            Path(log_path).write_text("passed", encoding="utf-8")
        return ValidationRunResult(
            True,
            "",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", repaired_validation)
    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=repaired_dir,
    ) == "passed"
    phased.preflight_publish(
        registry_path=str(registry),
        artifact_directory=repaired_dir,
    )


def test_validation_repair_refuses_out_of_scope_edits_and_preserves_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    repaired_dir = tmp_path / "repaired"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)

    calls = 0

    def candidate_failure(repo_dir, adapter, commands, *, log_path=None):
        nonlocal calls
        calls += 1
        if log_path:
            Path(log_path).write_text("candidate failed", encoding="utf-8")
        return ValidationRunResult(
            calls == 1,
            "" if calls == 1 else "candidate failed",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", candidate_failure)
    phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )
    before = sha256_file(artifact_dir / "change.patch")

    def out_of_scope_agent(_profile, _prompt, *, cwd):
        Path(cwd, "outside.txt").write_text("unexpected\n", encoding="utf-8")
        return type("AgentResult", (), {"returncode": 0})()

    monkeypatch.setattr(phased, "run_agent", out_of_scope_agent)
    assert phased.repair_validation_failure(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
        output_directory=repaired_dir,
    ) == "refused"
    assert not (repaired_dir / "prepared.json").exists()
    assert sha256_file(artifact_dir / "change.patch") == before
    assert load_validation(artifact_dir).prepared.patch_sha256 == (
        load_prepared(artifact_dir).patch_sha256
    )


def test_failed_backport_reports_and_later_resolves_needs_attention(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)

    calls = 0

    def candidate_failure(repo_dir, adapter, commands, *, log_path=None):
        nonlocal calls
        calls += 1
        if log_path:
            Path(log_path).write_text(
                "baseline passed" if calls == 1 else "compiler error",
                encoding="utf-8",
            )
        return ValidationRunResult(
            calls == 1,
            "" if calls == 1 else "compiler error",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", candidate_failure)
    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    ) == "failed"
    phased.preflight_failure_report(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )

    comments = []

    class Comment:
        def __init__(self, body: str) -> None:
            self.body = body
            self.user = type("User", (), {"login": "valkeyrie-bot[bot]"})()

        def edit(self, body: str) -> None:
            self.body = body

    class Issue:
        number = 42

        def get_comments(self):
            return list(comments)

        def create_comment(self, body: str):
            comment = Comment(body)
            comments.append(comment)
            return comment

    pull = type(
        "Pull",
        (),
        {
            "head": type("Head", (), {"sha": source})(),
            "html_url": "https://github.com/org/repo/pull/42",
        },
    )()
    issue = Issue()

    class Repo:
        full_name = "org/repo"

        def get_pull(self, _number: int):
            return pull

        def get_issue(self, _number: int):
            return issue

    repo = Repo()
    gh = type("GitHub", (), {"get_repo": lambda self, _name: repo})()
    monkeypatch.setattr(phased, "Github", lambda **_kwargs: gh)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "valkeyrie-bot[bot]")

    phased.report_failure(
        artifact_directory=artifact_dir,
        token="comment-token",
    )
    assert len(comments) == 1
    assert "## Backport needs attention" in comments[0].body
    assert "compiler error" in comments[0].body
    publication = load_publication_manifest(
        artifact_dir,
        expected_kind="backport-failure-publication",
        final_state_keys=phased._FAILURE_PUBLICATION_STATE_KEYS,
        expected_source_file="validated.json",
        expected_source_sha256=load_validation(artifact_dir).manifest_sha256,
    )
    assert publication.final_state["comment_status"] == "reconciled"
    failure_handoff = phased.load_failure_handoff(artifact_dir)
    assert failure_handoff["failure_kind"] == "validation-candidate"
    assert failure_handoff["prepared"].source_pr_number == 42

    phased.report_failure(
        artifact_directory=artifact_dir,
        token="comment-token",
    )
    assert len(comments) == 1

    phased._resolve_existing_failure_comment(
        gh,
        load_prepared(artifact_dir),
        writer_login="valkeyrie-bot[bot]",
        backport_url="https://github.com/org/repo/pull/99",
    )
    assert "## Backport automation recovered" in comments[0].body
    assert "/pull/99" in comments[0].body


def test_prepare_enforces_shared_max_conflicting_files_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    conflicts = [
        ConflictedFile(
            path=f"conflict-{index}.txt",
            target_branch_content="target",
            source_branch_content="source",
        )
        for index in range(11)
    ]
    monkeypatch.setattr(
        phased,
        "cherry_pick",
        lambda *_args, **_kwargs: CherryPickResult(
            success=False,
            conflicting_files=conflicts,
        ),
    )
    monkeypatch.setattr(
        phased,
        "resolve_conflicts_with_claude",
        lambda *_args, **_kwargs: pytest.fail(
            "AI must not run above max_conflicting_files",
        ),
    )

    assert phased.prepare(
        discovery_path=discovery,
        output_directory=artifact_dir,
    ) == "refused"
    prepared = load_prepared(artifact_dir)
    assert prepared.status == "refused"
    metadata = phased._load_json(prepared.metadata_path)
    assert "exceeds max_conflicting_files=10" in metadata["reason"]


def test_preflight_rejects_target_branch_movement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)
    phased.validate(registry_path=str(registry), artifact_directory=artifact_dir)

    run_git(str(remote), "branch", "-f", "release/1.0", source)

    with pytest.raises(ArtifactError, match="target branch moved"):
        phased.preflight_publish(
            registry_path=str(registry),
            artifact_directory=artifact_dir,
        )
    assert not (artifact_dir / "publisher-permit.json").exists()


def test_validation_rejects_tracked_file_side_effect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)
    calls = 0

    def mutating_validation(repo_dir, adapter, commands, *, log_path=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            Path(repo_dir, "value.txt").write_text("validation side effect\n")
        if log_path:
            Path(log_path).write_text("validation returned success", encoding="utf-8")
        return ValidationRunResult(
            True,
            "",
            tuple(command.id for command in commands),
            command_plan_payload(adapter, commands),
        )

    monkeypatch.setattr(phased, "run_validation_adapter", mutating_validation)

    assert phased.validate(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    ) == "failed"
    assert calls == 2
    assert load_validation(artifact_dir).failure_stage == "side-effect"


def test_stale_fork_branch_uses_an_exact_force_with_lease(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    push_remote = tmp_path / "push-remote"
    run_git(None, "clone", str(remote), str(push_remote))
    run_git(str(push_remote), "config", "receive.denyCurrentBranch", "updateInstead")
    branch = "agent/backport/42-to-release/1.0"
    run_git(str(push_remote), "branch", branch, base)
    stale_sha = git_output(str(push_remote), "rev-parse", branch).strip()

    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(
        discovery,
        base=base,
        source=source,
        push_repository="fork/repo",
    )
    monkeypatch.setattr(
        phased,
        "github_https_url",
        lambda repo: str(push_remote) if repo == "fork/repo" else str(remote),
    )
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)
    phased.validate(registry_path=str(registry), artifact_directory=artifact_dir)
    phased.preflight_publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )

    class FakeCreator:
        def __init__(self, _gh: Any, **_kwargs: Any) -> None:
            pass

        def check_duplicate(self, _source_pr: int, _target: str) -> None:
            return None

        def create_backport_pr(self, *_args: Any) -> str:
            return "https://github.com/org/repo/pull/99"

    push_calls: list[tuple[str, ...]] = []
    real_run_git = phased.run_git

    def recording_run_git(repo_dir, *args, **kwargs):
        if args and args[0] == "push":
            push_calls.append(args)
        return real_run_git(repo_dir, *args, **kwargs)

    monkeypatch.setattr(phased, "Github", lambda **_kwargs: object())
    monkeypatch.setattr(phased, "BackportPRCreator", FakeCreator)
    monkeypatch.setattr(phased, "run_git", recording_run_git)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    phased.publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
        token="publisher-token",
    )

    assert len(push_calls) == 1
    assert (
        f"--force-with-lease=refs/heads/{branch}:{stale_sha}"
        in push_calls[0]
    )
    published = git_output(
        str(push_remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert published != stale_sha


def test_publisher_retry_recovers_push_and_ambiguous_pr_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote, base, source = _repository(tmp_path)
    registry = _registry(tmp_path)
    discovery = tmp_path / "discovery.json"
    artifact_dir = tmp_path / "artifact"
    _descriptor(discovery, base=base, source=source)
    monkeypatch.setattr(phased, "github_https_url", lambda _repo: str(remote))
    phased.prepare(discovery_path=discovery, output_directory=artifact_dir)
    phased.validate(registry_path=str(registry), artifact_directory=artifact_dir)
    phased.preflight_publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
    )

    state = {"created": False, "create_calls": 0}

    class FlakyCreator:
        def __init__(self, _gh: Any, **_kwargs: Any) -> None:
            pass

        def check_duplicate(self, _source_pr: int, _target: str) -> str | None:
            if state["created"]:
                return "https://github.com/org/repo/pull/99"
            return None

        def create_backport_pr(self, *_args: Any) -> str:
            state["create_calls"] += 1
            state["created"] = True
            raise ConnectionError("response lost after PR creation")

    monkeypatch.setattr(phased, "Github", lambda **_kwargs: object())
    monkeypatch.setattr(phased, "BackportPRCreator", FlakyCreator)
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    with pytest.raises(ConnectionError, match="response lost"):
        phased.publish(
            registry_path=str(registry),
            artifact_directory=artifact_dir,
            token="publisher-token",
        )
    branch = "agent/backport/42-to-release/1.0"
    first_published = git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip()
    assert not (artifact_dir / "publication.json").exists()

    url = phased.publish(
        registry_path=str(registry),
        artifact_directory=artifact_dir,
        token="publisher-token",
    )

    assert url == "https://github.com/org/repo/pull/99"
    assert state["create_calls"] == 1
    assert git_output(
        str(remote),
        "rev-parse",
        f"refs/heads/{branch}",
    ).strip() == first_published
    publication = load_publication_manifest(
        artifact_dir,
        expected_kind="backport-publication",
        final_state_keys=phased._PUBLICATION_STATE_KEYS,
    )
    assert publication.final_state["remote_ref_sha"] == first_published
