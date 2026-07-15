from __future__ import annotations

from pathlib import Path

import pytest

from scripts.common.ai_evidence import finalize_ai_evidence
from scripts.common.phase_artifact import (
    ArtifactError,
    commands_digest,
    load_prepared,
    load_validated,
    load_validation,
    policy_digest,
    sha256_bytes,
    write_json,
)


def _prepared(tmp_path: Path) -> dict[str, object]:
    patch = b"diff --git a/a.txt b/a.txt\n"
    metadata = b'{"title":"test"}\n'
    (tmp_path / "change.patch").write_bytes(patch)
    (tmp_path / "metadata.json").write_bytes(metadata)
    discovery_sha = write_json(
        tmp_path / "discovery.json",
        {"schema_version": 1, "kind": "test-discovery"},
    )
    evidence_file, evidence_sha = finalize_ai_evidence(tmp_path)
    return {
        "schema_version": 1,
        "kind": "backport-prepared",
        "status": "ready",
        "discovery_file": "discovery.json",
        "discovery_sha256": discovery_sha,
        "repository": "org/repo",
        "push_repository": "org/repo",
        "target_branch": "release/1.0",
        "base_commit": "a" * 40,
        "source_pr_number": 12,
        "source_merge_commit": "b" * 40,
        "source_commits": ["c" * 40],
        "branch_name": "agent/backport/12-to-release-1.0",
        "patch_file": "change.patch",
        "patch_sha256": sha256_bytes(patch),
        "patch_bytes": len(patch),
        "changed_paths": ["a.txt"],
        "result_tree": "d" * 40,
        "policy_sha256": policy_digest({"adapter": "test"}),
        "metadata_file": "metadata.json",
        "metadata_sha256": sha256_bytes(metadata),
        "ai_evidence_file": evidence_file,
        "ai_evidence_sha256": evidence_sha,
        "attempt": 0,
        "parent_prepared_manifest_sha256": None,
        "failed_validation_manifest_sha256": None,
        "aggregate_file": None,
        "aggregate_sha256": None,
    }


def test_load_validated_checks_every_content_digest(tmp_path: Path) -> None:
    prepared = _prepared(tmp_path)
    prepared_digest = write_json(tmp_path / "prepared.json", prepared)
    log = b"validation passed\n"
    (tmp_path / "validation.log").write_bytes(log)
    plan = {"adapter": "test", "commands": [{"argv": ["true"]}]}
    plan_sha = write_json(tmp_path / "validation-plan.json", plan)
    write_json(
        tmp_path / "validated.json",
        {
            "schema_version": 1,
            "kind": "backport-validated",
            "status": "passed",
            "failure_stage": "none",
            "prepared_manifest_sha256": prepared_digest,
            "patch_sha256": prepared["patch_sha256"],
            "base_commit": prepared["base_commit"],
            "result_tree": prepared["result_tree"],
            "policy_sha256": prepared["policy_sha256"],
            "commands_sha256": commands_digest(plan),
            "plan_file": "validation-plan.json",
            "plan_sha256": plan_sha,
            "log_file": "validation.log",
            "log_sha256": sha256_bytes(log),
        },
    )

    artifact = load_validated(tmp_path)

    assert artifact.prepared.changed_paths == ("a.txt",)
    assert artifact.prepared.manifest_sha256 == prepared_digest
    assert artifact.plan == plan
    assert artifact.log_path.name == "validation.log"


def test_failed_validation_is_content_addressed_but_not_publishable(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    prepared_digest = write_json(tmp_path / "prepared.json", prepared)
    log = b"candidate failed\n"
    (tmp_path / "validation.log").write_bytes(log)
    plan = {"adapter": "test", "commands": [{"argv": ["false"]}]}
    plan_sha = write_json(tmp_path / "validation-plan.json", plan)
    write_json(
        tmp_path / "validated.json",
        {
            "schema_version": 1,
            "kind": "backport-validated",
            "status": "failed",
            "failure_stage": "candidate",
            "prepared_manifest_sha256": prepared_digest,
            "patch_sha256": prepared["patch_sha256"],
            "base_commit": prepared["base_commit"],
            "result_tree": prepared["result_tree"],
            "policy_sha256": prepared["policy_sha256"],
            "commands_sha256": commands_digest(plan),
            "plan_file": "validation-plan.json",
            "plan_sha256": plan_sha,
            "log_file": "validation.log",
            "log_sha256": sha256_bytes(log),
        },
    )

    failed = load_validation(tmp_path)
    assert failed.status == "failed"
    assert failed.failure_stage == "candidate"
    with pytest.raises(ArtifactError, match="does not have passed status"):
        load_validated(tmp_path)


def test_load_prepared_rejects_unknown_keys(tmp_path: Path) -> None:
    manifest = _prepared(tmp_path)
    manifest["surprise"] = True
    write_json(tmp_path / "prepared.json", manifest)

    with pytest.raises(ArtifactError, match="unknown=.*surprise"):
        load_prepared(tmp_path)


def test_load_prepared_rejects_patch_tampering(tmp_path: Path) -> None:
    write_json(tmp_path / "prepared.json", _prepared(tmp_path))
    (tmp_path / "change.patch").write_bytes(b"replacement")

    with pytest.raises(ArtifactError, match="patch digest or size"):
        load_prepared(tmp_path)


def test_load_prepared_rejects_git_metadata_path(tmp_path: Path) -> None:
    manifest = _prepared(tmp_path)
    manifest["changed_paths"] = [".git/config"]
    write_json(tmp_path / "prepared.json", manifest)

    with pytest.raises(ArtifactError, match="unsafe changed path"):
        load_prepared(tmp_path)


def test_non_ready_artifact_must_be_empty(tmp_path: Path) -> None:
    manifest = _prepared(tmp_path)
    manifest["status"] = "refused"
    write_json(tmp_path / "prepared.json", manifest)

    with pytest.raises(ArtifactError, match="non-ready"):
        load_prepared(tmp_path)
