from __future__ import annotations

import json

import pytest

from scripts.common.phase_artifact import ArtifactError, write_json
from scripts.common.publication_manifest import (
    load_publication_manifest,
    publisher_context,
    write_publication_manifest,
)


def test_publication_manifest_binds_source_publisher_and_final_state(
    tmp_path,
    monkeypatch,
) -> None:
    source_sha = write_json(
        tmp_path / "validated.json",
        {"schema_version": 1, "kind": "validated"},
    )
    monkeypatch.setenv("PUBLISHER_IDENTITY", "valkeyrie-bot[bot]")
    monkeypatch.setenv("GITHUB_REPOSITORY", "valkey-io/valkey-ci-agent")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")

    written = write_publication_manifest(
        tmp_path,
        kind="test-publication",
        source_manifest_file="validated.json",
        source_manifest_sha256=source_sha,
        publisher=publisher_context(),
        final_state={"commit": "b" * 40},
        final_state_keys={"commit"},
    )
    loaded = load_publication_manifest(
        tmp_path,
        expected_kind="test-publication",
        final_state_keys={"commit"},
        expected_source_file="validated.json",
        expected_source_sha256=source_sha,
    )

    assert loaded.manifest_sha256 == written.manifest_sha256
    assert loaded.publisher["identity"] == "valkeyrie-bot[bot]"
    assert loaded.publisher["run_id"] == 123
    assert loaded.final_state["commit"] == "b" * 40


def test_publication_manifest_rejects_source_replay_or_tampering(
    tmp_path,
    monkeypatch,
) -> None:
    source_sha = write_json(tmp_path / "validated.json", {"value": 1})
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")
    write_publication_manifest(
        tmp_path,
        kind="test-publication",
        source_manifest_file="validated.json",
        source_manifest_sha256=source_sha,
        publisher=publisher_context(),
        final_state={"commit": "b" * 40},
        final_state_keys={"commit"},
    )
    write_json(tmp_path / "validated.json", {"value": 2})

    with pytest.raises(ArtifactError, match="source manifest digest mismatch"):
        load_publication_manifest(
            tmp_path,
            expected_kind="test-publication",
            final_state_keys={"commit"},
        )


def test_publication_manifest_rejects_unknown_final_state_keys(
    tmp_path,
    monkeypatch,
) -> None:
    source_sha = write_json(tmp_path / "validated.json", {"value": 1})
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")
    write_publication_manifest(
        tmp_path,
        kind="test-publication",
        source_manifest_file="validated.json",
        source_manifest_sha256=source_sha,
        publisher=publisher_context(),
        final_state={"commit": "b" * 40},
        final_state_keys={"commit"},
    )
    manifest = json.loads((tmp_path / "publication.json").read_text())
    manifest["final_state"]["invented"] = True
    write_json(tmp_path / "publication.json", manifest)

    with pytest.raises(ArtifactError, match="unknown=.*invented"):
        load_publication_manifest(
            tmp_path,
            expected_kind="test-publication",
            final_state_keys={"commit"},
        )
