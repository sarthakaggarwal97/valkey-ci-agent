"""Tests for immutable backport provenance payloads."""

from __future__ import annotations

import pytest

from scripts.backport.provenance import (
    build_provenance,
    parse_provenance_commit,
    provenance_commit_message,
)
from scripts.common.phase_artifact import ArtifactError


def _provenance():
    return build_provenance(
        repository="valkey-io/valkey",
        target_branch="8.1",
        source_pr_number=42,
        source_merge_commit="a" * 40,
        source_commits=("b" * 40,),
        base_commit="c" * 40,
        target_commit="d" * 40,
        patch_sha256="e" * 64,
        patch_id="f" * 40,
        validated_tree="1" * 40,
        prepared_manifest_sha256="2" * 64,
        validated_manifest_sha256="3" * 64,
    )


def test_provenance_commit_round_trip():
    value = _provenance()
    assert parse_provenance_commit(provenance_commit_message(value)) == value


def test_provenance_digest_detects_payload_tampering():
    message = provenance_commit_message(_provenance())
    tampered = message.replace("Valkey-Backport-Provenance: e", "Valkey-Backport-Provenance: f")
    with pytest.raises(ArtifactError):
        parse_provenance_commit(tampered)


def test_provenance_rejects_unknown_keys():
    value = {**_provenance(), "invented": True}
    with pytest.raises(ArtifactError, match="unknown"):
        provenance_commit_message(value)
