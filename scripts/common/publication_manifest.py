"""Strict final-state manifests emitted by credentialed publishers."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.common.phase_artifact import (
    SCHEMA_VERSION,
    ArtifactError,
    load_json,
    sha256_file,
    write_json,
)

PUBLICATION_FILE = "publication.json"
_PUBLICATION_KEYS = {
    "schema_version",
    "kind",
    "source",
    "publisher",
    "final_state",
}
_SOURCE_KEYS = {"manifest_file", "manifest_sha256"}
_PUBLISHER_KEYS = {
    "identity",
    "installation_id",
    "workflow_repository",
    "workflow_sha",
    "workflow_ref",
    "workflow_name",
    "job",
    "run_id",
    "run_attempt",
    "published_at",
}
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_IDENTITY_RE = re.compile(r"^[A-Za-z0-9_.-]+(?:\[bot\])?$")
_KIND_RE = re.compile(r"^[a-z][a-z0-9-]{0,79}-publication$")
_MAX_NODES = 10_000
_MAX_DEPTH = 12
_MAX_STRING_BYTES = 256 * 1024


@dataclass(frozen=True)
class PublicationArtifact:
    kind: str
    source: dict[str, str]
    publisher: dict[str, Any]
    final_state: dict[str, Any]
    manifest_path: Path
    manifest_sha256: str


def publisher_context(identity: str | None = None) -> dict[str, Any]:
    """Capture the App identity and immutable Actions execution coordinates."""
    resolved_identity = (identity or os.environ.get("PUBLISHER_IDENTITY", "")).strip()
    if not _IDENTITY_RE.fullmatch(resolved_identity):
        raise ArtifactError("publisher identity is missing or invalid")
    installation_id = os.environ.get("PUBLISHER_INSTALLATION_ID", "").strip()
    if installation_id and (
        not installation_id.isdecimal() or int(installation_id) <= 0
    ):
        raise ArtifactError("publisher installation ID is invalid")
    repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if repository and not _REPO_RE.fullmatch(repository):
        raise ArtifactError("publisher workflow repository is invalid")
    sha = os.environ.get("GITHUB_SHA", "").strip().lower()
    if sha and not _SHA1_RE.fullmatch(sha):
        raise ArtifactError("publisher workflow SHA is invalid")
    return {
        "identity": resolved_identity,
        "installation_id": installation_id,
        "workflow_repository": repository,
        "workflow_sha": sha,
        "workflow_ref": _env_text("GITHUB_REF", 4096),
        "workflow_name": _env_text("GITHUB_WORKFLOW", 1024),
        "job": _env_text("GITHUB_JOB", 255),
        "run_id": _env_nonnegative_int("GITHUB_RUN_ID"),
        "run_attempt": _env_nonnegative_int("GITHUB_RUN_ATTEMPT"),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


def write_publication_manifest(
    directory: str | Path,
    *,
    kind: str,
    source_manifest_file: str,
    source_manifest_sha256: str,
    publisher: dict[str, Any],
    final_state: dict[str, Any],
    final_state_keys: set[str],
) -> PublicationArtifact:
    """Validate and write one content-addressable final publication record."""
    root = Path(directory).resolve()
    if not _KIND_RE.fullmatch(kind):
        raise ArtifactError("publication kind is invalid")
    _plain_name(source_manifest_file, "source manifest")
    _sha256(source_manifest_sha256, "source manifest")
    _validate_publisher(publisher)
    state = _exact(final_state, final_state_keys, "publication final_state")
    _validate_json_budget(state)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "source": {
            "manifest_file": source_manifest_file,
            "manifest_sha256": source_manifest_sha256,
        },
        "publisher": publisher,
        "final_state": state,
    }
    write_json(root / PUBLICATION_FILE, manifest)
    return load_publication_manifest(
        root,
        expected_kind=kind,
        final_state_keys=final_state_keys,
        expected_source_file=source_manifest_file,
        expected_source_sha256=source_manifest_sha256,
    )


def load_publication_manifest(
    directory: str | Path,
    *,
    expected_kind: str,
    final_state_keys: set[str],
    expected_source_file: str | None = None,
    expected_source_sha256: str | None = None,
) -> PublicationArtifact:
    """Load a strict publication record and verify its source manifest."""
    root = Path(directory).resolve()
    path = root / PUBLICATION_FILE
    data = _exact(
        load_json(path, max_bytes=4 * 1024 * 1024),
        _PUBLICATION_KEYS,
        "publication manifest",
    )
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != expected_kind:
        raise ArtifactError("unsupported publication manifest")
    source = _exact(data["source"], _SOURCE_KEYS, "publication source")
    source_file = _plain_name(source["manifest_file"], "source manifest")
    source_sha = _sha256(source["manifest_sha256"], "source manifest")
    if expected_source_file is not None and source_file != expected_source_file:
        raise ArtifactError("publication source file differs from expected phase")
    if expected_source_sha256 is not None and source_sha != expected_source_sha256:
        raise ArtifactError("publication source digest differs from expected phase")
    source_path = root / source_file
    actual_source_sha, _ = sha256_file(source_path, max_bytes=4 * 1024 * 1024)
    if actual_source_sha != source_sha:
        raise ArtifactError("publication source manifest digest mismatch")
    publisher = _exact(data["publisher"], _PUBLISHER_KEYS, "publisher")
    _validate_publisher(publisher)
    final_state = _exact(
        data["final_state"],
        final_state_keys,
        "publication final_state",
    )
    _validate_json_budget(final_state)
    manifest_sha, _ = sha256_file(path, max_bytes=4 * 1024 * 1024)
    return PublicationArtifact(
        kind=expected_kind,
        source={"manifest_file": source_file, "manifest_sha256": source_sha},
        publisher=publisher,
        final_state=final_state,
        manifest_path=path,
        manifest_sha256=manifest_sha,
    )


def _validate_publisher(raw: dict[str, Any]) -> None:
    data = _exact(raw, _PUBLISHER_KEYS, "publisher")
    if not isinstance(data["identity"], str) or not _IDENTITY_RE.fullmatch(
        data["identity"],
    ):
        raise ArtifactError("publisher identity is invalid")
    installation = data["installation_id"]
    if not isinstance(installation, str) or (
        installation and (
            not installation.isdecimal() or int(installation) <= 0
        )
    ):
        raise ArtifactError("publisher installation ID is invalid")
    repository = data["workflow_repository"]
    if not isinstance(repository, str) or (
        repository and not _REPO_RE.fullmatch(repository)
    ):
        raise ArtifactError("publisher workflow repository is invalid")
    sha = data["workflow_sha"]
    if not isinstance(sha, str) or (sha and not _SHA1_RE.fullmatch(sha)):
        raise ArtifactError("publisher workflow SHA is invalid")
    for key, limit in (
        ("workflow_ref", 4096),
        ("workflow_name", 1024),
        ("job", 255),
        ("published_at", 100),
    ):
        value = data[key]
        if not isinstance(value, str) or len(value.encode("utf-8")) > limit:
            raise ArtifactError(f"publisher {key} is invalid")
    try:
        datetime.fromisoformat(data["published_at"])
    except ValueError as exc:
        raise ArtifactError("publisher timestamp is invalid") from exc
    for key in ("run_id", "run_attempt"):
        value = data[key]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
        ):
            raise ArtifactError(f"publisher {key} is invalid")


def _validate_json_budget(value: Any) -> None:
    nodes = 0

    def visit(item: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_NODES or depth > _MAX_DEPTH:
            raise ArtifactError("publication final state is too complex")
        if item is None or isinstance(item, (bool, int, float)):
            return
        if isinstance(item, str):
            if len(item.encode("utf-8")) > _MAX_STRING_BYTES:
                raise ArtifactError("publication final state string is oversized")
            return
        if isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            for key, child in item.items():
                if len(key.encode("utf-8")) > 255:
                    raise ArtifactError("publication final state key is oversized")
                visit(child, depth + 1)
            return
        raise ArtifactError("publication final state contains a non-JSON value")

    visit(value, 0)


def _exact(raw: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ArtifactError(f"{label} must be an object")
    actual = set(raw)
    if actual != keys:
        raise ArtifactError(
            f"{label} keys invalid: unknown={sorted(actual - keys)}, "
            f"missing={sorted(keys - actual)}",
        )
    return dict(raw)


def _plain_name(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 255
        or "/" in value
        or "\\" in value
    ):
        raise ArtifactError(f"{label} file name is invalid")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ArtifactError(f"{label} SHA-256 is invalid")
    return value


def _env_text(name: str, limit: int) -> str:
    value = os.environ.get(name, "").strip()
    if len(value.encode("utf-8")) > limit:
        raise ArtifactError(f"{name} exceeds {limit} bytes")
    return value


def _env_nonnegative_int(name: str) -> int:
    value = os.environ.get(name, "").strip()
    if not value:
        return 0
    if not value.isdecimal():
        raise ArtifactError(f"{name} is not a non-negative integer")
    return int(value)
