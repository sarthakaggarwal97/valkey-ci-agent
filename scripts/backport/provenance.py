"""Content-addressed provenance embedded in a backport PR's head commit."""

from __future__ import annotations

import base64
import re
from typing import Any

from scripts.common.phase_artifact import (
    ArtifactError,
    canonical_json_bytes,
    sha256_bytes,
)
from scripts.common.proc import run_git_bytes

PROVENANCE_VERSION = 1
PROVENANCE_KIND = "valkey-backport-provenance"
_PAYLOAD_PREFIX = "Valkey-Backport-Provenance: "
_DIGEST_PREFIX = "Valkey-Backport-Provenance-SHA256: "
_MAX_PAYLOAD_BYTES = 128 * 1024
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_BRANCH_RE = re.compile(r"^(?!-)(?!.*\.\.)(?!.*//)[A-Za-z0-9._/-]+$")
_KEYS = {
    "version",
    "kind",
    "repository",
    "target_branch",
    "source_pr_number",
    "source_merge_commit",
    "source_commits",
    "base_commit",
    "target_commit",
    "patch_sha256",
    "patch_id",
    "validated_tree",
    "prepared_manifest_sha256",
    "validated_manifest_sha256",
}


def build_provenance(
    *,
    repository: str,
    target_branch: str,
    source_pr_number: int,
    source_merge_commit: str | None,
    source_commits: tuple[str, ...],
    base_commit: str,
    target_commit: str,
    patch_sha256: str,
    patch_id: str,
    validated_tree: str,
    prepared_manifest_sha256: str,
    validated_manifest_sha256: str,
) -> dict[str, Any]:
    value = {
        "version": PROVENANCE_VERSION,
        "kind": PROVENANCE_KIND,
        "repository": repository,
        "target_branch": target_branch,
        "source_pr_number": source_pr_number,
        "source_merge_commit": source_merge_commit,
        "source_commits": list(source_commits),
        "base_commit": base_commit,
        "target_commit": target_commit,
        "patch_sha256": patch_sha256,
        "patch_id": patch_id,
        "validated_tree": validated_tree,
        "prepared_manifest_sha256": prepared_manifest_sha256,
        "validated_manifest_sha256": validated_manifest_sha256,
    }
    return validate_provenance(value)


def provenance_commit_message(value: dict[str, Any]) -> str:
    normalized = validate_provenance(value)
    payload = canonical_json_bytes(normalized)
    encoded = base64.urlsafe_b64encode(payload).decode("ascii")
    return "\n".join([
        (
            f"Attest backport #{normalized['source_pr_number']} provenance "
            f"for {normalized['target_branch']}"
        ),
        "",
        f"Target-Commit: {normalized['target_commit']}",
        f"Validated-Tree: {normalized['validated_tree']}",
        f"Patch-ID: {normalized['patch_id']}",
        f"Patch-SHA256: {normalized['patch_sha256']}",
        "",
        f"{_PAYLOAD_PREFIX}{encoded}",
        f"{_DIGEST_PREFIX}{sha256_bytes(payload)}",
    ])


def parse_provenance_commit(message: str) -> dict[str, Any]:
    payloads = [
        line[len(_PAYLOAD_PREFIX):].strip()
        for line in message.splitlines()
        if line.startswith(_PAYLOAD_PREFIX)
    ]
    digests = [
        line[len(_DIGEST_PREFIX):].strip()
        for line in message.splitlines()
        if line.startswith(_DIGEST_PREFIX)
    ]
    if len(payloads) != 1 or len(digests) != 1:
        raise ArtifactError("commit must contain exactly one provenance payload and digest")
    if not _SHA256_RE.fullmatch(digests[0]):
        raise ArtifactError("provenance digest is invalid")
    try:
        payload = base64.b64decode(payloads[0], altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise ArtifactError("provenance payload is not valid base64url") from exc
    if not payload or len(payload) > _MAX_PAYLOAD_BYTES:
        raise ArtifactError("provenance payload is empty or oversized")
    if sha256_bytes(payload) != digests[0]:
        raise ArtifactError("provenance payload digest does not match")
    try:
        import json

        raw = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ArtifactError("provenance payload is not valid JSON") from exc
    normalized = validate_provenance(raw)
    if canonical_json_bytes(normalized) != payload:
        raise ArtifactError("provenance payload is not canonical JSON")
    return normalized


def stable_patch_id(repo_dir: str, patch: bytes) -> str:
    """Compute Git's stable patch ID for the exact published binary diff."""
    if not patch:
        raise ArtifactError("cannot compute a patch ID for an empty patch")
    result = run_git_bytes(
        repo_dir,
        "patch-id",
        "--stable",
        input=patch,
    )
    fields = result.stdout.decode("ascii", errors="strict").split()
    if not fields or not _SHA1_RE.fullmatch(fields[0]):
        raise ArtifactError("git patch-id did not produce a stable SHA-1")
    return fields[0]


def validate_provenance(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ArtifactError("provenance must be an object")
    actual = set(raw)
    if actual != _KEYS:
        raise ArtifactError(
            f"provenance keys invalid: unknown={sorted(actual - _KEYS)}, "
            f"missing={sorted(_KEYS - actual)}",
        )
    if raw["version"] != PROVENANCE_VERSION or raw["kind"] != PROVENANCE_KIND:
        raise ArtifactError("unsupported provenance version or kind")
    repository = raw["repository"]
    if not isinstance(repository, str) or not _REPO_RE.fullmatch(repository):
        raise ArtifactError("provenance repository is invalid")
    branch = raw["target_branch"]
    if not isinstance(branch, str) or not _BRANCH_RE.fullmatch(branch):
        raise ArtifactError("provenance target branch is invalid")
    number = raw["source_pr_number"]
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        raise ArtifactError("provenance source PR number is invalid")
    merge = raw["source_merge_commit"]
    if merge is not None:
        merge = _sha1(merge, "source merge commit")
    commits = raw["source_commits"]
    if not isinstance(commits, list) or not 1 <= len(commits) <= 1000:
        raise ArtifactError("provenance source commits are invalid")
    normalized_commits = [_sha1(value, "source commit") for value in commits]
    if len(normalized_commits) != len(set(normalized_commits)):
        raise ArtifactError("provenance source commits contain duplicates")
    normalized = {
        "version": PROVENANCE_VERSION,
        "kind": PROVENANCE_KIND,
        "repository": repository,
        "target_branch": branch,
        "source_pr_number": number,
        "source_merge_commit": merge,
        "source_commits": normalized_commits,
        "base_commit": _sha1(raw["base_commit"], "base commit"),
        "target_commit": _sha1(raw["target_commit"], "target commit"),
        "patch_sha256": _sha256(raw["patch_sha256"], "patch sha256"),
        "patch_id": _sha1(raw["patch_id"], "patch id"),
        "validated_tree": _sha1(raw["validated_tree"], "validated tree"),
        "prepared_manifest_sha256": _sha256(
            raw["prepared_manifest_sha256"],
            "prepared manifest sha256",
        ),
        "validated_manifest_sha256": _sha256(
            raw["validated_manifest_sha256"],
            "validated manifest sha256",
        ),
    }
    return normalized


def _sha1(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA1_RE.fullmatch(value):
        raise ArtifactError(f"provenance {label} is not a full lowercase SHA-1")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ArtifactError(f"provenance {label} is not a lowercase SHA-256")
    return value
