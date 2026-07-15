"""Strict content-addressed artifacts for untrusted workflow phases."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

SCHEMA_VERSION = 1
MAX_PATCH_BYTES = 32 * 1024 * 1024
MAX_PATHS = 2048
MAX_PATH_BYTES = 4096

_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_BRANCH_RE = re.compile(r"^(?!-)(?!.*\.\.)(?!.*//)[A-Za-z0-9._/-]+$")
_PREPARED_KEYS = {
    "schema_version",
    "kind",
    "status",
    "discovery_file",
    "discovery_sha256",
    "repository",
    "push_repository",
    "target_branch",
    "base_commit",
    "source_pr_number",
    "source_merge_commit",
    "source_commits",
    "branch_name",
    "patch_file",
    "patch_sha256",
    "patch_bytes",
    "changed_paths",
    "result_tree",
    "policy_sha256",
    "metadata_file",
    "metadata_sha256",
    "ai_evidence_file",
    "ai_evidence_sha256",
    "attempt",
    "parent_prepared_manifest_sha256",
    "failed_validation_manifest_sha256",
    "aggregate_file",
    "aggregate_sha256",
}
_VALIDATED_KEYS = {
    "schema_version",
    "kind",
    "status",
    "prepared_manifest_sha256",
    "patch_sha256",
    "base_commit",
    "result_tree",
    "policy_sha256",
    "commands_sha256",
    "plan_file",
    "plan_sha256",
    "log_file",
    "log_sha256",
    "failure_stage",
}

ArtifactStatus = Literal["ready", "no-change", "refused"]
ValidationStatus = Literal["passed", "failed"]
ValidationFailureStage = Literal["none", "baseline", "candidate", "side-effect"]


class ArtifactError(ValueError):
    """Raised when a phase artifact is malformed or fails integrity checks."""


@dataclass(frozen=True)
class PreparedArtifact:
    status: ArtifactStatus
    discovery_path: Path
    discovery_sha256: str
    repository: str
    push_repository: str
    target_branch: str
    base_commit: str
    source_pr_number: int
    source_merge_commit: str | None
    source_commits: tuple[str, ...]
    branch_name: str
    patch_path: Path
    patch_sha256: str
    changed_paths: tuple[str, ...]
    result_tree: str
    policy_sha256: str
    metadata_path: Path
    ai_evidence: tuple[dict[str, Any], ...]
    attempt: int
    parent_prepared_manifest_sha256: str | None
    failed_validation_manifest_sha256: str | None
    aggregate_path: Path | None
    aggregate_sha256: str | None
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True)
class ValidatedArtifact:
    prepared: PreparedArtifact
    status: ValidationStatus
    failure_stage: ValidationFailureStage
    commands_sha256: str
    plan: dict[str, Any]
    plan_path: Path
    log_path: Path
    manifest_path: Path
    manifest_sha256: str


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one JSON representation used for phase digests."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("ascii")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, *, max_bytes: int | None = None) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise ArtifactError(f"{path.name} exceeds {max_bytes} bytes")
                digest.update(chunk)
    except OSError as exc:
        raise ArtifactError(f"cannot read {path.name}: {exc}") from exc
    return digest.hexdigest(), total


def policy_digest(adapter: dict[str, Any]) -> str:
    """Digest one fully typed validation adapter policy."""
    payload = canonical_json_bytes(adapter)
    if len(payload) > 1024 * 1024:
        raise ArtifactError("validation adapter policy exceeds 1 MiB")
    return sha256_bytes(payload)


def commands_digest(plan: dict[str, Any]) -> str:
    """Digest the exact typed command plan executed by validation."""
    payload = canonical_json_bytes(plan)
    if len(payload) > 1024 * 1024:
        raise ArtifactError("validation command plan exceeds 1 MiB")
    return sha256_bytes(payload)


def write_json(path: Path, value: Any) -> str:
    data = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(data)
        temporary.replace(path)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise ArtifactError(f"cannot write {path.name}: {exc}") from exc
    return sha256_bytes(data)


def load_json(path: Path, *, max_bytes: int = 1024 * 1024) -> Any:
    try:
        size = path.stat().st_size
        if size > max_bytes:
            raise ArtifactError(f"{path.name} exceeds {max_bytes} bytes")
        raw = path.read_bytes()
    except OSError as exc:
        raise ArtifactError(f"cannot read {path.name}: {exc}") from exc
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ArtifactError(f"{path.name} is not valid JSON") from exc


def load_prepared(directory: str | Path) -> PreparedArtifact:
    root = Path(directory).resolve()
    manifest_path = root / "prepared.json"
    raw = load_json(manifest_path)
    data = _mapping(raw, "prepared manifest")
    _exact_keys(data, _PREPARED_KEYS, "prepared manifest")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "backport-prepared":
        raise ArtifactError("unsupported prepared artifact schema or kind")
    status = _status(data["status"])
    discovery_path = _contained_file(
        root,
        data["discovery_file"],
        "discovery_file",
    )
    discovery_sha256 = _sha256(
        data["discovery_sha256"],
        "discovery_sha256",
    )
    actual_discovery_sha, _ = sha256_file(
        discovery_path,
        max_bytes=4 * 1024 * 1024,
    )
    if actual_discovery_sha != discovery_sha256:
        raise ArtifactError("discovery digest does not match prepared manifest")
    repository = _repo(data["repository"], "repository")
    push_repository = _repo(data["push_repository"], "push_repository")
    target_branch = _branch(data["target_branch"])
    base_commit = _sha1(data["base_commit"], "base_commit")
    source_pr_number = data["source_pr_number"]
    if not isinstance(source_pr_number, int) or isinstance(source_pr_number, bool) or source_pr_number <= 0:
        raise ArtifactError("source_pr_number must be a positive integer")
    source_merge_commit = data["source_merge_commit"]
    if source_merge_commit is not None:
        source_merge_commit = _sha1(source_merge_commit, "source_merge_commit")
    source_commits_raw = data["source_commits"]
    if not isinstance(source_commits_raw, list) or len(source_commits_raw) > 1000:
        raise ArtifactError("source_commits must be a list with at most 1000 entries")
    source_commits = tuple(_sha1(value, "source commit") for value in source_commits_raw)
    branch_name = _branch(data["branch_name"])
    changed_paths = _paths(data["changed_paths"])
    result_tree = _sha1(data["result_tree"], "result_tree")
    policy_sha256 = _sha256(data["policy_sha256"], "policy_sha256")

    patch_path = _contained_file(root, data["patch_file"], "patch_file")
    patch_sha256 = _sha256(data["patch_sha256"], "patch_sha256")
    patch_bytes = data["patch_bytes"]
    if not isinstance(patch_bytes, int) or isinstance(patch_bytes, bool) or not 0 <= patch_bytes <= MAX_PATCH_BYTES:
        raise ArtifactError("patch_bytes is outside the allowed range")
    actual_patch_sha, actual_patch_bytes = sha256_file(
        patch_path,
        max_bytes=MAX_PATCH_BYTES,
    )
    if (actual_patch_sha, actual_patch_bytes) != (patch_sha256, patch_bytes):
        raise ArtifactError("patch digest or size does not match prepared manifest")

    metadata_path = _contained_file(root, data["metadata_file"], "metadata_file")
    metadata_sha256 = _sha256(data["metadata_sha256"], "metadata_sha256")
    actual_metadata_sha, _ = sha256_file(metadata_path, max_bytes=4 * 1024 * 1024)
    if actual_metadata_sha != metadata_sha256:
        raise ArtifactError("metadata digest does not match prepared manifest")
    from scripts.common.ai_evidence import load_ai_evidence_index

    ai_evidence = load_ai_evidence_index(
        root,
        data["ai_evidence_file"],
        data["ai_evidence_sha256"],
    )
    attempt = data["attempt"]
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt not in {0, 1}:
        raise ArtifactError("prepared attempt must be 0 or 1")
    parent_prepared_manifest_sha256 = data["parent_prepared_manifest_sha256"]
    failed_validation_manifest_sha256 = data["failed_validation_manifest_sha256"]
    if attempt == 0:
        if (
            parent_prepared_manifest_sha256 is not None
            or failed_validation_manifest_sha256 is not None
        ):
            raise ArtifactError("initial prepared artifact must not have repair lineage")
    else:
        parent_prepared_manifest_sha256 = _sha256(
            parent_prepared_manifest_sha256,
            "parent_prepared_manifest_sha256",
        )
        failed_validation_manifest_sha256 = _sha256(
            failed_validation_manifest_sha256,
            "failed_validation_manifest_sha256",
        )
    aggregate_file = data["aggregate_file"]
    aggregate_sha256 = data["aggregate_sha256"]
    aggregate_path: Path | None = None
    if aggregate_file is None or aggregate_sha256 is None:
        if aggregate_file is not None or aggregate_sha256 is not None:
            raise ArtifactError("aggregate file and digest must both be null or set")
        aggregate_sha256 = None
    else:
        aggregate_path = _contained_file(root, aggregate_file, "aggregate_file")
        aggregate_sha256 = _sha256(aggregate_sha256, "aggregate_sha256")
        actual_aggregate_sha, _ = sha256_file(
            aggregate_path,
            max_bytes=4 * 1024 * 1024,
        )
        if actual_aggregate_sha != aggregate_sha256:
            raise ArtifactError("aggregate report digest does not match prepared manifest")

    if status == "ready":
        if patch_bytes == 0 or not changed_paths:
            raise ArtifactError("ready artifact must contain a patch and changed paths")
    elif patch_bytes != 0 or changed_paths:
        raise ArtifactError("non-ready artifact must not contain a patch or changed paths")

    manifest_sha256, _ = sha256_file(manifest_path, max_bytes=1024 * 1024)
    return PreparedArtifact(
        status=status,
        discovery_path=discovery_path,
        discovery_sha256=discovery_sha256,
        repository=repository,
        push_repository=push_repository,
        target_branch=target_branch,
        base_commit=base_commit,
        source_pr_number=source_pr_number,
        source_merge_commit=source_merge_commit,
        source_commits=source_commits,
        branch_name=branch_name,
        patch_path=patch_path,
        patch_sha256=patch_sha256,
        changed_paths=changed_paths,
        result_tree=result_tree,
        policy_sha256=policy_sha256,
        metadata_path=metadata_path,
        ai_evidence=ai_evidence,
        attempt=attempt,
        parent_prepared_manifest_sha256=parent_prepared_manifest_sha256,
        failed_validation_manifest_sha256=failed_validation_manifest_sha256,
        aggregate_path=aggregate_path,
        aggregate_sha256=aggregate_sha256,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
    )


def load_validation(directory: str | Path) -> ValidatedArtifact:
    """Load a content-addressed passed or failed validation result."""
    root = Path(directory).resolve()
    prepared = load_prepared(root)
    manifest_path = root / "validated.json"
    raw = load_json(manifest_path)
    data = _mapping(raw, "validated manifest")
    _exact_keys(data, _VALIDATED_KEYS, "validated manifest")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "backport-validated":
        raise ArtifactError("unsupported validated artifact schema or kind")
    status = data["status"]
    if status not in {"passed", "failed"}:
        raise ArtifactError("validation status must be passed or failed")
    failure_stage = data["failure_stage"]
    if failure_stage not in {"none", "baseline", "candidate", "side-effect"}:
        raise ArtifactError("validation failure_stage is invalid")
    if (status == "passed") != (failure_stage == "none"):
        raise ArtifactError("validation status and failure_stage are inconsistent")
    expected = {
        "prepared_manifest_sha256": prepared.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "base_commit": prepared.base_commit,
        "result_tree": prepared.result_tree,
        "policy_sha256": prepared.policy_sha256,
    }
    for key, value in expected.items():
        if data[key] != value:
            raise ArtifactError(f"validated {key} does not match prepared artifact")
    commands_sha256 = _sha256(data["commands_sha256"], "commands_sha256")
    plan_path = _contained_file(root, data["plan_file"], "plan_file")
    plan_sha256 = _sha256(data["plan_sha256"], "plan_sha256")
    actual_plan_sha, _ = sha256_file(plan_path, max_bytes=1024 * 1024)
    if actual_plan_sha != plan_sha256:
        raise ArtifactError("validation plan digest does not match validated manifest")
    plan = _mapping(
        load_json(plan_path, max_bytes=1024 * 1024),
        "validation plan",
    )
    if commands_digest(plan) != commands_sha256:
        raise ArtifactError("validation plan does not match commands digest")
    log_path = _contained_file(root, data["log_file"], "log_file")
    log_sha256 = _sha256(data["log_sha256"], "log_sha256")
    actual_log_sha, _ = sha256_file(log_path, max_bytes=32 * 1024 * 1024)
    if actual_log_sha != log_sha256:
        raise ArtifactError("validation log digest does not match validated manifest")
    manifest_sha256, _ = sha256_file(manifest_path, max_bytes=1024 * 1024)
    return ValidatedArtifact(
        prepared=prepared,
        status=status,
        failure_stage=failure_stage,
        commands_sha256=commands_sha256,
        plan=dict(plan),
        plan_path=plan_path,
        log_path=log_path,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
    )


def load_validated(directory: str | Path) -> ValidatedArtifact:
    """Load a validation result and require successful validation."""
    artifact = load_validation(directory)
    if artifact.status != "passed":
        raise ArtifactError("validated artifact does not have passed status")
    return artifact


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ArtifactError(f"{label} must be a JSON object")
    return value


def _exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    unknown = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if unknown or missing:
        raise ArtifactError(f"{label} keys invalid: unknown={unknown}, missing={missing}")


def _repo(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _REPO_RE.fullmatch(value):
        raise ArtifactError(f"{label} is not a valid owner/repository")
    return value


def _branch(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 255 or not _BRANCH_RE.fullmatch(value):
        raise ArtifactError("branch name is invalid")
    return value


def _sha1(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA1_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a full lowercase Git SHA")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a lowercase SHA-256")
    return value


def _status(value: Any) -> ArtifactStatus:
    if value not in {"ready", "no-change", "refused"}:
        raise ArtifactError("prepared status is invalid")
    return value


def _paths(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > MAX_PATHS:
        raise ArtifactError(f"changed_paths must contain at most {MAX_PATHS} paths")
    paths: list[str] = []
    for path in value:
        if not isinstance(path, str) or not path or len(path.encode("utf-8")) > MAX_PATH_BYTES:
            raise ArtifactError("changed path is empty, non-string, or too long")
        pure = PurePosixPath(path)
        if pure.is_absolute() or ".." in pure.parts or "\0" in path or path == ".git" or path.startswith(".git/"):
            raise ArtifactError(f"unsafe changed path: {path!r}")
        paths.append(path)
    if len(set(paths)) != len(paths) or paths != sorted(paths):
        raise ArtifactError("changed_paths must be unique and sorted")
    return tuple(paths)


def _contained_file(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or "/" in value or "\\" in value:
        raise ArtifactError(f"{label} must be a plain file name")
    path = root / value
    try:
        if path.is_symlink() or not path.is_file() or path.resolve().parent != root:
            raise ArtifactError(f"{label} is not a contained regular file")
    except OSError as exc:
        raise ArtifactError(f"cannot inspect {label}: {exc}") from exc
    return path
