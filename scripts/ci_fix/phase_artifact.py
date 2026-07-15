"""Strict phase artifacts for credential-separated CI-fix execution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from scripts.common.phase_artifact import (
    MAX_PATCH_BYTES,
    SCHEMA_VERSION,
    ArtifactError,
    canonical_json_bytes,
    load_json,
    sha256_bytes,
    sha256_file,
)

_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_BRANCH_RE = re.compile(r"^(?!-)(?!.*\.\.)(?!.*//)[A-Za-z0-9._/-]+$")
_DISCOVERY_KEYS = {
    "schema_version",
    "kind",
    "request",
    "workflow",
    "failed_jobs",
    "logs",
}
_REQUEST_KEYS = {
    "repository",
    "pr_number",
    "head_repository",
    "head_branch",
    "head_sha",
    "run_id",
    "requested_by",
    "hint",
    "comment_id",
}
_WORKFLOW_KEYS = {
    "workflow_id",
    "workflow_path",
    "run_attempt",
    "file",
    "sha256",
}
_JOB_KEYS = {
    "database_id",
    "display_name",
    "conclusion",
    "labels",
    "runner_name",
    "runner_group_id",
    "job_id",
    "matrix",
    "environment",
    "image",
    "fidelity",
    "reason",
}
_FIDELITY_KEYS = {
    "mode",
    "authoritative_check",
    "reproduced",
    "not_reproduced",
}
_LOG_KEYS = {"source_name", "file", "sha256", "bytes"}
_PREPARED_KEYS = {
    "schema_version",
    "kind",
    "status",
    "discovery_sha256",
    "repository",
    "head_branch",
    "head_sha",
    "selected_job",
    "proposal_file",
    "proposal_sha256",
    "review_file",
    "review_sha256",
    "patch_file",
    "patch_sha256",
    "patch_bytes",
    "changed_paths",
    "result_tree",
    "port_commit",
    "reason",
    "ai_evidence_file",
    "ai_evidence_sha256",
}
_VALIDATED_KEYS = {
    "schema_version",
    "kind",
    "status",
    "prepared_sha256",
    "discovery_sha256",
    "patch_sha256",
    "head_sha",
    "result_tree",
    "selected_job",
    "command_sha256",
    "plan_file",
    "plan_sha256",
    "baseline_file",
    "baseline_sha256",
    "result_file",
    "result_sha256",
}

PreparedStatus = Literal["ready", "refused"]


@dataclass(frozen=True)
class DiscoveryArtifact:
    root: Path
    request: dict[str, Any]
    workflow: dict[str, Any]
    failed_jobs: tuple[dict[str, Any], ...]
    logs: tuple[dict[str, Any], ...]
    workflow_path: Path
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True)
class PreparedFixArtifact:
    root: Path
    status: PreparedStatus
    discovery: DiscoveryArtifact
    repository: str
    head_branch: str
    head_sha: str
    selected_job: str
    proposal_path: Path
    review_path: Path
    patch_path: Path
    patch_sha256: str
    changed_paths: tuple[str, ...]
    result_tree: str
    port_commit: str
    reason: str
    ai_evidence: tuple[dict[str, Any], ...]
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True)
class ValidatedFixArtifact:
    prepared: PreparedFixArtifact
    command_sha256: str
    plan: dict[str, Any]
    plan_path: Path
    baseline_path: Path
    result_path: Path
    manifest_path: Path
    manifest_sha256: str


def load_discovery(directory: str | Path) -> DiscoveryArtifact:
    root = Path(directory).resolve()
    manifest_path = root / "discovery.json"
    data = _exact(load_json(manifest_path, max_bytes=4 * 1024 * 1024), _DISCOVERY_KEYS, "discovery")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "ci-fix-discovery":
        raise ArtifactError("unsupported CI-fix discovery artifact")
    request = _parse_request(data["request"])
    workflow = _parse_workflow(root, data["workflow"])
    failed_jobs_raw = data["failed_jobs"]
    if not isinstance(failed_jobs_raw, list) or not 1 <= len(failed_jobs_raw) <= 100:
        raise ArtifactError("failed_jobs must contain between 1 and 100 entries")
    failed_jobs = tuple(_parse_job(item) for item in failed_jobs_raw)
    if len({item["database_id"] for item in failed_jobs}) != len(failed_jobs):
        raise ArtifactError("failed job database IDs must be unique")
    logs_raw = data["logs"]
    if not isinstance(logs_raw, list) or not 1 <= len(logs_raw) <= 2000:
        raise ArtifactError("logs must contain between 1 and 2000 entries")
    logs = tuple(_parse_log(root, item) for item in logs_raw)
    if len({item["file"] for item in logs}) != len(logs):
        raise ArtifactError("log file names must be unique")
    manifest_sha, _ = sha256_file(manifest_path, max_bytes=4 * 1024 * 1024)
    return DiscoveryArtifact(
        root=root,
        request=request,
        workflow=workflow,
        failed_jobs=failed_jobs,
        logs=logs,
        workflow_path=_contained(root, workflow["file"], "workflow file"),
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
    )


def load_prepared(directory: str | Path) -> PreparedFixArtifact:
    root = Path(directory).resolve()
    discovery = load_discovery(root)
    manifest_path = root / "prepared.json"
    data = _exact(load_json(manifest_path, max_bytes=2 * 1024 * 1024), _PREPARED_KEYS, "prepared")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "ci-fix-prepared":
        raise ArtifactError("unsupported CI-fix prepared artifact")
    if data["status"] not in {"ready", "refused"}:
        raise ArtifactError("prepared status is invalid")
    if data["discovery_sha256"] != discovery.manifest_sha256:
        raise ArtifactError("prepared artifact references a different discovery manifest")
    repository = _repo(data["repository"], "repository")
    head_branch = _branch(data["head_branch"])
    head_sha = _sha1(data["head_sha"], "head_sha")
    if (
        repository != discovery.request["repository"]
        or head_branch != discovery.request["head_branch"]
        or head_sha != discovery.request["head_sha"]
    ):
        raise ArtifactError("prepared repository identity differs from discovery")
    selected_job = _text(data["selected_job"], "selected_job", 1024)
    if selected_job and selected_job not in {
        item["display_name"] for item in discovery.failed_jobs
    }:
        raise ArtifactError("selected_job is not one of the discovered failed jobs")

    proposal_path = _verified_file(
        root,
        data["proposal_file"],
        data["proposal_sha256"],
        "proposal",
        1024 * 1024,
    )
    review_path = _verified_file(
        root,
        data["review_file"],
        data["review_sha256"],
        "review",
        1024 * 1024,
    )
    patch_path = _contained(root, data["patch_file"], "patch")
    patch_sha = _sha256(data["patch_sha256"], "patch_sha256")
    patch_size = data["patch_bytes"]
    if not isinstance(patch_size, int) or isinstance(patch_size, bool) or not 0 <= patch_size <= MAX_PATCH_BYTES:
        raise ArtifactError("patch_bytes is outside the allowed range")
    actual_patch_sha, actual_patch_size = sha256_file(
        patch_path,
        max_bytes=MAX_PATCH_BYTES,
    )
    if (patch_sha, patch_size) != (actual_patch_sha, actual_patch_size):
        raise ArtifactError("patch digest or size does not match prepared manifest")
    paths = _paths(data["changed_paths"])
    result_tree = _sha1(data["result_tree"], "result_tree")
    port_commit = data["port_commit"]
    if port_commit:
        port_commit = _sha1(port_commit, "port_commit")
    reason = _text(data["reason"], "reason", 16_384)
    from scripts.common.ai_evidence import load_ai_evidence_index

    ai_evidence = load_ai_evidence_index(
        root,
        data["ai_evidence_file"],
        data["ai_evidence_sha256"],
    )
    if data["status"] == "ready":
        if not selected_job or not patch_size or not paths:
            raise ArtifactError("ready CI-fix artifact lacks job, patch, or paths")
    elif selected_job or patch_size or paths:
        raise ArtifactError("refused CI-fix artifact must not carry actionable content")
    manifest_sha, _ = sha256_file(manifest_path, max_bytes=2 * 1024 * 1024)
    return PreparedFixArtifact(
        root=root,
        status=data["status"],
        discovery=discovery,
        repository=repository,
        head_branch=head_branch,
        head_sha=head_sha,
        selected_job=selected_job,
        proposal_path=proposal_path,
        review_path=review_path,
        patch_path=patch_path,
        patch_sha256=patch_sha,
        changed_paths=paths,
        result_tree=result_tree,
        port_commit=port_commit,
        reason=reason,
        ai_evidence=ai_evidence,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
    )


def load_validated(directory: str | Path) -> ValidatedFixArtifact:
    root = Path(directory).resolve()
    prepared = load_prepared(root)
    if prepared.status != "ready":
        raise ArtifactError("refused artifact cannot be validated")
    manifest_path = root / "validated.json"
    data = _exact(load_json(manifest_path), _VALIDATED_KEYS, "validated")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "ci-fix-validated":
        raise ArtifactError("unsupported CI-fix validated artifact")
    if data["status"] != "passed":
        raise ArtifactError("CI-fix validation status is not passed")
    expected = {
        "prepared_sha256": prepared.manifest_sha256,
        "discovery_sha256": prepared.discovery.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "head_sha": prepared.head_sha,
        "result_tree": prepared.result_tree,
        "selected_job": prepared.selected_job,
    }
    for key, value in expected.items():
        if data[key] != value:
            raise ArtifactError(f"validated {key} differs from prepared artifact")
    command_sha = _sha256(data["command_sha256"], "command_sha256")
    plan = _verified_json_file(
        root,
        data["plan_file"],
        data["plan_sha256"],
        "validation plan",
        1024 * 1024,
    )
    if sha256_bytes(canonical_json_bytes(plan)) != command_sha:
        raise ArtifactError("validation plan does not match command digest")
    baseline = _verified_file(
        root,
        data["baseline_file"],
        data["baseline_sha256"],
        "baseline",
        1024 * 1024,
    )
    result = _verified_file(
        root,
        data["result_file"],
        data["result_sha256"],
        "validation result",
        1024 * 1024,
    )
    manifest_sha, _ = sha256_file(manifest_path, max_bytes=1024 * 1024)
    return ValidatedFixArtifact(
        prepared=prepared,
        command_sha256=command_sha,
        plan=plan,
        plan_path=_contained(root, data["plan_file"], "validation plan"),
        baseline_path=baseline,
        result_path=result,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
    )


def _parse_request(raw: Any) -> dict[str, Any]:
    data = _exact(raw, _REQUEST_KEYS, "request")
    data["repository"] = _repo(data["repository"], "request repository")
    data["head_repository"] = _repo(data["head_repository"], "head repository")
    data["head_branch"] = _branch(data["head_branch"])
    data["head_sha"] = _sha1(data["head_sha"], "request head_sha")
    for key in ("pr_number", "run_id"):
        if not isinstance(data[key], int) or isinstance(data[key], bool) or data[key] <= 0:
            raise ArtifactError(f"request {key} must be a positive integer")
    if not isinstance(data["comment_id"], int) or isinstance(data["comment_id"], bool) or data["comment_id"] < 0:
        raise ArtifactError("request comment_id must be a nonnegative integer")
    data["requested_by"] = _text(data["requested_by"], "requested_by", 255)
    data["hint"] = _text(data["hint"], "hint", 500)
    return data


def _parse_workflow(root: Path, raw: Any) -> dict[str, Any]:
    data = _exact(raw, _WORKFLOW_KEYS, "workflow")
    for key in ("workflow_id", "run_attempt"):
        if not isinstance(data[key], int) or isinstance(data[key], bool) or data[key] <= 0:
            raise ArtifactError(f"workflow {key} must be positive")
    path = _text(data["workflow_path"], "workflow_path", 4096)
    pure = PurePosixPath(path)
    if pure.is_absolute() or ".." in pure.parts or not path.startswith(".github/workflows/"):
        raise ArtifactError("workflow_path is outside .github/workflows")
    _verified_file(root, data["file"], data["sha256"], "workflow", 1024 * 1024)
    return data


def _parse_job(raw: Any) -> dict[str, Any]:
    data = _exact(raw, _JOB_KEYS, "failed job")
    for key in ("database_id",):
        if not isinstance(data[key], int) or isinstance(data[key], bool) or data[key] <= 0:
            raise ArtifactError(f"failed job {key} must be positive")
    if not isinstance(data["runner_group_id"], int) or isinstance(data["runner_group_id"], bool) or data["runner_group_id"] < 0:
        raise ArtifactError("runner_group_id must be nonnegative")
    for key, limit in (
        ("display_name", 1024),
        ("conclusion", 100),
        ("runner_name", 1024),
        ("job_id", 255),
        ("environment", 100),
        ("image", 2048),
        ("reason", 4096),
    ):
        data[key] = _text(data[key], key, limit)
    if data["conclusion"] not in {"failure", "timed_out"}:
        raise ArtifactError("failed job conclusion is not actionable")
    if data["environment"] not in {"local", "docker", "macos", "unsupported"}:
        raise ArtifactError("failed job environment is invalid")
    if not isinstance(data["labels"], list) or len(data["labels"]) > 100:
        raise ArtifactError("failed job labels are invalid")
    data["labels"] = [_text(value, "job label", 255) for value in data["labels"]]
    if not isinstance(data["matrix"], dict) or len(data["matrix"]) > 32:
        raise ArtifactError("failed job matrix is invalid")
    for key, value in data["matrix"].items():
        _text(key, "matrix key", 255)
        _text(value, "matrix value", 1024)
    fidelity = _exact(data["fidelity"], _FIDELITY_KEYS, "verification fidelity")
    if fidelity["mode"] != "targeted-approximation-v1":
        raise ArtifactError("verification fidelity mode is invalid")
    if fidelity["authoritative_check"] != "pull-request-ci":
        raise ArtifactError("verification authoritative check is invalid")
    for key in ("reproduced", "not_reproduced"):
        values = fidelity[key]
        if (
            not isinstance(values, list)
            or not 1 <= len(values) <= 32
            or len(values) != len(set(values))
        ):
            raise ArtifactError(f"verification fidelity {key} is invalid")
        fidelity[key] = [
            _text(value, f"verification fidelity {key}", 255)
            for value in values
        ]
    data["fidelity"] = fidelity
    return data


def _parse_log(root: Path, raw: Any) -> dict[str, Any]:
    data = _exact(raw, _LOG_KEYS, "log")
    data["source_name"] = _text(data["source_name"], "log source_name", 4096)
    path = _contained(root, data["file"], "log")
    digest = _sha256(data["sha256"], "log sha256")
    size = data["bytes"]
    if not isinstance(size, int) or isinstance(size, bool) or not 0 <= size <= 16 * 1024 * 1024:
        raise ArtifactError("individual log size is invalid")
    actual_digest, actual_size = sha256_file(path, max_bytes=16 * 1024 * 1024)
    if (digest, size) != (actual_digest, actual_size):
        raise ArtifactError("log digest or size mismatch")
    return data


def _verified_file(
    root: Path,
    file_value: Any,
    digest_value: Any,
    label: str,
    max_bytes: int,
) -> Path:
    path = _contained(root, file_value, label)
    expected = _sha256(digest_value, f"{label} sha256")
    actual, _ = sha256_file(path, max_bytes=max_bytes)
    if actual != expected:
        raise ArtifactError(f"{label} digest mismatch")
    return path


def _verified_json_file(
    root: Path,
    file_value: Any,
    digest_value: Any,
    label: str,
    max_bytes: int,
) -> dict[str, Any]:
    path = _verified_file(root, file_value, digest_value, label, max_bytes)
    value = load_json(path, max_bytes=max_bytes)
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise ArtifactError(f"{label} must be an object")
    return dict(value)


def _contained(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or "/" in value or "\\" in value:
        raise ArtifactError(f"{label} file name is invalid")
    path = root / value
    try:
        if path.is_symlink() or not path.is_file() or path.resolve().parent != root:
            raise ArtifactError(f"{label} is not a contained regular file")
    except OSError as exc:
        raise ArtifactError(f"cannot inspect {label}: {exc}") from exc
    return path


def _paths(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list) or len(raw) > 2048:
        raise ArtifactError("changed_paths is invalid")
    paths: list[str] = []
    for value in raw:
        path = _text(value, "changed path", 4096)
        pure = PurePosixPath(path)
        if (
            not path
            or pure.is_absolute()
            or ".." in pure.parts
            or path == ".git"
            or path.startswith(".git/")
            or "\0" in path
        ):
            raise ArtifactError(f"unsafe changed path: {path!r}")
        paths.append(path)
    if paths != sorted(set(paths)):
        raise ArtifactError("changed_paths must be sorted and unique")
    return tuple(paths)


def _exact(raw: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ArtifactError(f"{label} must be an object")
    unknown = sorted(set(raw) - keys)
    missing = sorted(keys - set(raw))
    if unknown or missing:
        raise ArtifactError(f"{label} keys invalid: unknown={unknown}, missing={missing}")
    return dict(raw)


def _text(value: Any, label: str, limit: int) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > limit:
        raise ArtifactError(f"{label} must be a string no larger than {limit} bytes")
    return value


def _repo(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _REPO_RE.fullmatch(value):
        raise ArtifactError(f"{label} is invalid")
    return value


def _branch(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 255 or not _BRANCH_RE.fullmatch(value):
        raise ArtifactError("head branch is invalid")
    return value


def _sha1(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA1_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a full lowercase Git SHA")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a lowercase SHA-256")
    return value
