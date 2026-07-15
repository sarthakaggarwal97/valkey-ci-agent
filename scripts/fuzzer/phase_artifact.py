"""Strict artifacts crossing fuzzer discovery, AI, and publisher jobs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.common.phase_artifact import (
    SCHEMA_VERSION,
    ArtifactError,
    load_json,
    sha256_file,
)
from scripts.fuzzer.models import FuzzerRunAnalysis
from scripts.fuzzer.schema import FuzzerSchemaError, analysis_from_dict

MAX_RUNS = 20
MAX_FILES_PER_RUN = 256
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_TOTAL_EVIDENCE_BYTES = 256 * 1024 * 1024

_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.ya?ml$")
_DISCOVERY_KEYS = {
    "schema_version",
    "kind",
    "repository",
    "workflow_file",
    "expected_cursor",
    "bootstrap",
    "runs",
}
_RUN_KEYS = {
    "run_id",
    "run_url",
    "conclusion",
    "head_sha",
    "evidence_status",
    "evidence_error",
    "files",
}
_FILE_KEYS = {"path", "sha256", "bytes"}
_ANALYZED_KEYS = {
    "schema_version",
    "kind",
    "discovery_sha256",
    "analyses",
    "ai_evidence_file",
    "ai_evidence_sha256",
}
_ANALYSIS_REF_KEYS = {"run_id", "file", "sha256"}
_EVIDENCE_STATES = {"ready", "missing", "empty", "oversized"}


@dataclass(frozen=True)
class EvidenceFile:
    relative_path: str
    path: Path
    sha256: str
    size: int


@dataclass(frozen=True)
class DiscoveredRun:
    run_id: int
    run_url: str
    conclusion: str
    head_sha: str
    evidence_status: str
    evidence_error: str
    files: tuple[EvidenceFile, ...]


@dataclass(frozen=True)
class FuzzerDiscoveryArtifact:
    root: Path
    repository: str
    workflow_file: str
    expected_cursor: int
    bootstrap: bool
    runs: tuple[DiscoveredRun, ...]
    manifest_path: Path
    manifest_sha256: str


@dataclass(frozen=True)
class AnalyzedRun:
    analysis: FuzzerRunAnalysis
    path: Path
    sha256: str


@dataclass(frozen=True)
class FuzzerAnalyzedArtifact:
    discovery: FuzzerDiscoveryArtifact
    analyses: tuple[AnalyzedRun, ...]
    ai_evidence: tuple[dict[str, Any], ...]
    manifest_path: Path
    manifest_sha256: str


def load_discovery(directory: str | Path) -> FuzzerDiscoveryArtifact:
    root = Path(directory).resolve()
    manifest_path = root / "discovery.json"
    data = _exact(
        load_json(manifest_path, max_bytes=4 * 1024 * 1024),
        _DISCOVERY_KEYS,
        "fuzzer discovery",
    )
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "fuzzer-discovery":
        raise ArtifactError("unsupported fuzzer discovery artifact")
    repository = _repo(data["repository"])
    workflow_file = _workflow(data["workflow_file"])
    expected_cursor = data["expected_cursor"]
    if (
        not isinstance(expected_cursor, int)
        or isinstance(expected_cursor, bool)
        or expected_cursor < 0
    ):
        raise ArtifactError("expected_cursor must be a non-negative integer")
    bootstrap = data["bootstrap"]
    if not isinstance(bootstrap, bool) or bootstrap != (expected_cursor == 0):
        raise ArtifactError("bootstrap must exactly describe a zero cursor")
    raw_runs = data["runs"]
    if not isinstance(raw_runs, list) or len(raw_runs) > MAX_RUNS:
        raise ArtifactError(f"runs must contain at most {MAX_RUNS} entries")
    runs = tuple(_parse_run(root, raw) for raw in raw_runs)
    ids = [run.run_id for run in runs]
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        raise ArtifactError("fuzzer runs must have unique ascending IDs")
    if any(run_id <= expected_cursor for run_id in ids):
        raise ArtifactError("fuzzer run IDs must be above expected_cursor")
    total = sum(item.size for run in runs for item in run.files)
    if total > MAX_TOTAL_EVIDENCE_BYTES:
        raise ArtifactError("fuzzer discovery evidence exceeds the total size cap")
    manifest_sha, _ = sha256_file(manifest_path, max_bytes=4 * 1024 * 1024)
    return FuzzerDiscoveryArtifact(
        root=root,
        repository=repository,
        workflow_file=workflow_file,
        expected_cursor=expected_cursor,
        bootstrap=bootstrap,
        runs=runs,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
    )


def load_analyzed(directory: str | Path) -> FuzzerAnalyzedArtifact:
    root = Path(directory).resolve()
    discovery = load_discovery(root)
    manifest_path = root / "analyzed.json"
    data = _exact(
        load_json(manifest_path, max_bytes=2 * 1024 * 1024),
        _ANALYZED_KEYS,
        "fuzzer analyzed",
    )
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != "fuzzer-analyzed":
        raise ArtifactError("unsupported fuzzer analyzed artifact")
    if data["discovery_sha256"] != discovery.manifest_sha256:
        raise ArtifactError("analyzed artifact references a different discovery manifest")
    refs = data["analyses"]
    if not isinstance(refs, list) or len(refs) != len(discovery.runs):
        raise ArtifactError("analyzed artifact must contain one analysis per run")
    analyses: list[AnalyzedRun] = []
    for raw, discovered in zip(refs, discovery.runs):
        ref = _exact(raw, _ANALYSIS_REF_KEYS, "analysis reference")
        if ref["run_id"] != discovered.run_id:
            raise ArtifactError("analysis run order or identity differs from discovery")
        path = _contained(root, ref["file"], "analysis file")
        expected_sha = _sha256(ref["sha256"], "analysis sha256")
        actual_sha, _ = sha256_file(path, max_bytes=1024 * 1024)
        if actual_sha != expected_sha:
            raise ArtifactError("analysis digest does not match analyzed manifest")
        try:
            analysis = analysis_from_dict(load_json(path, max_bytes=1024 * 1024))
        except FuzzerSchemaError as exc:
            raise ArtifactError(f"invalid fuzzer analysis: {exc}") from exc
        if (
            analysis.repo != discovery.repository
            or analysis.workflow_file != discovery.workflow_file
            or analysis.run_id != discovered.run_id
            or analysis.run_url != discovered.run_url
            or analysis.conclusion != discovered.conclusion
            or analysis.head_sha != discovered.head_sha
        ):
            raise ArtifactError("analysis identity differs from discovery")
        analyses.append(AnalyzedRun(analysis=analysis, path=path, sha256=actual_sha))
    from scripts.common.ai_evidence import load_ai_evidence_index

    ai_evidence = load_ai_evidence_index(
        root,
        data["ai_evidence_file"],
        data["ai_evidence_sha256"],
    )
    manifest_sha, _ = sha256_file(manifest_path, max_bytes=2 * 1024 * 1024)
    return FuzzerAnalyzedArtifact(
        discovery=discovery,
        analyses=tuple(analyses),
        ai_evidence=ai_evidence,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
    )


def _parse_run(root: Path, raw: Any) -> DiscoveredRun:
    data = _exact(raw, _RUN_KEYS, "discovered run")
    run_id = data["run_id"]
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
        raise ArtifactError("run_id must be a positive integer")
    run_url = _text(data["run_url"], "run_url", 2_048, allow_empty=False)
    if not run_url.startswith("https://github.com/"):
        raise ArtifactError("run_url must be an https://github.com URL")
    conclusion = _text(data["conclusion"], "conclusion", 100, allow_empty=False)
    head_sha = _sha1(data["head_sha"], "head_sha")
    status = data["evidence_status"]
    if status not in _EVIDENCE_STATES:
        raise ArtifactError("evidence_status is invalid")
    error = _text(data["evidence_error"], "evidence_error", 2_000, allow_empty=True)
    raw_files = data["files"]
    if not isinstance(raw_files, list) or len(raw_files) > MAX_FILES_PER_RUN:
        raise ArtifactError("evidence files exceeds the member cap")
    files = tuple(_parse_file(root, run_id, item) for item in raw_files)
    names = [item.relative_path for item in files]
    if len(names) != len(set(names)):
        raise ArtifactError("evidence file names must be unique")
    if status == "ready" and not files:
        raise ArtifactError("ready evidence must contain files")
    if status != "ready" and files:
        raise ArtifactError("non-ready evidence must not contain files")
    if status == "ready" and error:
        raise ArtifactError("ready evidence must not carry an error")
    if status != "ready" and not error:
        raise ArtifactError("non-ready evidence must explain the failure")
    return DiscoveredRun(
        run_id=run_id,
        run_url=run_url,
        conclusion=conclusion,
        head_sha=head_sha,
        evidence_status=status,
        evidence_error=error,
        files=files,
    )


def _parse_file(root: Path, run_id: int, raw: Any) -> EvidenceFile:
    data = _exact(raw, _FILE_KEYS, "evidence file")
    path = _contained(root, data["path"], "evidence file")
    relative = path.relative_to(root).as_posix()
    expected_prefix = f"runs/{run_id}/"
    if not relative.startswith(expected_prefix):
        raise ArtifactError("evidence file is outside its run directory")
    expected_sha = _sha256(data["sha256"], "evidence sha256")
    size = data["bytes"]
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or not 0 <= size <= MAX_FILE_BYTES
    ):
        raise ArtifactError("evidence file size is outside the allowed range")
    actual_sha, actual_size = sha256_file(path, max_bytes=MAX_FILE_BYTES)
    if (actual_sha, actual_size) != (expected_sha, size):
        raise ArtifactError("evidence digest or size does not match discovery")
    return EvidenceFile(relative_path=relative, path=path, sha256=actual_sha, size=size)


def _contained(root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4_096:
        raise ArtifactError(f"{label} path is invalid")
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts or "." in pure.parts:
        raise ArtifactError(f"{label} path escapes its artifact")
    path = (root / Path(*pure.parts)).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArtifactError(f"{label} path escapes its artifact") from exc
    if not path.is_file() or path.is_symlink():
        raise ArtifactError(f"{label} is not a regular file")
    return path


def _exact(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ArtifactError(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        raise ArtifactError(
            f"{label} keys invalid: unknown={sorted(actual - expected)}, "
            f"missing={sorted(expected - actual)}",
        )
    return value


def _repo(value: Any) -> str:
    if not isinstance(value, str) or not _REPO_RE.fullmatch(value):
        raise ArtifactError("repository is invalid")
    return value


def _workflow(value: Any) -> str:
    if not isinstance(value, str) or not _WORKFLOW_RE.fullmatch(value):
        raise ArtifactError("workflow_file is invalid")
    return value


def _sha1(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA1_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a lowercase 40-character SHA")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ArtifactError(f"{label} must be a lowercase SHA-256")
    return value


def _text(value: Any, label: str, max_bytes: int, *, allow_empty: bool) -> str:
    if not isinstance(value, str):
        raise ArtifactError(f"{label} must be a string")
    if not allow_empty and not value:
        raise ArtifactError(f"{label} must not be empty")
    if len(value.encode("utf-8")) > max_bytes:
        raise ArtifactError(f"{label} exceeds {max_bytes} bytes")
    return value
