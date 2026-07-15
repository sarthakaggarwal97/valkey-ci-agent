"""Content-addressed evidence for isolated AI tool sessions."""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from scripts.common.phase_artifact import (
    ArtifactError,
    load_json,
    sha256_bytes,
    sha256_file,
    write_json,
)

INDEX_FILE = "ai-evidence-index.json"
_SCHEMA_VERSION = 1
_MAX_PROMPT_BYTES = 4 * 1024 * 1024
_MAX_TRANSCRIPT_BYTES = 32 * 1024 * 1024
_MAX_RUNS = 32
_RUN_FILE_RE = re.compile(r"^ai-run-[A-Za-z0-9_.-]{1,180}\.json$")
_RUN_KEYS = {
    "schema_version",
    "kind",
    "profile",
    "result",
    "repository_tree_sha",
    "runtime",
    "prompt",
    "stdout",
    "stderr",
}
_FILE_KEYS = {"file", "sha256", "bytes"}
_INDEX_KEYS = {"schema_version", "kind", "runs"}
_INDEX_RUN_KEYS = {"manifest_file", "manifest_sha256"}


@contextmanager
def ai_evidence_directory(directory: str | Path) -> Iterator[None]:
    """Route all AI calls in one phase into its artifact directory."""
    name = "CI_AGENT_EVIDENCE_DIR"
    previous = os.environ.get(name)
    os.environ[name] = str(Path(directory).resolve())
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def write_ai_run_evidence(
    directory: str | Path,
    *,
    run_id: str,
    profile: dict[str, Any],
    result: dict[str, Any],
    repository_tree_sha: str,
    runtime: dict[str, Any],
    prompt: str,
    stdout: str,
    stderr: str,
) -> Path:
    """Write one complete bounded tool transcript and rebuild its index."""
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_id).strip("-.")
    if not safe_id or len(safe_id) > 180:
        raise ArtifactError("AI evidence run ID is invalid")
    prefix = f"ai-run-{safe_id}"
    prompt_ref = _write_text(root, f"{prefix}-prompt.txt", prompt, _MAX_PROMPT_BYTES)
    stdout_ref = _write_text(
        root,
        f"{prefix}-stdout.jsonl",
        stdout,
        _MAX_TRANSCRIPT_BYTES,
    )
    stderr_ref = _write_text(
        root,
        f"{prefix}-stderr.txt",
        stderr,
        _MAX_TRANSCRIPT_BYTES,
    )
    manifest = {
        "schema_version": _SCHEMA_VERSION,
        "kind": "ai-run-evidence",
        "profile": profile,
        "result": result,
        "repository_tree_sha": repository_tree_sha,
        "runtime": runtime,
        "prompt": prompt_ref,
        "stdout": stdout_ref,
        "stderr": stderr_ref,
    }
    manifest_path = root / f"{prefix}.json"
    write_json(manifest_path, manifest)
    finalize_ai_evidence(root)
    return manifest_path


def finalize_ai_evidence(directory: str | Path) -> tuple[str, str]:
    """Validate all AI run manifests and write their content-addressed index."""
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifests = sorted(
        path
        for path in root.glob("ai-run-*.json")
        if path.name != INDEX_FILE
    )
    if len(manifests) > _MAX_RUNS:
        raise ArtifactError(f"AI evidence exceeds {_MAX_RUNS} runs")
    runs: list[dict[str, str]] = []
    for path in manifests:
        _load_run_manifest(root, path.name)
        digest, _ = sha256_file(path, max_bytes=2 * 1024 * 1024)
        runs.append(
            {
                "manifest_file": path.name,
                "manifest_sha256": digest,
            }
        )
    index = {
        "schema_version": _SCHEMA_VERSION,
        "kind": "ai-evidence-index",
        "runs": runs,
    }
    digest = write_json(root / INDEX_FILE, index)
    return INDEX_FILE, digest


def load_ai_evidence_index(
    root: str | Path,
    file_name: Any,
    expected_sha256: Any,
) -> tuple[dict[str, Any], ...]:
    """Verify an index and every prompt/transcript it references."""
    directory = Path(root).resolve()
    if file_name != INDEX_FILE:
        raise ArtifactError("AI evidence index file name is invalid")
    if (
        not isinstance(expected_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
    ):
        raise ArtifactError("AI evidence index SHA-256 is invalid")
    index_path = _contained_file(directory, file_name)
    actual_sha, _ = sha256_file(index_path, max_bytes=2 * 1024 * 1024)
    if actual_sha != expected_sha256:
        raise ArtifactError("AI evidence index digest mismatch")
    index = _exact(load_json(index_path), _INDEX_KEYS, "AI evidence index")
    if (
        index["schema_version"] != _SCHEMA_VERSION
        or index["kind"] != "ai-evidence-index"
    ):
        raise ArtifactError("unsupported AI evidence index")
    refs = index["runs"]
    if not isinstance(refs, list) or len(refs) > _MAX_RUNS:
        raise ArtifactError("AI evidence index run list is invalid")
    loaded: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in refs:
        ref = _exact(raw, _INDEX_RUN_KEYS, "AI evidence run reference")
        name = ref["manifest_file"]
        if not isinstance(name, str) or not _RUN_FILE_RE.fullmatch(name):
            raise ArtifactError("AI evidence manifest file name is invalid")
        if name in seen:
            raise ArtifactError("AI evidence index contains duplicate runs")
        seen.add(name)
        expected = ref["manifest_sha256"]
        if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ArtifactError("AI evidence manifest SHA-256 is invalid")
        path = _contained_file(directory, name)
        actual, _ = sha256_file(path, max_bytes=2 * 1024 * 1024)
        if actual != expected:
            raise ArtifactError("AI evidence manifest digest mismatch")
        loaded.append(_load_run_manifest(directory, name))
    return tuple(loaded)


def _load_run_manifest(root: Path, name: str) -> dict[str, Any]:
    path = _contained_file(root, name)
    data = _exact(load_json(path, max_bytes=2 * 1024 * 1024), _RUN_KEYS, "AI run evidence")
    if (
        data["schema_version"] != _SCHEMA_VERSION
        or data["kind"] != "ai-run-evidence"
    ):
        raise ArtifactError("unsupported AI run evidence")
    if not isinstance(data["profile"], dict) or not isinstance(data["result"], dict):
        raise ArtifactError("AI profile/result evidence must be objects")
    if not isinstance(data["runtime"], dict):
        raise ArtifactError("AI runtime evidence must be an object")
    tree = data["repository_tree_sha"]
    if tree and (
        not isinstance(tree, str)
        or not re.fullmatch(r"[0-9a-f]{40}", tree)
    ):
        raise ArtifactError("AI repository tree SHA is invalid")
    for key, limit in (
        ("prompt", _MAX_PROMPT_BYTES),
        ("stdout", _MAX_TRANSCRIPT_BYTES),
        ("stderr", _MAX_TRANSCRIPT_BYTES),
    ):
        _verify_file_ref(root, data[key], key, limit)
    return data


def _write_text(root: Path, name: str, value: str, limit: int) -> dict[str, Any]:
    payload = value.encode("utf-8")
    if len(payload) > limit:
        raise ArtifactError(f"AI evidence {name} exceeds {limit} bytes")
    path = root / name
    temporary = root / f".{name}.{os.getpid()}.tmp"
    try:
        temporary.write_bytes(payload)
        temporary.replace(path)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise ArtifactError(f"cannot write AI evidence {name}: {exc}") from exc
    return {
        "file": name,
        "sha256": sha256_bytes(payload),
        "bytes": len(payload),
    }


def _verify_file_ref(
    root: Path,
    raw: Any,
    label: str,
    limit: int,
) -> None:
    ref = _exact(raw, _FILE_KEYS, f"AI {label} reference")
    path = _contained_file(root, ref["file"])
    digest = ref["sha256"]
    size = ref["bytes"]
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ArtifactError(f"AI {label} digest is invalid")
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or not 0 <= size <= limit
    ):
        raise ArtifactError(f"AI {label} size is invalid")
    actual_digest, actual_size = sha256_file(path, max_bytes=limit)
    if (actual_digest, actual_size) != (digest, size):
        raise ArtifactError(f"AI {label} content does not match evidence")


def _contained_file(root: Path, name: Any) -> Path:
    if (
        not isinstance(name, str)
        or not name
        or "/" in name
        or "\\" in name
    ):
        raise ArtifactError("AI evidence file name is invalid")
    path = root / name
    if path.is_symlink() or not path.is_file() or path.resolve().parent != root:
        raise ArtifactError("AI evidence file is not a contained regular file")
    return path


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
