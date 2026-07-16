"""Canonical source provenance for validated backport commits."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import subprocess
from typing import Any, Callable

PROVENANCE_VERSION = 1
PROVENANCE_KIND = "valkey-backport-source-provenance"
PAYLOAD_PREFIX = "Valkey-Backport-Provenance: "
DIGEST_PREFIX = "Valkey-Backport-Provenance-SHA256: "

_MAX_PAYLOAD_BYTES = 16 * 1024
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_BRANCH_RE = re.compile(r"^(?!-)(?!.*\.\.)(?!.*//)[A-Za-z0-9._/-]+$")
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KEYS = {
    "version",
    "kind",
    "repository",
    "target_branch",
    "source_pr_number",
    "source_merge_commit",
}

RunProcess = Callable[..., subprocess.CompletedProcess[str]]


class ProvenanceError(ValueError):
    """Raised when a provenance record is malformed or cannot be attached."""


def build_provenance(
    *,
    repository: str,
    target_branch: str,
    source_pr_number: int,
    source_merge_commit: str,
) -> dict[str, Any]:
    return validate_provenance(
        {
            "version": PROVENANCE_VERSION,
            "kind": PROVENANCE_KIND,
            "repository": repository,
            "target_branch": target_branch,
            "source_pr_number": source_pr_number,
            "source_merge_commit": source_merge_commit,
        }
    )


def format_provenance(record: dict[str, Any]) -> str:
    normalized = validate_provenance(record)
    payload = _canonical_json(normalized)
    encoded = base64.urlsafe_b64encode(payload).decode("ascii")
    digest = hashlib.sha256(payload).hexdigest()
    return f"{PAYLOAD_PREFIX}{encoded}\n{DIGEST_PREFIX}{digest}"


def parse_provenance_records(message: str) -> list[dict[str, Any]]:
    payloads = [line[len(PAYLOAD_PREFIX) :].strip() for line in message.splitlines() if line.startswith(PAYLOAD_PREFIX)]
    digests = [line[len(DIGEST_PREFIX) :].strip() for line in message.splitlines() if line.startswith(DIGEST_PREFIX)]
    if not payloads and not digests:
        return []
    if len(payloads) != len(digests):
        raise ProvenanceError("provenance payload and digest counts differ")

    records: list[dict[str, Any]] = []
    for encoded, expected_digest in zip(payloads, digests):
        if not _SHA256_RE.fullmatch(expected_digest):
            raise ProvenanceError("provenance digest is invalid")
        try:
            payload = base64.b64decode(encoded, altchars=b"-_", validate=True)
        except (TypeError, ValueError) as exc:
            raise ProvenanceError("provenance payload is not valid base64url") from exc
        if not payload or len(payload) > _MAX_PAYLOAD_BYTES:
            raise ProvenanceError("provenance payload is empty or oversized")
        if hashlib.sha256(payload).hexdigest() != expected_digest:
            raise ProvenanceError("provenance payload digest does not match")
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, ValueError) as exc:
            raise ProvenanceError("provenance payload is not valid JSON") from exc
        normalized = validate_provenance(raw)
        if _canonical_json(normalized) != payload:
            raise ProvenanceError("provenance payload is not canonical JSON")
        records.append(normalized)
    return records


def validate_provenance(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ProvenanceError("provenance must be an object")
    actual = set(raw)
    if actual != _KEYS:
        raise ProvenanceError(
            f"provenance keys invalid: unknown={sorted(actual - _KEYS)}, missing={sorted(_KEYS - actual)}"
        )
    if raw["version"] != PROVENANCE_VERSION or raw["kind"] != PROVENANCE_KIND:
        raise ProvenanceError("unsupported provenance version or kind")

    repository = raw["repository"]
    if not isinstance(repository, str) or not _REPOSITORY_RE.fullmatch(repository):
        raise ProvenanceError("provenance repository is invalid")
    target_branch = raw["target_branch"]
    if not isinstance(target_branch, str) or not _BRANCH_RE.fullmatch(target_branch):
        raise ProvenanceError("provenance target branch is invalid")
    source_pr_number = raw["source_pr_number"]
    if not isinstance(source_pr_number, int) or isinstance(source_pr_number, bool) or source_pr_number <= 0:
        raise ProvenanceError("provenance source PR number is invalid")
    source_merge_commit = raw["source_merge_commit"]
    if not isinstance(source_merge_commit, str) or not _SHA1_RE.fullmatch(source_merge_commit):
        raise ProvenanceError("provenance source merge commit is invalid")

    return {
        "version": PROVENANCE_VERSION,
        "kind": PROVENANCE_KIND,
        "repository": repository,
        "target_branch": target_branch,
        "source_pr_number": source_pr_number,
        "source_merge_commit": source_merge_commit,
    }


def attach_provenance_to_head(
    repo_dir: str,
    *,
    repository: str,
    target_branch: str,
    source_pr_number: int,
    source_merge_commit: str,
    run_process: RunProcess = subprocess.run,
) -> tuple[dict[str, Any], str, str]:
    """Amend HEAD with provenance without changing its tree."""
    record = build_provenance(
        repository=repository,
        target_branch=target_branch,
        source_pr_number=source_pr_number,
        source_merge_commit=source_merge_commit,
    )
    message = _git_output(repo_dir, "show", "-s", "--format=%B", "HEAD", run_process=run_process)
    if PAYLOAD_PREFIX in message or DIGEST_PREFIX in message:
        raise ProvenanceError("commit message already contains reserved provenance markers")
    tree_before = _git_output(repo_dir, "rev-parse", "HEAD^{tree}", run_process=run_process).strip()
    commit_before = _git_output(repo_dir, "rev-parse", "HEAD", run_process=run_process).strip()
    amended_message = f"{message.rstrip()}\n\n{format_provenance(record)}"
    result = run_process(
        [
            "git",
            "commit",
            "--amend",
            "--no-gpg-sign",
            "-m",
            amended_message,
        ],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ProvenanceError(
            "could not attach backport provenance: "
            + ((result.stderr or result.stdout).strip()[:300] or "git commit failed")
        )
    tree_after = _git_output(repo_dir, "rev-parse", "HEAD^{tree}", run_process=run_process).strip()
    if tree_after != tree_before:
        raise ProvenanceError("provenance amendment changed the validated tree")
    commit_sha = _git_output(repo_dir, "rev-parse", "HEAD", run_process=run_process).strip()
    if not _SHA1_RE.fullmatch(commit_sha):
        raise ProvenanceError("provenance amendment produced an invalid commit SHA")
    return record, commit_before, commit_sha


def _git_output(
    repo_dir: str,
    *args: str,
    run_process: RunProcess,
) -> str:
    result = run_process(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ProvenanceError(
            f"git {' '.join(args)} failed: " + ((result.stderr or result.stdout).strip()[:300] or "git command failed")
        )
    return result.stdout


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
