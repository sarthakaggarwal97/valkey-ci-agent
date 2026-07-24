"""Strict, versioned backport provenance contract."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal, Sequence

TRAILER_KEY = "Valkey-CI-Backport"
TRAILER_PREFIX = f"{TRAILER_KEY}: "
MANIFEST_MARKER = "valkey-ci-agent-backport-manifest"

_SERIES_RE = re.compile(r"^[0-9a-f]{64}$")
_MANIFEST_BLOCK_RE = re.compile(
    rf"<!-- {re.escape(MANIFEST_MARKER)}:v1\n([^\x00]*?)\n-->",
)
_ANY_MANIFEST_BLOCK_RE = re.compile(
    rf"\n*<!-- {re.escape(MANIFEST_MARKER)}:[\s\S]*?(?:-->|\Z)\n*",
)

ParseStatus = Literal["absent", "valid", "invalid"]
ProvenanceAction = Literal["apply", "revert"]
ProvenanceKind = Literal["candidate", "inverse"]


@dataclass(frozen=True)
class ProvenanceRecord:
    source_pr: int
    title: str
    series: str
    part: int
    parts: int
    action: ProvenanceAction = "apply"
    kind: ProvenanceKind = "candidate"


@dataclass(frozen=True)
class ManifestEntry:
    source_pr: int
    title: str


@dataclass(frozen=True)
class ParsedTrailerSet:
    status: ParseStatus
    records: tuple[ProvenanceRecord, ...] = ()


@dataclass(frozen=True)
class ParsedManifest:
    status: ParseStatus
    entries: tuple[ManifestEntry, ...] = ()


def candidate_series_id(
    repo_full_name: str,
    source_pr: int,
    merge_commit_sha: str | None,
    source_commit_shas: Sequence[str],
) -> str:
    """Return a stable identity for one immutable source pull request."""

    payload = {
        "merge_commit": merge_commit_sha or "",
        "repo": repo_full_name,
        "source_commits": list(source_commit_shas),
        "source_pr": source_pr,
        "v": 1,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def normalize_title(value: str, source_pr: int) -> str:
    printable = "".join(
        char if char.isprintable() else " "
        for char in str(value or "")
    )
    title = " ".join(printable.split())
    return (title or f"Source PR #{source_pr}")[:512]


def render_trailer(record: ProvenanceRecord) -> str:
    _validate_record(record)
    payload = {
        "action": record.action,
        "kind": record.kind,
        "part": record.part,
        "parts": record.parts,
        "series": record.series,
        "source_pr": record.source_pr,
        "title": record.title,
        "v": 1,
    }
    return TRAILER_PREFIX + json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def parse_trailers(message: str) -> tuple[ProvenanceRecord, ...]:
    """Parse a complete valid trailer set; malformed reserved data yields none."""

    parsed = inspect_trailers(message)
    return parsed.records if parsed.status == "valid" else ()


def inspect_trailers(message: str) -> ParsedTrailerSet:
    """Distinguish absent provenance from malformed reserved provenance."""

    reserved = [
        line
        for line in message.splitlines()
        if line.startswith(f"{TRAILER_KEY}:")
    ]
    if not reserved:
        return ParsedTrailerSet("absent")

    records: list[ProvenanceRecord] = []
    for line in reserved:
        if not line.startswith(TRAILER_PREFIX):
            return ParsedTrailerSet("invalid")
        try:
            payload = json.loads(line[len(TRAILER_PREFIX) :])
            record = _record_from_payload(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return ParsedTrailerSet("invalid")
        records.append(record)
    return ParsedTrailerSet("valid", tuple(records))


def message_with_records(
    message: bytes,
    records: Sequence[ProvenanceRecord],
) -> bytes:
    """Replace reserved trailers in a raw commit message."""

    retained = [
        line
        for line in message.splitlines()
        if not line.startswith(f"{TRAILER_KEY}:".encode("ascii"))
    ]
    base = b"\n".join(retained).rstrip(b"\r\n")
    trailers = "\n".join(render_trailer(record) for record in records).encode(
        "ascii"
    )
    return base + b"\n\n" + trailers + b"\n"


def render_manifest(entries: Sequence[ManifestEntry]) -> str:
    normalized = _normalize_manifest_entries(entries)
    payload = {
        "entries": [
            {"source_pr": entry.source_pr, "title": entry.title}
            for entry in normalized
        ],
        "v": 1,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"<!-- {MANIFEST_MARKER}:v1\n{encoded}\n-->"


def parse_manifest(message: str) -> tuple[ManifestEntry, ...]:
    """Parse one complete valid manifest; malformed reserved data yields none."""

    parsed = inspect_manifest(message)
    return parsed.entries if parsed.status == "valid" else ()


def inspect_manifest(message: str) -> ParsedManifest:
    """Distinguish an absent manifest from a malformed reserved block."""

    if f"<!-- {MANIFEST_MARKER}:" not in message:
        return ParsedManifest("absent")
    matches = list(_MANIFEST_BLOCK_RE.finditer(message))
    if len(matches) != 1:
        return ParsedManifest("invalid")
    complete_reserved_blocks = list(_ANY_MANIFEST_BLOCK_RE.finditer(message))
    if len(complete_reserved_blocks) != 1:
        return ParsedManifest("invalid")

    raw_payload = matches[0].group(1)
    if "\n" in raw_payload or "\r" in raw_payload:
        return ParsedManifest("invalid")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return ParsedManifest("invalid")
    if (
        not isinstance(payload, dict)
        or set(payload) != {"entries", "v"}
        or payload.get("v") != 1
        or type(payload.get("v")) is not int
        or not isinstance(payload.get("entries"), list)
        or len(payload["entries"]) > 10000
    ):
        return ParsedManifest("invalid")

    entries: list[ManifestEntry] = []
    seen: set[int] = set()
    for value in payload["entries"]:
        if not isinstance(value, dict) or set(value) != {"source_pr", "title"}:
            return ParsedManifest("invalid")
        source_pr = value["source_pr"]
        title = value["title"]
        if (
            type(source_pr) is not int
            or source_pr <= 0
            or source_pr in seen
            or not isinstance(title, str)
            or not title
            or len(title) > 512
            or any(char in title for char in ("\x00", "\n", "\r"))
        ):
            return ParsedManifest("invalid")
        seen.add(source_pr)
        entries.append(ManifestEntry(source_pr, title))
    return ParsedManifest("valid", tuple(entries))


def replace_manifest(body: str, entries: Sequence[ManifestEntry]) -> str:
    """Replace any prior manifest with one canonical current-state manifest."""

    without_manifest = _ANY_MANIFEST_BLOCK_RE.sub("\n", body).rstrip()
    manifest = render_manifest(entries)
    return f"{without_manifest}\n\n{manifest}" if without_manifest else manifest


def _record_from_payload(payload: object) -> ProvenanceRecord:
    if not isinstance(payload, dict) or set(payload) != {
        "action",
        "kind",
        "part",
        "parts",
        "series",
        "source_pr",
        "title",
        "v",
    }:
        raise ValueError("invalid provenance record shape")
    if payload["v"] != 1 or type(payload["v"]) is not int:
        raise ValueError("unsupported provenance version")
    record = ProvenanceRecord(
        source_pr=payload["source_pr"],
        title=payload["title"],
        series=payload["series"],
        part=payload["part"],
        parts=payload["parts"],
        action=payload["action"],
        kind=payload["kind"],
    )
    _validate_record(record)
    return record


def _validate_record(record: ProvenanceRecord) -> None:
    if type(record.source_pr) is not int or record.source_pr <= 0:
        raise ValueError("source_pr must be a positive integer")
    if not isinstance(record.title, str) or not record.title or len(record.title) > 512:
        raise ValueError("title must be a non-empty bounded string")
    if "\x00" in record.title or "\n" in record.title or "\r" in record.title:
        raise ValueError("title contains an invalid control character")
    if not isinstance(record.series, str) or not _SERIES_RE.fullmatch(record.series):
        raise ValueError("series must be a lowercase SHA-256 value")
    if type(record.part) is not int or type(record.parts) is not int:
        raise ValueError("part values must be integers")
    if not 1 <= record.part <= record.parts <= 1000:
        raise ValueError("invalid provenance part range")
    if record.action not in {"apply", "revert"}:
        raise ValueError("invalid provenance action")
    if record.kind not in {"candidate", "inverse"}:
        raise ValueError("invalid provenance kind")
    if record.kind == "candidate" and record.action != "apply":
        raise ValueError("candidate provenance can only apply")


def _normalize_manifest_entries(
    entries: Sequence[ManifestEntry],
) -> tuple[ManifestEntry, ...]:
    normalized: list[ManifestEntry] = []
    seen: set[int] = set()
    for entry in entries:
        if entry.source_pr in seen:
            continue
        normalized_entry = ManifestEntry(
            entry.source_pr,
            normalize_title(entry.title, entry.source_pr),
        )
        _validate_manifest_entry(normalized_entry)
        seen.add(entry.source_pr)
        normalized.append(normalized_entry)
    return tuple(normalized)


def _validate_manifest_entry(entry: ManifestEntry) -> None:
    if type(entry.source_pr) is not int or entry.source_pr <= 0:
        raise ValueError("manifest source_pr must be a positive integer")
    if not isinstance(entry.title, str) or not entry.title or len(entry.title) > 512:
        raise ValueError("manifest title must be a non-empty bounded string")
    if any(char in entry.title for char in ("\x00", "\n", "\r")):
        raise ValueError("manifest title contains an invalid control character")
