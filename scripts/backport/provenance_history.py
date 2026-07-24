"""Revert-aware evaluation of backport provenance in Git history."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

from scripts.backport.provenance import (
    ManifestEntry,
    ProvenanceAction,
    ProvenanceRecord,
    inspect_manifest,
    inspect_trailers,
    normalize_title,
)

_TRAILING_PR_RE = re.compile(r"\s*\(#(\d+)\)\s*$")
_REVERT_TARGET_RE = re.compile(
    r"(?im)^This reverts commit ([0-9a-f]{7,64})"
    r"(?:,\s*reversing\s+changes made to [0-9a-f]{7,64})?\.\s*$"
)
_PR_CELL_RE = re.compile(r"^(?:\[)?#(\d+)(?:\]\([^)]*\))?$")


@dataclass(frozen=True)
class AppliedBackport:
    source_pr: int
    title: str


@dataclass(frozen=True)
class _Commit:
    sha: str
    parents: tuple[str, ...]
    message: str


@dataclass(frozen=True, order=True)
class _PartKey:
    source_pr: int
    series: str
    part: int
    parts: int


@dataclass
class _PartInfo:
    title: str
    first_seen: int
    last_seen: int


@dataclass(frozen=True)
class _DirectAttribution:
    effect: dict[_PartKey, int]
    origin: str
    suppress_message_revert: bool = False


@dataclass
class _HistoryEvaluation:
    commits: list[_Commit]
    intrinsic: dict[str, dict[_PartKey, int]]
    patch_effect: dict[str, dict[_PartKey, int]]
    state: dict[_PartKey, int]
    info: dict[_PartKey, _PartInfo]


def scan_applied_backports(
    repo_dir: str,
    revision: str,
) -> list[AppliedBackport]:
    """Return effectively applied source PRs after evaluating history reverts."""

    evaluation = _evaluate_history(repo_dir, revision)
    active_series: dict[int, list[tuple[str, int, str]]] = defaultdict(list)
    grouped: dict[tuple[int, str, int], dict[int, int]] = defaultdict(dict)

    for key, balance in evaluation.state.items():
        grouped[(key.source_pr, key.series, key.parts)][key.part] = balance

    for (source_pr, series, parts), balances in grouped.items():
        if any(balances.get(part, 0) <= 0 for part in range(1, parts + 1)):
            continue
        keys = [
            _PartKey(source_pr, series, part, parts)
            for part in range(1, parts + 1)
        ]
        infos = [
            evaluation.info[key]
            for key in keys
            if key in evaluation.info
        ]
        if not infos:
            continue
        first_seen = min(info.first_seen for info in infos)
        latest_info = max(infos, key=lambda info: info.last_seen)
        active_series[source_pr].append(
            (series, first_seen, latest_info.title)
        )

    applied: list[tuple[int, int, str]] = []
    for source_pr, series_entries in active_series.items():
        latest_series = max(series_entries, key=lambda item: item[1])
        order = min(item[1] for item in series_entries)
        applied.append((order, source_pr, latest_series[2]))
    applied.sort(key=lambda item: (item[0], item[1]))
    return [
        AppliedBackport(source_pr=source_pr, title=title)
        for _, source_pr, title in applied
    ]


def inverse_records_for_commit(
    repo_dir: str,
    revision: str,
    commit_sha: str,
) -> tuple[ProvenanceRecord, ...]:
    """Return durable inverse records for the selected commit's patch effect."""

    evaluation = _evaluate_history(repo_dir, revision)
    resolved = _resolve_history_sha(commit_sha, evaluation.commits)
    if resolved is None:
        return ()
    effect = evaluation.patch_effect.get(resolved, {})
    records: list[ProvenanceRecord] = []
    for key, value in sorted(effect.items()):
        if value == 0:
            continue
        info = evaluation.info.get(key)
        title = normalize_title(
            info.title if info else "",
            key.source_pr,
        )
        action: ProvenanceAction = "revert" if value > 0 else "apply"
        for _ in range(abs(value)):
            records.append(
                ProvenanceRecord(
                    source_pr=key.source_pr,
                    title=title,
                    series=key.series,
                    part=key.part,
                    parts=key.parts,
                    action=action,
                    kind="inverse",
                )
            )
    return tuple(records)


def _evaluate_history(repo_dir: str, revision: str) -> _HistoryEvaluation:
    commits = _read_history(repo_dir, revision)
    ancestor_bits: dict[str, int] = {}
    intrinsic: dict[str, dict[_PartKey, int]] = {}
    patch_effect: dict[str, dict[_PartKey, int]] = {}
    info: dict[_PartKey, _PartInfo] = {}

    for index, commit in enumerate(commits):
        bits = 1 << index
        for parent in commit.parents:
            bits |= ancestor_bits.get(parent, 0)
        ancestor_bits[commit.sha] = bits

        direct = _direct_attribution(commit, index, info)
        direct_effect = dict(direct.effect)

        if len(commit.parents) > 1:
            first_parent_bits = ancestor_bits.get(commit.parents[0], 0)
            side_bits = 0
            for parent in commit.parents[1:]:
                side_bits |= ancestor_bits.get(parent, 0)
            side_bits &= ~first_parent_bits
            side_effect: dict[_PartKey, int] = {}
            while side_bits:
                lowest = side_bits & -side_bits
                side_index = lowest.bit_length() - 1
                side_bits ^= lowest
                side_sha = commits[side_index].sha
                _merge_effect(side_effect, intrinsic.get(side_sha, {}))

            side_prs = {
                key.source_pr
                for key, value in side_effect.items()
                if value != 0
            }
            if side_prs:
                if direct.origin in {"subject", "summary"}:
                    direct_effect = {}
                elif direct.origin in {"manifest", "table"}:
                    direct_effect = {
                        key: value
                        for key, value in direct_effect.items()
                        if key.source_pr not in side_prs
                    }
            intrinsic[commit.sha] = direct_effect
            combined = dict(side_effect)
            _merge_effect(combined, direct_effect)
            patch_effect[commit.sha] = combined
            continue

        current = direct_effect
        if not direct.suppress_message_revert:
            target = _revert_target(commit.message)
            resolved_target = (
                _resolve_history_sha(target, commits[:index])
                if target is not None
                else None
            )
            if resolved_target is not None:
                _merge_effect(
                    current,
                    patch_effect.get(resolved_target, {}),
                    multiplier=-1,
                )
        intrinsic[commit.sha] = current
        patch_effect[commit.sha] = dict(current)

    state: dict[_PartKey, int] = {}
    for commit in commits:
        _merge_effect(state, intrinsic.get(commit.sha, {}))
    return _HistoryEvaluation(
        commits=commits,
        intrinsic=intrinsic,
        patch_effect=patch_effect,
        state=state,
        info=info,
    )


def _direct_attribution(
    commit: _Commit,
    index: int,
    info: dict[_PartKey, _PartInfo],
) -> _DirectAttribution:
    parsed_records = inspect_trailers(commit.message)
    if parsed_records.status == "valid":
        effect: dict[_PartKey, int] = {}
        for record in parsed_records.records:
            key = _PartKey(
                record.source_pr,
                record.series,
                record.part,
                record.parts,
            )
            effect[key] = effect.get(key, 0) + (
                1 if record.action == "apply" else -1
            )
            _remember_info(info, key, record.title, index)
        return _DirectAttribution(
            effect=effect,
            origin="trailer",
            suppress_message_revert=any(
                record.kind == "inverse"
                for record in parsed_records.records
            ),
        )
    if parsed_records.status == "invalid":
        return _DirectAttribution({}, "invalid")

    parsed_manifest = inspect_manifest(commit.message)
    if parsed_manifest.status == "valid":
        effect = _entries_effect(
            parsed_manifest.entries,
            kind="manifest",
            index=index,
            info=info,
        )
        return _DirectAttribution(effect, "manifest")
    if parsed_manifest.status == "invalid":
        return _DirectAttribution({}, "invalid")

    table_entries = _applied_table_entries(commit.message)
    if table_entries:
        return _DirectAttribution(
            _entries_effect(
                table_entries,
                kind="table",
                index=index,
                info=info,
            ),
            "table",
        )

    summary = _backport_summary_entry(commit.message)
    if summary is not None:
        return _DirectAttribution(
            _entries_effect(
                (summary,),
                kind="summary",
                index=index,
                info=info,
            ),
            "summary",
        )

    subject = commit.message.splitlines()[0].strip() if commit.message else ""
    matched = _TRAILING_PR_RE.search(subject)
    if matched:
        source_pr = int(matched.group(1))
        title = _TRAILING_PR_RE.sub("", subject).strip() or subject
        entry = ManifestEntry(source_pr, title)
        return _DirectAttribution(
            _entries_effect(
                (entry,),
                kind="subject",
                index=index,
                info=info,
            ),
            "subject",
        )
    return _DirectAttribution({}, "none")


def _entries_effect(
    entries: Sequence[ManifestEntry],
    *,
    kind: str,
    index: int,
    info: dict[_PartKey, _PartInfo],
) -> dict[_PartKey, int]:
    effect: dict[_PartKey, int] = {}
    for entry in entries:
        title = normalize_title(entry.title, entry.source_pr)
        key = _PartKey(
            source_pr=entry.source_pr,
            series=_synthetic_series(kind, entry.source_pr, title),
            part=1,
            parts=1,
        )
        effect[key] = effect.get(key, 0) + 1
        _remember_info(
            info,
            key,
            title,
            index,
        )
    return effect


def _remember_info(
    info: dict[_PartKey, _PartInfo],
    key: _PartKey,
    title: str,
    index: int,
) -> None:
    existing = info.get(key)
    if existing is None:
        info[key] = _PartInfo(title=title, first_seen=index, last_seen=index)
        return
    existing.title = title or existing.title
    existing.last_seen = index


def _merge_effect(
    target: dict[_PartKey, int],
    source: dict[_PartKey, int],
    *,
    multiplier: int = 1,
) -> None:
    for key, value in source.items():
        updated = target.get(key, 0) + value * multiplier
        if updated:
            target[key] = updated
        else:
            target.pop(key, None)


def _applied_table_entries(message: str) -> tuple[ManifestEntry, ...]:
    section = _markdown_section(message, "Applied")
    if not section:
        return ()
    rows = _markdown_rows(section)
    source_column: int | None = None
    title_column: int | None = None
    entries: list[ManifestEntry] = []
    seen: set[int] = set()

    for row in rows:
        cells = _split_markdown_row(row)
        if source_column is None:
            lowered = [cell.strip().lower() for cell in cells]
            if "source pr" in lowered:
                source_column = lowered.index("source pr")
                title_column = (
                    lowered.index("title")
                    if "title" in lowered
                    else None
                )
                continue
            source_column = 0
            title_column = 1 if len(cells) > 1 else None
        if all(set(cell) <= {"-", ":", " "} for cell in cells if cell):
            continue
        if source_column >= len(cells):
            continue
        matched = _PR_CELL_RE.fullmatch(cells[source_column].strip())
        if not matched:
            continue
        source_pr = int(matched.group(1))
        if source_pr in seen:
            continue
        title = (
            cells[title_column].strip()
            if title_column is not None and title_column < len(cells)
            else f"Source PR #{source_pr}"
        )
        seen.add(source_pr)
        entries.append(
            ManifestEntry(
                source_pr,
                normalize_title(title, source_pr),
            )
        )
    return tuple(entries)


def _backport_summary_entry(message: str) -> ManifestEntry | None:
    section = _markdown_section(message, "Backport Summary")
    if not section:
        return None
    source_pr: int | None = None
    title = ""
    for row in _markdown_rows(section):
        cells = _split_markdown_row(row)
        if len(cells) < 2:
            continue
        label = cells[0].strip().lower()
        if label == "source pr":
            matched = _PR_CELL_RE.fullmatch(cells[1].strip())
            if matched:
                source_pr = int(matched.group(1))
        elif label == "source title":
            title = cells[1].strip()
    if source_pr is None:
        return None
    return ManifestEntry(
        source_pr,
        normalize_title(title, source_pr),
    )


def _markdown_section(body: str, heading: str) -> str:
    pattern = re.compile(
        rf"(?ims)^##\s+{re.escape(heading)}\s*$"
        rf"([\s\S]*?)(?=^##\s+|\Z)"
    )
    matched = pattern.search(body)
    return matched.group(1) if matched else ""


def _markdown_rows(section: str) -> list[str]:
    rows: list[str] = []
    for line in section.splitlines():
        if line.lstrip().startswith("|"):
            rows.append(line)
        elif rows:
            rows[-1] += " " + line.strip()
    return rows


def _split_markdown_row(row: str) -> list[str]:
    text = row.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if char == "\\" and not escaped:
            escaped = True
            current.append(char)
            continue
        if char == "|" and not escaped:
            cells.append("".join(current).strip().replace("\\|", "|"))
            current = []
            continue
        current.append(char)
        escaped = False
    cells.append("".join(current).strip().replace("\\|", "|"))
    return cells


def _synthetic_series(kind: str, source_pr: int, title: str) -> str:
    return hashlib.sha256(
        (
            f"{kind}\0{source_pr}\0{title}"
        ).encode("utf-8")
    ).hexdigest()


def _revert_target(message: str) -> str | None:
    matched = _REVERT_TARGET_RE.search(message)
    return matched.group(1) if matched else None


def _resolve_history_sha(
    value: str,
    commits: Sequence[_Commit],
) -> str | None:
    exact = [commit.sha for commit in commits if commit.sha == value]
    if exact:
        return exact[0]
    matches = [commit.sha for commit in commits if commit.sha.startswith(value)]
    return matches[0] if len(matches) == 1 else None


def _read_history(repo_dir: str, revision: str) -> list[_Commit]:
    result = subprocess.run(
        ["git", "rev-list", "--reverse", "--topo-order", "--parents", revision],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"could not read backport history {revision!r}: "
            + ((result.stderr or "").strip()[:300] or "git rev-list failed")
        )
    graph: list[tuple[str, tuple[str, ...]]] = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if fields:
            graph.append((fields[0], tuple(fields[1:])))
    messages = _read_commit_messages(
        repo_dir,
        [sha for sha, _ in graph],
    )
    return [
        _Commit(sha=sha, parents=parents, message=messages[sha])
        for sha, parents in graph
    ]


def _read_commit_messages(
    repo_dir: str,
    shas: Sequence[str],
) -> dict[str, str]:
    if not shas:
        return {}
    result = subprocess.run(
        ["git", "cat-file", "--batch"],
        cwd=repo_dir,
        input="".join(f"{sha}\n" for sha in shas).encode("ascii"),
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "could not read backport commit messages: "
            + (
                os.fsdecode(result.stderr).strip()[:300]
                or "git cat-file failed"
            )
        )

    output = result.stdout
    offset = 0
    messages: dict[str, str] = {}
    for expected_sha in shas:
        header_end = output.find(b"\n", offset)
        if header_end < 0:
            raise RuntimeError("truncated git cat-file response")
        header = output[offset:header_end].decode("ascii", errors="replace")
        fields = header.split()
        if len(fields) != 3 or fields[0] != expected_sha or fields[1] != "commit":
            raise RuntimeError(f"unexpected git cat-file response: {header!r}")
        size = int(fields[2])
        start = header_end + 1
        end = start + size
        raw_commit = output[start:end]
        if end >= len(output) or output[end : end + 1] != b"\n":
            raise RuntimeError("malformed git cat-file commit framing")
        offset = end + 1
        _, separator, raw_message = raw_commit.partition(b"\n\n")
        if not separator:
            raw_message = b""
        messages[expected_sha] = raw_message.decode(
            "utf-8",
            errors="replace",
        )
    return messages
