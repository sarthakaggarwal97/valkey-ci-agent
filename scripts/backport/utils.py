"""Naming conventions and conflict detection helpers."""

from __future__ import annotations

import re

_CONFLICT_MARKERS = re.compile(
    r"^(<{7} \S|={7}$|>{7} \S|<{7}$|>{7}$)",
    re.MULTILINE,
)
_TRAILER_LINE = re.compile(r"^[A-Za-z0-9-]+:\s*.*$")
_MANUAL_BACKPORT_COMMIT_SUBJECT = re.compile(
    r"^\s*(?:\[(?:Backport\b[^\]]*|\d+\.\d+)\]\s+"
    r"|Backport\s+\d+\.\d+\s*:\s*).*"
    r"\(#(\d+)\)\s+\(#\d+\)\s*$",
    re.IGNORECASE,
)


def build_branch_name(source_pr_number: int, target_branch: str) -> str:
    return f"backport/{source_pr_number}-to-{target_branch}"


def pr_numbers_from_commit_subjects(subjects: list[str]) -> set[int]:
    """Source PR numbers from squash or standard GitHub merge subjects.

    Single source of truth for "which PRs does this commit history contain",
    shared by the sweep (to skip already-applied PRs) and mark-done (to verify
    a board item actually landed).

    A trailing ``(#N)`` identifies a squash commit's PR. An earlier
    ``(#N)`` in the subject is a reference, not the commit's own PR — e.g.
    ``Revert "... (#3544)" (#3756)`` is PR 3756, not 3544. Standard
    ``Merge pull request #N from ...`` subjects are also recognized.
    """
    numbers: set[int] = set()
    for line in subjects:
        m = re.search(r"\(#(\d+)\)\s*$", line)
        if m:
            numbers.add(int(m.group(1)))
            continue
        merge = re.match(r"Merge pull request #(\d+)\b", line, re.IGNORECASE)
        if merge:
            numbers.add(int(merge.group(1)))
    return numbers


def pr_numbers_from_commit_messages(messages: list[str]) -> set[int]:
    """Source PR numbers from subjects or durable backport trailers."""
    numbers: set[int] = set()
    for message in messages:
        lines = message.splitlines()
        if lines:
            numbers.update(pr_numbers_from_commit_subjects([lines[0]]))
        for line in _terminal_trailer_block(lines):
            trailer = re.match(
                r"Backport-Source-PR:\s*#?(\d+)\s*$",
                line,
                re.IGNORECASE,
            )
            if trailer:
                numbers.add(int(trailer.group(1)))
    return numbers


def source_pr_from_backport_commit_subject(subject: str) -> int | None:
    """Return the source PR from a release-prefixed backport commit subject.

    GitHub appends the backport PR number to a squash-merged title, producing
    subjects such as ``[9.0] Fix bug (#3548) (#4182)`` or
    ``Backport 8.1: Fix bug (#3430) (#3513)``. The first trailing number is the
    source PR and the second is the backport PR. Requiring a recognized
    release/backport prefix avoids treating an ordinary reference in subjects
    such as ``Revert "... (#3544)" (#3756)`` as shipped code.
    """
    match = _MANUAL_BACKPORT_COMMIT_SUBJECT.match(subject)
    return int(match.group(1)) if match else None


def _terminal_trailer_block(lines: list[str]) -> list[str]:
    """Return the final RFC-822-style trailer block, if structurally present."""
    end = len(lines)
    while end > 0 and not lines[end - 1].strip():
        end -= 1
    start = end
    while start > 0 and _TRAILER_LINE.match(lines[start - 1]):
        start -= 1
    if start == end:
        return []
    if start > 0 and lines[start - 1].strip():
        return []
    return lines[start:end]


def build_pr_title(source_pr_title: str, target_branch: str) -> str:
    return f"[Backport {target_branch}] {source_pr_title}"


def has_conflict_markers(content: str) -> bool:
    """Return ``True`` if *content* contains git conflict markers."""
    return bool(_CONFLICT_MARKERS.search(content))


def is_whitespace_only_conflict(target_content: str, source_content: str) -> bool:
    """Return ``True`` when the two contents differ only in whitespace."""
    return _strip_all_whitespace(target_content) == _strip_all_whitespace(source_content)


def _strip_all_whitespace(s: str) -> str:
    return re.sub(r"\s+", "", s)
