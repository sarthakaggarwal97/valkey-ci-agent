"""Naming conventions and conflict detection helpers."""

from __future__ import annotations

import re
from collections.abc import Iterable

_CONFLICT_MARKERS = re.compile(
    r"^(<{7} \S|={7}$|>{7} \S|<{7}$|>{7}$)",
    re.MULTILINE,
)


def build_branch_name(source_pr_number: int, target_branch: str) -> str:
    return f"backport/{source_pr_number}-to-{target_branch}"


def pr_numbers_from_commit_subjects(subjects: list[str]) -> set[int]:
    """Source PR numbers from the *trailing* ``(#N)`` of each commit subject.

    Single source of truth for "which PRs does this commit history contain",
    shared by the sweep (to skip already-applied PRs) and mark-done (to verify
    a board item actually landed).

    Only the trailing ``(#N)`` identifies the PR a commit belongs to. An earlier
    ``(#N)`` in the subject is a reference, not the commit's own PR — e.g.
    ``Revert "... (#3544)" (#3756)`` is PR 3756, not 3544.
    """
    numbers: set[int] = set()
    for line in subjects:
        m = re.search(r"\(#(\d+)\)\s*$", line)
        if m:
            numbers.add(int(m.group(1)))
    return numbers


def pr_numbers_from_applied_tables(commit_messages: Iterable[str]) -> set[int]:
    """Source PR numbers from ``## Applied`` tables in backport commit bodies.

    Squash-merged sweep PRs lose the individual cherry-pick subjects on the
    release branch. The sweep body is preserved as the squash commit message,
    so the structured ``## Applied`` table is the durable source-PR signal.

    Only the ``Source PR`` column is read. References in titles, details, prose,
    or ``## Needs attention`` rows are ignored.
    """
    numbers: set[int] = set()
    for message in commit_messages:
        applied_section = _markdown_section(message, "Applied")
        if applied_section:
            numbers.update(_pr_numbers_from_table_cells(applied_section))
    return numbers


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


def _markdown_section(body: str, heading: str) -> str:
    pattern = re.compile(
        rf"(?ims)^##\s+{re.escape(heading)}\s*$([\s\S]*?)(?=^##\s+|\Z)"
    )
    match = pattern.search(body)
    return match.group(1) if match else ""


def _pr_numbers_from_table_cells(markdown: str) -> set[int]:
    """Source PR numbers from the ``Source PR`` column of a markdown table.

    A logical row begins at a line starting with ``|`` and absorbs wrapped text
    until the next row. If no ``Source PR`` header is present, the first column
    is used because sweep tables list source PRs first.
    """
    rows: list[str] = []
    for line in markdown.splitlines():
        if line.lstrip().startswith("|"):
            rows.append(line)
        elif rows:
            rows[-1] += " " + line.strip()

    pr_cell = re.compile(r"^(?:\[)?#(\d+)(?:\]\([^)]*\))?$")
    column: int | None = None
    numbers: set[int] = set()
    for row in rows:
        cells = [cell.strip() for cell in row.strip().strip("|").split("|")]
        if column is None:
            for index, cell in enumerate(cells):
                if _normalize(cell) == "source pr":
                    column = index
                    break
            else:
                column = 0
            if any(_normalize(cell) == "source pr" for cell in cells):
                continue
        if all(set(cell) <= set("-: ") for cell in cells if cell):
            continue
        if column < len(cells):
            match = pr_cell.match(cells[column])
            if match:
                numbers.add(int(match.group(1)))
    return numbers


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()
