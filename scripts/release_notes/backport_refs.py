"""Recover the original PR of a backported commit for release-note attribution.

Reads the ``## Applied`` table from sweep commits, the ``(cherry picked from
commit <sha>)`` trailer from -x picks, and the backport branch name to trace a
range commit back to its source PR.
"""

from __future__ import annotations

import re

# Matches the ``(cherry picked from commit <sha>)`` trailer git appends with -x.
_CHERRY_PICK_TRAILER_RE = re.compile(
    r"(?im)^[ \t]*\(cherry picked from commit ([0-9a-f]{7,40})\)[ \t]*$"
)

# Matches a "[Backport <branch>] ..." PR title prefix.
_BACKPORT_TITLE_RE = re.compile(r"^\s*\[Backport\b", re.IGNORECASE)

# Captures and strips the "[Backport <branch>] " prefix to reveal the source title.
_BACKPORT_TITLE_PREFIX_RE = re.compile(r"^\s*\[Backport\b[^\]]*\]\s*", re.IGNORECASE)

# Some manually authored backport PRs append the source PR to the copied title.
_BACKPORT_SOURCE_SUFFIX_RE = re.compile(r"\s+\(#(\d+)\)\s*$")

# Manual backports sometimes identify the source only with a first-line
# ``backport of (https://github.com/owner/repo/pull/N)`` statement.
_BACKPORT_OF_BODY_RE = re.compile(
    r"(?im)^\s*backport(?:ed)?\s+of\s+\(?\s*"
    r"https://github\.com/[^/\s)]+/[^/\s)]+/pull/(\d+)"
    r"(?:[/?#][^\s)]*)?\s*\)?\s*[.!]?\s*$"
)

# A manually assembled multi-PR backport can identify its sources on the first
# non-empty line (for example, ``Backports #1826, #2750, and #3846 to 7.2``).
# Keep this deliberately narrow: arbitrary PR references elsewhere in a body are
# discussion, not attribution evidence.
_BACKPORTS_BODY_LINE_RE = re.compile(r"^\s*backports?\b", re.IGNORECASE)
_INLINE_PR_RE = re.compile(r"#(\d+)\b")

# Matches a PR reference cell: "#123" or "[#123](url)".
_PR_CELL_RE = re.compile(r"^(?:\[)?#(\d+)(?:\]\([^)]*\))?$")

# Matches "backport/<source_pr>-to-<branch>" (optionally "agent/" prefixed).
_BACKPORT_BRANCH_RE = re.compile(r"^(?:agent/)?backport/(\d+)-to-")


def is_backport_title(title: str) -> bool:
    """True if *title* has a ``[Backport ...]`` prefix."""
    return bool(_BACKPORT_TITLE_RE.match(title))


def source_title_from_backport_title(title: str) -> str | None:
    """Strip the ``[Backport <branch>] `` prefix and return the source title, or None."""
    stripped = _BACKPORT_TITLE_PREFIX_RE.sub("", title or "")
    if stripped == (title or ""):  # no prefix matched
        return None
    stripped = stripped.strip()
    return stripped or None


def source_pr_from_backport_title(title: str) -> int | None:
    """Return ``N`` from a manual ``[Backport ...] Title (#N)`` PR title."""
    source_title = source_title_from_backport_title(title)
    if source_title is None:
        return None
    match = _BACKPORT_SOURCE_SUFFIX_RE.search(source_title)
    return int(match.group(1)) if match else None


def source_pr_from_backport_body(body: str) -> int | None:
    """Return ``N`` from a standalone ``backport of <GitHub PR URL>`` line."""
    match = _BACKPORT_OF_BODY_RE.search(body or "")
    return int(match.group(1)) if match else None


def source_prs_from_backport_body(body: str) -> tuple[int, ...]:
    """Return ordered ``#N`` sources from an explicit leading Backport(s) line.

    This handles container PRs that intentionally carry several source changes.
    A single source is returned too, but callers that expand containers should
    require multiple entries plus corroborating commit subjects.
    """
    first_line = next((line.strip() for line in (body or "").splitlines() if line.strip()), "")
    if not _BACKPORTS_BODY_LINE_RE.match(first_line):
        return ()
    return tuple(dict.fromkeys(int(number) for number in _INLINE_PR_RE.findall(first_line)))


def cherry_pick_source_shas(commit_message: str) -> list[str]:
    """Return source SHAs from ``(cherry picked from commit <sha>)`` trailers.

    Returned in file order (oldest hop first). Empty list if no trailer present.
    """
    return _CHERRY_PICK_TRAILER_RE.findall(commit_message)


def _markdown_section(body: str, heading: str) -> str:
    """Return the body of the ``## <heading>`` section, or ``""`` if absent."""
    pattern = re.compile(
        rf"(?ims)^##\s+{re.escape(heading)}\s*$([\s\S]*?)(?=^##\s+|\Z)"
    )
    match = pattern.search(body)
    return match.group(1) if match else ""


# Matches a Revert-titled manifest row: the sweep cherry-picked a revert of the
# named PR, so the PR's change is absent from (not shipped by) this range.
_REVERT_TITLE_RE = re.compile(r"^\s*revert\b", re.IGNORECASE)


def _applied_rows(body: str) -> list[tuple[int, str]]:
    """Return ``(source_pr, title)`` for each row of the ``## Applied`` table.

    Title is "" when the table has no Title column.
    """
    applied = _markdown_section(body, "Applied")
    if not applied:
        return []
    rows: list[str] = []
    for line in applied.splitlines():
        if line.lstrip().startswith("|"):
            rows.append(line)
        elif rows:
            rows[-1] += " " + line.strip()  # fold wrapped cell

    column: int | None = None
    title_column = 1  # fallback: sweep lists title second
    parsed: list[tuple[int, str]] = []
    for row in rows:
        cells = [cell.strip() for cell in row.strip().strip("|").split("|")]
        if column is None:
            for index, cell in enumerate(cells):
                if cell.strip().lower() == "source pr":
                    column = index
                    break
            else:
                column = 0  # fallback: sweep lists source PR first
            for index, cell in enumerate(cells):
                if cell.strip().lower() == "title":
                    title_column = index
                    break
            if any(cell.strip().lower() == "source pr" for cell in cells):
                continue  # skip the header row
        if all(set(cell) <= set("-: ") for cell in cells if cell):
            continue  # separator row (|---|---|)
        if column < len(cells):
            match = _PR_CELL_RE.match(cells[column])
            if match:
                title = cells[title_column] if title_column < len(cells) else ""
                parsed.append((int(match.group(1)), title))
    return parsed


def applied_source_prs_from_body(body: str) -> set[int]:
    """Extract source PR numbers from the ``## Applied`` markdown table in *body*.

    Rows whose Title begins with ``Revert`` are excluded: the sweep shipped a
    revert of that PR, so attributing the original PR's change to this range
    would note a change the range does not contain. Those rows are available via
    :func:`applied_revert_source_prs_from_body` for maintainer review.
    """
    return {
        number
        for number, title in _applied_rows(body)
        if not _REVERT_TITLE_RE.match(title)
    }


def applied_revert_source_prs_from_body(body: str) -> dict[int, str]:
    """Return ``{source_pr: title}`` for Revert-titled ``## Applied`` rows."""
    return {
        number: title
        for number, title in _applied_rows(body)
        if _REVERT_TITLE_RE.match(title)
    }


def summary_source_pr_from_body(body: str) -> int | None:
    """Return the source PR number from the ``## Backport Summary`` table, or None."""
    cell = _summary_value_cell(body, "source pr")
    if cell is None:
        return None
    match = _PR_CELL_RE.match(cell)
    return int(match.group(1)) if match else None


def summary_source_title_from_body(body: str) -> str | None:
    """Return the source title from the ``## Backport Summary`` table, or None."""
    cell = _summary_value_cell(body, "source title")
    return cell or None


def _summary_value_cell(body: str, label: str) -> str | None:
    """Return the value cell for *label* in the ``## Backport Summary`` table, or None."""
    summary = _markdown_section(body, "Backport Summary")
    if not summary:
        return None
    rows: list[str] = []
    for line in summary.splitlines():
        if line.lstrip().startswith("|"):
            rows.append(line)
        elif rows:
            rows[-1] += " " + line.strip()  # fold wrapped cell
    for row in rows:
        cells = _split_table_row(row)
        if len(cells) < 2 or cells[0].strip().lower() != label:
            continue
        return cells[1]
    return None


def _split_table_row(row: str) -> list[str]:
    """Split a markdown table row on unescaped ``|`` and unescape ``\\|`` in cells."""
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


def source_pr_from_branch(head_ref: str) -> int | None:
    """Extract the source PR number from a per-PR backport branch name, or None."""
    if not head_ref:
        return None
    match = _BACKPORT_BRANCH_RE.match(head_ref)
    return int(match.group(1)) if match else None
