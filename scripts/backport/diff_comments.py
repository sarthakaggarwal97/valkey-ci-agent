"""Reconcile AI-resolution diffs as source-PR grouped comments.

The backport PR body stays a lean summary. Each source PR that needed AI
conflict resolution gets one PR comment containing all resolved files for that
source PR. Each file links to its native diff in the resolution commit's view
(``…/commit/<sha>#diff-<file>``) rather than inlining the diff, so the comment
carries no fenced diffs and cannot breach GitHub's comment size limit no matter
how large the conflict was. Comments are reconciled idempotently across re-runs
so a re-pushed backport edits the existing comment in place instead of posting
duplicates.

Identity is the source PR number. Older per-file markers from the first version
of this feature are recognized and collapsed into the new grouped comment, so
existing bot comments are not stranded when the format changes.
"""

from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass
from typing import Any

from scripts.backport.models import ResolutionResult
from scripts.common.github_client import retry_github_call

_MARKER_PREFIX = "valkey-ci-agent:ai-diff"

_MARKER_RE = re.compile(
    r"<!--\s*" + re.escape(_MARKER_PREFIX)
    + r'\s+source_pr="(?P<source_pr>\d+)"'
    + r'(?:\s+path="(?P<path>[^"]*)")?'
    + r'\s+sha="(?P<sha>[0-9a-f]{16})"\s*-->'
)

# Claude's rationale is the only free-form text in the comment; cap it so a
# runaway summary cannot bloat the body. Diffs are not inlined.
_MAX_SUMMARY_CHARS = 2000

# GitHub's comment limit is 65,536 characters and <details> content counts
# toward it. Inline diff content is therefore budgeted: complete diffs for
# small files, bounded hunk excerpts for medium ones, index-only for huge
# ones. The total budget leaves generous headroom for the comment skeleton.
_MAX_INLINE_DIFF_PER_FILE = 12_000
_MAX_INLINE_DIFF_TOTAL = 48_000
_SMALL_HUNK_CHARS = 1_500
_EXCERPT_HUNK_CHARS = 3_000
_HUNK_EDGE_LINES = 20
_MIN_INLINE_CHARS = 600


@dataclass(frozen=True)
class DiffCommentMarker:
    source_pr: int
    sha: str
    # Present only for legacy per-file comments. New grouped comments use None.
    path: str | None = None


def _payload_sha(payload: str) -> str:
    """Stable short hash of the exact rendered comment body reviewers see."""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _repo_html_url(pr_html_url: str) -> str:
    """Derive the repo URL (``…/owner/repo``) from a PR's html_url."""
    marker = "/pull/"
    idx = pr_html_url.find(marker)
    return pr_html_url[:idx] if idx != -1 else ""


def _files_changed_url(pr_html_url: str, path: str) -> str | None:
    """Return a link to *path* in the PR's Files changed tab."""
    if not pr_html_url:
        return None
    digest = hashlib.sha256(path.encode("utf-8")).hexdigest()
    return f"{pr_html_url}/files#diff-{digest}"



def _safe_text(value: str, *, limit: int | None = None) -> str:
    text = " ".join(value.replace("\r\n", "\n").replace("\r", "\n").split())
    if limit is not None and len(text) > limit:
        text = text[:limit].rstrip() + "..."
    return html.escape(text, quote=False)


def _path_code(path: str) -> str:
    return f"<code>{html.escape(path)}</code>"


def _summary_block(results: list[ResolutionResult]) -> str:
    summary = next((r.llm_summary for r in results if r.llm_summary), None)
    if not summary:
        return ""
    text = summary.strip()
    if len(text) > _MAX_SUMMARY_CHARS:
        text = text[:_MAX_SUMMARY_CHARS].rstrip() + "\n... summary truncated."
    quoted = "\n".join(f"> {html.escape(line, quote=False)}" for line in text.splitlines())
    return f"**Claude Summary**\n\n{quoted}\n\n"


def _commentable_results(results: list[ResolutionResult]) -> list[ResolutionResult]:
    return [
        result for result in results
        if result.resolved_content is not None
        and (result.reviewer_diff or result.resolution_diff)
    ]


def _commit_file_url(repo_html_url: str, commit_sha: str, path: str) -> str | None:
    """Link to *path* within the resolution commit's native diff view."""
    if not repo_html_url or not commit_sha:
        return None
    digest = hashlib.sha256(path.encode("utf-8")).hexdigest()
    return f"{repo_html_url}/commit/{commit_sha}#diff-{digest}"


def _file_line(
    result: ResolutionResult,
    *,
    repo_html_url: str,
    resolved_commit_sha: str | None,
    pr_html_url: str,
) -> str:
    """One bullet per resolved file, linking to its native diff view.

    Prefer the resolution commit's per-file anchor (shows exactly what the AI
    committed); fall back to the PR's Files changed tab if the commit sha is
    unavailable.
    """
    commit_url = (
        _commit_file_url(repo_html_url, resolved_commit_sha, result.path)
        if resolved_commit_sha else None
    )
    url = commit_url or _files_changed_url(pr_html_url, result.path)
    stats = ""
    diff = result.reviewer_diff or result.resolution_diff
    if diff:
        additions, deletions, hunks = _diff_stats(diff)
        hunk_word = "hunk" if hunks == 1 else "hunks"
        stats = f" — +{additions} / −{deletions} across {hunks} {hunk_word}"
    if url:
        return f"- {_path_code(result.path)}{stats} — [view diff]({url})"
    return f"- {_path_code(result.path)}{stats}"


def _diff_stats(diff: str) -> tuple[int, int, int]:
    """(additions, deletions, hunk count) of a single-file unified diff.

    Only lines inside hunks are counted, so the file header can never be
    mistaken for a deletion whose content starts with dashes.
    """
    additions = deletions = hunks = 0
    for line in diff.splitlines():
        if line.startswith("@@"):
            hunks += 1
        elif not hunks:
            continue
        elif line.startswith("+"):
            additions += 1
        elif line.startswith("-"):
            deletions += 1
    return additions, deletions, hunks


def _split_hunks(diff: str) -> tuple[str, list[str]]:
    """Split a unified diff into its file header and one string per hunk."""
    header_lines: list[str] = []
    hunks: list[list[str]] = []
    for line in diff.splitlines():
        if line.startswith("@@"):
            hunks.append([line])
        elif hunks:
            hunks[-1].append(line)
        else:
            header_lines.append(line)
    return "\n".join(header_lines), ["\n".join(hunk) for hunk in hunks]


def _bounded_hunk(hunk: str, max_chars: int) -> str:
    """A hunk excerpt of at most *max_chars*, never silently truncated.

    Oversized hunks show their beginning and end around an explicit
    ``... omitted N lines ...`` marker. Pathologically long lines fall back
    to a character-count marker; the bound holds in every case.
    """
    if len(hunk) <= max_chars:
        return hunk
    lines = hunk.splitlines()
    for edge in range(min(_HUNK_EDGE_LINES, len(lines) // 2), 0, -1):
        omitted = len(lines) - 2 * edge
        excerpt = "\n".join([
            *lines[:edge],
            f"... omitted {omitted} lines ...",
            *lines[-edge:],
        ])
        if len(excerpt) <= max_chars:
            return excerpt
    head = hunk[: max_chars // 2 - 40].rstrip()
    omitted = len(hunk) - len(head)
    return f"{head}\n... omitted {omitted} characters ..."


def _inline_diff_block(result: ResolutionResult, budget: int) -> str:
    """A ``<details>`` block for one file's diff, sized within *budget*.

    Small diffs embed completely; medium diffs show a bounded excerpt of
    every hunk that fits, with explicit accounting of anything omitted;
    when the budget is exhausted the file stays index-only ("" is returned)
    and its bullet still links to the full diff.
    """
    diff = (result.reviewer_diff or result.resolution_diff or "").rstrip("\n")
    if not diff:
        return ""
    limit = min(_MAX_INLINE_DIFF_PER_FILE, budget)
    if limit < _MIN_INLINE_CHARS:
        return ""

    if len(diff) <= limit:
        summary = "Complete resolved diff"
        content = diff
        accounting = ""
    else:
        header, hunks = _split_hunks(diff)
        parts = [header] if header else []
        remaining = limit - len(header)
        shown = excerpted = 0
        for hunk in hunks:
            if remaining <= _MIN_INLINE_CHARS:
                break
            if len(hunk) <= min(_SMALL_HUNK_CHARS, remaining):
                parts.append(hunk)
                remaining -= len(hunk)
                shown += 1
                continue
            excerpt = _bounded_hunk(hunk, min(_EXCERPT_HUNK_CHARS, remaining))
            parts.append(excerpt)
            remaining -= len(excerpt)
            shown += 1
            excerpted += 1
        if not shown:
            return ""
        omitted = len(hunks) - shown
        summary = "Representative conflict hunks"
        content = "\n".join(parts)
        bits = [f"Showing {shown} of {len(hunks)} hunks"]
        if excerpted:
            bits.append(f"{excerpted} excerpted")
        if omitted:
            bits.append(f"{omitted} omitted")
        accounting = (
            "\n\n_" + "; ".join(bits)
            + ". The complete diff is at the link above._"
        )

    return (
        f"\n<details>\n<summary>{summary}</summary>\n\n"
        f"````diff\n{content}\n````"
        f"{accounting}\n\n</details>"
    )


def _render_body(
    source_pr: int,
    results: list[ResolutionResult],
    *,
    source_title: str | None,
    cherry_pick_sha: str | None,
    repo_html_url: str,
    resolved_commit_sha: str | None,
    pr_html_url: str,
    evidence_patch_name: str | None,
) -> str:
    lines = [f"### AI backport resolution: source PR #{source_pr}", ""]
    title_text = _safe_text(source_title, limit=160) if source_title else ""
    if title_text:
        lines.extend([f"**{title_text}**", ""])
    meta_bits: list[str] = []
    if cherry_pick_sha:
        meta_bits.append(f"`{cherry_pick_sha[:12]}`")
    file_word = "file" if len(results) == 1 else "files"
    meta_bits.append(f"{len(results)} AI-edited {file_word}")
    lines.extend([" · ".join(meta_bits), ""])
    summary = _summary_block(results)
    if summary:
        lines.append(summary.rstrip())
        lines.append("")
    lines.append("**AI-edited files requiring review**")
    lines.append("")
    inline_budget = _MAX_INLINE_DIFF_TOTAL
    for result in results:
        lines.append(
            _file_line(
                result,
                repo_html_url=repo_html_url,
                resolved_commit_sha=resolved_commit_sha,
                pr_html_url=pr_html_url,
            )
        )
        block = _inline_diff_block(result, inline_budget)
        if block:
            inline_budget -= len(block)
            lines.append(block)
    if evidence_patch_name:
        lines.extend([
            "",
            f"Full AI edit trace: workflow artifact `{evidence_patch_name}`.",
        ])
    if resolved_commit_sha and repo_html_url:
        lines.extend([
            "",
            f"Full backport commit diff: "
            f"[commit {resolved_commit_sha[:12]}]"
            f"({repo_html_url}/commit/{resolved_commit_sha}).",
        ])
    lines.extend([
        "",
        "> Please review these AI resolutions for correctness before merging.",
    ])
    return "\n".join(lines)


def render_diff_comment(
    source_pr: int,
    resolution_results: list[ResolutionResult],
    *,
    source_title: str | None = None,
    cherry_pick_sha: str | None = None,
    repo_html_url: str = "",
    resolved_commit_sha: str | None = None,
    pr_html_url: str = "",
    evidence_patch_name: str | None = None,
) -> str:
    """Render one grouped diff comment for a source PR.

    Every file links to its native diff in the resolution commit view; inline
    diff content is tiered and budgeted (complete for small diffs, bounded
    hunk excerpts for medium ones, index-only for huge ones) so the comment
    stays well under GitHub's 65,536-character limit.
    """
    results = _commentable_results(resolution_results)
    body = _render_body(
        source_pr,
        results,
        source_title=source_title,
        cherry_pick_sha=cherry_pick_sha,
        repo_html_url=repo_html_url,
        resolved_commit_sha=resolved_commit_sha,
        pr_html_url=pr_html_url,
        evidence_patch_name=evidence_patch_name,
    )
    sha = _payload_sha(body)
    marker = f'<!-- {_MARKER_PREFIX} source_pr="{source_pr}" sha="{sha}" -->'
    return f"{marker}\n{body}"


def parse_marker(body: str) -> DiffCommentMarker | None:
    """Return marker data for a marked comment, else ``None``."""
    match = _MARKER_RE.search(body)
    if not match:
        return None
    path = match.group("path")
    return DiffCommentMarker(
        source_pr=int(match.group("source_pr")),
        sha=match.group("sha"),
        path=html.unescape(path) if path is not None else None,
    )


def _comment_sha(body: str) -> str | None:
    parsed = parse_marker(body)
    return parsed.sha if parsed else None


def _delete_comment(comment: Any) -> None:
    retry_github_call(
        lambda: comment.delete(), retries=3, description="delete AI-diff comment",
    )


def _edit_comment(comment: Any, body: str) -> None:
    retry_github_call(
        lambda: comment.edit(body), retries=3, description="edit AI-diff comment",
    )


def _create_comment(pr: Any, body: str) -> Any:
    return retry_github_call(
        lambda: pr.create_issue_comment(body),
        retries=3,
        description="post AI-diff comment",
    )


def _comment_author(comment: Any) -> str | None:
    user = getattr(comment, "user", None)
    return getattr(user, "login", None) if user is not None else None


def _owned_marked_comments(
    comments: list[Any],
    source_pr: int,
    *,
    bot_login: str | None,
) -> list[Any]:
    owned: list[Any] = []
    for comment in comments:
        parsed = parse_marker(getattr(comment, "body", "") or "")
        if parsed is None or parsed.source_pr != source_pr:
            continue
        if bot_login is not None and _comment_author(comment) != bot_login:
            continue
        owned.append(comment)
    return owned


def reconcile_diff_comments(
    pr: Any,
    source_pr: int,
    resolution_results: list[ResolutionResult],
    *,
    source_title: str | None = None,
    cherry_pick_sha: str | None = None,
    resolved_commit_sha: str | None = None,
    bot_login: str | None = None,
    evidence_patch_name: str | None = None,
) -> dict[str, str]:
    """Make the PR's AI-diff comment for *source_pr* match the resolutions.

    Returns ``{path: comment_url}`` so callers can link file status lines in the
    PR body. All paths for a source PR point at the same grouped comment.
    """
    pr_html_url = getattr(pr, "html_url", "") or ""
    repo_html_url = _repo_html_url(pr_html_url)
    desired_results = _commentable_results(resolution_results)
    existing = retry_github_call(
        lambda: list(pr.get_issue_comments()),
        retries=3,
        description="list PR comments for AI-diff reconcile",
    )
    ours = _owned_marked_comments(existing, source_pr, bot_login=bot_login)

    if not desired_results:
        for comment in ours:
            _delete_comment(comment)
        return {}

    desired_body = render_diff_comment(
        source_pr,
        desired_results,
        source_title=source_title,
        cherry_pick_sha=cherry_pick_sha,
        repo_html_url=repo_html_url,
        resolved_commit_sha=resolved_commit_sha,
        pr_html_url=pr_html_url,
        evidence_patch_name=evidence_patch_name,
    )

    # Prefer an existing grouped comment; otherwise edit the first legacy
    # per-file marker into the grouped shape and delete the remaining markers.
    keeper = next(
        (
            comment for comment in ours
            if (parse_marker(getattr(comment, "body", "") or "") or DiffCommentMarker(0, "")).path is None
        ),
        ours[0] if ours else None,
    )
    for comment in ours:
        if keeper is not None and comment is keeper:
            continue
        _delete_comment(comment)

    if keeper is None:
        keeper = _create_comment(pr, desired_body)
    elif _comment_sha(getattr(keeper, "body", "") or "") != _comment_sha(desired_body):
        _edit_comment(keeper, desired_body)

    url = getattr(keeper, "html_url", None)
    if not url:
        return {}
    return {result.path: url for result in desired_results}


def marked_source_pr_urls(pr: Any, *, bot_login: str | None = None) -> dict[int, str]:
    """Return source PR numbers mapped to their marked comment URLs on *pr*.

    If both legacy per-file comments and a grouped comment exist for the same
    source PR, prefer the grouped comment URL because that is the canonical
    destination after migration.
    """
    existing = retry_github_call(
        lambda: list(pr.get_issue_comments()),
        retries=3,
        description="list PR comments for AI-diff source-PR scan",
    )
    found: dict[int, str] = {}
    for comment in existing:
        parsed = parse_marker(getattr(comment, "body", "") or "")
        if parsed is None:
            continue
        if bot_login is not None and _comment_author(comment) != bot_login:
            continue
        url = getattr(comment, "html_url", None)
        if not url:
            continue
        if parsed.source_pr not in found or parsed.path is None:
            found[parsed.source_pr] = url
    return found

