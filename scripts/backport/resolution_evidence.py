"""Durable audit evidence for AI-authored backport resolutions.

PR comments are an index, not an evidence store: GitHub's 65,536-character
comment limit counts ``<details>`` content, so large diffs cannot live in
comments. The complete record is written instead to the run's
``agent-evidence`` directory, which the backport workflows already upload
as a 30-day artifact — no workflow changes required.

Each candidate appends one section per resolved file containing the source
PR and target branch, content digests of the target / source / resolved
states, the AI summary, the ``reviewer_diff`` (the complete landing delta
a reviewer must judge) and the ``resolution_diff`` (the literal edit trace
from the conflicted state).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from scripts.ai.runtime import default_evidence_dir
from scripts.backport.models import BackportCandidate, CandidateResult

logger = logging.getLogger(__name__)

EVIDENCE_PATCH_NAME = "ai-resolution-evidence.patch"


def write_resolution_evidence(
    candidate: BackportCandidate,
    result: CandidateResult,
    *,
    evidence_dir: str | None = None,
) -> Path | None:
    """Append this candidate's resolution evidence to the run's patch file.

    Best-effort by design: evidence must never fail a backport. Returns the
    patch path when something was written, ``None`` otherwise.
    """
    target_dir = evidence_dir if evidence_dir is not None else default_evidence_dir()
    if not target_dir:
        return None
    sections = _render_sections(candidate, result)
    if not sections:
        return None
    try:
        directory = Path(target_dir)
        directory.mkdir(parents=True, exist_ok=True)
        patch_path = directory / EVIDENCE_PATCH_NAME
        with patch_path.open("a", encoding="utf-8") as handle:
            handle.write(sections)
        return patch_path
    except OSError as exc:
        logger.warning("Could not write resolution evidence: %s", exc)
        return None


def _render_sections(
    candidate: BackportCandidate,
    result: CandidateResult,
) -> str:
    target_by_path = {
        item.path: item.target_branch_content
        for item in result.conflicting_files
    }
    source_by_path = {
        item.path: item.source_branch_content
        for item in result.conflicting_files
    }
    sections: list[str] = []
    for resolution in result.resolutions:
        if resolution.resolved_content is None:
            continue
        if not (resolution.reviewer_diff or resolution.resolution_diff):
            continue
        lines = [
            f"=== AI resolution evidence: {resolution.path} ===",
            f"Source-PR: #{candidate.source_pr_number}",
            f"Target-Branch: {candidate.target_branch}",
            f"Resolution-Commit: {result.resolved_commit_sha or '(none)'}",
            f"Target-Content-SHA256: {_digest(target_by_path.get(resolution.path))}",
            f"Source-Content-SHA256: {_digest(source_by_path.get(resolution.path))}",
            f"Resolved-Content-SHA256: {_digest(resolution.resolved_content)}",
            f"AI-Summary: {(resolution.llm_summary or resolution.resolution_summary or '').strip() or '(none)'}",
        ]
        if resolution.reviewer_diff:
            lines.extend([
                "",
                "--- reviewer_diff (complete landing delta) ---",
                resolution.reviewer_diff.rstrip("\n"),
            ])
        if resolution.resolution_diff and resolution.resolution_diff != resolution.reviewer_diff:
            lines.extend([
                "",
                "--- resolution_diff (literal resolver edit trace) ---",
                resolution.resolution_diff.rstrip("\n"),
            ])
        sections.append("\n".join(lines) + "\n\n")
    return "".join(sections)


def _digest(content: str | None) -> str:
    if content is None:
        return "(unavailable)"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
