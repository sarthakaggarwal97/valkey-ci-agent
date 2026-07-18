"""Prepare trusted, bounded PR inputs shared by release-note AI stages."""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Sequence

from scripts.common.proc import git_output
from scripts.release_notes.models import MergedPR

logger = logging.getLogger(__name__)

# Per-commit diff budget (characters) inlined into a prompt.
_MAX_DIFF_CHARS = 6000


def exact_pr_number(value: object) -> "int | None":
    """Return *value* iff it is an exact non-bool int, else None."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def build_prompt_payload(prs: Sequence[MergedPR], *, diffs: dict[int, str] | None = None) -> str:
    """Render factual PR metadata and optional bounded diffs as a JSON array."""
    diffs = diffs or {}
    payload = []
    for pr in prs:
        entry = {
            "number": pr.number,
            "title": pr.title,
            "author": pr.author,
            "url": pr.url,
            "body": pr.body,
        }
        diff = diffs.get(pr.number)
        if diff:
            entry["diff"] = diff
        payload.append(entry)
    return json.dumps(payload, indent=2)


class PRDiffCollector:
    """Collect bounded, unambiguous PR diffs with a SHA-keyed cache."""

    def __init__(self, repo_dir: str, prs: Sequence[MergedPR]) -> None:
        self._repo_dir = repo_dir
        self._cache: dict[str, str] = {}
        sha_prs: dict[str, set[int]] = {}
        for pr in prs:
            if pr.merge_commit_sha:
                sha_prs.setdefault(pr.merge_commit_sha, set()).add(pr.number)
        self._shared_shas = {
            sha for sha, pr_numbers in sha_prs.items() if len(pr_numbers) > 1
        }
        if self._shared_shas:
            logger.info(
                "Omitting ambiguous combined diffs for %d commit(s) shared by "
                "multiple source PRs",
                len(self._shared_shas),
            )

    def collect(self, prs: Sequence[MergedPR]) -> dict[int, str]:
        """Return diffs by PR number, reading each unique, attributable SHA once.

        Sweep expansion can map several source PRs to one combined range commit.
        Such a patch cannot be attributed to any one PR and is omitted rather than
        duplicated into every PR record. Failed and empty reads are cached too.
        """
        diffs: dict[int, str] = {}
        for pr in prs:
            sha = pr.merge_commit_sha
            if not sha or sha in self._shared_shas:
                continue
            if sha not in self._cache:
                self._cache[sha] = _collect_commit_diff(self._repo_dir, sha)
            diff = self._cache[sha]
            if diff:
                diffs[pr.number] = diff
        return diffs


def _collect_commit_diff(repo_dir: str, sha: str) -> str:
    """Return a bounded first-parent diff for *sha*, or ``""`` on error."""
    try:
        diff = git_output(
            repo_dir,
            "show",
            "--format=",
            "--stat",
            "--patch",
            "--first-parent",
            sha,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        logger.warning("Could not read diff for %s: %s", sha[:12], exc)
        return ""
    diff = diff.strip()
    if len(diff) <= _MAX_DIFF_CHARS:
        return diff
    # Clip on a line boundary so the last hunk is not torn mid-line.
    clipped = diff[:_MAX_DIFF_CHARS]
    newline = clipped.rfind("\n")
    if newline > 0:
        clipped = clipped[:newline]
    return clipped.rstrip() + "\n... (diff truncated)"
