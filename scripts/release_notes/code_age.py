"""Decide whether a change only touches code that has never been released.

A fix for a feature introduced in the same release is not a user-facing fix:
the feature ships correct, and no released version ever had the bug. Listing
such a fix invites the reader to look for a broken version that does not exist.

The question is answerable from history rather than from prose. Blame the lines
a commit *modifies* (its pre-image, not its additions) and ask whether the
commits that introduced those lines are reachable from the baseline release tag.
When none of them are, every line being fixed arrived after the baseline, so the
fix repairs code no user has run.

The check is deliberately one-sided. It reports "unreleased" only on positive
evidence that every modified line is new; anything unknown, unreadable, or
purely additive reports False so the note is kept. Under-reporting a real fix is
the worse error.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from scripts.common.proc import git_output

logger = logging.getLogger(__name__)

# "@@ -12,7 +12,9 @@" - the pre-image start line and count.
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@")
_FILE_RE = re.compile(r"^\+\+\+ b/(.+)$")
_BLAME_SHA_RE = re.compile(r"^([0-9a-f]{40})\s")

# A commit touching more files than this is a sweep or a mass refactor rather
# than a targeted fix; blaming all of it costs more than the answer is worth.
_MAX_FILES = 40


class ReleasedCodeOracle:
    """Answer "was this code released?" with a per-commit ancestry cache."""

    def __init__(self, repo_dir: str, base_tag: str) -> None:
        self._repo_dir = repo_dir
        self._base_tag = base_tag
        self._released: dict[str, bool] = {}

    @property
    def usable(self) -> bool:
        return bool(self._base_tag)

    def _is_released(self, sha: str) -> bool:
        cached = self._released.get(sha)
        if cached is not None:
            return cached
        try:
            git_output(
                self._repo_dir, "merge-base", "--is-ancestor", sha, self._base_tag,
            )
            released = True
        except Exception:
            # Non-zero exit means "not an ancestor", which is the answer we
            # want; a genuinely broken invocation is indistinguishable here, so
            # treat only a clean success as proof of release.
            released = False
        self._released[sha] = released
        return released

    def modifies_only_unreleased_code(self, sha: str) -> Optional[bool]:
        """True when every line *sha* modifies was introduced after the baseline.

        Returns None when the question cannot be answered: no baseline tag, an
        unreadable commit, a commit that only adds lines, or one too large to
        blame. Callers must treat None as "keep the note".
        """
        if not self.usable or not sha:
            return None
        try:
            diff = git_output(
                self._repo_dir, "show", "--format=", "--unified=0", "--no-color", sha,
            )
        except Exception:
            logger.debug("Could not read diff for %s; leaving code age unknown", sha[:12])
            return None

        ranges: list[tuple[str, int, int]] = []
        current_file = ""
        files: set[str] = set()
        for line in diff.splitlines():
            file_match = _FILE_RE.match(line)
            if file_match:
                current_file = file_match.group(1)
                files.add(current_file)
                if len(files) > _MAX_FILES:
                    return None
                continue
            hunk = _HUNK_RE.match(line)
            if hunk and current_file:
                start = int(hunk.group(1))
                count = int(hunk.group(2) or "1")
                if count:  # count 0 is a pure insertion: no pre-image to blame
                    ranges.append((current_file, start, start + count - 1))

        if not ranges:
            return None

        for path, start, end in ranges:
            try:
                blame = git_output(
                    self._repo_dir, "blame", "--line-porcelain",
                    f"-L{start},{end}", f"{sha}^", "--", path,
                )
            except Exception:
                logger.debug("Could not blame %s:%s-%s at %s^", path, start, end, sha[:12])
                return None
            introducing = {
                m.group(1) for m in (_BLAME_SHA_RE.match(l) for l in blame.splitlines()) if m
            }
            if not introducing:
                return None
            if any(self._is_released(commit) for commit in introducing):
                return False
        return True
