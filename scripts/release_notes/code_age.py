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

import datetime
import logging
import re
import subprocess
from typing import Optional

from scripts.common.proc import git_output

logger = logging.getLogger(__name__)

# "@@ -12,7 +12,9 @@" - the pre-image start line and count.
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@")
# The PRE-image path: blame runs at <sha>^, where only the old path exists.
# This is also what makes deletions and renames correct: a deletion has
# "+++ /dev/null" but a real "--- a/path", and a rename blames the old name.
_FILE_RE = re.compile(r"^--- a/(.+)$")
_NO_PREIMAGE_RE = re.compile(r"^--- /dev/null$")
_BLAME_SHA_RE = re.compile(r"^([0-9a-f]{40})\s")

# A commit touching more files than this is a sweep or a mass refactor rather
# than a targeted fix; blaming all of it costs more than the answer is worth.
_MAX_FILES = 40


class ReleasedCodeOracle:
    """Answer "was this code released?" with a per-commit ancestry cache."""

    def __init__(self, repo_dir: str, base_tag: str) -> None:
        self._repo_dir = repo_dir
        self._base_tag = base_tag
        self._released: dict[str, Optional[bool]] = {}

    @property
    def usable(self) -> bool:
        return bool(self._base_tag)

    def _is_released(self, sha: str) -> Optional[bool]:
        """True/False for a definite ancestry answer, None when git failed.

        git exits 1 for "not an ancestor" and anything else (128 for a bad
        object, a timeout, a scrubbed-config refusal) for a broken question.
        Only the definite 1 may count as "not released": mapping failures to
        False would classify shipped code as new and silently drop its note.
        """
        cached = self._released.get(sha)
        if cached is not None or sha in self._released:
            return cached
        try:
            git_output(
                self._repo_dir, "merge-base", "--is-ancestor", sha, self._base_tag,
            )
            released: Optional[bool] = True
        except subprocess.CalledProcessError as exc:
            released = False if exc.returncode == 1 else None
        except Exception:
            released = None
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
            if _NO_PREIMAGE_RE.match(line):
                current_file = ""  # new file: nothing to blame in this section
                continue
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
                m.group(1)
                for m in (_BLAME_SHA_RE.match(line) for line in blame.splitlines())
                if m
            }
            if not introducing:
                return None
            answers = [self._is_released(commit) for commit in introducing]
            if any(answer is True for answer in answers):
                return False
            if any(answer is None for answer in answers):
                return None
        return True

# References with introduction semantics: "introduced in #N", "regression from
# #N", "follow-up to #N", "broke(n) ... #N". A bare "#N" is deliberately NOT
# matched: PR bodies cite related work freely, and only an introduction claim
# says the bug arrived with that change.
# The reference itself may be "#N" or a full PR URL; valkey bodies use both.
# Only DIRECTIONAL introduction claims match: "introduced in #N", "regression
# from #N", "caused by <url>". A bare verb near a reference is not a claim -
# "Added regression tests for #N" describes this PR's own work, and matching
# it would drop a real fix, the one error this check refuses.
_INTRODUCER_RE = re.compile(
    r"(?:introduced (?:in|by|with)|added (?:in|by|with)|regression (?:from|in|of)"
    r"|broke[n]? (?:in|by|since)|caused by|follow[- ]?up (?:to|of|for))"
    r"\s.{0,50}?(?:#(\d+)|github\.com/[\w.-]+/[\w.-]+/pull/(\d+))",
    re.IGNORECASE,
)


def introduced_by_in_range(text: str, range_pr_numbers: frozenset[int]) -> Optional[int]:
    """Return the in-range PR *text* claims introduced this bug, if any.

    Complements the blame-based oracle: a fix to a new feature implemented
    inside a long-shipped file blames released lines, but its own description
    ("regression from #4460") names the unreleased introducer directly. Only an
    introduction-shaped reference counts, and only when the referenced PR is
    itself in the current unreleased range.
    """
    if not text or not range_pr_numbers:
        return None
    for match in _INTRODUCER_RE.finditer(text):
        number = int(match.group(1) or match.group(2))
        if number in range_pr_numbers:
            return number
    return None


# A backport subject carries the original PR and then its own: "... (#3516) (#4001)".
_SUBJECT_PR_RE = re.compile(r"\(#(\d+)\)(?:\s*\(#(\d+)\))?\s*$")

# The repository's history predates the fork, and those Redis-era PR numbers
# collide with current ones. Bound the scan to a window around the baseline tag
# so a 2016 "Merge pull request #4076" cannot mark a 2026 PR as shipped. Two
# years generously covers any backport window on a supported line.
_HISTORY_WINDOW_DAYS = 730


def released_pr_numbers(repo_dir: str, base_tag: str, notes_file: str) -> set[int]:
    """Return PR numbers the *base_tag* release already shipped.

    A fix merges to the development branch and is then cherry-picked onto the
    release branch, so the same change exists as two commits with different
    SHAs. Discovery excludes only what is reachable from *base_tag*, which is
    the release-branch copy; the development-branch copy is not an ancestor of
    that tag and therefore enters the range even though the PR already shipped.

    Two independent sources answer what reachability cannot, and they are
    complementary rather than redundant:

    - the baseline's own history, which records every shipped PR whether or not
      anyone wrote a note for it;
    - the baseline's changelog, which credits PRs whose commit subject lost the
      reference (a differently-squashed or hand-applied cherry-pick).

    Neither catches a fix backported under a *different* PR number, which needs
    patch equivalence rather than reference matching; those remain for
    maintainer review.
    """
    if not base_tag:
        return set()
    released: set[int] = set()

    try:
        tag_date = git_output(repo_dir, "log", "-1", "--format=%cI", base_tag).strip()
        since = (
            datetime.datetime.fromisoformat(tag_date)
            - datetime.timedelta(days=_HISTORY_WINDOW_DAYS)
        ).date().isoformat()
        subjects = git_output(
            repo_dir, "log", "--format=%s", f"--since={since}", base_tag,
        )
    except Exception:
        logger.warning("Could not read history at %s; relying on its changelog alone", base_tag)
        subjects = ""
    for subject in subjects.splitlines():
        match = _SUBJECT_PR_RE.search(subject)
        if match:
            released.add(int(match.group(1)))
            if match.group(2):
                released.add(int(match.group(2)))

    # Call-time import: release_cut imports this module at load time, so a
    # module-level import here would cycle. By the time this runs, both are
    # fully loaded.
    from scripts.release_notes.release_cut import _credited_pr_numbers

    try:
        released |= _credited_pr_numbers(git_output(repo_dir, "show", f"{base_tag}:{notes_file}"))
    except Exception:
        # A baseline predating the changelog, or a tag missing from a shallow
        # clone, simply contributes no exclusions from this source.
        logger.warning("Could not read %s at %s", notes_file, base_tag)

    return released
