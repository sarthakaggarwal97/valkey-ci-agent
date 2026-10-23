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
import os
import re
import tempfile
from typing import Optional

from scripts.common.proc import git_output, is_ancestor
from scripts.release_notes import release_format as rn

logger = logging.getLogger(__name__)

# "@@ -12,7 +12,9 @@" - the pre-image start line and count.
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@")
# The PRE-image path: blame runs at <sha>^, where only the old path exists.
# This is also what makes deletions and renames correct: a deletion has
# "+++ /dev/null" but a real "--- a/path", and a rename blames the old name.
_FILE_RE = re.compile(r"^--- a/(.+)$")
_NO_PREIMAGE_RE = re.compile(r"^--- /dev/null$")
_BLAME_SHA_RE = re.compile(r"^([0-9a-f]{40})\s")
# Formatting sweeps the repository asks blame to look through.
_IGNORE_REVS_FILE = ".git-blame-ignore-revs"

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

        Mapping a failure to False would classify shipped code as new and
        silently drop its note.
        """
        if sha not in self._released:
            self._released[sha] = is_ancestor(self._repo_dir, sha, self._base_tag)
        return self._released[sha]

    def modifies_only_unreleased_code(self, sha: str) -> Optional[bool]:
        """True when every line *sha* modifies was introduced after the baseline.

        Returns None when the question cannot be answered: no baseline tag, an
        unreadable commit, a commit that only adds lines, or one too large to
        blame. Callers must treat None as "keep the note".
        """
        if not self.usable or not sha:
            return None
        introducing = modified_line_origins(self._repo_dir, sha)
        if introducing is None:
            return None
        answers = [self._is_released(commit) for commit in introducing]
        if any(answer is True for answer in answers):
            return False
        if any(answer is None for answer in answers):
            return None
        return True


class OriginsUnknown(Exception):
    """Blame could not say which commits introduced the lines a change touches."""


def modified_line_origins(repo_dir: str, sha: str) -> Optional[set[str]]:
    """Return the commits that introduced every line *sha* modifies or deletes.

    Returns None when the answer is unknown, and when the commit only adds
    lines: purely additive code has no pre-image, so its age says nothing.
    """
    try:
        return line_origins(repo_dir, sha) or None
    except OriginsUnknown as exc:
        logger.debug("Leaving code age of %s unknown: %s", sha[:12], exc)
        return None


def line_origins(
    repo_dir: str,
    sha: str,
    *,
    base: Optional[str] = None,
    anchor_insertions: bool = False,
    max_files: int = _MAX_FILES,
) -> set[str]:
    """Return the commits that introduced the pre-image lines *sha* touches.

    Diffs the commit against its first parent and blames that same pre-image.
    With *base*, the change is instead the range ``base..sha`` (a rebased
    series of commits read as one change), diffed and blamed at *base*.
    Using an explicit first-parent diff makes a two-parent GitHub merge commit
    behave like its pull-request patch instead of Git's combined-diff format.
    Blame honours the parent's ``.git-blame-ignore-revs``, so a mass reformat
    does not stand in for the commit that actually wrote a line.

    With *anchor_insertions*, a hunk that only inserts lines is attributed to
    the existing lines on either side of it: a fix that adds a NULL or bounds
    check repairs the code around it. Without it such hunks contribute nothing.

    Returns an empty set when the commit touches no pre-existing line (for
    example, it only adds files). Raises ``OriginsUnknown`` when the answer is
    unknown: an unreadable commit, one touching more than *max_files* files
    (``_MAX_FILES`` by default), or a range blame cannot read.
    """
    if not sha:
        raise OriginsUnknown("no commit")
    try:
        parent = git_output(repo_dir, "rev-parse", base or f"{sha}^1").strip()
        diff = git_output(
            repo_dir,
            "diff",
            "--no-ext-diff",
            "--unified=0",
            "--no-color",
            parent,
            sha,
            "--",
        )
    except Exception as exc:
        raise OriginsUnknown(f"could not read the diff of {sha[:12]}") from exc

    ranges: dict[str, list[tuple[int, int]]] = {}
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
            if len(files) > max_files:
                raise OriginsUnknown(f"touches more than {max_files} files")
            continue
        hunk = _HUNK_RE.match(line)
        if hunk and current_file:
            start = int(hunk.group(1))
            count = int(hunk.group(2) or "1")
            if count:
                ranges.setdefault(current_file, []).append((start, start + count - 1))
            elif anchor_insertions:
                # "-N,0" inserts after pre-image line N; anchor on N and N+1.
                # The end is clamped to the file length once it is known.
                ranges.setdefault(current_file, []).append((max(start, 1), start + 1))

    if not ranges:
        return set()

    origins: set[str] = set()
    with tempfile.TemporaryDirectory(prefix="code-age-") as tmp:
        ignore_args = _ignore_revs_args(repo_dir, parent, tmp)
        for path, spans in ranges.items():
            length = _line_count(repo_dir, parent, path) if anchor_insertions else None
            bounds = [
                (start, min(end, length) if length is not None else end)
                for start, end in spans
                if length is None or start <= length
            ]
            if not bounds:
                continue  # an insertion into an empty file anchors on nothing
            try:
                blame = git_output(
                    repo_dir, "blame", "--line-porcelain", *ignore_args,
                    *(f"-L{start},{end}" for start, end in bounds), parent, "--", path,
                )
            except Exception as exc:
                raise OriginsUnknown(f"could not blame {path} at {parent[:12]}") from exc
            introducing = {
                m.group(1)
                for m in (_BLAME_SHA_RE.match(line) for line in blame.splitlines())
                if m
            }
            if not introducing:
                raise OriginsUnknown(f"blame of {path} at {parent[:12]} named no commit")
            origins |= introducing
    return origins


def _ignore_revs_args(repo_dir: str, rev: str, tmp: str) -> list[str]:
    """Blame arguments for the ``.git-blame-ignore-revs`` file *rev* carries.

    Read from the blamed revision rather than a checkout, so the list matches
    the history being blamed. A missing file means nothing is ignored.
    """
    try:
        content = git_output(repo_dir, "show", f"{rev}:{_IGNORE_REVS_FILE}")
    except Exception:
        return []
    path = os.path.join(tmp, "ignore-revs")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)
    return [f"--ignore-revs-file={path}"]


def _line_count(repo_dir: str, rev: str, path: str) -> int:
    try:
        content = git_output(repo_dir, "show", f"{rev}:{path}")
    except Exception as exc:
        raise OriginsUnknown(f"could not read {path} at {rev[:12]}") from exc
    return len(content.splitlines())


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
            datetime.datetime.fromisoformat(tag_date.replace("Z", "+00:00"))
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

    try:
        released |= rn.credited_pr_numbers(git_output(repo_dir, "show", f"{base_tag}:{notes_file}"))
    except Exception:
        # A baseline predating the changelog, or a tag missing from a shallow
        # clone, simply contributes no exclusions from this source.
        logger.warning("Could not read %s at %s", notes_file, base_tag)

    return released
