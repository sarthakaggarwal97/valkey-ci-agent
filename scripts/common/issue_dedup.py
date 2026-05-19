"""Marker-based create-or-update for GitHub issues.

A workflow-agnostic helper that:

1. Embeds an HTML comment "marker" containing a stable fingerprint into the
   first issue created for a given finding.
2. On subsequent runs, searches the target repo for an open issue containing
   the marker. If found, edits its body to bump an occurrence counter and
   appends a comment; if not found, creates a fresh issue.

Callers supply the rendered title, body, and comment — this module owns only
the dedup machinery, not the presentation.

Search failures are propagated. The caller is expected to wrap upsert in a
per-run try/except so a transient outage records as an error and the next
scheduled run retries against the same fingerprint instead of silently
creating a duplicate issue.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class IssueContent:
    """Pre-rendered issue text supplied by the caller."""

    title: str
    body: str
    comment: str
    labels: tuple[str, ...] = ()


class IssueDedupPublisher:
    """Create or update an issue, deduplicating on a fingerprint marker."""

    def __init__(self, github_client: Any, *, marker_namespace: str) -> None:
        """`marker_namespace` should be a stable workflow-scoped string,
        e.g. ``"valkey-ci-agent:fuzzer-issue"``. It appears literally in
        issue bodies and in search queries.
        """
        self._gh = github_client
        self._ns = marker_namespace

    def upsert(
        self,
        repo_name: str,
        *,
        fingerprint: str,
        render: Callable[[str, int], IssueContent],
    ) -> tuple[str, str]:
        """Create or update the issue for ``fingerprint``.

        ``render(marker, occurrences)`` is called with the dedup marker and
        the occurrence count (1 for new issues, >=2 for updates) and must
        return a fully rendered :class:`IssueContent`. Returns
        ``(action, html_url)`` where action is ``"created"`` or ``"updated"``.
        """
        repo = self._gh.get_repo(repo_name)
        marker = f"<!-- {self._ns}:{fingerprint} -->"
        existing = self._find_existing(repo_name, marker)

        if existing is None:
            content = render(marker, 1)
            issue = repo.create_issue(title=content.title, body=content.body)
            if content.labels:
                try:
                    issue.add_to_labels(*content.labels)
                except Exception as exc:
                    logger.info("Could not add labels to issue #%s: %s", issue.number, exc)
            logger.info("Created issue #%s for %s", issue.number, fingerprint)
            return "created", issue.html_url

        body = existing.body or ""
        # Re-inject the marker if the body lost it (e.g. an editor stripped
        # HTML comments) so future runs continue to dedupe against this issue.
        if marker not in body:
            body = f"{marker}\n{body}".rstrip()
        m = _occurrence_re(self._ns).search(body)
        count = int(m.group(1)) + 1 if m else 2
        marker_occurrences = f"<!-- {self._ns}:occurrences:{count} -->"
        new_body = (
            _occurrence_re(self._ns).sub(marker_occurrences, body)
            if m else f"{body}\n{marker_occurrences}"
        )
        content = render(marker, count)
        existing.edit(body=new_body, title=content.title)
        existing.create_comment(body=content.comment)
        logger.info("Updated issue #%s (occurrence %d)", existing.number, count)
        return "updated", existing.html_url

    def _find_existing(self, repo_name: str, marker: str) -> Any:
        """Find an open issue containing the marker, or None."""
        query = f'"{marker}" in:body repo:{repo_name} is:issue is:open'
        for issue in self._gh.search_issues(query):
            if marker in (issue.body or ""):
                # Reload via the actual repo so we get a mutable issue handle.
                return self._gh.get_repo(repo_name).get_issue(issue.number)
        return None


def _occurrence_re(namespace: str) -> re.Pattern[str]:
    """A namespaced occurrence-counter regex: ``<!-- <ns>:occurrences:<n> -->``."""
    return re.compile(rf"<!-- {re.escape(namespace)}:occurrences:(\d+) -->")
