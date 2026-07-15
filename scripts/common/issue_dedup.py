"""Serialized, marker-based create-or-update for GitHub issues.

The finding fingerprint lives in the issue body. Optional source-event keys
are recorded as immutable hashed markers in the initial body or recurrence
comments. A recurrence comment is written before mutable presentation state,
so retries can reconcile a partial failure without duplicating the event.

GitHub has neither transactional issue mutation nor unique body keys. Callers
must serialize publishers for each repository/fingerprint namespace.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from scripts.common.github_client import retry_github_call
from scripts.common.markdown import (
    GITHUB_COMMENT_MAX_BYTES,
    bounded_body,
    bounded_comment,
    bounded_title,
    with_required_suffix,
)
from scripts.common.metadata_reconciler import (
    reconcile_labels,
    with_desired_labels,
)

logger = logging.getLogger(__name__)

_MARKER_VALUE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,240}$")


@dataclass
class IssueContent:
    """Pre-rendered issue text supplied by the caller."""

    title: str
    body: str
    comment: str
    labels: tuple[str, ...] = ()


class IssueDedupPublisher:
    """Create or update one serialized issue stream per fingerprint."""

    def __init__(self, github_client: Any, *, marker_namespace: str) -> None:
        """Initialize with a stable workflow-scoped marker namespace."""
        if not _MARKER_VALUE_RE.fullmatch(marker_namespace):
            raise ValueError("marker_namespace contains unsafe marker characters")
        self._gh = github_client
        self._ns = marker_namespace

    def upsert(
        self,
        repo_name: str,
        *,
        fingerprint: str,
        render: Callable[[str, int], IssueContent],
        idempotency_key: str | None = None,
        body_transform: Callable[[str], str] | None = None,
        title_fallback: str | None = None,
    ) -> tuple[str, str]:
        """Create or update the issue for ``fingerprint``.

        Returns ``(action, html_url)`` where action is ``created``, ``updated``,
        or ``skipped-duplicate``. An event found in a comment but missing from
        the mutable body is reconciled before ``skipped-duplicate`` is returned.
        """
        if not _MARKER_VALUE_RE.fullmatch(fingerprint):
            raise ValueError("fingerprint contains unsafe marker characters")
        if idempotency_key is not None and not _IDEMPOTENCY_KEY_RE.fullmatch(
            idempotency_key,
        ):
            raise ValueError("idempotency_key contains unsafe marker characters")

        repo = retry_github_call(
            lambda: self._gh.get_repo(repo_name),
            retries=2,
            description=f"get repo {repo_name}",
        )
        marker = f"<!-- {self._ns}:{fingerprint} -->"
        existing = self._find_existing(repo_name, marker)
        if existing is None and title_fallback is not None:
            existing = self._find_by_title(repo_name, title_fallback)
            if existing is not None:
                logger.info(
                    "Adopting legacy issue #%s for %s via title fallback",
                    existing.number,
                    fingerprint,
                )

        if existing is None:
            content = _bounded_content(render(marker, 1))
            body = bounded_body(content.body)
            if idempotency_key is not None:
                body = with_required_suffix(
                    content.body,
                    "\n".join([
                    _event_marker(self._ns, idempotency_key, 1),
                    _last_key_marker(self._ns, idempotency_key),
                    ]),
                )
            body = with_desired_labels(body, content.labels)
            issue = retry_github_call(
                lambda: repo.create_issue(title=content.title, body=body),
                retries=2,
                description="create issue",
            )
            reconcile_labels(repo, issue, content.labels)
            logger.info("Created issue #%s for %s", issue.number, fingerprint)
            return "created", issue.html_url

        body = existing.body or ""
        if idempotency_key is not None:
            recorded_count = self._recorded_event_count(
                existing,
                body,
                idempotency_key,
            )
            if recorded_count is not None:
                content = _bounded_content(render(marker, recorded_count))
                current_match = _occurrence_re(self._ns).search(body)
                current_count = int(current_match.group(1)) if current_match else 0
                needs_state_repair = (
                    current_count < recorded_count or marker not in body
                )
                if needs_state_repair:
                    if body_transform is not None:
                        body = body_transform(body)
                    body = _body_with_state(
                        body,
                        namespace=self._ns,
                        marker=marker,
                        count=recorded_count,
                        idempotency_key=idempotency_key,
                    )
                reconciled = with_desired_labels(body, content.labels)
                if reconciled != (existing.body or ""):
                    retry_github_call(
                        lambda: existing.edit(
                            body=reconciled,
                            title=content.title,
                        ),
                        retries=2,
                        description=f"reconcile issue #{existing.number}",
                    )
                reconcile_labels(repo, existing, content.labels)
                logger.info(
                    "Issue #%s already records event %s; skipping recurrence",
                    existing.number,
                    idempotency_key,
                )
                return "skipped-duplicate", existing.html_url

        occurrence = _occurrence_re(self._ns).search(body)
        count = int(occurrence.group(1)) + 1 if occurrence else 2
        content = _bounded_content(render(marker, count))
        comment = content.comment
        if idempotency_key is not None:
            comment = with_required_suffix(
                comment.rstrip(),
                _event_marker(self._ns, idempotency_key, count),
                max_bytes=GITHUB_COMMENT_MAX_BYTES,
            )
        else:
            comment = bounded_comment(comment)

        # This comment is the immutable event commit. A failed body edit leaves
        # enough evidence for the next invocation to repair presentation state.
        retry_github_call(
            lambda: existing.create_comment(body=comment),
            retries=2,
            description=f"comment on issue #{existing.number}",
        )
        if body_transform is not None:
            body = body_transform(body)
        new_body = _body_with_state(
            body,
            namespace=self._ns,
            marker=marker,
            count=count,
            idempotency_key=idempotency_key,
        )
        new_body = with_desired_labels(new_body, content.labels)
        retry_github_call(
            lambda: existing.edit(body=new_body, title=content.title),
            retries=2,
            description=f"update issue #{existing.number}",
        )
        reconcile_labels(repo, existing, content.labels)
        logger.info("Updated issue #%s (occurrence %d)", existing.number, count)
        return "updated", existing.html_url

    def _recorded_event_count(
        self,
        issue: Any,
        body: str,
        idempotency_key: str,
    ) -> int | None:
        event = _event_re(self._ns, idempotency_key).search(body)
        if event:
            return int(event.group(1))

        # Migrate bodies written by the old last-key-only implementation.
        last = _last_key_re(self._ns).search(body)
        if last and last.group(1) == idempotency_key:
            occurrence = _occurrence_re(self._ns).search(body)
            return int(occurrence.group(1)) if occurrence else 1

        comments = retry_github_call(
            lambda: list(issue.get_comments()),
            retries=2,
            description=f"read event ledger for issue #{issue.number}",
        )
        for comment in comments:
            match = _event_re(self._ns, idempotency_key).search(
                comment.body or "",
            )
            if match:
                return int(match.group(1))
        return None

    def _find_existing(self, repo_name: str, marker: str) -> Any:
        """Find an open issue containing the marker, or None."""
        query = f'"{marker}" in:body repo:{repo_name} is:issue is:open'
        results = retry_github_call(
            lambda: list(self._gh.search_issues(query)),
            retries=2,
            description="search issues",
        )
        for issue in results:
            if marker in (issue.body or ""):
                return self._reload(repo_name, issue.number)
        return None

    def _find_by_title(self, repo_name: str, title: str) -> Any:
        """Find an open issue whose title exactly equals ``title``, or None."""
        tokens = re.findall(r"\w+", title)
        if not tokens:
            return None
        phrase = " ".join(tokens[:10])
        query = f'{phrase} in:title repo:{repo_name} is:issue is:open'
        results = retry_github_call(
            lambda: list(self._gh.search_issues(query)),
            retries=2,
            description="search issues by title",
        )
        for issue in results:
            if issue.title == title:
                return self._reload(repo_name, issue.number)
        return None

    def _reload(self, repo_name: str, number: int) -> Any:
        """Reload an issue via its repo so we get a mutable issue handle."""
        return retry_github_call(
            lambda: self._gh.get_repo(repo_name).get_issue(number),
            retries=2,
            description=f"get issue #{number}",
        )


def _occurrence_re(namespace: str) -> re.Pattern[str]:
    return re.compile(rf"<!-- {re.escape(namespace)}:occurrences:(\d+) -->")


def _last_key_marker(namespace: str, key: str) -> str:
    return f"<!-- {namespace}:last-key:{key} -->"


def _last_key_re(namespace: str) -> re.Pattern[str]:
    return re.compile(rf"<!-- {re.escape(namespace)}:last-key:([^\s>]+) -->")


def _event_digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _event_marker(namespace: str, key: str, count: int) -> str:
    return f"<!-- {namespace}:event:{_event_digest(key)}:{count} -->"


def _event_re(namespace: str, key: str) -> re.Pattern[str]:
    return re.compile(
        rf"<!-- {re.escape(namespace)}:event:{_event_digest(key)}:(\d+) -->",
    )


def _body_with_state(
    body: str,
    *,
    namespace: str,
    marker: str,
    count: int,
    idempotency_key: str | None,
) -> str:
    if marker not in body:
        body = f"{marker}\n{body}".rstrip()
    occurrence = f"<!-- {namespace}:occurrences:{count} -->"
    if _occurrence_re(namespace).search(body):
        body = _occurrence_re(namespace).sub("", body)
    suffix = [occurrence]
    if idempotency_key is not None:
        last = _last_key_marker(namespace, idempotency_key)
        if _last_key_re(namespace).search(body):
            body = _last_key_re(namespace).sub("", body)
        suffix.append(last)
    return with_required_suffix(body.rstrip(), "\n".join(suffix))


def _bounded_content(content: IssueContent) -> IssueContent:
    return IssueContent(
        title=bounded_title(content.title),
        body=bounded_body(content.body),
        comment=bounded_comment(content.comment),
        labels=content.labels,
    )
