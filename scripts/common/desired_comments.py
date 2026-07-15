"""Durable desired comments that converge after an expected branch update."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Any

from scripts.common.github_client import retry_github_call
from scripts.common.github_rest import GitHubRestClient
from scripts.common.markdown import bounded_comment
from scripts.common.phase_artifact import (
    ArtifactError,
    canonical_json_bytes,
    sha256_bytes,
)

_MARKER_PREFIX = "valkey-ci-agent:desired-comment:v1"
_MARKER_RE = re.compile(
    rf"<!-- {re.escape(_MARKER_PREFIX)} "
    r"payload=(?P<payload>[A-Za-z0-9_-]+) "
    r"sha256=(?P<sha>[0-9a-f]{64}) -->",
)
_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,240}$")
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_LOGIN_RE = re.compile(r"^[A-Za-z0-9-]{1,100}(?:\[bot\])?$")
_PAYLOAD_KEYS = {
    "version",
    "key",
    "expected_head_sha",
    "body",
    "reaction_comment_id",
    "reaction",
}
_MAX_BODY_BYTES = 20 * 1024


@dataclass(frozen=True)
class DesiredComment:
    key: str
    expected_head_sha: str
    body: str
    reaction_comment_id: int = 0
    reaction: str = ""


def record_desired_comment(
    issue: Any,
    desired: DesiredComment,
    *,
    writer_login: str,
) -> Any:
    """Create or update the pending record before its associated publication."""
    _validate_desired(desired)
    _validate_login(writer_login)
    matches = []
    for comment in retry_github_call(
        lambda: list(issue.get_comments()),
        retries=3,
        description=f"read desired comments for issue #{issue.number}",
    ):
        parsed = parse_desired_comment(str(getattr(comment, "body", "") or ""))
        if parsed is None or parsed.key != desired.key:
            continue
        if _login(comment) != writer_login:
            continue
        matches.append(comment)
    if len(matches) > 1:
        raise RuntimeError(f"multiple desired comments exist for {desired.key}")
    pending = _pending_body(desired)
    if matches:
        comment = matches[0]
        if str(getattr(comment, "body", "") or "") != pending:
            retry_github_call(
                lambda: comment.edit(pending),
                retries=3,
                description=f"refresh desired comment {desired.key}",
            )
        return comment
    return retry_github_call(
        lambda: issue.create_comment(pending),
        retries=3,
        description=f"record desired comment {desired.key}",
    )


def reconcile_desired_comment(
    repository: Any,
    comment: Any,
    desired: DesiredComment,
    *,
    current_head_sha: str,
) -> bool:
    """Publish final text and reaction once the expected branch head exists."""
    _validate_desired(desired)
    if current_head_sha != desired.expected_head_sha:
        return False
    final = _final_body(desired)
    if str(getattr(comment, "body", "") or "") != final:
        retry_github_call(
            lambda: comment.edit(final),
            retries=3,
            description=f"reconcile desired comment {desired.key}",
        )
    if desired.reaction_comment_id:
        GitHubRestClient(repository).add_issue_comment_reaction(
            str(getattr(repository, "full_name", "") or ""),
            desired.reaction_comment_id,
            desired.reaction,
        )
    return True


def reconcile_issue_desired_comments(
    repository: Any,
    issue: Any,
    *,
    current_head_sha: str,
    writer_login: str,
) -> int:
    """Converge every valid bot-authored desired comment on one pull request."""
    _validate_login(writer_login)
    reconciled = 0
    seen: set[str] = set()
    for comment in retry_github_call(
        lambda: list(issue.get_comments()),
        retries=3,
        description=f"scan desired comments for issue #{issue.number}",
    ):
        desired = parse_desired_comment(str(getattr(comment, "body", "") or ""))
        if desired is None or _login(comment) != writer_login:
            continue
        if desired.key in seen:
            raise RuntimeError(f"duplicate desired comment key {desired.key}")
        seen.add(desired.key)
        if reconcile_desired_comment(
            repository,
            comment,
            desired,
            current_head_sha=current_head_sha,
        ):
            reconciled += 1
    return reconciled


def parse_desired_comment(body: str) -> DesiredComment | None:
    matches = list(_MARKER_RE.finditer(body))
    if not matches:
        return None
    if len(matches) != 1:
        raise ArtifactError("comment contains multiple desired-state markers")
    match = matches[0]
    encoded = match.group("payload")
    padding = "=" * (-len(encoded) % 4)
    try:
        payload = base64.b64decode(
            encoded + padding,
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise ArtifactError("desired comment payload is invalid") from exc
    if not payload or len(payload) > 32 * 1024:
        raise ArtifactError("desired comment payload is empty or oversized")
    if sha256_bytes(payload) != match.group("sha"):
        raise ArtifactError("desired comment payload digest mismatch")
    try:
        raw = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ArtifactError("desired comment payload is not JSON") from exc
    if (
        not isinstance(raw, dict)
        or set(raw) != _PAYLOAD_KEYS
        or raw["version"] != 1
        or canonical_json_bytes(raw) != payload
    ):
        raise ArtifactError("desired comment payload schema is invalid")
    desired = DesiredComment(
        key=raw["key"],
        expected_head_sha=raw["expected_head_sha"],
        body=raw["body"],
        reaction_comment_id=raw["reaction_comment_id"],
        reaction=raw["reaction"],
    )
    _validate_desired(desired)
    return desired


def _marker(desired: DesiredComment) -> str:
    payload = canonical_json_bytes({
        "version": 1,
        "key": desired.key,
        "expected_head_sha": desired.expected_head_sha,
        "body": desired.body,
        "reaction_comment_id": desired.reaction_comment_id,
        "reaction": desired.reaction,
    })
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return (
        f"<!-- {_MARKER_PREFIX} payload={encoded} "
        f"sha256={sha256_bytes(payload)} -->"
    )


def _pending_body(desired: DesiredComment) -> str:
    return bounded_comment(
        "\n".join([
            _marker(desired),
            "CI metadata publication is pending the expected branch update "
            f"`{desired.expected_head_sha[:12]}`.",
        ]),
    )


def _final_body(desired: DesiredComment) -> str:
    return bounded_comment(f"{_marker(desired)}\n{desired.body}")


def _validate_desired(desired: DesiredComment) -> None:
    if not isinstance(desired.key, str) or not _KEY_RE.fullmatch(desired.key):
        raise ArtifactError("desired comment key is invalid")
    if (
        not isinstance(desired.expected_head_sha, str)
        or not _SHA1_RE.fullmatch(desired.expected_head_sha)
    ):
        raise ArtifactError("desired comment expected head SHA is invalid")
    if (
        not isinstance(desired.body, str)
        or len(desired.body.encode("utf-8")) > _MAX_BODY_BYTES
    ):
        raise ArtifactError("desired comment body is oversized")
    if (
        not isinstance(desired.reaction_comment_id, int)
        or isinstance(desired.reaction_comment_id, bool)
        or desired.reaction_comment_id < 0
    ):
        raise ArtifactError("desired reaction comment ID is invalid")
    if desired.reaction_comment_id:
        if desired.reaction not in {"+1", "-1"}:
            raise ArtifactError("desired reaction is invalid")
    elif desired.reaction:
        raise ArtifactError("desired reaction lacks a comment ID")


def _validate_login(value: str) -> None:
    if not _LOGIN_RE.fullmatch(value):
        raise ValueError("desired comment writer login is invalid")


def _login(value: Any) -> str:
    return str(getattr(getattr(value, "user", None), "login", "") or "")
