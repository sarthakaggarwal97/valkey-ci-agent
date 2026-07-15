"""Append-only GitHub issue ledger for comment-triggered CI-fix dispatch."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass, replace
from typing import Any, Literal

from scripts.common.github_client import retry_github_call
from scripts.common.phase_artifact import canonical_json_bytes, sha256_bytes

LEDGER_NAMESPACE = "valkey-ci-agent:ci-fix-dispatch-ledger"
LEDGER_MARKER = f"<!-- {LEDGER_NAMESPACE}:v1 -->"
LEDGER_TITLE = "[valkey-ci-agent] CI-fix dispatch ledger"
_EVENT_PREFIX = f"<!-- {LEDGER_NAMESPACE}:event:"
_MAX_EVENTS = 10_000
_MAX_COMMENT_BYTES = 16 * 1024
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_REPO_PART_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_LOGIN_RE = re.compile(r"^[A-Za-z0-9-]{1,100}(?:\[bot\])?$")
_CORRELATION_RE = re.compile(r"^[0-9a-f]{32}$")
_STATES = {"observed", "authorized", "dispatching", "dispatched", "completed"}
_KEYS = {
    "version",
    "kind",
    "comment_id",
    "correlation_id",
    "state",
    "repository",
    "pr_number",
    "run_owner",
    "run_repo",
    "source_run_id",
    "hint",
    "commenter",
    "authorization_attempts",
    "dispatch_attempts",
    "workflow_run_id",
    "sequence",
    "observed_at",
    "updated_at",
    "completion",
}
_TRANSITIONS = {
    "observed": {"observed", "authorized", "completed"},
    "authorized": {"dispatching"},
    "dispatching": {"dispatching", "dispatched"},
    "dispatched": {"completed"},
    "completed": set(),
}

DispatchState = Literal[
    "observed",
    "authorized",
    "dispatching",
    "dispatched",
    "completed",
]


@dataclass(frozen=True)
class DispatchEvent:
    comment_id: int
    correlation_id: str
    state: DispatchState
    repository: str
    pr_number: int
    run_owner: str
    run_repo: str
    source_run_id: int
    hint: str
    commenter: str
    authorization_attempts: int = 0
    dispatch_attempts: int = 0
    workflow_run_id: int = 0
    sequence: int = 1
    observed_at: int = 0
    updated_at: int = 0
    completion: str = ""


class DispatchLedger:
    """Serialized append-only event snapshots stored as issue comments."""

    def __init__(
        self,
        github_client: Any,
        *,
        agent_repo: str,
        writer_login: str,
    ) -> None:
        if not _LOGIN_RE.fullmatch(writer_login):
            raise ValueError("dispatch ledger writer login is invalid")
        self._gh = github_client
        self._agent_repo = agent_repo
        self._writer_login = writer_login
        self._issue: Any | None = None
        self._loaded = False
        self._events: dict[int, DispatchEvent] = {}

    def load(self) -> None:
        if self._loaded:
            return
        matches = self._find_issues()
        if len(matches) > 1:
            raise RuntimeError("multiple CI-fix dispatch ledger issues exist")
        if not matches:
            self._loaded = True
            return
        issue = self._reload(matches[0].number)
        events: dict[int, DispatchEvent] = {}
        comments = retry_github_call(
            lambda: list(issue.get_comments()),
            retries=3,
            description="read CI-fix dispatch ledger",
        )
        if len(comments) > _MAX_EVENTS:
            raise RuntimeError("CI-fix dispatch ledger exceeds its event cap")
        for comment in comments:
            body = str(getattr(comment, "body", "") or "")
            if _EVENT_PREFIX not in body:
                continue
            if _object_login(comment) != self._writer_login:
                continue
            event = parse_event_comment(body)
            previous = events.get(event.comment_id)
            if previous is None:
                if (
                    event.state != "observed"
                    or event.sequence != 1
                    or event.authorization_attempts
                ):
                    raise RuntimeError("dispatch ledger event does not start at observed")
            elif previous == event:
                # An ambiguous POST retry can commit the same immutable snapshot
                # twice. Treat the byte-identical replay as one append.
                continue
            else:
                _validate_transition(previous, event)
            events[event.comment_id] = event
        self._issue = issue
        self._events = events
        self._loaded = True

    def get(self, comment_id: int) -> DispatchEvent | None:
        self.load()
        return self._events.get(comment_id)

    def active(self) -> list[DispatchEvent]:
        self.load()
        return sorted(
            (
                event
                for event in self._events.values()
                if event.state != "completed"
            ),
            key=lambda event: (event.updated_at, event.comment_id),
        )

    def observe(
        self,
        *,
        comment_id: int,
        repository: str,
        pr_number: int,
        run_owner: str,
        run_repo: str,
        source_run_id: int,
        hint: str,
        commenter: str,
        now: int | None = None,
    ) -> DispatchEvent:
        self.load()
        existing = self._events.get(comment_id)
        if existing is not None:
            return existing
        correlation = hashlib.sha256(
            f"{repository}:{comment_id}".encode("utf-8"),
        ).hexdigest()[:32]
        timestamp = int(time.time() if now is None else now)
        event = DispatchEvent(
            comment_id=comment_id,
            correlation_id=correlation,
            state="observed",
            repository=repository,
            pr_number=pr_number,
            run_owner=run_owner,
            run_repo=run_repo,
            source_run_id=source_run_id,
            hint=_truncate_utf8(hint, 500),
            commenter=commenter,
            observed_at=timestamp,
            updated_at=timestamp,
        )
        self._append(event)
        return event

    def transition(
        self,
        event: DispatchEvent,
        state: DispatchState,
        *,
        authorization_attempts: int | None = None,
        dispatch_attempts: int | None = None,
        workflow_run_id: int | None = None,
        completion: str | None = None,
        now: int | None = None,
    ) -> DispatchEvent:
        self.load()
        current = self._events.get(event.comment_id)
        if current != event:
            raise RuntimeError("dispatch ledger transition used stale state")
        next_event = replace(
            event,
            state=state,
            authorization_attempts=(
                event.authorization_attempts
                if authorization_attempts is None
                else authorization_attempts
            ),
            dispatch_attempts=(
                event.dispatch_attempts
                if dispatch_attempts is None
                else dispatch_attempts
            ),
            workflow_run_id=(
                event.workflow_run_id
                if workflow_run_id is None
                else workflow_run_id
            ),
            completion=event.completion if completion is None else completion,
            sequence=event.sequence + 1,
            updated_at=int(time.time() if now is None else now),
        )
        _validate_transition(event, next_event)
        self._append(next_event)
        return next_event

    def _append(self, event: DispatchEvent) -> None:
        if _event_from_dict(_event_to_dict(event)) != event:
            raise ValueError("dispatch event does not satisfy the ledger schema")
        issue = self._ensure_issue()
        body = format_event_comment(event)
        try:
            retry_github_call(
                lambda: issue.create_comment(body),
                retries=1,
                description=f"append dispatch state for comment {event.comment_id}",
            )
        except Exception:
            comments = retry_github_call(
                lambda: list(issue.get_comments()),
                retries=3,
                description="reconcile ambiguous dispatch ledger append",
            )
            if not any(
                str(getattr(comment, "body", "") or "") == body
                and _object_login(comment) == self._writer_login
                for comment in comments
            ):
                raise
        self._events[event.comment_id] = event

    def _ensure_issue(self) -> Any:
        if self._issue is not None:
            return self._issue
        repo = retry_github_call(
            lambda: self._gh.get_repo(self._agent_repo),
            retries=3,
            description=f"get agent repo {self._agent_repo}",
        )
        body = "\n".join([
            LEDGER_MARKER,
            "",
            "Append-only state for comment-triggered CI-fix dispatch.",
            "Do not edit or close while the poller is enabled.",
        ])
        try:
            self._issue = retry_github_call(
                lambda: repo.create_issue(title=LEDGER_TITLE, body=body),
                retries=1,
                description="create CI-fix dispatch ledger issue",
            )
        except Exception:
            matches = self._find_issues()
            if len(matches) != 1:
                raise
            self._issue = self._reload(matches[0].number)
        return self._issue

    def _find_issues(self) -> list[Any]:
        query = f'"{LEDGER_MARKER}" in:body repo:{self._agent_repo} is:issue'
        values = retry_github_call(
            lambda: list(self._gh.search_issues(query)),
            retries=3,
            description="find CI-fix dispatch ledger",
        )
        return [
            value
            for value in values
            if LEDGER_MARKER in (value.body or "")
            and _object_login(value) == self._writer_login
        ]

    def _reload(self, number: int) -> Any:
        issue = retry_github_call(
            lambda: self._gh.get_repo(self._agent_repo).get_issue(number),
            retries=3,
            description=f"get CI-fix dispatch ledger issue #{number}",
        )
        if (
            LEDGER_MARKER not in str(getattr(issue, "body", "") or "")
            or _object_login(issue) != self._writer_login
        ):
            raise RuntimeError("CI-fix dispatch ledger identity changed")
        return issue


def format_event_comment(event: DispatchEvent) -> str:
    value = _event_to_dict(event)
    payload = canonical_json_bytes(value)
    encoded = base64.urlsafe_b64encode(payload).decode("ascii")
    body = (
        f"{_EVENT_PREFIX}{encoded}:{sha256_bytes(payload)} -->"
    )
    if len(body.encode("utf-8")) > _MAX_COMMENT_BYTES:
        raise ValueError("dispatch event comment exceeds its size limit")
    return body


def parse_event_comment(body: str) -> DispatchEvent:
    lines = [line.strip() for line in body.splitlines() if line.strip().startswith(_EVENT_PREFIX)]
    if len(lines) != 1 or not lines[0].endswith(" -->"):
        raise ValueError("dispatch comment must contain exactly one event marker")
    encoded_and_digest = lines[0][len(_EVENT_PREFIX):-4]
    try:
        encoded, digest = encoded_and_digest.rsplit(":", 1)
    except ValueError as exc:
        raise ValueError("dispatch event marker is malformed") from exc
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("dispatch event digest is malformed")
    try:
        payload = base64.b64decode(encoded, altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("dispatch event payload is malformed") from exc
    if sha256_bytes(payload) != digest:
        raise ValueError("dispatch event digest does not match")
    try:
        raw = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("dispatch event payload is not JSON") from exc
    event = _event_from_dict(raw)
    if canonical_json_bytes(_event_to_dict(event)) != payload:
        raise ValueError("dispatch event payload is not canonical")
    return event


def _event_to_dict(event: DispatchEvent) -> dict[str, Any]:
    return {
        "version": 1,
        "kind": "ci-fix-dispatch-event",
        "comment_id": event.comment_id,
        "correlation_id": event.correlation_id,
        "state": event.state,
        "repository": event.repository,
        "pr_number": event.pr_number,
        "run_owner": event.run_owner,
        "run_repo": event.run_repo,
        "source_run_id": event.source_run_id,
        "hint": event.hint,
        "commenter": event.commenter,
        "authorization_attempts": event.authorization_attempts,
        "dispatch_attempts": event.dispatch_attempts,
        "workflow_run_id": event.workflow_run_id,
        "sequence": event.sequence,
        "observed_at": event.observed_at,
        "updated_at": event.updated_at,
        "completion": event.completion,
    }


def _event_from_dict(raw: Any) -> DispatchEvent:
    if not isinstance(raw, dict) or set(raw) != _KEYS:
        raise ValueError("dispatch event keys are invalid")
    if raw["version"] != 1 or raw["kind"] != "ci-fix-dispatch-event":
        raise ValueError("dispatch event version or kind is invalid")
    for key in (
        "comment_id",
        "pr_number",
        "source_run_id",
        "sequence",
        "observed_at",
        "updated_at",
    ):
        if (
            not isinstance(raw[key], int)
            or isinstance(raw[key], bool)
            or raw[key] <= 0
        ):
            raise ValueError(f"dispatch event {key} is invalid")
    for key in ("authorization_attempts", "dispatch_attempts", "workflow_run_id"):
        if (
            not isinstance(raw[key], int)
            or isinstance(raw[key], bool)
            or raw[key] < 0
        ):
            raise ValueError(f"dispatch event {key} is invalid")
    if raw["state"] not in _STATES:
        raise ValueError("dispatch event state is invalid")
    if not isinstance(raw["repository"], str) or not _REPO_RE.fullmatch(raw["repository"]):
        raise ValueError("dispatch event repository is invalid")
    for key in ("run_owner", "commenter"):
        if not isinstance(raw[key], str) or not _LOGIN_RE.fullmatch(raw[key]):
            raise ValueError(f"dispatch event {key} is invalid")
    if not isinstance(raw["run_repo"], str) or not _REPO_PART_RE.fullmatch(
        raw["run_repo"],
    ):
        raise ValueError("dispatch event run_repo is invalid")
    if not isinstance(raw["correlation_id"], str) or not _CORRELATION_RE.fullmatch(
        raw["correlation_id"],
    ):
        raise ValueError("dispatch event correlation ID is invalid")
    for key, limit in (("hint", 500), ("completion", 200)):
        if not isinstance(raw[key], str) or len(raw[key].encode("utf-8")) > limit:
            raise ValueError(f"dispatch event {key} is invalid")
    event = DispatchEvent(
        comment_id=raw["comment_id"],
        correlation_id=raw["correlation_id"],
        state=raw["state"],
        repository=raw["repository"],
        pr_number=raw["pr_number"],
        run_owner=raw["run_owner"],
        run_repo=raw["run_repo"],
        source_run_id=raw["source_run_id"],
        hint=raw["hint"],
        commenter=raw["commenter"],
        authorization_attempts=raw["authorization_attempts"],
        dispatch_attempts=raw["dispatch_attempts"],
        workflow_run_id=raw["workflow_run_id"],
        sequence=raw["sequence"],
        observed_at=raw["observed_at"],
        updated_at=raw["updated_at"],
        completion=raw["completion"],
    )
    _validate_snapshot(event)
    return event


def _validate_transition(previous: DispatchEvent, current: DispatchEvent) -> None:
    _validate_snapshot(current)
    immutable = (
        "comment_id",
        "correlation_id",
        "repository",
        "pr_number",
        "run_owner",
        "run_repo",
        "source_run_id",
        "hint",
        "commenter",
        "observed_at",
    )
    if any(getattr(previous, key) != getattr(current, key) for key in immutable):
        raise RuntimeError("dispatch event immutable identity changed")
    if current.state not in _TRANSITIONS[previous.state]:
        raise RuntimeError(
            f"invalid dispatch transition {previous.state}->{current.state}",
        )
    if current.sequence != previous.sequence + 1:
        raise RuntimeError("dispatch event sequence is not consecutive")
    if current.updated_at < previous.updated_at:
        raise RuntimeError("dispatch event timestamp moved backwards")
    if current.updated_at < current.observed_at:
        raise RuntimeError("dispatch event predates observation")
    if current.authorization_attempts < previous.authorization_attempts:
        raise RuntimeError("authorization attempts moved backwards")
    if current.dispatch_attempts < previous.dispatch_attempts:
        raise RuntimeError("dispatch attempts moved backwards")
    if previous.state == "observed" and current.state == "observed":
        if current.authorization_attempts != previous.authorization_attempts + 1:
            raise RuntimeError("authorization retry did not increment attempts")
    elif current.authorization_attempts != previous.authorization_attempts:
        raise RuntimeError("authorization attempts changed outside observed state")
    if current.state == "dispatching":
        if current.dispatch_attempts != previous.dispatch_attempts + 1:
            raise RuntimeError("dispatching transition did not increment attempts")
    elif current.dispatch_attempts != previous.dispatch_attempts:
        raise RuntimeError("dispatch attempts changed outside dispatching state")
    if current.state in {"observed", "authorized"} and (
        current.workflow_run_id or current.completion
    ):
        raise RuntimeError("pre-dispatch event contains terminal state")
    if current.state == "dispatching" and (
        current.workflow_run_id or current.completion
    ):
        raise RuntimeError("dispatching event contains terminal state")
    if current.state == "dispatched" and current.workflow_run_id <= 0:
        raise RuntimeError("dispatched event has no workflow run ID")
    if current.state == "dispatched" and current.completion:
        raise RuntimeError("dispatched event already has an outcome")
    if current.state == "completed" and not current.completion:
        raise RuntimeError("completed event has no outcome")


def _validate_snapshot(event: DispatchEvent) -> None:
    if event.updated_at < event.observed_at:
        raise RuntimeError("dispatch event predates observation")
    if event.state == "observed" and (
        event.dispatch_attempts or event.workflow_run_id or event.completion
    ):
        raise RuntimeError("observed event contains later-phase state")
    if event.state == "authorized" and (
        event.dispatch_attempts or event.workflow_run_id or event.completion
    ):
        raise RuntimeError("authorized event contains later-phase state")
    if event.state == "dispatching" and (
        event.dispatch_attempts <= 0
        or event.workflow_run_id
        or event.completion
    ):
        raise RuntimeError("dispatching event state is inconsistent")
    if event.state == "dispatched" and (
        event.dispatch_attempts <= 0
        or event.workflow_run_id <= 0
        or event.completion
    ):
        raise RuntimeError("dispatched event state is inconsistent")
    if event.state == "completed" and not event.completion:
        raise RuntimeError("completed event has no outcome")


def _truncate_utf8(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def _object_login(value: Any) -> str:
    return str(getattr(getattr(value, "user", None), "login", "") or "")
