"""Durable high-water state for scheduled fuzzer run processing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from scripts.common.github_client import retry_github_call

STATE_NAMESPACE = "valkey-ci-agent:fuzzer-monitor-state"
STATE_MARKER = f"<!-- {STATE_NAMESPACE}:v1 -->"
STATE_TITLE = "[valkey-ci-agent] Fuzzer monitor state"
_CURSOR_RE = re.compile(
    rf"<!-- {re.escape(STATE_NAMESPACE)}:cursor:(\d+) -->",
)


@dataclass(frozen=True)
class FuzzerState:
    cursor: int
    issue_number: int | None


class FuzzerStateStore:
    """Stores the last consecutively published run in a dedicated issue."""

    def __init__(self, github_client: Any) -> None:
        self._gh = github_client

    def read(self, repo_name: str) -> FuzzerState:
        matches = self._find(repo_name)
        if not matches:
            return FuzzerState(cursor=0, issue_number=None)
        if len(matches) != 1:
            raise RuntimeError(
                f"found {len(matches)} fuzzer state issues; expected exactly one",
            )
        issue = matches[0]
        body = issue.body or ""
        cursor_matches = _CURSOR_RE.findall(body)
        if len(cursor_matches) != 1:
            raise RuntimeError("fuzzer state issue has an invalid cursor marker")
        cursor = int(cursor_matches[0])
        if cursor <= 0:
            raise RuntimeError("fuzzer state cursor must be positive")
        return FuzzerState(cursor=cursor, issue_number=int(issue.number))

    def advance(
        self,
        repo_name: str,
        *,
        expected_cursor: int,
        run_id: int,
        run_url: str,
        analysis_sha256: str,
    ) -> FuzzerState:
        """Compare-and-set the cursor after one event is fully reconciled."""
        if run_id <= expected_cursor:
            raise ValueError("new fuzzer cursor must be greater than the expected cursor")
        current = self.read(repo_name)
        if current.cursor != expected_cursor:
            raise RuntimeError(
                f"fuzzer cursor changed from {expected_cursor} to {current.cursor}",
            )
        body = _render_state_body(
            cursor=run_id,
            run_url=run_url,
            analysis_sha256=analysis_sha256,
        )
        repo = retry_github_call(
            lambda: self._gh.get_repo(repo_name),
            retries=3,
            description=f"get repo {repo_name}",
        )
        if current.issue_number is None:
            issue = retry_github_call(
                lambda: repo.create_issue(title=STATE_TITLE, body=body),
                retries=3,
                description="create fuzzer state issue",
            )
            return FuzzerState(cursor=run_id, issue_number=int(issue.number))

        issue = retry_github_call(
            lambda: repo.get_issue(current.issue_number),
            retries=3,
            description=f"get fuzzer state issue #{current.issue_number}",
        )
        retry_github_call(
            lambda: issue.edit(title=STATE_TITLE, body=body, state="open"),
            retries=3,
            description=f"advance fuzzer state issue #{current.issue_number}",
        )
        return FuzzerState(cursor=run_id, issue_number=current.issue_number)

    def _find(self, repo_name: str) -> list[Any]:
        query = f'"{STATE_MARKER}" in:body repo:{repo_name} is:issue'
        results = retry_github_call(
            lambda: list(self._gh.search_issues(query)),
            retries=3,
            description="find fuzzer state issue",
        )
        return [item for item in results if STATE_MARKER in (item.body or "")]


def _render_state_body(*, cursor: int, run_url: str, analysis_sha256: str) -> str:
    return "\n".join([
        STATE_MARKER,
        f"<!-- {STATE_NAMESPACE}:cursor:{cursor} -->",
        "",
        "This issue is durable automation state for the scheduled fuzzer monitor.",
        "Do not edit or close it while the monitor is enabled.",
        "",
        f"Last consecutively processed run: [{cursor}]({run_url})",
        f"Analysis artifact SHA-256: `{analysis_sha256}`",
    ])
