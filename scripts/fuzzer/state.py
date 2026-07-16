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
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class FuzzerState:
    cursor: int
    issue_number: int | None


class FuzzerStateStore:
    """Store the last consecutively reconciled run in a dedicated issue."""

    def __init__(self, github_client: Any) -> None:
        self._gh = github_client
        self._known_issue_numbers: dict[str, int] = {}

    def read(self, repo_name: str) -> FuzzerState:
        matches = self._find(repo_name)
        if not matches:
            return FuzzerState(cursor=0, issue_number=None)
        if len(matches) != 1:
            raise RuntimeError(f"found {len(matches)} fuzzer state issues; expected exactly one")
        state = self._parse_issue(matches[0])
        if state.issue_number is not None:
            self._known_issue_numbers[repo_name] = state.issue_number
        return state

    def initialize(
        self,
        repo_name: str,
        *,
        first_run_id: int,
        first_run_url: str,
    ) -> FuzzerState:
        """Persist an anchor immediately before the first live bootstrap run."""
        if (
            not isinstance(first_run_id, int)
            or isinstance(first_run_id, bool)
            or first_run_id <= 1
        ):
            raise ValueError("bootstrap fuzzer run ID must be greater than one")
        self._validate_run_url(first_run_url)
        current = self.read(repo_name)
        if current.cursor != 0 or current.issue_number is not None:
            raise RuntimeError("fuzzer state was initialized concurrently")

        cursor = first_run_id - 1
        body = _render_bootstrap_body(
            cursor=cursor,
            first_run_id=first_run_id,
            first_run_url=first_run_url,
        )
        repo = retry_github_call(
            lambda: self._gh.get_repo(repo_name),
            retries=3,
            description=f"get repo {repo_name}",
        )
        issue = retry_github_call(
            lambda: repo.create_issue(title=STATE_TITLE, body=body),
            retries=1,
            description="create fuzzer state issue",
        )
        issue_number = getattr(issue, "number", None)
        if not isinstance(issue_number, int) or issue_number <= 0:
            raise RuntimeError("created fuzzer state issue has no valid number")
        self._known_issue_numbers[repo_name] = issue_number
        return FuzzerState(cursor=cursor, issue_number=issue_number)

    @staticmethod
    def _parse_issue(issue: Any) -> FuzzerState:
        body = str(getattr(issue, "body", "") or "")
        if STATE_MARKER not in body:
            raise RuntimeError("fuzzer state issue has no state marker")
        cursor_matches = _CURSOR_RE.findall(body)
        if len(cursor_matches) != 1:
            raise RuntimeError("fuzzer state issue has an invalid cursor marker")
        cursor = int(cursor_matches[0])
        if cursor <= 0:
            raise RuntimeError("fuzzer state cursor must be positive")
        issue_number = getattr(issue, "number", None)
        if not isinstance(issue_number, int) or isinstance(issue_number, bool) or issue_number <= 0:
            raise RuntimeError("fuzzer state issue has an invalid issue number")
        return FuzzerState(cursor=cursor, issue_number=issue_number)

    def advance(
        self,
        repo_name: str,
        *,
        expected_cursor: int,
        run_id: int,
        run_url: str,
        result_sha256: str,
    ) -> FuzzerState:
        """Compare the cursor, then advance it after one run is reconciled."""
        if (
            not isinstance(expected_cursor, int)
            or isinstance(expected_cursor, bool)
            or expected_cursor < 0
        ):
            raise ValueError("expected fuzzer cursor must be non-negative")
        if (
            not isinstance(run_id, int)
            or isinstance(run_id, bool)
            or run_id <= expected_cursor
        ):
            raise ValueError("new fuzzer cursor must be greater than the expected cursor")
        if not _SHA256_RE.fullmatch(result_sha256):
            raise ValueError("fuzzer result digest must be a lowercase SHA-256")
        self._validate_run_url(run_url)

        known_issue_number = self._known_issue_numbers.get(repo_name)
        issue = None
        repo = None
        if known_issue_number is not None:
            repo = retry_github_call(
                lambda: self._gh.get_repo(repo_name),
                retries=3,
                description=f"get repo {repo_name}",
            )
            repo_obj = repo
            issue = retry_github_call(
                lambda: repo_obj.get_issue(known_issue_number),
                retries=3,
                description=f"get fuzzer state issue #{known_issue_number}",
            )
            current = self._parse_issue(issue)
        else:
            current = self.read(repo_name)
        if current.cursor != expected_cursor:
            raise RuntimeError(f"fuzzer cursor changed from {expected_cursor} to {current.cursor}")

        body = _render_state_body(
            cursor=run_id,
            run_url=run_url,
            result_sha256=result_sha256,
        )
        if repo is None:
            repo = retry_github_call(
                lambda: self._gh.get_repo(repo_name),
                retries=3,
                description=f"get repo {repo_name}",
            )
        if current.issue_number is None:
            issue = retry_github_call(
                lambda: repo.create_issue(title=STATE_TITLE, body=body),
                retries=1,
                description="create fuzzer state issue",
            )
            issue_number = getattr(issue, "number", None)
            if not isinstance(issue_number, int) or issue_number <= 0:
                raise RuntimeError("created fuzzer state issue has no valid number")
            self._known_issue_numbers[repo_name] = issue_number
            return FuzzerState(cursor=run_id, issue_number=issue_number)

        if issue is None:
            assert repo is not None
            repo_obj = repo
            issue = retry_github_call(
                lambda: repo_obj.get_issue(current.issue_number),
                retries=3,
                description=f"get fuzzer state issue #{current.issue_number}",
            )
            exact = self._parse_issue(issue)
            if exact.cursor != expected_cursor:
                raise RuntimeError(f"fuzzer cursor changed from {expected_cursor} to {exact.cursor}")
        retry_github_call(
            lambda: issue.edit(title=STATE_TITLE, body=body, state="open"),
            retries=3,
            description=f"advance fuzzer state issue #{current.issue_number}",
        )
        self._known_issue_numbers[repo_name] = current.issue_number
        return FuzzerState(cursor=run_id, issue_number=current.issue_number)

    @staticmethod
    def _validate_run_url(run_url: str) -> None:
        if "\n" in run_url or "\r" in run_url or not run_url.startswith("https://github.com/"):
            raise ValueError("fuzzer run URL is invalid")

    def _find(self, repo_name: str) -> list[Any]:
        query = f'"{STATE_MARKER}" in:body repo:{repo_name} is:issue'
        results = retry_github_call(
            lambda: list(self._gh.search_issues(query)),
            retries=3,
            description="find fuzzer state issue",
        )
        return [item for item in results if STATE_MARKER in str(getattr(item, "body", "") or "")]


def _render_state_body(*, cursor: int, run_url: str, result_sha256: str) -> str:
    return "\n".join(
        [
            STATE_MARKER,
            f"<!-- {STATE_NAMESPACE}:cursor:{cursor} -->",
            "",
            "This issue is durable automation state for the scheduled fuzzer monitor.",
            "Do not edit or close it while the monitor is enabled.",
            "",
            f"Last consecutively processed run: [{cursor}]({run_url})",
            f"Reconciliation result SHA-256: `{result_sha256}`",
        ]
    )


def _render_bootstrap_body(
    *,
    cursor: int,
    first_run_id: int,
    first_run_url: str,
) -> str:
    return "\n".join(
        [
            STATE_MARKER,
            f"<!-- {STATE_NAMESPACE}:cursor:{cursor} -->",
            "",
            "This issue is durable automation state for the scheduled fuzzer monitor.",
            "Do not edit or close it while the monitor is enabled.",
            "",
            f"Bootstrap pending run: [{first_run_id}]({first_run_url})",
            f"High-water run ID immediately before pending run: `{cursor}`",
        ]
    )
