"""Tests for GitHub API retry helpers."""

from __future__ import annotations

import pytest
from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call


def test_retry_github_call_retries_retryable_errors(monkeypatch) -> None:
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise GithubException(429, {"message": "rate limit"})
        return "ok"

    monkeypatch.setattr("scripts.common.github_client.time.sleep", lambda _seconds: None)
    monkeypatch.setattr("scripts.common.github_client.random.uniform", lambda _a, _b: 0.0)

    result = retry_github_call(operation, retries=3, description="test call")

    assert result == "ok"
    assert calls["count"] == 3


@pytest.mark.parametrize("status", [401, 404])
def test_retry_github_call_does_not_retry_permanent_errors(status: int) -> None:
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        raise GithubException(status, {"message": "permanent"})

    with pytest.raises(GithubException):
        retry_github_call(operation, retries=3, description="test call")

    assert calls["count"] == 1


def test_retry_github_call_raises_after_exhausting_retries(monkeypatch) -> None:
    calls = {"count": 0}

    def operation() -> str:
        calls["count"] += 1
        raise GithubException(429, {"message": "rate limit"})

    monkeypatch.setattr("scripts.common.github_client.time.sleep", lambda _seconds: None)
    monkeypatch.setattr("scripts.common.github_client.random.uniform", lambda _a, _b: 0.0)

    with pytest.raises(GithubException):
        retry_github_call(operation, retries=3, description="test call")

    assert calls["count"] == 3


def test_retry_github_call_logs_exhausted_retries(monkeypatch, caplog) -> None:
    monkeypatch.setattr("scripts.common.github_client.time.sleep", lambda _seconds: None)

    def operation() -> str:
        raise GithubException(503, {"message": "unavailable"})

    with pytest.raises(GithubException):
        retry_github_call(operation, retries=2, description="get repo org/x")

    messages = [r.getMessage() for r in caplog.records]
    assert any("attempt 1/2" in m for m in messages)
    assert any(
        r.levelname == "ERROR" and "get repo org/x failed after 2 attempt(s)" in r.getMessage()
        for r in caplog.records
    )


def test_retry_github_call_does_not_log_permanent_errors(caplog) -> None:
    def operation() -> str:
        raise GithubException(404, {"message": "Not Found"})

    with pytest.raises(GithubException):
        retry_github_call(operation, retries=3, description="probe")

    # A 404 probe is often expected; the caller decides whether it matters.
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
