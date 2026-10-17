"""Tests for GitHub API retry helpers."""

from __future__ import annotations

import io
import urllib.error
import urllib.request
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.common.github_client import dispatch_workflow, ensure_label, retry_github_call, urlopen_with_retry


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


def test_retry_github_call_honors_retry_after(monkeypatch) -> None:
    calls = {"count": 0}
    sleeps: list[float] = []

    def operation() -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise GithubException(429, {"message": "rate limit"}, {"Retry-After": "17"})
        return "ok"

    monkeypatch.setattr("scripts.common.github_client.time.sleep", sleeps.append)

    assert retry_github_call(operation, retries=2, description="test call") == "ok"
    assert sleeps == [17.0]


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


@pytest.mark.parametrize("accepted", [True, None])
def test_dispatch_workflow_accepts_true_or_an_empty_204(accepted) -> None:
    workflow = MagicMock()
    workflow.create_dispatch.return_value = accepted

    dispatch_workflow(workflow, "main", {"issue": "7"}, retries=1, description="fix for #7")

    workflow.create_dispatch.assert_called_once_with("main", inputs={"issue": "7"})


def test_dispatch_workflow_raises_when_github_rejects_it() -> None:
    workflow = MagicMock()
    workflow.create_dispatch.return_value = False

    with pytest.raises(RuntimeError, match="rejected the dispatch: fix for #7"):
        dispatch_workflow(workflow, "main", {"issue": "7"}, retries=1, description="fix for #7")


def test_ensure_label_creates_a_missing_label_and_tolerates_a_concurrent_create() -> None:
    repo = MagicMock()
    repo.get_label.side_effect = GithubException(404, {"message": "Not Found"})
    repo.create_label.side_effect = GithubException(422, {"message": "already_exists"})

    ensure_label(repo, "release-tracking", color="1d76db", description="d")

    repo.create_label.assert_called_once_with(name="release-tracking", color="1d76db", description="d")


def test_ensure_label_raises_on_other_errors() -> None:
    repo = MagicMock()
    repo.get_label.side_effect = GithubException(403, {"message": "Forbidden"})

    with pytest.raises(GithubException):
        ensure_label(repo, "release-tracking", color="1d76db", description="d")
    repo.create_label.assert_not_called()


def test_urlopen_with_retry_retries_transient_statuses_and_flagged_bodies(monkeypatch) -> None:
    responses = [
        urllib.error.HTTPError("https://api.github.com/x", 502, "bad gateway", {}, None),
        b"busy",
        b"ok",
    ]

    def fake_urlopen(request, timeout):
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return io.BytesIO(response)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr("scripts.common.github_client.time.sleep", lambda _seconds: None)

    body = urlopen_with_retry(
        urllib.request.Request("https://api.github.com/x"), timeout=1, retries=3, description="x",
        retry_body=lambda body: body == b"busy",
    )

    assert body == b"ok"
    assert responses == []


def test_urlopen_with_retry_raises_a_permanent_status_at_once(monkeypatch) -> None:
    calls = []

    def fake_urlopen(request, timeout):
        calls.append(request)
        raise urllib.error.HTTPError("https://api.github.com/x", 404, "missing", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(urllib.error.HTTPError):
        urlopen_with_retry(urllib.request.Request("https://api.github.com/x"), timeout=1, retries=3, description="x")
    assert len(calls) == 1

