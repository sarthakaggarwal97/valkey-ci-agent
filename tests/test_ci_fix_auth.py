"""Tests for repository-restricted refreshing CI-fix authentication."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.error import URLError

import pytest

from scripts.ci_fix import auth as auth_mod
from scripts.ci_fix.auth import RepositoryInstallationAuth


class _Response:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit):
        return self._payload


def _expiry(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z",
    )


def _auth(monkeypatch, **overrides):
    monkeypatch.setattr(
        auth_mod.Auth,
        "AppAuth",
        lambda *_args, **_kwargs: SimpleNamespace(token="signed-app-jwt"),
    )
    return RepositoryInstallationAuth(
        app_id=overrides.get("app_id", "123"),
        private_key=overrides.get("private_key", "private-key"),
        installation_id=overrides.get("installation_id", 456),
        repository=overrides.get("repository", "valkey-io/valkey"),
        permissions=overrides.get(
            "permissions",
            {"actions": "write", "contents": "write", "metadata": "read"},
        ),
        initial_token=overrides.get("initial_token", ""),
    )


def test_refresh_request_preserves_exact_repository_and_permissions(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(auth_mod.time, "time", lambda: now)
    captured = []

    def open_request(request, timeout):
        captured.append((request, timeout))
        return _Response({"token": "target-token", "expires_at": _expiry(now + 3600)})

    monkeypatch.setattr(auth_mod, "urlopen", open_request)
    auth = _auth(monkeypatch)

    assert auth.token == "target-token"
    assert auth.token == "target-token"
    assert len(captured) == 1
    request, timeout = captured[0]
    assert request.full_url.endswith("/app/installations/456/access_tokens")
    assert timeout == auth_mod._TOKEN_REQUEST_TIMEOUT_SECONDS
    assert json.loads(request.data) == {
        "permissions": {
            "actions": "write",
            "contents": "write",
            "metadata": "read",
        },
        "repositories": ["valkey"],
    }
    assert "Authorization" not in request.headers
    assert request.unredirected_hdrs["Authorization"] == "Bearer signed-app-jwt"


def test_token_refreshes_before_expiry(monkeypatch):
    state = {"now": 1_700_000_000.0}
    monkeypatch.setattr(auth_mod.time, "time", lambda: state["now"])
    responses = iter([
        {"token": "one", "expires_at": _expiry(state["now"] + 3600)},
        {"token": "two", "expires_at": _expiry(state["now"] + 7200)},
    ])
    calls = []

    def open_request(request, timeout):
        calls.append(request)
        return _Response(next(responses))

    monkeypatch.setattr(auth_mod, "urlopen", open_request)
    auth = _auth(monkeypatch)

    assert auth.token == "one"
    state["now"] += 3301
    assert auth.token == "two"
    assert len(calls) == 2


def test_recent_action_token_survives_transient_refresh_outage(monkeypatch):
    state = {"now": 1_700_000_000.0}
    monkeypatch.setattr(auth_mod.time, "time", lambda: state["now"])
    monkeypatch.setattr(auth_mod.time, "sleep", lambda _seconds: None)
    calls = []

    def unavailable(request, timeout):
        calls.append(request)
        raise URLError("temporary outage")

    monkeypatch.setattr(auth_mod, "urlopen", unavailable)
    auth = _auth(monkeypatch, initial_token="action-token")

    assert auth.token == "action-token"
    state["now"] += auth_mod._INITIAL_TOKEN_CONSERVATIVE_LIFETIME_SECONDS - 299
    assert auth.token == "action-token"
    assert len(calls) == auth_mod._TOKEN_REQUEST_RETRIES


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("installation_id", 0),
        ("repository", "not-a-repository"),
        ("permissions", {"contents": "admin"}),
    ],
)
def test_rejects_malformed_scope(monkeypatch, field, value):
    with pytest.raises(ValueError):
        _auth(monkeypatch, **{field: value})


def test_rejects_expired_token_response(monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(auth_mod.time, "time", lambda: now)
    monkeypatch.setattr(
        auth_mod,
        "urlopen",
        lambda *_args, **_kwargs: _Response(
            {"token": "stale", "expires_at": _expiry(now - 1)}
        ),
    )

    with pytest.raises(ValueError, match="already expired"):
        _auth(monkeypatch).token
