"""Repository-restricted, refreshing GitHub App installation authentication.

PyGithub can refresh installation tokens, but its built-in refresh request does
not preserve a repository allowlist. CI-fix needs both properties: campaigns
can outlive a one-hour token, and a target token must never expand from the
registry-selected repository to every repository in the App installation.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from github import Auth

from scripts.common.git_clone import REPO_RE
from scripts.common.github_client import (
    RETRYABLE_HTTP_STATUS,
    transient_backoff_delay,
)

logger = logging.getLogger(__name__)

_PERMISSION_RE = re.compile(r"^[a-z_]+$")
_PERMISSION_LEVELS = frozenset({"read", "write"})
_TOKEN_ENDPOINT = "https://api.github.com/app/installations/{installation_id}/access_tokens"
_TOKEN_REFRESH_MARGIN_SECONDS = 5 * 60
_INITIAL_TOKEN_CONSERVATIVE_LIFETIME_SECONDS = 50 * 60
_TOKEN_REQUEST_TIMEOUT_SECONDS = 30
_MAX_TOKEN_RESPONSE_BYTES = 1024 * 1024
_TOKEN_REQUEST_RETRIES = 3


class RepositoryInstallationAuth(Auth.Auth):
    """Refresh an installation token restricted to one exact repository."""

    def __init__(
        self,
        *,
        app_id: str,
        private_key: str,
        installation_id: int,
        repository: str,
        permissions: dict[str, str],
        initial_token: str = "",
    ) -> None:
        if not app_id.strip():
            raise ValueError("GitHub App id is required")
        if not private_key.strip():
            raise ValueError("GitHub App private key is required")
        if installation_id <= 0:
            raise ValueError("GitHub App installation id must be positive")
        if not REPO_RE.fullmatch(repository):
            raise ValueError(f"Malformed repository restriction: {repository!r}")
        if not permissions or any(
            not _PERMISSION_RE.fullmatch(name) or level not in _PERMISSION_LEVELS
            for name, level in permissions.items()
        ):
            raise ValueError("GitHub App token permissions are malformed")

        self._app_auth = Auth.AppAuth(app_id, private_key)
        self._installation_id = installation_id
        self._repository_name = repository.split("/", 1)[1]
        self._permissions = dict(permissions)
        self._lock = threading.Lock()
        self._token = initial_token
        self._expires_at = (
            time.time() + _INITIAL_TOKEN_CONSERVATIVE_LIFETIME_SECONDS
            if initial_token
            else 0.0
        )

    @property
    def token_type(self) -> str:
        return "token"

    @property
    def token(self) -> str:
        with self._lock:
            if (
                self._token
                and time.time() < self._expires_at - _TOKEN_REFRESH_MARGIN_SECONDS
            ):
                return self._token
            try:
                token, expires_at = self._mint()
            except Exception as exc:
                # The action-minted token normally lives for an hour. Its exact
                # expiry is not exposed, so the conservative local deadline
                # leaves room to retry a transient refresh outage without using
                # a token near its real expiry.
                if self._token and time.time() < self._expires_at:
                    logger.warning(
                        "GitHub App token refresh failed; using the still-valid "
                        "repository-scoped token: %s",
                        exc,
                    )
                    return self._token
                raise
            self._token = token
            self._expires_at = expires_at
            return token

    @property
    def _masked_token(self) -> str:
        return "token (installation token removed)"

    def _mint(self) -> tuple[str, float]:
        body = json.dumps(
            {
                "permissions": self._permissions,
                "repositories": [self._repository_name],
            },
            separators=(",", ":"),
        ).encode("utf-8")
        endpoint = _TOKEN_ENDPOINT.format(installation_id=self._installation_id)

        for attempt in range(_TOKEN_REQUEST_RETRIES):
            request = Request(
                endpoint,
                data=body,
                method="POST",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Content-Type": "application/json",
                    "User-Agent": "valkey-ci-agent",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            request.add_unredirected_header(
                "Authorization",
                f"Bearer {self._app_auth.token}",
            )
            try:
                with urlopen(request, timeout=_TOKEN_REQUEST_TIMEOUT_SECONDS) as response:
                    payload = _read_json_response(response)
                return _parse_token_response(payload)
            except HTTPError as exc:
                if exc.code not in RETRYABLE_HTTP_STATUS or attempt == _TOKEN_REQUEST_RETRIES - 1:
                    raise
            except (URLError, TimeoutError, ConnectionError):
                if attempt == _TOKEN_REQUEST_RETRIES - 1:
                    raise
            time.sleep(transient_backoff_delay(attempt))

        raise RuntimeError("unreachable: installation-token retry loop did not terminate")


def _read_json_response(response: Any) -> Any:
    payload = response.read(_MAX_TOKEN_RESPONSE_BYTES + 1)
    if len(payload) > _MAX_TOKEN_RESPONSE_BYTES:
        raise ValueError("GitHub App token response exceeded the size limit")
    return json.loads(payload.decode("utf-8"))


def _parse_token_response(payload: Any) -> tuple[str, float]:
    if not isinstance(payload, dict):
        raise ValueError("GitHub App token response was not an object")
    token = payload.get("token")
    expires_at = payload.get("expires_at")
    if not isinstance(token, str) or not token:
        raise ValueError("GitHub App token response did not contain a token")
    if not isinstance(expires_at, str) or not expires_at:
        raise ValueError("GitHub App token response did not contain an expiry")
    try:
        parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("GitHub App token response contained an invalid expiry") from exc
    if parsed.tzinfo is None:
        raise ValueError("GitHub App token expiry did not include a timezone")
    expiry_epoch = parsed.astimezone(timezone.utc).timestamp()
    if expiry_epoch <= time.time():
        raise ValueError("GitHub App token was already expired")
    return token, expiry_epoch
