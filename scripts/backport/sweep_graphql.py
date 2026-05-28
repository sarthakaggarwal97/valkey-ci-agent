"""Small GraphQL client used by scheduled backport sweeps."""

from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


class GitHubGraphQLClient:
    def __init__(self, token: str) -> None:
        self._token = token

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps({"query": query, "variables": variables}).encode()
        last_exc: Exception | None = None
        body: str | None = None
        for attempt in range(4):
            request = urllib.request.Request(
                "https://api.github.com/graphql",
                data=payload,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    body = response.read().decode("utf-8")
                break
            except urllib.error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="replace")
                if exc.code in (429, 500, 502, 503, 504) and attempt < 3:
                    last_exc = exc
                    wait = random.uniform(0, min(8.0, 1.0 * (2 ** attempt)))
                    logger.warning(
                        "GraphQL %d retry after %.2fs: %s",
                        exc.code, wait, details[:200],
                    )
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"GraphQL failed: {exc.code} {details}") from exc
            except urllib.error.URLError as exc:
                if attempt < 3:
                    last_exc = exc
                    wait = random.uniform(0, min(8.0, 1.0 * (2 ** attempt)))
                    logger.warning("GraphQL URL error retry after %.2fs: %s", wait, exc)
                    time.sleep(wait)
                    continue
                raise

        if body is None:
            raise RuntimeError("GraphQL request failed after retries") from last_exc

        data = json.loads(body)
        if data.get("errors"):
            msgs = "; ".join(str(e.get("message", e)) for e in data["errors"])
            raise RuntimeError(f"GraphQL errors: {msgs}")
        return data.get("data", {})
