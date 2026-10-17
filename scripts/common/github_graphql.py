"""Small GitHub GraphQL client."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from typing import Any

from scripts.common.github_client import urlopen_with_retry

logger = logging.getLogger(__name__)

_TRANSIENT_GRAPHQL_ERROR_TYPES = {"RATE_LIMITED", "SERVICE_UNAVAILABLE"}
_RETRIES = 3


def _is_transient_graphql_error(errors: list[dict[str, Any]]) -> bool:
    """True if any GraphQL error is a rate-limit or transient service failure.

    GitHub returns these in a 200 body, so they bypass HTTP-level retries.
    """
    for error in errors:
        if error.get("type") in _TRANSIENT_GRAPHQL_ERROR_TYPES:
            return True
        message = str(error.get("message", "")).lower()
        if "rate limit" in message or "secondary rate limit" in message or "timeout" in message:
            return True
    return False


def _has_transient_errors(body: bytes) -> bool:
    try:
        errors = json.loads(body).get("errors")
    except (ValueError, AttributeError):
        return False
    return bool(errors) and _is_transient_graphql_error(errors)


class GitHubGraphQLClient:
    def __init__(self, token: str) -> None:
        self._token = token

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            "https://api.github.com/graphql",
            data=json.dumps({"query": query, "variables": variables}).encode(),
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            body = urlopen_with_retry(
                request, timeout=60, retries=_RETRIES, description="GraphQL",
                retry_body=_has_transient_errors,
            )
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GraphQL failed: {exc.code} {details}") from exc
        data = json.loads(body.decode("utf-8"))
        errors = data.get("errors")
        if errors:
            msgs = "; ".join(str(e.get("message", e)) for e in errors)
            raise RuntimeError(f"GraphQL errors: {msgs}")
        return data.get("data", {})


def project_pages(
    gql: GitHubGraphQLClient,
    build_query: Callable[[str], str],
    *,
    owner: str,
    number: int,
    owner_type: str,
) -> Iterator[dict[str, Any]]:
    """Yield the ``projectV2`` object of each page of a project's items.

    ``build_query`` takes the owner field (``user`` or ``organization``) and
    returns a query over ``$owner``, ``$number`` and the items ``$cursor``.
    """
    owner_field = "user" if owner_type == "user" else "organization"
    query = build_query(owner_field)
    cursor = None
    while True:
        data = gql.execute(query, {"owner": owner, "number": number, "cursor": cursor})
        project = (data.get(owner_field) or {}).get("projectV2")
        if not project:
            raise RuntimeError(f"Project {owner}/{number} not found")
        yield project
        page_info = (project.get("items") or {}).get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return
        cursor = page_info.get("endCursor")

