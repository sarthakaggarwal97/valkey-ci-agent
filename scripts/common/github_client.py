"""Shared GitHub API retry helpers."""

from __future__ import annotations

import logging
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, TypeVar

from github.GithubException import GithubException

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
# HTTP statuses worth retrying. Shared with raw-urllib callers that can't use
# retry_github_call directly.
RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
_BASE_DELAY_SECONDS = 1.0
_MAX_DELAY_SECONDS = 8.0

# Substrings in 403 response messages that indicate rate limiting
# (as opposed to permanent permission errors).
_RATE_LIMIT_403_INDICATORS = (
    "rate limit",
    "abuse detection",
    "retry after",
    "secondary rate",
    "exceeded a secondary",
)


def _is_retryable_error(exc: Exception) -> bool:
    if not isinstance(exc, GithubException):
        return False
    if exc.status in RETRYABLE_HTTP_STATUS:
        return True
    # Only retry 403 when it's clearly rate-limiting, not a permission error.
    if exc.status == 403:
        msg = str(exc).lower()
        return any(indicator in msg for indicator in _RATE_LIMIT_403_INDICATORS)
    return False


def transient_backoff_delay(attempt: int) -> float:
    """Jittered, capped exponential backoff for transient-failure retries."""
    return random.uniform(0, min(_MAX_DELAY_SECONDS, _BASE_DELAY_SECONDS * (2 ** attempt)))


def _retry_delay(exc: Exception, attempt: int) -> float:
    headers = getattr(exc, "headers", None) or {}
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            pass
    return transient_backoff_delay(attempt)


def retry_github_call(
    operation: Callable[[], _T],
    *,
    retries: int,
    description: str,
) -> _T:
    """Retry transient GitHub API failures with exponential backoff."""
    for attempt in range(retries):
        try:
            return operation()
        except Exception as exc:
            if not _is_retryable_error(exc):
                # Non-retryable errors are often expected (404 probes); the
                # caller decides whether they matter.
                raise
            if attempt == retries - 1:
                logger.error(
                    "GitHub API call for %s failed after %d attempt(s): %s",
                    description, retries, exc,
                )
                raise
            wait_seconds = _retry_delay(exc, attempt)
            logger.warning(
                "Retrying GitHub API call for %s (attempt %d/%d) after %.2fs: %s",
                description,
                attempt + 1,
                retries,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)
    raise RuntimeError("unreachable: retry loop exited without returning or raising")


def dispatch_workflow(
    workflow: Any,
    ref: str,
    inputs: dict[str, str],
    *,
    retries: int,
    description: str,
) -> None:
    """Dispatch ``workflow`` at ``ref``, raising if GitHub rejects it.

    PyGithub returns False for a rejected dispatch and may return None for an
    accepted one (HTTP 204 has no body), so only False is a rejection.
    """
    accepted = retry_github_call(
        lambda: workflow.create_dispatch(ref, inputs=inputs),
        retries=retries,
        description=description,
    )
    if accepted is False:
        raise RuntimeError(f"GitHub rejected the dispatch: {description}")


def ensure_label(repo: Any, name: str, *, color: str, description: str, retries: int = 3) -> None:
    """Create label ``name`` on ``repo`` unless it exists.

    A 422 on creation means another run created it first.
    """
    try:
        retry_github_call(lambda: repo.get_label(name), retries=retries, description=f"get label {name!r}")
        return
    except GithubException as exc:
        if exc.status != 404:
            raise
    try:
        logger.info("Creating missing label %r", name)
        retry_github_call(
            lambda: repo.create_label(name=name, color=color, description=description),
            retries=retries,
            description=f"create label {name!r}",
        )
    except GithubException as exc:
        if exc.status != 422:
            raise


def urlopen_with_retry(
    request: urllib.request.Request,
    *,
    timeout: float,
    retries: int,
    description: str,
    retry_body: Callable[[bytes], bool] | None = None,
) -> bytes:
    """Return the body of a raw urllib *request*, retrying transient failures.

    A ``RETRYABLE_HTTP_STATUS`` response or a connection error is retried up to
    *retries* times with the shared backoff, and so is a body *retry_body*
    flags (GitHub reports GraphQL rate limits in a 200). The last attempt's
    body is returned, or its error raised.
    """
    for attempt in range(retries + 1):
        last = attempt == retries
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - callers pass https URLs
                body = response.read()
        except urllib.error.HTTPError as exc:
            if last or exc.code not in RETRYABLE_HTTP_STATUS:
                raise
            logger.warning("%s: HTTP %d, retrying", description, exc.code)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if last:
                raise
            logger.warning("%s: %s, retrying", description, exc)
        else:
            if last or retry_body is None or not retry_body(body):
                return body
            logger.warning("%s: transient error in the response, retrying", description)
        time.sleep(transient_backoff_delay(attempt))
    raise AssertionError("unreachable: the last attempt returns or raises")

