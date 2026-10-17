"""Build an alpha-sorted contributor list from original source-PR authors.

Commit authors and ``Co-authored-by`` trailers are intentionally not consulted:
merge and backport mechanics can attach identities that did not author the
original change. The caller supplies GitHub logins from resolved source PRs.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import List, Optional, Sequence

from scripts.common.github_client import urlopen_with_retry

logger = logging.getLogger(__name__)

_API_ROOT = "https://api.github.com"
_API_RETRIES = 3


def _is_bot(identity: str) -> bool:
    """True if *identity* is a bot account (ends in ``[bot]``)."""
    return identity.strip().casefold().endswith("[bot]")


def _api_get(url: str, token: Optional[str]) -> object:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "valkey-release-tools",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = "Bearer {}".format(token)
    body = urlopen_with_retry(
        urllib.request.Request(url, headers=headers),
        timeout=30,
        retries=_API_RETRIES - 1,
        description=url.removeprefix(_API_ROOT),
    )
    return json.loads(body.decode("utf-8"))


def _display_name(repo_login: str, token: Optional[str]) -> Optional[str]:
    """Resolve a login to its profile full name, or ``None`` if unavailable."""
    try:
        data = _api_get("{}/users/{}".format(_API_ROOT, repo_login), token)
    except (OSError, urllib.error.URLError, urllib.error.HTTPError, ValueError):
        return None
    if isinstance(data, dict):
        name = data.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return None


def list_contributors(original_pr_logins: Sequence[str], token: Optional[str] = None) -> List[str]:
    """Return unique ``"Full Name @handle"`` entries for source-PR authors.

    ``original_pr_logins`` is authoritative. Missing profile names degrade to the
    stable GitHub login; no commit, merge-author, or trailer fallback is used.
    """
    entries: List[str] = []
    seen: set[str] = set()
    for login in original_pr_logins:
        login = login.strip()
        key = login.casefold()
        if not login or key in seen or _is_bot(login):
            continue
        seen.add(key)
        name = _display_name(login, token) or login
        entries.append("{} @{}".format(name, login))
    entries.sort(key=lambda entry: entry.rsplit(" @", 1)[0].casefold())
    return entries
