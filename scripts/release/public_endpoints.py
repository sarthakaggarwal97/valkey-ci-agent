"""Anonymous verification of public release outputs.

Checks that artifacts the release promises are actually reachable by the
public: container image tags in Docker Hub / GHCR / ECR Public, and files on
download.valkey.io. Everything here is unauthenticated by design: the point
is to observe what an anonymous user sees, and no GitHub token may ever leak
to a third-party endpoint.

Registry flows (verified live against the valkey repos):

    Docker Hub  GET hub.docker.com/v2/repositories/{repo}/tags/{tag}
    GHCR        GET ghcr.io/token?scope=repository:{repo}:pull  (anonymous)
                GET ghcr.io/v2/{repo}/manifests/{tag}  (Bearer <token>)
    ECR Public  GET public.ecr.aws/token/  (anonymous)
                GET public.ecr.aws/v2/{repo}/manifests/{tag}  (Bearer <token>)
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

_TIMEOUT_S = 30
_MANIFEST_ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def url_exists(url: str, *, method: str = "HEAD",
               headers: dict[str, str] | None = None) -> bool:
    """True when *url* answers 2xx anonymously; False on 4xx.

    5xx and network errors raise, so a registry outage surfaces as a
    reconcile error instead of silently reporting an output as missing.
    """
    request = urllib.request.Request(url, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
            return 200 <= response.status < 300
    except urllib.error.HTTPError as exc:
        # 429 is rate limiting and 405 an endpoint rejecting the HEAD method;
        # neither means absence: raising surfaces them as reconcile errors
        # instead of silently stalling an output as missing.
        if 400 <= exc.code < 500 and exc.code not in (405, 429):
            return False
        raise


def _fetch_json(url: str) -> dict[str, object]:
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
        return json.load(response)


def dockerhub_tag_exists(repo: str, tag: str) -> bool:
    """True when Docker Hub serves *repo*:*tag* publicly (no auth needed)."""
    return url_exists(
        f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag}", method="GET",
    )


def ghcr_tag_exists(repo: str, tag: str) -> bool:
    """True when ghcr.io serves *repo*:*tag* to an anonymous pull.

    The anonymous token grant itself fails for private packages, which
    reads as not-public, which is exactly the answer we need.
    """
    try:
        token = _fetch_json(f"https://ghcr.io/token?scope=repository:{repo}:pull")["token"]
    except urllib.error.HTTPError as exc:
        if 400 <= exc.code < 500 and exc.code != 429:
            return False
        raise
    return url_exists(
        f"https://ghcr.io/v2/{repo}/manifests/{tag}",
        headers={"Authorization": f"Bearer {token}", "Accept": _MANIFEST_ACCEPT},
    )


def ecr_public_tag_exists(repo: str, tag: str) -> bool:
    """True when ECR Public serves *repo*:*tag* anonymously."""
    token = _fetch_json("https://public.ecr.aws/token/")["token"]
    return url_exists(
        f"https://public.ecr.aws/v2/{repo}/manifests/{tag}",
        headers={"Authorization": f"Bearer {token}", "Accept": _MANIFEST_ACCEPT},
    )


def fetch_text(url: str) -> str:
    """UTF-8 body of a public URL (for index files small enough to scan)."""
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
        return response.read().decode("utf-8", errors="replace")
