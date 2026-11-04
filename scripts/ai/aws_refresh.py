"""Fresh AWS credentials for each Claude run in a long GitHub Actions job.

The credentials step grants one hour, the most the role allows, and a job
that runs Claude several times outlives it. Before a run, this assumes the
role again with the job's OIDC token, as the credentials step did, and puts
the new credentials in the environment Claude inherits. It does nothing
outside a job that names the role in CI_AGENT_AWS_ROLE_ARN and may request
an OIDC token (``id-token: write``).
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ElementTree

logger = logging.getLogger(__name__)

ROLE_ENV = "CI_AGENT_AWS_ROLE_ARN"
# A run is shorter than this much of the hour the credentials last.
_REFRESH_AFTER_S = 10 * 60
_SESSION_S = 3600
_STS_NS = {"sts": "https://sts.amazonaws.com/doc/2011-06-15/"}

_refreshed_at: float | None = None


def refresh_aws_credentials() -> None:
    """Replace the environment's AWS credentials with an hour's fresh ones, at most every ten minutes."""
    global _refreshed_at
    role = os.environ.get(ROLE_ENV, "")
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    if not (role and request_url and request_token):
        return
    if _refreshed_at is not None and time.monotonic() - _refreshed_at < _REFRESH_AFTER_S:
        return
    try:
        os.environ.update(_assume_role(role, _oidc_token(request_url, request_token)))
    except (OSError, ValueError, KeyError, ElementTree.ParseError) as exc:
        logger.warning("Could not refresh the AWS credentials; keeping the current ones: %s", exc)
        return
    _refreshed_at = time.monotonic()
    logger.info("Refreshed the AWS credentials for an hour")


def _oidc_token(request_url: str, request_token: str) -> str:
    separator = "&" if "?" in request_url else "?"
    request = urllib.request.Request(
        f"{request_url}{separator}audience=sts.amazonaws.com",
        headers={"Authorization": f"bearer {request_token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return str(json.load(response)["value"])


def _assume_role(role: str, web_identity_token: str) -> dict[str, str]:
    """AssumeRoleWithWebIdentity, which takes no signature: the OIDC token is the proof."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    body = urllib.parse.urlencode({
        "Action": "AssumeRoleWithWebIdentity",
        "Version": "2011-06-15",
        "RoleArn": role,
        "RoleSessionName": f"ci-agent-{os.environ.get('GITHUB_RUN_ID', 'local')}",
        "WebIdentityToken": web_identity_token,
        "DurationSeconds": str(_SESSION_S),
    }).encode()
    request = urllib.request.Request(f"https://sts.{region}.amazonaws.com/", data=body, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        root = ElementTree.fromstring(response.read())
    credentials = root.find(".//sts:Credentials", _STS_NS)
    if credentials is None:
        raise ValueError("the STS reply holds no credentials")

    def field(name: str) -> str:
        value = credentials.findtext(f"sts:{name}", namespaces=_STS_NS)
        if not value:
            raise ValueError(f"the STS reply holds no {name}")
        return value

    return {
        "AWS_ACCESS_KEY_ID": field("AccessKeyId"),
        "AWS_SECRET_ACCESS_KEY": field("SecretAccessKey"),
        "AWS_SESSION_TOKEN": field("SessionToken"),
    }
