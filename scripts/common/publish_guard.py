"""Block accidental writes to upstream valkey-io repositories."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}
_UPSTREAM_REPOS = {
    "valkey-io/valkey",
}


def _env_true(name: str) -> bool:
    return (os.environ.get(name, "") or "").strip().lower() in _TRUTHY


def check_publish_allowed(
    target_repo: str,
    *,
    action: str = "write",
    context: str = "",
) -> None:
    if target_repo in _UPSTREAM_REPOS and not _env_true(
        "VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH"
    ):
        raise RuntimeError(
            f"Blocked {action} on {target_repo}: upstream publishing requires "
            "VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH=1"
            + (f" ({context})" if context else "")
        )
    logger.debug("Publish guard OK: %s on %s (%s)", action, target_repo, context)
