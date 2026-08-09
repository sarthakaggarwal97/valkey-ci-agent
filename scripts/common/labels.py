"""Ensure a GitHub label exists before something applies it.

The one shared ensure-label helper: the backport PR flows and the release
tracker all need a label to exist before ``add_to_labels`` (or an issue
create naming it) can succeed. Best-effort by design: a label problem must
never fail the flow that needed it; the subsequent apply surfaces a
persistent problem.
"""

from __future__ import annotations

import logging
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call

logger = logging.getLogger(__name__)


def ensure_label(repo: Any, name: str, color: str, description: str) -> None:
    """Create label *name* on *repo* if it does not already exist.

    Best-effort and idempotent: verification and creation failures are
    logged and swallowed, and a 422 from ``create_label`` (concurrent
    creation) counts as already-exists.
    """
    try:
        retry_github_call(
            lambda: repo.get_label(name),
            retries=2,
            description=f"check label {name!r}",
        )
        return
    except GithubException as exc:
        if exc.status != 404:
            logger.warning("Could not verify label %r: %s", name, exc)
            return
    except Exception as exc:  # noqa: BLE001 - transport/parse failure is non-fatal
        logger.warning("Could not verify label %r: %s", name, exc)
        return

    try:
        logger.info("Creating missing label %r", name)
        retry_github_call(
            lambda: repo.create_label(name=name, color=color, description=description),
            retries=2,
            description=f"create label {name!r}",
        )
    except GithubException as exc:
        if exc.status == 422:  # created concurrently, which is fine
            return
        logger.error("Failed to create label %r: %s", name, exc)
    except Exception as exc:  # noqa: BLE001 - transport/parse failure is non-fatal
        logger.error("Failed to create label %r: %s", name, exc)
