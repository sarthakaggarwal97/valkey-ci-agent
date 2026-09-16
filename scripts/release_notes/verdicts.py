"""Write the cut's per-PR triage verdicts to a machine-readable file.

The release PR body reports triage outcomes as counts: per-PR tables at release
scale exceeded GitHub's body-length limit, and at hundreds of rows nobody read
them there. But the per-PR detail is how a maintainer audits an exclusion, and
the 9.2.0-rc1 review recovered two wrongly-excluded changes by exactly that
audit. This file is the durable home for it: the workflow uploads it as a run
artifact, so the verdicts outlive the log's retention and are greppable without
scrolling a job log. It also accumulates a dataset for measuring triage
precision across releases.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _decision(pr: Any) -> dict[str, Any]:
    return {
        "pr": pr.number,
        "title": pr.title,
        "url": pr.url,
        "reason": pr.reason,
        "uncertain": bool(getattr(pr, "uncertain", False)),
        "guardrail": bool(getattr(pr, "guardrail", False)),
        "unreleased_code": bool(getattr(pr, "unreleased_code", False)),
    }


def write_verdicts(path: str, regen: Any, *, version: str, stage: str) -> None:
    """Serialize every triage outcome for this cut to *path* as JSON.

    Best-effort by design: the verdicts file is an audit aid, and failing the
    cut over it would invert its purpose.
    """
    payload = {
        "version": version,
        "stage": stage,
        "base_tag": regen.base_tag,
        "ai_included": [_decision(pr) for pr in regen.ai_included],
        "guardrail_included": [_decision(pr) for pr in regen.guardrail_included],
        "ai_excluded": [_decision(pr) for pr in regen.ai_excluded],
        "label_excluded": [_decision(pr) for pr in regen.label_excluded],
        "undecided": [
            {"pr": pr.number, "title": pr.title, "url": pr.url} for pr in regen.triage
        ],
    }
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        logger.info("Wrote triage verdicts for %s-%s to %s", version, stage, path)
    except OSError:
        logger.warning("Could not write triage verdicts to %s", path, exc_info=True)
