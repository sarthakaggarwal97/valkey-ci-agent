"""Classify PRs into include / candidate / hard-excluded buckets from labels.

``no-release-notes`` hard-excludes a PR: it is an explicit author opt-out and is
dropped before AI triage ever sees it (and takes precedence over ``release-notes``
if, contradictorily, both are present). ``release-notes`` hard-includes. Anything
else is a candidate that AI triage judges, so a change that is user-facing despite
a missing label is still caught. Unlike valkey's label-only ``check_release_notes``
gate (which only asks "is the label present"), we additionally ask "is the change
actually user-facing" for everything the author did not explicitly opt in or out of.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Sequence

from scripts.release_notes.models import MergedPR, PRDisposition

# The label the release-notes workflow gate enforces; its presence hard-includes.
RELEASE_LABEL = "release-notes"
# Explicit author opt-out; its presence hard-excludes, even over RELEASE_LABEL.
NO_RELEASE_LABEL = "no-release-notes"


def disposition_for(labels: Sequence[str]) -> PRDisposition:
    """Map a PR's labels to a disposition.

    ``no-release-notes`` present -> EXCLUDE (checked first, so it wins even when
    ``release-notes`` is also present). Otherwise ``release-notes`` present ->
    INCLUDE. Anything else -> CANDIDATE (AI triage decides).
    """
    if NO_RELEASE_LABEL in labels:
        return PRDisposition.EXCLUDE
    if RELEASE_LABEL in labels:
        return PRDisposition.INCLUDE
    return PRDisposition.CANDIDATE


def classify(
    prs: Sequence[MergedPR],
) -> tuple[list[MergedPR], list[MergedPR], list[MergedPR]]:
    """Partition *prs* into ``(include, candidates, excluded)`` with dispositions.

    ``include`` carried the ``release-notes`` label; ``candidates`` carried no
    gating label and go to AI triage; ``excluded`` carried ``no-release-notes`` and
    are hard-dropped (never triaged, never noted). Each returned PR is re-stamped
    with its resolved disposition.
    """
    include: list[MergedPR] = []
    candidates: list[MergedPR] = []
    excluded: list[MergedPR] = []
    for pr in prs:
        disposition = disposition_for(pr.labels)
        stamped = replace(pr, disposition=disposition)
        if disposition is PRDisposition.INCLUDE:
            include.append(stamped)
        elif disposition is PRDisposition.EXCLUDE:
            excluded.append(stamped)
        else:
            candidates.append(stamped)
    return include, candidates, excluded
