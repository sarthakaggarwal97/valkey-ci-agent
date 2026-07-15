"""Deterministic selection helpers for phased CI-fix preparation."""

from __future__ import annotations

from scripts.ci_fix.port_discovery import PortCandidate


def match_failed_job(hint: str, failed_jobs: tuple[str, ...]) -> str | None:
    """Return one exact failed job selected by a display-name hint."""
    if not hint or not failed_jobs:
        return None
    exact = [job for job in failed_jobs if job == hint]
    if exact:
        return exact[0]
    hint_base = hint.split(" (")[0]
    base_matches = [
        job for job in failed_jobs if job.split(" (")[0] == hint_base
    ]
    if len(base_matches) == 1:
        return base_matches[0]
    return None


def canonical_candidate_sha(
    chosen: str,
    candidates: tuple[PortCandidate, ...],
) -> str | None:
    """Resolve one unambiguous model-selected prefix to a discovered SHA."""
    chosen = chosen.strip().lower()
    if not chosen:
        return None
    matches = {
        candidate.sha
        for candidate in candidates
        if candidate.sha.lower().startswith(chosen)
        or chosen.startswith(candidate.sha.lower())
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None
