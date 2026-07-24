"""Typed data passed through the scheduled backport sweep."""

from __future__ import annotations

from dataclasses import dataclass, field

from scripts.backport import models as _models

ProjectBackportCandidate = _models.BackportCandidate
CandidateResult = _models.CandidateResult
DETAIL_DROPPED_TARGET_MISSING_TEST_PREFIX = (
    _models.DETAIL_DROPPED_TARGET_MISSING_TEST_PREFIX
)
DETAIL_EMPTY_ON_TARGET = _models.DETAIL_EMPTY_ON_TARGET
DETAIL_PORTED_TARGET_MISSING_TEST_PREFIX = (
    _models.DETAIL_PORTED_TARGET_MISSING_TEST_PREFIX
)
DETAIL_RESOLVED_BY_AI = _models.DETAIL_RESOLVED_BY_AI
DETAIL_VALIDATION_REPAIRED_PREFIX = _models.DETAIL_VALIDATION_REPAIRED_PREFIX


# Detail string used when a candidate PR is already cherry-picked onto the
# backport sweep branch. Reporting treats this as "on the branch", unlike
# empty cherry-picks that mean "already on the release branch".
DETAIL_ALREADY_ON_SWEEP_BRANCH = "already on backport branch"

@dataclass
class BranchSweepResult:
    target_branch: str
    candidates_found: int = 0
    results: list[CandidateResult] = field(default_factory=list)
    pr_url: str = ""
    error: str = ""

    @property
    def applied_count(self) -> int:
        """Number of candidates that were cherry-picked onto the branch."""
        return sum(1 for item in self.results if item.outcome == "applied")
