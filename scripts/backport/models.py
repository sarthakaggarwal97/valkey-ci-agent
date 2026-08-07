"""Data models for the Backport Agent pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

ResolutionSource = Literal["llm", "automatic"]
CandidateOutcome = Literal[
    "applied",
    "skipped-existing",
    "skipped-conflict",
    "skipped-validation-failed",
    "error",
]
BackportOutcome = Literal[
    "success",
    "conflicts-unresolved",
    "duplicate",
    "branch-missing",
    "pr-not-merged",
    "already-applied",
    "error",
]

# Stable detail recorded for a candidate whose conflicts were resolved by the
# AI. Kept as a constant so the sweep-PR-body round-trip can recognize and
# preserve the signal across runs.
DETAIL_RESOLVED_BY_AI = "conflicts resolved by Claude Code"

DETAIL_DROPPED_TARGET_MISSING_TEST_PREFIX = "dropped target-missing test file(s):"
DETAIL_PORTED_TARGET_MISSING_TEST_PREFIX = "ported target-missing test coverage to:"

# Detail recorded for a candidate whose cherry-pick (or post-resolution result)
# contributes no net change to the target branch, e.g. the fix targets code
# that does not exist on this release branch. Reporting surfaces these as
# intentionally skipped rather than dropping them silently.
DETAIL_EMPTY_ON_TARGET = "resolution was already satisfied on target branch"


@dataclass
class ConflictedFile:
    """A file with merge conflict markers after cherry-pick."""

    path: str
    target_branch_content: str
    source_branch_content: str


@dataclass
class ResolutionResult:
    """Outcome of conflict resolution for a single file."""

    path: str
    resolved_content: str | None  # None = resolution failed
    resolution_summary: str
    source: ResolutionSource = "llm"
    # The commit on the target branch that carries this resolution. Set once
    # the commit exists; diff comments link each file into the right commit
    # when a candidate spans several (cherry-pick + validation repair).
    commit_sha: str | None = None
    resolution_diff: str | None = None
    reviewer_diff: str | None = None
    llm_summary: str | None = None


@dataclass(frozen=True)
class BackportCandidate:
    """A merged source pull request selected for one target branch."""

    source_pr_number: int
    source_pr_title: str
    source_pr_url: str
    target_branch: str
    merge_commit_sha: str | None = None
    commit_shas: tuple[str, ...] = ()
    merged_at: str = ""
    source_commits_complete: bool = True


@dataclass
class CandidateResult:
    """Result of applying one source pull request to a local branch."""

    source_pr_number: int
    source_pr_title: str
    outcome: CandidateOutcome
    detail: str = ""
    # Per-file AI resolutions produced for this candidate (empty when the
    # cherry-pick applied cleanly). Used to post diff comments on the sweep PR.
    resolutions: list[ResolutionResult] = field(default_factory=list)
    # Durable signal that this candidate's conflicts were resolved by the AI.
    # Unlike `detail`, this survives the sweep-PR-body round-trip so later
    # sweeps don't lose the "resolved by Claude" record once the candidate is
    # already on the branch and no longer re-resolved.
    resolved_by_ai: bool = False
    # Human-facing reason a candidate was skipped, derived deterministically
    # from the resolution outcome (e.g. the resolved content matched the target
    # branch, so the cherry-pick added nothing). Surfaced in the Skipped table.
    skip_reason: str = ""
    # The plan's authoritative source commit this candidate cherry-picked
    # (or attempted to). One commit per candidate, by design.
    source_commit_sha: str | None = None
    # SHA of the resolution commit created on the target branch by
    # `cherry-pick --continue`. Lets diff comments link each resolved file to
    # its native diff in the commit view instead of inlining it.
    resolved_commit_sha: str | None = None
    conflicting_files: list[ConflictedFile] = field(default_factory=list)
    ai_summary: str = ""
    # False only when rollback itself failed and the worktree may be dirty;
    # callers must stop using the worktree for further candidates.
    worktree_restored: bool = True


@dataclass
class BackportResult:
    """Final outcome of a backport run."""

    outcome: BackportOutcome
    backport_pr_url: str | None = None
    commits_cherry_picked: int = 0
    files_conflicted: int = 0
    files_resolved: int = 0
    files_unresolved: int = 0
    error_message: str | None = None


@dataclass
class BackportConfig:
    """Configuration for the backport agent, derived from the registry."""

    backport_label: str = "backport"
    llm_conflict_label: str = "ai-resolved-conflicts"
    max_conflicting_files: int = 100
