"""Typed data model for the CI test-fix pipeline.

The pipeline is a chain of small, explicit handoffs:

    gate    -> FixRequest        (who asked, which run, which SHA)
    diagnose-> FixProposal       (AI: what failed, how to fix, how to run it)
    apply   -> (edits on disk)
    review  -> ReviewVerdict     (AI: is the proposed patch good)
    verify  -> workflow verdicts (code: repeated baseline/candidate evidence)
    push    -> FixOutcome        (what the agent did, for the PR comment)

AI populates the judgment fields (``FixProposal``, ``ReviewVerdict``); code
populates workflow evidence and ``FixOutcome``. The split is deliberate: an AI
never decides whether a test passed or whether a push happened.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FixPath(str, Enum):
    """How the agent intends to resolve the failure."""

    PORT = "port"        # an existing fix on the default branch ports cleanly
    AUTHOR = "author"    # a deterministic scaffolding fix the agent writes
    REFUSE = "refuse"    # not safely fixable; report with evidence, change nothing


class FailureMode(str, Enum):
    """The diagnosed behavior of the failing check."""

    DETERMINISTIC = "deterministic"
    FLAKY = "flaky"
    ENVIRONMENT = "environment"
    INFRASTRUCTURE = "infrastructure"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FixRequest:
    """A validated ``@valkeyrie-bot fix <ci-link>`` invocation.

    Produced by the gate only after fail-closed auth and SHA-bound run
    checks pass. ``head_sha`` is the commit the failed run was built from and
    the commit the repo is checked out at - they are guaranteed equal here.
    """

    repo_full_name: str
    pr_number: int
    head_repo_full_name: str
    head_branch: str
    head_sha: str
    run_id: int
    requested_by: str
    hint: str = ""


@dataclass(frozen=True)
class FixProposal:
    """The AI diagnosis and plan. Pure judgment - no side effects yet."""

    path: FixPath
    failing_check: str
    root_cause: str
    reasoning: str
    confidence: float
    failure_mode: FailureMode = FailureMode.UNKNOWN
    # The CI job the AI thinks the failure belongs to. A non-authoritative hint:
    # code fetches the real failed jobs from the run and requires this to match
    # one of them before trusting it. Code, not the AI, selects the environment.
    failing_job_hint: str = ""
    # Command the agent should run to reproduce/verify the single failing
    # check, expressed in the repo's own tooling. This is targeted verification
    # of the one failure, not a replay of the whole CI job. Code executes it;
    # the AI never runs it. Empty when path is REFUSE.
    build_command: str = ""
    verify_command: str = ""
    # Relative working directory for the commands (defaults to repo root).
    workdir: str = ""
    # For PORT: the default-branch commit that already fixes this.
    unstable_fix_commit: str = ""
    # Tests beyond the first that also failed in the run, reported so the
    # human can re-invoke for them. Not acted on this invocation.
    other_failing_checks: tuple[str, ...] = ()


class BaselineKind(str, Enum):
    """Code-observed behavior of the unmodified checkout."""

    DETERMINISTIC = "deterministic"
    FLAKY = "flaky"
    NOT_REPRODUCED = "not_reproduced"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class BaselineEvidence:
    """Repeated clean-tree observations made before applying a fix."""

    kind: BaselineKind
    attempts: int
    passed: int
    failed: int
    unavailable: int = 0
    detail: str = ""


@dataclass(frozen=True)
class ReviewVerdict:
    """The AI skeptic's judgment on an already-passing fix."""

    approved: bool
    reasoning: str


class OutcomeKind(str, Enum):
    PUSHED = "pushed"          # fix validated, reviewed, and pushed
    REFUSED = "refused"        # could not safely fix; nothing changed
    FAILED = "failed"          # an internal error stopped the run
    HANDOFF = "handoff"        # a fix was authored but could not be verified
                               # here; the patch is posted for a human


@dataclass(frozen=True)
class FixOutcome:
    """Terminal result of one invocation, rendered into the PR comment."""

    kind: OutcomeKind
    summary: str
    proposal: FixProposal | None = None
    review: ReviewVerdict | None = None
    commit_sha: str = ""
    # The failing CI run this invocation acted on, linked in the comment for
    # provenance.
    failing_run_url: str = ""
    # Which remote verifier judged the fix ("local", "docker:<image>",
    # "macos", "target-workflow"), shown as evidence in the PR comment.
    verify_backend: str = ""
    # URL of the remote verification run that most recently judged the fix.
    verification_run_url: str = ""
    # For an authored HANDOFF: the candidate patch posted for a human to apply.
    # Historical-port handoffs identify the trusted commit in ``summary``.
    handoff_patch: str = ""
    other_failing_checks: tuple[str, ...] = ()
    baseline: BaselineEvidence | None = None
