"""Typed data model for the CI test-fix pipeline.

The pipeline is a chain of small, explicit handoffs:

    front door -> FixRequest     (which failure, which code, where a fix may go)
    diagnose   -> FixProposal    (AI: what failed, why, which change caused it)
    apply      -> (edits on disk)
    run        -> RunResult      (code: the AI-proposed command's real verdict)
    review     -> ReviewVerdict  (AI: is the fix good, not just green)
    engine     -> FixOutcome     (READY with an approved patch, or a refusal)
    publish    -> FixOutcome     (what was pushed, posted, or opened)

AI populates the judgment fields (``FixProposal``, ``ReviewVerdict``); code
populates the factual fields (``RunResult``, ``FixOutcome``). The split is
deliberate: an AI never decides whether a test passed or whether a push
happened. The engine never writes to GitHub; publication is a separate step
that runs with a freshly minted write token.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class FixPath(str, Enum):
    """How the agent intends to resolve the failure."""

    PORT = "port"        # an existing fix on the default branch ports cleanly
    AUTHOR = "author"    # a fix the agent writes
    REFUSE = "refuse"    # not safely fixable; report with evidence, change nothing


class FailureType(str, Enum):
    """The AI's classification of the failure, reported to maintainers."""

    DETERMINISTIC = "deterministic"    # fails every time on this code
    FLAKY = "flaky"                    # timing- or environment-dependent
    INFRASTRUCTURE = "infrastructure"  # runner, network, or service outage
    UNKNOWN = "unknown"


class Policy(str, Enum):
    """What a fix may change, decided by code from where the failure was seen."""

    BACKPORT = "backport"  # bot-owned backport branch: port or mechanical adaptation
    FIX = "fix"            # fix PR, contributor PR: test or product fixes allowed


class Publication(str, Enum):
    """Where an approved fix goes. Code picks this; the AI never does."""

    PUSH = "push"        # commit onto the bot-owned ``agent/...`` PR branch
    SUGGEST = "suggest"  # post the patch on a contributor PR; never push to it
    NEW_PR = "new-pr"    # new ``agent/ci-fix/...`` branch, verified in CI, then a PR


@dataclass(frozen=True)
class FixRequest:
    """A validated request to fix one failing CI run.

    Produced by a front door (maintainer comment, sweep follow-up, Daily issue)
    only after its own fail-closed checks. ``head_sha`` is the commit the fix is
    built on. For PR flows it is also the commit the failed run tested; for the
    Daily issue flow it is the branch tip, and ``failing_sha`` records the
    commit the run tested.
    """

    repo_full_name: str
    pr_number: int
    head_repo_full_name: str
    head_branch: str
    head_sha: str
    run_id: int
    requested_by: str
    hint: str = ""
    base_branch: str = ""
    policy: Policy = Policy.BACKPORT
    publication: Publication = Publication.PUSH
    # False when the checkout's code must never run here: a fork PR, or a fix
    # the publication step verifies in the project's own CI instead.
    execute: bool = True
    # Revision range whose commits may have introduced the failure. Empty means
    # ``origin/<base_branch>..HEAD`` (the PR's own commits) when a base is known.
    culprit_range: str = ""
    failing_sha: str = ""
    issue_number: int = 0
    # Code-chosen description of the one failure to fix, when the run holds
    # several (a Daily run usually does). Empty lets the diagnosis pick.
    target: str = ""
    # The failed job the front door chose. When set, it is the only job the
    # engine diagnoses and verifies, whatever job the AI names.
    job: str = ""


@dataclass(frozen=True)
class FixProposal:
    """The AI diagnosis and plan. Pure judgment - no side effects yet."""

    path: FixPath
    failing_check: str
    root_cause: str
    reasoning: str
    confidence: float
    # The CI job the AI thinks the failure belongs to. A non-authoritative hint:
    # code requires it to match a job that actually failed before trusting it.
    failing_job_hint: str = ""
    # Command the agent should run to reproduce/verify the single failing
    # check, expressed in the repo's own tooling. Code executes it; the AI never
    # runs it. Empty when path is REFUSE.
    build_command: str = ""
    verify_command: str = ""
    # Relative working directory for the commands (defaults to repo root).
    workdir: str = ""
    # For PORT: the default-branch commit that already fixes this.
    unstable_fix_commit: str = ""
    # Tests beyond the first that also failed in the run, reported so the
    # human can re-invoke for them. Not acted on this invocation.
    other_failing_checks: tuple[str, ...] = ()
    failure_type: FailureType = FailureType.UNKNOWN
    # A commit the AI blames, from the code-supplied recent changes. Code
    # resolves it against that list before reporting it.
    culprit_commit: str = ""


@dataclass(frozen=True)
class RunResult:
    """The factual outcome of executing a proposed command.

    ``passed`` is derived from the subprocess exit code, never from any AI
    claim. ``ran`` is False only when the command could not be executed at
    all (e.g. an un-runnable variant), which the gate treats as a refusal
    rather than a pass.
    """

    ran: bool
    passed: bool
    exit_code: int
    command: str
    output_tail: str
    timed_out: bool = False


@dataclass(frozen=True)
class ReviewVerdict:
    """The AI skeptic's judgment on a candidate fix."""

    approved: bool
    reasoning: str


class OutcomeKind(str, Enum):
    READY = "ready"            # engine: an approved fix awaiting publication
    PUSHED = "pushed"          # fix pushed to the bot-owned PR branch
    SUGGESTED = "suggested"    # verified patch posted on a contributor PR
    REFUSED = "refused"        # could not safely fix; nothing changed
    FAILED = "failed"          # an internal error stopped the run
    HANDOFF = "handoff"        # a fix was authored but could not be verified
                               # here; the patch is posted for a human


@dataclass(frozen=True)
class FixOutcome:
    """Terminal result of one invocation, rendered into a comment."""

    kind: OutcomeKind
    summary: str
    proposal: FixProposal | None = None
    run_result: RunResult | None = None
    review: ReviewVerdict | None = None
    commit_sha: str = ""
    # The failing CI run this invocation acted on, linked for provenance.
    failing_run_url: str = ""
    # Which verifier proved the fix ("local", "docker:<image>", "macos",
    # "upstream-port"); empty when nothing was executed.
    verify_backend: str = ""
    # For the macOS backend: the URL of the verification run that proved the fix.
    macos_run_url: str = ""
    # For HANDOFF: the unverified candidate patch, posted for a human.
    handoff_patch: str = ""
    other_failing_checks: tuple[str, ...] = ()
    # READY: exactly what publication applies. ``patch`` for an authored fix,
    # ``port_commit`` (a full default-branch SHA) for a cherry-picked one.
    patch: str = ""
    changed_paths: tuple[str, ...] = ()
    port_commit: str = ""
    # The code-validated commit that introduced the failure, if any.
    culprit_sha: str = ""
    culprit_subject: str = ""


# --- State-file serialization -------------------------------------------------
#
# The engine and publication run as separate steps (publication mints its own
# write token), so the request and outcome cross a JSON file. Field names are
# the dataclass names; enums travel as their values.

def to_dict(value: Any) -> dict[str, Any]:
    """Serialize a request or outcome to plain JSON types."""
    return _plain(asdict(value))


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def request_from_dict(data: dict[str, Any]) -> FixRequest:
    return FixRequest(
        repo_full_name=_s(data, "repo_full_name"),
        pr_number=_i(data, "pr_number"),
        head_repo_full_name=_s(data, "head_repo_full_name"),
        head_branch=_s(data, "head_branch"),
        head_sha=_s(data, "head_sha"),
        run_id=_i(data, "run_id"),
        requested_by=_s(data, "requested_by"),
        hint=_s(data, "hint"),
        base_branch=_s(data, "base_branch"),
        # These decide what may run and be pushed, so they are never defaulted.
        policy=Policy(data["policy"]),
        publication=Publication(data["publication"]),
        execute=_bool(data, "execute"),
        culprit_range=_s(data, "culprit_range"),
        failing_sha=_s(data, "failing_sha"),
        issue_number=_i(data, "issue_number"),
        target=_s(data, "target"),
        job=_s(data, "job"),
    )


def outcome_from_dict(data: dict[str, Any]) -> FixOutcome:
    proposal = data.get("proposal")
    run = data.get("run_result")
    review = data.get("review")
    return FixOutcome(
        kind=OutcomeKind(data["kind"]),
        summary=_s(data, "summary"),
        proposal=_proposal_from_dict(proposal) if isinstance(proposal, dict) else None,
        run_result=RunResult(
            ran=bool(run["ran"]), passed=bool(run["passed"]),
            exit_code=int(run["exit_code"]), command=_s(run, "command"),
            output_tail=_s(run, "output_tail"), timed_out=bool(run.get("timed_out")),
        ) if isinstance(run, dict) else None,
        review=ReviewVerdict(
            approved=bool(review["approved"]), reasoning=_s(review, "reasoning"),
        ) if isinstance(review, dict) else None,
        commit_sha=_s(data, "commit_sha"),
        failing_run_url=_s(data, "failing_run_url"),
        verify_backend=_s(data, "verify_backend"),
        macos_run_url=_s(data, "macos_run_url"),
        handoff_patch=_s(data, "handoff_patch"),
        other_failing_checks=tuple(data.get("other_failing_checks") or ()),
        patch=_s(data, "patch"),
        changed_paths=tuple(data.get("changed_paths") or ()),
        port_commit=_s(data, "port_commit"),
        culprit_sha=_s(data, "culprit_sha"),
        culprit_subject=_s(data, "culprit_subject"),
    )


def _proposal_from_dict(data: dict[str, Any]) -> FixProposal:
    return FixProposal(
        path=FixPath(data["path"]),
        failing_check=_s(data, "failing_check"),
        root_cause=_s(data, "root_cause"),
        reasoning=_s(data, "reasoning"),
        confidence=float(data.get("confidence") or 0.0),
        failing_job_hint=_s(data, "failing_job_hint"),
        build_command=_s(data, "build_command"),
        verify_command=_s(data, "verify_command"),
        workdir=_s(data, "workdir"),
        unstable_fix_commit=_s(data, "unstable_fix_commit"),
        other_failing_checks=tuple(data.get("other_failing_checks") or ()),
        failure_type=FailureType(data.get("failure_type", FailureType.UNKNOWN.value)),
        culprit_commit=_s(data, "culprit_commit"),
    )


def _s(data: dict[str, Any], key: str) -> str:
    value = data.get(key, "")
    return value if isinstance(value, str) else ""


def _bool(data: dict[str, Any], key: str) -> bool:
    value = data[key]
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean, got {value!r}")
    return value


def _i(data: dict[str, Any], key: str) -> int:
    value = data.get(key, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
