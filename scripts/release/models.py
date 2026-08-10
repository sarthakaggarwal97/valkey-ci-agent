"""Typed data model for the release controller.

    start     -> DerivedRelease   (code: version/stage from branch + live tags)
    reconcile -> ReleaseStatus    (code: recomputed truth from GitHub, never issue text)
    render    -> release issue body (display only; carries no authority)

The controller is deterministic: every field here is either derived from live
GitHub state (tags, PRs, branch heads, check runs) or is an explicit operator
decision made through an authorized entry point (start, adopt). The release
issue displays this state but is never parsed for decisions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReleaseIntent(str, Enum):
    """What kind of release the operator is starting.

    ``SECURITY`` marks an embargoed release. It is break-glass-only for v1:
    the controller refuses it before creating any public artifact, so an
    embargoed fix is never announced by an automated public issue.
    """

    RC = "rc"
    GA = "ga"
    PATCH = "patch"
    SECURITY = "security"


class ReleasePhase(str, Enum):
    """Where the release stands, recomputed every reconcile pass.

    Strictly ordered; each phase's work begins only when every earlier
    phase's evidence is present in live GitHub/public state:

    ``NOTES``          no merged notes PR yet (cut pending or in review).
    ``CANDIDATE``      candidate SHA recorded; required CI not yet green.
    ``QUALIFICATION``  CI green; no-publish qualification not yet passed.
    ``READY``          all pre-publication evidence present; awaiting the
                       human-approved publish step.
    ``PUBLISHED``      the release/tag exists at the candidate SHA; core
                       downstream outputs not yet all verified public.
    ``BUNDLE_HELM``    core outputs public; Bundle/Helm ordering running.
    ``COMPLETE``       every required public output verified.
    """

    NOTES = "notes"
    CANDIDATE = "candidate"
    QUALIFICATION = "qualification"
    READY = "ready"
    PUBLISHED = "published"
    BUNDLE_HELM = "bundle-helm"
    COMPLETE = "complete"


class CheckState(str, Enum):
    """Outcome of one required check on the exact candidate SHA.

    ``STALLED`` marks a run still not completed after the policy's
    ``check_timeout_minutes``; it blocks like FAILED but tells the operator
    to rerun rather than wait.
    """

    PASSED = "passed"
    FAILED = "failed"
    PENDING = "pending"
    MISSING = "missing"
    STALLED = "stalled"


class CandidateState(str, Enum):
    """Lifecycle of the release candidate SHA.

    ``CURRENT``: the notes-PR merge commit is still the branch head.
    ``ADOPTED``: the branch moved and an authorized owner adopted the exact
    new head through the ``adopt`` entry point.
    ``INVALIDATED``: the branch moved past the candidate (or past an earlier
    adoption) and no valid adoption covers the current head. Qualification
    must not proceed.
    ``NONE``: no merged notes PR yet, so no candidate exists.
    """

    NONE = "none"
    CURRENT = "current"
    ADOPTED = "adopted"
    INVALIDATED = "invalidated"


def release_tag(version: str, stage: str) -> str:
    """The git tag / artifact identity for a release: ``M.m.p`` for ga,
    ``M.m.p-rcN`` for release candidates.

    Every public artifact (tag, dispatch payload, tarball names, hashes
    line, container branch and image tags, run-name markers) carries this
    form, not the bare version: an rc's artifacts are named after the tag.
    """
    return version if stage == "ga" else f"{version}-{stage}"


@dataclass(frozen=True)
class DerivedRelease:
    """Version derivation output: a pure function of branch + existing tags.

    ``tag`` is the git tag this release will create (``M.m.p`` for ga,
    ``M.m.p-rcN`` for release candidates).
    """

    version: str
    stage: str

    @property
    def tag(self) -> str:
        return release_tag(self.version, self.stage)


@dataclass(frozen=True)
class RequiredCheck:
    """One policy-required check evaluated against the exact candidate SHA.

    ``url`` links the latest run of that check ("" when no run exists).
    The latest run per check name wins, so a maintainer-triggered rerun of a
    failed job on the same SHA is recognized without any extra machinery.
    """

    name: str
    state: CheckState
    url: str = ""


@dataclass(frozen=True)
class Candidate:
    """The release candidate SHA and how it was established.

    ``sha`` is the commit under qualification: the notes-PR merge commit
    (CURRENT), an owner-adopted head (ADOPTED), or the last valid candidate
    the branch moved past (INVALIDATED). "" when NONE.
    ``branch_head`` is the live branch head at reconcile time.
    """

    state: CandidateState
    sha: str = ""
    branch_head: str = ""


@dataclass(frozen=True)
class QualificationStatus:
    """Evidence of the no-publish qualification run on the candidate SHA.

    Evidence is GitHub-native: the run id, its conclusion, and its job
    results, re-queried live, never a stored assertion. ``run_id`` 0 means
    no qualification run exists for this exact SHA yet.
    """

    run_id: int = 0
    url: str = ""
    passed: bool = False
    pending: bool = False
    failed_jobs: tuple[str, ...] = ()


class OutputState(str, Enum):
    """Verification state of one required public output."""

    VERIFIED = "verified"
    PENDING = "pending"
    FAILED = "failed"
    BLOCKED = "blocked"  # prerequisite output not yet verified
    SKIPPED = "skipped"  # not applicable to this release (e.g. packages on rc)


@dataclass(frozen=True)
class DownstreamOutput:
    """One required public output and its live verification state.

    ``name`` is a stable key (also used in the checklist); ``detail`` is a
    one-line human explanation of the current state; ``url`` links the
    evidence (PR, workflow run, registry tag, public file) when one exists.
    ``action`` names the idempotent side effect reconciliation should take
    to make progress ("" when none): the verifier is the one place that
    knows both the ordering gate and the current public state, so it, not
    the action runner, decides when starting work is safe.
    """

    name: str
    state: OutputState
    detail: str = ""
    url: str = ""
    action: str = ""
    run_id: int = 0  # the workflow run backing this evidence, when one does


@dataclass(frozen=True)
class ReleaseStatus:
    """Recomputed truth for one active release, rendered into the issue.

    ``version``/``stage`` come from the notes PR head branch
    (``agent/release-cut/<version>-<stage>``), the bot-created artifact that
    pins the release decision; both are "" until that PR exists.
    ``phase`` is the furthest phase whose entry evidence exists; ``ready``
    remains the stage-2 pre-publication gate (candidate valid + required CI
    green + qualification passed). ``blockers`` lists, in render order, what
    currently prevents the *next* phase transition. ``alerts`` are the
    subset needing human attention that the one-shot notifier escalates
    (they also render as blockers).
    """

    repo: str
    branch: str
    version: str = ""
    stage: str = ""
    notes_pr_number: int = 0
    notes_pr_url: str = ""
    notes_pr_merged: bool = False
    candidate: Candidate = Candidate(state=CandidateState.NONE)
    checks: tuple[RequiredCheck, ...] = ()
    qualification: QualificationStatus = QualificationStatus()
    phase: ReleasePhase = ReleasePhase.NOTES
    published: bool = False
    release_url: str = ""
    # URL of the publish run waiting at the approval gate, populated by
    # reconciliation when the phase is READY and the run is visible; ""
    # otherwise. Display-only: nothing gates on it.
    approval_run_url: str = ""
    outputs: tuple[DownstreamOutput, ...] = ()
    ready: bool = False
    blockers: tuple[str, ...] = ()
    alerts: tuple[str, ...] = ()
