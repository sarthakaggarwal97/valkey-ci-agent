"""Recompute release truth from GitHub and reconcile the tracking issue.

The controller's single source of truth is live GitHub state:

- The **release decision** (version + stage) is pinned by the release-notes
  PR, the bot-created artifact whose head branch is
  ``agent/release-cut/<version>-<stage>`` into the release branch. The
  tracker carries a durable **identity binding** (a controller receipt
  comment) recording the derived version/stage and, once identified, the
  notes PR number and its merge SHA; reconciliation reads the binding before
  any PR scan, so the bound PR can never be displaced or evicted.
- The **candidate SHA** is that PR's merge commit, but only while it remains
  the branch head. If the branch moves, the candidate is invalidated until an
  authorized owner adopts the *exact* new head, or re-adopts the pinned
  candidate to confirm shipping it despite the movement, via the ``adopt``
  entry point.
- **Required checks** are evaluated by exact candidate SHA. The latest check
  run per name wins, so a maintainer rerun of a failed job on the same SHA is
  recognized without special handling.

Reconciliation is short and idempotent: recompute, re-render the issue, skip
the edit when nothing changed. It never blocks waiting for CI or merges.
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import partial
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.common.labels import ensure_label
from scripts.release import actions as actions_mod
from scripts.release import checks as checks_mod
from scripts.release import issue as issue_mod
from scripts.release import qualification as qual_mod
from scripts.release import verify as verify_mod
from scripts.release.authorize import ensure_authorized
from scripts.release.models import (
    Candidate,
    CandidateState,
    DerivedRelease,
    QualificationStatus,
    ReleaseBinding,
    ReleaseIntent,
    ReleasePhase,
    ReleaseStatus,
    RequiredCheck,
    release_tag,
)
from scripts.release.policy import TRACKER_LABEL, RepoReleasePolicy
from scripts.release.release_refs import NOTES_PREP_BRANCH_RE, resolve_tag_commit
from scripts.release.versioning import derive_version

logger = logging.getLogger(__name__)

# Bound the scan for the release-notes PR among PRs targeting the release
# branch (which also receives backport PRs). PRs are listed newest-first, and
# a notes PR for an active release is by construction recent. The scan runs
# only until a binding exists; afterwards the bound PR is fetched by number.
_NOTES_PR_SCAN_LIMIT = 200


class ReleaseControlError(Exception):
    """A release action is impossible or refused; the message says why."""


class _BoundNotesPrLost(Exception):
    """The tracker's bound notes PR is missing or closed unmerged.

    Internal to this module: reconciliation converts it into a standing
    alert (never a rebind), and a duplicate start converts it into a
    refusal. ``binding`` carries the identity for display.
    """

    def __init__(self, message: str, binding: ReleaseBinding) -> None:
        super().__init__(message)
        self.binding = binding


@dataclass(frozen=True)
class StartResult:
    """Outcome of a Start Release request.

    ``created`` False means an active release already exists for the branch
    and its issue was reused (the duplicate start performed no writes).
    ``cut_needed`` True means no release-notes PR is bound to the tracker
    yet, so the workflow should (re)run the notes cut, both on a fresh start and
    on a resume after a failed cut. A duplicate start whose notes PR already
    exists reports the version that PR pins and ``cut_needed`` False.
    """

    created: bool
    cut_needed: bool
    issue_number: int
    issue_url: str
    version: str
    stage: str
    tag: str


def start_release(
    gh: Any,
    policy: RepoReleasePolicy,
    *,
    branch: str,
    intent: ReleaseIntent,
    actor: str,
    dry_run: bool = False,
) -> StartResult:
    """Authorize and start a release on *branch*: derive the version and
    create (or reuse) the tracking issue.

    Order matters and is safety-driven: the embargo refusal and the branch
    check run before any GitHub call, and authorization runs before any
    write. Raises :class:`ReleaseControlError` on refusal.
    """
    # Break-glass only: an embargoed release must never create a public
    # artifact. Refused before any API call so nothing leaks, not even reads.
    if intent is ReleaseIntent.SECURITY:
        raise ReleaseControlError(
            "security/embargoed releases are break-glass only: run the manual "
            "process; the controller will not create a public tracking issue"
        )

    if branch not in policy.branches:
        raise ReleaseControlError(
            f"branch {branch!r} is not a configured release branch of "
            f"{policy.repo} (policy allows: {', '.join(policy.branches)})"
        )

    ensure_authorized(gh, policy, actor)

    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )

    # Dedup: with a release already active on this branch, this start reuses
    # its issue and performs no writes. What it *reports* depends on whether
    # a notes PR is bound to the tracker yet:
    #   - none (the cut failed or never ran): re-derive and ask for the cut
    #     again, so recovery never requires hand-typing a version;
    #   - one whose tag already exists: the release shipped; refuse so the
    #     operator closes the tracker instead of silently no-opping forever;
    #   - one still in flight: report its pinned version, no cut needed.
    existing = issue_mod.find_release_issue(repo, branch, label=TRACKER_LABEL)
    if existing is not None:
        return _reuse_active_release(gh, repo, policy, branch, intent, existing)

    tags = _tag_names(repo, policy)
    derived = derive_version(branch, intent, tags)

    status = ReleaseStatus(
        repo=policy.repo,
        branch=branch,
        version=derived.version,
        stage=derived.stage,
        candidate=Candidate(state=CandidateState.NONE),
        ready=False,
        blockers=(
            "No release-notes PR exists yet; the Start Release workflow cuts "
            "one, and its merge commit becomes the candidate SHA.",
        ),
    )
    title = issue_mod.render_title(branch, derived.version, derived.stage)
    body = issue_mod.render_body(status, datetime.now(timezone.utc))
    if dry_run:
        logger.info("[dry-run] would create issue %r on %s", title, policy.repo)
        return StartResult(
            created=True, cut_needed=True, issue_number=0, issue_url="",
            version=derived.version, stage=derived.stage, tag=derived.tag,
        )
    issue_mod.ensure_tracker_labels(repo, branch, TRACKER_LABEL)
    created = retry_github_call(
        lambda: repo.create_issue(
            title=title, body=body,
            labels=[TRACKER_LABEL, issue_mod.branch_label(branch)],
        ),
        retries=2, description="create release tracking issue",
    )
    logger.info("Created release tracking issue #%s: %s", created.number, created.html_url)
    # The durable identity binding: the receipt travels with the tracker so
    # a duplicate start resumes THIS release instead of re-deriving one.
    issue_mod.write_binding(
        created, ReleaseBinding(version=derived.version, stage=derived.stage),
        gh, assume_absent=True,
    )
    return StartResult(
        created=True,
        cut_needed=True,
        issue_number=created.number,
        issue_url=created.html_url,
        version=derived.version,
        stage=derived.stage,
        tag=derived.tag,
    )


def _intent_matches_stage(intent: ReleaseIntent, stage: str) -> bool:
    """True when *intent* would produce a release of the bound *stage*'s kind.

    Stage granularity only: RC intents belong to ``rcN`` stages, GA and
    PATCH intents to ``ga`` stages. The bound version itself is never
    re-derived, so finer disagreement cannot arise.
    """
    if intent is ReleaseIntent.RC:
        return stage.startswith("rc")
    return stage == "ga"


def _reuse_active_release(
    gh: Any, repo: Any, policy: RepoReleasePolicy, branch: str,
    intent: ReleaseIntent, existing: Any,
) -> StartResult:
    """Resolve a duplicate start against the release already active on *branch*."""
    logger.info(
        "Active release already tracked in issue #%s; reusing it "
        "(one active release per branch)", existing.number,
    )
    binding = issue_mod.read_binding(existing, gh)
    if binding is not None and not _intent_matches_stage(intent, binding.stage):
        raise ReleaseControlError(
            f"a {binding.stage} release ({binding.version}) is already bound "
            f"to tracker #{existing.number}; refusing to restart it with "
            f"intent {intent.value!r}. Finish that release, or close its "
            f"tracker, before starting a different kind of release on {branch}"
        )
    try:
        notes_pr = _find_notes_pr(repo, policy, branch,
                                  created_after=existing.created_at,
                                  tracking_issue=existing, gh=gh)
    except _BoundNotesPrLost as exc:
        raise ReleaseControlError(str(exc)) from exc
    if notes_pr is None:
        if binding is not None:
            # The bound identity stands; never re-derive over it.
            pinned = DerivedRelease(version=binding.version, stage=binding.stage)
            logger.info("No notes PR yet for bound %s on issue #%s; "
                        "requesting a (re)cut", pinned.tag, existing.number)
        else:
            # Pre-binding tracker whose cut failed or never ran. Re-deriving
            # is safe (derivation is pure and the release has produced
            # nothing to disagree with); the result is bound so the next
            # resume no longer re-derives.
            pinned = derive_version(branch, intent, _tag_names(repo, policy))
            issue_mod.write_binding(
                existing,
                ReleaseBinding(version=pinned.version, stage=pinned.stage), gh,
            )
            logger.info("No notes PR bound to issue #%s; requesting a (re)cut "
                        "of %s", existing.number, pinned.tag)
        return StartResult(
            created=False, cut_needed=True,
            issue_number=existing.number, issue_url=existing.html_url,
            version=pinned.version, stage=pinned.stage, tag=pinned.tag,
        )

    match = NOTES_PREP_BRANCH_RE.match(notes_pr.head.ref)
    assert match is not None  # _find_notes_pr only returns matching PRs
    pinned = DerivedRelease(version=match.group(1), stage=match.group(2))
    if pinned.tag in set(_tag_names(repo, policy)):
        if _release_complete_shaped(gh, policy, branch, existing):
            raise ReleaseControlError(
                f"tag {pinned.tag} already exists and the release is "
                f"complete; close tracking issue #{existing.number} to start "
                f"the next release on {branch}"
            )
        raise ReleaseControlError(
            f"tag {pinned.tag} already exists but the release on this branch "
            f"is still in flight (tracker #{existing.number}); wait for it "
            f"to complete or investigate the tracker before starting the "
            f"next release"
        )
    return StartResult(
        created=False, cut_needed=False,
        issue_number=existing.number, issue_url=existing.html_url,
        version=pinned.version, stage=pinned.stage, tag=pinned.tag,
    )


def _release_complete_shaped(
    gh: Any, policy: RepoReleasePolicy, branch: str, tracking_issue: Any, *,
    gh_downstream: Any = None,
) -> bool:
    """True when the release *tracking_issue* observes is COMPLETE-shaped
    (published with every required public output verified)."""
    status = compute_status(gh, policy, branch, tracking_issue=tracking_issue,
                            gh_downstream=gh_downstream)
    return status.phase is ReleasePhase.COMPLETE


def _tag_names(repo: Any, policy: RepoReleasePolicy) -> list[str]:
    return [
        t.name for t in retry_github_call(
            lambda: list(repo.get_tags()),
            retries=2, description=f"list tags of {policy.repo}",
        )
    ]


def compute_status(
    gh: Any,
    policy: RepoReleasePolicy,
    branch: str,
    *,
    tracking_issue: Any = None,
    gh_downstream: Any = None,
) -> ReleaseStatus:
    """Recompute the full release status for *branch* from live GitHub and
    public state.

    ``tracking_issue`` supplies adoption acknowledgements (bot-authored
    comments) and binds the notes-PR search to this release (only PRs created
    after the tracker count, so a previous release's merged notes PR can
    never be mistaken for this one). Everything else comes from PRs, refs,
    check runs, workflow runs, and public endpoints.
    """
    gh_downstream = gh_downstream or gh
    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    branch_head = retry_github_call(
        lambda: repo.get_branch(branch).commit.sha,
        retries=2, description=f"resolve head of {branch}",
    )

    created_after = tracking_issue.created_at if tracking_issue is not None else None
    try:
        notes_pr = _find_notes_pr(repo, policy, branch, created_after=created_after,
                                  tracking_issue=tracking_issue, gh=gh)
    except _BoundNotesPrLost as exc:
        # The bound PR is gone or closed unmerged. Never rebind: surface a
        # standing alert so a human decides; every pass re-raises it until
        # the PR is restored or the tracker is closed.
        alert = str(exc)
        return ReleaseStatus(
            repo=policy.repo,
            branch=branch,
            version=exc.binding.version,
            stage=exc.binding.stage,
            candidate=Candidate(state=CandidateState.NONE, branch_head=branch_head),
            phase=ReleasePhase.NOTES,
            blockers=(alert,),
            alerts=(alert,),
        )
    if notes_pr is None:
        return ReleaseStatus(
            repo=policy.repo,
            branch=branch,
            candidate=Candidate(state=CandidateState.NONE, branch_head=branch_head),
            phase=ReleasePhase.NOTES,
            blockers=(
                "No release-notes PR is bound to this release yet; rerun "
                "Start Release to (re)cut one.",
            ),
        )

    match = NOTES_PREP_BRANCH_RE.match(notes_pr.head.ref)
    assert match is not None  # _find_notes_pr only returns matching PRs
    version, stage = match.group(1), match.group(2)

    # merged_at ships in the list payload (unlike .merged, which triggers a
    # lazy per-PR GET). _find_notes_pr drops closed-unmerged PRs, so an
    # unmerged PR here is open and awaiting merge.
    if notes_pr.merged_at is None:
        return ReleaseStatus(
            repo=policy.repo,
            branch=branch,
            version=version,
            stage=stage,
            notes_pr_number=notes_pr.number,
            notes_pr_url=notes_pr.html_url,
            notes_pr_merged=False,
            candidate=Candidate(state=CandidateState.NONE, branch_head=branch_head),
            phase=ReleasePhase.NOTES,
            blockers=(
                f"Release-notes PR #{notes_pr.number} is not merged; its merge "
                f"commit becomes the candidate SHA.",
            ),
        )

    tag = release_tag(version, stage)
    release = _find_release(repo, tag)
    if release is not None:
        return _published_status(
            gh, gh_downstream, repo, policy, branch=branch, version=version,
            stage=stage, tag=tag, release=release, branch_head=branch_head,
            notes_pr=notes_pr, tracking_issue=tracking_issue,
        )

    notes_merge_sha = notes_pr.merge_commit_sha or ""
    blockers: list[str] = []
    alerts: list[str] = []
    checks: tuple[RequiredCheck, ...] = ()
    qualification = QualificationStatus()
    phase = ReleasePhase.CANDIDATE

    # Branch-level daily-CI gate (observation-only): the release branch's
    # newest completed daily run must be green and fresh before READY. It
    # never stops the per-commit checks or qualification from progressing;
    # its blockers only hold the READY transition.
    daily = checks_mod.evaluate_daily(repo, policy, branch,
                                      datetime.now(timezone.utc))
    daily_blockers = checks_mod.daily_blockers(daily)

    # A tag without a release is a five-alarm state: the ruleset makes the
    # tag unmovable and undeletable, so the version is unshippable through
    # the controller. Surface it loudly instead of marching toward a READY
    # that publication will always refuse.
    stray_tag_sha = resolve_tag_commit(repo, tag)
    if stray_tag_sha:
        alerts.append(
            f"Tag `{tag}` exists (at `{stray_tag_sha[:12]}`) but no release "
            f"does; the tag cannot be moved or deleted, so this version is "
            f"unshippable through the controller. Investigate how the tag "
            f"was created; publishing requires cutting the next version."
        )

    if not notes_merge_sha:
        candidate = Candidate(state=CandidateState.NONE, branch_head=branch_head)
        blockers.append(
            f"Release-notes PR #{notes_pr.number} is merged but GitHub reports "
            f"no merge commit; cannot establish a candidate SHA."
        )
    else:
        candidate = _resolve_candidate(notes_merge_sha, branch_head, tracking_issue, gh)
        if candidate.state is CandidateState.INVALIDATED:
            blockers.append(
                f"Branch `{branch}` moved past the candidate (`{candidate.sha[:12]}`); "
                f"an authorized owner must adopt the exact new head "
                f"(`{branch_head[:12]}`) via the adopt command before qualification "
                f"continues."
            )
        else:
            checks = checks_mod.evaluate_required_checks(repo, policy, candidate.sha)
            blockers.extend(checks_mod.check_blockers(checks))
            if not blockers and not alerts:
                phase = ReleasePhase.QUALIFICATION
                qualification = qual_mod.evaluate_qualification(
                    gh_downstream, policy, tag=tag, sha=candidate.sha,
                )
                if qualification.passed:
                    # The daily gate holds READY (and with it the publish
                    # dispatch and the protected publish path, both keyed on
                    # the phase); its blocker below says why.
                    if not daily_blockers:
                        phase = ReleasePhase.READY
                elif qualification.pending:
                    blockers.append(
                        f"Qualification run {qualification.run_id} is still "
                        f"executing on the candidate SHA."
                    )
                elif qualification.run_id:
                    failed = ", ".join(qualification.failed_jobs[:5])
                    blockers.append(
                        f"Qualification run {qualification.run_id} failed "
                        f"({failed}); the first failure is retried once "
                        f"automatically. After a failed retry, fix the cause "
                        f"and re-dispatch the qualification workflow (a new "
                        f"run for the same SHA supersedes this one)."
                    )
                else:
                    blockers.append(
                        "No qualification run exists for the candidate SHA yet; "
                        "reconciliation dispatches one."
                    )

    blockers.extend(daily_blockers)
    blockers.extend(alerts)
    return ReleaseStatus(
        repo=policy.repo,
        branch=branch,
        version=version,
        stage=stage,
        notes_pr_number=notes_pr.number,
        notes_pr_url=notes_pr.html_url,
        notes_pr_merged=True,
        candidate=candidate,
        checks=checks,
        qualification=qualification,
        daily=daily,
        phase=phase,
        ready=not blockers,
        blockers=tuple(blockers),
        alerts=tuple(alerts),
    )


def _published_status(
    gh: Any, gh_downstream: Any, repo: Any, policy: RepoReleasePolicy, *,
    branch: str, version: str, stage: str, tag: str, release: Any,
    branch_head: str, notes_pr: Any, tracking_issue: Any = None,
) -> ReleaseStatus:
    """Status once the release exists: verify the tag pins the release and
    observe every downstream public output through completion.

    The tag's commit is trusted only when it is a SHA the release process
    actually vetted: the notes-PR merge commit or an owner-adopted head
    recorded on the tracker. Any other commit means the tag was created (or
    moved) outside the controller; that alerts loudly, blocks completion,
    and skips downstream verification so no untrusted artifact is pushed
    further downstream.
    """
    tag_sha = resolve_tag_commit(repo, tag)
    trusted_shas = {sha for sha in (notes_pr.merge_commit_sha,) if sha}
    if tracking_issue is not None:
        trusted_shas.update(issue_mod.adopted_shas(tracking_issue, gh))
    tag_trusted = tag_sha in trusted_shas
    base = {
        "repo": policy.repo,
        "branch": branch,
        "version": version,
        "stage": stage,
        "notes_pr_number": notes_pr.number,
        "notes_pr_url": notes_pr.html_url,
        "notes_pr_merged": True,
        "candidate": Candidate(state=CandidateState.CURRENT, sha=tag_sha,
                               branch_head=branch_head),
        "published": True,
        "release_url": release.html_url,
        "tag_trusted": tag_trusted,
    }
    if not tag_trusted:
        alert = (
            f"Release tag {tag} points at {tag_sha[:12] or '<unresolvable>'}, "
            f"which was never a trusted candidate (notes merge or "
            f"owner-adopted). Manual investigation required."
        )
        return ReleaseStatus(
            phase=ReleasePhase.PUBLISHED,
            blockers=(alert,),
            alerts=(alert,),
            **base,
        )
    if bool(release.prerelease) != (stage != "ga"):
        alert = (
            f"The prerelease flag on release {tag} is {release.prerelease} but "
            f"the stage is {stage}; fix the release metadata."
        )
        return ReleaseStatus(
            phase=ReleasePhase.PUBLISHED,
            blockers=(alert,),
            alerts=(alert,),
            **base,
        )

    core = verify_mod.verify_core_outputs(
        gh_downstream, policy, tag=tag, stage=stage,
        gh_source=gh, published_at=release.published_at,
    )
    ordered = verify_mod.verify_ordered_outputs(
        gh_downstream, policy, version=version, tag=tag, stage=stage, core=core,
        published_at=release.published_at,
    )
    outputs = verify_mod.escalate_stalled_outputs(
        core + ordered, release.published_at, policy.check_timeout_minutes,
    )
    if verify_mod.outputs_all_settled(outputs):
        phase = ReleasePhase.COMPLETE
    elif verify_mod.outputs_all_settled(core):
        phase = ReleasePhase.BUNDLE_HELM
    else:
        phase = ReleasePhase.PUBLISHED
    return ReleaseStatus(phase=phase, outputs=outputs, **base)


def _find_release(repo: Any, tag: str) -> Any:
    """The published GitHub release for *tag*, or None.

    ``GET /releases/tags/{tag}`` does not return drafts, which is the
    correct behavior here, since a draft creates no tag and publishes
    nothing.
    """
    try:
        return retry_github_call(
            lambda: repo.get_release(tag),
            retries=2, description=f"get release {tag}",
        )
    except GithubException as exc:
        if exc.status == 404:
            return None
        raise


# The only state label: needs-attention mirrors the failure state. The
# retired phase:* namespace is still recognized for cleanup (see
# _sync_phase_labels).
_ATTENTION_LABEL = "needs-attention"

_LABEL_COLORS = {_ATTENTION_LABEL: "d73a4a"}

_LABEL_DESCRIPTIONS = {
    _ATTENTION_LABEL: "The release controller found failures needing a human",
}


def _sync_phase_labels(repo: Any, tracking_issue: Any, status: ReleaseStatus) -> None:
    """Mirror the failure state into the ``needs-attention`` label, by diff.

    Titles stay constant and the phase lives inside the tracker, so the
    only state label is ``needs-attention`` while failures exist. The
    controller still owns the retired ``phase:*`` namespace solely to
    strip stale phase labels from trackers created before the retirement;
    labels outside the owned set are never touched.
    """
    current = {getattr(label, "name", "") for label in tracking_issue.labels}
    desired: set[str] = set()
    if issue_mod.has_failures(status):
        desired.add(_ATTENTION_LABEL)
    # The controller owns the whole phase:* namespace plus needs-attention;
    # everything else on the issue is out of bounds.
    owned = {name for name in current if name.startswith("phase:")}
    owned.add(_ATTENTION_LABEL)
    to_add = sorted(desired - current)
    to_remove = sorted((current & owned) - desired)
    for name in to_add:
        ensure_label(repo, name, _LABEL_COLORS[name], _LABEL_DESCRIPTIONS[name])
    if to_add:
        retry_github_call(
            lambda: tracking_issue.add_to_labels(*to_add),
            retries=2, description=f"add labels {', '.join(to_add)}",
        )
    for name in to_remove:
        retry_github_call(
            partial(tracking_issue.remove_from_labels, name),
            retries=2, description=f"remove label {name}",
        )


def _render_tracker(tracking_issue: Any, status: ReleaseStatus) -> None:
    """Re-render the tracking issue from *status*, skipping no-op edits.

    Bodies are compared with the footer timestamp normalized out
    (:func:`issue_mod.normalize_body_timestamp`), so a pass that changes
    nothing but the "Updated" stamp performs zero edits: an idle release
    never churns the tracker, and the freshness heartbeat lives in the
    workflow logs instead. The title is edited only when it differs, and
    keeps the start-time title until a notes PR pins a version, so it never
    flaps between derived and recomputed forms.
    """
    body = issue_mod.render_body(status, datetime.now(timezone.utc))
    title = (
        issue_mod.render_live_title(status)
        if status.version else tracking_issue.title
    )
    body_unchanged = (
        issue_mod.normalize_body_timestamp(tracking_issue.body)
        == issue_mod.normalize_body_timestamp(body)
    )
    if body_unchanged and tracking_issue.title == title:
        logger.info("Issue #%s already reflects current state; no edit "
                    "(timestamp-only difference is not a change)",
                    tracking_issue.number)
        return
    if tracking_issue.title == title:
        retry_github_call(
            lambda: tracking_issue.edit(body=body),
            retries=2, description=f"update issue #{tracking_issue.number}",
        )
    else:
        retry_github_call(
            lambda: tracking_issue.edit(title=title, body=body),
            retries=2, description=f"update issue #{tracking_issue.number}",
        )


def reconcile_branch(
    gh: Any, policy: RepoReleasePolicy, branch: str, *, act: bool = True,
    gh_downstream: Any = None, gh_agent: Any = None, agent_repo: str = "",
) -> ReleaseStatus | None:
    """Reconcile the tracking issue for *branch*; returns None when no
    release is active.

    Idempotent: re-renders the issue from recomputed state and skips the edit
    when title and body already match, so repeated runs cause no churn. With
    ``act`` (the default) it also performs the guarded progress actions the
    state calls for (dispatch qualification/bundle, open the helm PR, notify
    once, close on completion); ``act=False`` observes only.
    """
    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    tracking_issue = issue_mod.find_release_issue(repo, branch, label=TRACKER_LABEL)
    if tracking_issue is None:
        _warn_abandoned_tracker(gh, repo, branch, act=act)
        logger.info("No active release on %s %s; nothing to reconcile", policy.repo, branch)
        return None

    gh_downstream = gh_downstream or gh
    # Backfill the branch identity label on pre-label trackers so their
    # discovery stops depending on the editable body marker.
    if issue_mod.branch_label(branch) not in {
        getattr(label, "name", "") for label in tracking_issue.labels
    }:
        issue_mod.ensure_tracker_labels(repo, branch, TRACKER_LABEL)
        retry_github_call(
            lambda: tracking_issue.add_to_labels(issue_mod.branch_label(branch)),
            retries=2, description=f"backfill {issue_mod.branch_label(branch)}",
        )

    status = compute_status(gh, policy, branch, tracking_issue=tracking_issue,
                            gh_downstream=gh_downstream)

    # The trusted controller version, resolved once per pass: publish runs
    # parked at the approval gate on any other commit are stale and get
    # cancelled instead of blocking (or serving) a fresh dispatch.
    agent_head = ""
    if status.phase is ReleasePhase.READY and gh_agent is not None and agent_repo:
        agent_head = actions_mod.agent_head_sha(gh_agent, agent_repo)

    if act:
        for performed in actions_mod.advance(
            gh_downstream, policy, status=status, tracking_issue=tracking_issue,
            gh_agent=gh_agent, agent_repo=agent_repo, agent_head_sha=agent_head,
        ):
            logger.info("Action: %s", performed)

    # Display-only: link the waiting publish run so the READY callout can
    # say exactly where to approve. Fetched after advance() so a run the
    # dispatch just created has its best chance of being visible. Finder
    # only (never cancels), and candidate-bound: threading the tag and
    # candidate SHA means a run parked for another tag or candidate is
    # never presented as the place to approve.
    if status.phase is ReleasePhase.READY and gh_agent is not None and agent_repo:
        approval_url = actions_mod.waiting_publish_run_url(
            gh_agent, agent_repo, branch, agent_head,
            tag=release_tag(status.version, status.stage),
            candidate_sha=status.candidate.sha,
        )
        if approval_url:
            status = replace(status, approval_run_url=approval_url)

    _sync_phase_labels(repo, tracking_issue, status)
    _render_tracker(tracking_issue, status)
    logger.info("Reconciled issue #%s (phase=%s)", tracking_issue.number, status.phase.value)
    return status


def _warn_abandoned_tracker(gh: Any, repo: Any, branch: str, *, act: bool) -> None:
    """Warn (once) on a tracker closed while its release was still observed.

    With no open tracker, the newest CLOSED tracker for *branch* is
    inspected. A controller-closed tracker carries the completion marker (a
    finished release: silence is correct). One closed without it was
    abandoned mid-observation, silently ending reconciliation for a release
    that may still be moving; a one-shot marker-gated warning tells the
    human how to resume. ``act`` False observes only (no comment).
    """
    closed = issue_mod.find_release_issue(repo, branch, label=TRACKER_LABEL,
                                          state="closed")
    if closed is None:
        return
    comments = issue_mod.trusted_comments(closed, gh)
    if any(issue_mod.marker_present(comment.body, issue_mod.complete_marker())
           for comment in comments):
        return  # controller-closed on completion; nothing abandoned
    marker = issue_mod.closed_warning_marker()
    if any(issue_mod.marker_present(comment.body, marker)
           for comment in comments):
        return  # already warned once
    logger.warning(
        "Tracker #%s on %s was closed while the release was still being "
        "observed", closed.number, branch,
    )
    if not act:
        return
    retry_github_call(
        lambda: closed.create_comment(body=(
            f"{marker}\n"
            "> [!WARNING]\n"
            "> This tracker was closed while the release was still being "
            "observed. Reopen it or dispatch release-start to resume "
            "observation."
        )),
        retries=2, description=f"warn on abandoned tracker #{closed.number}",
    )
    issue_mod.invalidate_comment_memo(closed)


def adopt_candidate(
    gh: Any,
    policy: RepoReleasePolicy,
    *,
    branch: str,
    sha: str,
    actor: str,
) -> ReleaseStatus:
    """Record an authorized owner's adoption of the branch's exact new head.

    Refuses unless: the actor is authorized (live team check), an active
    release with an invalidated candidate exists, and *sha* equals the
    current branch head exactly. On success posts a bot-authored
    acknowledgement comment (the only adoption record reconciliation trusts)
    and returns the freshly reconciled status.
    """
    ensure_authorized(gh, policy, actor)

    sha = sha.strip().lower()
    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    tracking_issue = issue_mod.find_release_issue(repo, branch, label=TRACKER_LABEL)
    if tracking_issue is None:
        raise ReleaseControlError(f"no active release on {policy.repo} {branch}")

    status = compute_status(gh, policy, branch, tracking_issue=tracking_issue)
    if status.candidate.state is not CandidateState.INVALIDATED or status.published:
        raise ReleaseControlError(
            f"candidate is {status.candidate.state.value}"
            f"{' and the release is published' if status.published else ''}; "
            f"adoption applies only to an invalidated candidate after branch "
            f"movement, before publication"
        )
    # Two acceptable adoptions after branch movement: the exact new head
    # (ship what the branch became), or the pinned candidate itself (or the
    # bound notes merge SHA) as explicit reconfirmation to ship the pinned
    # candidate despite the movement. Anything else is refused: adoption is
    # never a way to pick an arbitrary commit.
    allowed = {status.candidate.branch_head, status.candidate.sha}
    binding = issue_mod.read_binding(tracking_issue, gh)
    if binding is not None and binding.merge_sha:
        allowed.add(binding.merge_sha)
    allowed.discard("")
    if sha not in allowed:
        raise ReleaseControlError(
            f"adoption requires the exact current head of {branch} "
            f"({status.candidate.branch_head}), or the pinned candidate "
            f"({status.candidate.sha}) to reconfirm shipping it despite the "
            f"branch movement; got {sha or '<empty>'!r}. "
            f"Re-check the branch and pass the full 40-character SHA."
        )

    comment = (
        f"{issue_mod.adopt_marker(sha)}\n"
        f"Candidate adoption: @{actor} acknowledged the branch movement and "
        f"adopted `{sha}` as the release candidate. Required checks will be "
        f"evaluated against this exact SHA on the next reconciliation."
    )
    retry_github_call(
        lambda: tracking_issue.create_comment(body=comment),
        retries=2, description=f"record adoption on issue #{tracking_issue.number}",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)
    logger.info("Recorded adoption of %s on issue #%s", sha, tracking_issue.number)

    refreshed = compute_status(gh, policy, branch, tracking_issue=tracking_issue)
    _render_tracker(tracking_issue, refreshed)
    return refreshed


def _find_notes_pr(
    repo: Any, policy: RepoReleasePolicy, branch: str, *, created_after: Any = None,
    tracking_issue: Any = None, gh: Any = None,
) -> Any:
    """The release-notes PR bound to the active release on *branch*, or None.

    With *tracking_issue* the durable identity binding is consulted first:
    a bound PR is fetched by number (no scan), so a newer PR with a
    notes-style head can never displace it and eviction from the scan
    window cannot unbind it. A bound PR that is missing or closed unmerged
    raises :class:`_BoundNotesPrLost` (never a rebind). The scan runs only
    while no PR is bound; a hit is then bound so the next pass skips the
    scan. A version-only binding constrains the scan: a PR pinning a
    different version or stage is not this release's notes PR.

    PRs into a release branch are mostly backports; the notes PR is
    identified by its namespaced head branch and its version matching the
    release line. Three guards keep the binding sound:

    - the head must live in the upstream repo itself (the cut pushes prep
      branches upstream): ``head.ref`` of a fork PR is an attacker-chosen
      string, so without this anyone could displace the real notes PR;
    - PRs closed without merging are abandoned cuts, skipped so they neither
      wedge the release nor shadow an older valid PR;
    - with ``created_after`` (the tracker's server-set creation time), only
      PRs newer than the tracker count, so a previous release's merged notes
      PR is never mistaken for this release's.

    Newest-first listing plus a scan cap bounds the work; hitting the cap is
    logged loudly since it would misreport the notes PR as missing.
    """
    binding = (issue_mod.read_binding(tracking_issue, gh)
               if tracking_issue is not None else None)
    if binding is not None and binding.notes_pr_number:
        pull = _fetch_bound_notes_pr(repo, binding)
        merge_sha = (pull.merge_commit_sha or "").lower()
        if merge_sha and merge_sha != binding.merge_sha:
            issue_mod.write_binding(
                tracking_issue, replace(binding, merge_sha=merge_sha), gh,
            )
        return pull

    pulls = retry_github_call(
        lambda: list(itertools.islice(
            repo.get_pulls(state="all", base=branch, sort="created", direction="desc"),
            _NOTES_PR_SCAN_LIMIT + 1,
        )),
        retries=2, description=f"list PRs into {branch}",
    )
    if len(pulls) > _NOTES_PR_SCAN_LIMIT:
        logger.warning(
            "Hit the %d-PR scan limit looking for the notes PR on %s; "
            "an older notes PR may be misreported as missing",
            _NOTES_PR_SCAN_LIMIT, branch,
        )
        pulls = pulls[:_NOTES_PR_SCAN_LIMIT]
    for pull in pulls:
        match = NOTES_PREP_BRANCH_RE.match(pull.head.ref)
        if not match or not match.group(1).startswith(f"{branch}."):
            continue
        if binding is not None and (
            match.group(1) != binding.version or match.group(2) != binding.stage
        ):
            continue  # another identity's cut; the bound version stands
        head_repo = pull.head.repo
        if head_repo is None or head_repo.full_name != policy.repo:
            logger.warning(
                "Ignoring PR #%s: notes-style head %r lives outside %s",
                pull.number, pull.head.ref, policy.repo,
            )
            continue
        if pull.state == "closed" and pull.merged_at is None:
            logger.info("Ignoring PR #%s: notes cut closed without merging", pull.number)
            continue
        if created_after is not None and pull.created_at <= created_after:
            continue
        if tracking_issue is not None:
            issue_mod.write_binding(
                tracking_issue,
                ReleaseBinding(
                    version=match.group(1), stage=match.group(2),
                    notes_pr_number=pull.number,
                    merge_sha=(pull.merge_commit_sha or "").lower(),
                ),
                gh,
            )
        return pull
    return None


def _fetch_bound_notes_pr(repo: Any, binding: ReleaseBinding) -> Any:
    """The notes PR the binding pins, fetched by number (never a scan).

    Raises :class:`_BoundNotesPrLost` when the PR cannot be fetched or was
    closed without merging: the binding is never rewritten to another PR,
    so the failure surfaces as an alert (reconcile) or refusal (start)
    instead of a silent rebind.
    """
    tag = release_tag(binding.version, binding.stage)
    try:
        pull = retry_github_call(
            lambda: repo.get_pull(binding.notes_pr_number),
            retries=2, description=f"get bound notes PR #{binding.notes_pr_number}",
        )
    except GithubException as exc:
        raise _BoundNotesPrLost(
            f"the release-notes PR #{binding.notes_pr_number} bound to this "
            f"{tag} release cannot be fetched (HTTP {exc.status}); the "
            f"binding is never rewritten, so investigate the PR before "
            f"proceeding", binding,
        ) from exc
    if pull.state == "closed" and pull.merged_at is None:
        raise _BoundNotesPrLost(
            f"the release-notes PR #{binding.notes_pr_number} bound to this "
            f"{tag} release was closed without merging; the controller never "
            f"rebinds to another PR. Reopen and merge PR "
            f"#{binding.notes_pr_number}, or close the tracker and start "
            f"over", binding,
        )
    return pull


def _resolve_candidate(notes_merge_sha: str, branch_head: str, tracking_issue: Any,
                       gh: Any) -> Candidate:
    """Establish the candidate from the notes merge, head, and adoptions.

    The notes merge commit is the candidate while it remains the branch head.
    After movement, a bot-recorded adoption re-establishes a candidate in
    exactly two shapes: the *exact current* head (adoptions of earlier heads
    are stale and do not count; the branch moved again, so the owner must
    look again), or the pinned notes-merge SHA itself (an explicit
    reconfirmation to ship the pinned candidate despite the movement, which
    stays valid however the branch moves since it re-affirms the pin, not a
    head). The INVALIDATED sha reports the last valid candidate (the latest
    adoption when one exists), so the operator message names what actually
    lapsed.
    """
    if branch_head == notes_merge_sha:
        return Candidate(state=CandidateState.CURRENT, sha=notes_merge_sha,
                         branch_head=branch_head)
    adoptions = issue_mod.adopted_shas(tracking_issue, gh) if tracking_issue is not None else ()
    if branch_head in adoptions:
        return Candidate(state=CandidateState.ADOPTED, sha=branch_head,
                         branch_head=branch_head)
    if notes_merge_sha in adoptions:
        return Candidate(state=CandidateState.ADOPTED, sha=notes_merge_sha,
                         branch_head=branch_head)
    return Candidate(
        state=CandidateState.INVALIDATED,
        sha=adoptions[-1] if adoptions else notes_merge_sha,
        branch_head=branch_head,
    )
