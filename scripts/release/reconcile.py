"""Recompute release truth from GitHub and reconcile the tracking issue.

The controller's single source of truth is live GitHub state:

- The **release decision** (version + stage) is pinned by the release-notes
  PR, the bot-created artifact whose head branch is
  ``agent/release-cut/<version>-<stage>`` into the release branch.
- The **candidate SHA** is that PR's merge commit, but only while it remains
  the branch head. If the branch moves, the candidate is invalidated until an
  authorized owner adopts the *exact* new head via the ``adopt`` entry point.
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
# a notes PR for an active release is by construction recent.
_NOTES_PR_SCAN_LIMIT = 200


class ReleaseControlError(Exception):
    """A release action is impossible or refused; the message says why."""


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
        return _reuse_active_release(repo, policy, branch, intent, existing)

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
    return StartResult(
        created=True,
        cut_needed=True,
        issue_number=created.number,
        issue_url=created.html_url,
        version=derived.version,
        stage=derived.stage,
        tag=derived.tag,
    )


def _reuse_active_release(
    repo: Any, policy: RepoReleasePolicy, branch: str,
    intent: ReleaseIntent, existing: Any,
) -> StartResult:
    """Resolve a duplicate start against the release already active on *branch*."""
    logger.info(
        "Active release already tracked in issue #%s; reusing it "
        "(one active release per branch)", existing.number,
    )
    notes_pr = _find_notes_pr(repo, policy, branch, created_after=existing.created_at)
    if notes_pr is None:
        # The cut failed or never ran. Re-deriving is safe (derivation is
        # pure and the release has produced nothing to disagree with) and
        # lets the workflow rerun the cut without a hand-typed version.
        derived = derive_version(branch, intent, _tag_names(repo, policy))
        logger.info("No notes PR bound to issue #%s; requesting a (re)cut of %s",
                    existing.number, derived.tag)
        return StartResult(
            created=False, cut_needed=True,
            issue_number=existing.number, issue_url=existing.html_url,
            version=derived.version, stage=derived.stage, tag=derived.tag,
        )

    match = NOTES_PREP_BRANCH_RE.match(notes_pr.head.ref)
    assert match is not None  # _find_notes_pr only returns matching PRs
    pinned = DerivedRelease(version=match.group(1), stage=match.group(2))
    if pinned.tag in set(_tag_names(repo, policy)):
        raise ReleaseControlError(
            f"tag {pinned.tag} already exists; close tracking issue "
            f"#{existing.number} to start the next release on {branch}"
        )
    return StartResult(
        created=False, cut_needed=False,
        issue_number=existing.number, issue_url=existing.html_url,
        version=pinned.version, stage=pinned.stage, tag=pinned.tag,
    )


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
    notes_pr = _find_notes_pr(repo, policy, branch, created_after=created_after)
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
            notes_pr=notes_pr,
        )

    notes_merge_sha = notes_pr.merge_commit_sha or ""
    blockers: list[str] = []
    alerts: list[str] = []
    checks: tuple[RequiredCheck, ...] = ()
    qualification = QualificationStatus()
    phase = ReleasePhase.CANDIDATE

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
        phase=phase,
        ready=not blockers,
        blockers=tuple(blockers),
        alerts=tuple(alerts),
    )


def _published_status(
    gh: Any, gh_downstream: Any, repo: Any, policy: RepoReleasePolicy, *,
    branch: str, version: str, stage: str, tag: str, release: Any,
    branch_head: str, notes_pr: Any,
) -> ReleaseStatus:
    """Status once the release exists: verify the tag pins the release and
    observe every downstream public output through completion."""
    tag_sha = resolve_tag_commit(repo, tag)
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
    }
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

    The body's freshness footer has minute resolution, so a pass in a new
    minute edits the body even when nothing else changed. Accepted churn:
    the timestamp is the tracker's staleness signal; passes within the same
    minute that change nothing still skip. The title is edited only when it
    differs, and keeps the start-time title until a notes PR pins a
    version, so it never flaps between derived and recomputed forms.
    """
    body = issue_mod.render_body(status, datetime.now(timezone.utc))
    title = (
        issue_mod.render_live_title(status)
        if status.version else tracking_issue.title
    )
    if tracking_issue.body == body and tracking_issue.title == title:
        logger.info("Issue #%s already reflects current state", tracking_issue.number)
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

    if act:
        for performed in actions_mod.advance(
            gh_downstream, policy, status=status, tracking_issue=tracking_issue,
            gh_agent=gh_agent, agent_repo=agent_repo,
        ):
            logger.info("Action: %s", performed)

    # Display-only: link the waiting publish run so the READY callout can
    # say exactly where to approve. Fetched after advance() so a run the
    # dispatch just created has its best chance of being visible.
    if status.phase is ReleasePhase.READY and gh_agent is not None and agent_repo:
        approval_url = actions_mod.waiting_publish_run_url(
            gh_agent, agent_repo, branch,
        )
        if approval_url:
            status = replace(status, approval_run_url=approval_url)

    _sync_phase_labels(repo, tracking_issue, status)
    _render_tracker(tracking_issue, status)
    logger.info("Reconciled issue #%s (phase=%s)", tracking_issue.number, status.phase.value)
    return status


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
    if sha != status.candidate.branch_head:
        raise ReleaseControlError(
            f"adoption requires the exact current head of {branch} "
            f"({status.candidate.branch_head}); got {sha or '<empty>'!r}. "
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
) -> Any:
    """The release-notes PR bound to the active release on *branch*, or None.

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
        return pull
    return None


def _resolve_candidate(notes_merge_sha: str, branch_head: str, tracking_issue: Any,
                       gh: Any) -> Candidate:
    """Establish the candidate from the notes merge, head, and adoptions.

    The notes merge commit is the candidate while it remains the branch head.
    After movement, only a bot-recorded adoption of the *exact current* head
    re-establishes a candidate; adoptions of earlier heads are stale and do
    not count (the branch moved again, so the owner must look again). The
    INVALIDATED sha reports the last valid candidate (the latest adoption
    when one exists), so the operator message names what actually lapsed.
    """
    if branch_head == notes_merge_sha:
        return Candidate(state=CandidateState.CURRENT, sha=notes_merge_sha,
                         branch_head=branch_head)
    adoptions = issue_mod.adopted_shas(tracking_issue, gh) if tracking_issue is not None else ()
    if branch_head in adoptions:
        return Candidate(state=CandidateState.ADOPTED, sha=branch_head,
                         branch_head=branch_head)
    return Candidate(
        state=CandidateState.INVALIDATED,
        sha=adoptions[-1] if adoptions else notes_merge_sha,
        branch_head=branch_head,
    )
