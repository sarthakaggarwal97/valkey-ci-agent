"""Protected publication of the GitHub release (stage 4).

Publication is the release's point of no return: creating the release fires
valkey's ``trigger-build-release`` workflow (``repository_dispatch
build-release {version, environment: prod}``), and upstream's active tag
ruleset forbids moving or deleting 8.x/9.x release tags (note: 7.x tags are
excluded from that ruleset, and forks have no such protection; treat the
tag as immovable everywhere anyway). The controller therefore
never publishes from reconciliation: publication happens only through this module, invoked by a
workflow job gated behind a protected ``release`` environment with required
reviewers.

Everything is revalidated *after* approval, immediately before the write:

- the actor's team membership (live);
- the full release status (candidate valid, required CI green on the exact
  SHA, qualification passed on the exact SHA);
- the version files at the candidate SHA (``src/version.h`` must record
  exactly the version and stage being published);
- the release notes at the candidate SHA (the dated section must exist,
  since it becomes the release body);
- tag availability (the tag must not exist: GitHub silently ignores
  ``target_commitish`` for an existing tag, which is how historical releases
  ended up displaying ``unstable``);
- the latest-release decision (``make_latest`` is always explicit: ``true``
  only when publishing a GA that advances the newest release line).

After the create call the tag is re-resolved and must point at exactly the
candidate SHA, or the failure is reported as critical.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import partial
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import issue as issue_mod
from scripts.release.authorize import ensure_authorized
from scripts.release.models import ReleasePhase
from scripts.release.policy import TRACKER_LABEL, RepoReleasePolicy
from scripts.release.reconcile import ReleaseControlError, compute_status
from scripts.release.release_refs import read_text_file, resolve_tag_commit
from scripts.release_notes.release_format import parse_version
from scripts.release_notes.version_bump import current_release_state

logger = logging.getLogger(__name__)

_NOTES_PATH = "00-RELEASENOTES"
_VERSION_H_PATH = "src/version.h"
_DATED_SECTION_RE = re.compile(r"^Valkey\s+\d+\.\d+\.\d+", re.MULTILINE)


@dataclass(frozen=True)
class PublishPlan:
    """Everything the publish step will do, computed by revalidation.

    Rendered into the workflow job summary by the ``validate`` job so the
    environment approver sees the exact tag, SHA, and latest decision they
    are approving.
    """

    tag: str
    sha: str
    prerelease: bool
    make_latest: str  # GitHub expects the string "true"/"false"
    body: str
    issue_number: int
    tracker_url: str = ""
    qualification_url: str = ""


def plan_publication(
    gh: Any, policy: RepoReleasePolicy, *, branch: str, actor: str,
    gh_downstream: Any = None, skip_authorization: bool = False,
) -> PublishPlan:
    """Run every pre-publication validation and return the exact plan.

    Raises :class:`ReleaseControlError` when any validation fails. Safe to
    call repeatedly; performs no writes.
    """
    # Unattended planning (the controller auto-dispatched this validate run
    # as the Actions bot) is read-only evidence generation: authorization is
    # enforced by the approval gate and re-checked with the approver's
    # identity on the execute path.
    if not skip_authorization:
        ensure_authorized(gh, policy, actor)

    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    tracking_issue = issue_mod.find_release_issue(repo, branch, label=TRACKER_LABEL)
    if tracking_issue is None:
        raise ReleaseControlError(f"no active release on {policy.repo} {branch}")

    status = compute_status(gh, policy, branch, tracking_issue=tracking_issue,
                            gh_downstream=gh_downstream)
    if status.phase is not ReleasePhase.READY:
        raise ReleaseControlError(
            f"release is not ready to publish (phase: {status.phase.value}); "
            f"blockers: {'; '.join(status.blockers) or 'none listed'}"
        )

    sha = status.candidate.sha
    tag = status.version if status.stage == "ga" else f"{status.version}-{status.stage}"

    if resolve_tag_commit(repo, tag):
        raise ReleaseControlError(
            f"tag {tag} already exists on {policy.repo}; creating the release "
            f"would silently attach to it regardless of the candidate SHA"
        )

    # The version files at the exact candidate SHA must record exactly what
    # is being published: a stale or wrong version.h means the notes PR and
    # the branch disagree, and publishing would ship a mislabelled server.
    version_h = read_text_file(repo, _VERSION_H_PATH, ref=sha)
    recorded_version, recorded_stage = current_release_state(version_h)
    if (recorded_version, recorded_stage) != (status.version, status.stage):
        raise ReleaseControlError(
            f"version.h at {sha[:12]} records {recorded_version} ({recorded_stage}) "
            f"but the release publishes {status.version} ({status.stage})"
        )

    notes = read_text_file(repo, _NOTES_PATH, ref=sha)
    body = _extract_release_section(notes, tag)
    if body is None:
        raise ReleaseControlError(
            f"{_NOTES_PATH} at {sha[:12]} has no dated section for Valkey {tag}; "
            f"the release body cannot be constructed"
        )
    body += _changelog_footer(repo, policy, tag, status.version, status.stage)

    return PublishPlan(
        tag=tag,
        sha=sha,
        prerelease=status.stage != "ga",
        make_latest=_make_latest_decision(repo, status.version, status.stage),
        body=body,
        issue_number=tracking_issue.number,
        tracker_url=tracking_issue.html_url,
        qualification_url=status.qualification.url,
    )


def publish_release(gh: Any, policy: RepoReleasePolicy, *, branch: str, actor: str,
                    expected_tag: str = "", expected_sha: str = "",
                    gh_downstream: Any = None) -> str:
    """Revalidate everything and publish the release at the candidate SHA.

    ``expected_tag`` and ``expected_sha``, when set, must equal the freshly
    revalidated plan's: they carry exactly what the environment approver
    saw. The SHA binding matters independently of the tag: if the branch
    moves and a new head is adopted while approval is pending, revalidation
    would produce the same tag over a different commit, and that commit was
    never reviewed. Returns the release URL. Publication fires the
    production build dispatch; there is deliberately no dry-run on this
    path (use :func:`plan_publication`).
    """
    plan = plan_publication(gh, policy, branch=branch, actor=actor,
                            gh_downstream=gh_downstream)
    if expected_tag and expected_tag != plan.tag:
        raise ReleaseControlError(
            f"revalidation produced tag {plan.tag} but approval was for "
            f"{expected_tag}; the release changed between approval and "
            f"execution: re-run validation"
        )
    if expected_sha and expected_sha != plan.sha:
        raise ReleaseControlError(
            f"revalidation produced candidate {plan.sha} but approval was for "
            f"{expected_sha}; the candidate changed between approval and "
            f"execution: re-run validation and approve the new commit"
        )

    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    release = retry_github_call(
        lambda: repo.create_git_release(
            plan.tag,
            name=plan.tag,
            message=plan.body,
            draft=False,
            prerelease=plan.prerelease,
            target_commitish=plan.sha,
            make_latest=plan.make_latest,
        ),
        retries=2, description=f"create release {plan.tag}",
    )
    logger.info("Published release %s: %s", plan.tag, release.html_url)

    # The create call silently reuses a pre-existing tag; re-resolve and
    # assert the tag points at exactly the approved SHA.
    tag_sha = resolve_tag_commit(repo, plan.tag)
    if tag_sha != plan.sha:
        raise ReleaseControlError(
            f"CRITICAL: release {plan.tag} was created but its tag points at "
            f"{tag_sha or '<unresolvable>'}, not the approved candidate "
            f"{plan.sha}. The tag ruleset forbids moving it; investigate "
            f"immediately before any downstream work proceeds."
        )

    tracking_issue = retry_github_call(
        lambda: repo.get_issue(plan.issue_number),
        retries=2, description=f"get issue #{plan.issue_number}",
    )
    retry_github_call(
        lambda: tracking_issue.create_comment(
            body=(
                f"Published **{plan.tag}** at `{plan.sha}` "
                f"(publication approved by @{actor}): {release.html_url}\n"
                f"Downstream outputs are now observed by reconciliation."
            )
        ),
        retries=2, description="record publication on tracker",
    )
    return release.html_url


def ensure_environment_protected(gh: Any, policy: RepoReleasePolicy,
                                 agent_repo: str, environment: str = "release") -> None:
    """Fail closed unless the publish gate is actually protected.

    Naming an environment guarantees nothing: GitHub auto-creates it with no
    protection rules and admin bypass enabled the first time a workflow
    references it, and the job then runs seconds after validation with no
    human in the loop. Publication therefore refuses unless the environment
    has at least one required reviewer, prevents self-review, and disallows
    admin bypass.

    Fork policies (``user:<login>`` authorization) skip this check: their
    approval model *is* the single authorized user, and requiring reviewers
    there would only force self-approval theater.
    """
    if policy.authorized_team.startswith("user:"):
        logger.info("Fork policy: skipping environment protection verification")
        return
    repo = retry_github_call(
        lambda: gh.get_repo(agent_repo),
        retries=2, description=f"get repo {agent_repo}",
    )
    try:
        env = retry_github_call(
            lambda: repo.get_environment(environment),
            retries=2, description=f"get environment {environment}",
        )
    except GithubException as exc:
        if exc.status == 404:
            raise ReleaseControlError(
                f"the {environment!r} environment does not exist on {agent_repo}; "
                f"create it with required reviewers before publishing"
            ) from exc
        raise
    payload = env.raw_data
    reviewer_rules = [
        rule for rule in payload.get("protection_rules", [])
        if rule.get("type") == "required_reviewers" and rule.get("reviewers")
    ]
    problems = []
    if not reviewer_rules:
        problems.append("no required reviewers are configured")
    elif not all(rule.get("prevent_self_review") for rule in reviewer_rules):
        problems.append("self-review is not prevented")
    if payload.get("can_admins_bypass", True):
        problems.append("administrators can bypass the protection rules")
    if problems:
        raise ReleaseControlError(
            f"the {environment!r} environment on {agent_repo} is not an "
            f"effective gate: {'; '.join(problems)}. Fix the environment "
            f"settings; publication refuses until approval is enforced."
        )


def render_plan_summary(plan: PublishPlan) -> str:
    """The approver's checklist: what will happen and what to verify.

    GitHub's approval modal shows nothing but the environment name, so this
    summary, rendered on the run page and posted to the tracker, is the
    approval evidence. It says explicitly what to check, with links.
    """
    return "\n".join([
        "## Awaiting approval: publish " + plan.tag,
        "",
        "> [!IMPORTANT]",
        "> Approving publishes the release, fires the production builds, and",
        "> creates a tag that cannot be moved or deleted. Verify the items",
        "> below, then approve the **Publish** job under Review deployments.",
        "",
        "**Verify before approving:**",
        "",
        f"- [ ] The tag is the release you expect: `{plan.tag}`",
        f"- [ ] The commit matches the tracker's candidate SHA: `{plan.sha}`"
        + (f" ([tracker]({plan.tracker_url}))" if plan.tracker_url else ""),
        "- [ ] Qualification evidence looks right"
        + (f" ([qualification run]({plan.qualification_url}))"
           if plan.qualification_url else ""),
        f"- [ ] The latest-release decision is correct: `make_latest={plan.make_latest}`"
        + (" (this release becomes the repo's latest)" if plan.make_latest == "true"
           else " (an older line: the latest pointer does not move)"),
        f"- [ ] The release notes below read correctly (prerelease: `{plan.prerelease}`)",
        "",
        "Execution re-runs every validation and refuses if the tag or commit",
        "differ from the values above.",
        "",
        "---",
        "",
        plan.body,
    ])


def _make_latest_decision(repo: Any, version: str, stage: str) -> str:
    """Explicit ``make_latest``: never rely on GitHub's default (true).

    ``true`` only for a GA that advances (or first establishes) the newest
    release line; an older line's patch (e.g. 8.0.x while 9.1.x is current)
    must never steal the latest pointer.
    """
    if stage != "ga":
        return "false"
    try:
        latest = retry_github_call(
            repo.get_latest_release,
            retries=2, description="get latest release",
        )
    except GithubException as exc:
        if exc.status == 404:  # no releases yet
            return "true"
        raise
    try:
        latest_version = parse_version(latest.tag_name)
    except ValueError:
        return "true"  # non-version latest tag: this GA takes over
    return "true" if parse_version(version) > latest_version else "false"


def _extract_release_section(notes: str, tag: str) -> str | None:
    """The dated section for *tag* from 00-RELEASENOTES, verbatim."""
    heading = re.compile(rf"^Valkey\s+{re.escape(tag)}\s", re.MULTILINE)
    match = heading.search(notes)
    if match is None:
        return None
    next_section = _DATED_SECTION_RE.search(notes, match.end())
    end = next_section.start() if next_section else len(notes)
    return notes[match.start():end].rstrip() + "\n"


def _changelog_footer(repo: Any, policy: RepoReleasePolicy, tag: str,
                      version: str, stage: str) -> str:
    """The ``Full Changelog`` compare link, against the previous tag when
    one can be determined; omitted otherwise rather than guessed."""
    previous = _previous_tag(repo, version, stage)
    if previous is None:
        return ""
    return (
        f"\n**Full Changelog**: "
        f"https://github.com/{policy.repo}/compare/{previous}...{tag}\n"
    )


def _previous_tag(repo: Any, version: str, stage: str) -> str | None:
    """The release tag this one follows: previous rc, last rc, or previous patch."""
    major, minor, patch = parse_version(version)
    tags = {
        t.name for t in retry_github_call(
            lambda: list(repo.get_tags()),
            retries=2, description="list tags",
        )
    }
    rc_match = re.fullmatch(r"rc([1-9]\d*)", stage)
    if rc_match:
        rc_number = int(rc_match.group(1))
        if rc_number > 1 and f"{version}-rc{rc_number - 1}" in tags:
            return f"{version}-rc{rc_number - 1}"
        return None  # rc1: the previous release is on another line; do not guess
    if patch > 0 and f"{major}.{minor}.{patch - 1}" in tags:
        return f"{major}.{minor}.{patch - 1}"
    # x.y.0 GA follows its last rc when one exists.
    rc_numbers = [
        int(m.group(1)) for m in (
            re.fullmatch(rf"{re.escape(version)}-rc([1-9]\d*)", t) for t in tags
        ) if m
    ]
    if rc_numbers:
        return f"{version}-rc{max(rc_numbers)}"
    return None




_APPROVAL_MARKER = f"<!-- {issue_mod.MARKER_NAMESPACE}:approval-evidence -->"


def post_approval_evidence(gh: Any, policy: RepoReleasePolicy,
                           plan: PublishPlan, run_url: str) -> None:
    """Put the approver's checklist on the tracker, linking the waiting run.

    The approver's journey usually starts from the tracker, and GitHub's
    approval modal shows no context, so the evidence lives where they are,
    with a pointer to where the button is. Edited in place on re-validation.
    """
    repo = retry_github_call(
        lambda: gh.get_repo(policy.repo),
        retries=2, description=f"get repo {policy.repo}",
    )
    tracking_issue = retry_github_call(
        lambda: repo.get_issue(plan.issue_number),
        retries=2, description=f"get issue #{plan.issue_number}",
    )
    # The mention notifies on first creation only: subsequent re-validations
    # edit the comment in place, which GitHub does not re-notify; one ping
    # per approval wait is the desired behavior.
    body = (
        f"{_APPROVAL_MARKER}\n"
        f"> [!IMPORTANT]\n"
        f"> **{policy.mention}, approval needed to publish `{plan.tag}`.**\n"
        f"\n"
        + render_plan_summary(plan)
        + f"\n\n**Approve here:** {run_url} (Review deployments -> `release` "
        f"-> Approve and deploy)"
    )
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        if _APPROVAL_MARKER in (comment.body or ""):
            retry_github_call(
                partial(comment.edit, body=body),
                retries=2, description="update approval evidence comment",
            )
            return
    retry_github_call(
        lambda: tracking_issue.create_comment(body=body),
        retries=2, description="post approval evidence comment",
    )
