"""Idempotent progress actions taken during reconciliation.

Reconciliation recomputes state, then performs at most the small set of
side effects that state calls for, each guarded so a rerun never repeats
completed work:

- dispatch the qualification run (only when CI is green on the candidate
  and no qualification run exists for that exact SHA);
- dispatch the Bundle update (only when the verifier says the ordering gate
  is satisfied and no update is in flight, signalled via ``action``);
- open the Helm chart bump PR (same gating, controller-authored since
  valkey-helm has no automation of its own);
- notify the authorized team exactly once per distinct failure state
  (keyed on a fingerprint marker in a bot comment; the same failure never
  notifies twice, a *new* failure does);
- nudge the authorized team exactly once per distinct awaiting-human state
  (an open notes PR, a moved branch) with the same fingerprint pattern;
- auto-dispatch build-release once per candidate when the valkey-side
  release trigger failed before dispatching the build (marker-gated, never
  loops; the failure notification still stands until the build verifies);
- retry a failed qualification run exactly once per candidate (same
  marker gate; a second failure waits for a human);
- close the tracking issue when every required public output is verified.

Publication is deliberately absent: it only happens through the protected
publish workflow.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import issue as issue_mod
from scripts.release import qualification as qual_mod
from scripts.release import verify as verify_mod
from scripts.release.models import (
    CandidateState,
    CheckState,
    OutputState,
    QualificationStatus,
    ReleasePhase,
    ReleaseStatus,
    release_tag,
)
from scripts.release.policy import RepoReleasePolicy
from scripts.release.release_refs import workflow_handle
from scripts.release_notes.release_format import parse_version

logger = logging.getLogger(__name__)

_CHART_PATH = "valkey/Chart.yaml"
_CHART_README_PATH = "valkey/README.md"


def advance(
    gh: Any, policy: RepoReleasePolicy, *,
    status: ReleaseStatus, tracking_issue: Any,
    gh_agent: Any = None, agent_repo: str = "",
    agent_head_sha: str = "",
) -> list[str]:
    """Perform the actions the recomputed *status* calls for; returns a
    log of what was done (empty when the state needs nothing).

    ``agent_head_sha`` is the agent repo's default-branch head (resolved
    once per pass by the caller): a publish run parked at the approval
    gate on any other commit is stale and gets cancelled rather than
    blocking a fresh dispatch. "" disables staleness detection.
    """
    performed: list[str] = []

    if status.phase is ReleasePhase.QUALIFICATION:
        if status.qualification == QualificationStatus():
            tag = release_tag(status.version, status.stage)
            qual_mod.dispatch_qualification(
                gh, policy, tag=tag, sha=status.candidate.sha,
            )
            performed.append(f"dispatched qualification of {tag} @ {status.candidate.sha[:12]}")
        elif (
            status.qualification.run_id
            and not status.qualification.pending
            and not status.qualification.passed
        ):
            retried = _retry_qualification_once(gh, policy, status, tracking_issue)
            if retried:
                performed.append(retried)

    if (
        status.phase is ReleasePhase.READY
        and gh_agent is not None and agent_repo
        and not _publish_run_active(gh_agent, agent_repo, status.branch,
                                    agent_head_sha)
    ):
        # Minimum-clicks: READY auto-starts the publish pipeline. This is
        # safe to automate because the dispatch was never the gate: the
        # validate job posts the approval evidence and the publish job holds
        # at the protected environment until a human approves.
        _dispatch_publish(gh_agent, agent_repo, status.branch)
        performed.append(f"dispatched the publish pipeline for {status.branch} "
                         f"(holds at the approval gate)")

    for output in status.outputs:
        if output.action == "dispatch-bundle":
            tag = release_tag(status.version, status.stage)
            _dispatch_bundle(gh, policy, tag)
            performed.append(f"dispatched bundle update for {tag}")
        elif output.action == "open-helm-pr":
            url = _open_helm_pr(gh, policy, status.version)
            performed.append(f"opened helm chart bump PR: {url}")
        elif output.action == "dispatch-build-release":
            dispatched = _dispatch_build_once(gh, policy, status, tracking_issue, output)
            if dispatched:
                performed.append(dispatched)

    note = _notify_once(gh, policy, status, tracking_issue)
    if note:
        performed.append(note)

    nudge = _nudge_once(gh, policy, status, tracking_issue)
    if nudge:
        performed.append(nudge)

    # Alerts block completion: a standing alert (an untrusted tag, broken
    # release metadata) must keep the tracker open for a human even if the
    # phase machine were ever to report COMPLETE alongside one.
    if status.phase is ReleasePhase.COMPLETE and not status.alerts:
        closed = _close_when_complete(gh, status, tracking_issue)
        if closed:
            performed.append(f"closed tracking issue #{tracking_issue.number}")

    return performed


def _dispatch_bundle(gh: Any, policy: RepoReleasePolicy, tag: str) -> None:
    """Fire the Bundle repo's own update hook (repository_dispatch).

    The payload carries the tag, mirroring the upstream trigger (which
    sends the release's tag_name as its version).
    """
    repo = retry_github_call(
        lambda: gh.get_repo(policy.downstream.bundle_repo),
        retries=2, description=f"get repo {policy.downstream.bundle_repo}",
    )
    retry_github_call(
        lambda: repo.create_repository_dispatch(
            event_type="valkey-release",
            client_payload={"version": tag, "component": "valkey"},
        ),
        retries=2, description="dispatch bundle update",
    )
    logger.info("Dispatched bundle update for valkey %s", tag)


def _autofix_once(gh: Any, tracking_issue: Any, *, key: str,
                  fingerprint_source: str, callout: str) -> bool:
    """Post the auto-remediation marker comment once per (key, fingerprint).

    Same trusted-marker pattern as the nudges: the marker is stamped into a
    bot comment, and while one exists for this fingerprint no further
    remediation fires, so the autofix can never loop. Returns False when the
    marker already exists (remediation was already attempted).

    The marker posts *before* the remediation runs: a remediation that
    dispatches but fails to record itself would retry forever, while a
    marker without a dispatch just leaves the normal failure notification
    standing. Fail closed.
    """
    fingerprint = hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()[:12]
    marker = f"<!-- {issue_mod.MARKER_NAMESPACE}:autofix:{key}:{fingerprint} -->"
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        if issue_mod.marker_present(comment.body, marker):
            return False
    retry_github_call(
        lambda: tracking_issue.create_comment(body=f"{marker}\n{callout}"),
        retries=2, description=f"post {key} auto-remediation comment",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)
    return True


def _post_dispatch_failure(gh: Any, tracking_issue: Any, *, key: str,
                           instruction: str) -> None:
    """Post the plain follow-up comment when an auto-remediation's dispatch
    itself failed.

    No autofix marker: the marker already posted (marker-first, fail
    closed), so the autofix never re-fires for this candidate; this comment
    only tells the human the tracker's 'Dispatching' callout did not land.
    """
    retry_github_call(
        lambda: tracking_issue.create_comment(
            body=(
                f"> [!WARNING]\n"
                f"> **Auto-remediation failed:** The dispatch itself failed. "
                f"{instruction}"
            )
        ),
        retries=2, description=f"post {key} dispatch-failure comment",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)


def _dispatch_build_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus,
    tracking_issue: Any, output: Any,
) -> str:
    """Dispatch build-release directly, once per candidate, when the
    valkey-side release trigger failed before reaching the automation repo.

    The fingerprint is the candidate SHA: strictly one auto-dispatch per
    candidate, even across distinct failed trigger runs. When the marker
    already exists nothing happens (the normal failure notification covers
    the state).
    """
    tag = release_tag(status.version, status.stage)
    posted = _autofix_once(
        gh, tracking_issue, key="build-dispatch",
        fingerprint_source=status.candidate.sha,
        callout=(
            f"> [!NOTE]\n"
            f"> **Auto-remediation:** Dispatching the build pipeline for "
            f"`{tag}` directly (the [release trigger run]({output.url}) "
            f"did not succeed)."
        ),
    )
    if not posted:
        return ""
    # The dispatch must not escape advance(): an exception here would skip
    # the notify/nudge/render steps and leave the tracker's 'Dispatching'
    # callout as a false last word. The marker stays (once per candidate);
    # the follow-up comment tells the human to dispatch manually.
    try:
        _dispatch_build_release(gh, policy, tag)
    except Exception:
        logger.exception("Auto-dispatch of build-release for %s failed", tag)
        _post_dispatch_failure(
            gh, tracking_issue, key="build-dispatch",
            instruction=f"Dispatch build-release for `{tag}` manually.",
        )
        return ""
    logger.info("Auto-dispatched build-release for %s", tag)
    return f"auto-dispatched build-release for {tag} (release trigger failed)"


def _dispatch_build_release(gh: Any, policy: RepoReleasePolicy, tag: str) -> None:
    """Fire the automation repo's build workflow for *tag* in prod.

    The inputs mirror the upstream release trigger exactly (it sends the
    release's tag_name as its version), so the resulting run carries the
    ``Build Release <tag> (prod)`` run-name the build-run verifier matches.
    """
    down = policy.downstream
    workflow = workflow_handle(gh, down.automation_repo, down.build_workflow)
    if workflow is None:
        raise RuntimeError(
            f"{down.build_workflow} does not exist on {down.automation_repo}"
        )
    repo = retry_github_call(
        lambda: gh.get_repo(down.automation_repo),
        retries=2, description=f"get repo {down.automation_repo}",
    )
    # Accepted non-idempotency window: retry_github_call around
    # create_dispatch can rarely double-dispatch when a success response is
    # lost; the verifier takes the newest run so verification stays correct.
    dispatched = retry_github_call(
        lambda: workflow.create_dispatch(
            repo.default_branch, inputs={"version": tag, "environment": "prod"},
        ),
        retries=2, description="dispatch build-release run",
    )
    if not dispatched:
        raise RuntimeError(
            f"build-release dispatch was rejected by "
            f"{down.automation_repo}/{down.build_workflow}"
        )


def _retry_qualification_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
) -> str:
    """Re-dispatch qualification exactly once per candidate after a failure.

    The deliberate never-redispatch-over-a-failed-run stance is softened by
    exactly one step: one automatic retry per candidate SHA. A second
    failure changes nothing further; the normal failure notification
    stands and a human decides.
    """
    tag = release_tag(status.version, status.stage)
    run_link = f"[run {status.qualification.run_id}]({status.qualification.url})"
    posted = _autofix_once(
        gh, tracking_issue, key="qual-retry",
        fingerprint_source=status.candidate.sha,
        callout=(
            f"> [!NOTE]\n"
            f"> **Auto-remediation:** Retrying qualification for `{tag}` "
            f"once (the previous run failed: {run_link})."
        ),
    )
    if not posted:
        return ""
    # Same containment as the build auto-dispatch: a dispatch failure must
    # not escape advance() and skip the notify/nudge/render steps.
    try:
        qual_mod.dispatch_qualification(gh, policy, tag=tag, sha=status.candidate.sha)
    except Exception:
        logger.exception("Auto-retry qualification dispatch for %s failed", tag)
        _post_dispatch_failure(
            gh, tracking_issue, key="qual-retry",
            instruction=f"Dispatch the qualification workflow for `{tag}` "
                        f"manually.",
        )
        return ""
    logger.info("Auto-retried qualification of %s @ %s", tag, status.candidate.sha[:12])
    return f"auto-retried qualification of {tag} @ {status.candidate.sha[:12]}"


def _open_helm_pr(gh: Any, policy: RepoReleasePolicy, version: str) -> str:
    """Open the chart bump PR: appVersion -> version, chart patch +1,
    README badge line updated. Idempotent via the fixed head branch (the
    verifier only requests this action when no such PR is open)."""
    down = policy.downstream
    repo = retry_github_call(
        lambda: gh.get_repo(down.helm_repo),
        retries=2, description=f"get repo {down.helm_repo}",
    )
    default_branch = repo.default_branch
    head_sha = retry_github_call(
        lambda: repo.get_branch(default_branch).commit.sha,
        retries=2, description=f"resolve {down.helm_repo} {default_branch} head",
    )

    chart_file = retry_github_call(
        lambda: repo.get_contents(_CHART_PATH, ref=head_sha),
        retries=2, description=f"read {_CHART_PATH}",
    )
    chart_text = chart_file.decoded_content.decode("utf-8")
    new_chart, old_chart_version, new_chart_version = _bump_chart(chart_text, version)

    readme_file = retry_github_call(
        lambda: repo.get_contents(_CHART_README_PATH, ref=head_sha),
        retries=2, description=f"read {_CHART_README_PATH}",
    )
    readme_text = readme_file.decoded_content.decode("utf-8")
    new_readme = _bump_readme_badges(readme_text, new_chart_version, version)

    branch = verify_mod.helm_update_branch(version)
    ref = f"refs/heads/{branch}"
    try:
        retry_github_call(
            lambda: repo.create_git_ref(ref=ref, sha=head_sha),
            retries=2, description=f"create branch {branch}",
        )
    except GithubException as exc:
        if exc.status != 422:  # already exists (from an interrupted earlier run)
            raise
        retry_github_call(
            lambda: repo.get_git_ref(f"heads/{branch}").edit(sha=head_sha, force=True),
            retries=2, description=f"reset branch {branch}",
        )

    retry_github_call(
        lambda: repo.update_file(
            _CHART_PATH,
            f"Update valkey chart to {new_chart_version} (appVersion {version})",
            new_chart, chart_file.sha, branch=branch,
        ),
        retries=2, description=f"update {_CHART_PATH}",
    )
    readme_on_branch = retry_github_call(
        lambda: repo.get_contents(_CHART_README_PATH, ref=branch),
        retries=2, description=f"re-read {_CHART_README_PATH} on {branch}",
    )
    retry_github_call(
        lambda: repo.update_file(
            _CHART_README_PATH,
            f"Update chart version badges for {new_chart_version}",
            new_readme, readme_on_branch.sha, branch=branch,
        ),
        retries=2, description=f"update {_CHART_README_PATH}",
    )

    pr = retry_github_call(
        lambda: repo.create_pull(
            title=f"Update valkey chart to appVersion {version}",
            body=(
                f"Automated chart bump for the Valkey {version} release: "
                f"`appVersion` {version}, chart `version` "
                f"{old_chart_version} -> {new_chart_version}.\n\n"
                f"Opened by the release controller after verifying "
                f"`docker.io/{down.dockerhub_repo}:{version}` is public."
            ),
            head=branch,
            base=default_branch,
        ),
        retries=2, description="open helm bump PR",
    )
    logger.info("Opened helm chart bump PR #%s", pr.number)
    return pr.html_url


def _bump_chart(chart_text: str, version: str) -> tuple[str, str, str]:
    """Return (new Chart.yaml, old chart version, new chart version)."""
    chart_match = verify_mod.CHART_VERSION_RE.search(chart_text)
    app_match = verify_mod.CHART_APP_VERSION_RE.search(chart_text)
    if chart_match is None or app_match is None:
        raise RuntimeError("cannot parse valkey/Chart.yaml version fields")
    old_chart_version = chart_match.group(1)
    major, minor, patch = parse_version(old_chart_version)
    new_chart_version = f"{major}.{minor}.{patch + 1}"
    text = verify_mod.CHART_VERSION_RE.sub(f"version: {new_chart_version}", chart_text, count=1)
    text = verify_mod.CHART_APP_VERSION_RE.sub(f'appVersion: "{version}"', text, count=1)
    return text, old_chart_version, new_chart_version


def _bump_readme_badges(readme: str, chart_version: str, app_version: str) -> str:
    """Update the shields badge line (``![Version: X]`` / ``![AppVersion: Y]``)."""
    readme = re.sub(r"!\[Version: [^\]]*\]", f"![Version: {chart_version}]", readme, count=1)
    # (?<!App): the plain Version pattern must never rewrite the AppVersion
    # badge, regardless of badge order in the README.
    readme = re.sub(r"(?<!App)Version-[0-9.]+-informational", f"Version-{chart_version}-informational", readme, count=1)
    readme = re.sub(r"!\[AppVersion: [^\]]*\]", f"![AppVersion: {app_version}]", readme, count=1)
    return re.sub(r"AppVersion-[0-9.]+-informational", f"AppVersion-{app_version}-informational", readme, count=1)


def _notify_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
) -> str:
    """Mention the authorized team once per distinct failure state.

    The failure fingerprint is stamped into the notification comment; while
    the observed failure set is unchanged no further comment is posted, and
    a different failure set notifies again, exactly once.
    """
    failures = _failure_items(status)
    if not failures:
        return ""
    # Fingerprint over the stable keys, not the rendered prose: a wording
    # tweak in a detail string must never re-ping the team, while a NEW
    # failure (a new failed run id, a new failing check) must.
    fingerprint = hashlib.sha256(
        "\n".join(sorted(key for key, _ in failures)).encode("utf-8")
    ).hexdigest()[:12]
    marker = f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:{fingerprint} -->"
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        if issue_mod.marker_present(comment.body, marker):
            return ""
    tag = release_tag(status.version, status.stage)
    rows = "\n".join(
        f"| {index} | {_problem_cell(text)} |"
        for index, (_, text) in enumerate(failures, start=1)
    )
    retry_github_call(
        lambda: tracking_issue.create_comment(
            body=(
                f"{marker}\n"
                f"> [!WARNING]\n"
                f"> **{policy.mention}: Release `{tag}` Needs Attention.**\n"
                f"\n"
                f"| # | Problem |\n"
                f"|---|---|\n"
                f"{rows}\n"
                f"\n"
                f"<sub>This notification repeats only if the failure state "
                f"changes.</sub>"
            )
        ),
        retries=2, description="post failure notification",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)
    logger.info("Notified %s of %d failure(s)", policy.authorized_team, len(failures))
    return f"notified {policy.authorized_team} ({len(failures)} failure(s))"


def _problem_cell(item: str) -> str:
    """One failure item as a table cell: 'name: detail' items get the name
    bolded; anything else renders verbatim."""
    name, sep, detail = item.partition(": ")
    if sep and detail:
        return f"**{name}:** {detail}"
    return item


def _nudge_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
) -> str:
    """Mention the authorized team once per distinct awaiting-human state.

    Failures already escalate through :func:`_notify_once`; this covers the
    two states that otherwise wait silently on a human: an open notes PR
    and a moved branch. Same fingerprint-marker pattern: an unchanged state
    never re-pings, a changed one (new notes PR, new branch head) does.
    """
    nudge = _nudge_item(status)
    if nudge is None:
        return ""
    key, message = nudge
    fingerprint = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    marker = f"<!-- {issue_mod.MARKER_NAMESPACE}:nudge:{fingerprint} -->"
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        if issue_mod.marker_present(comment.body, marker):
            return ""
    tag = release_tag(status.version, status.stage)
    retry_github_call(
        lambda: tracking_issue.create_comment(
            body=(
                f"{marker}\n"
                f"> [!IMPORTANT]\n"
                f"> **{policy.mention}: Action Needed for `{tag}`.**\n"
                f">\n"
                f"> {message}\n"
                f"\n"
                f"<sub>One-time nudge: posts again only if the state changes.</sub>"
            )
        ),
        retries=2, description="post action-needed nudge",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)
    logger.info("Nudged %s (%s)", policy.authorized_team, key)
    return f"nudged {policy.authorized_team} ({key})"


def _nudge_item(status: ReleaseStatus) -> "tuple[str, str] | None":
    """(fingerprint key, message) for the awaiting-human state, or None.

    The awaiting-publish-approval state is deliberately absent: the approval
    evidence comment posted by the publish pipeline carries the mention.
    """
    if status.published:
        return None
    if status.notes_pr_number and not status.notes_pr_merged:
        tag = release_tag(status.version, status.stage)
        return (
            f"notes-pr:{status.notes_pr_number}",
            f"Review and merge the release-notes PR {status.notes_pr_url} "
            f"to proceed with `{tag}`.",
        )
    if status.candidate.state is CandidateState.INVALIDATED:
        head = status.candidate.branch_head
        return (
            f"branch-moved:{head}",
            f"Branch `{status.branch}` moved to `{head[:12]}` after the "
            f"candidate was established. Adopt the new head "
            f"(Actions → release-adopt) or ship the pinned candidate.",
        )
    return None


def _failure_items(status: ReleaseStatus) -> "list[tuple[str, str]]":
    """(stable key, rendered text) per failure.

    Keys are identifiers, not prose: they feed the notification fingerprint,
    so rewording a detail never re-pings, while a new failed run id or a
    newly failing check does. Texts render into the comment body.
    """
    # Failure states rendered as verb phrases, not raw enum values.
    check_phrases = {
        CheckState.FAILED: "failed",
        CheckState.STALLED: "has stalled",
    }
    items: "list[tuple[str, str]]" = [(alert, alert) for alert in status.alerts]
    # Check keys carry the candidate SHA: unlike qualification and output
    # failures, whose keys carry run ids and so re-ping on a new run, a
    # check has no run id, and without the SHA the same check failing on a
    # NEW candidate (after branch movement and adoption) would be
    # suppressed forever by the old marker.
    items += [
        (f"check:{status.candidate.sha}:{check.name}:{check.state.value}",
         f"Required check `{check.name}` {check_phrases[check.state]}")
        for check in status.checks
        if check.state in (CheckState.FAILED, CheckState.STALLED)
    ]
    if status.qualification.failed_jobs:
        run_id = status.qualification.run_id
        items.append((
            f"qual:{run_id}",
            f"Qualification run {run_id} failed: "
            + ", ".join(status.qualification.failed_jobs[:5]),
        ))
    # run_id may be 0/empty; included anyway so a NEW failed run (with a
    # real id) re-pings exactly once.
    items.extend(
        (f"output:{output.name}:{output.run_id}",
         f"{output.name}: {output.detail}")
        for output in status.outputs
        if output.state is OutputState.FAILED
    )
    return items


def _close_when_complete(gh: Any, status: ReleaseStatus, tracking_issue: Any) -> bool:
    if tracking_issue.state == "closed":
        return False
    marker = f"<!-- {issue_mod.MARKER_NAMESPACE}:complete -->"
    already_commented = any(
        issue_mod.marker_present(comment.body, marker)
        for comment in issue_mod.trusted_comments(tracking_issue, gh)
    )
    if not already_commented:
        retry_github_call(
            lambda: tracking_issue.create_comment(
                body=(
                    f"{marker}\n"
                    f"> [!NOTE]\n"
                    f"> **Release `{status.version}` ({status.stage}) is complete.**\n"
                    f">\n"
                    f"> The release, tag, downloads, hashes, container images, "
                    f"docs, website, Bundle, and Helm outputs are all verified "
                    f"public. Closing."
                )
            ),
            retries=2, description="post completion comment",
        )
        issue_mod.invalidate_comment_memo(tracking_issue)
    retry_github_call(
        lambda: tracking_issue.edit(state="closed"),
        retries=2, description=f"close issue #{tracking_issue.number}",
    )
    return True


_PUBLISH_WORKFLOW = "release-publish.yml"

_PUBLISH_RUN_SCAN_LIMIT = 15

_PUBLISH_RUN_ACTIVE_STATUSES = ("queued", "in_progress", "waiting", "pending")


def agent_head_sha(gh_agent: Any, agent_repo: str) -> str:
    """The agent repo's default-branch head SHA, "" when unresolvable.

    The trusted controller version for publish runs: a run parked at the
    approval gate on any other commit would publish stale logic. "" means
    staleness cannot be judged, so no run is cancelled and any waiting run
    still counts as active (fail-safe: today's behavior).
    """
    try:
        repo = retry_github_call(
            lambda: gh_agent.get_repo(agent_repo),
            retries=2, description=f"get repo {agent_repo}",
        )
        return retry_github_call(
            lambda: repo.get_branch(repo.default_branch).commit.sha,
            retries=2, description=f"resolve {agent_repo} default-branch head",
        ) or ""
    except Exception:
        logger.exception("Cannot resolve the %s default-branch head; "
                         "skipping stale publish-run detection", agent_repo)
        return ""


def _cancel_stale_run(run: Any) -> bool:
    """Cancel a publish run whose head SHA is not the controller head.

    True when the cancel succeeded (the run no longer counts as active);
    False when it failed, in which case the caller treats the run as
    active so a fresh dispatch never races a gate that might still fire.
    """
    try:
        cancelled = retry_github_call(
            run.cancel,
            retries=2, description=f"cancel stale publish run {run.id}",
        )
    except Exception:
        logger.exception("Failed to cancel stale publish run %s; "
                         "treating it as active", run.id)
        return False
    if not cancelled:
        logger.error("Cancel of stale publish run %s was rejected; "
                     "treating it as active", run.id)
        return False
    logger.info("Cancelled stale publish run %s (head %s is not the "
                "controller head)", run.id, (run.head_sha or "")[:12])
    return True


def _active_publish_run(workflow: Any, branch: str, head_sha: str = "") -> Any:
    """The newest publish run for *branch* that is queued, running, or
    waiting at the approval gate; None when no such run exists.

    With *head_sha* (the agent repo's default-branch head), a run on any
    other commit is STALE controller code parked at the gate: it is
    cancelled and not counted as active, so a fresh dispatch replaces it.
    A failed cancel counts as active (fail-safe).
    """
    runs = retry_github_call(
        workflow.get_runs,
        retries=2, description="list publish runs",
    )
    marker = f" on {branch} "
    for index, run in enumerate(runs):
        if index >= _PUBLISH_RUN_SCAN_LIMIT:
            break
        if run.status not in _PUBLISH_RUN_ACTIVE_STATUSES:
            continue
        if marker not in f"{run.display_title or ''} ":
            continue
        if head_sha and (run.head_sha or "") != head_sha:
            if _cancel_stale_run(run):
                continue
        return run
    return None


def _publish_run_active(gh_agent: Any, agent_repo: str, branch: str,
                        head_sha: str = "") -> bool:
    """True when a publish run for *branch* is queued, running, or waiting
    at the approval gate; reconcile must not stack duplicates. Runs on a
    stale controller commit are cancelled and do not count (see
    :func:`_active_publish_run`)."""
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return True  # cannot see the workflow: do not dispatch blind
    return _active_publish_run(workflow, branch, head_sha) is not None


def waiting_publish_run_url(gh_agent: Any, agent_repo: str, branch: str,
                            head_sha: str = "") -> str:
    """The html_url of the active publish run for *branch*, "" when none
    is visible (including when the workflow itself is unreadable).

    Display-only companion to :func:`_publish_run_active`: reconciliation
    threads it into the READY callout's approval link; nothing gates on it.
    *head_sha* keeps the two views consistent: a stale run is never
    presented as the place to approve.
    """
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return ""
    run = _active_publish_run(workflow, branch, head_sha)
    if run is None:
        return ""
    return run.html_url or ""


def _dispatch_publish(gh_agent: Any, agent_repo: str, branch: str) -> None:
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    default_branch = retry_github_call(
        lambda: gh_agent.get_repo(agent_repo).default_branch,
        retries=2, description=f"resolve {agent_repo} default branch",
    )
    retry_github_call(
        lambda: workflow.create_dispatch(default_branch, inputs={"branch": branch}),
        retries=2, description="dispatch publish pipeline",
    )
    logger.info("Dispatched the publish pipeline for %s", branch)
