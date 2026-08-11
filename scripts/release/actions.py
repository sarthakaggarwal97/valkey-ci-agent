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
from functools import partial
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import issue as issue_mod
from scripts.release import qualification as qual_mod
from scripts.release import verify as verify_mod
from scripts.release.models import (
    CandidateState,
    CheckState,
    DailyCiState,
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
    ):
        # Minimum-clicks: READY auto-starts the publish pipeline. This is
        # safe to automate because the dispatch was never the gate: the
        # validate job posts the approval evidence and the publish job holds
        # at the protected environment until a human approves.
        publish_note = _advance_publish(gh, gh_agent, agent_repo, status,
                                        tracking_issue, agent_head_sha)
        if publish_note:
            performed.append(publish_note)

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

    # Recovery-aware notifications: the fingerprint of every notify and
    # wedge comment hashes (generation, sorted keys), and a pass observing
    # ZERO failure and ZERO wedge items advances the generation, so a
    # failure that recurs after a clean pass re-notifies exactly once while
    # a steady failure stays suppressed.
    failures = _failure_items(status)
    wedges = _wedge_items(status)
    if failures or wedges:
        generation, _ = _notify_generation(gh, tracking_issue)
    else:
        generation = 0
        _record_recovery(gh, tracking_issue)

    if failures:
        note = _notify_once(gh, policy, status, tracking_issue,
                            failures, generation)
        if note:
            performed.append(note)

    if wedges:
        wedge_note = _wedge_nudge_once(gh, policy, status, tracking_issue,
                                       wedges, generation)
        if wedge_note:
            performed.append(wedge_note)

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
    failures: "list[tuple[str, str]]", generation: int,
) -> str:
    """Mention the authorized team once per distinct failure state.

    The failure fingerprint is stamped into the notification comment; while
    the observed failure set is unchanged no further comment is posted, and
    a different failure set notifies again, exactly once. *generation* (the
    recovery generation, see :func:`_record_recovery`) is hashed into the
    fingerprint so the SAME failure set recurring after a clean pass
    notifies again.
    """
    # Fingerprint over (generation, stable keys), not the rendered prose: a
    # wording tweak in a detail string must never re-ping the team, while a
    # NEW failure (a new failed run id, a new failing check) or a recurrence
    # after recovery must.
    fingerprint = _notification_fingerprint(generation,
                                            [key for key, _ in failures])
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


def _notification_fingerprint(generation: int, keys: "list[str]") -> str:
    """The 12-hex fingerprint of (recovery generation, sorted stable keys).

    Hashing the generation in means an identical key set recurring AFTER a
    recovery (see :func:`_record_recovery`) produces a new fingerprint and
    so re-notifies exactly once, while an unchanged state within one
    generation stays suppressed.
    """
    return hashlib.sha256(
        "\n".join([str(generation), *sorted(keys)]).encode("utf-8")
    ).hexdigest()[:12]


_NOTIFY_GEN_MARKER_RE = re.compile(
    rf"<!-- {re.escape(issue_mod.MARKER_NAMESPACE)}:notify-gen:(\d+) -->"
)


def _marker_prefix_present(body: Any, prefix: str) -> bool:
    """True when a line of *body*, outside any code fence, starts with
    *prefix*. Same fence discipline as issue_mod.marker_present, for reads
    that need a marker family rather than one exact marker."""
    fenced = False
    for line in (body or "").splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if not fenced and line.startswith(prefix):
            return True
    return False


def _notify_generation(gh: Any, tracking_issue: Any) -> "tuple[int, Any]":
    """(current recovery generation, the trusted comment recording it).

    (0, None) before any recovery was ever recorded. The newest (highest)
    generation on a trusted comment wins; markers on untrusted comments are
    ignored exactly like every other marker read-back.
    """
    best_generation, best_comment = 0, None
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        fenced = False
        for line in (comment.body or "").splitlines():
            if line.lstrip().startswith("```"):
                fenced = not fenced
                continue
            if fenced:
                continue
            match = _NOTIFY_GEN_MARKER_RE.match(line)
            if match and int(match.group(1)) >= best_generation:
                best_generation, best_comment = int(match.group(1)), comment
    return best_generation, best_comment


def _record_recovery(gh: Any, tracking_issue: Any) -> None:
    """Advance the recovery generation after a clean pass (zero failure AND
    zero wedge items), so a later recurrence hashes to a new fingerprint.

    Skipped while the tracker has no notification history at all (no
    notify, wedge, or generation marker): a healthy release gets no
    bookkeeping comment. The generation lives in one tiny comment that is
    edited in place, never duplicated.
    """
    generation, comment = _notify_generation(gh, tracking_issue)
    if comment is None and not _has_notification_history(gh, tracking_issue):
        return
    body = (
        f"<!-- {issue_mod.MARKER_NAMESPACE}:notify-gen:{generation + 1} -->\n"
        f"<sub>Notification bookkeeping: generation "
        f"{generation + 1} (edited in place).</sub>"
    )
    if comment is None:
        retry_github_call(
            lambda: tracking_issue.create_comment(body=body),
            retries=2, description="post recovery-generation comment",
        )
    else:
        retry_github_call(
            lambda: comment.edit(body=body),
            retries=2, description="advance recovery-generation comment",
        )
    issue_mod.invalidate_comment_memo(tracking_issue)


def _has_notification_history(gh: Any, tracking_issue: Any) -> bool:
    """True when any trusted comment carries a notify or wedge marker."""
    prefixes = (f"<!-- {issue_mod.MARKER_NAMESPACE}:notify:",
                f"<!-- {issue_mod.MARKER_NAMESPACE}:wedge:")
    return any(
        _marker_prefix_present(comment.body, prefix)
        for comment in issue_mod.trusted_comments(tracking_issue, gh)
        for prefix in prefixes
    )


def _wedge_items(status: ReleaseStatus) -> "list[tuple[str, str]]":
    """(stable key, rendered text) per silently wedged gate.

    A MISSING required check or a MISSING/STALE daily gate is not a
    failure, so :func:`_failure_items` never escalates it, yet nothing is
    running that could change it: the release waits silently forever. No
    time-based grace this round (status carries no tracker age): observing
    the state at reconcile time is the whole trigger, keyed on the evidence
    identity so an unchanged wedge nudges once per generation.
    """
    items = [
        (f"wedge:check:{status.candidate.sha}:{check.name}",
         f"Blocked without progress: Required check `{check.name}` has no "
         f"run on the candidate SHA. This does not resolve on its own.")
        for check in status.checks
        if check.state is CheckState.MISSING
    ]
    if status.daily.state in (DailyCiState.MISSING, DailyCiState.STALE):
        detail = status.daily.detail or (
            f"Daily CI is {status.daily.state.value} on `{status.branch}`."
        )
        if not detail.endswith("."):
            detail += "."
        items.append((
            f"wedge:daily:{status.branch}:{status.daily.state.value}:"
            f"{status.daily.run_id}",
            f"Blocked without progress: {detail} "
            f"This does not resolve on its own.",
        ))
    return items


def _wedge_nudge_once(
    gh: Any, policy: RepoReleasePolicy, status: ReleaseStatus, tracking_issue: Any,
    wedges: "list[tuple[str, str]]", generation: int,
) -> str:
    """Mention the authorized team once per distinct wedged state (F24).

    Same fingerprint-marker pattern as :func:`_notify_once`, in its own
    ``wedge:`` marker family: an unchanged wedge never re-pings within a
    generation; a resolved-then-recurring one re-pings once through the
    generation bump.
    """
    fingerprint = _notification_fingerprint(generation,
                                            [key for key, _ in wedges])
    marker = f"<!-- {issue_mod.MARKER_NAMESPACE}:wedge:{fingerprint} -->"
    for comment in issue_mod.trusted_comments(tracking_issue, gh):
        if issue_mod.marker_present(comment.body, marker):
            return ""
    tag = release_tag(status.version, status.stage)
    lines = "\n".join(f"> {text}" for _, text in wedges)
    retry_github_call(
        lambda: tracking_issue.create_comment(
            body=(
                f"{marker}\n"
                f"> [!IMPORTANT]\n"
                f"> **{policy.mention}: Release `{tag}` Is Blocked Without "
                f"Progress.**\n"
                f">\n"
                f"{lines}\n"
                f"\n"
                f"<sub>One-time nudge: posts again only if the blocked state "
                f"changes.</sub>"
            )
        ),
        retries=2, description="post blocked-without-progress nudge",
    )
    issue_mod.invalidate_comment_memo(tracking_issue)
    logger.info("Nudged %s about %d wedged gate(s)", policy.authorized_team,
                len(wedges))
    return f"nudged {policy.authorized_team} ({len(wedges)} wedged gate(s))"


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


def _alert_key(text: str) -> str:
    """Stable notification key for an alert string.

    Alerts are produced elsewhere (reconcile, verify) as full prose, out of
    this module's reach, so the key is derived here: hex ids (7 to 40
    chars) and then all digit runs are stripped before hashing, so a
    rewording that only swaps a run id or SHA keeps the alert's identity
    and never re-pings. Known limitation, accepted: two DIFFERENT alerts
    whose prose differs only in ids or numbers collapse to one key, so the
    second one does not re-notify on its own (a recovery generation bump
    still re-arms it).
    """
    normalized = re.sub(r"[0-9a-fA-F]{7,40}", "", text)
    normalized = re.sub(r"[0-9]+", "", normalized)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"alert:{digest}"


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
    items: "list[tuple[str, str]]" = [(_alert_key(alert), alert)
                                      for alert in status.alerts]
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
    # Daily FAILED only: STALE and MISSING are blockers (waiting states),
    # not failures. The key carries the run id, so the same failing run
    # never re-pings while a NEW failing run does.
    if status.daily.state is DailyCiState.FAILED:
        items.append((
            f"daily:{status.daily.run_id}",
            f"Daily CI run {status.daily.run_id} failed: {status.daily.url}",
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
    marker = issue_mod.complete_marker()
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

# Per-listing cap on each server-side filtered runs query. The status
# filter (not this cap) is what keeps a long-waiting run visible: it can
# never fall out of a newest-N window regardless of how many completed
# runs pile up above it. The cap only bounds pathological volume within
# one filtered listing.
_PUBLISH_RUN_SCAN_LIMIT = 50

# Statuses that can hold the slot BEFORE the approval gate; these are the
# only statuses stale-run cleanup may ever cancel. in_progress is
# deliberately not here: it is past the gate (a human approved), so it is
# always active and never cancelled.
_PUBLISH_RUN_GATED_STATUSES = ("queued", "waiting", "pending")

# in_progress first: when both an in_progress and a gate-parked run match,
# the in_progress one (past the gate, the publication itself) is the one
# reported as active.
_PUBLISH_RUN_ACTIVE_STATUSES = ("in_progress",) + _PUBLISH_RUN_GATED_STATUSES

# The optional candidate binding release-publish.yml stamps into its
# run-name when dispatched with tag/candidate_sha inputs:
# "Publish release on <branch> · <tag> @ <sha> (requested by <actor>)".
_PUBLISH_TITLE_BINDING_RE = re.compile(
    r" · (?P<tag>\S+) @ (?P<sha>[0-9a-fA-F]{7,40})(?![0-9a-zA-Z])"
)


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


def _run_binding(run: Any) -> "tuple[str, str]":
    """(tag, candidate SHA) the run-name carries, ("", "") when the run was
    dispatched without bindings (manual dispatch, pre-binding runs)."""
    match = _PUBLISH_TITLE_BINDING_RE.search(run.display_title or "")
    if match is None:
        return "", ""
    return match.group("tag"), match.group("sha").lower()


def _matches_branch(run: Any, branch: str) -> bool:
    return f" on {branch} " in f"{run.display_title or ''} "


def _is_stale_binding(run: Any, head_sha: str, tag: str,
                      candidate_sha: str) -> bool:
    """True when a gate-parked run is bound to a different controller head
    or (via its run-name) a different tag or candidate than the current
    one. A run whose name carries no binding is never candidate-stale: its
    target cannot be proven different."""
    if head_sha and (run.head_sha or "") != head_sha:
        return True
    run_tag, run_sha = _run_binding(run)
    if candidate_sha and run_sha and run_sha != candidate_sha.lower():
        return True
    if tag and run_tag and run_tag != tag:
        return True
    return False


def _list_publish_runs(workflow: Any, statuses: "tuple[str, ...]") -> "list[Any]":
    """All runs in *statuses*, one server-side filtered listing per status.

    Each listing is re-checked client-side (run.status must equal the
    queried status) so an unfiltered or cached listing can never smuggle a
    completed run into the active set, and each run appears exactly once.
    """
    runs: "list[Any]" = []
    for wanted in statuses:
        listing = retry_github_call(
            partial(workflow.get_runs, status=wanted),
            retries=2, description=f"list {wanted} publish runs",
        )
        for index, run in enumerate(listing):
            if index >= _PUBLISH_RUN_SCAN_LIMIT:
                break
            if run.status != wanted:
                continue
            runs.append(run)
    return runs


def find_publish_runs(workflow: Any, branch: str, head_sha: str = "", *,
                      tag: str = "", candidate_sha: str = "",
                      ) -> "tuple[Any, list[Any]]":
    """Pure lookup of the publish runs currently holding *branch*'s slot.

    Returns ``(active, stale)`` and performs NO side effects (cancellation
    is :func:`cancel_stale_publish_runs`' job, wired separately by the
    caller):

    - *active* is a run genuinely in flight for the current controller
      head and candidate: any in_progress run (past the approval gate,
      always active regardless of bindings), or a gate-parked run whose
      head and run-name bindings match. None when no such run exists.
    - *stale* lists gate-parked runs (queued, waiting, pending ONLY) bound
      to a different controller head, tag, or candidate.

    With *head_sha* "" head staleness cannot be judged and no run is
    head-stale (fail-safe); with *tag*/*candidate_sha* "" the run-name
    binding is not checked. Both views (dispatch idempotency and the
    approval URL) share this one matcher so a stale or mismatched run is
    never presented as the place to approve.
    """
    active: Any = None
    stale: "list[Any]" = []
    for run in _list_publish_runs(workflow, _PUBLISH_RUN_ACTIVE_STATUSES):
        if not _matches_branch(run, branch):
            continue
        # An in_progress run is past the gate: it IS the publication in
        # progress. Bindings do not matter; it is always active, never
        # stale, and must never be cancelled.
        if (run.status != "in_progress"
                and _is_stale_binding(run, head_sha, tag, candidate_sha)):
            stale.append(run)
        elif active is None:
            active = run
    return active, stale


def _cancel_stale_run(run: Any) -> bool:
    """Cancel one gate-parked stale publish run.

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
    logger.info("Cancelled stale publish run %s (head %s)", run.id,
                (run.head_sha or "")[:12])
    return True


def cancel_stale_publish_runs(runs: "list[Any]", *, act: bool = True) -> bool:
    """Cancel every stale run in *runs* (the side-effect half of the
    finder/cancel split; :func:`find_publish_runs` never calls this).

    *runs* must come from the finder's stale list, which only ever contains
    queued/waiting/pending runs: an in_progress run is past the approval
    gate and is never staged for cancellation. The guard here is defense in
    depth for a caller passing a hand-built list.

    With ``act`` False this is a strict no-op returning False (observation
    mode: the runs still count as active). Returns True only when every run
    was cancelled; any failure returns False so the caller fails safe.
    """
    if not act:
        return False
    all_cancelled = True
    for run in runs:
        if run.status not in _PUBLISH_RUN_GATED_STATUSES:
            logger.error("Refusing to cancel publish run %s (status %s): "
                         "only gate-parked runs are ever cancelled",
                         run.id, run.status)
            all_cancelled = False
            continue
        if not _cancel_stale_run(run):
            all_cancelled = False
    return all_cancelled


def _active_publish_run(workflow: Any, branch: str, head_sha: str = "", *,
                        tag: str = "", candidate_sha: str = "",
                        act: bool = True) -> Any:
    """The run currently holding *branch*'s publish slot, cancelling stale
    gate-parked runs along the way; None when the slot is free.

    A stale run that could not (or, with ``act`` False, must not) be
    cancelled still holds the slot: its gate might still fire, so a fresh
    dispatch would race it.
    """
    active, stale = find_publish_runs(workflow, branch, head_sha,
                                      tag=tag, candidate_sha=candidate_sha)
    if stale and not cancel_stale_publish_runs(stale, act=act):
        return active if active is not None else stale[0]
    return active


def _publish_run_active(gh_agent: Any, agent_repo: str, branch: str,
                        head_sha: str = "", *, tag: str = "",
                        candidate_sha: str = "") -> bool:
    """True when a publish run for *branch* is queued, running, or waiting
    at the approval gate; reconcile must not stack duplicates. Stale
    gate-parked runs are cancelled and do not count (see
    :func:`find_publish_runs`)."""
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return True  # cannot see the workflow: do not dispatch blind
    return _active_publish_run(workflow, branch, head_sha, tag=tag,
                               candidate_sha=candidate_sha) is not None


def waiting_publish_run_url(gh_agent: Any, agent_repo: str, branch: str,
                            head_sha: str = "", *, tag: str = "",
                            candidate_sha: str = "") -> str:
    """The html_url of the active publish run for *branch*, "" when none
    is visible (including when the workflow itself is unreadable).

    Display-only companion to :func:`_publish_run_active`: reconciliation
    threads it into the READY callout's approval link; nothing gates on it.
    Observation only (finder, no cancel step): a stale run or a run bound
    to a different tag/candidate is simply never presented as the place to
    approve.
    """
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return ""
    active, _stale = find_publish_runs(workflow, branch, head_sha,
                                       tag=tag, candidate_sha=candidate_sha)
    if active is None:
        return ""
    return active.html_url or ""


def _halted_publish_failure(workflow: Any, branch: str, head_sha: str, *,
                            tag: str = "", candidate_sha: str = "") -> Any:
    """The completed publish run that halts re-dispatch, or None.

    The newest COMPLETED publish run for *branch* at the current controller
    head (and, when its run-name carries bindings, for the current
    tag/candidate) is inspected: failure or cancelled halts, anything else
    (or no such run) does not. A run on another controller head or another
    candidate is skipped entirely, so a new controller head or a new
    candidate re-arms dispatch by construction. With *head_sha* "" the head
    filter is skipped: halting on the newest matching failure beats looping
    when staleness cannot be judged.
    """
    listing = retry_github_call(
        lambda: workflow.get_runs(status="completed"),
        retries=2, description="list completed publish runs",
    )
    for index, run in enumerate(listing):
        if index >= _PUBLISH_RUN_SCAN_LIMIT:
            break
        if run.status != "completed":
            continue
        if not _matches_branch(run, branch):
            continue
        if head_sha and (run.head_sha or "") != head_sha:
            continue  # another controller version's run: a new head re-arms
        run_tag, run_sha = _run_binding(run)
        if candidate_sha and run_sha and run_sha != candidate_sha.lower():
            continue  # another candidate's run: a new candidate re-arms
        if tag and run_tag and run_tag != tag:
            continue
        if run.conclusion in ("failure", "cancelled"):
            return run
        return None  # the newest relevant completed run did not fail
    return None


def _advance_publish(gh: Any, gh_agent: Any, agent_repo: str,
                     status: ReleaseStatus, tracking_issue: Any,
                     head_sha: str) -> str:
    """Move the READY phase forward: dispatch the publish pipeline unless a
    run already holds the slot or a failed run halts re-dispatch.

    The halt is deliberate: a publish run that COMPLETED as failure or
    cancelled at this controller head, for this candidate, would fail the
    same way again; re-dispatching would loop. The one-shot marker-gated
    warning tells the human, and a new controller head or a new candidate
    re-arms dispatch (the failed run then no longer matches).
    """
    tag = release_tag(status.version, status.stage)
    candidate_sha = status.candidate.sha
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    if workflow is None:
        return ""  # cannot see the workflow: do not dispatch blind
    if _active_publish_run(workflow, status.branch, head_sha, tag=tag,
                           candidate_sha=candidate_sha) is not None:
        return ""
    halted = _halted_publish_failure(workflow, status.branch, head_sha,
                                     tag=tag, candidate_sha=candidate_sha)
    if halted is not None:
        posted = _autofix_once(
            gh, tracking_issue, key="publish-halt",
            fingerprint_source=f"{candidate_sha}:{halted.id}",
            callout=(
                f"> [!WARNING]\n"
                f"> **The publish pipeline failed;** the controller will "
                f"not re-dispatch until the controller code changes or a "
                f"human re-runs it: [run {halted.id}]({halted.html_url})."
            ),
        )
        if posted:
            return (f"halted publish re-dispatch for {status.branch} "
                    f"(publish run {halted.id} concluded {halted.conclusion})")
        return ""
    _dispatch_publish(gh_agent, agent_repo, status.branch, tag=tag,
                      candidate_sha=candidate_sha)
    return (f"dispatched the publish pipeline for {status.branch} "
            f"(holds at the approval gate)")


def _dispatch_publish(gh_agent: Any, agent_repo: str, branch: str,
                      tag: str = "", candidate_sha: str = "") -> None:
    """Dispatch release-publish.yml for *branch*, binding the run to the
    exact *tag* and *candidate_sha* it was dispatched for (stamped into the
    run-name so later passes can correlate by candidate, not just branch)."""
    workflow = workflow_handle(gh_agent, agent_repo, _PUBLISH_WORKFLOW)
    default_branch = retry_github_call(
        lambda: gh_agent.get_repo(agent_repo).default_branch,
        retries=2, description=f"resolve {agent_repo} default branch",
    )
    inputs = {"branch": branch, "tag": tag, "candidate_sha": candidate_sha}
    retry_github_call(
        lambda: workflow.create_dispatch(default_branch, inputs=inputs),
        retries=2, description="dispatch publish pipeline",
    )
    logger.info("Dispatched the publish pipeline for %s (%s @ %s)",
                branch, tag or "<no tag>", candidate_sha[:12] or "<no sha>")
