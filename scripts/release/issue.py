"""Render and locate the release tracking issue.

One open issue per (repo, release branch) shows the live release state. The
issue is a *display surface and dedup anchor only*:

- The identity marker ``<!-- valkey-ci-agent:release:<branch> -->`` locates
  the issue so duplicate starts reuse it and reconciliation can update it.
- The body is re-rendered wholesale from :class:`ReleaseStatus` on every
  reconcile pass. Nothing is ever parsed *out* of the body to make a
  decision, so editing the issue cannot authorize or alter anything: the
  next reconcile overwrites the edit with recomputed truth.
- Adoption acknowledgements are read from issue *comments*, and only from
  comments authored by the agent's own accounts (posted by the ``adopt``
  entry point after a live team-membership check).
"""

from __future__ import annotations

import os
import re
import weakref
from datetime import datetime
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN, BOT_LOGIN
from scripts.common.labels import ensure_label
from scripts.release.models import (
    CandidateState,
    CheckState,
    OutputState,
    ReleasePhase,
    ReleaseStatus,
    release_tag,
)

MARKER_NAMESPACE = "valkey-ci-agent:release"

# Comment authors whose adoption markers are trusted. GitHub Apps and the
# manual-dispatch bot comment under "<login>[bot]". Bare logins are excluded:
# app slugs and user accounts are distinct namespaces, so an outsider could
# register the bare name as a user and post trusted markers; the
# controller_login extension in trusted_comments covers PAT-authenticated runs.
TRUSTED_MARKER_AUTHORS = frozenset({f"{APP_LOGIN}[bot]", f"{BOT_LOGIN}[bot]"})

_ADOPT_MARKER_RE = re.compile(
    rf"<!-- {re.escape(MARKER_NAMESPACE)}:adopt:([0-9a-f]{{40}}) -->"
)

# Per-issue memo for comment fetches: one reconcile pass reads the same
# issue's comments several times (adoptions, notifications, completion) and
# the list only changes when the controller itself posts. Weak keys tie each
# entry to the issue object's lifetime (one pass) with no TTL machinery.
_COMMENTS_MEMO: "weakref.WeakKeyDictionary[Any, list[Any]]" = weakref.WeakKeyDictionary()


def invalidate_comment_memo(issue: Any) -> None:
    """Drop the memoized comment list for *issue* (call after posting to it)."""
    _COMMENTS_MEMO.pop(issue, None)

# Status vocabulary: the pipeline checkboxes plus these capitalized table
# icons (✅ ❌ ⏳ ⛔ ➖) are the only state symbols the tracker renders.
_CHECK_STATE_DISPLAY = {
    CheckState.PASSED: "✅ Passed",
    CheckState.FAILED: "❌ Failed",
    CheckState.PENDING: "⏳ Pending",
    CheckState.MISSING: "⛔ Missing",
    CheckState.STALLED: "❌ Stalled",
}

_OUTPUT_STATE_DISPLAY = {
    OutputState.VERIFIED: "✅ Verified",
    OutputState.PENDING: "⏳ Pending",
    OutputState.FAILED: "❌ Failed",
    OutputState.BLOCKED: "⛔ Blocked",
    OutputState.SKIPPED: "➖ N/A",
}

# Triage order for the Public outputs table: what needs a human first.
_OUTPUT_TRIAGE_ORDER = {
    OutputState.FAILED: 0,
    OutputState.BLOCKED: 1,
    OutputState.PENDING: 2,
    OutputState.SKIPPED: 3,
    OutputState.VERIFIED: 4,
}

# Render order for the progress checklist. COMPLETE is the terminal state of
# phase 6, not a seventh step.
_PHASE_ORDER = (
    ReleasePhase.NOTES,
    ReleasePhase.CANDIDATE,
    ReleasePhase.QUALIFICATION,
    ReleasePhase.READY,
    ReleasePhase.PUBLISHED,
    ReleasePhase.BUNDLE_HELM,
)

_PHASE_TITLES = {
    ReleasePhase.NOTES: "Release notes cut and merged",
    ReleasePhase.CANDIDATE: "Required CI green on the candidate",
    ReleasePhase.QUALIFICATION: "No-publish qualification passed",
    ReleasePhase.READY: "Published (human-approved)",
    ReleasePhase.PUBLISHED: "Core public outputs verified",
    ReleasePhase.BUNDLE_HELM: "Bundle and Helm verified",
}

def identity_marker(branch: str) -> str:
    """The marker that identifies the active release issue for *branch*."""
    return f"<!-- {MARKER_NAMESPACE}:{branch} -->"


def adopt_marker(sha: str) -> str:
    """The comment marker recording an owner's adoption of branch head *sha*."""
    return f"<!-- {MARKER_NAMESPACE}:adopt:{sha} -->"


def marker_present(body: Any, marker: str) -> bool:
    """True when *marker* starts a line of *body* outside any code fence.

    The controller always posts markers as their own line at column 0, so
    this matches every real marker, while a trusted comment that merely
    QUOTES one (indented, or inside a ``` fence as example text) can never
    forge the state the marker records. Every marker read-back (adopt,
    notify, nudge, autofix, complete, approval) must scan through this
    helper, never a raw substring test.
    """
    fenced = False
    for line in (body or "").splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if not fenced and line.startswith(marker):
            return True
    return False


def render_title(branch: str, version: str, stage: str) -> str:
    """Issue title. Falls back to the branch while no notes PR pins a version."""
    if version:
        suffix = version if stage == "ga" else f"{version}-{stage}"
        return f"Release {suffix}"
    return f"Next release on {branch}"


def render_live_title(status: ReleaseStatus) -> str:
    """The tracker title: constant ``Release {tag}``.

    Deliberately carries no phase or state (owner preference): the phase
    lives inside the tracker (badge, bar, checklist) and the list-level
    failure signal is the needs-attention label. Reconciliation compares
    against the current title and edits only on change, which also heals
    a manually mangled title.
    """
    return f"Release {_display_tag(status)}"


def render_body(status: ReleaseStatus, reconciled_at: datetime) -> str:
    """Render the full issue body from recomputed state.

    Deterministic for a given (status, reconciled_at), so reconciliation
    can compare the rendered body against the current one and skip no-op
    edits within the same minute. Layout is native left-aligned markdown:
    a header naming the release line, an alert callout stating exactly
    what happens next, the Pipeline checklist (the sole progress
    rendering), and evidence tables.
    """
    done = _phases_done(status)
    lines: list[str] = [
        identity_marker(status.branch),
        "",
        '<div align="center">',
        "",
        f"## Valkey {status.version or '(version pending)'}",
        "",
        _badge_row(status),
        "",
        _header_line(status),
        "",
        _progress_bar(status, done),
        "",
        "</div>",
        "",
    ]
    lines += _callout(status)
    lines += ["", "### Pipeline", ""]
    for phase in _PHASE_ORDER:
        marker = "x" if phase in done else " "
        title = _PHASE_TITLES[phase]
        if phase is status.phase and status.phase is not ReleasePhase.COMPLETE:
            title = f"**{title}**"
        lines.append(f"- [{marker}] {title}")
    lines += [
        "",
        "### Details",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Version | {_code(status.version) if status.version else '_Awaiting the release-notes PR_'} |",
        f"| Stage | {_code(status.stage.upper()) if status.stage else '_Awaiting the release-notes PR_'} |",
        f"| Release-notes PR | {_notes_pr_cell(status)} |",
        f"| Candidate SHA | {_candidate_cell(status)} |",
        f"| Qualification | {_qualification_cell(status)} |",
        f"| Release | {_release_cell(status)} |",
    ]

    if status.checks:
        lines += ["", "### Required Checks", ""]
        table = ["| Check | Result |", "|---|---|"]
        for check in status.checks:
            link = f" ([run]({check.url}))" if check.url else ""
            table.append(f"| `{check.name}` | {_CHECK_STATE_DISPLAY[check.state]}{link} |")
        if all(check.state is CheckState.PASSED for check in status.checks):
            lines += _collapsed(
                f"All {len(status.checks)} required checks passed",
                table,
            )
        else:
            lines += table

    if status.outputs:
        lines += ["", "### Public Outputs", ""]
        table = ["| Output | Status | Detail |", "|---|---|---|"]
        # Triage order (stable within groups): what needs a human first.
        for output in sorted(status.outputs,
                             key=lambda o: _OUTPUT_TRIAGE_ORDER[o.state]):
            link = f" ([evidence]({output.url}))" if output.url else ""
            table.append(
                f"| **{output.name}** | {_OUTPUT_STATE_DISPLAY[output.state]} | "
                f"{output.detail}{link} |"
            )
        if all(output.state in (OutputState.VERIFIED, OutputState.SKIPPED)
               for output in status.outputs):
            lines += _collapsed("All public outputs verified", table)
        else:
            lines += table

    lines += [
        "",
        "---",
        "",
        f"<sub>Reconciled {reconciled_at.strftime('%Y-%m-%d %H:%M')} UTC · "
        "auto-generated on every pass; manual edits are overwritten.</sub>",
    ]
    return "\n".join(lines) + "\n"


def _repo_url(status: ReleaseStatus) -> str:
    return f"https://github.com/{status.repo}"


# Short names for the phase badge in the header.
_PHASE_SHORT = {
    ReleasePhase.NOTES: "1/6 Notes",
    ReleasePhase.CANDIDATE: "2/6 Candidate CI",
    ReleasePhase.QUALIFICATION: "3/6 Qualification",
    ReleasePhase.READY: "4/6 Ready to Publish",
    ReleasePhase.PUBLISHED: "5/6 Public Outputs",
    ReleasePhase.BUNDLE_HELM: "6/6 Bundle & Helm",
    ReleasePhase.COMPLETE: "Complete",
}

# What the current phase is DOING, for the progress-bar label. The checklist
# uses the completed-form _PHASE_TITLES; the bar describes the phase in
# flight, so it must not assert the outcome ("Published" while unpublished).
_PHASE_ACTIVE = {
    ReleasePhase.NOTES: "Cutting Release Notes",
    ReleasePhase.CANDIDATE: "Waiting for Candidate CI",
    ReleasePhase.QUALIFICATION: "Qualifying the Candidate",
    ReleasePhase.READY: "Ready to Publish: Awaiting Approval",
    ReleasePhase.PUBLISHED: "Verifying Public Outputs",
    ReleasePhase.BUNDLE_HELM: "Verifying Bundle and Helm",
}


def _badge(label: str, message: str, color: str) -> str:
    """A shields.io static badge (deterministic URL, proxied by GitHub)."""
    def escape(text: str) -> str:
        # shields.io static-badge escaping: dash and underscore double, and
        # space/slash must be percent-encoded or they break the URL path.
        return (text.replace("-", "--").replace("_", "__")
                    .replace(" ", "%20").replace("/", "%2F"))
    return (
        f"![{label}](https://img.shields.io/badge/"
        f"{escape(label)}-{escape(message)}-{color}?style=flat-square)"
    )


def _badge_row(status: ReleaseStatus) -> str:
    version_badge = _badge("version", _display_tag(status) or "pending", "0969da")
    stage_badge = _badge("stage", status.stage.upper() if status.stage else "pending", "8250df")
    if status.phase is ReleasePhase.COMPLETE:
        phase_color = "1a7f37"
    elif has_failures(status):
        phase_color = "cf222e"
    elif status.ready:
        phase_color = "1a7f37"
    else:
        phase_color = "d29922"
    phase_badge = _badge("phase", _PHASE_SHORT[status.phase], phase_color)
    return f"{version_badge} {stage_badge} {phase_badge}"


def _progress_bar(status: ReleaseStatus, done: "set[ReleasePhase]") -> str:
    """Six-segment bar: 🟩 done, 🟦 current (🟥 when failing), ⬜ ahead."""
    current_block = "🟥" if has_failures(status) else "🟦"
    blocks = "".join(
        "🟩" if phase in done else (current_block if phase is status.phase else "⬜")
        for phase in _PHASE_ORDER
    )
    if status.phase is ReleasePhase.COMPLETE:
        return f"{blocks} **Complete**"
    label = _PHASE_ACTIVE[status.phase]
    if has_failures(status):
        label = f"{label} (Failures Need Attention)"
    return f"{blocks} **{label}**"


def _header_line(status: ReleaseStatus) -> str:
    """The one-line header under the H2: stage, release line, and the
    cross-tracker link. No badges, no HTML."""
    repo_url = _repo_url(status)
    trackers_url = (
        f"{repo_url}/issues?q=is%3Aissue+is%3Aopen+label%3Arelease-tracker"
    )
    # Compact: the stage already lives in the badge row above, so the
    # subtitle carries only identity links, short enough to read at a
    # glance under the heading.
    return (
        f"[`{status.repo}`]({repo_url}) · "
        f"[`{status.branch}`]({repo_url}/tree/{status.branch}) · "
        f"[All trackers]({trackers_url})"
    )


def _collapsed(summary: str, table: list[str]) -> list[str]:
    """An all-green table folded behind a one-line summary."""
    return [f"<details><summary>{summary}</summary>", "", *table, "", "</details>"]


def has_failures(status: ReleaseStatus) -> bool:
    return bool(
        status.alerts
        or any(o.state is OutputState.FAILED for o in status.outputs)
        or any(c.state in (CheckState.FAILED, CheckState.STALLED) for c in status.checks)
        or status.qualification.failed_jobs
    )


def _callout(status: ReleaseStatus) -> list[str]:
    """The alert block: current state and the exact next action."""
    if status.phase is ReleasePhase.COMPLETE:
        return [
            "> [!NOTE]",
            "> **Release complete.** Every required public output is verified; "
            "this issue closes automatically.",
        ]
    if status.published:
        failed = [o for o in status.outputs if o.state is OutputState.FAILED]
        if failed:
            names = ", ".join(f"`{o.name}`" for o in failed)
            return [
                "> [!CAUTION]",
                f"> **Published, with downstream failures needing attention: "
                f"{names}.** Details in the Public Outputs table; the release "
                f"team has been notified. Everything else continues to be "
                f"observed on every reconciliation pass.",
            ]
        return [
            "> [!NOTE]",
            "> **Published.** Downstream outputs are being verified against "
            "their canonical public locations (registries, downloads, merged "
            "PRs), not just workflow success. No action needed unless an "
            "output turns failed.",
        ]
    if status.ready:
        ready = [
            "> [!IMPORTANT]",
            "> **Ready to publish.** Every gate passed on the candidate. "
            "Approval publishes the release and creates a permanent tag; "
            "the checklist is posted below.",
        ]
        if status.approval_run_url:
            ready.append(">")
            ready.append(f"> **Approve here:** {status.approval_run_url}")
        return ready
    blocker_lines = [f"> - {blocker}" for blocker in status.blockers] or ["> - (None recorded)"]
    return ["> [!WARNING]", "> **Not ready. Blocked on:**", *blocker_lines]


def _phases_done(status: ReleaseStatus) -> set[ReleasePhase]:
    if status.phase is ReleasePhase.COMPLETE:
        return set(_PHASE_ORDER)
    return set(_PHASE_ORDER[: _PHASE_ORDER.index(status.phase)])


def _display_tag(status: ReleaseStatus) -> str:
    if not status.version:
        return ""
    return release_tag(status.version, status.stage)


def _code(value: str) -> str:
    return f"`{value}`"


def _short(sha: str) -> str:
    return sha[:12] if sha else ""


def _notes_pr_cell(status: ReleaseStatus) -> str:
    if not status.notes_pr_number:
        return "_None found_"
    state = "Merged" if status.notes_pr_merged else "Open (not merged)"
    return f"[#{status.notes_pr_number}]({status.notes_pr_url}): {state}"


def _candidate_cell(status: ReleaseStatus) -> str:
    candidate = status.candidate
    if candidate.state is CandidateState.NONE:
        return "_None (recorded when the release-notes PR merges)_"
    link = f"[`{_short(candidate.sha)}`]({_repo_url(status)}/commit/{candidate.sha})"
    if status.published:
        # Post-publication the tag pins the candidate; the branch may
        # legitimately move on, so "current branch head" would be a lie.
        return f"{link}: Pinned by the release tag"
    descriptions = {
        CandidateState.CURRENT: "Current branch head",
        CandidateState.ADOPTED: "Adopted branch head (owner-acknowledged)",
        CandidateState.INVALIDATED: (
            "**Invalidated** (the branch moved; an authorized owner must adopt "
            "the exact new head before qualification continues)"
        ),
    }
    return f"{link}: {descriptions[candidate.state]}"


def _qualification_cell(status: ReleaseStatus) -> str:
    qualification = status.qualification
    if qualification.run_id == 0:
        if status.published:
            # Post-publication the gate is history (publication revalidated
            # it); "no run yet" would misread as a missing prerequisite.
            return "_Gated before publication; not re-evaluated afterward_"
        return "_No run for the candidate SHA yet_"
    link = f"[qualification run]({qualification.url})"
    if qualification.pending:
        return f"In progress ({link})"
    if qualification.passed:
        return f"Passed ({link})"
    failed = ", ".join(qualification.failed_jobs[:3])
    return f"Failed ({link}): {failed}"


def _release_cell(status: ReleaseStatus) -> str:
    if not status.published:
        return "_Not published_"
    return f"[Published]({status.release_url})"


def branch_label(branch: str) -> str:
    """The per-branch identity label carried by the tracking issue."""
    return f"release:{branch}"


def find_release_issue(repo: Any, branch: str, *, label: str) -> Any:
    """Return the open tracking issue for *branch*, or None.

    Identity is the label pair (tracker label + ``release:<branch>``), not
    the rendered body: a hand-edit that strips text from the body must not
    orphan the release into a duplicate tracker. The body marker remains as
    a migration fallback for issues created before the branch label existed.
    The REST list endpoint returns PRs alongside issues; those are dropped
    the same way issue_dedup drops them (already-fetched payload, no GET).
    """
    labelled = retry_github_call(
        lambda: list(repo.get_issues(state="open", labels=[label, branch_label(branch)])),
        retries=2, description=f"list open {label}+{branch_label(branch)} issues",
    )
    for issue in labelled:
        if "pull_request" not in issue._rawData:
            return issue

    marker = identity_marker(branch)
    issues = retry_github_call(
        lambda: list(repo.get_issues(state="open", labels=[label])),
        retries=2, description=f"list open {label} issues",
    )
    for issue in issues:
        if "pull_request" in issue._rawData:
            continue
        if marker in (issue.body or ""):
            return issue
    return None


def controller_login(gh: Any) -> str:
    """The authenticated identity's login, "" when unavailable.

    App installation tokens have no user context (the /user endpoint
    refuses); PATs do. Cached per client so trust checks do not re-query.
    """
    cached = getattr(gh, "_release_controller_login", None)
    if cached is not None:
        return cached
    try:
        login = retry_github_call(
            lambda: gh.get_user().login,
            retries=2, description="resolve authenticated login",
        )
    except GithubException:
        login = ""
    gh._release_controller_login = login
    return login


def trusted_comments(issue: Any, gh: Any = None) -> list[Any]:
    """Comments on *issue* authored by the controller's identities, oldest first.

    The trust filter for every marker the controller reads back (adoptions,
    notification fingerprints): a marker pasted by anyone else is invisible.
    Trust covers the static bot identities, an env-provided App login
    (``RELEASE_BOT_LOGIN``, set by the release workflows from the minted
    App's slug so a fork's own App is trusted), plus the *currently
    authenticated* identity when it is a user (fork runs authenticate as the
    fork owner's PAT, and markers the controller wrote must remain readable
    to it).

    The fetch is memoized per issue object (a reconcile pass reads the same
    issue's comments up to three times); posting a comment must call
    :func:`invalidate_comment_memo` so the next read sees it.
    """
    trusted = set(TRUSTED_MARKER_AUTHORS)
    # A fork running its OWN GitHub App posts as "<forkslug>[bot]", which is
    # not in the static set, and controller_login() is empty for App tokens,
    # so without this the controller could not read back its own markers
    # (the autofix would re-fire every pass). The release workflows pass the
    # minted App's slug as RELEASE_BOT_LOGIN (formatted "<slug>[bot]").
    env_login = os.environ.get("RELEASE_BOT_LOGIN", "").strip()
    if env_login:
        trusted.add(env_login)
    if gh is not None:
        login = controller_login(gh)
        if login:
            trusted.add(login)
    comments = _COMMENTS_MEMO.get(issue)
    if comments is None:
        comments = retry_github_call(
            lambda: list(issue.get_comments()),
            retries=2, description=f"list comments on issue #{issue.number}",
        )
        _COMMENTS_MEMO[issue] = comments
    return [
        comment for comment in comments
        if (getattr(comment.user, "login", "") or "") in trusted
    ]


def adopted_shas(issue: Any, gh: Any = None) -> tuple[str, ...]:
    """Adoption acknowledgements recorded on *issue*, oldest first.

    Only comments authored by the agent's own accounts count; a marker pasted
    into a comment by anyone else is ignored, so commenting on (or editing)
    the issue cannot acknowledge a branch movement. The ``adopt`` entry point
    is the sole writer and it verifies team membership live before posting.
    """
    return tuple(
        match.group(1)
        for comment in trusted_comments(issue, gh)
        for match in _ADOPT_MARKER_RE.finditer(comment.body or "")
        if marker_present(comment.body, match.group(0))
    )


def ensure_tracker_labels(repo: Any, branch: str, tracker_label: str) -> None:
    """Create the tracker and branch identity labels when absent.

    GitHub's auto-creation of labels on issue writes is not a documented
    guarantee; creating them explicitly (mirroring the backport module's
    ensure-label pattern) removes the doubt.
    """
    ensure_label(repo, tracker_label, "0e8a16",
                 "Release tracking issue maintained by the release controller")
    ensure_label(repo, branch_label(branch), "1d76db",
                 f"Active release on the {branch} line")
