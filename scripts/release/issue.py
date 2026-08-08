"""Render and locate the release tracking issue.

One open issue per (repo, release branch) shows the live release state. The
issue is a *display surface and dedup anchor only*:

- The identity marker ``<!-- valkey-ci-agent:release:<branch> -->`` locates
  the issue so duplicate starts reuse it and reconciliation can update it.
- The body is re-rendered wholesale from :class:`ReleaseStatus` on every
  reconcile pass. Nothing is ever parsed *out* of the body to make a
  decision, so editing the issue cannot authorize or alter anything: the
  next reconcile simply overwrites the edit with recomputed truth.
- Adoption acknowledgements are read from issue *comments*, and only from
  comments authored by the agent's own accounts (posted by the ``adopt``
  entry point after a live team-membership check).
"""

from __future__ import annotations

import re
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN, BOT_LOGIN
from scripts.release.models import (
    CandidateState,
    CheckState,
    OutputState,
    ReleasePhase,
    ReleaseStatus,
)

MARKER_NAMESPACE = "valkey-ci-agent:release"

# Comment authors whose adoption markers are trusted. GitHub Apps and the
# manual-dispatch bot comment under "<login>[bot]"; the bare logins are
# accepted for locally driven runs authenticated as those accounts.
TRUSTED_MARKER_AUTHORS = frozenset(
    {APP_LOGIN, BOT_LOGIN, f"{APP_LOGIN}[bot]", f"{BOT_LOGIN}[bot]"}
)

_ADOPT_MARKER_RE = re.compile(
    rf"<!-- {re.escape(MARKER_NAMESPACE)}:adopt:([0-9a-f]{{40}}) -->"
)

_CHECK_STATE_DISPLAY = {
    CheckState.PASSED: "✅ passed",
    CheckState.FAILED: "❌ failed",
    CheckState.PENDING: "⏳ pending",
    CheckState.MISSING: "⚠️ missing",
    CheckState.STALLED: "🛑 stalled",
}

_OUTPUT_STATE_DISPLAY = {
    OutputState.VERIFIED: "✅ verified",
    OutputState.PENDING: "⏳ pending",
    OutputState.FAILED: "❌ failed",
    OutputState.BLOCKED: "⛔ blocked",
    OutputState.SKIPPED: "➖ n/a",
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

# Short names for the phase badge in the header.
_PHASE_SHORT = {
    ReleasePhase.NOTES: "1/6 notes",
    ReleasePhase.CANDIDATE: "2/6 candidate CI",
    ReleasePhase.QUALIFICATION: "3/6 qualification",
    ReleasePhase.READY: "4/6 ready to publish",
    ReleasePhase.PUBLISHED: "5/6 public outputs",
    ReleasePhase.BUNDLE_HELM: "6/6 bundle & helm",
    ReleasePhase.COMPLETE: "complete",
}


def identity_marker(branch: str) -> str:
    """The marker that identifies the active release issue for *branch*."""
    return f"<!-- {MARKER_NAMESPACE}:{branch} -->"


def adopt_marker(sha: str) -> str:
    """The comment marker recording an owner's adoption of branch head *sha*."""
    return f"<!-- {MARKER_NAMESPACE}:adopt:{sha} -->"


def render_title(branch: str, version: str, stage: str) -> str:
    """Issue title. Falls back to the branch while no notes PR pins a version."""
    if version:
        suffix = version if stage == "ga" else f"{version}-{stage}"
        return f"Release {suffix}"
    return f"Next release on {branch}"


def render_body(status: ReleaseStatus) -> str:
    """Render the full issue body from recomputed state.

    Deterministic for a given status, so reconciliation can compare the
    rendered body against the current one and skip no-op edits. Layout
    mirrors the release-notes PR body: an alert callout stating exactly
    what happens next, a progress checklist, and evidence tables.
    """
    done = _phases_done(status)
    lines: list[str] = [
        identity_marker(status.branch),
        "",
        '<div align="center">',
        "",
        f"## Valkey {_display_tag(status) or '(version pending)'}",
        "",
        _badge_row(status),
        "",
        f"`{status.repo}` · release line `{status.branch}`",
        "",
        f"{_progress_bar(status, done)}",
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
        "| | |",
        "|---|---|",
        f"| Version | {_code(status.version) if status.version else '_awaiting release-notes PR_'} |",
        f"| Stage | {_code(status.stage) if status.stage else '_awaiting release-notes PR_'} |",
        f"| Release-notes PR | {_notes_pr_cell(status)} |",
        f"| Candidate SHA | {_candidate_cell(status)} |",
        f"| Qualification | {_qualification_cell(status)} |",
        f"| Release | {_release_cell(status)} |",
    ]

    if status.checks:
        lines += [
            "",
            f"### Required checks on `{_short(status.candidate.sha)}`",
            "",
            "| Check | Result |",
            "|---|---|",
        ]
        for check in status.checks:
            link = f" ([run]({check.url}))" if check.url else ""
            lines.append(f"| `{check.name}` | {_CHECK_STATE_DISPLAY[check.state]}{link} |")

    if status.outputs:
        lines += [
            "",
            "### Public outputs",
            "",
            "| Output | Status | Detail |",
            "|---|---|---|",
        ]
        for output in status.outputs:
            link = f" ([evidence]({output.url}))" if output.url else ""
            lines.append(
                f"| **{output.name}** | {_OUTPUT_STATE_DISPLAY[output.state]} | "
                f"{output.detail}{link} |"
            )

    lines += [
        "",
        "---",
        "",
        "*Maintained by the release controller: state is recomputed from "
        "GitHub every reconciliation pass, so edits here have no effect and "
        "are overwritten. Failures are raised as a comment mentioning the "
        "release team — once per distinct failure state.*",
    ]
    return "\n".join(lines) + "\n"


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
    stage_badge = _badge("stage", status.stage or "pending", "8250df")
    if status.phase is ReleasePhase.COMPLETE:
        phase_color = "1a7f37"
    elif _has_failures(status):
        phase_color = "cf222e"
    elif status.ready:
        phase_color = "1a7f37"
    else:
        phase_color = "d29922"
    phase_badge = _badge("phase", _PHASE_SHORT[status.phase], phase_color)
    return f"{version_badge} {stage_badge} {phase_badge}"


def _progress_bar(status: ReleaseStatus, done: set[ReleasePhase]) -> str:
    """Six-segment bar: 🟩 done, 🟦 current (🟥 when failing), ⬜ ahead."""
    current_block = "🟥" if _has_failures(status) else "🟦"
    blocks = "".join(
        "🟩" if phase in done else (current_block if phase is status.phase else "⬜")
        for phase in _PHASE_ORDER
    )
    if status.phase is ReleasePhase.COMPLETE:
        return f"{blocks} **Complete**"
    return f"{blocks} **{_PHASE_TITLES[status.phase]}**"


def _has_failures(status: ReleaseStatus) -> bool:
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
                f"{names}.** Details in the Public outputs table; the release "
                f"team has been notified. Everything else continues to be "
                f"observed hourly.",
            ]
        return [
            "> [!NOTE]",
            "> **Published.** Downstream outputs are being verified against "
            "their canonical public locations (registries, downloads, merged "
            "PRs) — not just workflow success. No action needed unless an "
            "output turns failed.",
        ]
    if status.ready:
        return [
            "> [!IMPORTANT]",
            "> **Ready to publish.** Every pre-publication gate passed on the "
            "exact candidate SHA. A release owner dispatches the **Release "
            "Publish** workflow; publication waits for environment approval, "
            "revalidates everything, and is the point of no return (upstream's "
            "tag ruleset forbids moving or deleting the created tag).",
        ]
    blocker_lines = [f"> - {blocker}" for blocker in status.blockers] or ["> - (none recorded)"]
    return ["> [!WARNING]", "> **Not ready — blocked on:**", *blocker_lines]


def _phases_done(status: ReleaseStatus) -> set[ReleasePhase]:
    if status.phase is ReleasePhase.COMPLETE:
        return set(_PHASE_ORDER)
    return set(_PHASE_ORDER[: _PHASE_ORDER.index(status.phase)])


def _display_tag(status: ReleaseStatus) -> str:
    if not status.version:
        return ""
    return status.version if status.stage == "ga" else f"{status.version}-{status.stage}"


def _code(value: str) -> str:
    return f"`{value}`"


def _short(sha: str) -> str:
    return sha[:12] if sha else ""


def _notes_pr_cell(status: ReleaseStatus) -> str:
    if not status.notes_pr_number:
        return "_none found_"
    state = "merged" if status.notes_pr_merged else "open (not merged)"
    return f"[#{status.notes_pr_number}]({status.notes_pr_url}) — {state}"


def _candidate_cell(status: ReleaseStatus) -> str:
    candidate = status.candidate
    if candidate.state is CandidateState.NONE:
        return "_none (recorded when the release-notes PR merges)_"
    descriptions = {
        CandidateState.CURRENT: "current branch head",
        CandidateState.ADOPTED: "adopted branch head (owner-acknowledged)",
        CandidateState.INVALIDATED: (
            "**invalidated** — the branch moved; an authorized owner must adopt "
            "the exact new head before qualification continues"
        ),
    }
    return f"`{candidate.sha}` — {descriptions[candidate.state]}"


def _qualification_cell(status: ReleaseStatus) -> str:
    qualification = status.qualification
    if qualification.run_id == 0:
        if status.published:
            # Post-publication the gate is history (publication revalidated
            # it); "no run yet" would misread as a missing prerequisite.
            return "_gated before publication; not re-evaluated after_"
        return "_no run for the candidate SHA yet_"
    link = f"[run {qualification.run_id}]({qualification.url})"
    if qualification.pending:
        return f"⏳ {link} in progress"
    if qualification.passed:
        return f"✅ {link} passed"
    failed = ", ".join(qualification.failed_jobs[:3])
    return f"❌ {link} failed ({failed})"


def _release_cell(status: ReleaseStatus) -> str:
    if not status.published:
        return "_not published_"
    return f"[published]({status.release_url})"


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
    Trust covers the static bot identities plus the *currently authenticated*
    identity when it is a user (fork runs authenticate as the fork owner's
    PAT, and markers the controller wrote must remain readable to it).
    """
    trusted = set(TRUSTED_MARKER_AUTHORS)
    if gh is not None:
        login = controller_login(gh)
        if login:
            trusted.add(login)
    comments = retry_github_call(
        lambda: list(issue.get_comments()),
        retries=2, description=f"list comments on issue #{issue.number}",
    )
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
    )


def ensure_tracker_labels(repo: Any, branch: str, tracker_label: str) -> None:
    """Create the tracker and branch identity labels when absent.

    GitHub's auto-creation of labels on issue writes is not a documented
    guarantee; creating them explicitly (mirroring the backport module's
    ensure-label pattern) removes the doubt.
    """
    _ensure_label(repo, tracker_label, "0e8a16",
                  "Release tracking issue maintained by the release controller")
    _ensure_label(repo, branch_label(branch), "1d76db",
                  f"Active release on the {branch} line")


def _ensure_label(repo: Any, name: str, color: str, description: str) -> None:
    try:
        retry_github_call(
            lambda: repo.get_label(name),
            retries=2, description=f"get label {name}",
        )
    except GithubException as exc:
        if exc.status != 404:
            raise
        retry_github_call(
            lambda: repo.create_label(name=name, color=color, description=description),
            retries=2, description=f"create label {name}",
        )
