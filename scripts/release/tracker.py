"""A small, non-authoritative release dashboard and progress watcher.

The issue is presentation only. Every transition is derived again from live
PR, branch, workflow, and release state; issue text never authorizes a write.
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from github import Auth, Github
from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call

logger = logging.getLogger(__name__)

TRACKING_LABEL = "release-tracking"
_TRACKER_PREFIX = "<!-- valkey-release-tracker:v1 "
_STATUS_MARKER = "<!-- valkey-release-tracker:status -->"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_REFRESHED_RE = re.compile(r"Last refreshed \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC")
_PHASES = (
    "Prepare",
    "Review notes",
    "Validate & qualify",
    "Publish",
    "Production",
    "Follow-up",
)


@dataclass(frozen=True)
class Tracker:
    repo: str
    branch: str
    version: str
    stage: str
    tag: str
    prep_branch: str
    prepare_run_id: int

    def marker(self) -> str:
        payload = json.dumps(self.__dict__, sort_keys=True, separators=(",", ":"))
        return f"{_TRACKER_PREFIX}{payload} -->"


def ensure_tracker(gh: Any, tracker: Tracker, *, agent_repo: str) -> Any:
    """Create or reuse the one open dashboard for *tracker.tag*."""
    _validate_tracker(tracker)
    repo = _repo(gh, tracker.repo)
    label = _ensure_label(repo)
    issue = None
    for candidate in itertools.islice(repo.get_issues(state="all", labels=[label]), 200):
        existing = parse_tracker(candidate.body or "")
        if _is_bot_owned(candidate) and existing is not None and existing.tag == tracker.tag:
            issue = candidate
            break

    if issue is None:
        issue = retry_github_call(
            lambda: repo.create_issue(
                title=f"Release {tracker.tag}",
                body=_issue_body(tracker, agent_repo),
                labels=[label],
            ),
            retries=2,
            description=f"create release tracker for {tracker.tag}",
        )
    else:
        if issue.state != "open":
            retry_github_call(
                lambda: issue.edit(state="open"),
                retries=2,
                description=f"reopen release tracker #{issue.number}",
            )
        # A rerun may have a newer preparation run. Keep the stable dashboard
        # URL while refreshing only bot-owned metadata and instructions.
        retry_github_call(
            lambda: issue.edit(
                title=f"Release {tracker.tag}",
                body=_issue_body(tracker, agent_repo),
            ),
            retries=2,
            description=f"refresh release tracker #{issue.number}",
        )

    status, _ = _render_status(
        tracker,
        prepare_run=None,
        pr=None,
        branch_head="",
        candidate_sha="",
        publish_run=None,
        release=None,
        production_run=None,
        agent_repo=agent_repo,
        dispatched=False,
    )
    _upsert_status(issue, status)
    return issue


def parse_tracker(body: str) -> Tracker | None:
    start = body.find(_TRACKER_PREFIX)
    if start < 0:
        return None
    end = body.find(" -->", start)
    if end < 0:
        return None
    try:
        raw = json.loads(body[start + len(_TRACKER_PREFIX) : end])
        tracker = Tracker(**raw)
        _validate_tracker(tracker)
        return tracker
    except (TypeError, ValueError):
        return None


def sync_trackers(
    target_gh: Any,
    agent_gh: Any,
    automation_gh: Any,
    *,
    target_repo: str,
    agent_repo: str,
    automation_repo: str,
    dispatch: bool = True,
) -> list[str]:
    """Refresh every open tracker and advance a merged notes PR once."""
    repo = _repo(target_gh, target_repo)
    agent = _repo(agent_gh, agent_repo)
    automation = _repo(automation_gh, automation_repo)
    publish_workflow = retry_github_call(
        lambda: agent.get_workflow("release-publish.yml"),
        retries=2,
        description="get release publication workflow",
    )
    label = _ensure_label(repo)
    results: list[str] = []
    for issue in itertools.islice(repo.get_issues(state="open", labels=[label]), 100):
        if not _is_bot_owned(issue):
            logger.warning("Ignoring non-bot release tracker lookalike #%s", issue.number)
            continue
        tracker = parse_tracker(issue.body or "")
        if tracker is None or tracker.repo != target_repo:
            continue
        try:
            result = _sync_one(
                issue,
                tracker,
                repo,
                agent,
                automation,
                publish_workflow,
                agent_repo=agent_repo,
                dispatch=dispatch,
            )
        except Exception as exc:
            logger.exception("Could not refresh release tracker #%s", issue.number)
            detail = re.sub(r"\s+", " ", str(exc)).replace("`", "'")[:500]
            _upsert_status(
                issue,
                '<div align="center">\n\n## Release dashboard needs attention\n\n'
                f"{_badge('tracker', 'refresh failed', 'cf222e')}\n\n</div>\n\n"
                "> [!CAUTION]\n"
                f"> **Tracker refresh failed:** `{type(exc).__name__}: {detail}`\n>\n"
                "> No release action was authorized by this failure. Open the progress workflow logs and rerun it.",
            )
            result = f"#{issue.number}: refresh failed"
        results.append(result)
    return results


def _sync_one(
    issue: Any,
    tracker: Tracker,
    repo: Any,
    agent: Any,
    automation: Any,
    publish_workflow: Any,
    *,
    agent_repo: str,
    dispatch: bool,
) -> str:
    pr = _find_prep_pr(repo, tracker)
    release = _find_release(repo, tracker.tag)
    branch_head = _branch_head(repo, tracker.branch)
    prepare_run = retry_github_call(
        lambda: agent.get_workflow_run(tracker.prepare_run_id),
        retries=2,
        description=f"get preparation run {tracker.prepare_run_id}",
    )
    candidate_sha = ""
    if pr is not None and getattr(pr, "merged", False):
        candidate_sha = (getattr(pr, "merge_commit_sha", "") or "").lower()

    publish_title = f"Publish release on {tracker.branch} @ {candidate_sha}" if candidate_sha else ""
    publish_run = _find_run(publish_workflow, publish_title) if publish_title else None

    dispatched = False
    if (
        dispatch
        and release is None
        and candidate_sha
        and _SHA_RE.fullmatch(candidate_sha)
        and branch_head == candidate_sha
        and publish_run is None
    ):
        accepted = retry_github_call(
            lambda: publish_workflow.create_dispatch(
                "main",
                inputs={"branch": tracker.branch, "candidate_sha": candidate_sha},
            ),
            retries=1,
            description=f"dispatch publication for {tracker.tag}",
        )
        if accepted is False:
            raise RuntimeError(f"GitHub refused publication dispatch for {tracker.tag}")
        dispatched = True

    production_run = _find_production_run(automation, tracker.tag) if release else None
    body, summary = _render_status(
        tracker,
        prepare_run=prepare_run,
        pr=pr,
        branch_head=branch_head,
        candidate_sha=candidate_sha,
        publish_run=publish_run,
        release=release,
        production_run=production_run,
        agent_repo=agent_repo,
        dispatched=dispatched,
    )
    _upsert_status(issue, body)
    return f"#{issue.number}: {summary}"


def _render_status(
    tracker: Tracker,
    *,
    prepare_run: Any | None,
    pr: Any | None,
    branch_head: str,
    candidate_sha: str,
    publish_run: Any | None,
    release: Any | None,
    production_run: Any | None,
    agent_repo: str,
    dispatched: bool,
) -> tuple[str, str]:
    prep_url = f"https://github.com/{agent_repo}/actions/runs/{tracker.prepare_run_id}"
    prep_cell = _run_cell(prepare_run) if prepare_run is not None else f"[run]({prep_url})"
    candidate_cell = "⏳ Waiting for preparation PR"
    current = "Generating the release-notes PR."
    next_action = "Wait for preparation to finish; rerun **Prepare Release** if it fails."
    summary = "preparing notes"

    if prepare_run is not None and prepare_run.status == "completed":
        if prepare_run.conclusion == "success":
            current = "Release preparation completed; waiting for the release-notes PR."
            next_action = "Open the preparation run if the PR does not appear shortly."
            summary = "preparation completed"
        else:
            current = "Release preparation failed."
            next_action = "Open the failed preparation run, fix the failure, and rerun **Prepare Release**."
            summary = "preparation failed"

    if pr is not None:
        pr_link = f"[#{pr.number}]({pr.html_url})"
        if getattr(pr, "merged", False):
            candidate_cell = f"✅ {pr_link} merged at `{candidate_sha[:12] or 'unknown'}`"
            current = "The release candidate is bound to the merged preparation PR."
            next_action = "Publication will start automatically."
            summary = "notes PR merged"
        elif pr.state == "closed":
            candidate_cell = f"❌ {pr_link} closed without merge"
            current = "Preparation stopped because the release PR was closed."
            next_action = "Rerun **Prepare Release** to recreate or refresh the PR."
            summary = "notes PR closed"
        elif getattr(pr, "draft", False):
            candidate_cell = f"⚠️ {pr_link} is draft"
            current = "The release PR is held for maintainer review."
            next_action = "Resolve the warnings in the PR, rerun Prepare if needed, then mark it ready and merge."
            summary = "notes PR held"
        else:
            candidate_cell = f"⏳ {pr_link} awaiting review and merge"
            current = "The release-notes PR is ready for maintainer review."
            next_action = "Review and merge the release-notes PR."
            summary = "waiting for notes PR merge"

    publish_cell = "— Not started"
    if candidate_sha and branch_head != candidate_sha:
        publish_cell = f"🛑 Branch moved to `{branch_head[:12] or 'unknown'}` after candidate `{candidate_sha[:12]}`"
        current = "Automatic publication is blocked because the reviewed candidate is no longer branch HEAD."
        next_action = "Rerun **Prepare Release** so the new branch state is reviewed in a fresh PR."
        summary = "candidate invalidated by branch movement"
    elif dispatched:
        publish_cell = "⏳ Publication workflow dispatched"
        current = "Exact-SHA validation and qualification are starting."
        next_action = "Open the publication run when it appears; approve the `release` gate after reviewing its plan."
        summary = "publication dispatched"
    elif publish_run is not None:
        publish_cell = _run_cell(publish_run)
        if publish_run.status == "completed" and publish_run.conclusion != "success":
            current = "The publication workflow failed."
            next_action = "Open the failed run, fix or rerun it, and keep the candidate SHA unchanged."
            summary = "publication failed"
        elif publish_run.status == "completed":
            current = "The publication workflow completed."
            next_action = "Wait for the GitHub release event to start production automation."
            summary = "publication completed"
        elif publish_run.status in {"waiting", "pending"}:
            current = "Qualification passed or is finishing; publication is waiting at the protected release gate."
            next_action = "Review the rendered plan and approve the `release` environment."
            summary = "waiting for release approval"
        else:
            current = "Candidate validation and no-publish qualification are running."
            next_action = "Open the publication run for the exact failing or running job."
            summary = "validating and qualifying"

    release_cell = "— Not published"
    if release is not None:
        release_cell = f"✅ [GitHub release]({release.html_url})"
        current = "The GitHub release is published; production automation is next."
        next_action = "Wait for the production run, then approve `release-publish`."
        summary = "release published"

    production_cell = "— Not started"
    if production_run is not None:
        production_cell = _run_cell(production_run)
        if production_run.status == "completed" and production_run.conclusion == "success":
            current = "Production automation completed successfully."
            next_action = (
                "Review and merge the downstream container/Helm/docs/website PRs, confirm Bundle, "
                "then close this tracker."
            )
            summary = "production automation completed"
        elif production_run.status == "completed":
            current = "Production automation failed."
            next_action = "Open the production run, fix or rerun the failed job, and leave this tracker open."
            summary = "production automation failed"
        elif production_run.status in {"waiting", "pending"}:
            current = "Production automation is waiting at its protected environment."
            next_action = "Review and approve the `release-publish` environment."
            summary = "waiting for production approval"
        else:
            current = "Production packaging and downstream updates are running."
            next_action = "Open the production run for live job-level status."
            summary = "production automation running"

    refreshed = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    phase, failed = _presentation_state(summary)
    callout = "CAUTION" if failed else ("IMPORTANT" if "approval" in summary else "NOTE")
    phase_color = "cf222e" if failed else ("1a7f37" if phase == len(_PHASES) else "0969da")
    candidate_badge = candidate_sha[:12] if candidate_sha else "pending"
    return (
        "\n".join(
            (
                '<div align="center">',
                "",
                f"## Valkey {tracker.tag} release status",
                "",
                " ".join(
                    (
                        _badge("phase", f"{phase}/{len(_PHASES)} {_PHASES[phase - 1]}", phase_color),
                        _badge("candidate", candidate_badge, "57606a"),
                        _badge("stage", tracker.stage.upper(), "8250df"),
                    )
                ),
                "",
                _progress_bar(phase, failed),
                "",
                "</div>",
                "",
                f"> [!{callout}]",
                f"> **{current}**",
                ">",
                f"> **Next maintainer action:** {next_action}",
                "",
                "## Pipeline",
                "",
                *_phase_checklist(phase),
                "",
                "## Current release status",
                "",
                "| Phase | Status |",
                "|---|---|",
                f"| Prepare workflow | {prep_cell} |",
                f"| Release-notes PR | {candidate_cell} |",
                f"| Validate, qualify, approve, publish | {publish_cell} |",
                f"| GitHub release | {release_cell} |",
                f"| Production automation | {production_cell} |",
                "",
                "<details><summary>Security and recovery model</summary>",
                "",
                "This dashboard is a projection of live GitHub state. It cannot choose a candidate, authorize publication, or skip either protected approval. The canonical merged preparation PR, exact branch SHA, required checks, qualification run, live approver membership, and environment gates remain authoritative.",
                "",
                "</details>",
                "",
                f"<sub>Last refreshed {refreshed} · [Preparation run]({prep_url}) · Dashboard only; not release authority.</sub>",
            )
        ),
        summary,
    )


def _run_cell(run: Any) -> str:
    link = f"[run]({run.html_url})"
    if run.status != "completed":
        return f"⏳ {link} `{run.status}`"
    if run.conclusion == "success":
        return f"✅ {link} succeeded"
    return f"❌ {link} `{run.conclusion or 'failed'}`"


def _issue_body(tracker: Tracker, agent_repo: str) -> str:
    repo_url = f"https://github.com/{tracker.repo}"
    trackers_url = f"{repo_url}/issues?q=is%3Aissue+is%3Aopen+label%3A{TRACKING_LABEL}"
    return "\n".join(
        (
            tracker.marker(),
            '<div align="center">',
            "",
            f"# Valkey {tracker.tag}",
            "",
            " ".join(
                (
                    _badge("release", tracker.tag, "0969da"),
                    _badge("stage", tracker.stage.upper(), "8250df"),
                    _badge("line", tracker.branch, "57606a"),
                )
            ),
            "",
            f"[`{tracker.repo}`]({repo_url}) · [`{tracker.branch}`]({repo_url}/tree/{tracker.branch}) · [All active releases]({trackers_url})",
            "",
            "</div>",
            "",
            "> [!NOTE]",
            "> **This is the maintainer control center for the release.** The bot-owned status comment below shows the live phase, failure evidence, and exact next action.",
            ">",
            "> Editing this issue never authorizes or advances the release.",
            "",
            "## Release path",
            "",
            "`Prepare` → `Review notes` → `Validate & qualify` → `Approve release` → `Approve production` → `Follow-up`",
            "",
            "After the canonical release-notes PR merges, qualification starts automatically. The two approval gates remain deliberate human decisions.",
            "",
            "## Release identity",
            "",
            "| Field | Value |",
            "|---|---|",
            f"| Release line | [`{tracker.branch}`]({repo_url}/tree/{tracker.branch}) |",
            f"| Preparation branch | `{tracker.prep_branch}` |",
            f"| Preparation run | [Open run](https://github.com/{agent_repo}/actions/runs/{tracker.prepare_run_id}) |",
            "",
            "## Maintainer checklist",
            "",
            "- [ ] Review and merge the release-notes PR",
            "- [ ] Approve the protected `release` publication gate",
            "- [ ] Approve the protected `release-publish` production gate",
            "- [ ] Review and merge generated downstream PRs (container, docs/website when applicable, Helm for GA)",
            "- [ ] Confirm the Bundle update for supported release lines",
            "- [ ] Close this issue after downstream publication is complete",
        )
    )


def _badge(label: str, message: str, color: str) -> str:
    def escape(value: str) -> str:
        return value.replace("-", "--").replace("_", "__").replace(" ", "%20").replace("/", "%2F").replace("&", "%26")

    alt = f"{label}: {message}"
    return f"![{alt}](https://img.shields.io/badge/{escape(label)}-{escape(message)}-{color}?style=flat-square)"


def _presentation_state(summary: str) -> tuple[int, bool]:
    failed = any(word in summary for word in ("failed", "invalidated", "closed"))
    if summary in {"preparing notes", "preparation completed", "preparation failed"}:
        return 1, failed
    if summary in {"notes PR held", "waiting for notes PR merge", "notes PR closed"}:
        return 2, failed
    if summary in {
        "notes PR merged",
        "candidate invalidated by branch movement",
        "publication dispatched",
        "publication failed",
        "validating and qualifying",
    }:
        return 3, failed
    if summary in {"waiting for release approval", "publication completed"}:
        return 4, failed
    if summary in {
        "release published",
        "waiting for production approval",
        "production automation running",
        "production automation failed",
    }:
        return 5, failed
    if summary == "production automation completed":
        return 6, failed
    return 1, True


def _progress_bar(phase: int, failed: bool) -> str:
    current = "🟥" if failed else "🟦"
    blocks = "".join(
        "🟩" if index < phase else current if index == phase else "⬜" for index in range(1, len(_PHASES) + 1)
    )
    label = _PHASES[phase - 1]
    suffix = " · needs attention" if failed else ""
    return f"{blocks} **{label}{suffix}**"


def _phase_checklist(phase: int) -> tuple[str, ...]:
    lines = []
    for index, label in enumerate(_PHASES, start=1):
        checked = "x" if index < phase else " "
        rendered = f"**{label}**" if index == phase else label
        lines.append(f"- [{checked}] {rendered}")
    return tuple(lines)


def _find_prep_pr(repo: Any, tracker: Tracker) -> Any | None:
    pulls = retry_github_call(
        lambda: list(
            repo.get_pulls(
                state="all",
                sort="updated",
                direction="desc",
                head=f"{tracker.repo.split('/', 1)[0]}:{tracker.prep_branch}",
                base=tracker.branch,
            )
        ),
        retries=2,
        description=f"find preparation PR for {tracker.tag}",
    )
    if pulls:
        return pulls[0]

    # GitHub may stop matching the `head=owner:branch` filter after the
    # preparation branch is deleted. Closed PR metadata still retains its
    # head ref, so fall back to the release line without losing the reviewed
    # merge identity from the dashboard or automatic transition.
    closed = retry_github_call(
        lambda: repo.get_pulls(
            state="closed",
            sort="updated",
            direction="desc",
            base=tracker.branch,
        ),
        retries=2,
        description=f"find closed preparation PR fallback for {tracker.tag}",
    )
    return next(
        (
            pr
            for pr in itertools.islice(closed, 500)
            if getattr(getattr(pr, "head", None), "ref", "") == tracker.prep_branch
            and getattr(getattr(getattr(pr, "head", None), "repo", None), "full_name", "") == tracker.repo
        ),
        None,
    )


def _find_release(repo: Any, tag: str) -> Any | None:
    releases = retry_github_call(lambda: repo.get_releases(), retries=2, description="list releases")
    return next((release for release in itertools.islice(releases, 100) if release.tag_name == tag), None)


def _find_run(workflow: Any, title: str) -> Any | None:
    if not title:
        return None
    runs = retry_github_call(lambda: workflow.get_runs(), retries=2, description=f"list {workflow.name} runs")
    for run in itertools.islice(runs, 500):
        if (getattr(run, "display_title", "") or "") == title:
            return run
    return None


def _find_production_run(repo: Any, tag: str) -> Any | None:
    workflow = retry_github_call(
        lambda: repo.get_workflow("build-release.yml"),
        retries=2,
        description="get production workflow",
    )
    return _find_run(workflow, f"Build Release {tag} (prod)")


def _branch_head(repo: Any, branch: str) -> str:
    return retry_github_call(
        lambda: repo.get_branch(branch).commit.sha,
        retries=2,
        description=f"read {branch} head",
    )


def _ensure_label(repo: Any) -> Any:
    try:
        return retry_github_call(
            lambda: repo.get_label(TRACKING_LABEL),
            retries=2,
            description=f"get {TRACKING_LABEL} label",
        )
    except GithubException as exc:
        if exc.status != 404:
            raise
    return retry_github_call(
        lambda: repo.create_label(
            TRACKING_LABEL,
            "1d76db",
            "Tracks an in-progress Valkey release",
        ),
        retries=2,
        description=f"create {TRACKING_LABEL} label",
    )


def _upsert_status(issue: Any, body: str) -> None:
    rendered = f"{_STATUS_MARKER}\n{body.rstrip()}\n"
    comments = retry_github_call(
        lambda: list(issue.get_comments()),
        retries=2,
        description=f"list tracker #{issue.number} comments",
    )
    bot_login = getattr(getattr(issue, "user", None), "login", "")
    existing = next(
        (
            comment
            for comment in comments
            if _STATUS_MARKER in (comment.body or "")
            and getattr(getattr(comment, "user", None), "login", "") == bot_login
        ),
        None,
    )
    if existing is None:
        retry_github_call(
            lambda: issue.create_comment(rendered),
            retries=2,
            description=f"create tracker #{issue.number} status",
        )
    else:
        if _REFRESHED_RE.sub("Last refreshed <timestamp> UTC", existing.body or "") == _REFRESHED_RE.sub(
            "Last refreshed <timestamp> UTC", rendered
        ):
            return
        retry_github_call(
            lambda: existing.edit(rendered),
            retries=2,
            description=f"update tracker #{issue.number} status",
        )


def _repo(gh: Any, name: str) -> Any:
    return retry_github_call(lambda: gh.get_repo(name), retries=2, description=f"get {name}")


def _is_bot_owned(issue: Any) -> bool:
    login = (getattr(getattr(issue, "user", None), "login", "") or "").casefold()
    return login.endswith("[bot]")


def _validate_tracker(tracker: Tracker) -> None:
    if tracker.repo.count("/") != 1:
        raise ValueError("tracker repo must be owner/name")
    if not re.fullmatch(r"[0-9]+\.[0-9]+", tracker.branch):
        raise ValueError("tracker branch must be MAJOR.MINOR")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", tracker.version):
        raise ValueError("tracker version must be MAJOR.MINOR.PATCH")
    if not re.fullmatch(r"ga|rc[1-9][0-9]*", tracker.stage):
        raise ValueError("tracker stage must be ga or rcN")
    expected_tag = tracker.version if tracker.stage == "ga" else f"{tracker.version}-{tracker.stage}"
    if tracker.tag != expected_tag:
        raise ValueError("tracker tag does not match version and stage")
    if tracker.prep_branch != f"agent/release-cut/{tracker.version}-{tracker.stage}":
        raise ValueError("tracker preparation branch is not canonical")
    if not isinstance(tracker.prepare_run_id, int) or tracker.prepare_run_id <= 0:
        raise ValueError("tracker prepare run id must be positive")


def _write_outputs(values: dict[str, str]) -> None:
    path = os.environ.get("GITHUB_OUTPUT", "")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    ensure = sub.add_parser("ensure")
    for name in ("repo", "branch", "version", "stage", "tag", "prep-branch", "agent-repo"):
        ensure.add_argument(f"--{name}", required=True)
    ensure.add_argument("--prepare-run-id", required=True, type=int)

    sync = sub.add_parser("sync")
    sync.add_argument("--target-repo", default="valkey-io/valkey")
    sync.add_argument("--agent-repo", default="valkey-io/valkey-ci-agent")
    sync.add_argument("--automation-repo", default="valkey-io/valkey-release-automation")
    sync.add_argument("--no-dispatch", action="store_true")
    args = parser.parse_args(argv)

    target_token = os.environ.get("TARGET_GITHUB_TOKEN", "")
    agent_token = os.environ.get("AGENT_GITHUB_TOKEN", "") or os.environ.get("GITHUB_TOKEN", "")
    automation_token = os.environ.get("AUTOMATION_GITHUB_TOKEN", "")
    if not target_token:
        parser.error("TARGET_GITHUB_TOKEN is required")
    target_gh = Github(auth=Auth.Token(target_token))
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command == "ensure":
        tracker = Tracker(
            repo=args.repo,
            branch=args.branch,
            version=args.version,
            stage=args.stage,
            tag=args.tag,
            prep_branch=args.prep_branch,
            prepare_run_id=args.prepare_run_id,
        )
        issue = ensure_tracker(target_gh, tracker, agent_repo=args.agent_repo)
        _write_outputs({"issue_number": str(issue.number), "issue_url": issue.html_url})
        print(issue.html_url)
        return 0

    if not agent_token:
        parser.error("AGENT_GITHUB_TOKEN is required for sync")
    if not automation_token:
        parser.error("AUTOMATION_GITHUB_TOKEN is required for sync")
    agent_gh = Github(auth=Auth.Token(agent_token))
    automation_gh = Github(auth=Auth.Token(automation_token))
    for result in sync_trackers(
        target_gh,
        agent_gh,
        automation_gh,
        target_repo=args.target_repo,
        agent_repo=args.agent_repo,
        automation_repo=args.automation_repo,
        dispatch=not args.no_dispatch,
    ):
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
