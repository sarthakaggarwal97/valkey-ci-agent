"""Fix one issue a committer labelled ``needs-agent-fix``.

The label is the queue. This module is the front door: it checks that the
label was applied by someone with write access, that nobody is working on the
issue already, pins the issue as it was when labelled (title, body, and the
committers' comments up to then), and plans a fix on the default branch. The
fix runs like every other (:mod:`scripts.ci_fix.run`): the bot first writes a
test that fails because of the bug, then a change that makes it pass
(:mod:`scripts.factory.prepare`). The result is a draft pull request, or a
comment on the issue while the bot earns its pull requests (``FACTORY_STEP``),
and a status comment on the issue either way. The bot never merges.

The dispatcher (:mod:`scripts.factory.dispatch`) starts one run per labelled
issue worth an attempt; this module rechecks everything itself, so a run that
is started twice, or by hand, does the same thing once.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import replace
from datetime import datetime
from typing import Any, Callable

from scripts.ci_fix.comment import candidate_context, candidate_patch, check_items, sentence
from scripts.ci_fix.issue_state import has_open_linked_pr
from scripts.ci_fix.models import (
    MAX_FAILED_ATTEMPTS,
    Failure,
    FixCandidate,
    FixPlan,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    IssueBrief,
    IssueComment,
    IssueKind,
    IssuePublication,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.publish import reconcile_draft_pr
from scripts.common.github_client import retry_github_call
from scripts.common.text_utils import compact_text

logger = logging.getLogger(__name__)

LABEL = "needs-agent-fix"
BRANCH_PREFIX = "agent/factory/"
# The rollout step, a repository variable: ``off`` (the default: the dispatcher
# starts nothing), ``comment`` (the fix is posted on the issue), ``draft-pr``.
STEP_ENV = "FACTORY_STEP"
STEP_OFF = "off"
# Who may ask: the permission the labeller must have on the repository.
_COMMITTER_PERMISSIONS = frozenset({"admin", "write"})
# What of an issue the model is given.
_BODY_CHARS = 30_000
_COMMENT_CHARS = 8_000
_MAX_COMMENTS = 30
# Labels that make an issue a feature request, and the one that makes it a bug whatever else it has.
_FEATURE_LABELS = frozenset({"enhancement", "feature", "feature-request"})
_BUG_LABEL = "bug"
# A crash report: Valkey's crash log, as a reporter pastes it, or a title that says so.
_CRASH_BODY_RE = re.compile(r"BUG REPORT START|crashed by signal|ASSERTION FAILED|\bSIG(?:SEGV|ABRT|BUS)\b")
_CRASH_TITLE_RE = re.compile(r"\b(?:crash(?:es|ed)?|segfault|assert(?:ion)?|abort(?:s|ed)?)\b", re.IGNORECASE)

_RESULT_MARKER = "<!-- valkey-ci-agent:factory label={label} outcome={outcome} -->"
_RESULT_MARKER_RE = re.compile(r"<!-- valkey-ci-agent:factory label=(?P<label>\d+) outcome=(?P<outcome>[a-z]+) -->")


def publication() -> IssuePublication:
    """How a fix is published, from ``FACTORY_STEP``: a draft PR at that step, else a comment on the issue.

    The step also switches the dispatcher off; a run already started finishes
    as a comment, which pushes nothing.
    """
    value = os.environ.get(STEP_ENV, "").strip().lower()
    if value == IssuePublication.DRAFT_PR.value:
        return IssuePublication.DRAFT_PR
    if value in ("", STEP_OFF, IssuePublication.COMMENT.value):
        return IssuePublication.COMMENT
    raise ValueError(f"{STEP_ENV} must be {STEP_OFF}, comment or draft-pr, not {value!r}")


def plan_request(gh: Any, artifact_client: Any, request: dict[str, Any], *, bot_login: str) -> FixPlan | dict[str, Any]:
    """The fix plan for ``request`` (``{"repo": ..., "issue": N, "push_repo": ...}``), or the final result."""
    return prepare_issue(
        gh, str(request.get("repo") or ""), issue_number=int(request.get("issue") or 0), bot_login=bot_login,
        push_repo=str(request.get("push_repo") or ""),
    )


def prepare_issue(
    gh: Any, repo_full_name: str, *, issue_number: int, bot_login: str, push_repo: str = "",
) -> FixPlan | dict[str, Any]:
    """Validate one labelled issue and build its fix task, or the final result.

    A final result is a skip, a refusal (the label removed, with the reason on
    the issue), or an earlier attempt's draft PR recovered. ``push_repo`` is
    where the branch goes: a fork of the repository, or the repository itself.
    """
    if issue_number <= 0:
        raise ValueError("an explicit positive issue number is required")
    repo = retry_github_call(lambda: gh.get_repo(repo_full_name), retries=2, description=f"get {repo_full_name}")
    issue = retry_github_call(lambda: repo.get_issue(issue_number), retries=2, description=f"get issue #{issue_number}")

    def skipped(reason: str) -> dict[str, Any]:
        return {"repo": repo_full_name, "issue": issue_number, "action": "skipped", "reason": reason}

    if getattr(issue, "pull_request", None) is not None:
        return skipped(f"#{issue_number} is a pull request")
    if str(getattr(issue, "state", "") or "") != "open":
        return skipped(f"issue #{issue_number} is not open")
    labelled = labelling(issue)
    if labelled is None:
        return skipped(f"issue #{issue_number} is not labelled {LABEL}")
    event_id, labeller, labelled_at = labelled
    if not is_committer(repo, labeller):
        return _unlabel(
            repo_full_name, issue, event_id,
            f"the label was applied by @{labeller}, who does not have write access to {repo_full_name}. "
            "A committer applies it.",
        )
    if getattr(issue, "assignees", None):
        names = ", ".join(f"@{getattr(user, 'login', '')}" for user in issue.assignees)
        return _unlabel(repo_full_name, issue, event_id, f"the issue is assigned to {names}, so someone is on it.")
    if has_open_linked_pr(issue):
        return _unlabel(repo_full_name, issue, event_id, "an open pull request already says it fixes the issue.")
    if fix_reported(issue, event_id, bot_login=bot_login):
        return skipped(f"the bot is done with the labelling of issue #{issue_number} ({event_id})")

    try:
        return _plan_labelled(gh, repo, repo_full_name, issue, event_id, labeller, labelled_at, push_repo)
    except Exception as exc:  # noqa: BLE001 - every attempt gets a recorded result
        logger.exception("Planning a fix for issue #%s failed", issue_number)
        # Nothing is posted: the run fails, and the dispatcher counts failed runs.
        return {
            "repo": repo_full_name, "issue": issue_number, "action": ResultKind.FAILED.value,
            "summary": f"The attempt failed while planning: {exc}",
        }


def _plan_labelled(
    gh: Any, repo: Any, repo_full_name: str, issue: Any, event_id: int, labeller: str, labelled_at: datetime,
    push_repo: str,
) -> FixPlan | dict[str, Any]:
    """The fix plan for a validated labelled issue, or an earlier attempt's result recovered."""
    base_branch = str(getattr(repo, "default_branch", "") or "")
    if not base_branch:
        raise RuntimeError(f"{repo_full_name} has no default branch")
    tip = retry_github_call(
        lambda: repo.get_branch(base_branch), retries=2, description=f"get {repo_full_name}:{base_branch}",
    )
    base_sha = str(getattr(getattr(tip, "commit", None), "sha", "") or "")
    brief = read_brief(
        issue, event_id=event_id, labeller=labeller, labelled_at=labelled_at,
        committer=committer_check(repo, known={labeller: True}),
    )
    task = FixTask(
        failure=Failure(
            repo_full_name=repo_full_name,
            run_id=0,
            head_sha=base_sha,
            job_id=0,
            job_name="",
            base_branch=base_branch,
            description=f"Issue #{brief.number}: {compact_text(brief.title, 300)}",
            issue_number=brief.number,
        ),
        target=FixTarget(
            kind=TargetKind.DRAFT_PR,
            repo_full_name=push_repo or repo_full_name,
            base_repo_full_name=repo_full_name,
            branch=f"{BRANCH_PREFIX}issue-{brief.number}-{event_id}",
            expected_sha=base_sha,
        ),
        policy=FixPolicy.GENERAL,
    )
    # A branch an earlier attempt pushed before its PR could be opened.
    reconciled = reconcile_draft_pr(gh, task)
    if reconciled is not None:
        _terminal_comment(issue, brief, reconciled)
        return _result_dict(task, brief, reconciled)
    return FixPlan(front_door=FrontDoor.FACTORY, task=task, brief=brief)


def labelling(issue: Any) -> tuple[int, str, datetime] | None:
    """(event id, actor, time) of the latest application of the label to ``issue``, or None."""
    timeline = retry_github_call(
        lambda: list(issue.get_timeline()), retries=2, description=f"read issue #{issue.number} timeline",
    )
    latest: tuple[int, str, datetime] | None = None
    for event in timeline:
        if str(getattr(event, "event", "") or "") != "labeled":
            continue
        raw = getattr(event, "raw_data", None) or {}
        if str((raw.get("label") or {}).get("name") or "") != LABEL:
            continue
        created = getattr(event, "created_at", None)
        actor = str(getattr(getattr(event, "actor", None), "login", "") or "")
        if not isinstance(created, datetime) or not actor:
            continue
        if latest is None or created >= latest[2]:
            latest = (int(getattr(event, "id", 0) or 0), actor, created)
    return latest


def is_committer(repo: Any, login: str) -> bool:
    """Whether ``login`` has write access to ``repo``. Fails closed: an error reading it is a no."""
    if not login or login.endswith("[bot]"):
        return False
    try:
        permission = retry_github_call(
            lambda: repo.get_collaborator_permission(login), retries=2, description=f"permission of {login}",
        )
    except Exception as exc:  # noqa: BLE001 - fail closed on any read error
        logger.warning("Could not read %s's permission; treating them as not a committer: %s", login, exc)
        return False
    return str(permission or "").lower() in _COMMITTER_PERMISSIONS


def read_brief(
    issue: Any, *, event_id: int, labeller: str, labelled_at: datetime, committer: Callable[[Any], bool],
) -> IssueBrief:
    """The issue as it was when labelled: its text, and the comments up to then that ``committer`` accepts."""
    comments = retry_github_call(
        lambda: list(issue.get_comments()), retries=2, description=f"read issue #{issue.number} comments",
    )
    kept: list[IssueComment] = []
    for comment in comments:
        created = getattr(comment, "created_at", None)
        if isinstance(created, datetime) and created > labelled_at:
            continue
        author = str(getattr(getattr(comment, "user", None), "login", "") or "")
        body = str(getattr(comment, "body", "") or "").strip()
        if body and author and not author.endswith("[bot]") and committer(comment):
            kept.append(IssueComment(author=author, body=body[:_COMMENT_CHARS]))
    kind, crash_report = classify(issue)
    return IssueBrief(
        number=int(issue.number),
        title=str(getattr(issue, "title", "") or ""),
        body=str(getattr(issue, "body", "") or "")[:_BODY_CHARS],
        comments=tuple(kept[-_MAX_COMMENTS:]),
        kind=kind,
        crash_report=crash_report,
        label_event_id=event_id,
        labelled_by=labeller,
        labelled_at=labelled_at.isoformat(),
    )


def classify(issue: Any) -> tuple[IssueKind, bool]:
    """What ``issue`` asks for, from its labels, and whether it reports a crash, from its text."""
    labels = {str(getattr(label, "name", "") or "").lower() for label in (getattr(issue, "labels", None) or [])}
    title = str(getattr(issue, "title", "") or "")
    body = str(getattr(issue, "body", "") or "")
    kind = IssueKind.FEATURE if labels & _FEATURE_LABELS and _BUG_LABEL not in labels else IssueKind.BUG
    return kind, bool(_CRASH_BODY_RE.search(body) or _CRASH_TITLE_RE.search(title))


def committer_check(repo: Any, *, known: dict[str, bool]) -> Callable[[Any], bool]:
    """Whether a comment's author has write access to ``repo``, asked once per author."""

    def check(comment: Any) -> bool:
        author = str(getattr(getattr(comment, "user", None), "login", "") or "")
        if author not in known:
            known[author] = is_committer(repo, author)
        return known[author]

    return check


def fix_reported(issue: Any, event_id: int, *, bot_login: str) -> bool:
    """Whether the bot is done with the labelling ``event_id``: it reported on it, or failed twice."""
    comments = retry_github_call(
        lambda: list(issue.get_comments()), retries=2, description=f"read issue #{issue.number} comments",
    )
    failed = 0
    for comment in comments:
        if str(getattr(getattr(comment, "user", None), "login", "") or "") != bot_login:
            continue
        for match in _RESULT_MARKER_RE.finditer(str(getattr(comment, "body", "") or "")):
            if int(match.group("label")) != event_id:
                continue
            if match.group("outcome") != ResultKind.FAILED.value:
                return True
            failed += 1
    return failed >= MAX_FAILED_ATTEMPTS


def _unlabel(repo_full_name: str, issue: Any, event_id: int, reason: str) -> dict[str, Any]:
    """Take the label off an issue the bot will not act on, and say why."""
    body = (
        f"Removed the `{LABEL}` label: {reason} Label the issue again to ask for a fix.\n\n"
        + _RESULT_MARKER.format(label=event_id, outcome=ResultKind.REFUSED.value)
    )
    retry_github_call(lambda: issue.remove_from_labels(LABEL), retries=2, description=f"unlabel #{issue.number}")
    retry_github_call(lambda: issue.create_comment(body), retries=2, description=f"comment on #{issue.number}")
    return {
        "repo": repo_full_name, "issue": int(issue.number), "action": ResultKind.REFUSED.value, "summary": reason,
    }


def report_result(gh: Any, plan: FixPlan, result: FixResult) -> dict[str, Any]:
    """Comment the result on the issue, and return it as a result record."""
    brief = plan.brief
    if brief is None:
        raise ValueError("a factory plan carries its issue brief")
    issue = retry_github_call(
        lambda: gh.get_repo(plan.task.failure.repo_full_name).get_issue(brief.number),
        retries=2, description=f"get issue #{brief.number}",
    )
    _terminal_comment(issue, brief, result)
    return _result_dict(plan.task, brief, result)


def _result_dict(task: FixTask, brief: IssueBrief, result: FixResult) -> dict[str, Any]:
    return {
        "repo": task.failure.repo_full_name,
        "issue": brief.number,
        "label_event": brief.label_event_id,
        "branch": task.target.branch,
        "action": result.kind.value,
        "summary": result.summary,
        "pr": result.pr_number,
        "pr_url": result.pr_url,
    }


def _terminal_comment(issue: Any, brief: IssueBrief, result: FixResult) -> None:
    """The bot's status comment: what it did with this labelling, and the evidence it has."""
    if result.kind is ResultKind.FAILED:
        # A failed attempt says nothing about the issue: its failed run is the record.
        logger.info("Issue #%s: the attempt failed; nothing is posted: %s", issue.number, result.summary)
        return
    body = "\n".join([*_report_lines(result), "", _RESULT_MARKER.format(label=brief.label_event_id, outcome=result.kind.value)])
    retry_github_call(lambda: issue.create_comment(body), retries=2, description=f"record the result on #{issue.number}")


def _report_lines(result: FixResult) -> list[str]:
    candidate = result.candidate
    if result.kind is ResultKind.OPENED:
        link = f"[#{result.pr_number}]({result.pr_url})" if result.pr_url else f"#{result.pr_number}"
        lines = [
            f"Opened draft PR {link}: a test that demonstrates the issue, and a change that makes it pass. "
            "Its CI and a committer's review decide whether it merges.",
        ]
        if candidate is not None:
            checks = check_items(candidate.verification)
            if checks:
                lines += ["", "<details><summary>What the bot checked</summary>", "", *checks, "", "</details>"]
        return lines
    if result.kind is ResultKind.SUGGESTED and candidate is not None:
        lines = [
            "A tested fix, as a patch: the test fails without the change and passes with it. Nothing was pushed "
            "and no pull request was opened: a committer applies it, or not.",
            "",
            *candidate_context(candidate, include_reasoning=True),
            *_folded("The test and the fix", candidate),
        ]
        return lines
    # Refused: no fix, with what there is. The test, when one was written, is
    # the part a person can pick up.
    lines = [f"No fix was opened. {sentence(result.summary)}"]
    if candidate is not None:
        if candidate.evidence_patch and not candidate.patch:
            lines += [
                "",
                "The bot did write a test that fails on the current code, for whoever picks the issue up.",
                *_folded("The failing test", replace(candidate, patch=candidate.evidence_patch)),
            ]
        checks = check_items(candidate.verification)
        if checks:
            lines += ["", "**What the bot checked**", "", *checks]
    return lines


def _folded(title: str, candidate: FixCandidate) -> list[str]:
    """``candidate``'s patch under a folded heading, when it has one."""
    patch = candidate_patch(candidate)
    return [f"<details><summary>{title}</summary>", "", *patch, "</details>"] if patch else []
