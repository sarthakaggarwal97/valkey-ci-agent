"""Publish a prepared CI-fix candidate.

Publication is intentionally ordinary:

* update a bot-owned PR branch;
* suggest a patch on a contributor PR; or
* create a draft fix PR for a detector issue.

The target repository's normal pull-request CI is the verification authority.
This module never merges a pull request.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Protocol

from github.GithubException import GithubException

from scripts.backport.pr_creator import create_pull_from_push_repo
from scripts.backport.sweep_prs import find_existing_pr
from scripts.ci_fix.issue_state import has_open_linked_pr, parse_detector_occurrence
from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FixCandidate,
    FixResult,
    FixTask,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.policy import can_update_branch, target_capabilities
from scripts.ci_fix.push import (
    PushFailed,
    PushRefused,
    PushStale,
    fit_subject,
    push_candidate,
)
from scripts.common.git_clone import is_full_sha
from scripts.common.github_client import retry_github_call
from scripts.common.text_utils import compact_text
from scripts.common.valkey_ci import Source


class TargetRevalidationFailed(Exception):
    """The target could not be revalidated because GitHub was unavailable."""


class TargetPublisher(Protocol):
    """One target-specific publication adapter."""

    def __call__(
        self,
        gh: Any,
        task: FixTask,
        candidate: FixCandidate,
        *,
        git_env: dict[str, str],
    ) -> FixResult: ...


def publish_candidate(
    gh: Any,
    task: FixTask,
    candidate: FixCandidate,
    *,
    git_env: dict[str, str],
) -> FixResult:
    """Publish ``candidate`` according to ``task.target``."""
    if candidate.action is CandidateAction.REFUSE:
        return FixResult(
            kind=ResultKind.REFUSED,
            summary=candidate.summary or candidate.reasoning or "No safe fix was found.",
            task=task,
            candidate=candidate,
        )

    target = task.target
    if not target_capabilities(target.kind).allows(candidate.action):
        return _refused(
            task,
            candidate,
            f"The {candidate.action.value} action is not supported for "
            f"a {target.kind.value} target.",
        )
    if (
        target.kind is not TargetKind.DRAFT_PR
        and target.expected_sha != task.failure.head_sha
    ):
        return _refused(task, candidate, "The publication target does not match the analyzed head.")

    publisher = _TARGET_PUBLISHERS.get(target.kind)
    if publisher is None:
        return FixResult(
            kind=ResultKind.FAILED,
            summary=f"Unsupported publication target {target.kind.value}.",
            task=task,
            candidate=candidate,
        )
    return publisher(gh, task, candidate, git_env=git_env)


def _publish_suggestion(
    gh: Any,
    task: FixTask,
    candidate: FixCandidate,
    *,
    git_env: dict[str, str],
) -> FixResult:
    del git_env
    try:
        rejection = _validate_pr_target(gh, task, require_bot=False)
    except TargetRevalidationFailed as exc:
        return _failed(task, candidate, str(exc))
    if rejection:
        return _refused(task, candidate, f"Not posting the suggestion: {rejection}.")
    return FixResult(
        kind=ResultKind.SUGGESTED,
        summary="Prepared a patch for the PR author.",
        task=task,
        candidate=candidate,
    )


def _publish_update(
    gh: Any,
    task: FixTask,
    candidate: FixCandidate,
    *,
    git_env: dict[str, str],
) -> FixResult:
    def check() -> str:
        return _validate_pr_target(gh, task, require_bot=True)

    try:
        commit_sha = push_candidate(
            candidate,
            task.target,
            git_env=git_env,
            pre_push_check=check,
        )
    except PushRefused as exc:
        return _refused(task, candidate, str(exc))
    except PushFailed as exc:
        return _failed(task, candidate, str(exc))
    return FixResult(
        kind=ResultKind.PUSHED,
        summary=(
            f"Ported an upstream fix for {candidate.failing_check or task.failure.job_name}."
            if candidate.action is CandidateAction.PORT
            else f"Pushed a fix for {candidate.failing_check or task.failure.job_name}."
        ),
        task=task,
        candidate=candidate,
        commit_sha=commit_sha,
    )


def _publish_draft(
    gh: Any,
    task: FixTask,
    candidate: FixCandidate,
    *,
    git_env: dict[str, str],
) -> FixResult:
    # A deterministic occurrence branch may be left behind when a prior push
    # succeeded but PR creation failed. Its code is from that earlier attempt,
    # so recover it with generic metadata rather than describing it with this
    # run's newly generated candidate.
    reconciled = reconcile_draft_pr(gh, task)
    if reconciled is not None:
        return reconciled
    try:
        rejection = _validate_issue_target(gh, task)
    except TargetRevalidationFailed as exc:
        return _failed(task, candidate, str(exc))
    if rejection:
        return _refused(task, candidate, rejection)

    def check_issue() -> str:
        return _validate_issue_target(gh, task)

    try:
        commit_sha = push_candidate(
            candidate,
            task.target,
            git_env=git_env,
            pre_push_check=check_issue,
        )
    except PushStale as exc:
        # A concurrent or ambiguous prior push may have created the
        # deterministic occurrence branch after the first lookup.
        reconciled = reconcile_draft_pr(gh, task)
        if reconciled is not None:
            return reconciled
        return _refused(task, candidate, str(exc))
    except PushRefused as exc:
        return _refused(task, candidate, str(exc))
    except PushFailed as exc:
        return _failed(task, candidate, str(exc))

    reconciled = reconcile_draft_pr(
        gh,
        task,
        candidate=candidate,
        known_commit_sha=commit_sha,
    )
    if reconciled is not None:
        return reconciled
    return _failed(
        task,
        candidate,
        f"The fix was pushed as {commit_sha[:12]}, but its branch could "
        "not be found to open the draft PR.",
        commit_sha=commit_sha,
    )


_TARGET_PUBLISHERS: dict[TargetKind, TargetPublisher] = {
    TargetKind.SUGGEST: _publish_suggestion,
    TargetKind.UPDATE_PR: _publish_update,
    TargetKind.DRAFT_PR: _publish_draft,
}


def reconcile_draft_pr(
    gh: Any,
    task: FixTask,
    *,
    candidate: FixCandidate | None = None,
    known_commit_sha: str = "",
) -> FixResult | None:
    """Finish or reuse publication for one deterministic detector branch.

    ``None`` means the occurrence branch does not exist yet. An existing PR is
    returned unchanged; an existing branch without a PR is turned into the
    missing draft PR. This makes a retry complete an attempt whose branch push
    succeeded before PR creation failed.
    """
    target = task.target
    if target.kind is not TargetKind.DRAFT_PR:
        return None

    try:
        existing = find_existing_pr(
            gh,
            task.failure.repo_full_name,
            target.repo_full_name,
            target.branch,
        )
    except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
        return _failed(
            task,
            candidate,
            f"The draft publication target {target.branch} could not be "
            f"revalidated: {exc}",
            commit_sha=known_commit_sha,
        )
    if existing is not None:
        rejection = _validate_existing_draft_pr(
            existing,
            task,
            expected_sha=known_commit_sha,
        )
        if rejection:
            return _refused(task, candidate, rejection)
        return _opened(
            task,
            candidate if known_commit_sha else None,
            existing,
            commit_sha=known_commit_sha,
        )

    branch_sha = known_commit_sha
    if not branch_sha:
        try:
            push_repo = retry_github_call(
                lambda: gh.get_repo(target.repo_full_name),
                retries=2,
                description=f"get {target.repo_full_name}",
            )
            branch = retry_github_call(
                lambda: push_repo.get_branch(target.branch),
                retries=2,
                description=f"get {target.repo_full_name}:{target.branch}",
            )
        except GithubException as exc:
            if exc.status == 404:
                return None
            return _failed(
                task,
                candidate,
                f"The occurrence branch {target.branch} could not be checked: {exc}",
            )
        except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
            return _failed(
                task,
                candidate,
                f"The occurrence branch {target.branch} could not be checked: {exc}",
            )

        branch_sha = str(getattr(getattr(branch, "commit", None), "sha", "") or "")
        if not is_full_sha(branch_sha):
            return _failed(
                task,
                candidate,
                f"The occurrence branch {target.branch} has no valid commit SHA.",
            )
    commit_sha = branch_sha

    try:
        rejection = _validate_issue_target(gh, task)
    except TargetRevalidationFailed as exc:
        return _failed(task, candidate, str(exc), commit_sha=commit_sha)
    if rejection:
        # The exact PR may have appeared between the first lookup and the
        # issue-timeline check. One final exact lookup resolves that race.
        if "already has an open linked pull request" in rejection:
            try:
                existing = find_existing_pr(
                    gh,
                    task.failure.repo_full_name,
                    target.repo_full_name,
                    target.branch,
                )
            except Exception:
                existing = None
            if existing is not None:
                existing_rejection = _validate_existing_draft_pr(
                    existing,
                    task,
                    expected_sha=commit_sha,
                )
                if not existing_rejection:
                    return _opened(
                        task,
                        candidate if known_commit_sha else None,
                        existing,
                        commit_sha=commit_sha,
                    )
        return FixResult(
            kind=ResultKind.REFUSED,
            summary=rejection,
            task=task,
            candidate=candidate,
            commit_sha=commit_sha,
        )

    try:
        base_repo = retry_github_call(
            lambda: gh.get_repo(task.failure.repo_full_name),
            retries=2,
            description=f"get {task.failure.repo_full_name}",
        )
        existing = retry_github_call(
            lambda: create_pull_from_push_repo(
                base_repo,
                base_repo=task.failure.repo_full_name,
                push_repo=target.repo_full_name,
                title=_draft_title(
                    task,
                    candidate if known_commit_sha else None,
                ),
                body=_draft_body(
                    task,
                    candidate if known_commit_sha else None,
                ),
                head_branch=target.branch,
                base_branch=target.base_branch or task.failure.base_branch,
                draft=target.draft,
            ),
            retries=2,
            description=f"open draft CI-fix PR for issue #{target.issue_number}",
        )
    except Exception as exc:
        try:
            existing = find_existing_pr(
                gh,
                task.failure.repo_full_name,
                target.repo_full_name,
                target.branch,
            )
        except Exception:  # noqa: BLE001 - preserve the original create failure
            existing = None
        if existing is not None:
            rejection = _validate_existing_draft_pr(
                existing,
                task,
                expected_sha=commit_sha,
            )
            if not rejection:
                return _opened(
                    task,
                    candidate if known_commit_sha else None,
                    existing,
                    commit_sha=commit_sha,
                )
        return _failed(
            task,
            candidate,
            f"The fix branch {target.branch} is at {commit_sha[:12]}, but the "
            f"draft PR could not be opened: {exc}",
            commit_sha=commit_sha,
        )
    return _opened(
        task,
        candidate if known_commit_sha else None,
        existing,
        commit_sha=commit_sha,
    )


def _validate_pr_target(gh: Any, task: FixTask, *, require_bot: bool) -> str:
    target = task.target
    try:
        pr = retry_github_call(
            lambda: gh.get_repo(task.failure.repo_full_name).get_pull(target.pr_number),
            retries=2,
            description=f"revalidate PR #{target.pr_number}",
        )
    except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
        raise TargetRevalidationFailed(
            f"PR #{target.pr_number} could not be revalidated ({exc})"
        ) from exc
    if str(getattr(pr, "state", "") or "") != "open":
        return f"PR #{target.pr_number} is no longer open"
    if str(getattr(getattr(pr, "base", None), "ref", "") or "") != target.base_branch:
        return f"PR #{target.pr_number} no longer targets {target.base_branch}"
    head = getattr(pr, "head", None)
    if str(getattr(getattr(head, "repo", None), "full_name", "") or "") != target.repo_full_name:
        return f"PR #{target.pr_number} no longer comes from {target.repo_full_name}"
    if str(getattr(head, "ref", "") or "") != target.branch:
        return f"PR #{target.pr_number} no longer has head branch {target.branch}"
    current_sha = str(getattr(head, "sha", "") or "")
    if current_sha != target.expected_sha:
        return (
            f"the PR head moved from {target.expected_sha[:12]} to "
            f"{current_sha[:12] or '(missing)'}"
        )
    if require_bot:
        author = str(getattr(getattr(pr, "user", None), "login", "") or "")
        if not can_update_branch(
            same_repo=target.repo_full_name == task.failure.repo_full_name,
            branch=target.branch,
            author=author,
        ):
            return f"PR #{target.pr_number} is not owned by the CI agent"
    return ""


def _validate_issue_target(gh: Any, task: FixTask) -> str:
    """Recheck a detector issue immediately before creating its branch.

    The base branch may have moved since the candidate was prepared: the
    draft PR is opened from the commit it was prepared on, and its own CI
    runs against the branch as it is then.
    """
    issue_number = task.target.issue_number or task.failure.issue_number
    if not issue_number:
        return "The draft-PR target has no issue number."
    try:
        repo = retry_github_call(
            lambda: gh.get_repo(task.failure.repo_full_name),
            retries=2,
            description=f"get {task.failure.repo_full_name}",
        )
        issue = retry_github_call(
            lambda: repo.get_issue(issue_number),
            retries=2,
            description=f"revalidate issue #{issue_number}",
        )
    except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
        raise TargetRevalidationFailed(
            f"Issue #{issue_number} could not be revalidated ({exc})."
        ) from exc
    if str(getattr(issue, "state", "") or "") != "open":
        return f"Issue #{issue_number} is no longer open."
    if getattr(issue, "assignees", None):
        return f"Issue #{issue_number} is now assigned; leaving it to the assignee."
    try:
        if has_open_linked_pr(issue):
            return f"Issue #{issue_number} already has an open linked pull request."
    except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
        raise TargetRevalidationFailed(
            f"Issue #{issue_number} links could not be revalidated ({exc})."
        ) from exc
    body = str(getattr(issue, "body", "") or "")
    occurrence = parse_detector_occurrence(body)
    if occurrence is None:
        return f"Issue #{issue_number} no longer contains a detector occurrence."
    run_id, run_attempt = occurrence
    if (run_id, run_attempt) != (task.failure.run_id, task.failure.run_attempt):
        return (
            f"Issue #{issue_number} now points at run {run_id} attempt "
            f"{run_attempt}, not the occurrence that was analyzed."
        )
    if not (task.target.base_branch or task.failure.base_branch):
        return "The draft-PR target has no base branch."
    return ""


def _validate_existing_draft_pr(
    pull: Any,
    task: FixTask,
    *,
    expected_sha: str = "",
) -> str:
    """Return why an exact occurrence PR cannot be safely reused."""
    target = task.target
    number = int(getattr(pull, "number", 0) or 0)
    label = f"PR #{number}" if number else "The existing PR"
    if str(getattr(pull, "state", "open") or "") != "open":
        return f"{label} is no longer open."
    if str(getattr(getattr(pull, "base", None), "ref", "") or "") != target.base_branch:
        return f"{label} does not target {target.base_branch}."
    head = getattr(pull, "head", None)
    if str(getattr(getattr(head, "repo", None), "full_name", "") or "") != target.repo_full_name:
        return f"{label} does not come from {target.repo_full_name}."
    if str(getattr(head, "ref", "") or "") != target.branch:
        return f"{label} does not use occurrence branch {target.branch}."
    head_sha = str(getattr(head, "sha", "") or "")
    if not is_full_sha(head_sha):
        return f"{label} has no valid head SHA."
    if expected_sha and head_sha != expected_sha:
        return (
            f"{label} moved from the pushed commit {expected_sha[:12]} to "
            f"{head_sha[:12]}."
        )
    return ""


def _source_label(failure: Failure) -> str:
    return failure.source.label if failure.source else "CI"


def _draft_title(task: FixTask, candidate: FixCandidate | None) -> str:
    failing_check = candidate.failing_check if candidate is not None else ""
    fallback = f"{_source_label(task.failure)} failure"
    # Valkey titles a release-branch PR with its branch, e.g. "[8.1] ...".
    prefix = f"[{task.target.base_branch}] " if task.failure.source is Source.WEEKLY else ""
    return fit_subject(f"{prefix}Fix {failing_check or task.failure.job_name or fallback}")


def _draft_body(task: FixTask, candidate: FixCandidate | None) -> str:
    failure = task.failure
    lines = []
    if failure.issue_number and task.target.base_branch and failure.source is Source.WEEKLY:
        # GitHub closes an issue from a merged PR only on the default branch.
        lines += [
            f"Refs #{failure.issue_number}",
            "",
            f"This targets the `{task.target.base_branch}` release branch; merging it "
            f"does not close #{failure.issue_number}, so close it after merging.",
            "",
        ]
    elif failure.issue_number:
        # One file often holds several open failures, so a patch can fix more
        # than the issue it was asked about. Each extra issue was verified on
        # its own test; the ones that were not are only mentioned.
        closes = [failure.issue_number, *(candidate.also_fixes if candidate else ())]
        lines += [" ".join(f"Fixes #{number}" for number in closes), ""]
        if candidate and candidate.also_touches:
            touched = ", ".join(f"#{number}" for number in candidate.also_touches)
            lines += [
                f"This also changes the test(s) of {touched}, without showing that it fixes "
                "them; see what the bot checked below.",
                "",
            ]
    lines += [
        f"The [{_source_label(failure)} failure]({failure.run_url}) was in `{failure.job_name}`.",
        "",
    ]
    if failure.description:
        lines += [compact_text(failure.description, 2_000), ""]
    if candidate is not None and candidate.root_cause:
        lines += [
            f"**Root cause:** {compact_text(candidate.root_cause, 2_000)}",
            "",
        ]
    if candidate is not None and candidate.reasoning:
        lines += [
            f"**Change:** {compact_text(candidate.reasoning, 3_000)}",
            "",
        ]
    if candidate is not None and candidate.action is CandidateAction.PORT:
        lines += [f"**Upstream fix:** `{candidate.port_commit}`", ""]
    if candidate is not None and candidate.verification:
        lines += ["**Checked by the bot:**", ""]
        lines += [f"- {line}" for line in candidate.verification.splitlines() if line.strip()]
        lines += [""]
    if candidate is not None and candidate.validation:
        lines += [
            f"**Agent validation notes:** {compact_text(candidate.validation, 2_000)}",
            "",
        ]
    lines += [
        "> [!IMPORTANT]",
        "> This draft was prepared automatically. Normal pull-request CI and "
        "human review are authoritative; the bot never merges it.",
        "",
    ]
    if candidate is None:
        lines += [
            "This branch was prepared by an earlier automatic attempt. Review "
            "its commit and the normal pull-request CI results before merge.",
        ]
    elif candidate.action is CandidateAction.PORT:
        lines += [
            "The cherry-pick preserves the upstream commit message. The target "
            "repository's normal DCO check remains authoritative.",
        ]
    else:
        if failure.source is Source.WEEKLY:
            lines += [
                f"No merged fix to port was found, so this patches `{task.target.base_branch}` "
                "directly. Valkey fixes tests on `unstable` first: if the test fails there "
                "too, the fix belongs there and this PR should become its backport.",
                "",
            ]
        lines += [
            "The bot-authored commit intentionally has no DCO sign-off. A maintainer "
            "who takes over the change must review and sign it off before merge.",
        ]
    return "\n".join(lines)


def _opened(
    task: FixTask,
    candidate: FixCandidate | None,
    pull: Any,
    *,
    commit_sha: str = "",
) -> FixResult:
    head_sha = str(
        getattr(getattr(pull, "head", None), "sha", "") or ""
    )
    return FixResult(
        kind=ResultKind.OPENED,
        summary=f"Opened draft PR #{int(pull.number)} for review and normal CI.",
        task=task,
        candidate=candidate,
        commit_sha=commit_sha or head_sha,
        pr_number=int(pull.number),
        pr_url=str(getattr(pull, "html_url", "") or ""),
    )


def _refused(
    task: FixTask,
    candidate: FixCandidate | None,
    summary: str,
) -> FixResult:
    return FixResult(
        kind=ResultKind.REFUSED,
        summary=summary,
        task=task,
        candidate=replace(candidate, summary=summary) if candidate is not None else None,
    )


def _failed(
    task: FixTask,
    candidate: FixCandidate | None,
    summary: str,
    *,
    commit_sha: str = "",
) -> FixResult:
    return FixResult(
        kind=ResultKind.FAILED,
        summary=summary,
        task=task,
        candidate=candidate,
        commit_sha=commit_sha,
    )
