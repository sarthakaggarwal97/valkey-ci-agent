"""Open a draft fix PR for one Test Failure Detector issue.

It validates one detector-authored issue, binds one exact failed job of the
scheduled run it records (Daily on ``unstable``, or Weekly on a release
branch), and plans a draft PR against the branch that run tested. The fix
runs like every other (:mod:`scripts.ci_fix.run`); this module also rechecks
the issue right before publication, and reports the result on it.

The target repository's ordinary pull-request CI and human review are the
verification authority. Each detector occurrence has one deterministic
branch, so a retry can reuse its PR or finish opening a PR after an earlier
branch push.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

from github.GithubException import GithubException

from scripts.backport.registry import load_registry
from scripts.ci_fix.github_runs import (
    FailedJob,
    failed_jobs,
    job_is_ignored,
    jobs_for_run_attempt,
    select_job,
)
from scripts.ci_fix.issue_state import has_open_linked_pr, parse_detector_occurrence
from scripts.ci_fix.models import (
    Failure,
    FixCandidate,
    FixPlan,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    IssueFailure,
    OtherIssue,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.publish import publish_candidate, reconcile_draft_pr
from scripts.common.github_client import retry_github_call
from scripts.common.text_utils import compact_text
from scripts.common.valkey_ci import (
    Source,
    checked_out_sha,
    source_for_run,
    tested_branch,
)
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.test_failure_detector.download import download_all_test_failures
from scripts.test_failure_detector.issue_renderer import (
    LABEL_NAME,
    MARKER_NAMESPACE,
    job_fingerprint_for,
    parse_job_marker,
    parse_release_branch,
)
from scripts.test_failure_detector.job_failures import (
    UNBRANCHED,
    find_unrecorded_failures,
    merge_failures,
    normalize_job_name,
    split_by_branch,
)
from scripts.test_failure_detector.parse_failures import parse_and_deduplicate

logger = logging.getLogger(__name__)

BRANCH_PREFIX = "agent/ci-fix/"

_DETECTOR_RE = re.compile(
    rf"<!-- {re.escape(MARKER_NAMESPACE)}:(?P<fingerprint>[0-9a-f]+) -->"
)
_TEST_NAME_RE = re.compile(r"^- Test name: `(?P<value>.+)`\s*$", re.MULTILINE)
_TEST_FILE_RE = re.compile(r"^- Test file: `(?P<value>.+)`\s*$", re.MULTILINE)


def parse_issue(issue: Any) -> IssueFailure | None:
    """Parse a detector issue, accepting legacy occurrence-marker formats."""
    body = str(getattr(issue, "body", "") or "")
    occurrence = parse_detector_occurrence(body)
    detector = _DETECTOR_RE.search(body)
    if detector is None or occurrence is None:
        return None

    number = int(issue.number)
    run_id, run_attempt = occurrence
    fingerprint = detector.group("fingerprint")
    job = parse_job_marker(body)
    if job:
        return IssueFailure(
            number=number,
            run_id=run_id,
            run_attempt=run_attempt,
            job_name=job,
            fingerprint=fingerprint,
        )

    name = _TEST_NAME_RE.search(body)
    file = _TEST_FILE_RE.search(body)
    if name is None or file is None:
        return None
    return IssueFailure(
        number=number,
        run_id=run_id,
        run_attempt=run_attempt,
        test_name=name.group("value"),
        test_file=file.group("value"),
        fingerprint=fingerprint,
        branch=parse_release_branch(body),
    )


# Before the detector moved to this App it ran as the workflow's own token.
# Issues it filed then remain valid. Later github-actions issues are not a
# sufficient trust signal because any workflow in the repository can file one.
_LEGACY_DETECTOR_LOGIN = "github-actions[bot]"
_LEGACY_DETECTOR_UNTIL = datetime(2026, 7, 3, tzinfo=timezone.utc)


def _filed_by_detector(issue: Any, bot_login: str) -> bool:
    login = str(getattr(getattr(issue, "user", None), "login", "") or "")
    if login == bot_login:
        return True
    created = getattr(issue, "created_at", None)
    return (
        login == _LEGACY_DETECTOR_LOGIN
        and isinstance(created, datetime)
        and created.astimezone(timezone.utc) < _LEGACY_DETECTOR_UNTIL
    )


def validate_issue(
    issue: Any,
    *,
    bot_login: str,
    expected: IssueFailure | None = None,
    check_linked_pr: bool = True,
) -> tuple[IssueFailure | None, str]:
    """Return the current detector occurrence and any reason not to act."""
    if str(getattr(issue, "state", "") or "") != "open":
        return None, f"issue #{issue.number} is not open"
    if not _filed_by_detector(issue, bot_login):
        return None, f"issue #{issue.number} was not filed by the Test Failure Detector"
    failure = parse_issue(issue)
    if failure is None:
        return None, f"issue #{issue.number} does not contain a detector failure occurrence"
    if getattr(issue, "assignees", None):
        return failure, f"issue #{issue.number} is assigned"
    if check_linked_pr and has_open_linked_pr(issue):
        return failure, f"issue #{issue.number} already has an open linked pull request"
    if expected is not None and failure != expected:
        return failure, (
            f"issue #{issue.number} now points at run {failure.run_id} attempt "
            f"{failure.run_attempt}, not the occurrence that was analyzed"
        )
    return failure, ""


def _validate_scheduled_run(run: Any, repo_full_name: str, failure: IssueFailure) -> tuple[Source | None, str]:
    """The run's source, and why it cannot be fixed ("" when it can)."""
    source = source_for_run(run)
    if source is None:
        started = source_for_run(run, scheduled_only=False)
        if started is not None:
            event = str(getattr(run, "event", "") or "an unknown event")
            return None, (
                f"run {failure.run_id} is a {started.label} run triggered by {event}, "
                "not the schedule; only scheduled runs are fixed automatically"
            )
        workflows = " or ".join(f".github/workflows/{item.workflow_file}" for item in Source)
        return None, f"run {failure.run_id} is not a scheduled run of {workflows}"
    return source, _run_mismatch(run, repo_full_name, failure)


def _run_mismatch(run: Any, repo_full_name: str, failure: IssueFailure) -> str:
    head_repo = str(
        getattr(getattr(run, "head_repository", None), "full_name", "") or ""
    )
    if head_repo != repo_full_name:
        return f"run {failure.run_id} belongs to {head_repo or 'another repository'}"
    run_attempt = int(getattr(run, "run_attempt", 1) or 1)
    if run_attempt != failure.run_attempt:
        return (
            f"run {failure.run_id} is currently attempt {run_attempt}, while "
            f"the issue records attempt {failure.run_attempt}"
        )
    if not str(getattr(run, "head_branch", "") or ""):
        return f"run {failure.run_id} has no source branch"
    if not str(getattr(run, "head_sha", "") or ""):
        return f"run {failure.run_id} has no tested commit"
    return ""


def _failing_jobs(
    gh: Any,
    artifact_client: ArtifactClient,
    repo_full_name: str,
    run: Any,
    failure: IssueFailure,
    ignored_jobs: tuple[str, ...],
    source: Source,
) -> tuple[FailedJob, ...]:
    """Return exact matching failed job objects, best diagnosis target first.

    For Weekly the detector filed one issue per release branch, so every
    match is on the issue's branch.
    """
    raw_jobs = jobs_for_run_attempt(
        gh,
        repo_full_name,
        int(run.id),
        run_attempt=failure.run_attempt,
        require_current=True,
    )
    failed = [
        job
        for job in failed_jobs(raw_jobs)
        if not job_is_ignored(job.name, ignored_jobs)
    ]
    # Weekly artifacts cannot be attributed to a branch (see
    # scripts/common/valkey_ci.py); the detector read its job
    # logs only, and so does this.
    raw = download_all_test_failures(
        gh,
        repo_full_name,
        int(run.id),
        "",
        artifact_client=artifact_client,
    ) if source.artifact_is_attributable else None
    try:
        recorded = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        logger.warning("The failure artifact of run %s is malformed; using job logs", run.id)
        recorded = {}
    recorded = recorded if isinstance(recorded, dict) else {}

    from_logs, job_failures = find_unrecorded_failures(
        artifact_client, repo_full_name, recorded, jobs=raw_jobs, ignored_jobs=ignored_jobs,
    )
    tests = merge_failures(parse_and_deduplicate(recorded, {}), from_logs)
    if source is Source.WEEKLY:
        # The detector filed one issue per release branch.
        job_failures = split_by_branch(job_failures)
        tests = split_by_branch(tests)
    if failure.job_name:
        names = {
            normalize_job_name(ref.job)
            for group in job_failures
            if job_fingerprint_for(group) == failure.fingerprint
            for ref in group.jobs
        }
    else:
        names = {
            normalize_job_name(ref.job)
            for test in tests
            if (test.test_name, test.test_file, test.branch)
            == (failure.test_name, failure.test_file, failure.branch)
            for ref in test.jobs
        }

    candidates = [
        job
        for job in failed
        if normalize_job_name(job.name) in names
        and not job_is_ignored(job.name, ignored_jobs)
    ]
    if not candidates:
        return ()
    first = select_job(candidates)
    rest = sorted(
        (job for job in candidates if job is not first),
        key=lambda job: (int(job.id), job.name.lower()),
    )
    return first, *rest


def _tested_branch(source: Source, run: Any, failure: IssueFailure) -> str:
    """The branch ``failure``'s run tested, or "" when the issue does not say."""
    if source is Source.WEEKLY and failure.branch:
        return "" if failure.branch == UNBRANCHED else failure.branch
    return tested_branch(source, run, failure.job_name)


def _refuse(issue: Any, failure: IssueFailure, repo_full_name: str, summary: str) -> dict[str, Any]:
    _terminal_comment(
        issue,
        failure,
        repo_full_name=repo_full_name,
        action=ResultKind.REFUSED.value,
        summary=summary,
    )
    return _result_dict(
        repo_full_name,
        failure,
        action=ResultKind.REFUSED.value,
        summary=summary,
    )


# How many other open detector issues are considered as also-fixed
# candidates. Each one whose test the patch changes costs a verification.
_MAX_OTHER_ISSUES = 20


def open_detector_issues(repo: Any, bot_login: str, *, limit: int) -> list[tuple[Any, IssueFailure]]:
    """The newest ``limit`` open issues the detector filed, with the failure each records."""
    issues = retry_github_call(
        lambda: list(repo.get_issues(state="open", labels=[LABEL_NAME])[:limit]),
        retries=2,
        description="list open detector issues",
    )
    found = []
    for issue in issues:
        if getattr(issue, "pull_request", None) is not None:
            continue  # the issues API also returns pull requests
        if not _filed_by_detector(issue, bot_login):
            continue
        parsed = parse_issue(issue)
        if parsed is not None:
            found.append((issue, parsed))
    return found


def _other_open_issues(repo: Any, bot_login: str, *, exclude: int, branch: str) -> tuple[OtherIssue, ...]:
    """Other open detector issues that name one test, for the also-fixed check.

    Only issues about the same branch: a patch verified on 8.1 shows nothing
    about the Daily issue for the same test on ``unstable`` (``branch`` is ""
    for Daily issues, a release branch for Weekly ones).
    """
    try:
        listed = open_detector_issues(repo, bot_login, limit=_MAX_OTHER_ISSUES)
    except Exception as exc:  # noqa: BLE001 - without the list a patch simply claims less
        logger.warning("Could not list other detector issues: %s", exc)
        return ()
    return tuple(
        OtherIssue(number=parsed.number, test=parsed.failed_tests[0])
        for _issue, parsed in listed
        # a job-level issue has no one test to verify
        if parsed.number != exclude and not parsed.job_name and parsed.failed_tests and parsed.branch == branch
    )


def _branch_name(failure: IssueFailure) -> str:
    """Return the one durable branch name for this detector occurrence."""
    return (
        f"{BRANCH_PREFIX}issue-{failure.number}-{failure.run_id}-"
        f"{failure.run_attempt}"
    )


_RESULT_HEADING = "**Automatic CI fix — "


def _source_link(repo_full_name: str, failure: IssueFailure) -> str:
    """The Markdown link to the run that recorded ``failure``."""
    run_url = f"https://github.com/{repo_full_name}/actions/runs/{failure.run_id}"
    if failure.run_attempt > 1:
        return f"[run {failure.run_id} attempt {failure.run_attempt}]({run_url}/attempts/{failure.run_attempt})"
    return f"[run {failure.run_id}]({run_url})"


def fix_reported(issue: Any, failure: IssueFailure, *, repo_full_name: str, bot_login: str) -> bool:
    """Whether the bot already reported a fix attempt for ``failure``'s occurrence.

    The detector records a newer occurrence in the issue when the test fails
    again, which makes the issue worth another attempt.
    """
    source = f"Source: {_source_link(repo_full_name, failure)}"
    comments = retry_github_call(
        lambda: list(issue.get_comments()),
        retries=2,
        description=f"read issue #{issue.number} comments",
    )
    return any(
        str(getattr(getattr(comment, "user", None), "login", "") or "") == bot_login
        and str(getattr(comment, "body", "") or "").startswith(_RESULT_HEADING)
        and source in str(getattr(comment, "body", "") or "")
        for comment in comments
    )


def _terminal_comment(
    issue: Any,
    failure: IssueFailure,
    *,
    repo_full_name: str,
    action: str,
    summary: str,
    job_name: str = "",
    pr_number: int = 0,
    pr_url: str = "",
    checked: str = "",
) -> None:
    source = _source_link(repo_full_name, failure)
    if job_name:
        source += f", job `{job_name}`"
    if action == ResultKind.OPENED.value:
        link = f"[#{pr_number}]({pr_url})" if pr_url else f"#{pr_number}"
        message = (
            f"Opened draft PR {link}. Normal pull-request CI and human review "
            "are authoritative."
        )
    elif action == ResultKind.REFUSED.value:
        message = (
            f"No draft PR was opened: {compact_text(summary, 2_000)}"
        )
    elif action == ResultKind.FAILED.value:
        message = (
            "The automatic fix attempt failed: "
            f"{compact_text(summary, 2_000)}"
        )
    else:
        message = compact_text(summary, 2_000)
    body = (
        f"{_RESULT_HEADING}{action}.** {message}\n\n"
        f"Source: {source}."
    )
    lines = [line for line in checked.splitlines() if line.strip()]
    if lines:
        # The existing-fix search and the reproduction, for whoever picks
        # the issue up next.
        body += "\n\n**Checked by the bot:**\n\n" + "\n".join(f"- {compact_text(line, 1_000)}" for line in lines)
    retry_github_call(
        lambda: issue.create_comment(body),
        retries=2,
        description=f"record CI-fix result on issue #{issue.number}",
    )


def _result_dict(
    repo_full_name: str,
    failure: IssueFailure,
    *,
    action: str,
    summary: str,
    job_name: str = "",
    branch: str = "",
    pr_number: int = 0,
    pr_url: str = "",
) -> dict[str, Any]:
    return {
        "repo": repo_full_name,
        "issue": failure.number,
        "run": failure.run_id,
        "run_attempt": failure.run_attempt,
        "job": job_name,
        "branch": branch,
        "action": action,
        "summary": summary,
        "pr": pr_number,
        "pr_url": pr_url,
    }


@dataclass(frozen=True)
class PreparedIssue:
    """A validated detector issue and the fix task it asks for."""

    failure: IssueFailure
    task: FixTask
    branch_name: str


def plan_request(
    gh: Any,
    artifact_client: ArtifactClient,
    request: dict[str, Any],
    *,
    bot_login: str,
    registry: str = "repos.yml",
) -> FixPlan | dict[str, Any]:
    """The fix plan for ``request`` (``{"repo": ..., "issue": N}``), or the final result."""
    repo_full_name = str(request.get("repo") or "")
    entry = load_registry(registry).get_repo(repo_full_name)
    prepared = prepare_issue(
        gh, artifact_client, repo_full_name,
        issue_number=int(request.get("issue") or 0), bot_login=bot_login,
        ignored_jobs=entry.ci_followup_ignored_jobs,
    )
    if not isinstance(prepared, PreparedIssue):
        return prepared
    return FixPlan(front_door=FrontDoor.ISSUE, task=prepared.task, issue=prepared.failure)


def _prepared(plan: FixPlan) -> PreparedIssue:
    if plan.issue is None:
        raise ValueError("an issue fix plan names its issue")
    return PreparedIssue(failure=plan.issue, task=plan.task, branch_name=plan.task.target.branch)


def publisher(plan: FixPlan, *, bot_login: str) -> Callable[..., FixResult]:
    return publisher_for(_prepared(plan), bot_login=bot_login)


def report_result(gh: Any, plan: FixPlan, result: FixResult, *, bot_login: str) -> dict[str, Any]:  # noqa: ARG001
    return report(gh, _prepared(plan), result)


def get_issue(gh: Any, repo_full_name: str, issue_number: int) -> Any:
    return retry_github_call(
        lambda: gh.get_repo(repo_full_name).get_issue(issue_number),
        retries=2,
        description=f"get issue #{issue_number}",
    )


def publisher_for(prepared: PreparedIssue, *, bot_login: str) -> Callable[..., FixResult]:
    """Publication that first rechecks the issue still asks for this fix."""
    failure = prepared.failure
    repo_full_name = prepared.task.failure.repo_full_name

    def checked_publish(
        publish_gh: Any,
        publish_task: FixTask,
        candidate: FixCandidate,
        *,
        git_env: dict[str, str],
    ) -> FixResult:
        reconciled = reconcile_draft_pr(publish_gh, publish_task)
        if reconciled is not None:
            return reconciled
        latest = get_issue(publish_gh, repo_full_name, failure.number)
        _current, reason = validate_issue(latest, bot_login=bot_login, expected=failure)
        if reason:
            return FixResult(
                kind=ResultKind.REFUSED,
                summary=f"Publication stopped: {reason}.",
                task=publish_task,
                candidate=candidate,
            )
        return publish_candidate(publish_gh, publish_task, candidate, git_env=git_env)

    return checked_publish


def report(gh: Any, prepared: PreparedIssue, result: FixResult) -> dict[str, Any]:
    """Comment the fix's result on its issue, and return it as a result record."""
    task = prepared.task
    repo_full_name = task.failure.repo_full_name
    _terminal_comment(
        get_issue(gh, repo_full_name, prepared.failure.number),
        prepared.failure,
        repo_full_name=repo_full_name,
        action=result.kind.value,
        summary=result.summary,
        job_name=task.failure.job_name,
        pr_number=result.pr_number,
        pr_url=result.pr_url,
        checked=result.candidate.verification if result.candidate else "",
    )
    return _result_dict(
        repo_full_name,
        prepared.failure,
        action=result.kind.value,
        summary=result.summary,
        job_name=task.failure.job_name,
        branch=prepared.branch_name,
        pr_number=result.pr_number,
        pr_url=result.pr_url,
    )


def prepare_issue(
    gh: Any,
    artifact_client: ArtifactClient,
    repo_full_name: str,
    *,
    issue_number: int,
    bot_login: str,
    ignored_jobs: tuple[str, ...] = (),
) -> PreparedIssue | dict[str, Any]:
    """Validate one detector issue and build its fix task, or the final result.

    A final result is a skip, a refusal (commented on the issue), or an
    earlier attempt's draft PR recovered.
    """
    if issue_number <= 0:
        raise ValueError("an explicit positive issue number is required")

    repo = retry_github_call(
        lambda: gh.get_repo(repo_full_name),
        retries=2,
        description=f"get {repo_full_name}",
    )
    issue = retry_github_call(
        lambda: repo.get_issue(issue_number),
        retries=2,
        description=f"get issue #{issue_number}",
    )
    failure, rejection = validate_issue(
        issue,
        bot_login=bot_login,
        check_linked_pr=False,
    )
    if failure is None or rejection:
        return {
            "repo": repo_full_name,
            "issue": issue_number,
            "action": "skipped",
            "reason": rejection,
        }

    run = retry_github_call(
        lambda: repo.get_workflow_run(failure.run_id),
        retries=2,
        description=f"get run {failure.run_id}",
    )
    source, rejection = _validate_scheduled_run(run, repo_full_name, failure)
    if rejection or source is None:
        return _refuse(issue, failure, repo_full_name, rejection)

    # The branch the run tested: the Daily run's own, or the release branch
    # a Weekly issue records (its job name carries it for a job issue). It is
    # known before any job log is read, so a retry can recover an earlier
    # attempt's branch even after the logs expire.
    branch = _tested_branch(source, run, failure)
    if not branch:
        return _refuse(issue, failure, repo_full_name, (
            "the issue does not record which release branch the Weekly run tested"
        ))
    try:
        current = retry_github_call(
            lambda: repo.get_branch(branch),
            retries=2,
            description=f"get {repo_full_name}:{branch}",
        )
    except GithubException as exc:
        if exc.status != 404:
            raise
        return _refuse(issue, failure, repo_full_name, f"branch `{branch}` no longer exists")
    base_sha = str(getattr(getattr(current, "commit", None), "sha", "") or "")
    tested_sha = str(getattr(run, "head_sha", "") or "") if source is Source.DAILY else ""
    branch_name = _branch_name(failure)
    target = FixTarget(
        kind=TargetKind.DRAFT_PR,
        repo_full_name=repo_full_name,
        base_repo_full_name=repo_full_name,
        branch=branch_name,
        expected_sha=base_sha,
        base_branch=branch,
        issue_number=failure.number,
        draft=True,
    )
    others = _other_open_issues(repo, bot_login, exclude=failure.number, branch=failure.branch)
    recovery_task = FixTask(
        failure=Failure(
            repo_full_name=repo_full_name,
            run_id=failure.run_id,
            run_attempt=failure.run_attempt,
            head_sha=tested_sha or base_sha,
            job_id=0,
            job_name=failure.job_name or f"{source.label} CI failure",
            base_branch=branch,
            description="Recovering a previously prepared fix for this detector occurrence.",
            issue_number=failure.number,
            failed_tests=failure.failed_tests,
            source=source,
        ),
        target=target,
        policy=FixPolicy.GENERAL,
        other_issues=others,
    )

    # Recover a branch left by an earlier attempt before touching artifacts or
    # job logs, which may have expired by the time a retry runs.
    reconciled = reconcile_draft_pr(gh, recovery_task)
    if reconciled is not None:
        _terminal_comment(
            issue,
            failure,
            repo_full_name=repo_full_name,
            action=reconciled.kind.value,
            summary=reconciled.summary,
            pr_number=reconciled.pr_number,
            pr_url=reconciled.pr_url,
        )
        return _result_dict(
            repo_full_name,
            failure,
            action=reconciled.kind.value,
            summary=reconciled.summary,
            branch=branch_name,
            pr_number=reconciled.pr_number,
            pr_url=reconciled.pr_url,
        )

    if has_open_linked_pr(issue):
        return {
            "repo": repo_full_name,
            "issue": issue_number,
            "action": "skipped",
            "reason": f"issue #{issue.number} already has an open linked pull request",
        }

    jobs = _failing_jobs(gh, artifact_client, repo_full_name, run, failure, ignored_jobs, source)
    if not jobs:
        return _refuse(issue, failure, repo_full_name, (
            f"no failed job in run {failure.run_id} matches the failure recorded by this issue"
        ))

    job = jobs[0]
    other_jobs = tuple(item.name for item in jobs[1:])
    description = failure.describe(job.name, source)
    if source is Source.WEEKLY:
        # The Weekly run's own SHA is the scheduling branch's; the job log
        # names the release-branch commit it checked out.
        tested_sha = checked_out_sha(
            str(artifact_client.download_job_log(repo_full_name, int(job.id)) or ""),
            repo_full_name,
        )
        description += f" Weekly tests release branch `{branch}`"
        description += f" (it tested `{tested_sha[:12]}`)." if tested_sha else "."
    if tested_sha and tested_sha != base_sha:
        description += (
            f" The source run tested `{tested_sha[:12]}`; the candidate is "
            f"prepared on the current `{branch}` tip `{base_sha[:12]}`."
        )
    task = replace(
        recovery_task,
        failure=replace(
            recovery_task.failure,
            head_sha=tested_sha or base_sha,
            job_id=int(job.id),
            job_name=job.name,
            description=description,
            other_failed_jobs=other_jobs,
        ),
    )
    return PreparedIssue(failure=failure, task=task, branch_name=branch_name)
