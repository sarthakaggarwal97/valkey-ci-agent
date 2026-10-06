"""Fix CI failures reported by the Test Failure Detector.

The Daily issue front door. A scheduled workflow runs three steps:

1. ``reconcile`` (write token, no AI, no checkout): finish attempts whose Daily
   verification runs have completed - open the fix PR when the candidate
   passed, otherwise report it and delete the candidate branch - and end
   attempts whose run died or whose verification never finished.
2. ``prepare`` (read-only except issues:write for the claim comment): pick one
   open issue the detector filed with a new occurrence, claim it with a
   comment, plan its Daily verification, and run the shared engine. Nothing
   from the checkout is executed; the skeptic reviews the fix.
3. ``publish`` (fresh write token): push the fix to a new
   ``agent/ci-fix/issue-<N>-<run>-<claim id>`` branch, dispatch the Daily
   workflow on the unfixed base and on the fix, and record the attempt.

State lives on GitHub. Each attempt is one bot comment on the issue, created
as the claim and edited as the attempt moves through running, pending, and a
final state; its hidden marker carries the attempt as JSON. An issue gets at
most one attempt per occurrence and three in total; an issue that is assigned
or already has an open PR linked is left to its owner. Only one verification
is in flight at a time.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github
from github.GithubException import GithubException

from scripts.backport.pr_creator import create_pull_from_push_repo
from scripts.backport.registry import load_registry
from scripts.backport.sweep_prs import find_existing_pr
from scripts.ci_fix.comment import triage_lines
from scripts.ci_fix.daily_verify import (
    DAILY_WORKFLOW,
    DailyPlan,
    DailyResult,
    dispatch_daily,
    evaluate_daily_run,
    plan_daily_run,
    stress_loops,
)
from scripts.ci_fix.models import (
    FixOutcome,
    FixProposal,
    FixRequest,
    OutcomeKind,
    Policy,
    Publication,
    outcome_from_dict,
    request_from_dict,
    to_dict,
)
from scripts.ci_fix.pipeline import run_ci_fix_request
from scripts.ci_fix.publish import read_state, write_state
from scripts.ci_fix.push import (
    PushRefused,
    commit_and_push_fix,
    commit_and_push_port,
    commit_subject,
    fit_subject,
)
from scripts.ci_fix.verify.github_runs import failed_jobs_for_run, select_job
from scripts.common.git_auth import GitAuth
from scripts.common.github_client import replace_or_post_comment, retry_github_call
from scripts.common.identity import APP_LOGIN
from scripts.common.issue_dedup import drop_pull_requests
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.test_failure_detector.download import download_all_test_failures
from scripts.test_failure_detector.issue_renderer import (
    LABEL_NAME,
    MARKER_NAMESPACE,
    job_fingerprint_for,
    parse_job_marker,
)
from scripts.test_failure_detector.job_failures import (
    FILE_TIMEOUT_TEST_NAME,
    find_unrecorded_failures,
    merge_failures,
    normalize_job_name,
)
from scripts.test_failure_detector.parse_failures import parse_and_deduplicate

logger = logging.getLogger(__name__)

ATTEMPT_MARKER = "valkey-ci-agent:ci-fix-attempt"
BRANCH_PREFIX = "agent/ci-fix/"
MAX_ATTEMPTS_PER_ISSUE = 3
# A comment holds at most 65536 characters, and the attempt marker stores the
# PR body base64-encoded; these bounds keep the whole comment well under it.
_MAX_BODY_CHARS = 20000
_MAX_SUMMARY_CHARS = 2000
_MAX_TRIAGE_CHARS = 4000
# Reconcile also looks at recently closed issues so an attempt whose issue was
# closed mid-verification still has its candidate branch cleaned up.
_CLOSED_LOOKBACK = timedelta(days=14)
_PENDING_LIMIT_S = 3 * 24 * 3600
_RUNNING_LIMIT_S = 3 * 3600
_PREVIOUS_RUN_SCAN = 50

_ATTEMPT_RE = re.compile(rf"<!-- {re.escape(ATTEMPT_MARKER)} (?P<data>[A-Za-z0-9+/=]+) -->")
_LAST_KEY_RE = re.compile(rf"<!-- {re.escape(MARKER_NAMESPACE)}:last-key:(?P<run>\d+) -->")
_OCCURRENCES_RE = re.compile(rf"<!-- {re.escape(MARKER_NAMESPACE)}:occurrences:(?P<count>\d+) -->")
_DETECTOR_RE = re.compile(rf"<!-- {re.escape(MARKER_NAMESPACE)}:(?P<fingerprint>[0-9a-f]+) -->")
_TEST_NAME_RE = re.compile(r"^- Test name: `(?P<value>.+)`\s*$", re.MULTILINE)
_TEST_FILE_RE = re.compile(r"^- Test file: `(?P<value>.+)`\s*$", re.MULTILINE)


@dataclass(frozen=True)
class IssueFailure:
    """The failure a detector issue tracks, read from its body."""

    number: int
    run_id: int          # the latest occurrence (the detector's last-key)
    test_name: str = ""
    test_file: str = ""
    job_name: str = ""   # set for a job-level failure issue
    occurrences: int = 1
    fingerprint: str = ""  # the detector's identity for the failure

    def describe(self, job: str) -> str:
        if self.job_name:
            return f"the failure of job `{job}`, which recorded no test failure"
        if self.test_name == FILE_TIMEOUT_TEST_NAME:
            return (
                f"a test client running `{self.test_file}` that timed out without "
                f"reporting a test name, in job `{job}`"
            )
        return f"`{self.test_name}` in `{self.test_file}`, as it failed in job `{job}`"

    def addressed_by(self, proposal: FixProposal | None) -> bool:
        """Whether a diagnosis is about this issue's test, when it names one.

        A Daily run holds many failures, and verification reruns only the
        target test, so a fix for another test could pass on a lucky run and
        open a PR that closes this issue.
        """
        if not self.test_name or self.test_name == FILE_TIMEOUT_TEST_NAME:
            return True
        if proposal is None:
            return False
        return _normalized(self.test_name) in _normalized(proposal.failing_check)


@dataclass
class Attempt:
    comment: Any
    data: dict[str, Any]

    @property
    def state(self) -> str:
        return str(self.data.get("state", ""))

    @property
    def run(self) -> int:
        value = self.data.get("run", 0)
        return value if isinstance(value, int) else 0


# --- Reading issues -----------------------------------------------------------


def parse_issue(issue: Any) -> IssueFailure | None:
    """Return the failure a detector-filed issue tracks, or None for any other issue."""
    body = str(getattr(issue, "body", "") or "")
    last = _LAST_KEY_RE.search(body)
    detector = _DETECTOR_RE.search(body)
    if detector is None or last is None:
        return None
    number = int(issue.number)
    run_id = int(last.group("run"))
    count = _OCCURRENCES_RE.search(body)
    occurrences = max(1, int(count.group("count"))) if count else 1
    job = parse_job_marker(body)
    if job:
        return IssueFailure(number=number, run_id=run_id, job_name=job, occurrences=occurrences,
                            fingerprint=detector.group("fingerprint"))
    name = _TEST_NAME_RE.search(body)
    file = _TEST_FILE_RE.search(body)
    if name is None or file is None:
        return None
    return IssueFailure(
        number=number, run_id=run_id,
        test_name=name.group("value"), test_file=file.group("value"), occurrences=occurrences,
    )


def attempts_for(issue: Any, bot_login: str) -> list[Attempt]:
    comments = retry_github_call(
        lambda: list(issue.get_comments()), retries=2,
        description=f"list comments on #{issue.number}",
    )
    attempts = []
    for comment in comments:
        if str(getattr(getattr(comment, "user", None), "login", "") or "") != bot_login:
            continue
        match = _ATTEMPT_RE.search(str(getattr(comment, "body", "") or ""))
        data = _decode(match.group("data")) if match else None
        if data is not None:
            attempts.append(Attempt(comment, data))
    return attempts


def _labelled_issues(repo: Any, *, state: str, since: datetime | None = None) -> list[Any]:
    kwargs: dict[str, Any] = {"state": state, "labels": [LABEL_NAME]}
    if since is not None:
        kwargs["since"] = since
    issues = retry_github_call(
        lambda: list(repo.get_issues(**kwargs)), retries=2, description=f"list {state} {LABEL_NAME} issues",
    )
    return drop_pull_requests(issues)


def _pr_is_open(repo: Any, number: Any) -> bool:
    if not isinstance(number, int) or number <= 0:
        return False
    pr = retry_github_call(lambda: repo.get_pull(number), retries=2, description=f"get PR #{number}")
    return str(getattr(pr, "state", "") or "") == "open"


def _has_open_linked_pr(issue: Any) -> bool:
    """True when an open PR references the issue (someone, maybe us, is fixing it)."""
    events = retry_github_call(
        lambda: list(issue.get_timeline()), retries=2, description=f"timeline of #{issue.number}",
    )
    for event in events:
        if getattr(event, "event", "") != "cross-referenced":
            continue
        source = getattr(getattr(event, "source", None), "issue", None)
        if source is not None and getattr(source, "pull_request", None) is not None \
                and str(getattr(source, "state", "") or "") == "open":
            return True
    return False


# --- Step 1: reconcile --------------------------------------------------------


def reconcile_attempts(
    gh: Any, artifact_client: ArtifactClient, repo_full_name: str, *, bot_login: str,
) -> list[dict[str, Any]]:
    """Finish every pending attempt whose verification runs have completed.

    Every open issue is checked, since a pending attempt there blocks new ones,
    plus recently closed issues so their candidate branches are cleaned up. One
    attempt's error is logged and does not stop the others.
    """
    repo = gh.get_repo(repo_full_name)
    since = datetime.now(timezone.utc) - _CLOSED_LOOKBACK
    issues = _labelled_issues(repo, state="open") + _labelled_issues(repo, state="closed", since=since)
    results = []
    for issue in issues:
        for attempt in attempts_for(issue, bot_login):
            if attempt.state not in ("pending", "running"):
                continue
            try:
                results.append(_reconcile(gh, artifact_client, repo, issue, attempt))
            except Exception:  # noqa: BLE001 - one bad attempt must not stall the queue
                logger.exception("could not reconcile the attempt on #%s", issue.number)
                results.append({"issue": int(issue.number), "action": "error"})
    return results


def _reconcile(
    gh: Any, artifact_client: ArtifactClient, repo: Any, issue: Any, attempt: Attempt,
) -> dict[str, Any]:
    data = attempt.data
    result = {"issue": int(issue.number), "branch": data.get("branch", "")}
    if attempt.state == "running":
        # The run that claimed it is still working, or died. The job timeout
        # is two hours, so an older claim belongs to a run that is gone.
        if time.time() - float(data.get("started_at") or 0) <= _RUNNING_LIMIT_S:
            return {**result, "action": "waiting"}
        return _end_attempt(repo, attempt, result, "the attempt's run stopped before it finished")
    if str(getattr(issue, "state", "") or "") != "open":
        _delete_branch(repo, data)
        data["state"] = "abandoned"
        _edit(attempt)
        return {**result, "action": "abandoned"}

    plan = DailyPlan.from_dict(data["plan"])
    try:
        baseline = evaluate_daily_run(gh, artifact_client, repo.full_name, int(data["baseline"]), plan)
        candidate = evaluate_daily_run(gh, artifact_client, repo.full_name, int(data["candidate"]), plan)
    except GithubException as exc:
        if exc.status != 404:
            raise
        return _end_attempt(repo, attempt, result, "a verification run no longer exists")
    if baseline is None or candidate is None:
        # Daily jobs time out after a day and GitHub drops runs queued for a
        # day, so a run unfinished after this long is never going to finish.
        if time.time() - float(data.get("dispatched_at") or 0) > _PENDING_LIMIT_S:
            return _end_attempt(repo, attempt, result, "the verification runs did not finish in time")
        return {**result, "action": "waiting"}

    if "cancelled" in (baseline.state, candidate.state):
        return _end_attempt(repo, attempt, result, "a verification run was cancelled, so the result is inconclusive")
    if not baseline.reproduces(plan):
        # A pass with the fix means nothing if the failure does not happen
        # without it.
        return _end_attempt(
            repo, attempt, result, f"the run without the fix did not reproduce the failure ({baseline.detail})",
        )
    if not candidate.verified:
        data["candidate_result"] = asdict(candidate)
        return _end_attempt(repo, attempt, result, candidate.detail)
    if _branch_sha(repo, data["branch"]) != data["sha"]:
        return _end_attempt(repo, attempt, result, "the candidate branch changed while it was being verified")

    body = data["body"] + _evidence(plan, baseline, candidate) + _PR_FOOTER
    pr = find_existing_pr(gh, repo.full_name, repo.full_name, data["branch"])
    if pr is None:
        pr = retry_github_call(
            lambda: create_pull_from_push_repo(
                repo, base_repo=repo.full_name, push_repo=None, title=data["title"],
                body=body, head_branch=data["branch"], base_branch=data["base"], draft=False,
            ),
            retries=2, description=f"open the fix PR for #{issue.number}",
        )
    data["state"] = "done"
    data["pr"] = int(pr.number)
    _edit(attempt)
    return {**result, "action": "opened", "pr": int(pr.number)}


def _end_attempt(repo: Any, attempt: Attempt, result: dict[str, Any], summary: str) -> dict[str, Any]:
    _delete_branch(repo, attempt.data)
    attempt.data["state"] = "failed"
    attempt.data["summary"] = summary
    _edit(attempt)
    return {**result, "action": "failed", "detail": summary}


def _evidence(plan: DailyPlan, baseline: DailyResult, candidate: DailyResult) -> str:
    # Only called once the baseline reproduced the failure. A whole-job rerun
    # shows only that the job failed, not how.
    before = f"reproduced the failure ({baseline.detail})" if plan.test_name else f"failed ({baseline.detail})"
    return (
        f"\n\n**Verification** in the Daily workflow, {plan.describe()}:\n"
        f"- Without the fix: [run]({baseline.run_url}) {before}.\n"
        f"- With the fix: [run]({candidate.run_url}) {candidate.detail}.\n"
    )


# --- Step 2: select and prepare -----------------------------------------------


def select_issue(
    gh: Any, repo_full_name: str, *, bot_login: str, issue_number: int = 0,
) -> tuple[Any, IssueFailure] | str:
    """Pick the issue to attempt, or return why there is none."""
    repo = gh.get_repo(repo_full_name)
    candidates = []
    for issue in _labelled_issues(repo, state="open"):
        # The label alone is not a trust anchor: Valkey's issue template adds
        # it to any user's report. Only issues the detector (this App) filed.
        if str(getattr(getattr(issue, "user", None), "login", "") or "") != bot_login:
            continue
        failure = parse_issue(issue)
        if failure is None:
            continue
        attempts = attempts_for(issue, bot_login)
        if any(attempt.state == "pending" for attempt in attempts):
            return "verification-in-flight"
        candidates.append((issue, failure, attempts))
    for issue, failure, attempts in sorted(candidates, key=lambda item: -item[1].run_id):
        if issue_number and failure.number != issue_number:
            continue
        if len(attempts) >= MAX_ATTEMPTS_PER_ISSUE:
            continue
        # One attempt per occurrence. A maintainer dispatching for this issue
        # explicitly may retry the same occurrence, within the overall cap.
        if not issue_number and any(attempt.run == failure.run_id for attempt in attempts):
            continue
        if getattr(issue, "assignees", None) or _has_open_linked_pr(issue):
            continue
        # The timeline's cross-reference to a PR we just opened can lag, so
        # also check the PRs our own attempts recorded.
        if any(_pr_is_open(repo, attempt.data.get("pr")) for attempt in attempts if attempt.state == "done"):
            continue
        return issue, failure
    return "no-actionable-issue"


def prepare_issue_fix(
    gh: Any,
    artifact_client: ArtifactClient,
    repo_full_name: str,
    *,
    bot_login: str,
    state_path: str,
    issue_number: int = 0,
    ignored_jobs: tuple[str, ...] = (),
) -> dict[str, Any]:
    picked = select_issue(gh, repo_full_name, bot_login=bot_login, issue_number=issue_number)
    if isinstance(picked, str):
        return {"repo": repo_full_name, "action": "skipped", "reason": picked}
    issue, failure = picked
    run_url = f"https://github.com/{repo_full_name}/actions/runs/{failure.run_id}"
    # Claim the occurrence on the issue before anything that can fail or run
    # long, so a crash or a cancelled job still leaves a recorded attempt and
    # the same occurrence is not retried every hour.
    started_at = int(time.time())
    claim = retry_github_call(
        lambda: issue.create_comment(render_attempt(
            {"v": 1, "run": failure.run_id, "run_url": run_url, "state": "running",
             "started_at": started_at})),
        retries=3, description=f"claim the fix attempt on #{failure.number}",
    )
    context: dict[str, Any] = {
        "issue": failure.number, "run": failure.run_id, "comment": int(claim.id), "started_at": started_at,
    }

    def record(outcome: FixOutcome, request: FixRequest | None = None) -> dict[str, Any]:
        write_state(state_path, {
            "context": context,
            "request": to_dict(request) if request is not None else None,
            "outcome": to_dict(outcome),
        })
        return {
            "repo": repo_full_name, "action": "prepared", "issue": failure.number,
            "run": failure.run_id, "decision": outcome.kind.value, "summary": outcome.summary,
        }

    record(_interrupted())
    request: FixRequest | None = None
    try:
        outcome, request = _prepare_attempt(
            gh, artifact_client, repo_full_name, failure, context,
            bot_login=bot_login, ignored_jobs=ignored_jobs,
        )
    except Exception:  # noqa: BLE001 - every attempt needs an audit result
        logger.exception("issue fix attempt failed unexpectedly")
        outcome = FixOutcome(
            kind=OutcomeKind.FAILED,
            summary="an internal error stopped the attempt; see the valkey-ci-agent workflow logs",
        )
    return record(outcome, request)


def _prepare_attempt(
    gh: Any,
    artifact_client: ArtifactClient,
    repo_full_name: str,
    failure: IssueFailure,
    context: dict[str, Any],
    *,
    bot_login: str,
    ignored_jobs: tuple[str, ...],
) -> tuple[FixOutcome, FixRequest | None]:
    """Plan the verification and run the engine; ``context`` gains job and plan."""
    repo = gh.get_repo(repo_full_name)
    run = retry_github_call(
        lambda: repo.get_workflow_run(failure.run_id), retries=2, description=f"get run {failure.run_id}",
    )
    # The run's logs and the code it tested drive the fix, so only the
    # project's own scheduled Daily run qualifies, never a pull request's.
    if (
        str(getattr(run, "event", "") or "") != "schedule"
        or str(getattr(run, "path", "") or "") != f".github/workflows/{DAILY_WORKFLOW}"
        or str(getattr(getattr(run, "head_repository", None), "full_name", "") or "") != repo_full_name
    ):
        return _refusal(f"run {failure.run_id} is not a scheduled {DAILY_WORKFLOW} run of {repo_full_name}"), None
    branch = str(getattr(run, "head_branch", "") or "")
    failing_sha = str(getattr(run, "head_sha", "") or "")

    job = _failing_job(gh, artifact_client, repo_full_name, run, failure, ignored_jobs)
    if job is None:
        return _refusal(f"I could not find the job in which this failed in run {failure.run_id}"), None
    context["job"] = job
    workflow = retry_github_call(
        lambda: repo.get_contents(f".github/workflows/{DAILY_WORKFLOW}", ref=repo.default_branch),
        retries=2, description=f"read {DAILY_WORKFLOW}",
    )
    plan = plan_daily_run(
        workflow.decoded_content.decode("utf-8", errors="replace"),
        job_name=job, test_file=failure.test_file,
        # A client timeout between tests has no test name to look for, so only
        # the job result can tell whether the file now passes.
        test_name="" if failure.test_name == FILE_TIMEOUT_TEST_NAME else failure.test_name,
        loops=stress_loops(),
    )
    if isinstance(plan, str):
        return _refusal(f"I cannot verify a fix for this in the Daily workflow: {plan}"), None
    context["plan"] = plan.to_dict()

    base_sha = retry_github_call(
        lambda: str(repo.get_branch(branch).commit.sha), retries=2, description=f"get {branch} tip",
    )
    # Only a first occurrence was introduced between the previous run and this
    # one; a recurrence was already failing before its range, so listing those
    # commits as culprit candidates could only blame an innocent one.
    previous = _previous_daily_sha(repo, run) if failure.occurrences == 1 else ""
    request = FixRequest(
        repo_full_name=repo_full_name,
        pr_number=0,
        head_repo_full_name=repo_full_name,
        # The claim comment id makes the name unique per attempt, so a retry of
        # the same occurrence never collides with an earlier attempt's branch.
        head_branch=f"{BRANCH_PREFIX}issue-{failure.number}-{failure.run_id}-{context['comment']}",
        head_sha=base_sha,
        run_id=failure.run_id,
        requested_by=bot_login,
        base_branch=branch,
        policy=Policy.FIX,
        publication=Publication.NEW_PR,
        execute=False,
        culprit_range=f"{previous}..{failing_sha}" if previous else "",
        failing_sha=failing_sha,
        issue_number=failure.number,
        target=failure.describe(job),
        job=job,
    )
    outcome = run_ci_fix_request(gh, request=request, failed_jobs=(job,), artifact_client=artifact_client)
    if outcome.kind is not OutcomeKind.READY:
        return outcome, request
    if not failure.addressed_by(outcome.proposal):
        named = outcome.proposal.failing_check if outcome.proposal else "another failure"
        return replace(
            outcome, kind=OutcomeKind.REFUSED,
            summary=f"the diagnosis addressed {named!r}, not the test this issue tracks",
        ), request
    workflows = [path for path in outcome.changed_paths if path.startswith(".github/workflows/")]
    if workflows:
        # Verification dispatches the Daily workflow from the default branch,
        # so a changed workflow file would never run.
        return replace(
            outcome, kind=OutcomeKind.REFUSED,
            summary=f"the fix changes {', '.join(workflows)}, which the Daily verification cannot exercise",
        ), request
    return outcome, request


def _failing_job(
    gh: Any, artifact_client: ArtifactClient, repo_full_name: str, run: Any,
    failure: IssueFailure, ignored_jobs: tuple[str, ...],
) -> str | None:
    """The job of ``run`` to verify against: one that really failed with this failure."""
    failed = failed_jobs_for_run(gh, repo_full_name, int(run.id))
    raw = download_all_test_failures(gh, repo_full_name, int(run.id), "", artifact_client=artifact_client)
    try:
        recorded = json.loads(raw) if raw else {}
    except ValueError:
        recorded = {}
    recorded = recorded if isinstance(recorded, dict) else {}
    from_logs, job_failures = find_unrecorded_failures(
        gh, artifact_client, repo_full_name, int(run.id), recorded, ignored_jobs=ignored_jobs,
    )
    if failure.job_name:
        # Any job the detector filed this failure for in this run (a merged
        # failure lists several, e.g. a Valgrind leg and its twin).
        names = {
            normalize_job_name(ref.job)
            for group in job_failures if job_fingerprint_for(group) == failure.fingerprint
            for ref in group.jobs
        }
    else:
        names = {
            normalize_job_name(ref.job)
            for test in merge_failures(parse_and_deduplicate(recorded, {}), from_logs)
            if (test.test_name, test.test_file) == (failure.test_name, failure.test_file)
            for ref in test.jobs
        }
    candidates = [job for job in failed if normalize_job_name(job.name) in names]
    return select_job(candidates).name if candidates else None


def _previous_daily_sha(repo: Any, run: Any) -> str:
    """The commit the scheduled run before ``run`` tested, or "" when unknown."""
    try:
        workflow = repo.get_workflow(int(run.workflow_id))
        runs = workflow.get_runs(branch=run.head_branch, event="schedule", status="completed")
        for index, previous in enumerate(runs):
            if index >= _PREVIOUS_RUN_SCAN:
                break
            # A cancelled or skipped run tested nothing, so it bounds nothing.
            if str(getattr(previous, "conclusion", "") or "") in ("cancelled", "skipped"):
                continue
            if int(previous.id) < int(run.id):
                sha = str(getattr(previous, "head_sha", "") or "")
                return sha if sha != run.head_sha else ""
    except Exception as exc:  # noqa: BLE001 - the culprit list is optional context
        logger.info("Could not find the previous Daily run: %s", exc)
    return ""


# --- Step 3: publish ----------------------------------------------------------


def publish_issue_fix(
    gh: Any, repo_full_name: str, *, git_env: dict[str, str], state_path: str,
) -> dict[str, Any]:
    state = read_state(state_path)
    if state is None:
        return {"repo": repo_full_name, "action": "skipped", "reason": "nothing-prepared"}
    context = state["context"]
    outcome = outcome_from_dict(state["outcome"])
    request = request_from_dict(state["request"]) if state.get("request") else None
    repo = gh.get_repo(repo_full_name)
    issue = retry_github_call(
        lambda: repo.get_issue(int(context["issue"])), retries=2, description="get the issue",
    )
    data: dict[str, Any] = {
        "v": 1,
        "run": int(context["run"]),
        "job": context.get("job", ""),
        "run_url": outcome.failing_run_url or f"https://github.com/{repo_full_name}/actions/runs/{context['run']}",
        "summary": _clip(outcome.summary, _MAX_SUMMARY_CHARS),
        "triage": _clip("\n".join(triage_lines(outcome)).strip(), _MAX_TRIAGE_CHARS),
        "state": "refused" if outcome.kind in (OutcomeKind.REFUSED, OutcomeKind.HANDOFF) else "failed",
    }
    if str(getattr(issue, "state", "") or "") != "open":
        data["state"] = "abandoned"
    elif outcome.kind is OutcomeKind.READY and request is not None:
        def checkpoint(fields: dict[str, Any]) -> None:
            # A branch that exists only in an unrecorded attempt can never be
            # cleaned up, so record it before the next side effect. If the
            # claim was deleted this posts a new comment; the final record
            # below must edit that one, not post a second.
            running = {**data, **fields, "state": "running", "started_at": context.get("started_at") or int(time.time())}
            context["comment"] = _record_attempt(issue, context.get("comment"), render_attempt(running))

        data.update(_start_verification(
            gh, repo, request, outcome, DailyPlan.from_dict(context["plan"]), git_env, checkpoint,
        ))
    _record_attempt(issue, context.get("comment"), render_attempt(data))
    return {"repo": repo_full_name, "action": data["state"], "issue": int(issue.number),
            "summary": data["summary"], "branch": data.get("branch", "")}


def _start_verification(
    gh: Any, repo: Any, request: FixRequest, outcome: FixOutcome, plan: DailyPlan,
    git_env: dict[str, str], checkpoint: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    """Push the candidate branch and dispatch both Daily runs; return attempt fields."""
    try:
        if outcome.port_commit:
            sha = commit_and_push_port(
                head_repo_full_name=request.head_repo_full_name, head_branch=request.head_branch,
                head_sha=request.head_sha, unstable_fix_commit=outcome.port_commit,
                git_env=git_env, create=True,
            )
        else:
            assert outcome.proposal is not None
            sha = commit_and_push_fix(
                patch=outcome.patch, changed_paths=outcome.changed_paths,
                head_repo_full_name=request.head_repo_full_name, head_branch=request.head_branch,
                head_sha=request.head_sha, proposal=outcome.proposal, git_env=git_env, create=True,
            )
    except PushRefused as exc:
        return {"state": "refused", "summary": str(exc)}
    fields: dict[str, Any] = {"branch": request.head_branch, "sha": sha}
    try:
        checkpoint(fields)
        baseline, baseline_url = dispatch_daily(
            gh, repo.full_name, ref=repo.default_branch, plan=plan, sha=request.head_sha,
        )
        candidate, candidate_url = dispatch_daily(
            gh, repo.full_name, ref=repo.default_branch, plan=plan, sha=sha,
        )
    except Exception as exc:  # noqa: BLE001 - report and clean up instead of leaving a branch behind
        logger.exception("could not dispatch the Daily verification")
        _delete_branch(repo, fields)
        return {"state": "failed", "summary": f"I could not start the Daily verification: {exc}"}
    return {
        **fields,
        "state": "pending",
        "base": request.base_branch,
        "base_sha": request.head_sha,
        "plan": plan.to_dict(),
        "baseline": baseline,
        "baseline_url": baseline_url,
        "candidate": candidate,
        "candidate_url": candidate_url,
        "dispatched_at": int(time.time()),
        "title": _pr_title(outcome),
        "body": _pr_body(request, outcome),
    }


def _pr_body(request: FixRequest, outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    run_url = outcome.failing_run_url
    lines = [f"Fixes #{request.issue_number}", ""]
    lines += [f"The [Daily run]({run_url}) on `{request.base_branch}` failed: {request.target}.", ""]
    lines += triage_lines(outcome)
    if outcome.port_commit:
        lines += [f"**Fix:** cherry-picks {outcome.port_commit} from the default branch.", ""]
    elif proposal is not None and proposal.reasoning:
        lines += [f"**Fix:** {proposal.reasoning}", ""]
    if outcome.review is not None and outcome.review.reasoning:
        lines += [f"**Review:** {outcome.review.reasoning}", ""]
    product = [path for path in outcome.changed_paths if not path.startswith("tests/")]
    if product:
        lines += [
            "> [!WARNING]",
            "> This changes product code, not only tests: "
            + ", ".join(f"`{path}`" for path in product)
            + ". Review it as a product fix.",
            "",
        ]
    return _clip("\n".join(lines), _MAX_BODY_CHARS)


_PR_FOOTER = (
    "\n\n---\nOpened by valkey-ci-agent. The commit is authored by the bot without "
    "a DCO sign-off: a maintainer who takes it over must review it and add their "
    "sign-off before merging."
)


def _pr_title(outcome: FixOutcome) -> str:
    """Name the PR after the test it fixes; the build-failure guess does not apply here."""
    if outcome.proposal is None:
        return "Port an upstream CI fix"
    if outcome.proposal.failing_check:
        return fit_subject(f"Fix {outcome.proposal.failing_check}")
    return commit_subject(outcome.proposal)


# --- Rendering and helpers ----------------------------------------------------


def render_attempt(data: dict[str, Any]) -> str:
    """The issue comment for one attempt; re-rendered whenever its state changes."""
    job = f" (`{data['job']}`)" if data.get("job") else ""
    lines = [f"**Automatic fix attempt** for the failure in [this run]({data.get('run_url', '')}){job}.", ""]
    if data.get("triage"):
        lines += [str(data["triage"]), ""]
    state = data.get("state")
    plan = DailyPlan.from_dict(data["plan"]) if data.get("plan") else None
    if state == "running":
        lines.append(
            "I am diagnosing this failure; this comment is updated with the result. If it "
            "is not, the attempt stopped before finishing and the valkey-ci-agent workflow "
            "logs have the details."
        )
    elif state == "pending" and plan is not None:
        lines.append(
            f"I pushed a candidate fix to `{data['branch']}` and am verifying it with the Daily "
            f"workflow, {plan.describe()}: {_run_link(data, 'baseline', 'without the fix')} and "
            f"{_run_link(data, 'candidate', 'with the fix')}. I will open a PR if the fix passes."
        )
    elif state == "done":
        lines.append(f"The fix passed verification; opened #{data['pr']}.")
    elif state == "failed" and data.get("candidate_result"):
        result = data["candidate_result"]
        lines.append(
            f"The candidate fix did not pass verification ([run]({result['run_url']}): "
            f"{result['detail']}), so I deleted `{data['branch']}`."
        )
    elif state == "abandoned":
        deleted = f", so I deleted `{data['branch']}`" if data.get("branch") else ""
        lines.append(f"The issue was closed before the attempt finished{deleted}.")
    elif state == "refused":
        lines.append(f"I did not prepare a fix: {data.get('summary', '')}")
    else:
        lines.append(f"The attempt did not complete: {data.get('summary', '')}")
    lines += ["", f"<!-- {ATTEMPT_MARKER} {_encode(data)} -->"]
    return "\n".join(lines)


def _run_link(data: dict[str, Any], key: str, label: str) -> str:
    url = data.get(f"{key}_url")
    return f"[{label}]({url})" if url else f"{label} (run {data.get(key)})"


def _record_attempt(issue: Any, comment_id: Any, body: str) -> int:
    """Replace the attempt's claim comment, or post one if the claim is gone; return its id."""
    comment = replace_or_post_comment(
        issue.get_comment, issue.create_comment, comment_id, body,
        description=f"record the fix attempt on #{issue.number}",
    )
    return int(getattr(comment, "id", 0) or 0)


def _normalized(text: str) -> str:
    return " ".join(text.lower().split())


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _edit(attempt: Attempt) -> None:
    retry_github_call(
        lambda: attempt.comment.edit(render_attempt(attempt.data)), retries=3,
        description="update the fix attempt",
    )


def _branch_sha(repo: Any, branch: str) -> str:
    try:
        ref = repo.get_git_ref(f"heads/{branch}")
    except GithubException as exc:
        if exc.status == 404:
            return ""
        raise
    return str(ref.object.sha)


def _delete_branch(repo: Any, data: dict[str, Any]) -> None:
    """Delete a candidate branch, but only ours and only if nobody moved it."""
    branch = str(data.get("branch", ""))
    if not branch.startswith(BRANCH_PREFIX) or not data.get("sha"):
        return
    try:
        if _branch_sha(repo, branch) != data["sha"]:
            return
        repo.get_git_ref(f"heads/{branch}").delete()
    except Exception as exc:  # noqa: BLE001 - a leftover branch is harmless
        logger.warning("Could not delete %s: %s", branch, exc)


def _encode(data: dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(data, sort_keys=True).encode("utf-8")).decode("ascii")


def _decode(text: str) -> dict[str, Any] | None:
    try:
        data = json.loads(base64.b64decode(text, validate=True).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) and data.get("v") == 1 else None


def _refusal(summary: str) -> FixOutcome:
    return FixOutcome(kind=OutcomeKind.REFUSED, summary=summary)


def _interrupted() -> FixOutcome:
    return FixOutcome(
        kind=OutcomeKind.FAILED,
        summary="the attempt stopped before it finished; see the valkey-ci-agent workflow logs",
    )


# --- CLI ----------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=("reconcile", "prepare", "publish"))
    parser.add_argument("--repo", required=True)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--state", default="", help="Handoff state file (prepare/publish)")
    parser.add_argument("--issue", type=int, default=0, help="Attempt this issue only")
    parser.add_argument("--target-token", default=os.environ.get("TARGET_TOKEN", ""))
    args = parser.parse_args(argv)
    if not args.target_token:
        parser.error("--target-token or TARGET_TOKEN is required")
    if args.step != "reconcile" and not args.state:
        parser.error("--state is required for prepare and publish")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    entry = load_registry(args.registry).get_repo(args.repo)
    gh = Github(auth=Auth.Token(args.target_token))
    artifact_client = ArtifactClient(gh, token=args.target_token)
    bot_login = os.environ.get("CI_FIX_ISSUES_BOT_LOGIN", f"{APP_LOGIN}[bot]")
    if args.step == "reconcile":
        result: Any = reconcile_attempts(gh, artifact_client, args.repo, bot_login=bot_login)
    elif args.step == "prepare":
        if not entry.automatic_issue_followup and not args.issue:
            result = {"repo": args.repo, "action": "skipped", "reason": "disabled"}
        else:
            result = prepare_issue_fix(
                gh, artifact_client, args.repo, bot_login=bot_login, state_path=args.state,
                issue_number=args.issue, ignored_jobs=entry.ci_followup_ignored_jobs,
            )
    else:
        with GitAuth(args.target_token, prefix="ci-fix-issues-git-askpass-") as auth:
            result = publish_issue_fix(gh, args.repo, git_env=auth.env(), state_path=args.state)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
