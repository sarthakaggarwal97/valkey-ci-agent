"""Automatically diagnose current-head CI failures on open sweep PRs.

The follow-up is deliberately narrower than the maintainer-triggered CI-fix
entry point. It acts only on the one bot-owned ``agent/backport/sweep/<base>``
PR for a registered branch, only after every current-head workflow run has
completed, and never retries the same logical workflow job on one head. The
shared CI-fix engine still owns diagnosis, verification, skeptical review, and
lease-protected push.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Iterable

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.backport.registry import RepoEntry, load_registry
from scripts.backport.sweep import _BRANCH_PREFIX
from scripts.backport.sweep_prs import find_existing_pr
from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.models import FixOutcome, FixRequest, OutcomeKind
from scripts.ci_fix.pipeline import run_ci_fix_request
from scripts.ci_fix.review import DEFAULT_VERIFY_RUNS
from scripts.ci_fix.verify.base import FailedJob, VerifyBackend
from scripts.ci_fix.verify.github_runs import failed_jobs_for_run
from scripts.ci_fix.verify.macos import MacosVerifier
from scripts.common.git_auth import GitAuth
from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN
from scripts.common.polling import env_int
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

_MARKER_RE = re.compile(
    r"<!-- valkey-ci-agent:auto-ci-followup "
    r"head=(?P<head>[0-9a-f]{40}) run=(?P<run>\d+) job=(?P<job>\d+)"
    r"(?: key=(?P<key>[0-9a-f]{64}))? -->"
)
# A run can conclude "cancelled" (a manual stop, or a concurrency cancel) and
# still contain a job that genuinely failed before the cancel landed. Matching
# the interactive gate, run-level conclusion is only a cheap filter for "no job
# can have failed here"; `failed_jobs_for_run` is what decides truth per job,
# and it deliberately ignores cancelled *jobs*.
_FAILED_RUN_CONCLUSIONS = {"failure", "timed_out", "cancelled"}
_MAX_ATTEMPTS_PER_PR = 3


@dataclass(frozen=True)
class FollowupTarget:
    pr: Any
    run: Any
    head_sha: str
    head_branch: str
    jobs: tuple[FailedJob, ...]


@dataclass(frozen=True)
class _HandledFailures:
    job_ids: frozenset[int]
    job_keys: frozenset[str]
    attempt_count: int


def find_followup_target(
    gh: Any,
    *,
    repo_entry: RepoEntry,
    target_branch: str,
    bot_login: str,
) -> tuple[FollowupTarget | None, str]:
    """Return one safe, unhandled current-head failure or a skip reason."""
    if not repo_entry.automatic_ci_followup:
        return None, "disabled"
    if repo_entry.effective_push_repo != repo_entry.repo:
        return None, "fork-head-unsupported"

    head_branch = f"{_BRANCH_PREFIX}/{target_branch}"
    pr = find_existing_pr(
        gh,
        repo_entry.repo,
        repo_entry.effective_push_repo,
        head_branch,
    )
    if pr is None:
        return None, "no-open-sweep-pr"

    rejection = _validate_sweep_pr(
        pr,
        repo_full_name=repo_entry.repo,
        target_branch=target_branch,
        head_branch=head_branch,
        bot_login=bot_login,
    )
    if rejection:
        return None, rejection

    head_sha = str(getattr(pr.head, "sha", "") or "")
    handled = _handled_failures(pr, head_sha, bot_login)
    if handled.attempt_count >= _MAX_ATTEMPTS_PER_PR:
        return None, "attempt-budget-exhausted"
    repo = gh.get_repo(repo_entry.repo)
    # Filter by head SHA server-side: a long-lived sweep branch accumulates a
    # run per push, and paging its whole history every hour just to discard
    # older heads wastes the target repository's API budget. The client-side
    # re-check below still holds, so a server that ignores the filter cannot
    # widen the target beyond the current head.
    current_runs = []
    for run in repo.get_workflow_runs(branch=head_branch, head_sha=head_sha):
        if str(getattr(run, "head_sha", "") or "") != head_sha:
            continue
        current_runs.append(run)

    if not current_runs:
        return None, "no-current-head-runs"
    if any(str(getattr(run, "status", "") or "") != "completed" for run in current_runs):
        return None, "current-head-ci-running"

    candidates: list[tuple[int, FollowupTarget]] = []
    for run in current_runs:
        conclusion = str(getattr(run, "conclusion", "") or "")
        if conclusion not in _FAILED_RUN_CONCLUSIONS:
            continue
        failed = tuple(
            job
            for job in failed_jobs_for_run(gh, repo_entry.repo, int(run.id))
            if job.id
            and job.id not in handled.job_ids
            and _job_key(run, job.name) not in handled.job_keys
            and not _ignored_job(job.name, repo_entry.ci_followup_ignored_jobs)
        )
        if not failed:
            continue
        selected = _select_job(failed)
        priority = _job_priority(selected.name)
        candidates.append(
            (
                priority,
                FollowupTarget(
                    pr=pr,
                    run=run,
                    head_sha=head_sha,
                    head_branch=head_branch,
                    jobs=(selected,),
                ),
            )
        )

    if not candidates:
        return None, "no-unhandled-actionable-failures"
    candidates.sort(key=lambda item: (item[0], -int(item[1].run.id)))
    return candidates[0][1], "actionable"


def run_followup(
    gh: Any,
    *,
    repo_entry: RepoEntry,
    target_branch: str,
    bot_login: str,
    git_env: dict[str, str],
    artifact_client: ArtifactClient,
    macos_verifier: VerifyBackend | None = None,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
) -> dict[str, Any]:
    """Attempt at most one fix for the current head and record the outcome."""
    target, reason = find_followup_target(
        gh,
        repo_entry=repo_entry,
        target_branch=target_branch,
        bot_login=bot_login,
    )
    if target is None:
        return {
            "repo": repo_entry.repo,
            "branch": target_branch,
            "action": "skipped",
            "reason": reason,
        }

    selected_job = _select_job(target.jobs)
    jobs = (selected_job,)
    job_names = (selected_job.name,)
    request = FixRequest(
        repo_full_name=repo_entry.repo,
        pr_number=int(target.pr.number),
        head_repo_full_name=repo_entry.repo,
        head_branch=target.head_branch,
        head_sha=target.head_sha,
        run_id=int(target.run.id),
        requested_by=bot_login,
        hint=(
            "Automatic backport CI follow-up. Prioritize deterministic failures "
            "in this order: " + ", ".join(job_names)
        )[:500],
    )
    # Claim the logical job *before* the engine runs. The claim comment carries
    # the same hidden markers as the result, so a crash, an OOM kill, or the job
    # timeout can no longer leave the attempt unrecorded - without this, every
    # scheduled run would re-diagnose the same failure on the same head forever.
    # The claim is edited in place afterwards, so one attempt is still one
    # comment on the PR.
    claim = retry_github_call(
        lambda: target.pr.create_issue_comment(
            _render_claim_comment(request, target.run, jobs)
        ),
        retries=3,
        description=f"claim automatic CI follow-up on #{request.pr_number}",
    )

    try:
        def pre_push_check() -> str:
            """Revalidate the automation's authority at the last possible moment."""
            try:
                current = retry_github_call(
                    lambda: gh.get_repo(repo_entry.repo).get_pull(request.pr_number),
                    retries=2,
                    description=f"revalidate PR #{request.pr_number} before push",
                )
            except Exception:  # noqa: BLE001 - a failed authorization check must deny the push
                logger.exception("could not revalidate automatic follow-up before push")
                return "the automatic follow-up PR could not be revalidated"

            rejection = _validate_sweep_pr(
                current,
                repo_full_name=repo_entry.repo,
                target_branch=target_branch,
                head_branch=request.head_branch,
                bot_login=bot_login,
            )
            if rejection:
                return f"automatic follow-up authorization expired ({rejection})"
            current_sha = str(getattr(current.head, "sha", "") or "")
            if current_sha != request.head_sha:
                return (
                    "the PR head moved from "
                    f"{request.head_sha[:12]} to {current_sha[:12] or '(missing)'}"
                )
            return ""

        outcome = run_ci_fix_request(
            gh,
            request=request,
            failed_jobs=job_names,
            git_env=git_env,
            artifact_client=artifact_client,
            macos_verifier=macos_verifier,
            verify_runs=verify_runs,
            pre_push_check=pre_push_check,
        )
    except Exception:  # noqa: BLE001 - every automatic attempt needs an audit result
        logger.exception("automatic CI follow-up failed unexpectedly")
        outcome = FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=(
                "An internal error stopped automatic CI follow-up; see the "
                "valkey-ci-agent workflow logs for details."
            ),
        )

    current_pr = retry_github_call(
        lambda: gh.get_repo(repo_entry.repo).get_pull(request.pr_number),
        retries=2,
        description=f"recheck PR #{request.pr_number} head",
    )
    current_sha = str(getattr(current_pr.head, "sha", "") or "")
    pushed = outcome.kind is OutcomeKind.PUSHED and bool(outcome.commit_sha)
    head_mismatch_after_push = pushed and current_sha != outcome.commit_sha
    stale = (
        not outcome.commit_sha
        if outcome.kind is OutcomeKind.PUSHED
        else current_sha != request.head_sha
    )
    if stale:
        body = _render_stale_comment(request, target.run, jobs)
    else:
        body = _render_followup_comment(outcome, request, target.run, jobs)
        if head_mismatch_after_push:
            body += _render_post_push_head_mismatch(outcome.commit_sha, current_sha)
    retry_github_call(
        lambda: claim.edit(body),
        retries=3,
        description=f"post automatic CI follow-up on #{request.pr_number}",
    )
    result = {
        "repo": repo_entry.repo,
        "branch": target_branch,
        "action": "stale" if stale else outcome.kind.value,
        "pr": request.pr_number,
        "head": request.head_sha,
        "run": request.run_id,
        "jobs": [job.name for job in jobs],
    }
    if stale:
        result["reason"] = "PR head moved during follow-up; the result was discarded"
    else:
        result["summary"] = outcome.summary
        if head_mismatch_after_push:
            result["head_moved_after_push"] = True
            result["current_head"] = current_sha
    return result


def _validate_sweep_pr(
    pr: Any,
    *,
    repo_full_name: str,
    target_branch: str,
    head_branch: str,
    bot_login: str,
) -> str:
    """Return ``""`` when this PR may be auto-fixed, or a skip-reason slug.

    This is the gate that replaces the human-team membership check the
    maintainer-triggered entry point relies on, so every field is checked
    explicitly rather than trusted: open, exact base and head refs, head in the
    target repository itself, a 40-hex head SHA, and bot authorship. Anything
    else is a PR the automation has no mandate over.
    """
    if str(getattr(pr, "state", "open") or "") != "open":
        return "pr-not-open"
    if str(getattr(pr.base, "ref", "") or "") != target_branch:
        return "base-branch-mismatch"
    if str(getattr(pr.head, "ref", "") or "") != head_branch:
        return "head-branch-mismatch"
    head_repo = str(getattr(getattr(pr.head, "repo", None), "full_name", "") or "")
    if head_repo != repo_full_name:
        return "head-repository-mismatch"
    if not re.fullmatch(r"[0-9a-f]{40}", str(getattr(pr.head, "sha", "") or "")):
        return "invalid-head-sha"
    author = str(getattr(getattr(pr, "user", None), "login", "") or "")
    if author != bot_login:
        return "pr-not-bot-owned"
    return ""


def _handled_failures(
    pr: Any,
    head_sha: str,
    bot_login: str,
) -> _HandledFailures:
    """Return the PR attempt count and jobs already attempted on this head.

    A bot-authored comment with at least one valid marker counts as one attempt,
    regardless of how many jobs it names. Exact job ids and logical keys count
    only when the marker's head matches, so an older attempt cannot suppress a
    different failure on the current head. Logical keys suppress the twin
    push/pull_request run of the same workflow job, whose GitHub job ids are
    necessarily different.
    """
    job_ids: set[int] = set()
    job_keys: set[str] = set()
    attempt_count = 0
    comments = retry_github_call(
        lambda: list(pr.get_issue_comments()),
        retries=2,
        description=f"list follow-up markers on #{pr.number}",
    )
    for comment in comments:
        author = str(getattr(getattr(comment, "user", None), "login", "") or "")
        if author != bot_login:
            continue
        matches = tuple(
            _MARKER_RE.finditer(str(getattr(comment, "body", "") or ""))
        )
        if matches:
            attempt_count += 1
        for match in matches:
            if match.group("head") == head_sha:
                job_ids.add(int(match.group("job")))
                if match.group("key"):
                    job_keys.add(match.group("key"))
    return _HandledFailures(
        job_ids=frozenset(job_ids),
        job_keys=frozenset(job_keys),
        attempt_count=attempt_count,
    )


def _ignored_job(name: str, patterns: Iterable[str]) -> bool:
    lowered = name.lower()
    return any(fnmatch(lowered, pattern.lower()) for pattern in patterns)


def _job_priority(name: str) -> int:
    """Rank a failed job by how deterministic its failure usually is.

    Only the best tier present gets an attempt, so the ordering matters: a
    formatting or build break has one obvious fix, a test failure needs
    diagnosis, and a sanitizer or valgrind failure is the most likely to be
    flaky or to need a judgement call. Unrecognised names sort last.
    """
    lowered = name.lower()
    if any(
        token in lowered
        for token in ("format", "lint", "schema", "generated", "build", "compile")
    ):
        return 0
    if any(
        token in lowered
        for token in ("asan", "ubsan", "tsan", "sanitizer", "valgrind")
    ):
        return 2
    if any(token in lowered for token in ("unit", "integration", "test")):
        return 1
    return 3


def _select_job(jobs: Iterable[FailedJob]) -> FailedJob:
    """Choose one deterministic failure for this automatic attempt."""
    return min(
        jobs,
        key=lambda job: (
            _job_priority(job.name),
            int(job.id),
            job.name.lower(),
        ),
    )


def _job_key(run: Any, job_name: str) -> str:
    """Identify one logical job across twin events for the same workflow/head."""
    workflow_identity = ""
    for attribute in ("workflow_id", "path", "name"):
        value = str(getattr(run, attribute, "") or "").strip()
        if value:
            workflow_identity = f"{attribute}:{value}"
            break
    if not workflow_identity:
        # Fail open when a partial API object exposes no workflow identity:
        # distinct runs must not suppress one another merely because they both
        # contain a generic job name such as "build".
        workflow_identity = f"run:{int(getattr(run, 'id', 0) or 0)}"
    normalized_name = " ".join(job_name.casefold().split())
    return hashlib.sha256(
        f"{workflow_identity}\0{normalized_name}".encode()
    ).hexdigest()


def _markers(
    request: FixRequest,
    run: Any,
    jobs: tuple[FailedJob, ...],
) -> str:
    """Render hidden exact/logical claim markers read by ``_handled_failures``."""
    return "\n".join(
        f"<!-- valkey-ci-agent:auto-ci-followup head={request.head_sha} "
        f"run={request.run_id} job={job.id} key={_job_key(run, job.name)} -->"
        for job in jobs
    )


def _render_claim_comment(
    request: FixRequest,
    run: Any,
    jobs: tuple[FailedJob, ...],
) -> str:
    """Render the pre-attempt claim, replaced in place once the engine returns."""
    names = ", ".join(f"`{job.name}`" for job in jobs) or "the current failure"
    return (
        "Automatic follow-up for the current backport head is diagnosing "
        f"{names} at `{request.head_sha[:12]}`. This comment is replaced with "
        "the result; if it is not, the agent run itself failed and the "
        "valkey-ci-agent workflow logs have the details.\n\n"
        + _markers(request, run, jobs)
    )


def _render_stale_comment(
    request: FixRequest,
    run: Any,
    jobs: tuple[FailedJob, ...],
) -> str:
    """Render the outcome for an attempt whose head moved while it ran."""
    return (
        "Automatic follow-up for `"
        + request.head_sha[:12]
        + "` was discarded: this PR's head moved while the fix was being "
        "verified, so nothing was pushed. CI on the new head is followed up "
        "separately.\n\n"
        + _markers(request, run, jobs)
    )


def _render_followup_comment(
    outcome: FixOutcome,
    request: FixRequest,
    run: Any,
    jobs: tuple[FailedJob, ...],
) -> str:
    """Render the engine's outcome plus markers that retire this logical job."""
    return (
        "Automatic follow-up for the current backport head.\n\n"
        + render_comment(outcome)
        + "\n\n"
        + _markers(request, run, jobs)
    )


def _render_post_push_head_mismatch(pushed_sha: str, current_sha: str) -> str:
    """Explain a post-push recheck mismatch without denying the real push."""
    return (
        "\n\nThe fix was pushed as `"
        + pushed_sha[:12]
        + "`. At the post-push recheck, the PR API reported `"
        + (current_sha[:12] or "(missing)")
        + "` as the current head, so CI follow-up will evaluate that head "
        "separately."
    )


def _macos_verifier() -> VerifyBackend | None:
    """Build the macOS verification backend, or ``None`` when it is not configured.

    Both ``CI_FIX_MACOS_AGENT_REPO`` and ``CI_FIX_MACOS_TOKEN`` have to be set, so
    macOS verification stays opt-in and a deployment without it degrades to the
    other backends instead of failing.
    """
    agent_repo = os.environ.get("CI_FIX_MACOS_AGENT_REPO", "")
    token = os.environ.get("CI_FIX_MACOS_TOKEN", "")
    if not agent_repo or not token:
        return None
    agent_gh = Github(auth=Auth.Token(token))
    return MacosVerifier(
        agent_gh,
        agent_repo_full_name=agent_repo,
        ref=os.environ.get("CI_FIX_MACOS_AGENT_REF", "main"),
        artifact_client=ArtifactClient(agent_gh, token=token),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--target-token", default=os.environ.get("TARGET_TOKEN", ""))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    if not args.target_token:
        parser.error("--target-token or TARGET_TOKEN is required")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    registry = load_registry(args.registry)
    repo_entry, _branch_entry = registry.get_branch(args.repo, args.branch)
    gh = Github(auth=Auth.Token(args.target_token))
    bot_login = os.environ.get("AUTO_CI_FOLLOWUP_BOT_LOGIN", f"{APP_LOGIN}[bot]")
    with GitAuth(args.target_token, prefix="backport-ci-followup-git-askpass-") as auth:
        result = run_followup(
            gh,
            repo_entry=repo_entry,
            target_branch=args.branch,
            bot_login=bot_login,
            git_env=auth.env(),
            artifact_client=ArtifactClient(gh, token=args.target_token),
            macos_verifier=_macos_verifier(),
            verify_runs=env_int("CI_FIX_VERIFY_RUNS", DEFAULT_VERIFY_RUNS, minimum=1, maximum=10),
        )
    print(json.dumps(result, indent=2))
    return 1 if result.get("action") == OutcomeKind.FAILED.value else 0


if __name__ == "__main__":
    raise SystemExit(main())
