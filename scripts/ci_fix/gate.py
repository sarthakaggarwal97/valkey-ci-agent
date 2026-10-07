"""Authorization and integrity gate for ``@valkeyrie-ops fix [<ci-link>]``.

This is the security boundary for the comment front door. Nothing downstream
runs until every check here passes, and every check fails closed.

- Command shape: the comment must start with the fix command. A run link is
  optional; without one the gate picks the most deterministic failure among
  the completed runs on the PR's current head.
- Authorization: the commenter must be an active member of a configured GitHub
  team (``valkey-io/contributors``); a failed or negative membership read is a
  refusal, never a fallback to a looser check.
- SHA binding: the failed run must have tested the PR's current head SHA. If
  the branch moved, the log no longer describes the code, so we refuse rather
  than fix a stale failure.

The gate also decides, from the PR itself, what may happen to a fix. Only a
branch in a namespace this engine owns (``agent/backport/...``,
``agent/ci-fix/...``) on the repository itself is ever pushed to. Every other
PR, including other automation's ``agent/`` PRs, gets the fix as a suggestion,
and a fork's code is never run by the bot at all.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any

from scripts.ci_fix.models import FixRequest, Policy, Publication
from scripts.ci_fix.push import ALLOWED_BRANCH_PREFIXES
from scripts.ci_fix.verify.github_runs import failed_jobs_for_run, job_priority, select_job
from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN, BOT_LOGIN

logger = logging.getLogger(__name__)

# The comment must *begin* with the invocation (after optional leading
# whitespace) so quoting or mentioning the command mid-discussion does not
# trigger a fix. Either bot identity may drive it: the manual-dispatch bot or
# the App that opens the PRs (used by the comment poller); both come from
# scripts.common.identity so the accepted mentions track the real accounts. The
# rest is only the remainder of the invocation line, not the whole comment, so
# a multi-line conversational reply is not folded into the hint.
_COMMAND_RE = re.compile(
    r"^\s*@(?:" + "|".join(re.escape(login) for login in (BOT_LOGIN, APP_LOGIN))
    + r")\s+fix\b[^\S\n]*(?P<rest>[^\n]*)",
    re.IGNORECASE,
)
# Actions run URL: .../<owner>/<repo>/actions/runs/<run_id>
_RUN_URL_RE = re.compile(
    r"github\.com/(?P<owner>[A-Za-z0-9._-]+)/(?P<repo>[A-Za-z0-9._-]+)/actions/runs/(?P<run_id>\d+)"
    r"(?:/jobs?/(?P<job_id>\d+))?",
)

_DEFAULT_AUTH_TEAM = "contributors"
BACKPORT_BRANCH_PREFIX = "agent/backport/"
_FAILED_RUN_CONCLUSIONS = frozenset({"failure", "timed_out", "cancelled"})


@dataclass(frozen=True)
class ParsedCommand:
    run_owner: str
    run_repo: str
    run_id: int
    hint: str
    # Set when the link points at one job of the run (".../runs/<r>/job/<j>").
    job_id: int = 0


@dataclass(frozen=True)
class GateRejection:
    """A refusal with a human-readable reason for the PR comment."""

    reason: str


def parse_command(body: str) -> ParsedCommand | None:
    """Parse a comment body into a command, or None if it isn't one.

    ``fix <run-url> <hint>``, ``fix <hint>`` and a bare ``fix`` are commands.
    The run link may sit anywhere on the invocation line and be wrapped (``<...>``,
    a Markdown link); the rest of the line is the hint. A line that holds a URL
    but no Actions run link is rejected rather than read as a hint, so a
    mistyped link never targets a different run.
    """
    if not body:
        return None
    match = _COMMAND_RE.search(body)
    if not match:
        return None
    tokens = match.group("rest").split()
    for index, token in enumerate(tokens):
        url_match = _RUN_URL_RE.search(token)
        if url_match:
            return ParsedCommand(
                run_owner=url_match.group("owner"),
                run_repo=url_match.group("repo"),
                run_id=int(url_match.group("run_id")),
                hint=" ".join(tokens[:index] + tokens[index + 1:]),
                job_id=int(url_match.group("job_id") or 0),
            )
    if any("://" in token or "github.com/" in token.lower() for token in tokens):
        return None
    return ParsedCommand(run_owner="", run_repo="", run_id=0, hint=" ".join(tokens))


def parse_run_url(run_url: str) -> ParsedCommand | None:
    """Parse a bare Actions run URL (dispatch input) into a command without a hint."""
    match = _RUN_URL_RE.search(run_url or "")
    if not match:
        return None
    return ParsedCommand(
        run_owner=match.group("owner"), run_repo=match.group("repo"),
        run_id=int(match.group("run_id")), hint="", job_id=int(match.group("job_id") or 0),
    )


def is_authorized(
    gh: Any,
    org: str,
    team_slug: str,
    username: str,
    *,
    retries: int = 2,
) -> bool:
    """Return True only if ``username`` is allowed to drive the bot.

    ``username`` must be a GitHub-verified principal (``github.actor`` for a
    dispatch, or ``comment.user.login`` read from the API by the poller), never a
    value forwarded through an input any dispatcher could set.

    The primary source is active membership of ``org/team_slug``. Fails closed:
    any error reading membership (permission, network, missing team) returns
    False, and a ``pending`` invitation does not authorize.

    An explicit allowlist may be supplied via ``CI_FIX_AUTH_ALLOWLIST`` (a
    comma-separated list of logins). It is empty by default - in production the
    team membership check is the only path. It exists so the same gate can be
    exercised end-to-end in a fork environment where the production team is not
    readable, without weakening the default behavior.
    """
    if not username:
        return False
    if username in _auth_allowlist():
        logger.info("Authorizing %s via CI_FIX_AUTH_ALLOWLIST", username)
        return True
    try:
        team = retry_github_call(
            lambda: gh.get_organization(org).get_team_by_slug(team_slug),
            retries=retries, description=f"get team {org}/{team_slug}",
        )
        membership = retry_github_call(
            lambda: team.get_team_membership(username),
            retries=retries, description=f"team membership {username}",
        )
    except Exception as exc:  # noqa: BLE001 - fail closed on any read error
        logger.warning("Authorization check failed closed for %s: %s", username, exc)
        return False
    state = getattr(membership, "state", None)
    return state == "active"


def _auth_allowlist() -> frozenset[str]:
    raw = os.environ.get("CI_FIX_AUTH_ALLOWLIST", "")
    return frozenset(login.strip() for login in raw.split(",") if login.strip())


def build_fix_request(
    gh: Any,
    *,
    command: ParsedCommand,
    pr_repo_full_name: str,
    pr_number: int,
    commenter: str,
    org: str = "valkey-io",
    auth_team: str = _DEFAULT_AUTH_TEAM,
    retries: int = 2,
) -> FixRequest | GateRejection:
    """Run all gate checks and return a FixRequest or a GateRejection.

    Assumes the comment was already confirmed to be on a pull request by the
    caller; this function enforces authorization, run ownership, and SHA
    binding, and decides the publication mode from the PR head.
    """
    if not is_authorized(gh, org, auth_team, commenter, retries=retries):
        return GateRejection(
            reason=f"@{commenter} is not an active member of {org}/{auth_team}; refusing."
        )

    if command.run_id:
        run_repo_full_name = f"{command.run_owner}/{command.run_repo}"
        if run_repo_full_name != pr_repo_full_name:
            return GateRejection(
                reason=(
                    f"The linked run belongs to {run_repo_full_name}, not this PR's "
                    f"repository {pr_repo_full_name}; refusing."
                )
            )

    try:
        repo = retry_github_call(
            lambda: gh.get_repo(pr_repo_full_name), retries=retries,
            description=f"get repo {pr_repo_full_name}",
        )
        pr = retry_github_call(
            lambda: repo.get_pull(pr_number), retries=retries, description=f"get PR #{pr_number}",
        )
    except Exception as exc:  # noqa: BLE001 - fail closed
        return GateRejection(reason=f"Could not load PR #{pr_number}: {exc}")

    pr_head_sha = str(getattr(pr.head, "sha", "") or "")
    pr_head_ref = str(getattr(pr.head, "ref", "") or "")
    pr_head_repo = str(getattr(getattr(pr.head, "repo", None), "full_name", "") or "")
    pr_base_ref = str(getattr(pr.base, "ref", "") or "")
    if not pr_head_sha:
        return GateRejection(reason="Could not determine the PR head commit; refusing.")
    if not pr_head_repo:
        return GateRejection(reason="Could not determine the PR head repository; refusing.")

    target = selected_job = ""
    if command.run_id:
        try:
            run = retry_github_call(
                lambda: repo.get_workflow_run(command.run_id), retries=retries,
                description=f"get run {command.run_id}",
            )
        except Exception as exc:  # noqa: BLE001 - fail closed
            return GateRejection(reason=f"Could not load run {command.run_id}: {exc}")
    else:
        picked = _most_actionable_failed_run(gh, repo, pr_repo_full_name, pr_head_sha, retries=retries)
        if picked is None:
            return GateRejection(
                reason=(
                    f"No completed, failed CI run found for this PR's head "
                    f"{pr_head_sha[:12]}; link the failing run to target one."
                )
            )
        run, selected_job = picked
        target = f"the failure in job `{selected_job}`"

    run_status = str(getattr(run, "status", "") or "")
    if run_status != "completed":
        return GateRejection(
            reason=(
                f"The linked run is not finished yet (status: {run_status or 'unknown'}). "
                "Its logs are only available once it completes - re-run me when it has."
            )
        )
    # We deliberately do NOT gate on the run's overall conclusion. A run can be
    # "cancelled" overall (a manual stop, or fail-fast after one job failed) yet
    # still contain genuine job failures worth fixing. Whether there is a real
    # failure to act on is decided per-job downstream.

    run_head_sha = str(getattr(run, "head_sha", "") or "")
    run_head_branch = str(getattr(run, "head_branch", "") or "")
    if not run_head_sha:
        return GateRejection(reason="Could not determine the run's head commit; refusing.")
    if run_head_sha != pr_head_sha:
        return GateRejection(
            reason=(
                "The PR branch has moved since this run "
                f"(run built {run_head_sha[:12]}, PR head is "
                f"{pr_head_sha[:12]}); re-run CI and try again."
            )
        )
    if run_head_branch and pr_head_ref and run_head_branch != pr_head_ref:
        return GateRejection(
            reason=(
                f"The run's branch ({run_head_branch}) does not match the PR head "
                f"branch ({pr_head_ref}); refusing."
            )
        )

    if command.run_id:
        failed = failed_jobs_for_run(gh, pr_repo_full_name, command.run_id, retries=retries)
        if not failed:
            # Without a failed job nothing could be verified, and a fork's fix
            # is never run here, so the AI would be guessing at a green run.
            return GateRejection(reason=f"Run {command.run_id} has no failed job to fix.")
    if command.job_id:  # a job link names the failure to fix
        job = next((job for job in failed if job.id == command.job_id), None)
        if job is None:
            return GateRejection(
                reason=f"Job {command.job_id} is not a failed job of run {command.run_id}; refusing."
            )
        selected_job = job.name
        target = f"the failure in job `{selected_job}`"

    same_repo = pr_head_repo == pr_repo_full_name
    bot_branch = same_repo and pr_head_ref.startswith(ALLOWED_BRANCH_PREFIXES)
    return FixRequest(
        repo_full_name=pr_repo_full_name,
        pr_number=pr_number,
        head_repo_full_name=pr_head_repo,
        head_branch=pr_head_ref,
        head_sha=pr_head_sha,
        run_id=int(getattr(run, "id", 0) or command.run_id),
        requested_by=commenter,
        hint=command.hint,
        base_branch=pr_base_ref,
        policy=(
            Policy.BACKPORT
            if bot_branch and pr_head_ref.startswith(BACKPORT_BRANCH_PREFIX)
            else Policy.FIX
        ),
        publication=Publication.PUSH if bot_branch else Publication.SUGGEST,
        # A fork's code is never run here; see the module docstring.
        execute=same_repo,
        target=target,
        job=selected_job,
    )


def _most_actionable_failed_run(
    gh: Any, repo: Any, repo_full_name: str, head_sha: str, *, retries: int,
) -> tuple[Any, str] | None:
    """Pick the completed run on ``head_sha`` holding the most deterministic failure.

    Returns the run and the chosen job's name, so the diagnosis works on the
    same failure the choice was made for.
    """
    # A listing error propagates: "no failed run" would be a false answer.
    runs = retry_github_call(
        lambda: list(repo.get_workflow_runs(head_sha=head_sha)), retries=retries,
        description=f"list runs for {head_sha[:12]}",
    )
    best: tuple[tuple[int, int, str], Any, str] | None = None
    for run in runs:
        if str(getattr(run, "head_sha", "") or "") != head_sha:
            continue
        if str(getattr(run, "status", "") or "") != "completed":
            continue
        if str(getattr(run, "conclusion", "") or "") not in _FAILED_RUN_CONCLUSIONS:
            continue
        failed = failed_jobs_for_run(gh, repo_full_name, int(run.id), retries=retries)
        if not failed:
            continue
        job = select_job(failed)
        key = (job_priority(job.name), -int(run.id), job.name)
        if best is None or key < best[0]:
            best = (key, run, job.name)
    return (best[1], best[2]) if best else None
