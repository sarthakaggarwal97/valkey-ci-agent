"""Entry point for the ``@valkeyrie-ops fix [<ci-link>]`` workflow.

Three steps connected by a state file:

- ``gate`` authorizes the commenter and binds the run to the PR head. No AI or
  PR code runs yet. It records the request with an "interrupted" outcome, so a
  later step killed by a timeout still leaves something to report, and tells
  the workflow whether publication may push (a bot-owned ``agent/...`` branch)
  or only comment (any other PR), so the workflow mints a token no wider.
- ``prepare`` runs the engine with read-only credentials.
- ``publish`` receives the token the gate's decision allowed. It pushes to the
  bot-owned PR branch or posts the fix as a suggestion, then comments the
  outcome and reacts to the command.

``gate`` takes the command pieces from ``workflow_dispatch`` inputs, or a raw
``issue_comment`` event payload (``--event-path``). When the input is not an
actionable fix command it records nothing and exits 0.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.gate import GateRejection, ParsedCommand, build_fix_request, parse_command, parse_run_url
from scripts.ci_fix.models import FixOutcome, OutcomeKind, outcome_from_dict, request_from_dict, to_dict
from scripts.ci_fix.pipeline import run_ci_fix_request
from scripts.ci_fix.publish import pr_head_check, publish_to_pr, read_state, write_state
from scripts.ci_fix.review import DEFAULT_VERIFY_RUNS
from scripts.ci_fix.verify.macos import macos_verifier_from_env
from scripts.common.git_auth import GitAuth
from scripts.common.github_client import retry_github_call
from scripts.common.logging_utils import configure_logging, log_outcome, workflow_logs
from scripts.common.polling import env_int
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

# The team authorization is configurable so the same entry point can run
# against a different org/team in a fork test environment. Defaults to the
# production target; override only via these env vars.
_AUTH_ORG = os.environ.get("CI_FIX_AUTH_ORG", "valkey-io")
_AUTH_TEAM = os.environ.get("CI_FIX_AUTH_TEAM", "contributors")

_MAX_VERIFY_RUNS = 10

_INTERRUPTED = FixOutcome(
    kind=OutcomeKind.FAILED,
    summary=f"The run stopped before it finished; see {workflow_logs()}.",
)


def _verify_runs() -> int:
    """Return the local/Docker verification repeat count."""
    return env_int(
        "CI_FIX_VERIFY_RUNS",
        DEFAULT_VERIFY_RUNS,
        minimum=1,
        maximum=_MAX_VERIFY_RUNS,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="step", required=True)

    gate = sub.add_parser("gate", help="Authorize and bind the request; no AI runs")
    gate.add_argument("--event-path", default="", help="Path to an issue_comment event JSON")
    # Dispatch mode: supply the command pieces directly instead of an event.
    gate.add_argument("--repo", default="", help="PR repository (owner/name)")
    gate.add_argument("--pr", type=int, default=0, help="PR number")
    gate.add_argument("--run-url", default="", help="Failing CI run URL (optional)")
    gate.add_argument("--commenter", default="", help="Requesting user login")
    gate.add_argument("--hint", default="", help="Optional diagnosis hint")
    gate.add_argument("--comment-id", type=int, default=0,
                      help="Triggering comment id, reacted to with the outcome")

    sub.add_parser("prepare", help="Diagnose and verify the gated request with read-only credentials")

    publish = sub.add_parser("publish", help="Publish the prepared decision with a write token")
    publish.add_argument("--publication", required=True, choices=_GATE_PUBLICATIONS,
                         help="The gate step's publication output")

    for step in sub.choices.values():
        step.add_argument("--state", required=True, help="Path of the handoff state file")
        step.add_argument("--target-token", default=os.environ.get("TARGET_TOKEN", ""),
                          help="GitHub App installation token for this step")
    args = parser.parse_args(argv)

    configure_logging()
    if not args.target_token:
        parser.error("--target-token/TARGET_TOKEN is required")
    if args.step == "gate":
        return _gate(args)
    if args.step == "prepare":
        return _prepare(args.state, args.target_token)
    return _publish(args.state, args.target_token, args.publication)


# What the gate step reports to the workflow. The workflow mints a push-capable
# token only for "push", decided here before any AI or PR code runs, so nothing
# a later step does can widen the token it publishes with.
_GATE_PUBLICATIONS = ("push", "suggest", "none")


def _gate(args: argparse.Namespace) -> int:
    request = _request_from_dispatch(args) if (args.repo or args.run_url) else _request_from_event(args)
    if request is None:
        logger.info("No actionable fix command; nothing to do.")
        _set_outputs(publication="none", execute="false")
        return 0
    repo_full_name, pr_number, commenter, command, comment_id = request
    context = {"repo": repo_full_name, "pr": pr_number, "comment_id": comment_id}
    gh = Github(auth=Auth.Token(args.target_token))
    try:
        gated = build_fix_request(
            gh, command=command, pr_repo_full_name=repo_full_name, pr_number=pr_number,
            commenter=commenter, org=_AUTH_ORG, auth_team=_AUTH_TEAM,
        )
    except Exception:  # noqa: BLE001 - never stop without telling the PR
        logger.exception("ci_fix gate raised unexpectedly")
        failed = FixOutcome(kind=OutcomeKind.FAILED, summary=f"An internal error stopped the run; see {workflow_logs()}.")
        write_state(args.state, {"context": context, "request": None, "outcome": to_dict(failed)})
        _set_outputs(publication="none", execute="false")
        return 0
    if isinstance(gated, GateRejection):
        refused = FixOutcome(kind=OutcomeKind.REFUSED, summary=gated.reason)
        write_state(args.state, {"context": context, "request": None, "outcome": to_dict(refused)})
        _set_outputs(publication="none", execute="false")
        return 0
    write_state(args.state, {"context": context, "request": to_dict(gated), "outcome": to_dict(_INTERRUPTED)})
    _set_outputs(publication=gated.publication.value, execute=str(gated.execute).lower())
    return 0


def _prepare(state_path: str, token: str) -> int:
    state = read_state(state_path)
    if state is None:
        # The gate step always writes state, so its absence is a broken run.
        logger.error("No gate state at %s; cannot prepare.", state_path)
        return 1
    if not state.get("request"):
        logger.info("The gate refused the request; nothing to prepare.")
        return 0
    request = request_from_dict(state["request"])
    gh = Github(auth=Auth.Token(token))
    try:
        outcome = run_ci_fix_request(
            gh,
            request=request,
            artifact_client=ArtifactClient(gh, token=token),
            verify_runs=_verify_runs(),
            macos_verifier=macos_verifier_from_env(),
        )
    except Exception:  # noqa: BLE001 - never stop without telling the PR
        logger.exception("ci_fix pipeline raised unexpectedly")
        outcome = FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=f"An internal error stopped the run; see {workflow_logs()}.",
        )
    write_state(state_path, {**state, "outcome": to_dict(outcome)})
    logger.info("ci_fix decision: %s - %s", outcome.kind.value, outcome.summary)
    return 0


def _publish(state_path: str, token: str, gate_publication: str) -> int:
    state = read_state(state_path)
    if state is None:
        logger.info("No prepared decision; nothing to publish.")
        return 0
    context: dict[str, Any] = state["context"]
    repo_full_name = str(context["repo"])
    pr_number = int(context["pr"])
    outcome = outcome_from_dict(state["outcome"])
    gh = Github(auth=Auth.Token(token))
    if state.get("request") and outcome.kind is OutcomeKind.READY:
        request = request_from_dict(state["request"])
        try:
            if request.publication.value != gate_publication:
                raise RuntimeError("the prepared decision does not match the gate step")
            if (request.repo_full_name, request.pr_number) != (repo_full_name, pr_number):
                raise RuntimeError("the prepared decision is for a different PR")
            with GitAuth(token=token) as auth:
                outcome = publish_to_pr(
                    outcome, request, git_env=auth.env(), pre_push_check=pr_head_check(gh, request),
                )
        except Exception:  # noqa: BLE001 - the PR must still hear about it
            logger.exception("ci_fix publication raised unexpectedly")
            outcome = FixOutcome(
                kind=OutcomeKind.FAILED,
                summary=f"An internal error stopped publication; see {workflow_logs()}.",
                proposal=outcome.proposal, failing_run_url=outcome.failing_run_url,
            )
    try:
        _post_comment(gh, repo_full_name, pr_number, render_comment(outcome))
    except Exception:  # noqa: BLE001 - a failed comment must not mask the outcome
        logger.exception("Failed to post outcome comment on #%s", pr_number)
    _react_outcome(gh, repo_full_name, int(context.get("comment_id") or 0), outcome.kind)
    level = {OutcomeKind.FAILED: logging.ERROR, OutcomeKind.PUSHED: logging.INFO}.get(
        outcome.kind, logging.WARNING)
    log_outcome(logger, level, "CI fix for %s#%s %s: %s", repo_full_name, pr_number,
                outcome.kind.value, " ".join(outcome.summary.split()))
    return 1 if outcome.kind is OutcomeKind.FAILED else 0


def _set_outputs(**outputs: str) -> None:
    """Write step outputs for the workflow (no-op outside GitHub Actions)."""
    path = os.environ.get("GITHUB_OUTPUT", "")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def _request_from_event(args: argparse.Namespace) -> tuple[str, int, str, ParsedCommand, int] | None:
    if not args.event_path:
        return None
    event = json.loads(Path(args.event_path).read_text(encoding="utf-8"))
    parsed = _parse_event(event)
    if parsed is None:
        return None
    repo_full_name, pr_number, commenter, body, comment_id = parsed
    command = parse_command(body)
    if command is None:
        return None
    return repo_full_name, pr_number, commenter, command, comment_id


def _request_from_dispatch(args: argparse.Namespace) -> tuple[str, int, str, ParsedCommand, int] | None:
    if not (args.repo and args.pr and args.commenter):
        return None
    if args.run_url:
        command = parse_run_url(args.run_url)
        if command is None:
            return None
        command = ParsedCommand(command.run_owner, command.run_repo, command.run_id, args.hint.strip(), command.job_id)
    else:
        command = ParsedCommand("", "", 0, args.hint.strip())
    return args.repo, args.pr, args.commenter, command, args.comment_id


def _parse_event(event: dict) -> tuple[str, int, str, str, int] | None:
    """Extract (repo_full_name, pr_number, commenter, body, comment_id) from the event.

    Returns None when the event is not a created comment on a pull request.
    """
    if event.get("action") != "created":
        return None
    issue = event.get("issue") or {}
    if "pull_request" not in issue:
        return None
    comment = event.get("comment") or {}
    body = comment.get("body") or ""
    commenter = (comment.get("user") or {}).get("login") or ""
    comment_id = comment.get("id") or 0
    pr_number = issue.get("number")
    repo_full_name = (event.get("repository") or {}).get("full_name") or ""
    if not (body and commenter and isinstance(pr_number, int) and repo_full_name):
        return None
    return repo_full_name, pr_number, commenter, body, comment_id


def _post_comment(gh: Github, repo_full_name: str, pr_number: int, body: str) -> None:
    def _post() -> None:
        issue = gh.get_repo(repo_full_name).get_issue(pr_number)
        issue.create_comment(body)

    retry_github_call(_post, retries=3, description=f"comment on #{pr_number}")


# Reaction added to the triggering comment once the run is done, on top of the
# poller's "eyes" claim marker, so the comment shows the verdict at a glance:
# "+1" when a fix was pushed or a verified fix was posted, "-1" for anything
# else. The eyes marker is left in place - it is the poller's idempotency claim.
_OUTCOME_REACTIONS: dict[OutcomeKind, str] = {
    OutcomeKind.PUSHED: "+1",
    OutcomeKind.SUGGESTED: "+1",
    OutcomeKind.REFUSED: "-1",
    OutcomeKind.HANDOFF: "-1",
    OutcomeKind.FAILED: "-1",
}


def _react_outcome(gh: Github, repo_full_name: str, comment_id: int, kind: OutcomeKind) -> None:
    """Add the outcome reaction to the triggering comment (best-effort).

    Skips silently when no comment id was supplied (e.g. a manual dispatch that
    did not forward one). The reaction is issued through the requester against
    the comment's reactions endpoint - the same path ``comment_poll`` uses for
    the claim marker - because PyGithub's ``Repository`` exposes no getter for a
    single issue comment. A failed reaction never masks the outcome: the comment
    and exit code are the authoritative report.
    """
    if not comment_id:
        return

    def _react() -> None:
        # A new OutcomeKind without a mapping falls back to "-1": any outcome we
        # did not explicitly mark as a success is a non-success. The whole body -
        # including the repo/requester lookup - is inside the guarded call so a
        # transient API error here can never escape into the run's exit code.
        content = _OUTCOME_REACTIONS.get(kind, "-1")
        url = f"/repos/{repo_full_name}/issues/comments/{comment_id}/reactions"
        requester = gh.get_repo(repo_full_name)._requester  # noqa: SLF001 - matches workflow_artifacts
        requester.requestJsonAndCheck("POST", url, input={"content": content})

    try:
        retry_github_call(_react, retries=2, description=f"react to comment {comment_id}")
    except Exception:  # noqa: BLE001 - the reaction is a nicety, not the report
        logger.exception("Failed to react to comment %s", comment_id)


if __name__ == "__main__":
    raise SystemExit(main())
