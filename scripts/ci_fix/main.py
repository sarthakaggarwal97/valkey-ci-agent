"""Entry point for the ``@valkeyrie-bot fix <ci-link>`` workflow.

Driven by ``workflow_dispatch``: a maintainer supplies the PR and failing-run
URL, and we run the pipeline and post the rendered outcome as a PR comment. It
also accepts a raw ``issue_comment`` event payload (``--event-path``) so a
future target-repo wrapper can forward a comment event here without changing
this entry point.

When the input is not an actionable fix command, we exit silently (rc 0). The
pipeline itself never raises for a refusal - it returns a ``FixOutcome`` we
always turn into a comment - so the only error path here is an unexpected
internal failure, which we surface both as a comment and a non-zero exit code.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.ci_fix.auth import RepositoryInstallationAuth
from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.gate import ParsedCommand, parse_command
from scripts.ci_fix.models import FixOutcome, OutcomeKind
from scripts.ci_fix.pipeline import run_ci_fix
from scripts.ci_fix.policy import DEFAULT_PROTECTED_PATTERNS
from scripts.ci_fix.registry import CiFixRepoConfig, load_ci_fix_registry
from scripts.ci_fix.review import DEFAULT_VERIFY_RUNS
from scripts.ci_fix.verify.linux import LinuxVerifier
from scripts.ci_fix.verify.macos import MacosVerifier
from scripts.ci_fix.verify.target_workflow import TargetWorkflowVerifier
from scripts.common.git_auth import GitAuth
from scripts.common.github_client import retry_github_call
from scripts.common.identity import BOT_LOGIN
from scripts.common.polling import env_int
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

# The team authorization is configurable so the same entry point can run
# against a different org/team in a fork test environment. Defaults to the
# production target; override only via these env vars.
_AUTH_ORG = os.environ.get("CI_FIX_AUTH_ORG", "valkey-io")
_AUTH_TEAM = os.environ.get("CI_FIX_AUTH_TEAM", "contributors")

# The agent repo hosts credential-free Linux and macOS verification workflows.
# Legacy macOS-only names remain accepted for one deployment transition.
_AGENT_REPO = os.environ.get(
    "CI_FIX_AGENT_REPO",
    os.environ.get("CI_FIX_MACOS_AGENT_REPO", ""),
)
_AGENT_REF = os.environ.get(
    "CI_FIX_AGENT_REF",
    os.environ.get("CI_FIX_MACOS_AGENT_REF", "main"),
)
_AGENT_TOKEN = os.environ.get(
    "CI_FIX_AGENT_TOKEN",
    os.environ.get("CI_FIX_MACOS_TOKEN", ""),
)
_MAX_VERIFY_RUNS = 10
_TARGET_PERMISSIONS_BASE = {
    "members": "read",
    "contents": "write",
    "pull_requests": "write",
    "issues": "write",
    "metadata": "read",
}
_AGENT_PERMISSIONS = {
    "actions": "write",
    "metadata": "read",
}


def _verify_runs() -> int:
    """Return the required candidate verification repeat count."""
    return env_int(
        "CI_FIX_VERIFY_RUNS",
        DEFAULT_VERIFY_RUNS,
        minimum=1,
        maximum=_MAX_VERIFY_RUNS,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-path", default=os.environ.get("GITHUB_EVENT_PATH", ""),
                        help="Path to the issue_comment event JSON")
    parser.add_argument("--target-token", default=os.environ.get("TARGET_TOKEN", ""),
                        help="GitHub App installation token")
    # Dispatch mode: supply the command pieces directly instead of an event.
    # Used by the workflow_dispatch entry for manual/fork testing.
    parser.add_argument("--repo", default="", help="PR repository (owner/name)")
    parser.add_argument("--pr", type=int, default=0, help="PR number")
    parser.add_argument("--run-url", default="", help="Failing CI run URL")
    parser.add_argument("--commenter", default="", help="Requesting user login")
    parser.add_argument("--hint", default="", help="Optional diagnosis hint")
    parser.add_argument("--comment-id", type=int, default=env_int("CI_FIX_COMMENT_ID", 0, minimum=0),
                        help="Triggering comment id, reacted to with the outcome")
    parser.add_argument("--registry", default=os.environ.get("CI_FIX_REGISTRY", ""),
                        help="repos.yml path used to authorize target repositories")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.target_token:
        parser.error("--target-token/TARGET_TOKEN is required")

    request = _request_from_dispatch(args) if args.run_url else _request_from_event(args)
    if request is None:
        logger.info("No actionable fix command; nothing to do.")
        return 0

    repo_full_name, pr_number, commenter, command, comment_id = request
    config = _load_repo_config(args.registry, repo_full_name)
    if args.registry and config is None:
        return 2
    return _run_and_comment(
        args.target_token, repo_full_name, pr_number, commenter, command, comment_id,
        config=config,
    )


def _run_and_comment(
    token: str,
    repo_full_name: str,
    pr_number: int,
    commenter: str,
    command: ParsedCommand,
    comment_id: int = 0,
    *,
    config: CiFixRepoConfig | None = None,
) -> int:
    target_permissions = {
        **_TARGET_PERMISSIONS_BASE,
        "actions": "write" if config and config.verification_workflow else "read",
    }
    target_auth = _repository_auth(
        token,
        repo_full_name=repo_full_name,
        installation_id_env="CI_FIX_TARGET_INSTALLATION_ID",
        permissions=target_permissions,
    )

    def target_token() -> str:
        return target_auth.token

    gh = Github(auth=target_auth)
    artifact_client = ArtifactClient(gh, token=target_token)
    exact_verifier = None
    if config and config.verification_workflow:
        exact_verifier = TargetWorkflowVerifier(
            gh,
            repo_full_name=repo_full_name,
            workflow=config.verification_workflow,
            ref=config.verification_ref,
            artifact_client=artifact_client,
        )
    linux_verifier = None
    macos_verifier = None
    if exact_verifier is None and _AGENT_REPO and _AGENT_TOKEN:
        # Dispatching agent-owned verifier workflows needs actions:write on the
        # agent repo. The target-scoped token does not carry that permission.
        agent_auth = _repository_auth(
            _AGENT_TOKEN,
            repo_full_name=_AGENT_REPO,
            installation_id_env="CI_FIX_AGENT_INSTALLATION_ID",
            permissions=_AGENT_PERMISSIONS,
        )

        def agent_token() -> str:
            return agent_auth.token

        agent_gh = Github(auth=agent_auth)
        agent_artifacts = ArtifactClient(agent_gh, token=agent_token)
        linux_verifier = LinuxVerifier(
            agent_gh,
            agent_repo_full_name=_AGENT_REPO,
            ref=_AGENT_REF,
            artifact_client=agent_artifacts,
        )
        macos_verifier = MacosVerifier(
            agent_gh,
            agent_repo_full_name=_AGENT_REPO,
            ref=_AGENT_REF,
            artifact_client=agent_artifacts,
        )
    try:
        with GitAuth(token=target_token) as auth:
            outcome = run_ci_fix(
                gh,
                command=command,
                pr_repo_full_name=repo_full_name,
                pr_number=pr_number,
                commenter=commenter,
                git_env=auth.env,
                artifact_client=artifact_client,
                org=config.authorization_org if config else _AUTH_ORG,
                auth_team=config.authorization_team if config else _AUTH_TEAM,
                verify_runs=_verify_runs(),
                linux_verifier=linux_verifier,
                macos_verifier=macos_verifier,
                exact_verifier=exact_verifier,
                history_branches=config.history_branches if config else (),
                baseline_runs=config.baseline_runs if config else 3,
                flaky_verify_runs=config.flaky_verify_runs if config else 10,
                minimum_confidence=config.minimum_confidence if config else 0.8,
                protected_paths=(
                    config.protected_paths if config else DEFAULT_PROTECTED_PATTERNS
                ),
                auto_publish_paths=config.auto_publish_paths if config else (),
                allowed_branch_prefixes=(
                    config.allowed_branch_prefixes
                    if config else ("agent/backport/",)
                ),
                remote_parallelism=config.remote_parallelism if config else 5,
                remote_sample_timeout_seconds=(
                    config.remote_sample_timeout_minutes * 60
                    if config else 15 * 60
                ),
                remote_budget_seconds=(
                    config.remote_budget_minutes * 60
                    if config else 45 * 60
                ),
            )
    except Exception:  # noqa: BLE001 - never crash without telling the PR
        logger.exception("ci_fix pipeline raised unexpectedly")
        outcome = FixOutcome(
            kind=OutcomeKind.FAILED,
            summary="An internal error stopped the run; see the bot run logs for details.",
        )
    try:
        _post_comment(gh, repo_full_name, pr_number, render_comment(outcome))
    except Exception:  # noqa: BLE001 - a failed comment must not mask the outcome
        logger.exception("Failed to post outcome comment on #%s", pr_number)
    _react_outcome(gh, repo_full_name, comment_id, outcome.kind)
    logger.info("ci_fix outcome: %s - %s", outcome.kind.value, outcome.summary)
    return 1 if outcome.kind is OutcomeKind.FAILED else 0


def _repository_auth(
    fallback_token: str,
    *,
    repo_full_name: str,
    installation_id_env: str,
    permissions: dict[str, str],
) -> Auth.Auth:
    """Build refreshing restricted auth, retaining static-token compatibility."""
    app_id = os.environ.get("CI_FIX_APP_ID", "").strip()
    private_key = os.environ.get("CI_FIX_APP_PRIVATE_KEY", "")
    installation_id_raw = os.environ.get(installation_id_env, "").strip()
    supplied = (bool(app_id), bool(private_key.strip()), bool(installation_id_raw))
    if not any(supplied):
        return Auth.Token(fallback_token)
    if not all(supplied):
        logger.warning(
            "Incomplete refreshing GitHub App credentials for %s; using the "
            "already repository-scoped workflow token",
            repo_full_name,
        )
        return Auth.Token(fallback_token)
    try:
        installation_id = int(installation_id_raw)
        return RepositoryInstallationAuth(
            app_id=app_id,
            private_key=private_key,
            installation_id=installation_id,
            repository=repo_full_name,
            permissions=permissions,
            initial_token=fallback_token,
        )
    except (TypeError, ValueError) as exc:
        logger.warning(
            "Invalid refreshing GitHub App credentials for %s (%s); using the "
            "already repository-scoped workflow token",
            repo_full_name,
            exc,
        )
        return Auth.Token(fallback_token)


def _load_repo_config(
    registry_path: str, repo_full_name: str,
) -> CiFixRepoConfig | None:
    """Resolve an enabled target from the registry, failing closed on errors."""
    if not registry_path:
        return None
    try:
        return load_ci_fix_registry(registry_path).get_repo(repo_full_name)
    except (OSError, KeyError, ValueError) as exc:
        logger.error("CI-fix registry rejected %s: %s", repo_full_name, exc)
        return None


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
    command = parse_command(f"@{BOT_LOGIN} fix {args.run_url} {args.hint}".strip())
    if command is None:
        return None
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
# "+1" when a fix was pushed, "-1" for any non-push outcome (refused, handoff,
# or internal failure). The eyes marker is left in place - it is the poller's
# idempotency claim, not a progress indicator.
_OUTCOME_REACTIONS: dict[OutcomeKind, str] = {
    OutcomeKind.PUSHED: "+1",
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
        # did not explicitly mark as pushed is a non-success. The whole body -
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
