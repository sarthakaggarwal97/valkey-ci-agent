"""Create a draft fix PR from an issue and failed default-branch CI run."""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable

from github import Auth, Github

from scripts.ci_fix.diagnose import diagnose_issue_failure
from scripts.ci_fix.issue_gate import (
    DEFAULT_DETECTOR_LOGIN,
    IssueFixInvocation,
    IssueGateRejection,
    build_issue_fix_request,
)
from scripts.ci_fix.issue_publish import (
    IssueFixPullRequest,
    create_issue_fix_pull_request,
)
from scripts.ci_fix.main import _post_comment, _react_outcome
from scripts.ci_fix.models import FixOutcome, OutcomeKind
from scripts.ci_fix.pipeline import run_fix_request
from scripts.ci_fix.push import (
    PushRefused,
    commit_and_push_issue_fix,
    commit_and_push_issue_patch,
)
from scripts.ci_fix.rendering import markdown_generated_text
from scripts.ci_fix.review import run_fix_loop
from scripts.ci_fix.verify.base import VerifyBackend
from scripts.ci_fix.verify.macos import MacosVerifier
from scripts.common.git_auth import GitAuth
from scripts.common.polling import env_int
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

_DEFAULT_FLAKY_RUNS = 5
_MAX_FLAKY_RUNS = 20


@dataclass(frozen=True)
class IssueFixResult:
    outcome: FixOutcome
    publication: IssueFixPullRequest | None = None
    verified: bool = False


ExecuteFix = Callable[..., FixOutcome]
CreatePull = Callable[..., IssueFixPullRequest]


def run_issue_fix(
    gh: Any,
    *,
    repo_full_name: str,
    issue_number: int,
    commenter: str,
    invocation: IssueFixInvocation,
    git_env: dict[str, str],
    artifact_client: ArtifactClient,
    org: str = "valkey-io",
    auth_team: str = "contributors",
    detector_login: str = DEFAULT_DETECTOR_LOGIN,
    verify_runs: int = _DEFAULT_FLAKY_RUNS,
    baseline_runs: int = _DEFAULT_FLAKY_RUNS,
    macos_verifier: VerifyBackend | None = None,
    execute_func: ExecuteFix = run_fix_request,
    create_pull_func: CreatePull = create_issue_fix_pull_request,
) -> IssueFixResult:
    """Gate, diagnose, publish, and open one issue-driven draft PR."""
    gated = build_issue_fix_request(
        gh,
        repo_full_name=repo_full_name,
        issue_number=issue_number,
        commenter=commenter,
        invocation=invocation,
        org=org,
        auth_team=auth_team,
        detector_login=detector_login,
    )
    if isinstance(gated, IssueGateRejection):
        return IssueFixResult(
            FixOutcome(kind=OutcomeKind.REFUSED, summary=gated.reason)
        )

    diagnose = partial(
        diagnose_issue_failure,
        issue_title=gated.issue_title,
        issue_body=gated.issue_evidence,
    )
    loop = partial(
        run_fix_loop,
        baseline_runs=baseline_runs,
        allow_passing_baseline_handoff=True,
        require_named_baseline=True,
    )
    published_base_sha = gated.base_sha

    def _push_verified(repo_dir: str, **kwargs: Any) -> str:
        nonlocal published_base_sha
        push_result = commit_and_push_issue_fix(
            repo_dir,
            repo_full_name=gated.fix_request.repo_full_name,
            base_branch=gated.base_branch,
            branch_name=gated.branch_name,
            issue_number=gated.issue_number,
            run_id=gated.fix_request.run_id,
            run_sha=gated.fix_request.head_sha,
            proposal=kwargs["proposal"],
            changed_paths=kwargs["changed_paths"],
            git_env=kwargs["git_env"],
        )
        published_base_sha = push_result.base_sha
        return push_result.commit_sha

    def _refuse_port(*_args: Any, **_kwargs: Any) -> str:
        raise PushRefused("Default-branch issue fixes cannot use the upstream-port path.")

    outcome = execute_func(
        gh,
        request=gated.fix_request,
        git_env=git_env,
        artifact_client=artifact_client,
        verify_runs=verify_runs,
        diagnose_func=diagnose,
        run_loop_func=loop,
        push_func=_push_verified,
        port_push_func=_refuse_port,
        macos_verifier=macos_verifier,
        macos_baseline_runs=baseline_runs,
        macos_verify_runs=verify_runs,
        allow_passing_macos_baseline_handoff=True,
    )
    pushed = outcome.kind is OutcomeKind.PUSHED
    verified = pushed and gated.fix_request.head_sha == published_base_sha
    if not pushed and not (
        outcome.kind is OutcomeKind.HANDOFF and outcome.handoff_patch
    ):
        return IssueFixResult(outcome)

    if not pushed:
        if outcome.proposal is None:
            return IssueFixResult(
                FixOutcome(
                    kind=OutcomeKind.FAILED,
                    summary="Reviewed handoff patch had no fix proposal metadata.",
                    failing_run_url=outcome.failing_run_url,
                )
            )
        if not outcome.handoff_paths:
            return IssueFixResult(
                FixOutcome(
                    kind=OutcomeKind.FAILED,
                    summary="Reviewed handoff patch had no reviewed path metadata.",
                    proposal=outcome.proposal,
                    run_result=outcome.run_result,
                    review=outcome.review,
                    failing_run_url=outcome.failing_run_url,
                )
            )
        try:
            push_result = commit_and_push_issue_patch(
                outcome.handoff_patch,
                repo_full_name=gated.fix_request.repo_full_name,
                base_branch=gated.base_branch,
                branch_name=gated.branch_name,
                issue_number=gated.issue_number,
                run_id=gated.fix_request.run_id,
                run_sha=gated.fix_request.head_sha,
                proposal=outcome.proposal,
                expected_paths=outcome.handoff_paths,
                git_env=git_env,
            )
            published_base_sha = push_result.base_sha
        except PushRefused as exc:
            return IssueFixResult(
                FixOutcome(
                    kind=OutcomeKind.FAILED,
                    summary=f"Could not publish the reviewed draft patch: {exc}",
                    proposal=outcome.proposal,
                    run_result=outcome.run_result,
                    review=outcome.review,
                    failing_run_url=outcome.failing_run_url,
                )
            )

    try:
        publication = create_pull_func(
            gh,
            repo_full_name=gated.fix_request.repo_full_name,
            issue_number=gated.issue_number,
            issue_url=gated.issue_url,
            run_id=gated.fix_request.run_id,
            base_branch=gated.base_branch,
            branch_name=gated.branch_name,
            outcome=outcome,
            verified=verified,
            verification_sha=gated.fix_request.head_sha,
            close_issue=gated.detector_issue,
        )
    except Exception as exc:  # noqa: BLE001 - report orphan branch for recovery
        logger.exception("Failed to create issue-fix pull request")
        return IssueFixResult(
            FixOutcome(
                kind=OutcomeKind.FAILED,
                summary=(
                    f"Pushed {gated.branch_name!r}, but could not create its "
                    f"draft pull request: {exc}"
                ),
                proposal=outcome.proposal,
                run_result=outcome.run_result,
                review=outcome.review,
                commit_sha=outcome.commit_sha,
                failing_run_url=outcome.failing_run_url,
            )
        )
    return IssueFixResult(outcome, publication=publication, verified=verified)


def render_issue_comment(result: IssueFixResult) -> str:
    publication = result.publication
    outcome = result.outcome
    if publication is not None:
        if result.verified:
            status = (
                "The targeted failure reproduced and the fix passed repeated verification."
            )
        elif outcome.kind is OutcomeKind.PUSHED:
            status = (
                "The fix passed repeated verification at the failing-run commit, "
                "but the default branch had advanced. The draft PR's CI is the "
                "verification authority for the published branch."
            )
        else:
            status = _handoff_status(outcome)
        root_cause = markdown_generated_text(
            outcome.proposal.root_cause if outcome.proposal else outcome.summary
        )
        return "\n".join(
            [
                f"Opened draft pull request [#{publication.number}]({publication.url}).",
                "",
                f"**Root cause:** {root_cause}",
                "",
                status,
                "",
                "_I do not merge pull requests._",
            ]
        )
    if outcome.kind is OutcomeKind.FAILED:
        summary = markdown_generated_text(outcome.summary)
        return f"I hit an error and could not open a fix pull request: {summary}"
    summary = markdown_generated_text(outcome.summary)
    return f"I did not open a fix pull request: {summary}"


def _handoff_status(outcome: FixOutcome) -> str:
    if outcome.verify_backend == "macos":
        return (
            "The unpatched macOS baseline did not establish a reproducible "
            "failure. The reviewed patch passed repeated macOS runs, but the "
            "draft PR's CI remains the verification authority."
        )
    run_result = outcome.run_result
    if run_result is not None and run_result.ran and run_result.passed:
        return (
            "The agent did not establish a failing baseline; the reviewed "
            "patch passed repeated post-change runs, but the draft PR's CI is "
            "the verification authority."
        )
    return (
        "The patch passed skeptic review, but the agent environment could not "
        "produce a reliable post-change test verdict. This is an unverified "
        "draft; its PR CI is the verification authority."
    )


def _flaky_runs(name: str) -> int:
    return env_int(
        name,
        _DEFAULT_FLAKY_RUNS,
        minimum=2,
        maximum=_MAX_FLAKY_RUNS,
    )


def _macos_verifier() -> MacosVerifier | None:
    agent_repo = os.environ.get("CI_FIX_MACOS_AGENT_REPO", "")
    token = os.environ.get("CI_FIX_MACOS_TOKEN", "")
    if not agent_repo or not token:
        return None
    gh = Github(auth=Auth.Token(token))
    return MacosVerifier(
        gh,
        agent_repo_full_name=agent_repo,
        ref=os.environ.get("CI_FIX_MACOS_AGENT_REF", "main"),
        artifact_client=ArtifactClient(gh, token=token),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-token", default=os.environ.get("TARGET_TOKEN", ""))
    parser.add_argument("--repo", required=True)
    parser.add_argument("--issue", required=True, type=int)
    parser.add_argument("--run-url", default="")
    parser.add_argument("--commenter", required=True)
    parser.add_argument("--hint", default="")
    parser.add_argument(
        "--comment-id",
        type=int,
        default=env_int("CI_FIX_COMMENT_ID", 0, minimum=0),
    )
    args = parser.parse_args(argv)
    if not args.target_token:
        parser.error("--target-token/TARGET_TOKEN is required")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    gh = Github(auth=Auth.Token(args.target_token))
    artifact_client = ArtifactClient(gh, token=args.target_token)
    try:
        with GitAuth(token=args.target_token) as auth:
            result = run_issue_fix(
                gh,
                repo_full_name=args.repo,
                issue_number=args.issue,
                commenter=args.commenter,
                invocation=IssueFixInvocation(run_url=args.run_url, hint=args.hint),
                git_env=auth.env(),
                artifact_client=artifact_client,
                org=os.environ.get("CI_FIX_AUTH_ORG", "valkey-io"),
                auth_team=os.environ.get("CI_FIX_AUTH_TEAM", "contributors"),
                detector_login=os.environ.get(
                    "CI_FIX_DETECTOR_LOGIN",
                    DEFAULT_DETECTOR_LOGIN,
                ),
                verify_runs=_flaky_runs("CI_FIX_ISSUE_VERIFY_RUNS"),
                baseline_runs=_flaky_runs("CI_FIX_ISSUE_BASELINE_RUNS"),
                macos_verifier=_macos_verifier(),
            )
    except Exception:  # noqa: BLE001 - every terminal outcome must be visible
        logger.exception("issue-fix pipeline raised unexpectedly")
        result = IssueFixResult(
            FixOutcome(
                kind=OutcomeKind.FAILED,
                summary=(
                    "An internal error stopped the issue fix; see the bot run "
                    "logs for details."
                ),
            )
        )

    try:
        _post_comment(
            gh,
            args.repo,
            args.issue,
            render_issue_comment(result),
        )
    except Exception:  # noqa: BLE001 - reaction and exit still report outcome
        logger.exception("Failed to post issue-fix outcome comment")
    reaction_kind = (
        OutcomeKind.PUSHED if result.publication is not None else result.outcome.kind
    )
    _react_outcome(gh, args.repo, args.comment_id, reaction_kind)
    return 1 if result.outcome.kind is OutcomeKind.FAILED else 0


if __name__ == "__main__":
    raise SystemExit(main())
