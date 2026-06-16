"""Top-level orchestration for ``@valkeyrie-bot fix <ci-link>``.

Wires the pieces in order and returns a ``FixOutcome``:

    gate (auth + SHA binding)        -> FixRequest | refuse
    download run logs                -> ci.log in a tempdir
    clone repo at the failed SHA     -> working tree
    diagnose (read-only AI)          -> FixProposal | refuse
    fix-feedback loop (apply/run/review)
    commit + namespace-restricted push
    -> FixOutcome (rendered into a PR comment by the caller)

Every refusal path returns a ``FixOutcome`` with a clear reason rather than
raising, so the workflow can always post an explanatory comment. The only
remote mutation is the final push, guarded by the branch-namespace check.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any, Callable

from scripts.ci_fix.diagnose import diagnose_failure, write_log_to_workspace
from scripts.ci_fix.gate import GateRejection, ParsedCommand, build_fix_request
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
)
from scripts.ci_fix.push import PushRefused, commit_and_push_fix
from scripts.ci_fix.review import LoopResult, run_fix_loop
from scripts.common.git_clone import shallow_clone_at_sha
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

Diagnose = Callable[..., FixProposal]
RunLoop = Callable[..., LoopResult]
Push = Callable[..., str]


def run_ci_fix(
    gh: Any,
    *,
    command: ParsedCommand,
    pr_repo_full_name: str,
    pr_number: int,
    commenter: str,
    git_env: dict[str, str],
    artifact_client: ArtifactClient,
    org: str = "valkey-io",
    auth_team: str = "contributors",
    diagnose_func: Diagnose = diagnose_failure,
    run_loop_func: RunLoop = run_fix_loop,
    push_func: Push = commit_and_push_fix,
) -> FixOutcome:
    """Run the whole pipeline and return a terminal ``FixOutcome``."""
    request = build_fix_request(
        gh, command=command, pr_repo_full_name=pr_repo_full_name,
        pr_number=pr_number, commenter=commenter, org=org, auth_team=auth_team,
    )
    if isinstance(request, GateRejection):
        return FixOutcome(kind=OutcomeKind.REFUSED, summary=request.reason)

    with tempfile.TemporaryDirectory(prefix="ci-fix-") as workdir_str:
        return _run_in_workspace(
            Path(workdir_str), request,
            artifact_client=artifact_client, git_env=git_env,
            diagnose_func=diagnose_func, run_loop_func=run_loop_func, push_func=push_func,
        )


def _run_in_workspace(
    workdir: Path,
    request: FixRequest,
    *,
    artifact_client: ArtifactClient,
    git_env: dict[str, str],
    diagnose_func: Diagnose,
    run_loop_func: RunLoop,
    push_func: Push,
) -> FixOutcome:
    logs = artifact_client.download_run_logs(request.repo_full_name, request.run_id)
    if not logs:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary="The run's logs are unavailable or expired; cannot diagnose.",
        )
    log_path = write_log_to_workspace(logs, workdir)

    repo_dir = workdir / "repo"
    if not shallow_clone_at_sha(request.repo_full_name, repo_dir, request.head_sha):
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=f"Could not clone {request.repo_full_name} at {request.head_sha[:12]}.",
        )

    proposal = diagnose_func(str(log_path), str(repo_dir), hint=request.hint)
    if proposal.path is FixPath.REFUSE:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary=proposal.reasoning or "No safe fix found.",
            proposal=proposal,
            other_failing_tests=proposal.other_failing_tests,
        )

    loop = run_loop_func(str(repo_dir), proposal)
    if not loop.success:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary=loop.detail,
            proposal=proposal,
            run_result=loop.run_result,
            review=loop.review,
            other_failing_tests=proposal.other_failing_tests,
        )

    try:
        commit_sha = push_func(
            str(repo_dir),
            head_repo_full_name=request.head_repo_full_name,
            head_branch=request.head_branch,
            head_sha=request.head_sha,
            proposal=proposal,
            changed_paths=loop.changed_paths,
            git_env=git_env,
        )
    except PushRefused as exc:
        return FixOutcome(
            kind=OutcomeKind.REFUSED, summary=str(exc),
            proposal=proposal, run_result=loop.run_result, review=loop.review,
        )

    return FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary=f"Pushed fix for {proposal.failing_test}",
        proposal=proposal, run_result=loop.run_result, review=loop.review,
        commit_sha=commit_sha,
        other_failing_tests=proposal.other_failing_tests,
    )
