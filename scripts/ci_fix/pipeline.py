"""The CI-fix engine: diagnose one failing run and produce an approved fix.

Thin wiring over a clean data flow. Every path returns a ``FixOutcome``; the
engine never writes to GitHub. Publication (push, suggestion comment, or a new
PR) is a separate step with its own freshly minted write token:

    failed_jobs_for_run (code)   -> the jobs that actually failed
    download logs, clone at SHA, list the commits that may have caused it
    diagnose (read-only AI)      -> FixProposal (fix, job hint, class, culprit)
    resolve the culprit (code)   -> a commit from the code-computed list, or none
    execute=False                -> apply + skeptic review only; verified later in CI
    execute=True:
      plan_verification (code)   -> VerificationPlan (code-selected backend) | refuse
      apply + verify + review    -> approved patch and the real verdict
    PORT                         -> a discovered upstream commit; PR CI verifies

A READY outcome carries exactly what publication applies: the approved patch
(or the port commit) and its changed paths.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from scripts.ci_fix.apply import apply_fix, declined_detail
from scripts.ci_fix.diagnose import diagnose_failure, format_recent_changes, write_logs_to_workspace
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
    Publication,
    ReviewVerdict,
)
from scripts.ci_fix.port_discovery import (
    PortCandidate,
    commits_in_range,
    discover_port_candidates,
    resolve_commit,
)
from scripts.ci_fix.review import (
    DEFAULT_VERIFY_RUNS,
    LoopResult,
    build_and_review_patch,
    combined_command,
    precheck_command,
    reset_worktree,
    review_fix,
    run_fix_loop,
)
from scripts.ci_fix.verify.base import (
    VerificationPlan,
    VerificationResult,
    VerifyBackend,
    VerifyEnv,
    backend_label,
)
from scripts.ci_fix.verify.github_runs import failed_jobs_for_run
from scripts.ci_fix.verify.workflow_env import JobEnvironment, classify_job_environment
from scripts.common.git_clone import shallow_clone_at_sha
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

Diagnose = Callable[..., FixProposal]
RunLoop = Callable[..., LoopResult]
ReviewFix = Callable[..., ReviewVerdict]

_MACOS_FIX_MAX_ATTEMPTS = 5
# Without local execution only the skeptic can send a fix back, so fewer
# rounds are useful.
_UNEXECUTED_FIX_MAX_ATTEMPTS = 3


def run_ci_fix_request(
    gh: Any,
    *,
    request: FixRequest,
    artifact_client: ArtifactClient,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
    diagnose_func: Diagnose = diagnose_failure,
    run_loop_func: RunLoop = run_fix_loop,
    macos_verifier: VerifyBackend | None = None,
    failed_jobs: tuple[str, ...] | None = None,
    review_func: ReviewFix = review_fix,
) -> FixOutcome:
    """Run the engine for a request a trusted front door already validated.

    Every front door (maintainer comment, sweep follow-up, Daily issue) applies
    its own gate first, then shares this path so diagnosis, verification and
    review cannot drift between them.
    """
    confirmed_jobs = failed_jobs
    if confirmed_jobs is None:
        confirmed_jobs = tuple(
            job.name
            for job in failed_jobs_for_run(gh, request.repo_full_name, request.run_id)
        )
    if request.job:
        if request.job not in confirmed_jobs:
            return FixOutcome(
                kind=OutcomeKind.REFUSED,
                summary=f"The selected job {request.job!r} is not a failed job of the linked run; refusing.",
            )
        confirmed_jobs = (request.job,)

    with tempfile.TemporaryDirectory(prefix="ci-fix-") as workdir_str:
        # Traversable but not listable, so a separate verification user can
        # reach the checkout inside it (see runner.py).
        os.chmod(workdir_str, 0o711)
        outcome = _run_in_workspace(
            Path(workdir_str), request, confirmed_jobs,
            artifact_client=artifact_client, diagnose_func=diagnose_func,
            run_loop_func=run_loop_func, macos_verifier=macos_verifier,
            verify_runs=verify_runs, review_func=review_func,
        )
    run_url = f"https://github.com/{request.repo_full_name}/actions/runs/{request.run_id}"
    return replace(outcome, failing_run_url=run_url)


def _run_in_workspace(
    workdir: Path,
    request: FixRequest,
    failed_jobs: tuple[str, ...],
    *,
    artifact_client: ArtifactClient,
    diagnose_func: Diagnose,
    run_loop_func: RunLoop,
    macos_verifier: VerifyBackend | None,
    verify_runs: int,
    review_func: ReviewFix,
) -> FixOutcome:
    logs = artifact_client.download_run_logs(request.repo_full_name, request.run_id)
    if not logs:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary="The run's logs have expired and can no longer be downloaded; cannot diagnose.",
        )
    logs_dir = write_logs_to_workspace(logs, workdir)

    repo_dir = workdir / "repo"
    if not shallow_clone_at_sha(
        request.repo_full_name, repo_dir, request.head_sha, pull_number=request.pr_number,
    ):
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=f"Could not clone {request.repo_full_name} at {request.head_sha[:12]}.",
        )

    port_candidates = discover_port_candidates(str(repo_dir), str(logs_dir))
    suspects, recent_text = _recent_changes(str(repo_dir), request)
    proposal = diagnose_func(
        str(logs_dir), str(repo_dir), hint=request.hint,
        port_candidates=port_candidates, request=request, recent_changes=recent_text,
    )
    outcome = _decide(
        repo_dir, request, proposal, failed_jobs,
        port_candidates=port_candidates, run_loop_func=run_loop_func,
        macos_verifier=macos_verifier, verify_runs=verify_runs, review_func=review_func,
    )
    culprit = resolve_commit(proposal.culprit_commit, suspects) if proposal.culprit_commit else None
    if culprit is None:
        return outcome
    return replace(outcome, culprit_sha=culprit.sha, culprit_subject=culprit.subject)


def _decide(
    repo_dir: Path,
    request: FixRequest,
    proposal: FixProposal,
    failed_jobs: tuple[str, ...],
    *,
    port_candidates: tuple[PortCandidate, ...],
    run_loop_func: RunLoop,
    macos_verifier: VerifyBackend | None,
    verify_runs: int,
    review_func: ReviewFix,
) -> FixOutcome:
    if proposal.path is FixPath.REFUSE:
        return _refuse(proposal, proposal.reasoning or "No safe fix found.")

    if proposal.path is FixPath.PORT:
        return _port(request, proposal, failed_jobs, port_candidates=port_candidates)

    if not request.execute:
        return _author_without_execution(repo_dir, request, proposal, review_func=review_func)

    plan = _plan_verification(repo_dir, request, proposal, failed_jobs)
    if isinstance(plan, str):  # a refusal reason
        return _refuse(proposal, plan)

    if plan.env is VerifyEnv.MACOS:
        return _verify_on_macos(repo_dir, request, proposal, plan, verifier=macos_verifier)
    return _verify_locally(
        repo_dir, request, proposal, plan, run_loop_func=run_loop_func, verify_runs=verify_runs,
    )


def _recent_changes(
    repo_dir: str, request: FixRequest,
) -> tuple[tuple[PortCandidate, ...], str]:
    """Return the commits that may have caused the failure and their prompt text.

    PR flows default to the PR's own commits (``origin/<base>..HEAD``). The
    Daily issue flow passes the range between the previous Daily run and the
    failing one, plus whatever landed after the failing run, which can only
    explain why the failure is already fixed, never cause it.
    """
    culprit_range = request.culprit_range
    # A Daily fix branch has no commits of its own: no range means no suspects.
    if not culprit_range and request.base_branch and request.publication is not Publication.NEW_PR:
        culprit_range = f"origin/{request.base_branch}..HEAD"
    suspects = commits_in_range(repo_dir, culprit_range) if culprit_range else ()
    later: tuple[PortCandidate, ...] = ()
    if request.failing_sha and request.failing_sha != request.head_sha:
        later = commits_in_range(repo_dir, f"{request.failing_sha}..HEAD")
    if request.publication is Publication.NEW_PR:
        title = "Landed between the previous Daily run and the failing run"
    else:
        title = "Commits on this PR"
    text = format_recent_changes(title, suspects) + format_recent_changes(
        "Landed after the failing run (cannot have caused it)", later,
    )
    return suspects, text


def _plan_verification(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, failed_jobs: tuple[str, ...],
) -> VerificationPlan | str:
    """Select the verification backend from the real failed job, or return a refusal reason.

    The AI's ``failing_job_hint`` must match a job that actually failed in the
    linked run; code then classifies that job's workflow environment. The AI
    never selects the environment.
    """
    job = _match_failed_job(proposal.failing_job_hint, failed_jobs)
    if job is None:
        return (
            f"The named job {proposal.failing_job_hint or '(none)'!r} is not among the failed "
            f"jobs of the linked run ({', '.join(failed_jobs) or 'none found'}); "
            "refusing rather than verifying a job that did not fail."
        )
    env = _classify_failing_job(repo_dir, job)
    if env.env is VerifyEnv.UNSUPPORTED:
        return (
            f"Cannot verify the {job!r} job in a controlled environment "
            f"({env.reason}); refusing rather than publishing an unverified fix."
        )
    return VerificationPlan(
        env=env.env,
        command=combined_command(proposal),
        workdir=proposal.workdir,
        image=env.image,
        job_name=job,
        head_sha=request.head_sha,
        target_repo=request.head_repo_full_name,
    )


def _verify_locally(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, plan: VerificationPlan,
    *, run_loop_func: RunLoop, verify_runs: int,
) -> FixOutcome:
    """Local/Docker: apply, verify in-loop (retry on fail), review."""
    loop = run_loop_func(
        str(repo_dir), proposal, container_image=plan.image, verify_runs=verify_runs,
        policy=request.policy,
    )
    if loop.handoff:
        return FixOutcome(
            kind=OutcomeKind.HANDOFF, summary=loop.detail, proposal=proposal,
            run_result=loop.run_result, review=loop.review,
            handoff_patch=loop.handoff_patch,
            other_failing_checks=proposal.other_failing_checks,
        )
    if not loop.success:
        return FixOutcome(
            kind=OutcomeKind.REFUSED, summary=loop.detail, proposal=proposal,
            run_result=loop.run_result, review=loop.review,
            other_failing_checks=proposal.other_failing_checks,
        )
    return _ready(
        proposal, patch=loop.patch, changed_paths=loop.changed_paths,
        review=loop.review, run_result=loop.run_result,
        verify_backend=backend_label(plan.env, plan.image),
    )


def _author_without_execution(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, *, review_func: ReviewFix,
) -> FixOutcome:
    """Apply and skeptically review a fix without running the checkout's code.

    Used for a fork PR, whose code never runs here, and for a Daily issue fix,
    which publication verifies by rerunning the failing job in the project's
    own CI. Only the reviewer can send the fix back.
    """
    feedback = ""
    last_detail = "no attempt made"
    last_review: ReviewVerdict | None = None
    try:
        for _attempt in range(_UNEXECUTED_FIX_MAX_ATTEMPTS):
            reset_worktree(str(repo_dir))
            applied = apply_fix(str(repo_dir), proposal, feedback=feedback, policy=request.policy)
            if not applied.applied:
                return _refuse(proposal, declined_detail(applied), review=last_review)
            reviewed = build_and_review_patch(
                str(repo_dir), applied.changed, proposal, review_func=review_func, policy=request.policy,
            )
            last_review = reviewed.review
            if reviewed.ok:
                return _ready(
                    proposal, patch=reviewed.patch, changed_paths=applied.changed,
                    review=reviewed.review,
                )
            last_detail = reviewed.detail
            if reviewed.review is None:
                break  # an empty or oversized patch; nothing to retry on
            feedback = _rejection_feedback(reviewed.review, reviewed.patch)
    finally:
        reset_worktree(str(repo_dir))
    return _refuse(proposal, last_detail, review=last_review)


def _port(
    request: FixRequest, proposal: FixProposal, failed_jobs: tuple[str, ...],
    *, port_candidates: tuple[PortCandidate, ...],
) -> FixOutcome:
    """Approve porting an already-merged upstream fix.

    PORT is the only path allowed to rely on CI as the authoritative verifier.
    That trust is bounded by code: the SHA must be one of the candidates
    discovered from this failure's logs (so the model cannot point at an
    arbitrary default-branch commit), the hinted job must be a real failed job
    when the job is not already code-selected, and the push path independently
    re-verifies the commit's ancestry. Original authorship is preserved.
    """
    if request.execute and _match_failed_job(proposal.failing_job_hint, failed_jobs) is None:
        return _refuse(
            proposal,
            f"The named job {proposal.failing_job_hint or '(none)'!r} is not among the failed "
            f"jobs of the linked run ({', '.join(failed_jobs) or 'none found'}); "
            "refusing rather than porting a fix for a job that did not fail.",
        )
    chosen = proposal.unstable_fix_commit.strip()
    if not chosen:
        return _refuse(proposal, "diagnosis chose PORT but did not name an upstream fix commit")
    candidate = resolve_commit(chosen, port_candidates)
    if candidate is None:
        return _refuse(
            proposal,
            f"The chosen commit {chosen[:12]} is not among the "
            "fixes discovered for this failure; refusing to port a commit the code "
            "did not surface as a candidate.",
        )
    review = ReviewVerdict(
        approved=True,
        reasoning=(
            f"Porting upstream commit {candidate.sha[:12]} with its original "
            "authorship; CI is the verification authority for a port."
        ),
    )
    return FixOutcome(
        kind=OutcomeKind.READY,
        summary=f"Port upstream fix for {proposal.failing_check}",
        proposal=proposal, review=review, port_commit=candidate.sha,
        changed_paths=candidate.paths, verify_backend="upstream-port",
        other_failing_checks=proposal.other_failing_checks,
    )


def _verify_on_macos(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, plan: VerificationPlan,
    *, verifier: VerifyBackend | None,
) -> FixOutcome:
    """macOS: apply, review, and remotely verify with bounded feedback retries.

    Any unexpected error becomes a FAILED outcome so the invocation always
    produces a report.
    """
    if verifier is None:
        return _refuse(proposal, "macOS verification is not configured for this run; refusing.")
    try:
        precheck = precheck_command(proposal)
        if precheck:
            return _refuse(proposal, precheck)
        try:
            return _macos_fix_loop(repo_dir, request, proposal, plan, verifier=verifier)
        finally:
            try:
                reset_worktree(str(repo_dir))
            except Exception:  # noqa: BLE001 - cleanup failure must not mask the real outcome
                logger.warning(
                    "failed to reset worktree after macOS verification", exc_info=True,
                )
    except Exception:  # noqa: BLE001 - every outcome must become a report
        logger.exception("macOS verification raised unexpectedly")
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=(
                "An internal error stopped macOS verification before a fix could "
                "be confirmed; see the bot run logs for details."
            ),
            proposal=proposal, other_failing_checks=proposal.other_failing_checks,
        )


def _macos_fix_loop(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, plan: VerificationPlan,
    *, verifier: VerifyBackend,
) -> FixOutcome:
    """Apply, review, and remotely verify the fix up to N times with feedback.

    Each attempt starts from a clean tree and feeds the previous rejection or
    failed run's log tail back to the agent. The caller resets the worktree on
    exit, so this loop never has to.
    """
    feedback = ""
    last_review: Any = None
    last_summary = "no macOS verification attempt made"
    last_run_url = ""

    for _attempt in range(1, _MACOS_FIX_MAX_ATTEMPTS + 1):
        reset_worktree(str(repo_dir))

        applied = apply_fix(str(repo_dir), proposal, feedback=feedback, policy=request.policy)
        if not applied.applied:
            return _refuse(proposal, declined_detail(applied))

        reviewed = build_and_review_patch(str(repo_dir), applied.changed, proposal, policy=request.policy)
        last_review = reviewed.review
        if not reviewed.ok:
            if reviewed.review is None:
                return FixOutcome(
                    kind=OutcomeKind.REFUSED, summary=reviewed.detail,
                    proposal=proposal, review=reviewed.review,
                    other_failing_checks=proposal.other_failing_checks,
                )
            feedback = _rejection_feedback(reviewed.review, reviewed.patch)
            last_summary = reviewed.detail
            continue

        result = verifier.verify(str(repo_dir), plan, reviewed.patch)
        last_run_url = result.run_url
        if result.verified:
            return _ready(
                proposal, patch=reviewed.patch, changed_paths=applied.changed,
                review=reviewed.review, verify_backend=backend_label(VerifyEnv.MACOS),
                macos_run_url=result.run_url,
            )
        if not result.ran:
            return FixOutcome(
                kind=OutcomeKind.REFUSED, summary=result.detail,
                proposal=proposal, review=reviewed.review, macos_run_url=result.run_url,
                other_failing_checks=proposal.other_failing_checks,
            )
        feedback = _macos_retry_feedback(result)
        last_summary = result.detail

    return FixOutcome(
        kind=OutcomeKind.REFUSED, summary=last_summary,
        proposal=proposal, review=last_review, macos_run_url=last_run_url,
        other_failing_checks=proposal.other_failing_checks,
    )


def _rejection_feedback(review: ReviewVerdict, patch: str) -> str:
    return (
        f"A reviewer rejected your previous fix: {review.reasoning}\n\n"
        f"Your previous diff was:\n{patch}\n\n"
        "Address the rejection; do not reproduce the same change."
    )


def _macos_retry_feedback(result: VerificationResult) -> str:
    lines = [
        f"The previous macOS verification run failed: {result.detail}",
    ]
    if result.run_url:
        lines.append(f"Run URL: {result.run_url}")
    if result.output_tail:
        lines.append("Output tail:")
        lines.append(result.output_tail)
    else:
        lines.append("No macOS log tail was available; inspect the run URL if needed.")
    return "\n".join(lines)


def _ready(
    proposal: FixProposal, *, patch: str, changed_paths: tuple[str, ...],
    review: ReviewVerdict | None, run_result: Any = None, verify_backend: str = "",
    macos_run_url: str = "",
) -> FixOutcome:
    return FixOutcome(
        kind=OutcomeKind.READY,
        summary=f"Fix for {proposal.failing_check}",
        proposal=proposal, run_result=run_result, review=review,
        verify_backend=verify_backend, macos_run_url=macos_run_url,
        other_failing_checks=proposal.other_failing_checks,
        patch=patch, changed_paths=changed_paths,
    )


def _refuse(
    proposal: FixProposal, summary: str, *, review: ReviewVerdict | None = None,
) -> FixOutcome:
    return FixOutcome(
        kind=OutcomeKind.REFUSED, summary=summary, proposal=proposal, review=review,
        other_failing_checks=proposal.other_failing_checks,
    )


def _match_failed_job(hint: str, failed_jobs: tuple[str, ...]) -> str | None:
    """Return the single failed job the AI's ``hint`` refers to, or None.

    Requires the hint to correspond to a job that actually failed in the linked
    run, so the AI cannot pick an arbitrary or safer job. Matches exactly, or on
    the base name before a matrix suffix (GitHub names matrix legs like
    ``test-sanitizer (clang)``). If more than one failed job shares that base
    name (e.g. ``test (a)`` and ``test (b)`` both failed and the hint is
    ``test``), the target is ambiguous and we return None rather than guess.
    """
    if not hint or not failed_jobs:
        return None
    exact = [j for j in failed_jobs if j == hint]
    if exact:
        return exact[0]
    hint_base = hint.split(" (")[0]
    base_matches = [j for j in failed_jobs if j.split(" (")[0] == hint_base]
    if len(base_matches) == 1:
        return base_matches[0]
    return None  # zero matches, or ambiguous (multiple matrix legs)


_MAX_WORKFLOW_BYTES = 1024 * 1024  # workflow YAML over 1 MiB is not a real workflow


def read_workflow_safely(path: Path) -> str | None:
    """Read a workflow file from an untrusted checkout, or return ``None``.

    The checkout is PR-controlled, so skip symlinks (which could point outside
    the tree), cap the size, and swallow ``OSError`` rather than letting a
    crafted entry abort classification.
    """
    try:
        if path.is_symlink() or not path.is_file():
            return None
        if path.stat().st_size > _MAX_WORKFLOW_BYTES:
            return None
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _classify_failing_job(repo_dir: Path, failing_job: str) -> JobEnvironment:
    """Classify the failed job's environment from the repo's own workflows.

    Code (not the AI) decides the environment. Job names are not unique across
    workflow files: if the same name appears in more than one workflow with
    different environments, we cannot tell which produced the failure, so we
    refuse rather than guess. If all matches agree, that environment is used.
    """
    workflows = repo_dir / ".github" / "workflows"
    if not workflows.is_dir():
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason="no .github/workflows in the repo")

    matches: list[JobEnvironment] = []
    for path in sorted(workflows.glob("*.y*ml")):
        content = read_workflow_safely(path)
        if content is None:
            continue
        env = classify_job_environment(content, failing_job)
        if env.env is not VerifyEnv.UNSUPPORTED:
            matches.append(env)
    if not matches:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=f"job {failing_job!r} not found in any workflow, or its environment is unsupported",
        )
    if len({(m.env, m.image) for m in matches}) > 1:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=(
                f"job {failing_job!r} appears in multiple workflows with different "
                "environments; cannot determine which failed, refusing"
            ),
        )
    return matches[0]
