"""Top-level orchestration for ``@valkeyrie-bot fix <ci-link>``.

Thin wiring over a clean data flow; every refusal returns a ``FixOutcome`` so
the workflow always posts an explanatory comment:

    gate                         -> FixRequest | refuse
    failed_jobs_for_run (code)   -> the jobs that actually failed
    download logs, clone at SHA
    diagnose (read-only AI)      -> FixProposal (a fix + a job *hint*)
    plan_verification (code)     -> VerificationPlan (code-selected backend)
    apply + review               -> approved PatchReview
    backend.verify(plan)         -> repeated baseline/candidate facts
    policy + namespace push      -> FixOutcome (push | handoff | refuse)
"""

from __future__ import annotations

import logging
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from scripts.ci_fix.apply import PortApplyError, apply_fix, apply_port_commit
from scripts.ci_fix.diagnose import diagnose_failure, write_logs_to_workspace
from scripts.ci_fix.gate import GateRejection, ParsedCommand, build_fix_request
from scripts.ci_fix.models import (
    BaselineEvidence,
    BaselineKind,
    FixOutcome,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
    ReviewVerdict,
)
from scripts.ci_fix.policy import (
    DEFAULT_AUTO_PUBLISH_PATTERNS,
    DEFAULT_PROTECTED_PATTERNS,
    authored_publication_decision,
    port_publication_decision,
)
from scripts.ci_fix.port_discovery import PortCandidate, discover_port_candidates
from scripts.ci_fix.push import PushRefused, commit_and_push_fix, commit_and_push_port
from scripts.ci_fix.review import (
    DEFAULT_BASELINE_RUNS,
    DEFAULT_FLAKY_VERIFY_RUNS,
    DEFAULT_VERIFY_RUNS,
    build_and_review_patch,
    combined_command,
    precheck_command,
    reset_worktree,
    sampling_policy,
)
from scripts.ci_fix.verify.base import (
    VerificationPhase,
    VerificationPlan,
    VerificationResult,
    VerifyBackend,
    VerifyEnv,
    backend_label,
)
from scripts.ci_fix.verify.github_runs import failed_jobs_for_run
from scripts.ci_fix.verify.workflow_env import JobEnvironment, classify_job_environment
from scripts.common.git_auth import GitEnvironment
from scripts.common.git_clone import shallow_clone_at_sha
from scripts.common.proc import EmptyPatch, build_approved_patch
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

Diagnose = Callable[..., FixProposal]
Push = Callable[..., str]
PortPush = Callable[..., str]

_REMOTE_FIX_MAX_ATTEMPTS = 5
DEFAULT_REMOTE_PARALLELISM = 5
DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS = 15 * 60
DEFAULT_REMOTE_BUDGET_SECONDS = 45 * 60


@dataclass(frozen=True)
class _PortCandidateEvidence:
    """Normalized workflow verdict for one immutable historical port."""

    ran: bool
    passed: bool
    detail: str
    run_url: str = ""


def run_ci_fix(
    gh: Any,
    *,
    command: ParsedCommand,
    pr_repo_full_name: str,
    pr_number: int,
    commenter: str,
    git_env: GitEnvironment,
    artifact_client: ArtifactClient,
    org: str = "valkey-io",
    auth_team: str = "contributors",
    verify_runs: int = DEFAULT_VERIFY_RUNS,
    diagnose_func: Diagnose = diagnose_failure,
    push_func: Push = commit_and_push_fix,
    port_push_func: PortPush = commit_and_push_port,
    linux_verifier: VerifyBackend | None = None,
    macos_verifier: VerifyBackend | None = None,
    exact_verifier: VerifyBackend | None = None,
    history_branches: tuple[str, ...] = (),
    baseline_runs: int = DEFAULT_BASELINE_RUNS,
    flaky_verify_runs: int = DEFAULT_FLAKY_VERIFY_RUNS,
    minimum_confidence: float = 0.8,
    protected_paths: tuple[str, ...] = DEFAULT_PROTECTED_PATTERNS,
    auto_publish_paths: tuple[str, ...] = DEFAULT_AUTO_PUBLISH_PATTERNS,
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",),
    remote_parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    remote_sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    remote_budget_seconds: int = DEFAULT_REMOTE_BUDGET_SECONDS,
) -> FixOutcome:
    """Run the whole pipeline and return a terminal ``FixOutcome``."""
    request = build_fix_request(
        gh, command=command, pr_repo_full_name=pr_repo_full_name,
        pr_number=pr_number, commenter=commenter, org=org, auth_team=auth_team,
    )
    if isinstance(request, GateRejection):
        return FixOutcome(kind=OutcomeKind.REFUSED, summary=request.reason)

    failed_jobs = tuple(j.name for j in failed_jobs_for_run(gh, request.repo_full_name, request.run_id))

    with tempfile.TemporaryDirectory(prefix="ci-fix-") as workdir_str:
        outcome = _run_in_workspace(
            Path(workdir_str), request, failed_jobs,
            artifact_client=artifact_client, git_env=git_env,
            diagnose_func=diagnose_func, push_func=push_func,
            port_push_func=port_push_func, linux_verifier=linux_verifier,
            macos_verifier=macos_verifier, exact_verifier=exact_verifier,
            verify_runs=verify_runs, history_branches=history_branches,
            baseline_runs=baseline_runs, flaky_verify_runs=flaky_verify_runs,
            minimum_confidence=minimum_confidence,
            protected_paths=protected_paths, auto_publish_paths=auto_publish_paths,
            allowed_branch_prefixes=allowed_branch_prefixes,
            remote_parallelism=remote_parallelism,
            remote_sample_timeout_seconds=remote_sample_timeout_seconds,
            remote_budget_seconds=remote_budget_seconds,
        )
    run_url = f"https://github.com/{request.repo_full_name}/actions/runs/{request.run_id}"
    return replace(outcome, failing_run_url=run_url)


def _run_in_workspace(
    workdir: Path,
    request: FixRequest,
    failed_jobs: tuple[str, ...],
    *,
    artifact_client: ArtifactClient,
    git_env: GitEnvironment,
    diagnose_func: Diagnose,
    push_func: Push,
    port_push_func: PortPush,
    linux_verifier: VerifyBackend | None,
    macos_verifier: VerifyBackend | None,
    exact_verifier: VerifyBackend | None,
    verify_runs: int,
    history_branches: tuple[str, ...],
    baseline_runs: int,
    flaky_verify_runs: int,
    minimum_confidence: float,
    protected_paths: tuple[str, ...],
    auto_publish_paths: tuple[str, ...],
    allowed_branch_prefixes: tuple[str, ...],
    remote_parallelism: int,
    remote_sample_timeout_seconds: int,
    remote_budget_seconds: int,
) -> FixOutcome:
    logs = artifact_client.download_run_logs(request.repo_full_name, request.run_id)
    if not logs:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary="The run's logs have expired and can no longer be downloaded; cannot diagnose.",
        )
    logs_dir = write_logs_to_workspace(logs, workdir)

    repo_dir = workdir / "repo"
    if not shallow_clone_at_sha(request.repo_full_name, repo_dir, request.head_sha):
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=f"Could not clone {request.repo_full_name} at {request.head_sha[:12]}.",
        )

    port_candidates = discover_port_candidates(
        str(repo_dir), str(logs_dir), history_branches=history_branches,
    )
    proposal = diagnose_func(
        str(logs_dir), str(repo_dir), hint=request.hint,
        port_candidates=port_candidates,
    )
    if proposal.path is FixPath.REFUSE:
        return _refuse(proposal, proposal.reasoning or "No safe fix found.")
    if proposal.confidence < minimum_confidence:
        return _refuse(
            proposal,
            f"Diagnosis confidence {proposal.confidence:.2f} is below this "
            f"repository's {minimum_confidence:.2f} action threshold; "
            "reporting the diagnosis without changing the branch.",
        )

    port_candidate: PortCandidate | None = None
    if proposal.path is FixPath.PORT:
        resolved = _resolve_port_candidate(proposal, port_candidates)
        if isinstance(resolved, str):
            return _refuse(proposal, resolved)
        port_candidate = resolved

    plan = _plan_verification(
        repo_dir,
        request,
        proposal,
        failed_jobs,
        exact_verifier_available=exact_verifier is not None,
    )
    if isinstance(plan, str):  # a refusal reason
        return _refuse(proposal, plan)

    verifier = _verifier_for_plan(
        plan,
        linux_verifier=linux_verifier,
        macos_verifier=macos_verifier,
        exact_verifier=exact_verifier,
    )
    if port_candidate is not None:
        return _verify_port_and_push(
            repo_dir,
            request,
            proposal,
            port_candidate,
            plan,
            verifier=verifier,
            git_env=git_env,
            port_push_func=port_push_func,
            verify_runs=verify_runs,
            baseline_runs=baseline_runs,
            flaky_verify_runs=flaky_verify_runs,
            protected_paths=protected_paths,
            allowed_branch_prefixes=allowed_branch_prefixes,
            remote_parallelism=remote_parallelism,
            remote_sample_timeout_seconds=remote_sample_timeout_seconds,
            remote_budget_seconds=remote_budget_seconds,
        )

    return _verify_remotely_and_push(
        repo_dir, request, proposal, plan,
        verifier=verifier, git_env=git_env, push_func=push_func,
        verify_runs=verify_runs, baseline_runs=baseline_runs,
        flaky_verify_runs=flaky_verify_runs,
        allowed_branch_prefixes=allowed_branch_prefixes,
        protected_paths=protected_paths, auto_publish_paths=auto_publish_paths,
        remote_parallelism=remote_parallelism,
        remote_sample_timeout_seconds=remote_sample_timeout_seconds,
        remote_budget_seconds=remote_budget_seconds,
    )


def _plan_verification(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, failed_jobs: tuple[str, ...],
    *,
    exact_verifier_available: bool = False,
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
    if exact_verifier_available:
        env = JobEnvironment(VerifyEnv.TARGET)
    else:
        env = _classify_failing_job(repo_dir, job)
    return VerificationPlan(
        env=env.env,
        command=combined_command(proposal),
        workdir=proposal.workdir,
        image=env.image,
        job_name=job,
        head_sha=request.head_sha,
        target_repo=request.head_repo_full_name,
        source_run_id=request.run_id,
        unsupported_reason=env.reason,
    )


def _verifier_for_plan(
    plan: VerificationPlan,
    *,
    linux_verifier: VerifyBackend | None,
    macos_verifier: VerifyBackend | None,
    exact_verifier: VerifyBackend | None,
) -> VerifyBackend | None:
    """Return the one backend selected by code for this failed job."""
    if plan.env in {VerifyEnv.LOCAL, VerifyEnv.DOCKER}:
        return linux_verifier
    if plan.env is VerifyEnv.MACOS:
        return macos_verifier
    if plan.env is VerifyEnv.TARGET:
        return exact_verifier
    return None


def _resolve_port_candidate(
    proposal: FixProposal,
    port_candidates: tuple[PortCandidate, ...],
) -> PortCandidate | str:
    """Resolve a model-selected SHA to one code-discovered trusted candidate."""
    if not proposal.unstable_fix_commit.strip():
        return "diagnosis chose PORT but did not name a historical fix commit"

    chosen = proposal.unstable_fix_commit.strip()
    candidate = _canonical_candidate(chosen, port_candidates)
    if candidate is None:
        return (
            f"The chosen commit {chosen[:12]} is not among the "
            "fixes discovered for this failure; refusing to port a commit the code "
            "did not surface as a candidate."
        )
    return candidate


def _verify_port_and_push(
    repo_dir: Path,
    request: FixRequest,
    proposal: FixProposal,
    candidate: PortCandidate,
    plan: VerificationPlan,
    *,
    verifier: VerifyBackend | None,
    git_env: GitEnvironment,
    port_push_func: PortPush = commit_and_push_port,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
    baseline_runs: int = DEFAULT_BASELINE_RUNS,
    flaky_verify_runs: int = DEFAULT_FLAKY_VERIFY_RUNS,
    protected_paths: tuple[str, ...] = DEFAULT_PROTECTED_PATTERNS,
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",),
    remote_parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    remote_sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    remote_budget_seconds: int = DEFAULT_REMOTE_BUDGET_SECONDS,
) -> FixOutcome:
    """Prove a trusted historical commit before publishing the original port.

    The candidate is applied without committing in this disposable checkout,
    then verified with the same clean-baseline and repeated-candidate policy as
    an authored fix. Publication still uses ``commit_and_push_port`` in a fresh
    clone, preserving the original author and independently rechecking source
    branch reachability.
    """
    if plan.env in {VerifyEnv.LOCAL, VerifyEnv.DOCKER, VerifyEnv.MACOS}:
        precheck = precheck_command(proposal)
        if precheck:
            return _refuse(proposal, precheck)

    baseline_runs = max(1, baseline_runs)
    remote_parallelism = max(1, remote_parallelism)
    remote_sample_timeout_seconds = max(1, remote_sample_timeout_seconds)
    baseline: BaselineEvidence | None = None
    verification_run_url = ""

    try:
        reset_worktree(str(repo_dir))
        if verifier is None:
            baseline = BaselineEvidence(
                kind=BaselineKind.UNAVAILABLE,
                attempts=0,
                passed=0,
                failed=0,
                unavailable=1,
                detail=_unavailable_verifier_detail(plan),
            )
            remote_deadline = None
        else:
            remote_deadline = time.monotonic() + max(1, remote_budget_seconds)
            baseline, remote_baseline_result = _observe_remote_baseline(
                str(repo_dir),
                plan,
                verifier=verifier,
                runs=baseline_runs,
                parallelism=remote_parallelism,
                sample_timeout_seconds=remote_sample_timeout_seconds,
                deadline=remote_deadline,
            )
            verification_run_url = remote_baseline_result.run_url

        assert baseline is not None
        policy = sampling_policy(
            baseline,
            proposal.failure_mode,
            verify_runs=verify_runs,
            flaky_verify_runs=flaky_verify_runs,
        )

        reset_worktree(str(repo_dir))
        changed = apply_port_commit(str(repo_dir), candidate.sha)
        policy_paths = tuple(sorted(set(candidate.paths).union(changed)))
        publication = port_publication_decision(
            policy_paths,
            protected_patterns=protected_paths,
        )
        source = candidate.source_branch or candidate.source_ref or "the default branch"

        evidence = _verify_port_candidate(
            repo_dir,
            plan,
            changed,
            baseline,
            verifier=verifier,
            runs=policy.candidate_runs,
            remote_parallelism=remote_parallelism,
            remote_sample_timeout_seconds=remote_sample_timeout_seconds,
            remote_deadline=remote_deadline,
        )
        verification_run_url = evidence.run_url or verification_run_url
        if not evidence.ran:
            return FixOutcome(
                kind=OutcomeKind.HANDOFF,
                summary=(
                    f"{evidence.detail}; handing off trusted commit "
                    f"{candidate.sha} from {source} instead of publishing "
                    "without complete runner evidence"
                ),
                proposal=proposal,
                review=_port_review(candidate, "candidate verification was unavailable"),
                verification_run_url=verification_run_url,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )
        if not evidence.passed:
            return FixOutcome(
                kind=OutcomeKind.REFUSED,
                summary=evidence.detail,
                proposal=proposal,
                verification_run_url=verification_run_url,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )

        review = _port_review(
            candidate,
            f"the targeted check passed {policy.candidate_runs} candidate sample(s)",
        )
        backend = backend_label(plan.env, plan.image)
        if policy.handoff_only:
            return FixOutcome(
                kind=OutcomeKind.HANDOFF,
                summary=(
                    f"{baseline.detail}; the port passed "
                    f"{policy.candidate_runs} candidate sample(s), but the linked "
                    "failure was not established on the clean tree"
                ),
                proposal=proposal,
                review=review,
                verify_backend=backend,
                verification_run_url=verification_run_url,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )
        if not publication.auto_publish:
            return FixOutcome(
                kind=OutcomeKind.HANDOFF,
                summary=(
                    f"The port passed {policy.candidate_runs} candidate sample(s), "
                    f"but {publication.reason}. Proposed historical port: "
                    f"{candidate.sha} from {source}."
                ),
                proposal=proposal,
                review=review,
                verify_backend=backend,
                verification_run_url=verification_run_url,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )
        return _publish_port(
            repo_dir,
            request,
            proposal,
            candidate,
            review=review,
            verify_backend=backend,
            verification_run_url=verification_run_url,
            baseline=baseline,
            git_env=git_env,
            port_push_func=port_push_func,
            allowed_branch_prefixes=allowed_branch_prefixes,
        )
    except PortApplyError as exc:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary=str(exc),
            proposal=proposal,
            other_failing_checks=proposal.other_failing_checks,
            baseline=baseline,
        )
    except Exception:  # noqa: BLE001 - every terminal state must become a comment
        logger.exception("historical port verification raised unexpectedly")
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=(
                "An internal error stopped historical port verification; "
                "see the bot run logs for details."
            ),
            proposal=proposal,
            other_failing_checks=proposal.other_failing_checks,
            baseline=baseline,
        )
    finally:
        try:
            reset_worktree(str(repo_dir))
        except Exception:  # noqa: BLE001 - cleanup must not mask the real outcome
            logger.warning("failed to reset worktree after port verification", exc_info=True)


def _verify_port_candidate(
    repo_dir: Path,
    plan: VerificationPlan,
    changed: tuple[str, ...],
    baseline: BaselineEvidence,
    *,
    verifier: VerifyBackend | None,
    runs: int,
    remote_parallelism: int,
    remote_sample_timeout_seconds: int,
    remote_deadline: float | None,
) -> _PortCandidateEvidence:
    """Run one immutable port through the selected backend and normalize its verdict."""
    if verifier is None or baseline.kind is BaselineKind.UNAVAILABLE:
        return _PortCandidateEvidence(
            ran=False,
            passed=False,
            detail=baseline.detail or _unavailable_verifier_detail(plan),
        )

    try:
        patch = build_approved_patch(str(repo_dir), changed)
    except EmptyPatch as exc:
        raise PortApplyError(
            f"historical port produced no verifiable patch: {exc}"
        ) from exc

    remote_result = _verify_remote_repeatedly(
        str(repo_dir),
        plan,
        patch,
        verifier=verifier,
        runs=runs,
        parallelism=remote_parallelism,
        sample_timeout_seconds=remote_sample_timeout_seconds,
        deadline=remote_deadline,
    )
    return _PortCandidateEvidence(
        ran=remote_result.ran,
        passed=remote_result.verified,
        detail=remote_result.detail,
        run_url=remote_result.run_url,
    )


def _port_review(candidate: PortCandidate, evidence: str) -> ReviewVerdict:
    source = candidate.source_branch or candidate.source_ref or "the default branch"
    return ReviewVerdict(
        approved=True,
        reasoning=(
            f"Commit {candidate.sha[:12]} is already merged on {source}; "
            f"{evidence}. Publication preserves its original authorship."
        ),
    )


def _publish_port(
    repo_dir: Path,
    request: FixRequest,
    proposal: FixProposal,
    candidate: PortCandidate,
    *,
    review: ReviewVerdict,
    git_env: GitEnvironment,
    port_push_func: PortPush,
    verify_backend: str,
    verification_run_url: str = "",
    baseline: BaselineEvidence,
    allowed_branch_prefixes: tuple[str, ...],
) -> FixOutcome:
    """Publish the original historical commit after candidate verification."""
    try:
        commit_sha = port_push_func(
            str(repo_dir),
            head_repo_full_name=request.head_repo_full_name,
            head_branch=request.head_branch,
            head_sha=request.head_sha,
            unstable_fix_commit=candidate.sha,
            source_ref=candidate.source_ref,
            git_env=git_env,
            allowed_branch_prefixes=allowed_branch_prefixes,
        )
    except PushRefused as exc:
        return FixOutcome(
            kind=OutcomeKind.REFUSED,
            summary=str(exc),
            proposal=proposal,
            review=review,
            verification_run_url=verification_run_url,
            other_failing_checks=proposal.other_failing_checks,
            baseline=baseline,
        )

    return FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary=f"Ported upstream fix for {proposal.failing_check}",
        proposal=proposal,
        review=review,
        commit_sha=commit_sha,
        verify_backend=verify_backend,
        verification_run_url=verification_run_url,
        other_failing_checks=proposal.other_failing_checks,
        baseline=baseline,
    )


def _verify_remotely_and_push(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, plan: VerificationPlan,
    *, verifier: VerifyBackend | None, git_env: GitEnvironment, push_func: Push,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
    baseline_runs: int = DEFAULT_BASELINE_RUNS,
    flaky_verify_runs: int = DEFAULT_FLAKY_VERIFY_RUNS,
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",),
    protected_paths: tuple[str, ...] = DEFAULT_PROTECTED_PATTERNS,
    auto_publish_paths: tuple[str, ...] = DEFAULT_AUTO_PUBLISH_PATTERNS,
    remote_parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    remote_sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    remote_budget_seconds: int = DEFAULT_REMOTE_BUDGET_SECONDS,
) -> FixOutcome:
    """Apply, review, and remotely verify with bounded feedback retries.

    When no faithful verifier is configured, the same authoring and skeptic
    review still run, but the approved patch is returned as a handoff.
    """
    try:
        # Agent-owned workflows execute the proposed command. A target-owned
        # workflow selects its own trusted recipe and never receives it.
        if plan.env in {VerifyEnv.LOCAL, VerifyEnv.DOCKER, VerifyEnv.MACOS}:
            precheck = precheck_command(proposal)
            if precheck:
                return _refuse(proposal, precheck)

        # The push path extracts an approved patch and applies it in a separate
        # clean clone, so resetting this worktree on the way out is safe
        # for every outcome, including a successful push.
        try:
            return _remote_fix_loop(
                repo_dir, request, proposal, plan,
                verifier=verifier, git_env=git_env, push_func=push_func,
                verify_runs=verify_runs,
                baseline_runs=baseline_runs,
                flaky_verify_runs=flaky_verify_runs,
                allowed_branch_prefixes=allowed_branch_prefixes,
                protected_paths=protected_paths,
                auto_publish_paths=auto_publish_paths,
                remote_parallelism=remote_parallelism,
                remote_sample_timeout_seconds=remote_sample_timeout_seconds,
                remote_budget_seconds=remote_budget_seconds,
            )
        finally:
            try:
                reset_worktree(str(repo_dir))
            except Exception:  # noqa: BLE001 - cleanup failure must not mask the real outcome
                logger.warning(
                    "failed to reset worktree after remote verification", exc_info=True,
                )
    except Exception:  # noqa: BLE001 - every outcome must become a comment
        logger.exception("remote verification raised unexpectedly")
        return FixOutcome(
            kind=OutcomeKind.FAILED,
            summary=(
                "An internal error stopped remote verification before a fix could "
                "be confirmed; see the bot run logs for details."
            ),
            proposal=proposal, other_failing_checks=proposal.other_failing_checks,
        )


def _remote_fix_loop(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, plan: VerificationPlan,
    *, verifier: VerifyBackend | None, git_env: GitEnvironment, push_func: Push,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
    baseline_runs: int = DEFAULT_BASELINE_RUNS,
    flaky_verify_runs: int = DEFAULT_FLAKY_VERIFY_RUNS,
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",),
    protected_paths: tuple[str, ...] = DEFAULT_PROTECTED_PATTERNS,
    auto_publish_paths: tuple[str, ...] = DEFAULT_AUTO_PUBLISH_PATTERNS,
    remote_parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    remote_sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    remote_budget_seconds: int = DEFAULT_REMOTE_BUDGET_SECONDS,
) -> FixOutcome:
    """Sample, author, review, and remotely verify a candidate.

    Each attempt starts from a clean tree and feeds the previous rejection or
    failed run's log tail back to the agent. The caller resets the worktree on
    exit, so this loop never has to.
    """
    verify_runs = max(1, verify_runs)
    baseline_runs = max(1, baseline_runs)
    flaky_verify_runs = max(verify_runs, flaky_verify_runs)
    remote_parallelism = max(1, remote_parallelism)
    remote_sample_timeout_seconds = max(1, remote_sample_timeout_seconds)
    remote_deadline = time.monotonic() + max(1, remote_budget_seconds)

    if verifier is None:
        baseline = BaselineEvidence(
            kind=BaselineKind.UNAVAILABLE,
            attempts=0,
            passed=0,
            failed=0,
            unavailable=1,
            detail=_unavailable_verifier_detail(plan),
        )
        baseline_result = None
    else:
        baseline, baseline_result = _observe_remote_baseline(
            str(repo_dir),
            plan,
            verifier=verifier,
            runs=baseline_runs,
            parallelism=remote_parallelism,
            sample_timeout_seconds=remote_sample_timeout_seconds,
            deadline=remote_deadline,
        )

    policy = sampling_policy(
        baseline,
        proposal.failure_mode,
        verify_runs=verify_runs,
        flaky_verify_runs=flaky_verify_runs,
    )
    effective_proposal = (
        replace(proposal, failure_mode=policy.observed_mode)
        if policy.observed_mode is not proposal.failure_mode
        else proposal
    )

    feedback = ""
    last_review: Any = None
    last_summary = "no remote verification attempt made"
    last_run_url = baseline_result.run_url if baseline_result is not None else ""
    backend = backend_label(plan.env, plan.image)

    for _attempt in range(1, _REMOTE_FIX_MAX_ATTEMPTS + 1):
        reset_worktree(str(repo_dir))

        applied, changed = apply_fix(
            str(repo_dir),
            effective_proposal,
            feedback=feedback,
        )
        if not applied:
            return FixOutcome(
                kind=OutcomeKind.REFUSED,
                summary="fix not applied (agent declined or made no edits)",
                proposal=proposal,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )

        reviewed = build_and_review_patch(
            str(repo_dir),
            changed,
            effective_proposal,
        )
        last_review = reviewed.review
        if not reviewed.ok:
            if reviewed.review is None:
                return FixOutcome(
                    kind=OutcomeKind.REFUSED, summary=reviewed.detail,
                    proposal=proposal, review=reviewed.review,
                    other_failing_checks=proposal.other_failing_checks,
                    baseline=baseline,
                )
            feedback = (
                f"A reviewer rejected your previous fix: {reviewed.review.reasoning}\n\n"
                f"Your previous diff was:\n{reviewed.patch}\n\n"
                "Address the rejection; do not reproduce the same change."
            )
            last_summary = reviewed.detail
            continue

        if verifier is None or baseline.kind is BaselineKind.UNAVAILABLE:
            reason = baseline.detail or _unavailable_verifier_detail(plan)
            return FixOutcome(
                kind=OutcomeKind.HANDOFF,
                summary=(
                    f"{reason}; handing off the reviewed patch instead of "
                    "claiming an environment-faithful fix"
                ),
                proposal=proposal,
                review=reviewed.review,
                handoff_patch=reviewed.patch,
                verification_run_url=last_run_url,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
            )

        result = _verify_remote_repeatedly(
            str(repo_dir),
            plan,
            reviewed.patch,
            verifier=verifier,
            runs=policy.candidate_runs,
            parallelism=remote_parallelism,
            sample_timeout_seconds=remote_sample_timeout_seconds,
            deadline=remote_deadline,
        )
        last_run_url = result.run_url
        if result.verified:
            if policy.handoff_only:
                return FixOutcome(
                    kind=OutcomeKind.HANDOFF,
                    summary=(
                        f"{baseline.detail}; the candidate passed "
                        f"{policy.candidate_runs} remote sample(s), but the "
                        "linked failure was not established on the clean tree"
                    ),
                    proposal=proposal,
                    review=reviewed.review,
                    verify_backend=backend,
                    handoff_patch=reviewed.patch,
                    other_failing_checks=proposal.other_failing_checks,
                    baseline=baseline,
                    verification_run_url=result.run_url,
                )
            publication = authored_publication_decision(
                changed,
                protected_patterns=protected_paths,
                auto_publish_patterns=auto_publish_paths,
            )
            if not publication.auto_publish:
                return FixOutcome(
                    kind=OutcomeKind.HANDOFF,
                    summary=(
                        "The candidate passed remote verification, but "
                        f"{publication.reason}"
                    ),
                    proposal=proposal,
                    review=reviewed.review,
                    verify_backend=backend,
                    handoff_patch=reviewed.patch,
                    other_failing_checks=proposal.other_failing_checks,
                    baseline=baseline,
                    verification_run_url=result.run_url,
                )
            return _push(
                repo_dir, request, proposal, changed,
                review=reviewed.review,
                verify_backend=backend,
                verification_run_url=result.run_url,
                git_env=git_env,
                push_func=push_func,
                baseline=baseline,
                allowed_branch_prefixes=allowed_branch_prefixes,
            )
        if not result.ran:
            return FixOutcome(
                kind=OutcomeKind.HANDOFF,
                summary=(
                    f"{result.detail}; handing off the reviewed patch instead "
                    "of treating missing remote evidence as a pass"
                ),
                proposal=proposal,
                review=reviewed.review,
                handoff_patch=reviewed.patch,
                other_failing_checks=proposal.other_failing_checks,
                baseline=baseline,
                verification_run_url=result.run_url,
            )
        feedback = _remote_retry_feedback(result)
        last_summary = result.detail

    return FixOutcome(
        kind=OutcomeKind.REFUSED, summary=last_summary,
        proposal=proposal, review=last_review,
        other_failing_checks=proposal.other_failing_checks,
        baseline=baseline,
        verification_run_url=last_run_url,
    )


def _observe_remote_baseline(
    repo_dir: str,
    plan: VerificationPlan,
    *,
    verifier: VerifyBackend,
    runs: int,
    parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    deadline: float | None = None,
) -> tuple[BaselineEvidence, VerificationResult]:
    """Run clean remote samples and classify deterministic/flaky behavior."""
    results = _run_remote_samples(
        repo_dir,
        plan,
        "",
        verifier=verifier,
        phase=VerificationPhase.BASELINE,
        runs=runs,
        parallelism=parallelism,
        sample_timeout_seconds=sample_timeout_seconds,
        deadline=deadline,
        stop_on_failure=False,
    )

    passed = sum(result.ran and result.verified for result in results)
    failed = sum(result.ran and not result.verified for result in results)
    unavailable = sum(not result.ran for result in results)
    attempts = len(results)
    if unavailable:
        kind = BaselineKind.UNAVAILABLE
        unavailable_detail = next(result.detail for result in results if not result.ran)
        detail = (
            "remote baseline verifier was unavailable before sampling completed: "
            f"{unavailable_detail}"
        )
    elif failed and passed:
        kind = BaselineKind.FLAKY
        detail = (
            f"clean remote baseline was flaky: {failed} failure(s) and "
            f"{passed} pass(es) in {attempts} run(s)"
        )
    elif failed:
        kind = BaselineKind.DETERMINISTIC
        detail = f"clean remote baseline failed {failed}/{attempts} run(s)"
    else:
        kind = BaselineKind.NOT_REPRODUCED
        detail = f"clean remote baseline passed all {passed}/{attempts} run(s)"
    return BaselineEvidence(
        kind=kind,
        attempts=attempts,
        passed=passed,
        failed=failed,
        unavailable=unavailable,
        detail=detail,
    ), results[-1]


def _verify_remote_repeatedly(
    repo_dir: str,
    plan: VerificationPlan,
    patch: str,
    *,
    verifier: VerifyBackend,
    runs: int,
    parallelism: int = DEFAULT_REMOTE_PARALLELISM,
    sample_timeout_seconds: int = DEFAULT_REMOTE_SAMPLE_TIMEOUT_SECONDS,
    deadline: float | None = None,
) -> VerificationResult:
    """Require every remote candidate repetition to produce a green verdict."""
    results = _run_remote_samples(
        repo_dir,
        plan,
        patch,
        verifier=verifier,
        phase=VerificationPhase.CANDIDATE,
        runs=runs,
        parallelism=parallelism,
        sample_timeout_seconds=sample_timeout_seconds,
        deadline=deadline,
        stop_on_failure=True,
    )
    return next(
        (result for result in results if not result.ran or not result.verified),
        results[-1],
    )


def _run_remote_samples(
    repo_dir: str,
    plan: VerificationPlan,
    patch: str,
    *,
    verifier: VerifyBackend,
    phase: VerificationPhase,
    runs: int,
    parallelism: int,
    sample_timeout_seconds: int,
    deadline: float | None,
    stop_on_failure: bool,
) -> list[VerificationResult]:
    """Run bounded sample batches and preserve repetition order in the result.

    A batch completes before another is dispatched. This bounds fan-out and
    avoids launching additional candidate runs after a failed batch. Each plan
    carries the smaller of the per-sample timeout and remaining campaign budget,
    so executor shutdown cannot wait indefinitely beyond the campaign deadline.
    """
    total = max(1, runs)
    width = min(total, max(1, parallelism))
    per_sample_timeout = max(1, sample_timeout_seconds)
    campaign_deadline = (
        deadline
        if deadline is not None
        else time.monotonic() + DEFAULT_REMOTE_BUDGET_SECONDS
    )
    results: list[VerificationResult] = []

    for first in range(1, total + 1, width):
        remaining = campaign_deadline - time.monotonic()
        if remaining <= 0:
            exhausted = _remote_budget_exhausted(phase)
            if results:
                exhausted = replace(exhausted, run_url=results[-1].run_url)
            results.append(exhausted)
            break

        timeout = max(1, min(per_sample_timeout, int(remaining)))
        repetitions = range(first, min(first + width, total + 1))
        sample_plans = [
            replace(
                plan,
                phase=phase,
                repetition=repetition,
                repetition_count=total,
                timeout_seconds=timeout,
            )
            for repetition in repetitions
        ]
        with ThreadPoolExecutor(
            max_workers=len(sample_plans),
            thread_name_prefix="ci-fix-verify",
        ) as executor:
            futures = [
                executor.submit(
                    _run_remote_sample,
                    verifier,
                    repo_dir,
                    sample_plan,
                    patch,
                )
                for sample_plan in sample_plans
            ]
            batch = [future.result() for future in futures]

        results.extend(batch)
        if any(not result.ran for result in batch):
            break
        if stop_on_failure and any(not result.verified for result in batch):
            break

    return results


def _run_remote_sample(
    verifier: VerifyBackend,
    repo_dir: str,
    plan: VerificationPlan,
    patch: str,
) -> VerificationResult:
    """Turn a backend exception into unavailable evidence for this sample."""
    try:
        return verifier.verify(repo_dir, plan, patch)
    except Exception:  # noqa: BLE001 - remote evidence failures become handoffs
        logger.exception(
            "remote %s verification sample %d/%d raised",
            plan.phase.value,
            plan.repetition,
            plan.repetition_count,
        )
        return VerificationResult(
            verified=False,
            ran=False,
            detail=(
                f"remote {plan.phase.value} verification sample "
                f"{plan.repetition}/{plan.repetition_count} failed internally"
            ),
        )


def _remote_budget_exhausted(phase: VerificationPhase) -> VerificationResult:
    return VerificationResult(
        verified=False,
        ran=False,
        detail=f"remote {phase.value} sampling budget was exhausted",
    )


def _unavailable_verifier_detail(plan: VerificationPlan) -> str:
    if plan.unsupported_reason:
        return (
            f"the failed job needs a target-owned verifier "
            f"({plan.unsupported_reason}), but none is configured"
        )
    if plan.env is VerifyEnv.MACOS:
        return "macOS verification is not configured for this repository"
    if plan.env in {VerifyEnv.LOCAL, VerifyEnv.DOCKER}:
        return "Linux Actions verification is not configured for this repository"
    return "target-owned exact-environment verification is not configured"


def _remote_retry_feedback(result: VerificationResult) -> str:
    lines = [
        f"The previous remote verification run failed: {result.detail}",
    ]
    if result.run_url:
        lines.append(f"Run URL: {result.run_url}")
    if result.output_tail:
        lines.append("Output tail:")
        lines.append(result.output_tail)
    else:
        lines.append("No remote log tail was available; inspect the run URL if needed.")
    return "\n".join(lines)


def _push(
    repo_dir: Path, request: FixRequest, proposal: FixProposal, changed_paths: tuple[str, ...],
    *, review: Any = None, verify_backend: str = "",
    verification_run_url: str = "",
    git_env: GitEnvironment, push_func: Push,
    baseline: Any = None,
    allowed_branch_prefixes: tuple[str, ...] = ("agent/backport/",),
) -> FixOutcome:
    try:
        commit_sha = push_func(
            str(repo_dir),
            head_repo_full_name=request.head_repo_full_name,
            head_branch=request.head_branch,
            head_sha=request.head_sha,
            proposal=proposal,
            changed_paths=changed_paths,
            git_env=git_env,
            allowed_branch_prefixes=allowed_branch_prefixes,
        )
    except PushRefused as exc:
        return FixOutcome(
            kind=OutcomeKind.REFUSED, summary=str(exc),
            proposal=proposal, review=review,
            verification_run_url=verification_run_url,
            other_failing_checks=proposal.other_failing_checks,
            baseline=baseline,
        )
    return FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary=f"Pushed fix for {proposal.failing_check}",
        proposal=proposal, review=review, commit_sha=commit_sha,
        verify_backend=verify_backend,
        verification_run_url=verification_run_url,
        other_failing_checks=proposal.other_failing_checks,
        baseline=baseline,
    )


def _refuse(proposal: FixProposal, summary: str) -> FixOutcome:
    return FixOutcome(
        kind=OutcomeKind.REFUSED, summary=summary, proposal=proposal,
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


def _canonical_candidate(
    chosen: str, candidates: tuple[PortCandidate, ...],
) -> PortCandidate | None:
    """Resolve the model's chosen commit to one code-discovered candidate.

    The diagnosis prompt renders candidates as short (12-char) SHAs, so the
    model may echo a short SHA. Match by prefix in either direction while
    preserving the source-ref provenance attached by discovery. Ambiguous
    prefixes fail closed.
    """
    chosen = chosen.strip().lower()
    if not chosen:
        return None
    matches = [
        candidate
        for candidate in candidates
        if candidate.sha.lower().startswith(chosen)
        or chosen.startswith(candidate.sha.lower())
    ]
    unique = {
        candidate.sha: candidate
        for candidate in matches
    }
    if len(unique) == 1:
        return next(iter(unique.values()))
    return None


_MAX_WORKFLOW_BYTES = 1024 * 1024  # workflow YAML over 1 MiB is not a real workflow


def _read_workflow_safely(path: Path) -> str | None:
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
    base = failing_job.split(" (")[0]  # strip matrix suffix; jobs: key is the base name
    workflows = repo_dir / ".github" / "workflows"
    if not workflows.is_dir():
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason="no .github/workflows in the repo")

    matches: list[JobEnvironment] = []
    for path in sorted(workflows.glob("*.y*ml")):
        content = _read_workflow_safely(path)
        if content is None:
            continue
        env = classify_job_environment(content, base)
        if env.matched:
            matches.append(env)
    if not matches:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=f"job {base!r} not found in any workflow, or its environment is unsupported",
        )
    unsupported = [match for match in matches if match.env is VerifyEnv.UNSUPPORTED]
    if unsupported:
        reasons = "; ".join(
            dict.fromkeys(match.reason for match in unsupported if match.reason)
        )
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=(
                f"job {base!r} has a matching workflow definition that cannot be "
                f"replayed safely{f': {reasons}' if reasons else ''}"
            ),
        )
    if len({(m.env, m.image) for m in matches}) > 1:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=(
                f"job {base!r} appears in multiple workflows with different "
                "environments; cannot determine which failed, refusing"
            ),
        )
    return matches[0]
