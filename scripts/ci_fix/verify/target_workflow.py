"""Target-repository-owned exact-environment workflow verification.

The target repository owns the workflow and every command it executes.  The
agent supplies only factual identity and candidate data: the linked failed run
and job, gated head SHA, patch, phase, and repetition index.  In particular,
the AI-authored verification command is never sent to this authoritative
backend.

The configured workflow must include the correlation token in its run name:

``run-name: "ci-fix verify [token:${{ inputs.correlation }}]"``

It is dispatched from the registry-configured protected ref, not from the
candidate branch.  A fresh UUID plus a creation-time check binds the conclusion
to this invocation.
"""

from __future__ import annotations

import base64
import re
import time
import uuid
from typing import Any

from scripts.ci_fix.review import MAX_REVIEWABLE_PATCH_CHARS
from scripts.ci_fix.verify.actions import (
    WorkflowDispatchTransport,
    completed_workflow_result,
)
from scripts.ci_fix.verify.base import (
    VerificationPhase,
    VerificationPlan,
    VerificationResult,
    VerifyEnv,
)
from scripts.common.workflow_artifacts import ArtifactClient

_MAX_PATCH_BYTES = (MAX_REVIEWABLE_PATCH_CHARS * 4) // 3 + 1024
_DEFAULT_TIMEOUT_S = 2 * 60 * 60
_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_VERIFY_STEP_MARKERS = (
    "Run CI-fix verification",
    "Run exact verification",
    "Run targeted verification",
    "Run recipe",
)


class TargetWorkflowVerifier:
    """Dispatch and await one target-owned verification repetition."""

    def __init__(
        self,
        github_client: Any,
        *,
        repo_full_name: str,
        workflow: str,
        ref: str,
        timeout: int = _DEFAULT_TIMEOUT_S,
        artifact_client: ArtifactClient | None = None,
    ) -> None:
        self._repo = repo_full_name
        self._transport = WorkflowDispatchTransport(
            github_client,
            repo_full_name=repo_full_name,
            workflow=workflow,
            ref=ref,
            timeout=timeout,
            artifact_client=artifact_client,
            log_markers=_VERIFY_STEP_MARKERS,
            clock=time.time,
            sleep=time.sleep,
        )

    def verify(
        self,
        repo_dir: str,
        plan: VerificationPlan,
        patch: str,
    ) -> VerificationResult:
        """Verify one baseline or candidate sample in the target workflow."""
        del repo_dir  # The target workflow checks out the gated SHA itself.
        if plan.env is not VerifyEnv.TARGET:
            return VerificationResult(
                verified=False,
                ran=False,
                detail=(
                    "target workflow verifier cannot run environment "
                    f"{plan.env.value!r}"
                ),
            )
        if plan.target_repo != self._repo:
            return VerificationResult(
                verified=False,
                ran=False,
                detail=(
                    "target workflow repository does not match the gated "
                    f"repository ({plan.target_repo!r} != {self._repo!r})"
                ),
            )
        if (
            not plan.source_run_id
            or not plan.job_name
            or not _SHA_RE.fullmatch(plan.head_sha)
        ):
            return VerificationResult(
                verified=False,
                ran=False,
                detail="target workflow plan is missing failed-run identity",
            )
        if not 1 <= plan.repetition <= plan.repetition_count <= 100:
            return VerificationResult(
                verified=False,
                ran=False,
                detail="target workflow plan has an invalid repetition range",
            )
        if plan.phase is VerificationPhase.BASELINE and patch:
            return VerificationResult(
                verified=False,
                ran=False,
                detail="baseline target-workflow verification cannot include a patch",
            )
        if plan.phase is VerificationPhase.CANDIDATE and not patch:
            return VerificationResult(
                verified=False,
                ran=False,
                detail="candidate target-workflow verification requires a patch",
            )

        encoded = base64.b64encode(patch.encode("utf-8")).decode("ascii")
        if len(encoded) > _MAX_PATCH_BYTES:
            return VerificationResult(
                verified=False,
                ran=False,
                detail=(
                    "patch is too large for target-workflow dispatch "
                    f"({len(encoded)} > {_MAX_PATCH_BYTES} bytes)"
                ),
            )

        token = uuid.uuid4().hex
        dispatched_at = time.time()
        if not self._dispatch(plan, encoded, token):
            return VerificationResult(
                verified=False,
                ran=False,
                detail="could not dispatch the target-owned verification workflow",
            )

        timeout = self._transport.effective_timeout(plan.timeout_seconds)
        run = self._transport.wait_for_run(
            token,
            since=dispatched_at,
            timeout=timeout,
        )
        if run is None:
            return VerificationResult(
                verified=False,
                ran=False,
                detail=(
                    "target-owned verification did not complete within "
                    f"{timeout}s"
                ),
            )

        sample = f"({plan.repetition}/{plan.repetition_count})"
        return completed_workflow_result(
            self._transport,
            run,
            success_detail=(
                f"target-owned {plan.phase.value} verification passed {sample}"
            ),
            failure_detail=(
                f"target-owned {plan.phase.value} verification failed "
                f"{sample}"
            ),
            unavailable_detail=(
                "target-owned verification completed without a test verdict"
            ),
        )

    def _dispatch(
        self,
        plan: VerificationPlan,
        encoded_patch: str,
        token: str,
    ) -> bool:
        inputs = {
            "head_sha": plan.head_sha,
            "patch_b64": encoded_patch,
            "failing_run_id": str(plan.source_run_id),
            "failing_job": plan.job_name,
            "phase": plan.phase.value,
            "repetition": str(plan.repetition),
            "repetition_count": str(plan.repetition_count),
            "correlation": token,
        }

        return self._transport.dispatch(inputs)
