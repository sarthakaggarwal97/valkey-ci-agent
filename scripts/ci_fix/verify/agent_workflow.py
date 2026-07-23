"""Agent-owned, credential-free GitHub Actions verification backend.

Linux and macOS fallback verification share one transport and input contract.
Each sample runs in a separate agent-repository workflow with no secrets and
no repository write permission. The controller supplies only the gated target
identity, candidate patch, code-selected environment, and targeted command.
"""

from __future__ import annotations

import base64
import re
import uuid
from collections.abc import Callable
from pathlib import PurePosixPath
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
from scripts.common.git_clone import REPO_RE
from scripts.common.workflow_artifacts import ArtifactClient

_MAX_PATCH_BYTES = (MAX_REVIEWABLE_PATCH_CHARS * 4) // 3 + 1024
_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_VERDICT_STEP = "Run targeted verification"


class AgentWorkflowVerifier:
    """Dispatch and await one sample on an agent-owned Actions runner."""

    def __init__(
        self,
        github_client: Any,
        *,
        agent_repo_full_name: str,
        workflow: str,
        ref: str,
        runner_label: str,
        accepted_envs: frozenset[VerifyEnv],
        timeout: int,
        artifact_client: ArtifactClient | None,
        command_for_plan: Callable[[VerificationPlan], str],
        clock: Callable[[], float],
        sleep: Callable[[float], None],
    ) -> None:
        self._runner_label = runner_label
        self._accepted_envs = accepted_envs
        self._command_for_plan = command_for_plan
        self._clock = clock
        self._transport = WorkflowDispatchTransport(
            github_client,
            repo_full_name=agent_repo_full_name,
            workflow=workflow,
            ref=ref,
            timeout=timeout,
            artifact_client=artifact_client,
            log_markers=(_VERDICT_STEP,),
            clock=clock,
            sleep=sleep,
        )

    def verify(
        self,
        repo_dir: str,
        plan: VerificationPlan,
        patch: str,
    ) -> VerificationResult:
        """Verify one clean baseline or candidate patch in the agent workflow."""
        del repo_dir
        invalid = self._validate(plan, patch)
        if invalid:
            return VerificationResult(verified=False, ran=False, detail=invalid)

        encoded = base64.b64encode(patch.encode("utf-8")).decode("ascii")
        if len(encoded) > _MAX_PATCH_BYTES:
            return VerificationResult(
                verified=False,
                ran=False,
                detail=(
                    f"patch is too large for {self._runner_label} dispatch "
                    f"verification ({len(encoded)} > {_MAX_PATCH_BYTES} bytes)"
                ),
            )

        token = uuid.uuid4().hex
        dispatched_at = self._clock()
        inputs = {
            "target_repo": plan.target_repo,
            "head_sha": plan.head_sha,
            "patch_b64": encoded,
            "verify_command": self._command_for_plan(plan),
            "workdir": plan.workdir,
            "container_image": plan.image,
            "phase": plan.phase.value,
            "repetition": str(plan.repetition),
            "repetition_count": str(plan.repetition_count),
            "correlation": token,
        }
        if not self._transport.dispatch(inputs):
            return VerificationResult(
                verified=False,
                ran=False,
                detail=f"could not dispatch the {self._runner_label} verification job",
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
                    f"{self._runner_label} verification did not complete "
                    f"within {timeout}s"
                ),
            )

        sample = f"({plan.repetition}/{plan.repetition_count})"
        return completed_workflow_result(
            self._transport,
            run,
            success_detail=(
                f"targeted {self._runner_label} {plan.phase.value} "
                f"verification passed {sample}"
            ),
            failure_detail=(
                f"targeted {self._runner_label} {plan.phase.value} "
                f"verification failed {sample}"
            ),
            unavailable_detail=(
                f"{self._runner_label} verification completed without a test verdict"
            ),
            verdict_step=_VERDICT_STEP,
        )

    def _validate(self, plan: VerificationPlan, patch: str) -> str:
        if plan.env not in self._accepted_envs:
            return (
                f"{self._runner_label} verifier cannot run environment "
                f"{plan.env.value!r}"
            )
        if not _valid_repo(plan.target_repo) or not _SHA_RE.fullmatch(plan.head_sha):
            return f"{self._runner_label} verification plan is missing target identity"
        if not plan.command.strip():
            return f"{self._runner_label} verification plan has no command"
        if not _valid_workdir(plan.workdir):
            return f"{self._runner_label} verification plan has an invalid workdir"
        if not 1 <= plan.repetition <= plan.repetition_count <= 100:
            return f"{self._runner_label} verification plan has an invalid repetition range"
        if plan.phase is VerificationPhase.BASELINE and patch:
            return f"{self._runner_label} baseline verification cannot include a patch"
        if plan.phase is VerificationPhase.CANDIDATE and not patch:
            return f"{self._runner_label} candidate verification requires a patch"
        if plan.env is VerifyEnv.DOCKER and not plan.image:
            return "Docker verification requires a container image"
        if plan.env is not VerifyEnv.DOCKER and plan.image:
            return (
                f"{self._runner_label} non-Docker verification cannot include "
                "a container image"
            )
        return ""


def _valid_workdir(workdir: str) -> bool:
    if "\0" in workdir or "\n" in workdir or "\r" in workdir or "\\" in workdir:
        return False
    path = PurePosixPath(workdir)
    return not path.is_absolute() and ".." not in path.parts


def _valid_repo(repo: str) -> bool:
    if not REPO_RE.fullmatch(repo):
        return False
    return all(part not in {".", ".."} for part in repo.split("/", 1))
