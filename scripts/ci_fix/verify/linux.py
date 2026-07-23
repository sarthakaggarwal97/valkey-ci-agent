"""Linux verification on an agent-owned, credential-free Actions runner."""

from __future__ import annotations

import time
from typing import Any

from scripts.ci_fix.verify.agent_workflow import AgentWorkflowVerifier
from scripts.ci_fix.verify.base import VerificationPlan, VerifyEnv
from scripts.common.workflow_artifacts import ArtifactClient

VERIFY_LINUX_WORKFLOW = "ci-fix-verify-linux.yml"

_DEFAULT_TIMEOUT_S = 60 * 60


def _command_for_plan(plan: VerificationPlan) -> str:
    return plan.command


class LinuxVerifier(AgentWorkflowVerifier):
    """Run one host-Linux or static-container sample in GitHub Actions."""

    def __init__(
        self,
        github_client: Any,
        *,
        agent_repo_full_name: str,
        ref: str = "main",
        timeout: int = _DEFAULT_TIMEOUT_S,
        artifact_client: ArtifactClient | None = None,
    ) -> None:
        super().__init__(
            github_client,
            agent_repo_full_name=agent_repo_full_name,
            workflow=VERIFY_LINUX_WORKFLOW,
            ref=ref,
            runner_label="Linux",
            accepted_envs=frozenset({VerifyEnv.LOCAL, VerifyEnv.DOCKER}),
            timeout=timeout,
            artifact_client=artifact_client,
            command_for_plan=_command_for_plan,
            clock=time.time,
            sleep=time.sleep,
        )
