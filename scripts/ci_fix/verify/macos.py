"""macOS verification: dispatch the agent's verify-macos job and wait.

macOS cannot be built on the Linux runner, so clean baselines and candidate
patches can be sampled on a macOS runner the agent controls. A candidate patch
is transported as an input (not a local commit, whose SHA a separate workflow
cannot fetch); a baseline sends an empty patch. The job checks out the PR head
SHA, conditionally applies the patch, runs the command, and its conclusion is
the verdict. This backend supplies macOS-specific inputs over the shared
correlated GitHub Actions transport; orchestration lives in the pipeline.
"""

from __future__ import annotations

import logging
import re
import shlex
import time
from typing import Any

from scripts.ci_fix.verify.agent_workflow import AgentWorkflowVerifier
from scripts.ci_fix.verify.base import VerificationPlan, VerifyEnv
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

VERIFY_MACOS_WORKFLOW = "ci-fix-verify-macos.yml"

_DEFAULT_TIMEOUT_S = 60 * 60
_MAKE_COMMAND_RE = re.compile(
    r"(?P<prefix>(?:^|(?<=[;&|{}])\s*))"
    r"(?P<command>make(?:\s+(?:\"[^\"]*\"|'[^']*'|[^\s;&|{}]+))*)"
)
_ROOT_SRC_OBJECT_RE = re.compile(r"^src/(?P<target>.+\.o)$")


def normalize_macos_verify_command(command: str) -> str:
    """Rewrite unsafe root-level Valkey object builds for macOS verification.

    A command such as ``make src/unit/test_networking.o`` from the repo root
    does not use Valkey's ``src/Makefile``; GNU make falls back to an implicit
    compile rule and misses the project's include paths and generated
    prerequisites. Run that targeted object build through ``make -C src``
    instead, while leaving other commands unchanged.
    """
    return _MAKE_COMMAND_RE.sub(_rewrite_make_command, command)


def _rewrite_make_command(match: re.Match[str]) -> str:
    prefix = match.group("prefix")
    make_command = match.group("command")
    try:
        tokens = shlex.split(make_command)
    except ValueError:
        return match.group(0)
    if len(tokens) < 2 or tokens[0] != "make" or _has_make_directory(tokens):
        return match.group(0)

    rewritten = False
    args: list[str] = []
    for token in tokens[1:]:
        target = _ROOT_SRC_OBJECT_RE.match(token)
        if target:
            args.append(target.group("target"))
            rewritten = True
        else:
            args.append(token)
    if not rewritten:
        return match.group(0)
    return f"{prefix}{shlex.join(['make', '-C', 'src', *args])}"


def _has_make_directory(tokens: list[str]) -> bool:
    for index, token in enumerate(tokens[1:], start=1):
        if token == "-C" and index + 1 < len(tokens):
            return True
        if token.startswith("-C") and token != "-C":
            return True
        if token == "--directory" and index + 1 < len(tokens):
            return True
        if token.startswith("--directory="):
            return True
    return False


def _command_for_plan(plan: VerificationPlan) -> str:
    command = plan.command
    if not plan.workdir.strip():
        command = normalize_macos_verify_command(command)
    if command != plan.command:
        logger.info("Normalized macOS verification command: %s", command)
    return command


class MacosVerifier(AgentWorkflowVerifier):
    """Run one sample on the agent-owned macOS Actions workflow."""

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
            workflow=VERIFY_MACOS_WORKFLOW,
            ref=ref,
            runner_label="macOS",
            accepted_envs=frozenset({VerifyEnv.MACOS}),
            timeout=timeout,
            artifact_client=artifact_client,
            command_for_plan=_command_for_plan,
            clock=time.time,
            sleep=time.sleep,
        )
