"""Verification backends: the boundary where a candidate fix is proven.

The AI proposes a fix and a targeted command (a ``FixProposal``). Code owns
everything about *where and whether* that command is trusted: it determines the
failed job from the linked run, classifies that job's environment, and selects
a backend. A backend takes a ``VerificationPlan`` and returns a
``VerificationResult`` whose verdict comes only from a real exit code (or a real
CI run conclusion), never from the model.

Backends:
- ``local`` dispatches host-Linux verification to an agent-owned runner.
- ``docker`` dispatches a sandboxed static container on that Linux runner.
- ``macos`` dispatches it to a macOS runner the agent controls and waits.
- ``target`` dispatches a target-repository-owned exact-environment workflow.

Each is a thin implementation of the same ``VerifyBackend`` protocol, so the
pipeline selects one and calls ``verify`` without knowing the mechanics.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class VerifyEnv(str, Enum):
    """The environment a failed job runs in, as classification supports it."""

    LOCAL = "local"          # plain Linux job: agent-owned ubuntu runner
    DOCKER = "docker"        # Linux container job: sandboxed on that runner
    MACOS = "macos"          # macOS runner: verify via the macOS backend
    TARGET = "target-workflow"  # target-owned workflow replays the failed environment
    UNSUPPORTED = "unsupported"  # cannot be classified/verified safely


class VerificationPhase(str, Enum):
    """Which candidate state a remote workflow must verify."""

    BASELINE = "baseline"
    CANDIDATE = "candidate"


@dataclass(frozen=True)
class FailedJob:
    """A job that did not succeed in the linked CI run (owned by code)."""

    name: str
    conclusion: str


@dataclass(frozen=True)
class VerificationPlan:
    """How and where code will verify a candidate fix.

    Produced by code from the actual failed job and its workflow definition,
    not from the AI. ``command`` is the AI's targeted verify command, but the
    environment, image, and backend are code's decision.
    """

    env: VerifyEnv
    command: str
    workdir: str = ""
    image: str = ""          # set only for DOCKER
    job_name: str = ""       # the failed job this plan verifies
    head_sha: str = ""       # the PR head SHA a remote backend verifies
    target_repo: str = ""    # repository whose protected verifier is dispatched
    source_run_id: int = 0    # linked failing run, for target-owned recipe selection
    phase: VerificationPhase = VerificationPhase.CANDIDATE
    repetition: int = 1
    repetition_count: int = 1
    timeout_seconds: int = 0  # per-sample cap supplied by remote orchestration
    unsupported_reason: str = ""


@dataclass(frozen=True)
class VerificationResult:
    """The factual outcome of running a plan. ``verified`` is never AI-decided.

    ``output_tail`` carries bounded failure feedback; ``run_url`` identifies
    the remote CI run. ``ran`` is False only when verification could not be
    attempted or did not produce a test verdict, which the pipeline treats as
    unavailable evidence rather than a pass.
    """

    verified: bool
    ran: bool
    detail: str
    command: str = ""
    output_tail: str = ""
    run_url: str = ""


class VerifyBackend(Protocol):
    """Verify one clean baseline or exact candidate patch selected by code.

    Remote backends check out ``plan.head_sha`` and independently apply
    ``patch`` for candidate samples. They do not trust controller worktree
    state.
    """

    def verify(self, repo_dir: str, plan: VerificationPlan, patch: str) -> VerificationResult:
        ...


def backend_label(env: VerifyEnv, image: str = "") -> str:
    """A short human label for the verifier used, for the PR comment.

    Derived from the env (and image) so the comment string and the routing
    enum cannot drift apart.
    """
    if env is VerifyEnv.DOCKER:
        return f"docker:{image}" if image else "docker"
    return env.value
