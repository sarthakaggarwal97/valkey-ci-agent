"""Deterministic environment selection for a CI workflow job.

The AI hints which job a failure belongs to; code owns the security-relevant
decision of which environment that job runs in, and therefore which controlled
verifier may run the command. This module parses a GitHub Actions workflow
conservatively. It classifies simple ``runs-on`` and ``container.image`` jobs,
and scans structural setup signals without extracting or replaying steps. A
host Linux or macOS recipe may reproduce an unconditional auxiliary checkout
when its repository, commit, and destination are all static. Sandboxed
containers cannot because they deliberately have no network. Anything else
that needs exact setup (matrix jobs, services, architecture/OS emulation,
setup actions), or that this parser does not clearly understand (dynamic
images, self-hosted runners, non-Linux/non-macOS platforms), must use a
target-owned verifier.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

import yaml

from scripts.ci_fix.verify.base import VerifyEnv
from scripts.common.git_clone import REPO_RE


@dataclass(frozen=True)
class JobEnvironment:
    """The verifier environment for one workflow job.

    ``matched`` distinguishes "this workflow defines the requested job but its
    environment is unsupported" from "the job is not in this workflow".  That
    distinction lets cross-workflow classification fail closed when one matching
    definition needs exact setup. ``image`` is set only for DOCKER. ``reason``
    is set only for UNSUPPORTED.
    """

    env: VerifyEnv
    image: str = ""
    reason: str = ""
    matched: bool = True


# A container image we are willing to run: a normal image reference (optionally
# with a registry host and port, and a tag or @sha256 digest), no expression
# interpolation (``${{ ... }}``) and no shell-surprising characters.
_IMAGE_RE = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*"            # registry/repository path
    r"(:[0-9]+)?"                         # optional registry port
    r"([a-z0-9._/-]*)"                    # optional path after port
    r"(:[a-zA-Z0-9._-]+)?"                # optional tag
    r"(@sha256:[a-f0-9]{64})?$"           # optional digest
)

# The agent-owned workflows launch these exact labels. A versioned or arm label
# is not equivalent merely because it shares an OS family.
_X86_LINUX_RUNNERS = frozenset({
    "ubuntu-latest",
})

_PLUMBING_ACTION_PREFIXES = (
    "actions/upload-artifact@",
)
_PLUMBING_LOCAL_ACTIONS = frozenset({
    "./.github/actions/upload-test-failures",
})
_PINNED_CHECKOUT_RE = re.compile(r"^actions/checkout@[0-9a-f]{40}$", re.IGNORECASE)
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
_CHECKOUT_PATH_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
_PRIMARY_CHECKOUT_KEYS = frozenset({"repository", "ref", "persist-credentials"})
_AUXILIARY_CHECKOUT_KEYS = frozenset({
    "repository",
    "ref",
    "path",
    "persist-credentials",
})
_CONDITIONAL_ACTION_KEYS = frozenset({
    "if",
    "env",
    "continue-on-error",
    "timeout-minutes",
})
_SPECIAL_PLATFORM_RUN_RE = re.compile(
    r"\b(?:qemu(?:-system)?|s390x|freebsd|setarch|binfmt)\b"
    r"|docker\s+run\b.*?--platform",
    re.IGNORECASE | re.DOTALL,
)


def classify_job_environment(workflow_yaml: str, job_id: str) -> JobEnvironment:
    """Classify the runner environment of ``job_id`` in ``workflow_yaml``.

    Returns a ``JobEnvironment``; ``env`` is UNSUPPORTED (with a reason)
    whenever the job's environment cannot be determined safely. Never raises for
    malformed input - a parse failure is reported as UNSUPPORTED.
    """
    try:
        doc = yaml.safe_load(workflow_yaml)
    except yaml.YAMLError as exc:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=f"workflow YAML did not parse: {exc}",
            matched=False,
        )
    if not isinstance(doc, dict):
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason="workflow root is not a mapping",
            matched=False,
        )

    jobs = doc.get("jobs")
    if not isinstance(jobs, dict) or job_id not in jobs:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED,
            reason=f"job {job_id!r} not found in workflow",
            matched=False,
        )
    job = jobs[job_id]
    if not isinstance(job, dict):
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason=f"job {job_id!r} is not a mapping")

    env = _classify_env(job)
    exact_reason = _target_workflow_reason(job, env)
    if exact_reason:
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason=exact_reason)

    if env is VerifyEnv.UNSUPPORTED:
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason=f"unsupported runner: {job.get('runs-on')!r}")
    if env is VerifyEnv.DOCKER:
        image = _container_image(job)
        if not image:
            return JobEnvironment(
                VerifyEnv.UNSUPPORTED,
                reason="container image is dynamic or malformed; cannot run it safely",
            )
        return JobEnvironment(VerifyEnv.DOCKER, image=image)
    return JobEnvironment(env)


def _target_workflow_reason(job: dict[str, Any], env: VerifyEnv) -> str:
    """Return why replaying only a command would lose environment fidelity."""
    if "uses" in job:
        return "reusable-workflow jobs require the target-owned verifier"
    strategy = job.get("strategy")
    if isinstance(strategy, dict) and "matrix" in strategy:
        return "matrix jobs require resolved target-owned environment setup"
    if job.get("services"):
        return "job services require the target-owned verifier"
    if job.get("defaults") or job.get("env"):
        return "job defaults or environment require the target-owned verifier"

    container = job.get("container")
    if isinstance(container, dict) and any(
        key in container for key in ("credentials", "env", "options", "ports", "volumes")
    ):
        return "container options require the target-owned verifier"

    steps = job.get("steps")
    if not isinstance(steps, list):
        return ""
    primary_checkout_seen = False
    for step in steps:
        if not isinstance(step, dict):
            continue
        action = step.get("uses")
        if isinstance(action, str):
            normalized = action.strip().lower()
            if normalized.startswith("actions/checkout@"):
                checkout_with = step.get("with")
                if checkout_with is None:
                    checkout_with = {}
                if not isinstance(checkout_with, dict):
                    return "malformed checkout setup requires the target-owned verifier"

                if _is_primary_checkout(checkout_with):
                    if primary_checkout_seen:
                        return "multiple target checkouts require the target-owned verifier"
                    primary_checkout_seen = True
                    if not set(checkout_with).issubset(_PRIMARY_CHECKOUT_KEYS):
                        return "target checkout options require the target-owned verifier"
                elif _is_static_auxiliary_checkout(action, step, checkout_with):
                    if env is VerifyEnv.DOCKER:
                        return (
                            "additional checkout cannot run in the network-disabled "
                            "container verifier; use the target-owned verifier"
                        )
                    if env not in {VerifyEnv.LOCAL, VerifyEnv.MACOS}:
                        continue
                else:
                    return (
                        "additional checkout must be unconditional and use a static "
                        "repository, full commit SHA, and relative path; use the "
                        "target-owned verifier"
                    )
            elif not _is_plumbing_action(normalized):
                return f"setup action {action!r} requires the target-owned verifier"
        run = step.get("run")
        if isinstance(run, str) and _SPECIAL_PLATFORM_RUN_RE.search(run):
            return "architecture or operating-system emulation requires the target-owned verifier"
    return ""


def _is_plumbing_action(action: str) -> bool:
    return (
        action in _PLUMBING_LOCAL_ACTIONS
        or action.startswith(_PLUMBING_ACTION_PREFIXES)
    )


def _is_primary_checkout(options: dict[str, Any]) -> bool:
    """Recognize the target checkout that the agent workflow replaces."""
    if "path" in options:
        return False
    repository = options.get("repository")
    return repository is None or (
        isinstance(repository, str) and "${{" in repository
    )


def _is_static_auxiliary_checkout(
    action: str,
    step: dict[str, Any],
    options: dict[str, Any],
) -> bool:
    """Return whether a credential-free host recipe can recreate this checkout."""
    if not _PINNED_CHECKOUT_RE.fullmatch(action.strip()):
        return False
    if any(key in step for key in _CONDITIONAL_ACTION_KEYS):
        return False
    if not set(options).issubset(_AUXILIARY_CHECKOUT_KEYS):
        return False

    repository = options.get("repository")
    ref = options.get("ref")
    path = options.get("path")
    return (
        isinstance(repository, str)
        and REPO_RE.fullmatch(repository) is not None
        and all(part not in {".", ".."} for part in repository.split("/", 1))
        and isinstance(ref, str)
        and _FULL_SHA_RE.fullmatch(ref) is not None
        and isinstance(path, str)
        and _valid_relative_checkout_path(path)
    )


def _valid_relative_checkout_path(value: str) -> bool:
    if (
        not value
        or "\0" in value
        or "\n" in value
        or "\r" in value
        or "\\" in value
        or _CHECKOUT_PATH_RE.fullmatch(value) is None
    ):
        return False
    path = PurePosixPath(value)
    return (
        not path.is_absolute()
        and path.as_posix() == value
        and bool(path.parts)
        and all(part not in {".", ".."} for part in path.parts)
    )


def _classify_env(job: dict[str, Any]) -> VerifyEnv:
    runs_on = job.get("runs-on")
    if not isinstance(runs_on, str):
        # A list runner (self-hosted matrix) or a matrix expression.
        return VerifyEnv.UNSUPPORTED
    label = runs_on.strip().lower()
    if "${{" in label:
        return VerifyEnv.UNSUPPORTED  # matrix-indirected runner
    if label == "macos-latest":
        return VerifyEnv.MACOS
    # Only the exact x86-64 Linux label launched by the generic verifier is
    # supported. Versioned and arm labels require a matching target verifier.
    if label in _X86_LINUX_RUNNERS:
        return VerifyEnv.DOCKER if job.get("container") is not None else VerifyEnv.LOCAL
    # windows, self-hosted, arm Linux, or anything else.
    return VerifyEnv.UNSUPPORTED


def _container_image(job: dict[str, Any]) -> str:
    container = job.get("container")
    if isinstance(container, str):
        image = container.strip()
    elif isinstance(container, dict) and isinstance(container.get("image"), str):
        image = container["image"].strip()
    else:
        return ""
    if "${{" in image or not _IMAGE_RE.fullmatch(image):
        return ""
    return image
