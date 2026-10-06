"""Deterministic environment selection for a CI workflow job.

The AI hints which job a failure belongs to; code owns the security-relevant
decision of which environment that job runs in, and therefore which controlled
verifier may run the command. This module parses a GitHub Actions workflow
narrowly, reading only ``runs-on`` and ``container.image`` to classify the job
as local, docker(image), macos, or unsupported. It does not extract or replay
the job's steps. Anything it does not clearly understand (dynamic runners or
images, self-hosted runners, non-Linux/non-macOS platforms) is unsupported, and
the caller refuses.

GitHub names a job in a run by its rendered ``name:`` (``${{ matrix.name }}``
becomes ``test-almalinux8-jemalloc``) or, without one, by its key plus the
matrix values (``test-sanitizer-address (gcc)``). ``resolve_job`` maps such a
display name back to the job key and the matrix values that produced it, so a
matrix-named job is classified with its real container image.
"""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

from scripts.ci_fix.verify.base import VerifyEnv


@dataclass(frozen=True)
class JobEnvironment:
    """The verifier environment for one workflow job.

    ``image`` is set only for DOCKER. ``reason`` is set only for UNSUPPORTED.
    """

    env: VerifyEnv
    image: str = ""
    reason: str = ""


@dataclass(frozen=True)
class ResolvedJob:
    """A workflow job matched to a run's display name."""

    job_id: str
    job: dict[str, Any]
    matrix: dict[str, str] = field(default_factory=dict)


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

# GitHub-hosted x86-64 Linux runner labels we can reproduce locally. An arm
# label (e.g. ubuntu-24.04-arm) is deliberately excluded: verifying an
# arm-specific failure on x86 would be wrong, so it stays unsupported.
_X86_LINUX_RUNNERS = frozenset({
    "ubuntu-latest",
    "ubuntu-24.04",
    "ubuntu-22.04",
    "ubuntu-20.04",
})

_MATRIX_EXPR_RE = re.compile(r"\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}")
# A matrix larger than this is not expanded: no real CI job needs it, and it
# bounds the work a crafted workflow file can cause.
_MAX_MATRIX_COMBOS = 256


def load_workflow(workflow_yaml: str) -> dict[str, Any] | None:
    """Parse workflow YAML, returning None for anything that is not a mapping."""
    try:
        doc = yaml.safe_load(workflow_yaml)
    except yaml.YAMLError:
        return None
    return doc if isinstance(doc, dict) else None


def resolve_job(doc: dict[str, Any], display_name: str) -> ResolvedJob | None:
    """Return the job (and matrix values) a run's ``display_name`` refers to.

    Matches, in order: the job key itself, GitHub's default matrix rendering
    ``<key> (<values>)``, and an explicit ``name:`` rendered with each matrix
    combination. Returns None when nothing matches or the match is ambiguous.
    """
    jobs = doc.get("jobs")
    if not isinstance(jobs, dict) or not display_name:
        return None
    matches: list[ResolvedJob] = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        combos = _matrix_combos(job)
        name = job.get("name")
        if isinstance(name, str) and name.strip():
            rendered = [(_render(name, combo), combo) for combo in combos]
            hit = next((combo for text, combo in rendered if text == display_name), None)
            if hit is not None:
                matches.append(ResolvedJob(str(job_id), job, hit))
                continue
            if all(text for text, _combo in rendered):
                continue  # a fully rendered name that is simply a different job
            # The name uses a matrix computed at run time (``fromJSON(...)``),
            # so it cannot be rendered here; fall back to the job key below.
        if display_name == job_id:
            matches.append(ResolvedJob(str(job_id), job, {}))
        elif display_name.startswith(f"{job_id} (") and display_name.endswith(")"):
            matches.append(ResolvedJob(str(job_id), job, _combo_for_suffix(combos, display_name)))
    return matches[0] if len(matches) == 1 else None


def classify_job_environment(workflow_yaml: str, display_name: str) -> JobEnvironment:
    """Classify the runner environment of the job a run calls ``display_name``.

    Returns a ``JobEnvironment``; ``env`` is UNSUPPORTED (with a reason)
    whenever the job's environment cannot be determined safely. Never raises for
    malformed input - a parse failure is reported as UNSUPPORTED.
    """
    doc = load_workflow(workflow_yaml)
    if doc is None:
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason="workflow YAML did not parse")
    resolved = resolve_job(doc, display_name)
    if resolved is None:
        return JobEnvironment(
            VerifyEnv.UNSUPPORTED, reason=f"job {display_name!r} not found in workflow",
        )
    runs_on = _substitute(resolved.job.get("runs-on"), resolved.matrix)
    env = _classify_env(runs_on, resolved.job.get("container") is not None)
    if env is VerifyEnv.UNSUPPORTED:
        return JobEnvironment(VerifyEnv.UNSUPPORTED, reason=f"unsupported runner: {runs_on!r}")
    if env is VerifyEnv.DOCKER:
        image = _container_image(resolved.job.get("container"), resolved.matrix)
        if not image:
            return JobEnvironment(
                VerifyEnv.UNSUPPORTED,
                reason="container image is dynamic or malformed; cannot run it safely",
            )
        return JobEnvironment(VerifyEnv.DOCKER, image=image)
    return JobEnvironment(env)


def _matrix_combos(job: dict[str, Any]) -> list[dict[str, str]]:
    """Every matrix combination of ``job`` as string values ({} when no matrix)."""
    strategy = job.get("strategy")
    matrix = strategy.get("matrix") if isinstance(strategy, dict) else None
    if not isinstance(matrix, dict):
        return [{}]
    axes = {
        str(key): [str(item) for item in values if isinstance(item, (str, int, float))]
        for key, values in matrix.items()
        if key not in {"include", "exclude"} and isinstance(values, list)
    }
    size = 1
    for values in axes.values():
        size *= max(1, len(values))
    if size > _MAX_MATRIX_COMBOS:
        return [{}]
    combos = [dict(zip(axes, values)) for values in itertools.product(*axes.values())] if axes else []
    include = matrix.get("include")
    if isinstance(include, list):
        for entry in include[:_MAX_MATRIX_COMBOS]:
            if isinstance(entry, dict):
                combos.append({str(k): str(v) for k, v in entry.items() if isinstance(v, (str, int, float))})
    return combos or [{}]


def _combo_for_suffix(combos: list[dict[str, str]], display_name: str) -> dict[str, str]:
    """Find the matrix values GitHub joined into ``<key> (<v1>, <v2>)``."""
    suffix = display_name[display_name.index(" (") + 2:-1]
    for combo in combos:
        if combo and ", ".join(combo.values()) == suffix:
            return combo
    return {}


def _render(template: str, combo: dict[str, str]) -> str:
    """Render ``${{ matrix.x }}`` references; "" if any expression remains."""
    rendered = _MATRIX_EXPR_RE.sub(lambda m: combo.get(m.group(1), m.group(0)), template)
    return "" if "${{" in rendered else rendered.strip()


def _substitute(value: Any, combo: dict[str, str]) -> Any:
    """Resolve matrix references in a string value, leaving it as-is otherwise.

    An unresolvable string keeps its ``${{`` so the caller rejects it.
    """
    if not isinstance(value, str):
        return value
    return _render(value, combo) or value


def _classify_env(runs_on: Any, has_container: bool) -> VerifyEnv:
    if not isinstance(runs_on, str):
        # A list runner (self-hosted labels) or an unexpanded mapping.
        return VerifyEnv.UNSUPPORTED
    label = runs_on.strip().lower()
    if "${{" in label:
        return VerifyEnv.UNSUPPORTED  # an expression we could not resolve
    if label.startswith("macos"):
        return VerifyEnv.MACOS
    # Only x86-64 Linux is locally verifiable on this runner. An arm Linux
    # runner (e.g. ubuntu-24.04-arm) must NOT be verified on x86.
    if label in _X86_LINUX_RUNNERS:
        return VerifyEnv.DOCKER if has_container else VerifyEnv.LOCAL
    # windows, self-hosted, arm Linux, or anything else.
    return VerifyEnv.UNSUPPORTED


def _container_image(container: Any, combo: dict[str, str]) -> str:
    if isinstance(container, str):
        image = container.strip()
    elif isinstance(container, dict) and isinstance(container.get("image"), str):
        image = container["image"].strip()
    else:
        return ""
    image = _substitute(image, combo)
    if not isinstance(image, str) or "${{" in image or not _IMAGE_RE.fullmatch(image):
        return ""
    return image
