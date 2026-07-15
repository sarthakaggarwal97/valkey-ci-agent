"""Resolve a Jobs API display name against one exact workflow revision."""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass
from typing import Any

import yaml

from scripts.ci_fix.verify.workflow_env import JobEnvironment, classify_job_environment

_EXPRESSION_RE = re.compile(r"\$\{\{\s*matrix\.([A-Za-z_][A-Za-z0-9_-]*)\s*\}\}")
_MAX_MATRIX_COMBINATIONS = 256


@dataclass(frozen=True)
class ResolvedWorkflowJob:
    job_id: str
    display_name: str
    matrix: tuple[tuple[str, str], ...]
    environment: JobEnvironment
    fidelity: dict[str, Any]
    reason: str = ""


def resolve_workflow_job(workflow_yaml: str, display_name: str) -> ResolvedWorkflowJob:
    """Resolve one GitHub Jobs API name to an exact YAML job and environment."""
    try:
        document = yaml.safe_load(workflow_yaml)
    except yaml.YAMLError as exc:
        return _unsupported(display_name, f"workflow YAML did not parse: {exc}")
    if not isinstance(document, dict) or not isinstance(document.get("jobs"), dict):
        return _unsupported(display_name, "workflow has no jobs mapping")

    matches: list[tuple[str, dict[str, Any], dict[str, str]]] = []
    for raw_job_id, raw_job in document["jobs"].items():
        if not isinstance(raw_job_id, str) or not isinstance(raw_job, dict):
            continue
        for matrix in _matrix_combinations(raw_job):
            rendered = _render_job_name(raw_job_id, raw_job.get("name"), matrix)
            if rendered == display_name:
                matches.append((raw_job_id, raw_job, matrix))
    if not matches:
        return _unsupported(
            display_name,
            f"display name {display_name!r} does not resolve in the exact workflow",
        )
    unique = {
        (job_id, tuple(sorted(matrix.items())))
        for job_id, _job, matrix in matches
    }
    if len(unique) != 1:
        return _unsupported(
            display_name,
            f"display name {display_name!r} resolves to multiple workflow jobs",
        )
    job_id, job, matrix = matches[0]
    materialized = _materialize_job(job, matrix)
    materialized_document = {"jobs": {job_id: materialized}}
    environment = classify_job_environment(
        yaml.safe_dump(materialized_document, sort_keys=False),
        job_id,
    )
    reason = environment.reason
    return ResolvedWorkflowJob(
        job_id=job_id,
        display_name=display_name,
        matrix=tuple(sorted(matrix.items())),
        environment=environment,
        fidelity=_fidelity_contract(),
        reason=reason,
    )


def _matrix_combinations(job: dict[str, Any]) -> list[dict[str, str]]:
    strategy = job.get("strategy")
    if not isinstance(strategy, dict) or "matrix" not in strategy:
        return [{}]
    matrix = strategy.get("matrix")
    if not isinstance(matrix, dict):
        return []

    axes: list[tuple[str, list[str]]] = []
    includes: list[dict[str, str]] = []
    excludes: list[dict[str, str]] = []
    for key, raw_values in matrix.items():
        if key == "include":
            includes = _matrix_rows(raw_values)
            continue
        if key == "exclude":
            excludes = _matrix_rows(raw_values)
            continue
        if not isinstance(key, str) or not isinstance(raw_values, list) or not raw_values:
            return []
        values: list[str] = []
        for value in raw_values:
            if not isinstance(value, (str, int, float, bool)):
                return []
            values.append(_matrix_scalar(value))
        axes.append((key, values))

    count = 1
    for _key, values in axes:
        count *= len(values)
        if count > _MAX_MATRIX_COMBINATIONS:
            return []
    combinations = [
        dict(zip((key for key, _values in axes), values))
        for values in itertools.product(*(values for _key, values in axes))
    ]
    combinations = [
        item
        for item in combinations
        if not any(_row_matches(item, excluded) for excluded in excludes)
    ]
    for included in includes:
        matched = False
        for item in combinations:
            axis_values = {key: included[key] for key, _ in axes if key in included}
            if _row_matches(item, axis_values):
                item.update(included)
                matched = True
        if not matched:
            combinations.append(dict(included))
    if len(combinations) > _MAX_MATRIX_COMBINATIONS:
        return []
    return combinations


def _matrix_rows(value: Any) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > _MAX_MATRIX_COMBINATIONS:
        return []
    rows: list[dict[str, str]] = []
    for raw_row in value:
        if not isinstance(raw_row, dict) or not all(isinstance(key, str) for key in raw_row):
            return []
        row: dict[str, str] = {}
        for key, raw_value in raw_row.items():
            if not isinstance(raw_value, (str, int, float, bool)):
                return []
            row[key] = _matrix_scalar(raw_value)
        rows.append(row)
    return rows


def _matrix_scalar(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _row_matches(candidate: dict[str, str], expected: dict[str, str]) -> bool:
    return all(candidate.get(key) == value for key, value in expected.items())


def _render_job_name(job_id: str, raw_name: Any, matrix: dict[str, str]) -> str:
    if isinstance(raw_name, str):
        rendered, complete = _substitute_matrix(raw_name, matrix)
        return rendered if complete else ""
    if not matrix:
        return job_id
    # GitHub's default matrix display name appends values in matrix declaration
    # order. Include-only keys retain insertion order as delivered by YAML.
    return f"{job_id} ({', '.join(matrix.values())})"


def _materialize_job(job: dict[str, Any], matrix: dict[str, str]) -> dict[str, Any]:
    materialized = dict(job)
    runs_on = job.get("runs-on")
    if isinstance(runs_on, str):
        rendered, complete = _substitute_matrix(runs_on, matrix)
        materialized["runs-on"] = rendered if complete else runs_on
    container = job.get("container")
    if isinstance(container, str):
        rendered, complete = _substitute_matrix(container, matrix)
        materialized["container"] = rendered if complete else container
    elif isinstance(container, dict):
        materialized_container = dict(container)
        image = container.get("image")
        if isinstance(image, str):
            rendered, complete = _substitute_matrix(image, matrix)
            materialized_container["image"] = rendered if complete else image
        materialized["container"] = materialized_container
    return materialized


def _substitute_matrix(template: str, matrix: dict[str, str]) -> tuple[str, bool]:
    complete = True

    def _replace(match: re.Match[str]) -> str:
        nonlocal complete
        key = match.group(1)
        if key not in matrix:
            complete = False
            return match.group(0)
        return matrix[key]

    rendered = _EXPRESSION_RE.sub(_replace, template)
    if "${{" in rendered:
        complete = False
    return rendered, complete


def _unsupported(display_name: str, reason: str) -> ResolvedWorkflowJob:
    from scripts.ci_fix.verify.base import VerifyEnv

    return ResolvedWorkflowJob(
        job_id="",
        display_name=display_name,
        matrix=(),
        environment=JobEnvironment(VerifyEnv.UNSUPPORTED, reason=reason),
        fidelity=_fidelity_contract(),
        reason=reason,
    )


def _fidelity_contract() -> dict[str, Any]:
    """Declare the exact boundary of the targeted verifier.

    This contract is persisted with the workflow/job metadata and included in
    the command digest. It deliberately does not claim equivalence to a GitHub
    Actions job.
    """
    return {
        "mode": "targeted-approximation-v1",
        "authoritative_check": "pull-request-ci",
        "reproduced": [
            "exact-workflow-revision",
            "job-id-and-display-name",
            "matrix-values",
            "runner-family",
            "container-image-when-declared",
            "clean-head-sha",
            "failing-baseline",
            "targeted-command",
            "repeated-patched-pass",
        ],
        "not_reproduced": [
            "complete-step-order",
            "uses-action-side-effects",
            "services-and-health-checks",
            "workflow-and-job-environment",
            "permissions-and-oidc",
            "shell-defaults",
            "cache-state",
            "runner-image-and-architecture-details",
        ],
    }
