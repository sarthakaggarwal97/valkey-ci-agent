"""Find the copied Valkey CI job a fix is prepared in.

This repository keeps callable copies of Valkey's CI workflows
(``scripts/valkey_workflows``) that hold the jobs the agent can run in. A
copied job runs its own setup and build, and then prepares the fix in its own checkout, with its own
build and test commands (``scripts/ci_fix/job.sh``). :func:`copy_for` finds
the copy and job of a fix's failed job through ``manifest.json``;
:func:`fix_inputs` is what the copy is called with.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ci_fix.models import FailedTest, FixTask, TargetKind
from scripts.common.valkey_ci import called_job_name
from scripts.valkey_workflows.sync import REPO

MANIFEST = Path(__file__).resolve().parents[1] / "valkey_workflows" / "manifest.json"
# Daily and Weekly both run unstable's daily.yml (Weekly against a release
# branch), so a scheduled run's job is that copy's, at the tested branch.
SCHEDULED_WORKFLOW = ("unstable", "daily")
_EXPRESSION_RE = re.compile(r"\\\$\\\{\\\{.*?\\\}\\\}")
_MATRIX_SUFFIX_RE = re.compile(r" \((?P<value>[^()]*)\)$")


@dataclass(frozen=True)
class CopyJob:
    """One job of a branch's copied workflow."""

    branch: str
    workflow: str
    job: str
    # The job runs one file from ``valgrind_test``, which collapses its matrix.
    targeted: bool = False
    # The matrix key whose value tells the failed entry, or "".
    matrix_key: str = ""


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def copy_for(manifest: dict[str, Any], task: FixTask) -> CopyJob | None:
    """The copied job that hosts ``task``'s fix, or None.

    A PR's run used its base branch's workflow. A suggestion is never run,
    so it needs no job; only Valkey's own workflows are copied.
    """
    failure = task.failure
    if failure.repo_full_name != REPO or task.target.kind is TargetKind.SUGGEST:
        return None
    branch, workflow = (
        SCHEDULED_WORKFLOW if failure.scheduled else (task.target.base_branch, failure.workflow)
    )
    return copy_job_for(manifest, branch, workflow, failure.job_name)


def copy_job_for(manifest: dict[str, Any], branch: str, workflow: str, job_name: str) -> CopyJob | None:
    """The copied job that runs as ``job_name`` on ``branch``, or None."""
    entry = manifest.get(f"{branch}/{workflow}")
    if not entry:
        return None
    called = called_job_name(job_name)
    # GitHub appends a matrix job's values to a name that does not use them:
    # ``test-sanitizer-address (gcc)``.
    wanted = (called, _MATRIX_SUFFIX_RE.sub("", called))
    matrix = entry.get("matrix") or {}
    for job_id, names in (entry.get("jobs") or {}).items():
        for name in names:
            pattern = _EXPRESSION_RE.sub(".+", re.escape(name))
            if any(re.fullmatch(pattern, candidate) for candidate in wanted):
                return CopyJob(branch=branch, workflow=workflow, job=job_id,
                               targeted=job_id in (entry.get("targeted") or ()), matrix_key=matrix.get(job_id, ""))
    return None


def fix_inputs(copy: CopyJob, task: FixTask, tests: tuple[FailedTest, ...]) -> dict[str, str] | None:
    """The copy's inputs that prepare ``task``'s fix of ``tests`` in its job, or None when they cannot."""
    failure = task.failure
    matrix = ""
    if copy.matrix_key == "name":
        matrix = called_job_name(failure.job_name)  # an entry's name is its job's
    elif copy.matrix_key:
        suffix = _MATRIX_SUFFIX_RE.search(called_job_name(failure.job_name))
        if suffix is None:
            return None  # which entry failed is unknown
        matrix = suffix.group("value")
    if copy.targeted and not tests:
        return None  # its shards would each run the fix
    return {
        "branch": copy.branch,
        "workflow": copy.workflow,
        "job": copy.job,
        "use_repo": task.target.repo_full_name,
        "use_git_ref": task.target.expected_sha or failure.head_sha,
        "fix_matrix": matrix,
        # Only to collapse a targeted valgrind job's shards to one.
        "valgrind_test": tests[0].unit if copy.targeted and tests else "",
        "accurate": "true" if failure.scheduled else "false",
    }
