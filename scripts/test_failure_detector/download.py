"""Download test failure artifacts from a Valkey CI workflow run"""

from __future__ import annotations

import logging
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from github import Github
from github.WorkflowRun import WorkflowRun

from scripts.common.github_client import retry_github_call
from scripts.common.workflow_artifacts import ArtifactClient, ArtifactState

logger = logging.getLogger(__name__)

# Name of the JSON file the Valkey CI workflow uploads inside its artifact zip.
_FAILURES_JSON_NAME = "all-test-failures.json"
_FAILURES_ARTIFACT_NAME = "all-test-failures"


@dataclass(frozen=True)
class TestFailureArtifact:
    state: ArtifactState
    content: bytes | None = None
    detail: str = ""


def get_latest_daily_run(
    gh: Github,
    repo_full_name: str,
    workflow_name: str = "Daily",
    branch: str = "unstable",
) -> WorkflowRun | None:
    """Find the most recent completed (non-cancelled) Daily workflow run."""
    repo = retry_github_call(
        lambda: gh.get_repo(repo_full_name),
        retries=3,
        description=f"get repo {repo_full_name}",
    )

    workflows = retry_github_call(
        lambda: repo.get_workflows(),
        retries=3,
        description="list workflows",
    )

    daily_workflow = None
    for wf in workflows:
        if wf.name == workflow_name:
            daily_workflow = wf
            break

    if daily_workflow is None:
        logger.warning("Workflow %r not found in %s", workflow_name, repo_full_name)
        return None

    # Restrict to scheduled runs. The Valkey Daily workflow also runs on
    # pull_request (and fork PRs sit at action_required with no artifacts),
    # so we'd sometimes silently analyze the wrong run.
    runs = retry_github_call(
        lambda: daily_workflow.get_runs(
            branch=branch, status="completed", event="schedule",
        ),
        retries=3,
        description=f"list runs for {workflow_name}",
    )

    for run in runs:
        # Skip runs that never actually executed: cancelled/skipped, runs
        # awaiting approval (action_required, e.g. fork PRs) or expired
        # (stale), and runs with no conclusion yet. These produce no test
        # artifacts and would be mistaken for a clean pass.
        if run.conclusion in ("cancelled", "skipped", "action_required", "stale", None):
            logger.debug(
                "Skipping run #%d (conclusion=%s)", run.run_number, run.conclusion,
            )
            continue
        logger.info(
            "Found daily run #%d (id=%d, conclusion=%s, created=%s)",
            run.run_number, run.id, run.conclusion, run.created_at,
        )
        return run

    logger.warning("No completed non-cancelled run found for %s/%s", workflow_name, branch)
    return None

def download_all_test_failures(
    gh: Github,
    repo_full_name: str,
    run_id: int,
    github_token: str,
    *,
    artifact_client: ArtifactClient | None = None,
) -> TestFailureArtifact:
    """Download the 'all-test-failures' artifact from a workflow run.

    Returns a typed state so absence, expiration, corruption, oversize, and
    transport failures cannot be mistaken for a clean CI run.
    """
    client = artifact_client or ArtifactClient(gh, token=github_token)

    artifacts = client.list_run_artifacts(repo_full_name, run_id)
    target = next(
        (a for a in artifacts if a.name == _FAILURES_ARTIFACT_NAME), None
    )
    if target is None:
        logger.info(
            "No %r artifact found in run %d", _FAILURES_ARTIFACT_NAME, run_id
        )
        return TestFailureArtifact(
            ArtifactState.NOT_FOUND,
            detail=f"artifact {_FAILURES_ARTIFACT_NAME!r} was not listed",
        )
    if target.expired:
        logger.warning(
            "Artifact %r (id=%d) in run %d has expired",
            target.name, target.artifact_id, run_id,
        )
        return TestFailureArtifact(
            ArtifactState.EXPIRED,
            detail=f"artifact {target.artifact_id} has expired",
        )

    logger.info("Downloading artifact: %s (id=%d)", target.name, target.artifact_id)
    with tempfile.TemporaryDirectory(prefix="test-failures-") as temporary:
        result = client.download_artifact(
            repo_full_name,
            target.artifact_id,
            destination=Path(temporary),
            requested={_FAILURES_JSON_NAME},
        )
        if result.state is not ArtifactState.AVAILABLE:
            return TestFailureArtifact(result.state, detail=result.detail)
        member = result.member(_FAILURES_JSON_NAME)
        if member is None:
            logger.warning(
                "Artifact zip for run %d does not contain %s",
                run_id,
                _FAILURES_JSON_NAME,
            )
            return TestFailureArtifact(
                ArtifactState.MEMBER_MISSING,
                detail=f"artifact does not contain {_FAILURES_JSON_NAME}",
            )
        content = member.path.read_bytes()

    logger.info("Extracted %s from artifact zip", _FAILURES_JSON_NAME)
    return TestFailureArtifact(ArtifactState.AVAILABLE, content=content)

def get_job_urls(
    gh: Github,
    repo_full_name: str,
    run_id: int,
) -> dict[str, str]:
    """Get a mapping of job name -> HTML URL for all jobs in a workflow run.

    Also includes normalized variants (parentheses replaced with dashes,
    spaces replaced with dashes) for fuzzy matching.
    """

    repo = retry_github_call(
        lambda: gh.get_repo(repo_full_name),
        retries=3,
        description=f"get repo {repo_full_name}",
    )

    run = retry_github_call(
        lambda: repo.get_workflow_run(run_id),
        retries=3,
        description=f"get run {run_id}",
    )

    jobs = retry_github_call(
        lambda: run.jobs(),
        retries=3,
        description=f"list jobs for run {run_id}",
    )

    # Materialize once: jobs may be a lazy paginated list, and we iterate twice.
    job_list = list(jobs)

    # First pass: exact job names. These are authoritative, so they take
    # precedence over any normalized alias.
    job_url_map: dict[str, str] = {job.name: job.html_url for job in job_list}

    # Second pass: normalized variants for fuzzy matching against artifact
    # names. Only add an alias when it does not collide with an exact job name,
    # so a normalized alias of one job can never overwrite another job's exact
    # mapping and attach the wrong CI URL.
    for job in job_list:
        normalized = re.sub(r"\s*\(([^)]+)\)", r"-\1", job.name)
        normalized = re.sub(r"\s+", "-", normalized)
        if normalized != job.name and normalized not in job_url_map:
            job_url_map[normalized] = job.html_url

    logger.info("Found %d job URL mappings for run %d", len(job_url_map), run_id)
    return job_url_map
