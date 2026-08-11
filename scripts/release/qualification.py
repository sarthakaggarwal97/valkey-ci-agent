"""Dispatch and observe no-publish release qualification (stage 3).

Qualification runs the archive and package builds against the exact
candidate SHA *without* any production write, in the automation repo's
``qualification_workflow``. Its evidence is GitHub-native and re-queried
live every time: the run id, the run's conclusion, and its job results,
never a stored assertion.

Correlation: ``workflow_dispatch`` returns no run id, and a run's metadata
does not carry its inputs. The qualification workflow therefore embeds the
candidate SHA in its ``run-name``; the controller matches runs by that
marker. A run whose name does not carry the exact SHA never counts.
"""

from __future__ import annotations

import logging
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import QualificationStatus
from scripts.release.policy import RepoReleasePolicy
from scripts.release.release_refs import workflow_handle

logger = logging.getLogger(__name__)

# Bound the run scan; qualification runs for an active release are recent.
# Shared with verify.py's build-run scan.
RUN_SCAN_LIMIT = 50

# The failed-jobs sentinel a startup_failure run reports. The dataclass
# gains no flag (models.py is owned elsewhere and the shape must not
# change): run_id set + pending False + passed False + this entry is the
# whole encoding, and it routes through the same marker-gated one-retry
# path as any other failed run, so reconciliation can never redispatch it
# every pass.
STARTUP_FAILURE_JOB = "(Workflow startup failed)"

# The metadata-only manifest artifact the qualification workflow uploads
# (schema 1: nonce/version/tag/source_sha/automation_sha/job counts).
# Presence + unexpired is the whole check this round; content is never
# fetched (follow-up: download and validate the JSON against the policy).
MANIFEST_ARTIFACT = "qualification-manifest"


def evaluate_qualification(
    gh: Any, policy: RepoReleasePolicy, *, tag: str, sha: str,
) -> QualificationStatus:
    """Live qualification evidence for release *tag* at exactly *sha*.

    The newest matching run wins, so re-dispatching after a failure
    supersedes it. A successful run must also show zero failed jobs and at
    least ``qualification_min_jobs`` jobs: a truncated matrix (an empty
    generate step) must not pass vacuously.
    """
    run = _find_run(gh, policy, tag, sha)
    if run is None:
        return QualificationStatus()

    # A run that never planned jobs (startup_failure: invalid workflow file,
    # permission mismatch) is not evidence about the candidate; no build was
    # attempted. It must still not read as "no run": that would redispatch
    # every pass forever. It reports as a failed run (run id preserved, the
    # sentinel as the failed job), so actions.advance routes it through the
    # marker-gated one-retry path; after a second startup failure nothing
    # dispatches and the failure notification stands for a human.
    if run.conclusion == "startup_failure":
        logger.warning(
            "Qualification run %s failed at startup (never planned any jobs)",
            run.id,
        )
        return QualificationStatus(
            run_id=run.id, url=run.html_url, passed=False,
            failed_jobs=(STARTUP_FAILURE_JOB,),
        )

    if run.status != "completed":
        return QualificationStatus(run_id=run.id, url=run.html_url, pending=True)

    jobs = retry_github_call(
        lambda: list(run.jobs()),
        retries=2, description=f"list jobs of qualification run {run.id}",
    )
    # GitHub can report the run completed while individual jobs are still
    # finishing (observed live: run.conclusion success with two test jobs
    # in_progress). An incomplete job means the verdict is not in yet:
    # report pending, never failed, so nobody gets paged for a job that
    # is merely still running.
    if any((j.status or "") != "completed" for j in jobs):
        return QualificationStatus(run_id=run.id, url=run.html_url, pending=True)
    # (j.name or ""): a job served with a null name must read as unnamed,
    # not raise through the caller and abort the pass.
    failed = tuple((j.name or "") for j in jobs
                   if j.conclusion not in ("success", "skipped"))
    if run.conclusion != "success" or failed:
        return QualificationStatus(
            run_id=run.id, url=run.html_url, passed=False,
            failed_jobs=failed or (f"(Run concluded: {run.conclusion})",),
        )
    gaps = _evidence_gaps(policy, run, jobs, tag)
    if gaps:
        return QualificationStatus(
            run_id=run.id, url=run.html_url, passed=False, failed_jobs=gaps,
        )
    return QualificationStatus(run_id=run.id, url=run.html_url, passed=True)


def _evidence_gaps(policy: RepoReleasePolicy, run: Any, jobs: list,
                   tag: str) -> tuple[str, ...]:
    """Structural evidence a green run must still produce, by stage.

    A run conclusion alone is satisfiable by an empty or truncated matrix
    (a broken generate step, a skipped package call): the specific job
    groups and their artifacts must exist. Release candidates skip the
    package matrix by design; a GA must have it in full.
    """
    down = policy.downstream
    succeeded = [(j.name or "") for j in jobs if j.conclusion == "success"]

    def count(marker: str) -> int:
        # Distinct names, not occurrences: the same job listed twice (rerun
        # attempts served together) must not satisfy the exact inventory
        # with a platform missing.
        return len({name for name in succeeded if marker in name})

    # Exact counts, not floors: "the GA matrix ran in full" means exactly
    # the reviewed inventory (from the policy file). A platform addition or
    # removal is a deliberate policy edit, reviewed alongside the matrix
    # change; a floor would silently tolerate losing a third of the
    # coverage. The " / RPM · " child-prefix anchor excludes the
    # "Test RPM · ..." legs from the build count.
    # Job-name prefixes are set by
    # valkey-release-automation/.github/workflows/qualify-release.yml.
    expectations = [
        ("Qualify x86 archives /", down.qualification_x86_archive_jobs,
         "x86 archive builds"),
        ("Qualify ARM archives /", down.qualification_arm_archive_jobs,
         "ARM archive builds"),
    ]
    is_ga = "-rc" not in tag
    if is_ga:
        expectations += [
            (" / RPM · ", down.qualification_rpm_jobs, "RPM package builds"),
            (" / DEB · ", down.qualification_deb_jobs, "DEB package builds"),
        ]
    gaps = [
        f"(Evidence mismatch: {label}, {count(marker)} succeeded, "
        f"expected exactly {expected})"
        for marker, expected, label in expectations
        if count(marker) != expected
    ]

    artifacts = retry_github_call(
        lambda: list(run.get_artifacts()),
        retries=2, description=f"list artifacts of qualification run {run.id}",
    )
    # An expired or empty artifact is a name, not evidence.
    usable = [
        a.name for a in artifacts
        if not a.expired and a.size_in_bytes > 0
    ]
    archive_artifacts = sum(name.startswith("qualify-") for name in usable)
    expected_archives = (down.qualification_x86_archive_jobs
                         + down.qualification_arm_archive_jobs)
    if archive_artifacts != expected_archives:
        gaps.append(
            f"(Evidence mismatch: usable archive artifacts, "
            f"{archive_artifacts} present, expected exactly {expected_archives})"
        )
    if is_ga:
        rpm_artifacts = sum(name.startswith("valkey-rpms-") for name in usable)
        deb_artifacts = sum(name.startswith("valkey-debs-") for name in usable)
        if rpm_artifacts != down.qualification_rpm_jobs:
            gaps.append(
                f"(Evidence mismatch: usable RPM artifacts, {rpm_artifacts} "
                f"present, expected exactly {down.qualification_rpm_jobs})"
            )
        if deb_artifacts != down.qualification_deb_jobs:
            gaps.append(
                f"(Evidence mismatch: usable DEB artifacts, {deb_artifacts} "
                f"present, expected exactly {down.qualification_deb_jobs})"
            )
    # The qualification manifest (uploaded by the qualification workflow)
    # must be present and unexpired. Metadata-only this round: presence is
    # the evidence, no download, and the job counts it would restate are
    # already validated exactly by the expectations above. A run without it
    # (any legacy run) fails closed going forward.
    if MANIFEST_ARTIFACT not in usable:
        gaps.append("(Evidence mismatch: no qualification manifest)")
    return tuple(gaps)


def dispatch_qualification(
    gh: Any, policy: RepoReleasePolicy, *, tag: str, sha: str,
) -> None:
    """Start a qualification run for release *tag* at exactly *sha*.

    The tag (not the bare version) is the dispatched identity: the
    qualification workflow mirrors production applicability from it (an rc
    skips the distro package matrix exactly as the production build does).

    Callers guard idempotency (dispatch only when no pending or passed run
    exists for this SHA); this function just fires the dispatch on the
    automation repo's default branch.
    """
    workflow = workflow_handle(gh, policy.downstream.automation_repo,
                               policy.downstream.qualification_workflow)
    if workflow is None:
        raise RuntimeError(
            f"{policy.downstream.qualification_workflow} does not exist on "
            f"{policy.downstream.automation_repo}"
        )
    repo = retry_github_call(
        lambda: gh.get_repo(policy.downstream.automation_repo),
        retries=2, description=f"get repo {policy.downstream.automation_repo}",
    )
    dispatched = retry_github_call(
        lambda: workflow.create_dispatch(
            repo.default_branch, inputs={"version": tag, "source_sha": sha},
        ),
        retries=2, description="dispatch qualification run",
    )
    if not dispatched:
        raise RuntimeError(
            f"qualification dispatch was rejected by "
            f"{policy.downstream.automation_repo}/{policy.downstream.qualification_workflow}"
        )
    logger.info("Dispatched qualification of %s @ %s", tag, sha[:12])


def _find_run(gh: Any, policy: RepoReleasePolicy, tag: str, sha: str) -> Any:
    """The newest qualification run for exactly this release and commit.

    Two binding rules keep the evidence honest:
    - the run-name must carry the full ``Qualify <tag> @ <sha>`` marker, so
      a run dispatched with a different version for the same SHA (e.g. an
      rc-suffixed dispatch that legitimately skips the package matrix) can
      never satisfy this release's qualification;
    - the run must have executed the default branch's workflow definition,
      so a doctored qualify workflow on a side branch cannot manufacture
      evidence.
    """
    # One repo fetch serves both the workflow lookup and the default-branch
    # check (workflow_handle would hide the repo and force a second fetch).
    repo = retry_github_call(
        lambda: gh.get_repo(policy.downstream.automation_repo),
        retries=2, description=f"get repo {policy.downstream.automation_repo}",
    )
    try:
        workflow = retry_github_call(
            lambda: repo.get_workflow(policy.downstream.qualification_workflow),
            retries=2,
            description=f"get workflow {policy.downstream.qualification_workflow}",
        )
    except GithubException as exc:
        if exc.status == 404:
            return None
        raise
    runs = retry_github_call(
        workflow.get_runs,
        retries=2, description="list qualification runs",
    )
    # Run-name marker set by
    # valkey-release-automation/.github/workflows/qualify-release.yml.
    marker = f"Qualify {tag} @ {sha}"
    default_branch = repo.default_branch
    for index, run in enumerate(runs):
        if index >= RUN_SCAN_LIMIT:
            break
        if marker not in (run.display_title or ""):
            continue
        if run.head_branch != default_branch:
            logger.warning(
                "Ignoring qualification run %s: executed from ref %r, not the "
                "default branch", run.id, run.head_branch,
            )
            continue
        return run
    return None

