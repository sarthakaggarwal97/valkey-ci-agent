"""Dispatch and observe no-publish release qualification (stage 3).

Qualification runs the archive and package builds against the exact
candidate SHA *without* any production write, in the automation repo's
``qualification_workflow``. Its evidence is GitHub-native and re-queried
live every time: the run id, the run's conclusion, and its job results —
never a stored assertion.

Correlation: ``workflow_dispatch`` returns no run id, and a run's metadata
does not carry its inputs. The qualification workflow therefore embeds the
candidate SHA in its ``run-name``; the controller matches runs by that
marker. A run whose name does not carry the exact SHA never counts.
"""

from __future__ import annotations

import logging
from typing import Any

from scripts.common.github_client import retry_github_call
from scripts.release.models import QualificationStatus
from scripts.release.policy import RepoReleasePolicy
from scripts.release.release_refs import workflow_handle

logger = logging.getLogger(__name__)

# Bound the run scan; qualification runs for an active release are recent.
_RUN_SCAN_LIMIT = 50


def evaluate_qualification(
    gh: Any, policy: RepoReleasePolicy, *, tag: str, sha: str,
) -> QualificationStatus:
    """Live qualification evidence for release *tag* at exactly *sha*.

    The newest matching run wins, so re-dispatching after a failure
    supersedes it. A successful run must also show zero failed jobs and at
    least ``qualification_min_jobs`` jobs — a truncated matrix (an empty
    generate step) must not pass vacuously.
    """
    run = _find_run(gh, policy, tag, sha)
    if run is None:
        return QualificationStatus()

    # A run that never planned jobs (startup_failure: invalid workflow file,
    # permission mismatch) is not evidence about the candidate — no build was
    # attempted. Treat it as absent so reconciliation redispatches once the
    # workflow is fixed; a real build failure still requires a human.
    if run.conclusion == "startup_failure":
        logger.warning(
            "Qualification run %s failed at startup (never planned); ignoring it",
            run.id,
        )
        return QualificationStatus()

    if run.status != "completed":
        return QualificationStatus(run_id=run.id, url=run.html_url, pending=True)

    jobs = retry_github_call(
        lambda: list(run.jobs()),
        retries=2, description=f"list jobs of qualification run {run.id}",
    )
    failed = tuple(j.name for j in jobs if j.conclusion not in ("success", "skipped"))
    if run.conclusion != "success" or failed:
        return QualificationStatus(
            run_id=run.id, url=run.html_url, passed=False,
            failed_jobs=failed or (f"(run concluded: {run.conclusion})",),
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
    succeeded = [j.name for j in jobs if j.conclusion == "success"]

    def count(marker: str) -> int:
        return sum(marker in name for name in succeeded)

    # Exact counts, not floors: "the GA matrix ran in full" means exactly
    # the reviewed inventory (30 RPM / 10 DEB legs today). A platform
    # addition or removal is a deliberate policy edit, reviewed alongside
    # the matrix change; a floor would silently tolerate losing a third of
    # the coverage. The " / RPM · " child-prefix anchor excludes the
    # "Test RPM · ..." legs from the build count.
    expectations = [
        ("Qualify x86 archives /", 2, "x86 archive builds"),
        ("Qualify ARM archives /", 2, "ARM archive builds"),
    ]
    is_ga = "-rc" not in tag
    if is_ga:
        expectations += [
            (" / RPM · ", down.qualification_rpm_jobs, "RPM package builds"),
            (" / DEB · ", down.qualification_deb_jobs, "DEB package builds"),
        ]
    gaps = [
        f"(evidence mismatch: {label} — {count(marker)} succeeded, "
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
        if not getattr(a, "expired", False) and getattr(a, "size_in_bytes", 1) > 0
    ]
    archive_artifacts = sum(name.startswith("qualify-") for name in usable)
    if archive_artifacts != 4:
        gaps.append(
            f"(evidence mismatch: usable archive artifacts — "
            f"{archive_artifacts} present, expected exactly 4)"
        )
    if is_ga:
        rpm_artifacts = sum(name.startswith("valkey-rpms-") for name in usable)
        deb_artifacts = sum(name.startswith("valkey-debs-") for name in usable)
        if rpm_artifacts != down.qualification_rpm_jobs:
            gaps.append(
                f"(evidence mismatch: usable RPM artifacts — {rpm_artifacts} "
                f"present, expected exactly {down.qualification_rpm_jobs})"
            )
        if deb_artifacts != down.qualification_deb_jobs:
            gaps.append(
                f"(evidence mismatch: usable DEB artifacts — {deb_artifacts} "
                f"present, expected exactly {down.qualification_deb_jobs})"
            )
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
    workflow = workflow_handle(gh, policy.downstream.automation_repo,
                               policy.downstream.qualification_workflow)
    if workflow is None:
        return None
    runs = retry_github_call(
        workflow.get_runs,
        retries=2, description="list qualification runs",
    )
    marker = f"Qualify {tag} @ {sha}"
    default_branch = retry_github_call(
        lambda: gh.get_repo(policy.downstream.automation_repo).default_branch,
        retries=2, description="resolve automation default branch",
    )
    for index, run in enumerate(runs):
        if index >= _RUN_SCAN_LIMIT:
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

