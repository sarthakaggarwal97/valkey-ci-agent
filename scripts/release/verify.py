"""Verify the release's required public outputs (stages 5 and 6).

Every verifier checks the *canonical public artifact* (a registry manifest,
a public file, a merged PR, a git tag), not merely that some workflow
reported success. "PR opened" and "dispatch accepted" are progress states,
never completion.

Ordering is encoded as BLOCKED states: Bundle work is blocked until the
Valkey container images are public, and Helm until the chart's image tag is
public, so downstream work that must not start early is visibly *blocked*
rather than silently absent.

All verifiers are read-only. Side effects (dispatching Bundle, opening the
Helm PR, notifications) live in :mod:`scripts.release.actions`.

Verification proves public existence and workflow-run evidence, not
cryptographic provenance: digest/checksum binding of the published
artifacts to the candidate SHA is a known follow-up.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import yaml
from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import public_endpoints as pub
from scripts.release.models import DownstreamOutput, OutputState
from scripts.release.policy import RepoReleasePolicy
from scripts.release.qualification import RUN_SCAN_LIMIT
from scripts.release.release_refs import (
    get_repo,
    humanize_minutes,
    read_text_file,
    resolve_tag_commit,
    workflow_handle,
)
from scripts.release_notes.release_format import parse_version

logger = logging.getLogger(__name__)

# The image tags valkey-container publishes per release that downstream
# consumers require: the bare tag (Helm chart default) and the two variant
# tags the Bundle build pulls as base images.
CONTAINER_TAG_SUFFIXES = ("", "-trixie", "-alpine")

CHART_APP_VERSION_RE = re.compile(r'^appVersion:\s*"?([0-9.]+)"?\s*$', re.MULTILINE)
CHART_VERSION_RE = re.compile(r"^version:\s*([0-9.]+)\s*$", re.MULTILINE)
_BUNDLE_UPDATE_BRANCH = "valkey-bundle-update"  # fixed reused head branch
# The valkey repo's release-event workflow that dispatches the production
# build; observed so a dispatch that dies before reaching the automation
# repo reads FAILED instead of pending forever.
_TRIGGER_WORKFLOW = "trigger-build-release.yml"


def verify_core_outputs(
    gh: Any, policy: RepoReleasePolicy, *, tag: str, stage: str,
    gh_source: Any, published_at: Any,
) -> tuple[DownstreamOutput, ...]:
    """Stage-5 outputs: build run, tarballs+hashes, hashes repo, container,
    docs, website. Assumes the GitHub release/tag was already verified by
    the caller (it gates the PUBLISHED phase).

    Every artifact is named after the *tag* (``9.2.0-rc1``, not ``9.2.0``):
    the production dispatch sends the tag as its version, so tarballs,
    hashes lines, container branches, and image tags all carry it.
    """
    down = policy.downstream
    resolved_run: list[Any] = [None]

    def _build() -> DownstreamOutput:
        output, resolved_run[0] = _verify_build_run(
            gh, gh_source, policy, tag, published_at)
        return output

    (build,) = _guarded("build-run", _build)
    # The run object and its jobs are resolved once and shared by the
    # packages and Try Valkey verifiers (previously three fetches per pass).
    jobs = _build_run_jobs(build, resolved_run[0])
    return (
        build,
        *_guarded("tarballs", lambda: _verify_tarballs(down, tag)),
        *_guarded("packages", lambda: _verify_packages(down, stage, build, jobs)),
        *_guarded("try-valkey", lambda: _verify_try_valkey(
            stage, build, resolved_run[0], jobs,
            gh_source=gh_source, repo_name=policy.repo, tag=tag)),
        *_guarded("hashes", lambda: _verify_hashes(gh, down, tag)),
        *_guarded("container-pr", lambda: _verify_container(gh, down, tag)),
        *_guarded("docs", lambda: _verify_docs(gh, down, tag, stage)),
        *_guarded("website", lambda: _verify_website(gh, down, tag, stage)),
    )


def verify_ordered_outputs(
    gh: Any, policy: RepoReleasePolicy, *, version: str, tag: str, stage: str,
    core: tuple[DownstreamOutput, ...], published_at: Any = None,
) -> tuple[DownstreamOutput, ...]:
    """Stage-6 outputs: Bundle and Helm, gated on public base images.

    ``published_at``, when known, lets the Bundle verifier ignore update
    PRs that predate this release's publication.
    """
    images_public = any(
        output.name == "container-images" and output.state is OutputState.VERIFIED
        for output in core
    )
    return (
        *_guarded("bundle", lambda: _verify_bundle(
            gh, policy.downstream, version, tag, images_public=images_public,
            published_at=published_at)),
        # container-images includes the bare tag (the chart's default image).
        *_guarded("helm", lambda: _verify_helm(
            gh, policy.downstream, version, stage, image_public=images_public)),
    )


def outputs_all_settled(outputs: tuple[DownstreamOutput, ...]) -> bool:
    """True when every output is VERIFIED or SKIPPED (nothing pending/failed)."""
    return all(
        output.state in (OutputState.VERIFIED, OutputState.SKIPPED)
        for output in outputs
    )


def escalate_stalled_outputs(
    outputs: tuple[DownstreamOutput, ...], published_at: Any, timeout_minutes: int,
) -> tuple[DownstreamOutput, ...]:
    """PENDING past the deadline becomes FAILED, so stalls enter the
    notification path exactly once instead of staying invisible forever
    (the 'Helm discovered next day' failure mode). BLOCKED outputs are
    exempt: their prerequisite carries the escalation.

    The deadline is per-attempt: for an output that already matched a
    workflow run or downstream PR the timeout runs from THAT object's
    creation timestamp (``attempt_started_at``), not the release-wide
    ``published_at``. This matters when the release spent hours BLOCKED
    upstream before the downstream attempt could even start; charging
    that upstream block against a freshly-opened Bundle or Helm PR would
    kill the very first attempt on its first pass. When an output carries
    no attempt evidence the release-wide clock still applies.
    """
    def _stalled(o: DownstreamOutput) -> bool:
        if o.state is not OutputState.PENDING:
            return False
        # An action-bearing output with no attempt evidence (empty run_id
        # AND empty url) has never been tried: the release-wide clock
        # started at publication, but such an output may have spent that
        # entire window BLOCKED behind a prerequisite (Bundle and Helm wait
        # for the container images) and only just become eligible to start.
        # Escalating it would clear the action and kill the very first
        # attempt before one was ever observed, so it stays PENDING with
        # its action intact. Once an attempt exists (run/PR evidence fills
        # run_id or url), the normal deadline applies.
        if o.action and not o.run_id and not o.url:
            return False
        deadline_start = o.attempt_started_at or published_at
        return _past_deadline(deadline_start, timeout_minutes)

    return tuple(
        DownstreamOutput(
            name=o.name, state=OutputState.FAILED,
            detail=f"Stalled after {humanize_minutes(timeout_minutes)}: "
                   f"{o.detail.rstrip('.')}",
            # action cleared: an escalated stall pages a human; it must not
            # also keep auto-dispatching every pass.
            url=o.url, action="", run_id=o.run_id,
            attempt_started_at=o.attempt_started_at,
        ) if _stalled(o) else o
        for o in outputs
    )


def _guarded(name: str, verifier: Callable[[], Any]) -> tuple[DownstreamOutput, ...]:
    """Run a verifier, degrading its API failure to a FAILED output.

    The one result shape every verifier flows through: a verifier may
    return one output or a sequence of them, and callers always receive a
    tuple, never dispatching on the return type.

    One missing or renamed downstream repository (a 404 on its first read)
    must report as that output failing, feeding the checklist and the
    one-shot notification, rather than aborting the whole reconcile pass
    and freezing the tracker at stale state.
    """
    try:
        result = verifier()
    except GithubException as exc:
        logger.warning("Verifier %s failed: HTTP %s", name, exc.status)
        return (DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"GitHub returned HTTP {exc.status}: the target repository "
                   f"is missing or unreadable",
        ),)
    except (OSError, ValueError) as exc:
        # OSError covers URLError and TimeoutError; ValueError covers
        # json.JSONDecodeError. public_endpoints deliberately raises on
        # 5xx/429 so registry outages surface loudly: they land here as a
        # probe error for THIS output only, never a pass abort.
        logger.warning("Verifier %s failed: %s", name, exc)
        reason = str(exc).strip() or type(exc).__name__
        return (DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"Probe error: {reason}",
        ),)
    if isinstance(result, (list, tuple)):
        return tuple(result)
    return (result,)


# ---------------------------------------------------------------------------
# Stage 5 verifiers


def _verify_build_run(gh: Any, gh_source: Any, policy: RepoReleasePolicy,
                      tag: str, published_at: Any) -> "tuple[DownstreamOutput, Any]":
    """The production build for this release, observed from the trigger out.

    Returns the output plus the matched run object (None when absent) so
    the caller can reuse the run for job/artifact evidence without
    re-fetching it.

    Three properties the previous implementation lacked, each of which the
    live E2E showed matters:

    - the *trigger* run in the valkey repo is observed first, so a dispatch
      that dies before reaching the automation repo reads FAILED instead of
      leaving the output pending forever;
    - the automation run is selected from build-release.yml explicitly and
      must carry the exact ``Build Release <tag> (prod)`` marker: a dev
      dispatch or unrelated repository_dispatch run can never satisfy it;
    - absence is bounded: no matching run within the policy check timeout of
      publication is a failure, not an eternal pending.
    """
    down = policy.downstream
    # The build run is looked for FIRST: a failed trigger must never veto a
    # build that exists (recovery may dispatch build-release directly, or
    # re-run the trigger; either way, a marked build run supersedes the
    # trigger's failure).
    # Run-name set by
    # valkey-release-automation/.github/workflows/build-release.yml.
    marker = f"Build Release {tag} (prod)"
    run = _newest_marked_run(
        gh, down.automation_repo, down.build_workflow, marker, published_at,
    )
    if run is None:
        trigger = _newest_marked_run(
            gh_source, policy.repo, _TRIGGER_WORKFLOW, tag, published_at,
        )
        if trigger is not None and trigger.status == "completed" \
                and trigger.conclusion != "success":
            # action: reconciliation dispatches build-release directly, once
            # per candidate (marker-gated in actions.advance); the FAILED
            # state still reaches the notification path unchanged. A
            # cancelled trigger is a human decision, not a retry condition
            # (the same stance the closed-PR verifiers take), so it renders
            # FAILED without requesting the auto-dispatch.
            action = ("dispatch-build-release"
                      if trigger.conclusion in ("failure", "timed_out") else "")
            return DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail=f"The release trigger run {_concluded(trigger.conclusion)} "
                       f"before dispatching the build. Re-run it, or dispatch "
                       f"build-release for {tag} directly.",
                url=trigger.html_url,
                action=action,
            ), None
        if _past_deadline(published_at, policy.check_timeout_minutes):
            return DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail=f"No '{marker}' run appeared within "
                       f"{policy.check_timeout_minutes} minutes of publication; "
                       f"the dispatch chain needs investigation",
            ), None
        return DownstreamOutput(
            name="build-run", state=OutputState.PENDING,
            detail=f"No '{marker}' run found yet",
        ), None
    if run.status != "completed":
        return DownstreamOutput(
            name="build-run", state=OutputState.PENDING,
            detail=f"Build-release run {run.id} is still executing", url=run.html_url,
            attempt_started_at=run.created_at,
        ), run
    if run.conclusion == "success":
        return DownstreamOutput(
            name="build-run", state=OutputState.VERIFIED,
            detail=f"Build-release run {run.id} succeeded", url=run.html_url,
            run_id=run.id,
        ), run
    return DownstreamOutput(
        name="build-run", state=OutputState.FAILED,
        detail=f"Build-release run {run.id} {_concluded(run.conclusion)}",
        url=run.html_url,
    ), run


# Render a workflow-run conclusion as a proper verb phrase instead of the
# raw enum-ish "concluded <value>" fragment.
_CONCLUSION_PHRASES = {
    "failure": "failed",
    "cancelled": "was cancelled",
    "timed_out": "timed out",
}


def _concluded(conclusion: str) -> str:
    return _CONCLUSION_PHRASES.get(conclusion, f"concluded {conclusion}")


def _newest_marked_run(gh: Any, repo_name: str, workflow_file: str,
                       marker: str, published_at: Any) -> Any:
    """Newest run of *workflow_file* whose title carries *marker*, created
    at or after *published_at* (when known)."""
    workflow = workflow_handle(gh, repo_name, workflow_file)
    if workflow is None:
        return None
    runs = retry_github_call(
        workflow.get_runs,
        retries=2, description=f"list {workflow_file} runs",
    )
    pattern = _exact_token_re(marker)
    for index, run in enumerate(runs):
        if index >= RUN_SCAN_LIMIT:
            break
        if published_at is not None and run.created_at < published_at:
            continue
        # Boundary-anchored: the bare-tag trigger marker for 9.1.2 must not
        # match a 9.1.20 run title, and (the trailing '-') the GA marker
        # 9.1.2 must never match a 9.1.2-rc1 title. (Release-event trigger
        # runs title as the tag itself: GitHub display-title behavior, not
        # a run-name we set; a manually dispatched trigger has no tag in its
        # title and is invisible here, which the FAILED detail's recovery
        # text covers.)
        if pattern.search(run.display_title or ""):
            return run
    return None


def _past_deadline(published_at: Any, timeout_minutes: int) -> bool:
    if published_at is None:
        return False
    return datetime.now(timezone.utc) - published_at > timedelta(minutes=timeout_minutes)


def _build_run_jobs(build: DownstreamOutput, run: Any) -> Any:
    """Jobs of the verified build run, fetched once and shared by the
    packages and Try Valkey verifiers (None when unavailable)."""
    if run is None or build.state is not OutputState.VERIFIED:
        return None
    try:
        return retry_github_call(
            lambda: list(run.jobs()),
            retries=2, description=f"list jobs of run {run.id}",
        )
    except GithubException as exc:
        logger.warning("Listing jobs of run %s failed: HTTP %s", run.id, exc.status)
        return None


def _build_gated(name: str, build: DownstreamOutput,
                 jobs: Any) -> "DownstreamOutput | None":
    """The shared job-evidence preamble: BLOCKED until the build run is
    verified, FAILED when its jobs could not be listed; None once job
    evidence is available."""
    if build.state is not OutputState.VERIFIED:
        return DownstreamOutput(
            name=name, state=OutputState.BLOCKED,
            detail="Waiting for the build-release run to succeed",
        )
    if jobs is None:
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail="Could not list the build run's jobs", url=build.url,
        )
    return None


def _verify_packages(down: Any, stage: str, build: DownstreamOutput,
                     jobs: Any) -> DownstreamOutput:
    """RPM/DEB publication (GA only), evidenced by the build run's publish
    and pages jobs succeeding in exactly the reviewed inventory.

    Job-level evidence rather than a public endpoint: the package repos'
    public layout is not a stable URL contract, and the plan explicitly
    allows the authoritative publish workflow as the v1 canonical signal.

    Counts are exact, not floors (the qualification verifier's stance): a
    green-but-smaller publish matrix means a platform was silently dropped,
    which must read FAILED, not VERIFIED. The expected counts are the
    policy's qualification_rpm_jobs/deb_jobs inventory: the production
    matrix publishes the same reviewed platform set qualification builds.
    """
    if stage != "ga":
        return DownstreamOutput(
            name="packages", state=OutputState.SKIPPED,
            detail="Distro packages are not built for release candidates",
        )
    gated = _build_gated("packages", build, jobs)
    if gated is not None:
        return gated
    # Job names set by
    # valkey-release-automation/.github/workflows/build-release.yml.
    # A hostile payload can serve a job with a null name; (j.name or "")
    # keeps the match a str test instead of a TypeError through _guarded.
    publish_jobs = [j for j in jobs
                    if "Publish to S3" in (j.name or "") or "Deploy Pages" in (j.name or "")]
    if not publish_jobs:
        return DownstreamOutput(
            name="packages", state=OutputState.FAILED,
            detail="The build run has no package publish jobs; the matrix "
                   "may not have run", url=build.url,
        )
    failed = [j.name for j in publish_jobs if j.conclusion != "success"]
    if failed:
        return DownstreamOutput(
            name="packages", state=OutputState.FAILED,
            detail=f"Package publication jobs failed: {', '.join(failed)}",
            url=build.url,
        )
    # Distinct names, not occurrences: the same job listed twice (rerun
    # attempts served together) must not satisfy the inventory with a
    # platform missing. The RPM/DEB markers follow the qualification
    # verifier's job-name convention.
    succeeded = {(j.name or "") for j in publish_jobs if j.conclusion == "success"}
    for marker, expected, label in (
        ("RPM", down.qualification_rpm_jobs, "RPM"),
        ("DEB", down.qualification_deb_jobs, "DEB"),
    ):
        count = sum(marker in name for name in succeeded)
        if count != expected:
            return DownstreamOutput(
                name="packages", state=OutputState.FAILED,
                detail=f"(Evidence mismatch: {count} {label} publish jobs "
                       f"succeeded, expected exactly {expected})",
                url=build.url,
            )
    # Only claim Pages succeeded when a Deploy Pages job is actually
    # present in the succeeded set. Otherwise keep the detail honest: the
    # RPM/DEB matrix satisfied its inventory, but the site deployment was
    # not observed by name and is unverified here. Full digest/provenance
    # binding is a deferred redesign; not lying about pages is the fix.
    pages_succeeded = any("Deploy Pages" in name for name in succeeded)
    pages_detail = "and the pages jobs succeeded" if pages_succeeded else "(pages not checked)"
    return DownstreamOutput(
        name="packages", state=OutputState.VERIFIED,
        detail=f"All {down.qualification_rpm_jobs} RPM and "
               f"{down.qualification_deb_jobs} DEB publish jobs succeeded "
               f"{pages_detail}", url=build.url,
    )


def _verify_try_valkey(stage: str, build: DownstreamOutput, run: Any,
                       jobs: Any, *, gh_source: Any, repo_name: str,
                       tag: str) -> DownstreamOutput:
    """Try Valkey upload, evidenced by the build run's update-try-valkey job.

    The job itself skips for release candidates and for versions that are
    not the latest release. A missing sentinel is only acceptable
    (SKIPPED) when this release is provably not the repository's latest:
    for the latest GA a missing or expired sentinel means the public Try
    Valkey deployment was never updated, which is a failure.
    """
    if stage != "ga":
        return DownstreamOutput(
            name="try-valkey", state=OutputState.SKIPPED,
            detail="Try Valkey is not updated for release candidates",
        )
    gated = _build_gated("try-valkey", build, jobs)
    if gated is not None:
        return gated
    try_jobs = [j for j in jobs
                if "try-valkey" in (j.name or "").lower()
                or "try valkey" in (j.name or "").lower()]
    if not try_jobs:
        return DownstreamOutput(
            name="try-valkey", state=OutputState.FAILED,
            detail="The build run has no Try Valkey job", url=build.url,
        )
    failed = [j.name for j in try_jobs if j.conclusion not in ("success", "skipped")]
    if failed:
        return DownstreamOutput(
            name="try-valkey", state=OutputState.FAILED,
            detail=f"Try Valkey jobs failed: {', '.join(failed)}", url=build.url,
        )
    # The wrapper job succeeds whether or not it uploaded (it internally
    # skips non-latest releases), so job conclusion cannot support a
    # VERIFIED claim. The workflow uploads a native sentinel artifact when
    # (and only when) the upload happened. Sentinel name set by
    # valkey-release-automation/.github/workflows/build-release.yml.
    if _run_has_artifact(run, "try-valkey-uploaded"):
        return DownstreamOutput(
            name="try-valkey", state=OutputState.VERIFIED,
            detail="Try Valkey upload confirmed by the run's upload sentinel",
            url=build.url,
        )
    if _release_is_latest(gh_source, repo_name, tag):
        return DownstreamOutput(
            name="try-valkey", state=OutputState.FAILED,
            detail="Try Valkey evidence is missing for the latest release",
            url=build.url,
        )
    return DownstreamOutput(
        name="try-valkey", state=OutputState.SKIPPED,
        detail="No upload sentinel on the run: Try Valkey tracks only the "
               "latest release and this release is provably not it",
        url=build.url,
    )


def _release_is_latest(gh_source: Any, repo_name: str, tag: str) -> bool:
    """Whether *tag* is (or must be presumed) the repository's latest release.

    The same comparison the publish path's latest-release decision uses,
    reimplemented locally against gh_source (verify must not import
    publish). Fails closed: when the latest release cannot be read or its
    tag cannot be compared, the release is treated as the latest, so a
    missing Try Valkey upload reads FAILED rather than silently settling
    as SKIPPED.
    """
    try:
        repo = get_repo(gh_source, repo_name)
        latest = retry_github_call(
            repo.get_latest_release,
            retries=2, description="get latest release",
        )
    except GithubException:
        # 404 means no latest release exists, so this one is it; any other
        # API failure is an inconclusive comparison, which fails closed.
        return True
    try:
        return parse_version(tag) >= parse_version(latest.tag_name or "")
    except ValueError:
        return True


def _run_has_artifact(run: Any, prefix: str) -> bool:
    artifacts = retry_github_call(
        lambda: list(run.get_artifacts()),
        retries=2, description=f"list artifacts of run {run.id}",
    )
    return any(a.name.startswith(prefix) and not a.expired for a in artifacts)


def _verify_tarballs(down: Any, tag: str) -> DownstreamOutput:
    """Every tarball and its .sha256 must answer publicly on downloads."""
    bases = [
        f"{down.downloads_base_url}/valkey-{tag}-{target.replace('/', '-')}.tar.gz"
        for target in down.tarball_targets
    ]
    missing = [
        url.rsplit("/", 1)[-1]
        for base in bases
        for url in (base, f"{base}.sha256")
        if not pub.url_exists(url)
    ]
    if missing:
        return DownstreamOutput(
            name="tarballs", state=OutputState.PENDING,
            detail=f"Not yet public: {', '.join(missing)}",
        )
    return DownstreamOutput(
        name="tarballs", state=OutputState.VERIFIED,
        detail=f"All {len(down.tarball_targets)} tarballs and their hashes are public",
        url=f"{down.downloads_base_url}/",
    )


def _verify_hashes(gh: Any, down: Any, tag: str) -> DownstreamOutput:
    """The valkey-hashes README must record this version's tarball hash."""
    repo = get_repo(gh, down.hashes_repo)
    readme = read_text_file(repo, "README")
    if f"valkey-{tag}.tar.gz" in readme:
        return DownstreamOutput(
            name="hashes", state=OutputState.VERIFIED,
            detail=f"Hash line for valkey-{tag}.tar.gz recorded",
            url=f"https://github.com/{down.hashes_repo}",
        )
    return DownstreamOutput(
        name="hashes", state=OutputState.PENDING,
        detail=f"No hash line for valkey-{tag}.tar.gz yet",
    )


def _verify_container(gh: Any, down: Any, tag: str) -> list[DownstreamOutput]:
    """Container PR merged, then the image tags actually public on Docker Hub."""
    pr = _find_update_pr(gh, down.container_repo, f"update-{tag}")
    if pr is None:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.PENDING,
            detail=f"No update-{tag} PR on {down.container_repo} yet",
        )
    elif pr.merged_at is not None:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.VERIFIED,
            detail=f"PR #{pr.number} merged", url=pr.html_url,
        )
    elif pr.state == "closed":
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.FAILED,
            detail=f"PR #{pr.number} was closed without merging", url=pr.html_url,
        )
    else:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.PENDING,
            detail=f"PR #{pr.number} is open, awaiting merge", url=pr.html_url,
            attempt_started_at=pr.created_at,
        )

    checks = {
        f"docker.io/{down.dockerhub_repo}:{tag}{suffix}":
            pub.dockerhub_tag_exists(down.dockerhub_repo, f"{tag}{suffix}")
        for suffix in CONTAINER_TAG_SUFFIXES
    }
    # The variant tags feed the Bundle build and live on Docker Hub; the
    # cross-registry requirement (GHCR, ECR) applies to the bare tag.
    checks[f"ghcr.io/{down.ghcr_image_repo}:{tag}"] = pub.ghcr_tag_exists(
        down.ghcr_image_repo, tag)
    checks[f"public.ecr.aws/{down.ecr_namespace}/valkey:{tag}"] = (
        pub.ecr_public_tag_exists(f"{down.ecr_namespace}/valkey", tag))
    missing = [ref for ref, exists in checks.items() if not exists]
    if missing:
        images = DownstreamOutput(
            name="container-images", state=OutputState.PENDING,
            detail=f"Not yet public: {', '.join(missing)}",
        )
    else:
        images = DownstreamOutput(
            name="container-images", state=OutputState.VERIFIED,
            detail=f"Tag {tag} is public in Docker Hub (including "
                   f"-trixie/-alpine), GHCR, and ECR",
            url=f"https://hub.docker.com/r/{down.dockerhub_repo}/tags?name={tag}",
        )
    return [pr_output, images]


def _verify_docs(gh: Any, down: Any, tag: str, stage: str) -> DownstreamOutput:
    """Docs: rc skips; a patch pushes a tag; a minor opens a docs PR."""
    if stage != "ga":
        return DownstreamOutput(
            name="docs", state=OutputState.SKIPPED,
            detail="Documentation is not updated for release candidates",
        )
    _major, _minor, patch = parse_version(tag)  # ga only: tag == version
    repo = get_repo(gh, down.doc_repo)
    if patch > 0:
        tag_sha = resolve_tag_commit(repo, tag)
        if not tag_sha:
            return DownstreamOutput(
                name="docs", state=OutputState.PENDING,
                detail=f"Docs tag {tag} has not been pushed yet",
            )
        # The tag alone is metadata anyone can push; the update-valkey-doc
        # flow lands its commit on the default branch, so the tag's commit
        # must be reachable there for the tag to be release evidence.
        if not _commit_on_default_branch(repo, tag_sha):
            return DownstreamOutput(
                name="docs", state=OutputState.FAILED,
                detail=f"Docs tag {tag} exists but its commit is not "
                       f"reachable on the default branch, so it is not the "
                       f"release flow's update",
                url=f"https://github.com/{down.doc_repo}/releases/tag/{tag}",
            )
        return DownstreamOutput(
            name="docs", state=OutputState.VERIFIED,
            detail=f"Docs tag {tag} exists and its commit is on the default "
                   f"branch. Repo evidence only; the public docs deployment "
                   f"is not verified",
            url=f"https://github.com/{down.doc_repo}/releases/tag/{tag}",
        )
    pr = _find_update_pr(gh, down.doc_repo, f"update-docs-{tag}")
    return _pr_progress_output("docs", pr, f"update-docs-{tag}", down.doc_repo)


def _verify_website(gh: Any, down: Any, tag: str, stage: str) -> DownstreamOutput:
    if stage != "ga":
        return DownstreamOutput(
            name="website", state=OutputState.SKIPPED,
            detail="The website release page is not updated for release candidates",
        )
    pr = _find_update_pr(gh, down.website_repo, f"update-website-{tag}")
    return _pr_progress_output("website", pr, f"update-website-{tag}", down.website_repo)


# ---------------------------------------------------------------------------
# Stage 6 verifiers (dependency ordered)


def _verify_bundle(
    gh: Any, down: Any, version: str, tag: str, *, images_public: bool,
    published_at: Any = None,
) -> DownstreamOutput:
    """Bundle: applicable for lines >= 8.1; blocked until base images public;
    verified only when versions.json records the version AND the bundle
    image for that line is public in all three registries."""
    major, minor, _patch = parse_version(version)
    line = f"{major}.{minor}"
    if (major, minor) < (8, 1):
        return DownstreamOutput(
            name="bundle", state=OutputState.SKIPPED,
            detail=f"The `valkey-bundle` repo does not track the {line} line",
        )
    if not images_public:
        return DownstreamOutput(
            name="bundle", state=OutputState.BLOCKED,
            detail="Waiting for the Valkey base images to be public "
                   "(the bundle builds FROM the -trixie/-alpine tags)",
        )

    repo = get_repo(gh, down.bundle_repo)
    versions_raw = read_text_file(repo, "versions.json")
    try:
        versions = json.loads(versions_raw)
    except json.JSONDecodeError:
        versions = None
    # Shape-validate every access: dict.get(key, {}) returns None (not the
    # default) when the key exists with a null value, and a scalar where a
    # mapping belongs would raise through _guarded (which only catches
    # GithubException) and abort the pass. Malformed downstream data is
    # this output failing, not a reconcile abort.
    parse_failed = DownstreamOutput(
        name="bundle", state=OutputState.FAILED,
        detail=f"Could not parse `versions.json` in {down.bundle_repo}",
    )
    if not isinstance(versions, dict):
        return parse_failed
    line_info = versions.get(line, {})
    if not isinstance(line_info, dict):
        return parse_failed
    server_info = line_info.get("valkey-server", {})
    if not isinstance(server_info, dict):
        return parse_failed
    recorded = server_info.get("version")
    if recorded is not None and not isinstance(recorded, str):
        return parse_failed
    if recorded != tag:  # versions.json records the tag form for rc releases
        # The bundle flow reuses one fixed head branch across releases, so
        # the branch alone cannot correlate a PR to THIS release: the PR
        # must carry the exact tag (title or head ref) and postdate this
        # release's publication. An older release's leftover PR (open or
        # closed) is simply not this release's evidence: with no matching
        # PR the not-started path holds and the dispatch can proceed.
        pr = _find_update_pr(gh, down.bundle_repo, _BUNDLE_UPDATE_BRANCH,
                             must_reference=tag, created_after=published_at)
        in_flight = _pr_in_flight(
            "bundle", pr, "Bundle update PR",
            open_suffix=f" (`versions.json` still records "
                        f"{recorded or 'nothing'} for {line})",
            closed_recovery="Re-dispatch the bundle update manually if the "
                            "closure was unrelated.")
        if in_flight is not None:
            return in_flight
        # Images are public, versions.json is stale, and no update PR is in
        # flight: the ordering gate is satisfied and the dispatch is safe.
        return DownstreamOutput(
            name="bundle", state=OutputState.PENDING,
            detail=f"`versions.json` records {recorded or 'nothing'} for {line}; "
                   f"the bundle update has not started yet",
            action="dispatch-bundle",
        )

    bundle_version = line_info.get("version")
    if not isinstance(bundle_version, str) or not bundle_version:
        # An empty or missing bundle version must never reach the registry
        # probes: Docker Hub answers 200 on the bare tags/ list URL, so an
        # empty tag would verify vacuously.
        return parse_failed
    missing = [
        registry for registry, exists in (
            ("Docker Hub", pub.dockerhub_tag_exists(down.bundle_dockerhub_repo, bundle_version)),
            ("GHCR", pub.ghcr_tag_exists(down.bundle_repo, bundle_version)),
            # ECR repo mirrors the GitHub repo name.
            ("ECR", pub.ecr_public_tag_exists(
                f"{down.ecr_namespace}/{down.bundle_repo.split('/')[1]}", bundle_version)),
        ) if not exists
    ]
    if missing:
        return DownstreamOutput(
            name="bundle", state=OutputState.PENDING,
            detail=f"Bundle {bundle_version} is merged but not yet public in: "
                   f"{', '.join(missing)}",
        )
    return DownstreamOutput(
        name="bundle", state=OutputState.VERIFIED,
        detail=f"Bundle {bundle_version} is public in Docker Hub, GHCR, and ECR",
        url=f"https://hub.docker.com/r/{down.bundle_dockerhub_repo}/tags?name={bundle_version}",
    )


def _verify_helm(
    gh: Any, down: Any, version: str, stage: str, *, image_public: bool,
) -> DownstreamOutput:
    """Helm: GA only, and only when the version advances the chart's
    appVersion; blocked until the chart's default image tag is public."""
    if stage != "ga":
        return DownstreamOutput(
            name="helm", state=OutputState.SKIPPED,
            detail="The chart does not track release candidates",
        )

    repo = get_repo(gh, down.helm_repo)
    chart_yaml = read_text_file(repo, "valkey/Chart.yaml")
    app_match = CHART_APP_VERSION_RE.search(chart_yaml)
    chart_match = CHART_VERSION_RE.search(chart_yaml)
    if app_match is None or chart_match is None:
        return DownstreamOutput(
            name="helm", state=OutputState.FAILED,
            detail="Could not parse appVersion/version from valkey/Chart.yaml",
        )
    app_version, chart_version = app_match.group(1), chart_match.group(1)

    if app_version != version:
        # CHART_APP_VERSION_RE accepts forms parse_version rejects (e.g.
        # "9.1"); malformed downstream data is this output failing, not a
        # reconcile abort (_guarded only catches GithubException).
        try:
            chart_is_newer = parse_version(app_version) > parse_version(version)
        except ValueError:
            return DownstreamOutput(
                name="helm", state=OutputState.FAILED,
                detail=f"Could not parse `valkey/Chart.yaml` in {down.helm_repo}",
            )
        if chart_is_newer:
            return DownstreamOutput(
                name="helm", state=OutputState.SKIPPED,
                detail=f"The chart already tracks the newer {app_version}",
            )
        if not image_public:
            return DownstreamOutput(
                name="helm", state=OutputState.BLOCKED,
                detail=f"Waiting for docker.io/{down.dockerhub_repo}:{version} "
                       f"to be public (the chart's default image)",
            )
        pr = _find_update_pr(gh, down.helm_repo, helm_update_branch(version))
        in_flight = _pr_in_flight(
            "helm", pr, "Chart bump PR",
            open_suffix=", awaiting merge",
            closed_recovery="Re-open it or bump the chart manually if the "
                            "closure was unrelated.")
        if in_flight is not None:
            return in_flight
        # Image public, chart stale, no PR in flight: safe to open the bump PR.
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"The chart appVersion is {app_version}; the bump to "
                   f"{version} has not started yet",
            action="open-helm-pr",
        )

    # appVersion matches: verify the chart actually published.
    chart_tag = f"valkey-{chart_version}"
    if not _tag_exists(repo, chart_tag):
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"Chart {chart_version} is merged but release {chart_tag} "
                   f"has not been cut yet",
        )
    if not pub.ghcr_tag_exists(f"{down.helm_repo}/valkey", chart_version):
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"Chart release {chart_tag} exists but the GHCR OCI chart "
                   f"{chart_version} is not public yet",
            url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
        )
    listed = _chart_listed_in_index(pub.fetch_text(down.helm_index_url), chart_version)
    if listed is None:
        return DownstreamOutput(
            name="helm", state=OutputState.FAILED,
            detail=f"Could not parse the public chart index ({down.helm_index_url})",
        )
    if not listed:
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"Chart {chart_version} is not yet listed in the public "
                   f"index ({down.helm_index_url})",
            url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
        )
    return DownstreamOutput(
        name="helm", state=OutputState.VERIFIED,
        detail=f"Chart {chart_version} (appVersion {version}) is released, in "
               f"the public index, and on GHCR",
        url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
    )


def helm_update_branch(version: str) -> str:
    """The controller's head branch for the Helm chart bump PR."""
    return f"agent/release-controller/valkey-{version}"


def _chart_listed_in_index(index_text: str, chart_version: str) -> "bool | None":
    """Whether the public index lists *chart_version* under the valkey chart.

    Scoped to ``entries['valkey']``: a matching version under a different
    chart (e.g. valkey-bundle) must never satisfy the valkey chart. Returns
    None when the index cannot be parsed (malformed YAML, missing entries
    mapping), which the caller degrades to the output failing. An empty
    index is simply not-listed-yet, not a parse failure.
    """
    if not index_text.strip():
        return False
    try:
        index = yaml.safe_load(index_text)
    except yaml.YAMLError:
        return None
    if not isinstance(index, dict) or not isinstance(index.get("entries"), dict):
        return None
    charts = index["entries"].get("valkey", [])
    if not isinstance(charts, list):
        return None
    return any(
        isinstance(entry, dict) and entry.get("version") == chart_version
        for entry in charts
    )


# ---------------------------------------------------------------------------
# Shared helpers


def _find_update_pr(gh: Any, repo_name: str, head_branch: str, *,
                    must_reference: str = "", created_after: Any = None) -> Any:
    """Newest PR in *repo_name* whose head branch is *head_branch* and that
    is actually bound to this release.

    Two bindings always apply: the PR's base must be the target repo's
    default branch (a PR retargeted at a side branch never lands the
    update), and the PR must change at least one file (an empty PR is not
    update evidence). ``must_reference``, when set, additionally requires
    the PR title or head ref to carry that exact token (boundary-anchored):
    needed for flows that reuse one fixed head branch across releases.
    ``created_after``, when set, ignores PRs created before it, so an
    earlier release's leftover PR can never be read as this release's.
    """
    repo = get_repo(gh, repo_name)
    owner = repo_name.split("/", maxsplit=1)[0]
    pulls = retry_github_call(
        lambda: list(repo.get_pulls(state="all", head=f"{owner}:{head_branch}",
                                    sort="created", direction="desc")),
        retries=2, description=f"list {head_branch} PRs on {repo_name}",
    )
    pattern = _exact_token_re(must_reference) if must_reference else None
    for pr in pulls:
        if pr.base.ref != repo.default_branch:
            continue
        if not pr.changed_files:
            continue
        if created_after is not None and pr.created_at < created_after:
            continue
        if pattern is not None and not (
            pattern.search(pr.title or "") or pattern.search(pr.head.ref or "")
        ):
            continue
        return pr
    return None


def _exact_token_re(token: str) -> "re.Pattern[str]":
    """Boundary-anchored match for *token*: 9.1.2 never matches 9.1.20 or
    9.1.2-rc1 (the same anchoring the run-marker scan uses)."""
    return re.compile(rf"(?<![\w.]){re.escape(token)}(?![\w.-])")


def _commit_on_default_branch(repo: Any, sha: str) -> bool:
    """True when *sha* is the repo's default branch head or an ancestor of
    it, i.e. the commit actually landed on the branch."""
    comparison = retry_github_call(
        lambda: repo.compare(sha, repo.default_branch),
        retries=2, description=f"compare {str(sha)[:12]}...{repo.default_branch}",
    )
    return comparison.status in ("identical", "ahead")


def _pr_in_flight(name: str, pr: Any, label: str, *, open_suffix: str,
                  closed_recovery: str) -> "DownstreamOutput | None":
    """The Bundle/Helm in-flight PR states: open (PENDING, or FAILED when
    its checks are red) and closed without merging.

    A human closing the update PR is a decision, not a retry condition:
    re-dispatching every reconcile pass (or force-resetting the branch
    under their feet) would reopen the fight, so it reads FAILED with no
    auto-action and *closed_recovery* names the manual path. None when
    there is no in-flight decision to report (no PR, or a merged one).
    """
    if pr is None:
        return None
    if pr.state == "open":
        if _pr_checks_failing(pr):
            return DownstreamOutput(
                name=name, state=OutputState.FAILED,
                detail=f"{label} #{pr.number} is open with failing checks",
                url=pr.html_url,
            )
        return DownstreamOutput(
            name=name, state=OutputState.PENDING,
            detail=f"{label} #{pr.number} is open{open_suffix}",
            url=pr.html_url,
            attempt_started_at=pr.created_at,
        )
    if pr.merged_at is None:
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"{label} #{pr.number} was closed without merging and "
                   f"needs a human decision. {closed_recovery}",
            url=pr.html_url,
        )
    return None


def _pr_progress_output(name: str, pr: Any, branch: str, repo_name: str) -> DownstreamOutput:
    if pr is None:
        return DownstreamOutput(
            name=name, state=OutputState.PENDING,
            detail=f"No {branch} PR on {repo_name} yet",
        )
    if pr.merged_at is not None:
        # The policy carries no public URL for the docs/website content
        # itself, so a merged PR is the strongest evidence available here;
        # the detail says so rather than implying a deployment check.
        return DownstreamOutput(
            name=name, state=OutputState.VERIFIED,
            detail=f"PR #{pr.number} merged. Merged PR evidence only; the "
                   f"public deployment is not verified", url=pr.html_url,
        )
    if pr.state == "closed":
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"PR #{pr.number} was closed without merging", url=pr.html_url,
        )
    if _pr_checks_failing(pr):
        # An open PR whose CI is red will not merge on its own; leaving it
        # pending keeps it out of the notification path indefinitely.
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"PR #{pr.number} is open with failing checks; it needs a "
                   f"fix before it can merge", url=pr.html_url,
        )
    return DownstreamOutput(
        name=name, state=OutputState.PENDING,
        detail=f"PR #{pr.number} is open, awaiting merge", url=pr.html_url,
        attempt_started_at=pr.created_at,
    )


def _pr_checks_failing(pr: Any) -> bool:
    """True when any completed check on the PR's head commit failed."""
    commit = retry_github_call(
        lambda: pr.base.repo.get_commit(pr.head.sha),
        retries=2, description=f"get PR #{pr.number} head commit",
    )
    runs = retry_github_call(
        lambda: list(commit.get_check_runs()),
        retries=2, description=f"list checks on PR #{pr.number} head",
    )
    # "cancelled" included: a cancelled required check leaves the PR
    # unmergeable just like a failure.
    return any(
        run.status == "completed"
        and run.conclusion in ("failure", "timed_out", "action_required", "cancelled")
        for run in runs
    )


def _tag_exists(repo: Any, tag: str) -> bool:
    return bool(resolve_tag_commit(repo, tag))
