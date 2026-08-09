"""Verify the release's required public outputs (stages 5 and 6).

Every verifier checks the *canonical public artifact* — a registry manifest,
a public file, a merged PR, a git tag — not merely that some workflow
reported success. "PR opened" and "dispatch accepted" are progress states,
never completion.

Ordering is encoded as BLOCKED states: Bundle work is blocked until the
Valkey container images are public, and Helm until the chart's image tag is
public, so downstream work that must not start early is visibly *blocked*
rather than silently absent.

All verifiers are read-only. Side effects (dispatching Bundle, opening the
Helm PR, notifications) live in :mod:`scripts.release.actions`.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release import public_endpoints as pub
from scripts.release.models import DownstreamOutput, OutputState
from scripts.release.policy import RepoReleasePolicy
from scripts.release.qualification import RUN_SCAN_LIMIT
from scripts.release.release_refs import read_text_file, resolve_tag_commit, workflow_handle
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
    build_result = _guarded("build-run", lambda: _verify_build_run(
        gh, gh_source, policy, tag, published_at))
    build, build_run = (build_result if isinstance(build_result, tuple)
                        else (build_result, None))
    # The run object and its jobs are resolved once and shared by the
    # packages and Try Valkey verifiers (previously three fetches per pass).
    jobs = _build_run_jobs(build, build_run)
    outputs = [
        build,
        _guarded("tarballs", lambda: _verify_tarballs(down, tag)),
        _guarded("packages", lambda: _verify_packages(stage, build, jobs)),
        _guarded("try-valkey", lambda: _verify_try_valkey(stage, build, build_run, jobs)),
        _guarded("hashes", lambda: _verify_hashes(gh, down, tag)),
    ]
    container = _guarded("container-pr", lambda: _verify_container(gh, down, tag))
    outputs.extend(container if isinstance(container, list) else [container])
    outputs.append(_guarded("docs", lambda: _verify_docs(gh, down, tag, stage)))
    outputs.append(_guarded("website", lambda: _verify_website(gh, down, tag, stage)))
    return tuple(outputs)


def verify_ordered_outputs(
    gh: Any, policy: RepoReleasePolicy, *, version: str, tag: str, stage: str,
    core: tuple[DownstreamOutput, ...],
) -> tuple[DownstreamOutput, ...]:
    """Stage-6 outputs: Bundle and Helm, gated on public base images."""
    images_public = any(
        output.name == "container-images" and output.state is OutputState.VERIFIED
        for output in core
    )
    return (
        _guarded("bundle", lambda: _verify_bundle(
            gh, policy.downstream, version, tag, images_public=images_public)),
        # container-images includes the bare tag (the chart's default image).
        _guarded("helm", lambda: _verify_helm(
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
    exempt: their prerequisite carries the escalation."""
    if published_at is None or not _past_deadline(published_at, timeout_minutes):
        return outputs
    return tuple(
        DownstreamOutput(
            name=o.name, state=OutputState.FAILED,
            detail=f"stalled: still pending {timeout_minutes} minutes after "
                   f"publication — {o.detail}",
            # action cleared: an escalated stall pages a human; it must not
            # also keep auto-dispatching every pass.
            url=o.url, action="", run_id=o.run_id,
        ) if o.state is OutputState.PENDING else o
        for o in outputs
    )


def _guarded(name: str, verifier: Callable[[], Any]) -> Any:
    """Degrade a verifier's API failure to a FAILED output.

    One missing or renamed downstream repository (a 404 on its first read)
    must report as that output failing — feeding the checklist and the
    one-shot notification — rather than aborting the whole reconcile pass
    and freezing the tracker at stale state.
    """
    try:
        return verifier()
    except GithubException as exc:
        logger.warning("Verifier %s failed: HTTP %s", name, exc.status)
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"verification failed against GitHub (HTTP {exc.status}); "
                   f"is the target repository present and readable?",
        )


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
      must carry the exact ``Build Release <tag> (prod)`` marker — a dev
      dispatch or unrelated repository_dispatch run can never satisfy it;
    - absence is bounded: no matching run within the policy check timeout of
      publication is a failure, not an eternal pending.
    """
    down = policy.downstream
    # The build run is looked for FIRST: a failed trigger must never veto a
    # build that exists (recovery may dispatch build-release directly, or
    # re-run the trigger — either way, a marked build run supersedes the
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
            return DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail=f"the release trigger run concluded {trigger.conclusion} "
                       f"before dispatching the build; re-run it (or dispatch "
                       f"build-release for {tag} directly)",
                url=trigger.html_url,
            ), None
        if _past_deadline(published_at, policy.check_timeout_minutes):
            return DownstreamOutput(
                name="build-run", state=OutputState.FAILED,
                detail=f"no '{marker}' run appeared within "
                       f"{policy.check_timeout_minutes} minutes of publication; "
                       f"the dispatch chain needs investigation",
            ), None
        return DownstreamOutput(
            name="build-run", state=OutputState.PENDING,
            detail=f"no '{marker}' run found yet",
        ), None
    if run.status != "completed":
        return DownstreamOutput(
            name="build-run", state=OutputState.PENDING,
            detail=f"build-release run {run.id} still executing", url=run.html_url,
        ), run
    if run.conclusion == "success":
        return DownstreamOutput(
            name="build-run", state=OutputState.VERIFIED,
            detail=f"build-release run {run.id} succeeded", url=run.html_url,
            run_id=run.id,
        ), run
    return DownstreamOutput(
        name="build-run", state=OutputState.FAILED,
        detail=f"build-release run {run.id} concluded {run.conclusion}",
        url=run.html_url,
    ), run


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
    pattern = re.compile(rf"(?<![\w.]){re.escape(marker)}(?![\w.-])")
    for index, run in enumerate(runs):
        if index >= RUN_SCAN_LIMIT:
            break
        if published_at is not None and run.created_at < published_at:
            continue
        # Boundary-anchored: the bare-tag trigger marker for 9.1.2 must not
        # match a 9.1.20 run title, and (the trailing '-') the GA marker
        # 9.1.2 must never match a 9.1.2-rc1 title. (Release-event trigger
        # runs title as the tag itself — GitHub display-title behavior, not
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


def _verify_packages(stage: str, build: DownstreamOutput, jobs: Any) -> DownstreamOutput:
    """RPM/DEB publication (GA only), evidenced by the build run's publish
    and pages jobs succeeding.

    Job-level evidence rather than a public endpoint: the package repos'
    public layout is not a stable URL contract, and the plan explicitly
    allows the authoritative publish workflow as the v1 canonical signal.
    """
    if stage != "ga":
        return DownstreamOutput(
            name="packages", state=OutputState.SKIPPED,
            detail="distro packages are not built for release candidates",
        )
    if build.state is not OutputState.VERIFIED:
        return DownstreamOutput(
            name="packages", state=OutputState.BLOCKED,
            detail="waiting for the build-release run to succeed",
        )
    if jobs is None:
        return DownstreamOutput(
            name="packages", state=OutputState.FAILED,
            detail="could not list the build run's jobs", url=build.url,
        )
    # Job names set by
    # valkey-release-automation/.github/workflows/build-release.yml.
    publish_jobs = [j for j in jobs if "Publish to S3" in j.name or "Deploy Pages" in j.name]
    if not publish_jobs:
        return DownstreamOutput(
            name="packages", state=OutputState.FAILED,
            detail="the build run has no package publish jobs; the matrix "
                   "may not have run", url=build.url,
        )
    failed = [j.name for j in publish_jobs if j.conclusion != "success"]
    if failed:
        return DownstreamOutput(
            name="packages", state=OutputState.FAILED,
            detail=f"package publication jobs failed: {', '.join(failed)}",
            url=build.url,
        )
    return DownstreamOutput(
        name="packages", state=OutputState.VERIFIED,
        detail="RPM/DEB publish and pages jobs succeeded", url=build.url,
    )


def _verify_try_valkey(stage: str, build: DownstreamOutput, run: Any,
                       jobs: Any) -> DownstreamOutput:
    """Try Valkey upload, evidenced by the build run's update-try-valkey job.

    The job itself skips for release candidates and for versions that are
    not the latest release; a skipped job is therefore not-applicable, not
    a failure.
    """
    if stage != "ga":
        return DownstreamOutput(
            name="try-valkey", state=OutputState.SKIPPED,
            detail="Try Valkey is not updated for release candidates",
        )
    if build.state is not OutputState.VERIFIED:
        return DownstreamOutput(
            name="try-valkey", state=OutputState.BLOCKED,
            detail="waiting for the build-release run to succeed",
        )
    if jobs is None:
        return DownstreamOutput(
            name="try-valkey", state=OutputState.FAILED,
            detail="could not list the build run's jobs", url=build.url,
        )
    try_jobs = [j for j in jobs if "try-valkey" in j.name.lower() or "try valkey" in j.name.lower()]
    if not try_jobs:
        return DownstreamOutput(
            name="try-valkey", state=OutputState.FAILED,
            detail="the build run has no Try Valkey job", url=build.url,
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
    # (and only when) the upload happened; its absence on a green job means
    # the upload was intentionally skipped. Sentinel name set by
    # valkey-release-automation/.github/workflows/build-release.yml.
    if _run_has_artifact(run, "try-valkey-uploaded"):
        return DownstreamOutput(
            name="try-valkey", state=OutputState.VERIFIED,
            detail="Try Valkey upload confirmed by the run's upload sentinel",
            url=build.url,
        )
    return DownstreamOutput(
        name="try-valkey", state=OutputState.SKIPPED,
        detail="no upload sentinel on the run: Try Valkey was intentionally "
               "skipped (release candidate or not the latest release)",
        url=build.url,
    )


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
            detail=f"not yet public: {', '.join(missing)}",
        )
    return DownstreamOutput(
        name="tarballs", state=OutputState.VERIFIED,
        detail=f"all {len(down.tarball_targets)} tarballs + hashes public",
        url=f"{down.downloads_base_url}/",
    )


def _verify_hashes(gh: Any, down: Any, tag: str) -> DownstreamOutput:
    """The valkey-hashes README must record this version's tarball hash."""
    repo = retry_github_call(
        lambda: gh.get_repo(down.hashes_repo),
        retries=2, description=f"get repo {down.hashes_repo}",
    )
    readme = read_text_file(repo, "README")
    if f"valkey-{tag}.tar.gz" in readme:
        return DownstreamOutput(
            name="hashes", state=OutputState.VERIFIED,
            detail=f"hash line for valkey-{tag}.tar.gz recorded",
            url=f"https://github.com/{down.hashes_repo}",
        )
    return DownstreamOutput(
        name="hashes", state=OutputState.PENDING,
        detail=f"no hash line for valkey-{tag}.tar.gz yet",
    )


def _verify_container(gh: Any, down: Any, tag: str) -> list[DownstreamOutput]:
    """Container PR merged, then the image tags actually public on Docker Hub."""
    pr = _find_update_pr(gh, down.container_repo, f"update-{tag}")
    if pr is None:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.PENDING,
            detail=f"no update-{tag} PR on {down.container_repo} yet",
        )
    elif pr.merged_at is not None:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.VERIFIED,
            detail=f"PR #{pr.number} merged", url=pr.html_url,
        )
    elif pr.state == "closed":
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.FAILED,
            detail=f"PR #{pr.number} closed without merging", url=pr.html_url,
        )
    else:
        pr_output = DownstreamOutput(
            name="container-pr", state=OutputState.PENDING,
            detail=f"PR #{pr.number} open, awaiting merge", url=pr.html_url,
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
            detail=f"not yet public: {', '.join(missing)}",
        )
    else:
        images = DownstreamOutput(
            name="container-images", state=OutputState.VERIFIED,
            detail=f"{tag} public in Docker Hub (incl. -trixie/-alpine), GHCR, and ECR",
            url=f"https://hub.docker.com/r/{down.dockerhub_repo}/tags?name={tag}",
        )
    return [pr_output, images]


def _verify_docs(gh: Any, down: Any, tag: str, stage: str) -> DownstreamOutput:
    """Docs: rc skips; a patch pushes a tag; a minor opens a docs PR."""
    if stage != "ga":
        return DownstreamOutput(
            name="docs", state=OutputState.SKIPPED,
            detail="documentation is not updated for release candidates",
        )
    _major, _minor, patch = parse_version(tag)  # ga only: tag == version
    repo = retry_github_call(
        lambda: gh.get_repo(down.doc_repo),
        retries=2, description=f"get repo {down.doc_repo}",
    )
    if patch > 0:
        if _tag_exists(repo, tag):
            return DownstreamOutput(
                name="docs", state=OutputState.VERIFIED,
                detail=f"docs tag {tag} exists",
                url=f"https://github.com/{down.doc_repo}/releases/tag/{tag}",
            )
        return DownstreamOutput(
            name="docs", state=OutputState.PENDING,
            detail=f"docs tag {tag} not pushed yet",
        )
    pr = _find_update_pr(gh, down.doc_repo, f"update-docs-{tag}")
    return _pr_progress_output("docs", pr, f"update-docs-{tag}", down.doc_repo)


def _verify_website(gh: Any, down: Any, tag: str, stage: str) -> DownstreamOutput:
    if stage != "ga":
        return DownstreamOutput(
            name="website", state=OutputState.SKIPPED,
            detail="the website release page is not updated for release candidates",
        )
    pr = _find_update_pr(gh, down.website_repo, f"update-website-{tag}")
    return _pr_progress_output("website", pr, f"update-website-{tag}", down.website_repo)


# ---------------------------------------------------------------------------
# Stage 6 verifiers (dependency ordered)


def _verify_bundle(
    gh: Any, down: Any, version: str, tag: str, *, images_public: bool,
) -> DownstreamOutput:
    """Bundle: applicable for lines >= 8.1; blocked until base images public;
    verified only when versions.json records the version AND the bundle
    image for that line is public in all three registries."""
    major, minor, _patch = parse_version(version)
    line = f"{major}.{minor}"
    if (major, minor) < (8, 1):
        return DownstreamOutput(
            name="bundle", state=OutputState.SKIPPED,
            detail=f"valkey-bundle does not track the {line} line",
        )
    if not images_public:
        return DownstreamOutput(
            name="bundle", state=OutputState.BLOCKED,
            detail="waiting for the Valkey base images to be public "
                   "(bundle builds FROM the -trixie/-alpine tags)",
        )

    repo = retry_github_call(
        lambda: gh.get_repo(down.bundle_repo),
        retries=2, description=f"get repo {down.bundle_repo}",
    )
    versions_raw = read_text_file(repo, "versions.json")
    try:
        versions = json.loads(versions_raw)
    except json.JSONDecodeError:
        versions = None
    if not isinstance(versions, dict):
        # Malformed downstream data is this output failing, not a reconcile
        # abort (_guarded only catches GithubException).
        return DownstreamOutput(
            name="bundle", state=OutputState.FAILED,
            detail=f"could not parse `versions.json` in {down.bundle_repo}",
        )
    recorded = versions.get(line, {}).get("valkey-server", {}).get("version")
    if recorded != tag:  # versions.json records the tag form for rc releases
        pr = _find_update_pr(gh, down.bundle_repo, _BUNDLE_UPDATE_BRANCH)
        if pr is not None and pr.state == "open":
            if _pr_checks_failing(pr):
                return DownstreamOutput(
                    name="bundle", state=OutputState.FAILED,
                    detail=f"bundle update PR #{pr.number} is open with failing "
                           f"checks", url=pr.html_url,
                )
            return DownstreamOutput(
                name="bundle", state=OutputState.PENDING,
                detail=f"bundle update PR #{pr.number} open (versions.json still "
                       f"records {recorded or 'nothing'} for {line})",
                url=pr.html_url,
            )
        if pr is not None and pr.merged_at is None:
            # A human closed the update PR: that is a decision, not a retry
            # condition. Re-dispatching every reconcile pass would reopen
            # the fight; a human re-dispatches manually if the closure was
            # unrelated.
            return DownstreamOutput(
                name="bundle", state=OutputState.FAILED,
                detail=f"bundle update PR #{pr.number} was closed without "
                       f"merging; needs a human decision (re-dispatch the "
                       f"bundle update manually if the closure was unrelated)",
                url=pr.html_url,
            )
        # Images are public, versions.json is stale, and no update PR is in
        # flight: the ordering gate is satisfied and the dispatch is safe.
        return DownstreamOutput(
            name="bundle", state=OutputState.PENDING,
            detail=f"versions.json records {recorded or 'nothing'} for {line}; "
                   f"bundle update not started yet",
            action="dispatch-bundle",
        )

    bundle_version = versions.get(line, {}).get("version", "")
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
            detail=f"bundle {bundle_version} merged but not yet public in: "
                   f"{', '.join(missing)}",
        )
    return DownstreamOutput(
        name="bundle", state=OutputState.VERIFIED,
        detail=f"bundle {bundle_version} public in Docker Hub, GHCR, and ECR",
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
            detail="the chart does not track release candidates",
        )

    repo = retry_github_call(
        lambda: gh.get_repo(down.helm_repo),
        retries=2, description=f"get repo {down.helm_repo}",
    )
    chart_yaml = read_text_file(repo, "valkey/Chart.yaml")
    app_match = CHART_APP_VERSION_RE.search(chart_yaml)
    chart_match = CHART_VERSION_RE.search(chart_yaml)
    if app_match is None or chart_match is None:
        return DownstreamOutput(
            name="helm", state=OutputState.FAILED,
            detail="could not parse appVersion/version from valkey/Chart.yaml",
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
                detail=f"could not parse `valkey/Chart.yaml` in {down.helm_repo}",
            )
        if chart_is_newer:
            return DownstreamOutput(
                name="helm", state=OutputState.SKIPPED,
                detail=f"chart already tracks the newer {app_version}",
            )
        if not image_public:
            return DownstreamOutput(
                name="helm", state=OutputState.BLOCKED,
                detail=f"waiting for docker.io/{down.dockerhub_repo}:{version} "
                       f"to be public (the chart's default image)",
            )
        pr = _find_update_pr(gh, down.helm_repo, helm_update_branch(version))
        if pr is not None and pr.state == "open":
            if _pr_checks_failing(pr):
                return DownstreamOutput(
                    name="helm", state=OutputState.FAILED,
                    detail=f"chart bump PR #{pr.number} is open with failing "
                           f"checks", url=pr.html_url,
                )
            return DownstreamOutput(
                name="helm", state=OutputState.PENDING,
                detail=f"chart bump PR #{pr.number} open, awaiting merge",
                url=pr.html_url,
            )
        if pr is not None and pr.merged_at is None:
            # A closed bump PR is a human decision; never reopen it
            # automatically (which would also force-reset the branch under
            # their feet).
            return DownstreamOutput(
                name="helm", state=OutputState.FAILED,
                detail=f"chart bump PR #{pr.number} was closed without merging; "
                       f"needs a human decision",
                url=pr.html_url,
            )
        # Image public, chart stale, no PR in flight: safe to open the bump PR.
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"chart appVersion is {app_version}; bump to {version} not started yet",
            action="open-helm-pr",
        )

    # appVersion matches: verify the chart actually published.
    chart_tag = f"valkey-{chart_version}"
    if not _tag_exists(repo, chart_tag):
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"chart {chart_version} merged but release {chart_tag} not cut yet",
        )
    if not pub.ghcr_tag_exists(f"{down.helm_repo}/valkey", chart_version):
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"chart release {chart_tag} exists but the GHCR OCI chart "
                   f"{chart_version} is not public yet",
            url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
        )
    # Anchored to the line end: "version: 1.0.1" must not be satisfied by a
    # 1.0.10 entry in the index. The optional dash is the YAML list item.
    index_entry = re.compile(
        rf"^\s*(?:- )?version:\s*{re.escape(chart_version)}\s*$", re.MULTILINE)
    if not index_entry.search(pub.fetch_text(down.helm_index_url)):
        return DownstreamOutput(
            name="helm", state=OutputState.PENDING,
            detail=f"chart {chart_version} not yet listed in the public index "
                   f"({down.helm_index_url})",
            url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
        )
    return DownstreamOutput(
        name="helm", state=OutputState.VERIFIED,
        detail=f"chart {chart_version} (appVersion {version}) released, in the "
               f"public index, and on GHCR",
        url=f"https://github.com/{down.helm_repo}/releases/tag/{chart_tag}",
    )


def helm_update_branch(version: str) -> str:
    """The controller's head branch for the Helm chart bump PR."""
    return f"agent/release-controller/valkey-{version}"


# ---------------------------------------------------------------------------
# Shared helpers


def _find_update_pr(gh: Any, repo_name: str, head_branch: str) -> Any:
    """Newest PR (any state) in *repo_name* whose head branch is *head_branch*."""
    repo = retry_github_call(
        lambda: gh.get_repo(repo_name),
        retries=2, description=f"get repo {repo_name}",
    )
    owner = repo_name.split("/", maxsplit=1)[0]
    pulls = retry_github_call(
        lambda: list(repo.get_pulls(state="all", head=f"{owner}:{head_branch}",
                                    sort="created", direction="desc")),
        retries=2, description=f"list {head_branch} PRs on {repo_name}",
    )
    return pulls[0] if pulls else None


def _pr_progress_output(name: str, pr: Any, branch: str, repo_name: str) -> DownstreamOutput:
    if pr is None:
        return DownstreamOutput(
            name=name, state=OutputState.PENDING,
            detail=f"no {branch} PR on {repo_name} yet",
        )
    if pr.merged_at is not None:
        return DownstreamOutput(
            name=name, state=OutputState.VERIFIED,
            detail=f"PR #{pr.number} merged", url=pr.html_url,
        )
    if pr.state == "closed":
        return DownstreamOutput(
            name=name, state=OutputState.FAILED,
            detail=f"PR #{pr.number} closed without merging", url=pr.html_url,
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
        detail=f"PR #{pr.number} open, awaiting merge", url=pr.html_url,
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
