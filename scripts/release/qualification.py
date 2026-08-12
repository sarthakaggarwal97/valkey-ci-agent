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

import io
import json
import logging
import zipfile
from functools import partial
from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import QualificationStatus
from scripts.release.policy import RepoReleasePolicy

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
# (schema 1 JSON: nonce/version/tag/source_sha/automation_sha/job counts).
# Presence plus content is the whole check: F5 removed the "presence is
# evidence" shortcut, so a non-empty artifact whose bytes disagree with
# what was dispatched must not pass.
MANIFEST_ARTIFACT = "qualification-manifest"

# The schema version emitted by valkey-release-automation's
# .github/workflows/qualify-release.yml. A different value means the
# producer changed shape underneath us and its evidence must not count.
MANIFEST_SCHEMA_VERSION = 1

# Cap the manifest read to a size that can only ever be a small JSON file
# (the producer writes a handful of fields). A cap protects against a
# hostile or damaged upload that would otherwise pin runner memory.
_MAX_MANIFEST_BYTES = 32 * 1024

# Required manifest fields (schema 1). automation_sha is recorded into the
# evidence log rather than compared to a dispatch record: the controller
# does not yet pass a per-dispatch nonce, so nonce binding is deferred
# until the dispatch path can pin one and require it back in the manifest.
_MANIFEST_REQUIRED_FIELDS = (
    "schema", "nonce", "version", "tag", "source_sha", "automation_sha",
    "rpm_jobs", "deb_jobs", "archive_jobs",
)


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
    result = partial(QualificationStatus, run_id=run.id, url=run.html_url)

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
        return result(passed=False, failed_jobs=(STARTUP_FAILURE_JOB,))

    if run.status != "completed":
        return result(pending=True)

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
        return result(pending=True)
    # (j.name or ""): a job served with a null name must read as unnamed,
    # not raise through the caller and abort the pass.
    failed = tuple((j.name or "") for j in jobs
                   if j.conclusion not in ("success", "skipped"))
    if run.conclusion != "success" or failed:
        return result(
            passed=False,
            failed_jobs=failed or (f"(Run concluded: {run.conclusion})",),
        )
    gaps = _evidence_gaps(policy, run, jobs, tag=tag, sha=sha)
    if gaps:
        return result(passed=False, failed_jobs=gaps)
    return result(passed=True)


def _evidence_gaps(policy: RepoReleasePolicy, run: Any, jobs: list,
                   *, tag: str, sha: str) -> tuple[str, ...]:
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
        a for a in artifacts
        if not a.expired and a.size_in_bytes > 0
    ]
    usable_names = [a.name for a in usable]
    expected_archives = (down.qualification_x86_archive_jobs
                         + down.qualification_arm_archive_jobs)
    artifact_expectations = [("qualify-", expected_archives, "archive")]
    if is_ga:
        artifact_expectations += [
            ("valkey-rpms-", down.qualification_rpm_jobs, "RPM"),
            ("valkey-debs-", down.qualification_deb_jobs, "DEB"),
        ]
    for prefix, expected, label in artifact_expectations:
        present = sum(name.startswith(prefix) for name in usable_names)
        if present != expected:
            gaps.append(
                f"(Evidence mismatch: usable {label} artifacts, {present} "
                f"present, expected exactly {expected})"
            )
    # F5: the qualification manifest artifact is not just a name -- its
    # content must bind the run to the release identity that dispatched it.
    # A non-empty unexpired artifact whose bytes disagree with what was
    # dispatched (tag, source_sha, version, job counts) never passes.
    manifest_artifact = next(
        (a for a in usable if a.name == MANIFEST_ARTIFACT), None,
    )
    if manifest_artifact is None:
        gaps.append("(Evidence mismatch: no qualification manifest)")
    else:
        gaps.extend(_validate_manifest_content(
            manifest_artifact, policy=policy, tag=tag, sha=sha,
            expected_archive_jobs=expected_archives, is_ga=is_ga,
        ))
    return tuple(gaps)


def _validate_manifest_content(
    artifact: Any, *, policy: RepoReleasePolicy, tag: str, sha: str,
    expected_archive_jobs: int, is_ga: bool,
) -> list[str]:
    """Load the manifest JSON and require every dispatched field to match.

    Any download/parse/shape failure or field mismatch becomes an evidence
    mismatch naming what differed, so nothing about a hostile manifest
    (wrong SHA, wrong tag, wrong counts, malformed JSON, empty file) reads
    as passable evidence. ``automation_sha`` is logged as evidence detail;
    nonce binding to dispatch is deferred (the controller does not yet
    pass a per-dispatch nonce, so requiring it back would refuse every
    real run).
    """
    down = policy.downstream
    try:
        payload = _load_manifest_payload(artifact)
    except _ManifestReadError as exc:
        return [f"(Evidence mismatch: qualification manifest unreadable: {exc})"]

    gaps: list[str] = []
    if payload.get("schema") != MANIFEST_SCHEMA_VERSION:
        gaps.append(
            f"(Evidence mismatch: qualification manifest schema "
            f"{payload.get('schema')!r}, expected {MANIFEST_SCHEMA_VERSION})"
        )
    for field in _MANIFEST_REQUIRED_FIELDS:
        if field not in payload:
            gaps.append(
                f"(Evidence mismatch: qualification manifest missing "
                f"required field {field!r})"
            )
    if gaps:  # a malformed manifest cannot be trusted for further comparisons
        return gaps

    # The tag already carries the version+stage but the manifest emits
    # ``version`` separately (M.m.p, no stage suffix); binding it too
    # protects against a producer that renders the two independently.
    identity_bindings = (
        ("tag", payload["tag"], tag),
        ("source_sha", payload["source_sha"], sha),
        ("version", payload["version"], tag.split("-rc", 1)[0]),
    )
    for key, actual, expected in identity_bindings:
        if actual == expected:
            continue
        if key == "source_sha":
            # Truncated hashes so the operator can diff them at a glance.
            actual, expected = str(actual)[:12], expected[:12]
        gaps.append(
            f"(Evidence mismatch: qualification manifest {key} "
            f"{actual!r}, expected {expected!r})"
        )
    expected_rpm = down.qualification_rpm_jobs if is_ga else 0
    expected_deb = down.qualification_deb_jobs if is_ga else 0
    for key, expected_count in (("rpm_jobs", expected_rpm),
                                ("deb_jobs", expected_deb),
                                ("archive_jobs", expected_archive_jobs)):
        count = payload[key]
        # An int that happens to string-format to the same digits is a
        # different type, and the exact-count discipline forbids either
        # coercing it or reading it as a match.
        if not isinstance(count, int) or isinstance(count, bool) or count != expected_count:
            gaps.append(
                f"(Evidence mismatch: qualification manifest {key} "
                f"{count!r}, expected {expected_count})"
            )
    if not gaps:
        # automation_sha is retained as evidence detail (dispatch-nonce
        # binding is deferred; see MANIFEST comment above).
        logger.info(
            "Qualification manifest for %s @ %s validated (automation_sha=%s, "
            "nonce=%s)", tag, sha[:12],
            str(payload.get("automation_sha"))[:12],
            str(payload.get("nonce"))[:16],
        )
    return gaps


class _ManifestReadError(RuntimeError):
    """A qualification manifest artifact could not be downloaded or parsed."""


def _load_manifest_payload(artifact: Any) -> dict:
    """Download and parse the JSON payload from the manifest artifact zip.

    The producer uploads a single JSON file inside a zip. The download uses
    the Artifact's own requester (``artifact.requester``) so the GitHub
    token stays inside PyGithub's transport and does not leak to the signed
    blob URL PyGithub redirects to; tests inject content through that
    requester. The response body of a successful download is the zip bytes
    as a str (PyGithub decodes the raw response); str encodes back
    losslessly via latin-1 for zip parsing.
    """
    requester = getattr(artifact, "requester", None)
    if requester is None:
        raise _ManifestReadError(
            "artifact has no ``requester`` attribute; cannot download manifest"
        )
    status, _headers, body = requester.requestBlob(
        "GET", artifact.archive_download_url, None, None, None, None,
    )
    if not (200 <= int(status) < 300):
        raise _ManifestReadError(
            f"artifact download returned HTTP {status}"
        )
    if isinstance(body, str):
        body = body.encode("latin-1")
    elif not isinstance(body, bytes):
        raise _ManifestReadError(
            f"artifact download returned unexpected body type: {type(body).__name__}"
        )
    if not body:
        raise _ManifestReadError("artifact download returned no bytes")
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            members = [m for m in zf.infolist()
                       if not m.is_dir() and m.filename.lower().endswith(".json")]
            if not members:
                raise _ManifestReadError("zip contains no JSON file")
            # The producer uploads exactly one JSON file; if there are more
            # than one, refuse rather than guess which is authoritative.
            if len(members) > 1:
                raise _ManifestReadError(
                    f"zip contains {len(members)} JSON files, expected exactly one"
                )
            member = members[0]
            if member.file_size > _MAX_MANIFEST_BYTES:
                raise _ManifestReadError(
                    f"manifest declared size {member.file_size} exceeds "
                    f"cap {_MAX_MANIFEST_BYTES}"
                )
            with zf.open(member) as fh:
                data = fh.read()
    except zipfile.BadZipFile as exc:
        raise _ManifestReadError(f"corrupt zip: {exc}") from exc
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _ManifestReadError(f"malformed JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise _ManifestReadError(
            f"top-level JSON must be an object, got {type(parsed).__name__}"
        )
    return parsed


def _automation_workflow(gh: Any, policy: RepoReleasePolicy) -> "tuple[Any, Any]":
    """The automation repo and its qualification workflow, one repo fetch.

    One fetch serves both the workflow lookup and the default-branch
    check (``workflow_handle`` would hide the repo and force a second
    fetch). The workflow slot is None when the qualification workflow does
    not exist (404); callers decide whether that is a hard error (dispatch)
    or no-evidence (evaluation).
    """
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
            return repo, None
        raise
    return repo, workflow


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
    repo, workflow = _automation_workflow(gh, policy)
    if workflow is None:
        raise RuntimeError(
            f"{policy.downstream.qualification_workflow} does not exist on "
            f"{policy.downstream.automation_repo}"
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
    repo, workflow = _automation_workflow(gh, policy)
    if workflow is None:
        return None
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

