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
import uuid
import zipfile
from functools import partial
from typing import Any

# Module-level import (not function-local) so tests can patch
# scripts.release.qualification.urlopen and pin the no-token, size-capped
# fetch behavior without reaching into urllib globally.
from urllib.request import Request, urlopen

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import QualificationStatus
from scripts.release.policy import RepoReleasePolicy

logger = logging.getLogger(__name__)

# Bound the run scan; qualification runs for an active release are recent.
# Shared with verify.py's build-run scan and checks.py's daily-run scan.
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
# Presence plus content is the whole check: a non-empty artifact whose
# bytes disagree with what was dispatched must not pass; artifact presence
# alone was never evidence.
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
# evidence log rather than compared to a dispatch record. The nonce binds
# during RUN SELECTION when the caller supplies the dispatch-recorded value
# (``expected_nonce``): the controller passes a per-dispatch nonce and
# records it on the tracker receipt, the producer echoes it back in the
# manifest, and a completed run whose manifest does not echo it is skipped
# (invisible, never a failure). A receipt from before nonce wiring carries
# none, in which case the nonce stays evidence detail only.
_MANIFEST_REQUIRED_FIELDS = (
    "schema", "nonce", "version", "tag", "source_sha", "automation_sha",
    "rpm_jobs", "deb_jobs", "archive_jobs",
)


def evaluate_qualification(
    gh: Any, policy: RepoReleasePolicy, *, tag: str, sha: str,
    expected_nonce: str = "",
) -> QualificationStatus:
    """Live qualification evidence for release *tag* at exactly *sha*.

    The newest matching run wins, so re-dispatching after a failure
    supersedes it. A successful run must also show zero failed jobs and at
    least ``qualification_min_jobs`` jobs: a truncated matrix (an empty
    generate step) must not pass vacuously.

    ``expected_nonce``, when set, is the nonce the controller's dispatch
    receipt recorded for this candidate. SKIP semantics, not fail: a
    completed run whose manifest does not echo the recorded nonce is not
    the controller's dispatch, so it is invisible during run selection --
    not evidence, not a failure -- and the newest ECHOING run is the
    evidence. A nonce-less manual re-dispatch (whose producer falls back
    to a run-derived nonce) therefore never poisons the slot by
    superseding the controller's own run. When no echoing run exists at
    all, this returns the empty status and the caller's blocker renders
    the recorded nonce with the exact manual-dispatch instruction. ""
    (legacy receipts from before nonce wiring, or callers with no tracker
    access) keeps the prior behavior: the manifest nonce is recorded as
    evidence detail only.
    """
    run = _find_run(gh, policy, tag, sha, expected_nonce=expected_nonce)
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
    gaps = _evidence_gaps(policy, run, jobs, tag=tag, sha=sha,
                          expected_nonce=expected_nonce)
    if gaps:
        return result(passed=False, failed_jobs=gaps)
    return result(passed=True)


def _evidence_gaps(policy: RepoReleasePolicy, run: Any, jobs: list,
                   *, tag: str, sha: str,
                   expected_nonce: str = "") -> tuple[str, ...]:
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
    # The qualification manifest artifact is not just a name -- its
    # content must bind the run to the release identity that dispatched it.
    # A non-empty unexpired artifact whose bytes disagree with what was
    # dispatched (tag, source_sha, version, job counts, and the dispatch
    # nonce when the receipt recorded one) never passes.
    manifest_artifact = next(
        (a for a in usable if a.name == MANIFEST_ARTIFACT), None,
    )
    if manifest_artifact is None:
        gaps.append("(Evidence mismatch: no qualification manifest)")
    else:
        gaps.extend(_validate_manifest_content(
            manifest_artifact, policy=policy, tag=tag, sha=sha,
            expected_archive_jobs=expected_archives, is_ga=is_ga,
            expected_nonce=expected_nonce,
        ))
    return tuple(gaps)


def _validate_manifest_content(
    artifact: Any, *, policy: RepoReleasePolicy, tag: str, sha: str,
    expected_archive_jobs: int, is_ga: bool, expected_nonce: str = "",
) -> list[str]:
    """Load the manifest JSON and require every dispatched field to match.

    Any download/parse/shape failure or field mismatch becomes an evidence
    mismatch naming what differed, so nothing about a hostile manifest
    (wrong SHA, wrong tag, wrong counts, malformed JSON, empty file) reads
    as passable evidence. ``automation_sha`` is logged as evidence detail.
    ``expected_nonce``, when set, must be echoed exactly by the manifest's
    ``nonce`` field (the controller recorded it on the dispatch receipt);
    "" keeps the nonce as logged detail only, so receipts from before
    nonce wiring keep verifying.
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
    if expected_nonce and payload["nonce"] != expected_nonce:
        # Defense in depth: run selection already skips completed runs
        # whose manifest does not echo the recorded nonce, so this gap is
        # normally unreachable; it stands against artifact content
        # changing between the selection read and this validation read.
        # The message names the remedial ACTION, not just the values.
        gaps.append(
            f"(Evidence mismatch: qualification manifest nonce "
            f"{payload['nonce']!r}, expected {expected_nonce!r} from the "
            f"dispatch receipt; dispatch the qualification workflow with "
            f"the recorded nonce as its `nonce` input)"
        )
    if not gaps:
        # automation_sha is retained as evidence detail; the nonce is
        # logged here too so a receipt-less evaluation still records it.
        logger.info(
            "Qualification manifest for %s @ %s validated (automation_sha=%s, "
            "nonce=%s)", tag, sha[:12],
            str(payload.get("automation_sha"))[:12],
            str(payload.get("nonce"))[:16],
        )
    return gaps


class _ManifestReadError(RuntimeError):
    """A qualification manifest artifact could not be downloaded or parsed."""


def _fetch_signed_url(url: str) -> bytes:
    """The bytes behind a signed blob-storage URL, size-capped.

    Deliberately sends NO Authorization header: the URL is self-
    authenticating and the API token must never reach the blob host.

    The stdlib's default redirect handler happily follows an https URL to
    an http target, so the final URL's scheme is re-checked after the
    open and a downgrade refuses. Checking after the fact is the smallest
    sound guard here: no credential ever rides the request, so refusing
    to trust bytes that traveled a cleartext hop is all that is needed.
    """
    if not url.startswith("https://"):
        raise _ManifestReadError("artifact redirect points at a non-https URL")
    req = Request(url, headers={"User-Agent": "valkey-ci-agent"})
    try:
        with urlopen(req, timeout=30) as resp:  # noqa: S310 (https enforced)
            final_url = str(resp.geturl() or "")
            if not final_url.startswith("https://"):
                raise _ManifestReadError(
                    "artifact blob redirect chain left https "
                    f"(final URL: {final_url or '<unknown>'})"
                )
            return resp.read(_MAX_MANIFEST_BYTES * 4 + 1)
    except OSError as exc:
        raise _ManifestReadError(f"artifact blob download failed: {exc}") from exc


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
    status, headers, body = requester.requestBlob(
        "GET", artifact.archive_download_url, None, None, None, None,
    )
    if 300 <= int(status) < 400:
        # GitHub answers artifact downloads with a redirect to short-lived
        # signed blob storage. Follow it WITHOUT the API token: the signed
        # URL authenticates itself, and forwarding our token to another
        # host would leak it (the same discipline as
        # scripts/common/workflow_artifacts.ArtifactClient).
        location = ""
        for key, value in (headers or {}).items():
            if str(key).lower() == "location":
                location = str(value)
                break
        if not location:
            raise _ManifestReadError(
                f"artifact download returned HTTP {status} with no Location"
            )
        body = _fetch_signed_url(location)
    elif not (200 <= int(status) < 300):
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
    nonce: str = "",
) -> str:
    """Start a qualification run for release *tag* at exactly *sha*.

    The tag (not the bare version) is the dispatched identity: the
    qualification workflow mirrors production applicability from it (an rc
    skips the distro package matrix exactly as the production build does).

    A per-dispatch ``nonce`` (uuid4 hex, generated here when the caller
    does not supply one) rides along as a workflow input; the producer
    echoes it into the qualification manifest, and the evaluator requires
    it back when the dispatch receipt recorded one. The nonce actually
    dispatched is returned so the caller can record it. The producer falls
    back to a run-derived nonce when the input is absent, so deploy order
    between the two repos is safe either way.

    Callers guard idempotency (dispatch only when no pending or passed run
    exists for this SHA); this function just fires the dispatch on the
    automation repo's default branch.
    """
    nonce = nonce or uuid.uuid4().hex
    repo, workflow = _automation_workflow(gh, policy)
    if workflow is None:
        raise RuntimeError(
            f"{policy.downstream.qualification_workflow} does not exist on "
            f"{policy.downstream.automation_repo}"
        )
    dispatched = retry_github_call(
        lambda: workflow.create_dispatch(
            repo.default_branch,
            inputs={"version": tag, "source_sha": sha, "nonce": nonce},
        ),
        retries=2, description="dispatch qualification run",
    )
    if not dispatched:
        raise RuntimeError(
            f"qualification dispatch was rejected by "
            f"{policy.downstream.automation_repo}/{policy.downstream.qualification_workflow}"
        )
    logger.info("Dispatched qualification of %s @ %s (nonce %s)",
                tag, sha[:12], nonce[:16])
    return nonce


def _run_echoes_nonce(run: Any, expected_nonce: str) -> bool:
    """Whether a completed run's manifest echoes the recorded dispatch nonce.

    The run-selection filter behind skip semantics. False never fails
    anything: a non-echoing run is simply invisible (the caller keeps
    scanning for an echoing one), so failing closed here can only delay
    evidence, never fabricate or destroy it. A run with no readable
    manifest (never uploaded, expired, corrupt, or a transient download
    error) cannot prove it is the controller's dispatch and reads as
    non-echoing.
    """
    try:
        artifacts = retry_github_call(
            lambda: list(run.get_artifacts()),
            retries=2,
            description=f"list artifacts of qualification run {run.id}",
        )
        manifest = next(
            (a for a in artifacts
             if a.name == MANIFEST_ARTIFACT and not a.expired
             and a.size_in_bytes > 0),
            None,
        )
        if manifest is None:
            return False
        payload = _load_manifest_payload(manifest)
    except Exception:
        logger.warning(
            "Could not read the manifest of qualification run %s while "
            "matching the recorded dispatch nonce; treating the run as "
            "non-echoing (skipped, not failed)", run.id, exc_info=True,
        )
        return False
    return payload.get("nonce") == expected_nonce


def _find_run(gh: Any, policy: RepoReleasePolicy, tag: str, sha: str,
              expected_nonce: str = "") -> Any:
    """The newest qualification run for exactly this release and commit.

    Three binding rules keep the evidence honest:
    - the run-name must carry the full ``Qualify <tag> @ <sha>`` marker, so
      a run dispatched with a different version for the same SHA (e.g. an
      rc-suffixed dispatch that legitimately skips the package matrix) can
      never satisfy this release's qualification;
    - the run must have executed the default branch's workflow definition,
      so a doctored qualify workflow on a side branch cannot manufacture
      evidence;
    - when ``expected_nonce`` is set (the dispatch receipt recorded one),
      a completed run whose manifest does not echo it is SKIPPED, not
      failed: it is some other dispatch (typically a nonce-less manual
      re-dispatch, whose producer falls back to a run-derived nonce) and
      must be neither evidence nor a failure, so the newest ECHOING run
      still wins even when a non-echoing run is newer.

    Two deliberate carve-outs from the nonce filter:
    - a run still executing cannot have uploaded its manifest yet, so it
      is returned (and reads as pending) rather than skipped; skipping it
      would instruct the operator to dispatch a duplicate while the
      controller's own run may be the one in flight;
    - a ``startup_failure`` run never planned jobs or artifacts, so its
      identity is undecidable by manifest -- but its failure is real for
      EVERY dispatch of this workflow (the same default-branch workflow
      definition the controller dispatches), so it stays visible and
      keeps routing through the marker-gated one-retry path instead of
      reading as "no run".
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
        if (expected_nonce and run.status == "completed"
                and run.conclusion != "startup_failure"
                and not _run_echoes_nonce(run, expected_nonce)):
            logger.info(
                "Skipping qualification run %s: its manifest does not echo "
                "the recorded dispatch nonce (not evidence, not a failure)",
                run.id,
            )
            continue
        return run
    return None

