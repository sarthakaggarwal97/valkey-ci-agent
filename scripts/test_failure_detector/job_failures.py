"""Find Daily job failures that the failures artifact does not record.

The ``all-test-failures`` artifact only holds assertion failures. A job also
fails on a build break, a test timeout, a valgrind or sanitizer report at
server shutdown, or a crash, and none of those reach the artifact. For every
failed job this reads the job log's own summary ("The following tests
failed:" followed by ``*** [status]: ...`` lines): a line naming a test and its
file becomes an ordinary test failure, so it lands on that test's issue;
anything else becomes a job-level failure. A job the artifact already records
is read too, since one assertion there does not explain a Valgrind report or a
timeout in the same job; only its summary-less failure is left out, because
the recorded assertion accounts for it.

Job-level failures that print the same summary in the same step and matrix
leg (e.g. a Valgrind leak in ``test-valgrind-test (unit)`` and its
``test-valgrind-no-malloc-usable-size-test (unit)`` twin) are one failure with
several jobs, like a test that fails in several environments. A failure that
printed no summary is kept per job, and one in a setup step (installing
packages, checking out, uploading results) is not reported at all: it is the
runner's or a mirror's problem, not the repository's.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, TypeVar

from scripts.ci_fix.github_runs import FAILED_JOB_CONCLUSIONS, job_is_ignored
from scripts.common.incidents import compute_fingerprint
from scripts.common.valkey_ci import release_branch, strip_log_prefix, test_in_file
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.test_failure_detector.parse_failures import JobReference, UniqueFailure

logger = logging.getLogger(__name__)

_SUMMARY_RE = re.compile(r"^\*\*\* \[(?P<status>[A-Za-z]+)\]: (?P<data>.+)$")
_MAX_SUMMARY_LINES = 10
# Steps that prepare the runner rather than build or test the repository.
_SETUP_STEP_RE = re.compile(
    r"^(?:set up job|initialize containers|complete job|stop containers|run actions/|post )"
    r"|\b(?:install|checkout|cache|upload|prep|testprep)\b",
    re.IGNORECASE,
)


@dataclass
class JobFailure:
    """A failure not attributable to a recorded test, in one or more jobs.

    ``job`` and ``url`` name the first job seen; ``jobs`` lists every job of
    the run that failed this way.
    """

    job: str
    url: str
    step: str = ""
    summary: tuple[str, ...] = ()
    jobs: list[JobReference] = field(default_factory=list)
    # The release branch of a Weekly failure; "" for Daily.
    branch: str = ""

    def identity(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The (namespace, shapes) that make two failures the same one.

        A printed summary identifies the failure within its step and matrix
        leg; without one there is nothing to compare, so the job name does.
        A Weekly failure is also bound to its release branch.
        """
        branch = ("branch", self.branch) if self.branch else ()
        if not self.summary:
            return ("name", self.job, *branch), ()
        return ("step", self.step, matrix_leg(self.job), *branch), self.summary


def matrix_leg(job_name: str) -> str:
    """``unit`` for ``test-valgrind-test (unit)``; "" for a job without a leg."""
    if not job_name.endswith(")") or " (" not in job_name:
        return ""
    return job_name.rsplit(" (", 1)[1][:-1]


def normalize_job_name(name: str) -> str:
    """Compare a run's job name with an artifact key, ignoring separators.

    The artifact keys a matrix job as ``test-valgrind-test-unit`` while the run
    displays ``test-valgrind-test (unit)``.
    """
    return re.sub(r"[^a-z0-9]", "", name.lower())


def find_unrecorded_failures(
    artifact_client: ArtifactClient,
    repo_full_name: str,
    all_failures: dict[str, Any],
    *,
    jobs: Iterable[Any],
    ignored_jobs: Iterable[str] = (),
) -> tuple[list[UniqueFailure], list[JobFailure]]:
    """Return failures found only in the logs of ``jobs``.

    ``jobs`` is one exact-attempt snapshot from
    :func:`scripts.ci_fix.github_runs.jobs_for_run_attempt`, so every view of
    the run uses the same GitHub objects.
    """
    recorded = {
        normalize_job_name(job)
        for job, suites in all_failures.items()
        if isinstance(suites, dict) and any(isinstance(entries, list) and entries for entries in suites.values())
    }
    patterns = tuple(ignored_jobs)
    tests: dict[tuple[str, str], UniqueFailure] = {}
    job_failures: dict[str, JobFailure] = {}
    for job in jobs:
        name = str(getattr(job, "name", "") or "")
        if str(getattr(job, "conclusion", "") or "") not in FAILED_JOB_CONCLUSIONS:
            continue
        if job_is_ignored(name, patterns):
            continue
        url = str(getattr(job, "html_url", "") or "")
        summary = _failure_summary(artifact_client, repo_full_name, job)
        unattributed: list[str] = []
        for status, data in summary:
            test = test_in_file(data)
            if test is None or status == "exception":
                unattributed.append(f"[{status}]: {data}")
                continue
            key = test
            failure = tests.setdefault(key, UniqueFailure(
                test_name=key[0], test_file=key[1], error=f"[{status}]: {data}",
            ))
            if not any(ref.job == name for ref in failure.jobs):
                failure.jobs.append(JobReference(job=name, url=url))
        if unattributed:
            pass
        elif summary or normalize_job_name(name) in recorded:
            # Every line named a test, or the artifact explains the failure.
            continue
        step = _failed_step(job)
        if not unattributed and _SETUP_STEP_RE.search(step):
            logger.info("Not reporting %s: it failed in setup step %r without a summary", name, step)
            continue
        found = JobFailure(job=name, url=url, step=step, summary=tuple(unattributed[:_MAX_SUMMARY_LINES]))
        namespace, shapes = found.identity()
        same = job_failures.setdefault(compute_fingerprint(namespace=namespace, shapes=shapes), found)
        if not any(ref.job == name for ref in same.jobs):
            same.jobs.append(JobReference(job=name, url=url))
    return list(tests.values()), list(job_failures.values())


# A Weekly job outside the per-branch calls (Weekly's own branch-listing job).
UNBRANCHED = "no release branch"
_Failure = TypeVar("_Failure", UniqueFailure, JobFailure)


def split_by_branch(failures: list[_Failure]) -> list[_Failure]:
    """One failure per Weekly release branch its jobs ran on.

    ``failures`` are grouped across the whole run; each comes back once per
    branch, with that branch's jobs and ``branch`` set. A job outside the
    per-branch calls gets ``UNBRANCHED``, so it is still filed as a Weekly
    failure, apart from Daily's.
    """
    split: list[_Failure] = []
    for failure in failures:
        by_branch: dict[str, list[JobReference]] = {}
        for ref in failure.jobs:
            by_branch.setdefault(release_branch(ref.job) or UNBRANCHED, []).append(ref)
        if not by_branch:
            split.append(replace(failure, branch=UNBRANCHED))
        for branch, refs in by_branch.items():
            # A job failure also names its first job.
            names: dict[str, Any] = (
                {"job": refs[0].job, "url": refs[0].url} if isinstance(failure, JobFailure) else {}
            )
            split.append(replace(failure, jobs=refs, branch=branch, **names))
    return split


def merge_failures(recorded: list[UniqueFailure], extra: list[UniqueFailure]) -> list[UniqueFailure]:
    """Fold log-derived test failures into the artifact's, one entry per test."""
    by_key = {(f.test_name, f.test_file): f for f in recorded}
    for failure in extra:
        existing = by_key.get((failure.test_name, failure.test_file))
        if existing is None:
            by_key[(failure.test_name, failure.test_file)] = failure
            recorded.append(failure)
            continue
        for ref in failure.jobs:
            if not any(r.job == ref.job for r in existing.jobs):
                existing.jobs.append(ref)
    return recorded


def _failure_summary(artifact_client: ArtifactClient, repo_full_name: str, job: Any) -> list[tuple[str, str]]:
    try:
        log = artifact_client.download_job_log(repo_full_name, int(job.id))
    except Exception as exc:  # noqa: BLE001 - a missing log degrades to a job-level failure
        logger.warning("Could not download the log of job %s: %s", getattr(job, "id", "?"), exc)
        return []
    lines: list[tuple[str, str]] = []
    for raw in log.splitlines():
        match = _SUMMARY_RE.match(strip_log_prefix(raw))
        if match:
            lines.append((match.group("status"), match.group("data").strip()))
    return lines


def _failed_step(job: Any) -> str:
    for step in getattr(job, "steps", None) or ():
        if str(getattr(step, "conclusion", "") or "") in FAILED_JOB_CONCLUSIONS:
            return str(getattr(step, "name", "") or "")
    return ""
