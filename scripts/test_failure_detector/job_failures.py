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

One cause is one issue, however many jobs it fails: job-level failures that
print the same summary in the same step (e.g. a Valgrind leak in
``test-valgrind-test (unit)`` and its ``test-valgrind-no-malloc-usable-size-test
(unit)`` twin) are one failure with several jobs, like a test that fails in
several environments. Failures that printed no summary at all (a unit test,
a build break, a crash) are one failure per step they failed in: nothing tells
them apart, and an issue per job would be noise. Telling them apart needs the
failure in Valkey's structured output, not more parsing of its logs. One in a
setup step (installing packages, checking out, uploading results)
is not reported at all: it is the runner's or a mirror's problem, not the
repository's.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from scripts.common.incidents import compute_fingerprint
from scripts.common.valkey_ci import strip_log_prefix, test_in_file
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.common.workflow_jobs import FAILED_JOB_CONCLUSIONS, job_is_ignored
from scripts.test_failure_detector.parse_failures import JobReference, UniqueFailure

logger = logging.getLogger(__name__)

_SUMMARY_RE = re.compile(r"^\*\*\* \[(?P<status>[A-Za-z]+)\]: (?P<data>.+)$")
_MAX_SUMMARY_LINES = 10
# The leak check runtest runs at each server's shutdown is reported as a test
# named by that server's pid ("Check for memory leaks (pid 4321)"). Like a
# valgrind report it is not a test's own failure, and runtest's artifact
# leaves it out too, so it is a job-level failure: as a test it would get a
# new issue for every pid.
_LEAK_CHECK_RE = re.compile(r"^Check for memory leaks\b")
# Steps that prepare the runner rather than build or test the repository.
_SETUP_STEP_RE = re.compile(
    r"^(?:set up job|initialize containers|complete job|stop containers|run actions/|post )"
    r"|\b(?:install|checkout|cache|upload|prep|testprep|libbacktrace)\b",
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

    def identity(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The (namespace, shapes) that make two failures the same one.

        A printed summary identifies the failure within its step. Without one
        there is nothing to compare, so every job that failed in the same step
        without saying why is one failure: an issue per job would be noise.
        """
        if not self.summary:
            return ("unexplained", self.step), ()
        return ("step", self.step), self.summary


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
    unread: list[str] | None = None,
) -> tuple[list[UniqueFailure], list[JobFailure]]:
    """Return failures found only in the logs of ``jobs``.

    A failed job whose log cannot be read (expired, or the download failed)
    is left out: nothing is known about how it failed, so filing it would
    make an issue of the download error. Its name is appended to ``unread``,
    so the caller can report the inspection as incomplete.

    ``jobs`` is one exact-attempt snapshot from
    :func:`scripts.common.workflow_jobs.jobs_for_run_attempt`, so every view of
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
        if summary is None:
            if unread is not None:
                unread.append(name)
            continue
        unattributed: dict[str, None] = {}  # ordered, without repeats
        for status, data in summary:
            test = test_in_file(data)
            if test is None or status == "exception" or _LEAK_CHECK_RE.match(data):
                unattributed[f"[{status}]: {data}"] = None
                continue
            failure = tests.setdefault(test, UniqueFailure(
                test_name=test[0], test_file=test[1], error=f"[{status}]: {data}",
            ))
            if not any(ref.job == name for ref in failure.jobs):
                failure.jobs.append(JobReference(job=name, url=url))
        if not unattributed and (summary or normalize_job_name(name) in recorded):
            # Every line named a test, or the artifact explains the failure.
            continue
        step = _failed_step(job)
        if not unattributed and _SETUP_STEP_RE.search(step):
            logger.info("Not reporting %s: it failed in setup step %r without a summary", name, step)
            continue
        found = JobFailure(job=name, url=url, step=step, summary=tuple(unattributed)[:_MAX_SUMMARY_LINES])
        namespace, shapes = found.identity()
        same = job_failures.setdefault(compute_fingerprint(namespace=namespace, shapes=shapes), found)
        if not any(ref.job == name for ref in same.jobs):
            same.jobs.append(JobReference(job=name, url=url))
    return list(tests.values()), list(job_failures.values())


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


def _failure_summary(artifact_client: ArtifactClient, repo_full_name: str, job: Any) -> list[tuple[str, str]] | None:
    """The job log's runtest summary lines, or None when the log cannot be read."""
    try:
        log = artifact_client.download_job_log(repo_full_name, int(job.id))
    except Exception as exc:  # noqa: BLE001 - reported to the caller as an unread log
        logger.warning("Could not download the log of job %s: %s", getattr(job, "id", "?"), exc)
        return None
    if not log.strip():
        # The download answers an expired log with nothing; a real log is never empty.
        logger.warning("The log of job %s is gone (expired)", getattr(job, "id", "?"))
        return None
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
