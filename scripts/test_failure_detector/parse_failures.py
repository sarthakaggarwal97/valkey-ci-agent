"""Parse and deduplicate test failures from the consolidated artifact"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_MAX_JOB_NAME_BYTES = 256
_MAX_SUITE_NAME_BYTES = 256
_MAX_TEST_NAME_BYTES = 1024
_MAX_TEST_FILE_BYTES = 2048
_MAX_ERROR_BYTES = 64 * 1024
_MAX_FAILURE_ENTRIES = 20_000

@dataclass
class JobReference:
    """A reference to a specific CI job where a test failed."""

    job: str
    suite: str
    url: str = ""

@dataclass
class UniqueFailure:
    """A deduplicated test failure that may appear across multiple jobs."""

    test_name: str
    test_file: str
    error: str = ""
    jobs: list[JobReference] = field(default_factory=list)

    @property
    def display_name(self) -> str:
        return f"{self.test_name} in {self.test_file}"

def parse_and_deduplicate(
    all_failures: dict[str, Any],
    job_urls: dict[str, str],
) -> list[UniqueFailure]:
    """Parse the all-test-failures JSON and deduplicate by test name + file.

    Args:
        all_failures: The parsed all-test-failures.json content.
            Structure: {job_name: {suite_name: [{test_name, test_file, error}]}}
        job_urls: Mapping of job name -> HTML URL for CI links.

    Returns:
        List of UniqueFailure objects, deduplicated across jobs.
    """
    # Local grouping key: the (test_name, test_file) tuple. A tuple can't
    # collide the way any single-separator string ("<name> in <file>" or
    # "<name>::<file>") would when a test name contains that separator.
    # This is independent of issue_renderer.fingerprint_for's marker key (a
    # hash of the same pair) — they need not match: this groups failures within
    # one run, that dedupes issues across runs. Both avoid separator collisions.
    grouped: dict[tuple[str, str], UniqueFailure] = {}
    entries_seen = 0

    if not isinstance(all_failures, dict):
        logger.warning(
            "Unexpected top-level format: expected dict, got %s",
            type(all_failures).__name__,
        )
        return []

    for job_name, suites in all_failures.items():
        if not _valid_text(job_name, _MAX_JOB_NAME_BYTES):
            logger.warning("Skipping non-string or oversized job name")
            continue
        if not isinstance(suites, dict):
            logger.warning("Unexpected format for job %r: expected dict, got %s", job_name, type(suites).__name__)
            continue

        for suite_name, entries in suites.items():
            if not _valid_text(suite_name, _MAX_SUITE_NAME_BYTES):
                logger.warning("Skipping non-string or oversized suite name")
                continue
            if not isinstance(entries, list):
                logger.warning(
                    "Unexpected format for %s/%s: expected list, got %s",
                    job_name, suite_name, type(entries).__name__,
                )
                continue

            for entry in entries:
                entries_seen += 1
                if entries_seen > _MAX_FAILURE_ENTRIES:
                    logger.warning(
                        "Failure artifact exceeds %d entries; ignoring the remainder",
                        _MAX_FAILURE_ENTRIES,
                    )
                    return list(grouped.values())
                if not isinstance(entry, dict):
                    continue

                test_name = entry.get("test_name", "")
                test_file = entry.get("test_file", "")
                if (
                    not _valid_text(test_name, _MAX_TEST_NAME_BYTES)
                    or not _valid_text(test_file, _MAX_TEST_FILE_BYTES)
                    or not test_name
                    or not test_file
                ):
                    logger.debug("Skipping entry with missing test_name or test_file: %s", entry)
                    continue
                raw_error = entry.get("error", "")
                if not isinstance(raw_error, str):
                    logger.warning(
                        "Ignoring non-string error for %s in %s",
                        test_name,
                        test_file,
                    )
                    raw_error = ""
                error = _truncate_utf8(raw_error, _MAX_ERROR_BYTES)

                key = (test_name, test_file)

                if key not in grouped:
                    grouped[key] = UniqueFailure(
                        test_name=test_name,
                        test_file=test_file,
                        error=error,
                    )

                # Deduplicate: skip if this job is already recorded
                failure = grouped[key]
                if not any(j.job == job_name for j in failure.jobs):
                    failure.jobs.append(
                        JobReference(
                            job=job_name,
                            suite=suite_name,
                            url=_job_url(job_urls, job_name),
                        )
                    )
                    logger.debug("%s in %s/%s", failure.display_name, job_name, suite_name)

    unique_failures = list(grouped.values())
    logger.info("Total unique failures: %d", len(unique_failures))
    return unique_failures


def _valid_text(value: Any, max_bytes: int) -> bool:
    return isinstance(value, str) and len(value.encode("utf-8")) <= max_bytes


def _truncate_utf8(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _job_url(job_urls: dict[str, str], job_name: str) -> str:
    value = job_urls.get(job_name, "")
    return value if isinstance(value, str) else ""
