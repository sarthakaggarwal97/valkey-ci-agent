"""Invoke Trivy as a subprocess and parse findings.

Each image is scanned per platform (``--platform``); findings are merged and
deduplicated by (image, package, cve_id, installed_version, platform) so
cross-platform findings stay distinct for per-platform base verification.
"""

from __future__ import annotations

import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from scripts.cve_scan.config import DEFAULT_PLATFORMS
from scripts.cve_scan.models import Finding, Severity
from scripts.parsers.cve_findings_parser import ParseError, filter_by_threshold, parse_findings

logger = logging.getLogger(__name__)

#: Per-scan subprocess timeout in seconds (cached-DB scans take tens of seconds).
_SCAN_TIMEOUT_SECONDS = 180
_MAX_SCAN_WORKERS = 4


class ScanError(Exception):
    """Raised when a scanner subprocess fails or produces unparseable output."""


def _build_command(scanner: str, image: str, platform: str | None = None) -> list[str]:
    """Build the scanner command as an argument list (no shell interpolation)."""
    if scanner == "trivy":
        cmd = [
            "trivy", "image", "--format", "json", "--quiet",
            "--scanners", "vuln", "--pkg-types", "os",
        ]
        if platform:
            cmd.extend(["--platform", platform])
        cmd.append(image)
        return cmd
    raise ValueError(f"Unsupported scanner: {scanner!r}. Must be 'trivy'.")


def _run_scanner(command: list[str], timeout: int = _SCAN_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Run the scanner subprocess and return parsed JSON.

    Raises ScanError on non-zero exit, timeout, or invalid JSON.
    """
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ScanError(
            f"Scanner timed out after {timeout}s: {' '.join(command)}"
        ) from exc
    except OSError as exc:
        raise ScanError(
            f"Failed to execute scanner: {' '.join(command)}: {exc}"
        ) from exc

    if result.returncode != 0:
        stderr_snippet = result.stderr[:500] if result.stderr else "(no stderr)"
        raise ScanError(
            f"Scanner exited with code {result.returncode}: {' '.join(command)}\n"
            f"stderr: {stderr_snippet}"
        )

    if not result.stdout.strip():
        raise ScanError(f"Scanner produced empty output: {' '.join(command)}")

    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ScanError(
            f"Scanner output is not valid JSON: {' '.join(command)}: {exc}"
        ) from exc

    if not isinstance(parsed, dict):
        raise ScanError(
            f"Scanner output is not a JSON object: {' '.join(command)}: "
            f"got {type(parsed).__name__}"
        )
    return parsed


def scan_image(image: str, scanner: str, platform: str | None = None) -> list[Finding]:
    """Scan a single image (optionally per platform) and return all findings.

    Args:
        image: Container image reference (e.g. "valkey/valkey:8.0-alpine").
        scanner: Scanner to use ("trivy").
        platform: Optional platform (e.g. "linux/amd64") passed as ``--platform``.

    Returns:
        List of Finding objects from the scan.

    Raises:
        ScanError: If the scanner fails, produces invalid output, or its
            JSON does not match the expected schema.
        ValueError: If the scanner name is not recognized.
    """
    command = _build_command(scanner, image, platform=platform)
    json_obj = _run_scanner(command)
    try:
        return parse_findings(scanner, json_obj, image, platform=platform or "")
    except ParseError as exc:
        raise ScanError(
            f"Scanner output failed schema validation for {image}: {exc}"
        ) from exc


def _dedup_findings(findings: list[Finding]) -> list[Finding]:
    """Collapse same-platform duplicates; keep cross-platform findings distinct.

    First occurrence wins. Cross-platform findings stay separate so base
    verification can check each platform's base image independently.
    """
    seen: set[tuple[str, str, str, str, str]] = set()
    deduped: list[Finding] = []
    for f in findings:
        key = (f.image, f.package, f.cve_id, f.installed_version, f.platform)
        if key not in seen:
            seen.add(key)
            deduped.append(f)
    return deduped


def scan_images(
    images: list[str],
    scanner: str,
    threshold: Severity,
    platforms: list[str] | None = None,
) -> list[Finding]:
    """Scan multiple images per platform; return deduplicated findings at or above threshold.

    Args:
        images: List of container image references to scan.
        scanner: Scanner to use ("trivy").
        threshold: Minimum severity level; findings below this are excluded.
        platforms: Platforms to scan per image. Defaults to DEFAULT_PLATFORMS.

    Returns:
        Combined deduplicated findings from all images and platforms.

    Raises:
        ScanError: If any scanner invocation fails.
        ValueError: If the scanner name is not recognized.
    """
    if platforms is None:
        platforms = DEFAULT_PLATFORMS

    jobs = [
        (image_index, image, platform)
        for image_index, image in enumerate(images)
        for platform in platforms
    ]
    results: list[list[Finding] | None] = [None] * len(jobs)
    workers = min(_MAX_SCAN_WORKERS, len(jobs))
    logger.info("Scanning %d image/platform pair(s) with %d worker(s)", len(jobs), workers)

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cve-scan") as executor:
        futures = {
            executor.submit(scan_image, image, scanner, platform): result_index
            for result_index, (_image_index, image, platform) in enumerate(jobs)
        }
        try:
            for future in as_completed(futures):
                result_index = futures[future]
                _image_index, image, platform = jobs[result_index]
                findings = future.result()
                results[result_index] = findings
                logger.info("%s [%s]: %d finding(s)", image, platform, len(findings))
        except Exception:
            for future in futures:
                future.cancel()
            raise

    # Flatten in input order so summaries and tests stay deterministic despite
    # concurrent subprocess completion.
    all_findings = [
        finding
        for result in results
        if result is not None
        for finding in result
    ]
    return filter_by_threshold(_dedup_findings(all_findings), threshold)
