"""Scan published images and emit exact findings for candidate verification."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.common.job_summary import emit_job_summary
from scripts.cve_scan.config import CveScanSettings, load_settings
from scripts.cve_scan.image_matrix import resolve_matrix
from scripts.cve_scan.models import Classification
from scripts.cve_scan.rebuild_decider import classify_all
from scripts.cve_scan.scanner import ScanError, scan_images
from scripts.cve_scan.summary import render_findings_table

logger = logging.getLogger(__name__)


def _affected_versions(candidates: list[Classification]) -> list[str]:
    """Return version lines required by valkey-container's dispatch input."""
    versions: set[str] = set()
    for candidate in candidates:
        tag = candidate.finding.image.rsplit(":", 1)[-1]
        if tag.endswith("-alpine"):
            tag = tag[: -len("-alpine")]
        if tag:
            versions.add(tag)
    return sorted(versions)


def _encode_targets(candidates: list[Classification]) -> str:
    """Encode the immutable-promotion verification contract as compact base64 JSON."""
    targets_by_key: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for candidate in candidates:
        target = {
            "image": candidate.finding.image,
            "package": candidate.finding.package,
            "cve_id": candidate.finding.cve_id,
            "platform": candidate.finding.platform,
        }
        key = (
            target["image"],
            target["platform"],
            target["cve_id"],
            target["package"],
        )
        targets_by_key.setdefault(key, target)
    targets = list(targets_by_key.values())
    targets.sort(key=lambda value: (
        str(value["image"]),
        str(value["platform"]),
        str(value["cve_id"]),
        str(value["package"]),
    ))
    raw = json.dumps(targets, separators=(",", ":"), sort_keys=True).encode()
    return base64.b64encode(raw).decode()


def _emit_outputs(
    rebuild_required: bool,
    versions: list[str] | None = None,
    targets: str = "",
) -> None:
    """Emit single-line GitHub outputs used by the dispatch job."""
    values = {
        "rebuild_required": "true" if rebuild_required else "false",
        "versions": " ".join(versions or []),
        "targets": targets,
    }
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a") as output:
            for key, value in values.items():
                output.write(f"{key}={value}\n")
    else:
        for key, value in values.items():
            print(f"{key}={value}")


def _emit_summary(
    *,
    images: list[str],
    findings_count: int,
    candidates: list[Classification],
    unresolved: list[Classification],
    threshold: str,
    dry_run: bool,
) -> None:
    """Write the scan decision and verification boundary to the job summary."""
    lines = [
        "## CVE Scan Summary",
        "",
        f"| Images scanned | Findings ({threshold}+) | Candidate rebuild targets | No published fix |",
        "|---|---|---|---|",
        f"| {len(images)} | {findings_count} | {len(candidates)} | {len(unresolved)} |",
        "",
    ]
    if dry_run:
        lines.extend(["Mode: dry run; no downstream build will be dispatched.", ""])
    if candidates:
        versions = " ".join(_affected_versions(candidates))
        lines.extend([
            f"### Candidate rebuild targets ({versions})",
            "",
            "These findings have a published fix. The downstream workflow must build an "
            "immutable candidate, prove each targeted CVE/package/platform tuple is absent, "
            "and promote that exact digest. This scan does not predict rebuild behavior.",
            "",
            render_findings_table(candidates),
            "",
        ])
    if unresolved:
        lines.extend([
            "### Findings without a published fix",
            "",
            render_findings_table(unresolved),
            "",
        ])
    if findings_count == 0:
        lines.extend(["No findings at or above the severity threshold.", ""])
    emit_job_summary("\n".join(lines))


def _emit_failure_summary(exc: Exception) -> None:
    """Report a fail-closed scan error before propagating it."""
    emit_job_summary("\n".join([
        "## CVE Scan Summary",
        "",
        "### Scan failed",
        "",
        "No candidate rebuild was dispatched because complete scan coverage could not be proven.",
        "",
        f"Error: {exc}",
        "",
    ]))


def run_sweep(*, settings: CveScanSettings, dry_run: bool = False) -> None:
    """Scan, classify published-fix targets, and emit the downstream contract."""
    images = resolve_matrix(settings)
    logger.info(
        "Scanning %d image(s) x %d platform(s) with %s",
        len(images),
        len(settings.platforms),
        settings.scanner,
    )
    try:
        findings = scan_images(
            images,
            settings.scanner,
            settings.severity_threshold,
            platforms=settings.platforms,
        )
    except ScanError as exc:
        _emit_outputs(False)
        _emit_failure_summary(exc)
        raise

    classifications = classify_all(findings)
    candidates = [classification for classification in classifications if classification.fixable]
    unresolved = [classification for classification in classifications if not classification.fixable]
    versions = _affected_versions(candidates)
    targets = _encode_targets(candidates) if candidates else ""

    _emit_outputs(bool(candidates) and not dry_run, versions, targets)
    _emit_summary(
        images=images,
        findings_count=len(findings),
        candidates=candidates,
        unresolved=unresolved,
        threshold=settings.severity_threshold.name,
        dry_run=dry_run,
    )

    if dry_run and candidates:
        print(f"[DRY RUN] Would dispatch candidate builds for: {' '.join(versions)}")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Scan published images and emit candidate rebuild targets.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    run_sweep(settings=load_settings(), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
