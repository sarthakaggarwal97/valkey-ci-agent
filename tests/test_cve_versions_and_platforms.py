from __future__ import annotations

import threading
import time

from scripts.cve_scan.models import Finding, Severity
from scripts.cve_scan.scanner import _dedup_findings, scan_images


def _finding(image: str, platform: str, *, cve: str = "CVE-1") -> Finding:
    return Finding(
        image=image,
        package="pkg",
        installed_version="1",
        cve_id=cve,
        severity=Severity.HIGH,
        fixed_version="2",
        platform=platform,
    )


def test_cross_platform_findings_remain_distinct() -> None:
    findings = [
        _finding("valkey/valkey:9.0", "linux/amd64"),
        _finding("valkey/valkey:9.0", "linux/arm64"),
    ]
    assert _dedup_findings(findings) == findings


def test_same_platform_duplicates_collapse() -> None:
    finding = _finding("valkey/valkey:9.0", "linux/amd64")
    assert _dedup_findings([finding, finding]) == [finding]


def test_scan_images_runs_pairs_concurrently_and_returns_input_order(monkeypatch) -> None:
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_scan(image: str, _scanner: str, platform: str | None = None):
        nonlocal active, max_active
        assert platform is not None
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return [_finding(image, platform)]

    monkeypatch.setattr("scripts.cve_scan.scanner.scan_image", fake_scan)
    findings = scan_images(
        ["valkey/valkey:8.1", "valkey/valkey:9.0"],
        "trivy",
        Severity.HIGH,
        platforms=["linux/amd64", "linux/arm64"],
    )

    assert max_active > 1
    assert [(finding.image, finding.platform) for finding in findings] == [
        ("valkey/valkey:8.1", "linux/amd64"),
        ("valkey/valkey:8.1", "linux/arm64"),
        ("valkey/valkey:9.0", "linux/amd64"),
        ("valkey/valkey:9.0", "linux/arm64"),
    ]


def test_scan_images_filters_threshold_after_merge(monkeypatch) -> None:
    def fake_scan(image: str, _scanner: str, platform: str | None = None):
        assert platform is not None
        low = _finding(image, platform, cve="CVE-low")
        low.severity = Severity.LOW
        return [low, _finding(image, platform, cve="CVE-high")]

    monkeypatch.setattr("scripts.cve_scan.scanner.scan_image", fake_scan)
    findings = scan_images(
        ["valkey/valkey:9.0"],
        "trivy",
        Severity.HIGH,
        platforms=["linux/amd64"],
    )
    assert [finding.cve_id for finding in findings] == ["CVE-high"]
