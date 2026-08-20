from __future__ import annotations

import base64
import json

import pytest

from scripts.cve_scan.config import CveScanSettings
from scripts.cve_scan.models import Finding, Severity
from scripts.cve_scan.scanner import ScanError
from scripts.cve_scan.sweep import run_sweep


def _settings() -> CveScanSettings:
    return CveScanSettings(
        versions_url="https://example.test/versions.json",
        repository="valkey/valkey",
        include_unstable=False,
        scanner="trivy",
        severity_threshold=Severity.HIGH,
        platforms=["linux/amd64", "linux/arm64"],
    )


def _finding(*, fixed: str | None, platform: str = "linux/amd64") -> Finding:
    return Finding(
        image="valkey/valkey:9.0",
        package="libssl3t64",
        installed_version="3.0.17-1",
        cve_id="CVE-2026-0001",
        severity=Severity.HIGH,
        fixed_version=fixed,
        platform=platform,
    )


def _outputs(path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


def test_published_fix_emits_exact_downstream_contract(monkeypatch, tmp_path) -> None:
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("scripts.cve_scan.sweep.resolve_matrix", lambda _settings: ["valkey/valkey:9.0"])
    monkeypatch.setattr(
        "scripts.cve_scan.sweep.scan_images",
        lambda *_args, **_kwargs: [_finding(fixed="3.0.18-1")],
    )

    run_sweep(settings=_settings())

    values = _outputs(output)
    assert values["rebuild_required"] == "true"
    assert values["versions"] == "9.0"
    targets = json.loads(base64.b64decode(values["targets"]))
    assert targets == [{
        "cve_id": "CVE-2026-0001",
        "image": "valkey/valkey:9.0",
        "package": "libssl3t64",
        "platform": "linux/amd64",
    }]
    assert "does not predict rebuild behavior" in summary.read_text()


def test_no_published_fix_reports_without_dispatch(monkeypatch, tmp_path) -> None:
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("scripts.cve_scan.sweep.resolve_matrix", lambda _settings: ["valkey/valkey:9.0"])
    monkeypatch.setattr(
        "scripts.cve_scan.sweep.scan_images",
        lambda *_args, **_kwargs: [_finding(fixed=None)],
    )

    run_sweep(settings=_settings())

    values = _outputs(output)
    assert values == {"rebuild_required": "false", "versions": "", "targets": ""}
    assert "without a published fix" in summary.read_text()


def test_dry_run_preserves_targets_but_suppresses_dispatch(monkeypatch, tmp_path) -> None:
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr("scripts.cve_scan.sweep.resolve_matrix", lambda _settings: ["valkey/valkey:9.0"])
    monkeypatch.setattr(
        "scripts.cve_scan.sweep.scan_images",
        lambda *_args, **_kwargs: [_finding(fixed="3.0.18-1")],
    )

    run_sweep(settings=_settings(), dry_run=True)

    values = _outputs(output)
    assert values["rebuild_required"] == "false"
    assert values["versions"] == "9.0"
    assert values["targets"]


def test_scan_failure_emits_false_and_failure_summary(monkeypatch, tmp_path) -> None:
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("scripts.cve_scan.sweep.resolve_matrix", lambda _settings: ["valkey/valkey:9.0"])

    def fail(*_args, **_kwargs):
        raise ScanError("linux/arm64 failed")

    monkeypatch.setattr("scripts.cve_scan.sweep.scan_images", fail)
    with pytest.raises(ScanError):
        run_sweep(settings=_settings())

    assert _outputs(output)["rebuild_required"] == "false"
    assert "complete scan coverage could not be proven" in summary.read_text()


def test_version_lines_strip_only_alpine_suffix(monkeypatch, tmp_path) -> None:
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr(
        "scripts.cve_scan.sweep.resolve_matrix",
        lambda _settings: ["valkey/valkey:8.1-alpine", "valkey/valkey:9.0"],
    )
    findings = [
        Finding(
            image=image,
            package="pkg",
            installed_version="1",
            cve_id=f"CVE-2026-{index}",
            severity=Severity.HIGH,
            fixed_version="2",
            platform="linux/amd64",
        )
        for index, image in enumerate(("valkey/valkey:8.1-alpine", "valkey/valkey:9.0"), 1)
    ]
    monkeypatch.setattr("scripts.cve_scan.sweep.scan_images", lambda *_args, **_kwargs: findings)

    run_sweep(settings=_settings())
    assert _outputs(output)["versions"] == "8.1 9.0"
