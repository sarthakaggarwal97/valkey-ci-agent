from __future__ import annotations

import json
from dataclasses import replace
from io import BytesIO
from unittest.mock import patch

import pytest

from scripts.cve_scan.config import CveScanSettings
from scripts.cve_scan.image_matrix import MatrixResolutionError, resolve_matrix
from scripts.cve_scan.models import Severity


def _settings(**overrides: object) -> CveScanSettings:
    value = CveScanSettings(
        versions_url="https://example.test/versions.json",
        repository="valkey/valkey",
        include_unstable=False,
        scanner="trivy",
        severity_threshold=Severity.HIGH,
        platforms=["linux/amd64"],
    )
    return replace(value, **overrides)


def _manifest(**entries: object) -> dict[str, object]:
    if entries:
        return entries
    return {
        "8.1": {
            "version": "8.1.9",
            "debian": {"version": "trixie"},
            "alpine": {"version": "3.24"},
        },
        "9.0": {
            "version": "9.0.5",
            "debian": {"version": "trixie"},
            "alpine": {"version": "3.24"},
        },
        "unstable": {
            "version": "unstable",
            "debian": {"version": "trixie"},
            "alpine": {"version": "3.24"},
        },
    }


def _response(payload: object) -> BytesIO:
    response = BytesIO(json.dumps(payload).encode())
    response.status = 200  # type: ignore[attr-defined]
    return response


def test_static_override_does_not_fetch() -> None:
    settings = _settings(images=["valkey/valkey:9.0"])
    with patch("urllib.request.urlopen") as urlopen:
        assert resolve_matrix(settings) == ["valkey/valkey:9.0"]
    urlopen.assert_not_called()


def test_dynamic_matrix_requires_both_variants_and_excludes_unstable() -> None:
    with patch("urllib.request.urlopen", return_value=_response(_manifest())):
        images = resolve_matrix(_settings())
    assert images == [
        "valkey/valkey:8.1",
        "valkey/valkey:8.1-alpine",
        "valkey/valkey:9.0",
        "valkey/valkey:9.0-alpine",
    ]


def test_include_unstable_adds_both_variants() -> None:
    with patch("urllib.request.urlopen", return_value=_response(_manifest())):
        images = resolve_matrix(_settings(include_unstable=True))
    assert "valkey/valkey:unstable" in images
    assert "valkey/valkey:unstable-alpine" in images


@pytest.mark.parametrize(
    "payload",
    [
        {"9.0": "corrupt", "8.1": _manifest()["8.1"]},
        {"9.0": {"debian": {"version": "trixie"}}},
        {"9.0": {"debian": {"version": "trixie"}, "alpine": {}}},
        {"metadata": {}, "9.0": _manifest()["9.0"]},
        {},
        [],
    ],
)
def test_malformed_or_incomplete_manifest_fails_closed(payload: object) -> None:
    with patch("urllib.request.urlopen", return_value=_response(payload)):
        with pytest.raises(MatrixResolutionError):
            resolve_matrix(_settings())


def test_custom_repository_is_used() -> None:
    with patch("urllib.request.urlopen", return_value=_response({"9.0": _manifest()["9.0"]})):
        images = resolve_matrix(_settings(repository="example/valkey"))
    assert images == ["example/valkey:9.0", "example/valkey:9.0-alpine"]
