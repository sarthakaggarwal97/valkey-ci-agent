"""Resolve the complete published image matrix from versions.json."""

from __future__ import annotations

import json
import logging
import re
import urllib.request
from typing import TYPE_CHECKING
from urllib.error import URLError

if TYPE_CHECKING:
    from scripts.cve_scan.config import CveScanSettings

logger = logging.getLogger(__name__)

_FETCH_TIMEOUT_SECONDS = 15
_VERSION_KEY_RE = re.compile(r"^\d+\.\d+$")
_REQUIRED_VARIANTS = ("debian", "alpine")


class MatrixResolutionError(Exception):
    """Raised when the manifest cannot prove complete scan coverage."""


def _fetch_versions_json(url: str) -> dict[str, object]:
    """Fetch a non-empty JSON object or fail closed."""
    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "valkey-ci-agent/cve-scan"},
        )
        with urllib.request.urlopen(request, timeout=_FETCH_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise MatrixResolutionError(
                    f"Failed to fetch versions manifest: HTTP {response.status} from {url}"
                )
            body = response.read().decode("utf-8")
    except (URLError, OSError, TimeoutError) as exc:
        raise MatrixResolutionError(
            f"Failed to fetch versions manifest from {url}: {exc}"
        ) from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise MatrixResolutionError(
            f"Invalid JSON in versions manifest from {url}: {exc}"
        ) from exc
    if not isinstance(payload, dict) or not payload:
        raise MatrixResolutionError("Versions manifest must be a non-empty JSON object")
    return payload


def _validate_variant(version_key: str, variant: str, value: object) -> None:
    """Validate the variant shape used by valkey-container."""
    if not isinstance(value, dict):
        raise MatrixResolutionError(
            f"Manifest entry {version_key!r}.{variant} must be an object"
        )
    version = value.get("version")
    if not isinstance(version, str) or not version.strip():
        raise MatrixResolutionError(
            f"Manifest entry {version_key!r}.{variant}.version must be a non-empty string"
        )


def _derive_images(
    versions: dict[str, object],
    repository: str,
    include_unstable: bool,
) -> list[str]:
    """Derive both required image variants for every release entry."""
    images: list[str] = []
    for version_key, value in versions.items():
        if version_key == "unstable" and not include_unstable:
            continue
        if version_key != "unstable" and not _VERSION_KEY_RE.fullmatch(version_key):
            raise MatrixResolutionError(f"Unexpected versions manifest key {version_key!r}")
        if not isinstance(value, dict):
            raise MatrixResolutionError(
                f"Manifest release entry {version_key!r} must be an object"
            )
        for variant in _REQUIRED_VARIANTS:
            if variant not in value:
                raise MatrixResolutionError(
                    f"Manifest release entry {version_key!r} is missing required variant {variant!r}"
                )
            _validate_variant(version_key, variant, value[variant])
        images.extend([
            f"{repository}:{version_key}",
            f"{repository}:{version_key}-alpine",
        ])
    if not images:
        raise MatrixResolutionError("Dynamic resolution produced zero images")
    return sorted(images)


def resolve_matrix(settings: CveScanSettings) -> list[str]:
    """Return static overrides or the complete validated dynamic image list."""
    if settings.images:
        logger.info("Using static image override: %s", ", ".join(settings.images))
        return settings.images

    versions = _fetch_versions_json(settings.versions_url)
    images = _derive_images(
        versions,
        settings.repository,
        settings.include_unstable,
    )
    logger.info("Resolved %d image(s): %s", len(images), ", ".join(images))
    return images
