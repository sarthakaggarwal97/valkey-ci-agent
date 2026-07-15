"""Replay-relevant identity for the trusted coordinator runtime."""

from __future__ import annotations

import os
import platform
from pathlib import Path
from typing import Any

from scripts.common.phase_artifact import ArtifactError, sha256_file

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_LOCK_FILE = _REPOSITORY_ROOT / "requirements" / "runtime.txt"


def agent_execution_identity() -> dict[str, Any]:
    """Return immutable dependencies plus the observable runner identity."""
    if not _LOCK_FILE.is_file():
        raise ArtifactError("runtime dependency lock is unavailable")
    lock_sha, lock_bytes = sha256_file(_LOCK_FILE, max_bytes=4 * 1024 * 1024)
    return {
        "dependency_lock_file": "requirements/runtime.txt",
        "dependency_lock_sha256": lock_sha,
        "dependency_lock_bytes": lock_bytes,
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "agent_commit": os.environ.get("GITHUB_SHA", "").strip().lower(),
        "runner": {
            "os": os.environ.get("RUNNER_OS", platform.system()).strip(),
            "architecture": os.environ.get(
                "RUNNER_ARCH",
                platform.machine(),
            ).strip(),
            "name": os.environ.get("RUNNER_NAME", "").strip(),
            "environment": os.environ.get("RUNNER_ENVIRONMENT", "").strip(),
            "image_os": os.environ.get("ImageOS", "").strip(),
            "image_version": os.environ.get("ImageVersion", "").strip(),
        },
    }
