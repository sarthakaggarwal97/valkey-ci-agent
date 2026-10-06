"""Write a small JSON file atomically and privately."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def write_json_atomic(path: str | Path, payload: Any) -> None:
    """Replace ``path`` with ``payload`` as JSON, readable only by this user.

    Written to a temporary file in the same directory and renamed into place,
    so a reader never sees a partial file and a crash leaves the old one.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", text=True)
    temporary_path = Path(temporary)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary_path, target)
    finally:
        temporary_path.unlink(missing_ok=True)
