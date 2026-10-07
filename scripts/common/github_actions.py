"""Small helpers for GitHub Actions file commands."""

from __future__ import annotations

import os
from collections.abc import Mapping


def write_outputs(
    values: Mapping[str, str],
    *,
    print_if_unset: bool = False,
) -> bool:
    """Write single-line workflow outputs, returning whether a file was used."""
    path = os.environ.get("GITHUB_OUTPUT", "")
    if not path and not print_if_unset:
        return False

    for key, value in values.items():
        if "\n" in value or "\r" in value:
            raise ValueError(f"multiline workflow output refused for {key}")

    if not path:
        for key, value in values.items():
            print(f"{key}={value}")
        return False

    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")
    return True
