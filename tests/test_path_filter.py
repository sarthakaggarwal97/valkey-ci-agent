"""Tests for reviewer path filtering."""

from __future__ import annotations

from scripts.models import ChangedFile
from scripts.path_filter import PathFilter


def _file(path: str, *, patch: str | None = "@@ -1 +1 @@\n-old\n+new") -> ChangedFile:
    return ChangedFile(
        path=path,
        status="modified",
        additions=1,
        deletions=1,
        patch=patch,
        contents=None,
        is_binary=patch is None,
    )


def test_path_filter_excludes_generated_binary_and_lock_files() -> None:
    files = [
        _file("src/server.c"),
        _file("vendor/library.c"),
        _file("Cargo.lock"),
        _file("docs/logo.png", patch=None),
    ]

    selected = PathFilter().select(files, [])

    assert [changed_file.path for changed_file in selected] == ["src/server.c"]


def test_path_filter_applies_ordered_patterns() -> None:
    files = [
        _file("src/main.c"),
        _file("src/generated/config.c"),
        _file("tests/test_api.py"),
        _file("docs/readme.md"),
    ]

    selected = PathFilter().select(
        files,
        ["src/**", "tests/**", "!src/generated/**"],
    )

    assert [changed_file.path for changed_file in selected] == [
        "src/main.c",
        "tests/test_api.py",
    ]
