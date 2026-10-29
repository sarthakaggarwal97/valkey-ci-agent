"""Path rules shared by CI-fix planning and trusted workspace preparation."""

from __future__ import annotations

from pathlib import PurePosixPath


def checkout_subdirectory(value: str) -> PurePosixPath:
    """Return a canonical, non-control checkout subdirectory or raise."""
    path = PurePosixPath(value)
    folded_parts = tuple(part.casefold() for part in path.parts)
    if (
        not value
        or value != path.as_posix()
        or path.is_absolute()
        or path == PurePosixPath(".")
        or ".." in path.parts
        or "\\" in value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
        or ".git" in folded_parts
        or folded_parts[0] == ".ci-fix-agent"
    ):
        raise ValueError(f"{value!r} is not a safe repository subdirectory")
    return path


def is_checkout_subdirectory(value: str) -> bool:
    """Whether ``value`` satisfies :func:`checkout_subdirectory`."""
    try:
        checkout_subdirectory(value)
    except ValueError:
        return False
    return True
