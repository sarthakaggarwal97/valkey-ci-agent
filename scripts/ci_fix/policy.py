"""Deterministic publication policy for CI-fix candidates.

Verification answers whether a patch changes the observed result.  It does not
answer whether an autonomous actor should publish that class of change.
Repository policy therefore makes a separate, code-owned decision:

* authored changes auto-publish only inside explicit safe path patterns;
* workflow, local-action, CODEOWNERS, and other protected paths always require
  human publication;
* historical ports may publish ordinary source changes because the commit was
  already merged on a trusted branch, but protected execution-policy paths
  still require a human.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from pathlib import PurePosixPath

DEFAULT_PROTECTED_PATTERNS = (
    ".github/workflows/**",
    ".github/actions/**",
    ".github/CODEOWNERS",
    "CODEOWNERS",
    "**/CODEOWNERS",
)
DEFAULT_AUTO_PUBLISH_PATTERNS = (
    "tests/**",
    "test/**",
    "**/tests/**",
    "**/test/**",
)


@dataclass(frozen=True)
class PublicationDecision:
    auto_publish: bool
    reason: str


def authored_publication_decision(
    changed_paths: tuple[str, ...],
    *,
    protected_patterns: tuple[str, ...],
    auto_publish_patterns: tuple[str, ...],
) -> PublicationDecision:
    """Decide whether an AI-authored patch may be pushed automatically."""
    invalid = _invalid_paths(changed_paths)
    if invalid:
        return PublicationDecision(
            False,
            f"candidate contains malformed changed paths: {', '.join(invalid)}",
        )
    if not changed_paths:
        return PublicationDecision(False, "candidate contains no changed paths")

    protected = _matching_paths(changed_paths, protected_patterns)
    if protected:
        return PublicationDecision(
            False,
            "candidate touches protected execution or ownership policy: "
            + ", ".join(protected),
        )

    outside = tuple(
        path
        for path in changed_paths
        if not _matches_any(path, auto_publish_patterns)
    )
    if outside:
        return PublicationDecision(
            False,
            "authored changes outside the repository's auto-publish path "
            "allowlist require a human: " + ", ".join(outside),
        )
    return PublicationDecision(
        True,
        "all authored paths are inside the repository's auto-publish allowlist",
    )


def port_publication_decision(
    changed_paths: tuple[str, ...],
    *,
    protected_patterns: tuple[str, ...],
) -> PublicationDecision:
    """Decide whether an already-merged historical port may be pushed."""
    invalid = _invalid_paths(changed_paths)
    if invalid:
        return PublicationDecision(
            False,
            f"candidate contains malformed changed paths: {', '.join(invalid)}",
        )
    if not changed_paths:
        return PublicationDecision(
            False,
            "the historical candidate's changed paths could not be established",
        )
    protected = _matching_paths(changed_paths, protected_patterns)
    if protected:
        return PublicationDecision(
            False,
            "historical fix touches protected execution or ownership policy: "
            + ", ".join(protected),
        )
    return PublicationDecision(
        True,
        "historical commit is merged on a trusted branch and avoids protected paths",
    )


def _matching_paths(
    paths: tuple[str, ...], patterns: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(path for path in paths if _matches_any(path, patterns))


def _matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _invalid_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    invalid = []
    for path in paths:
        pure = PurePosixPath(path)
        if (
            not path
            or path.startswith("/")
            or "\\" in path
            or ".." in pure.parts
        ):
            invalid.append(path or "(empty)")
    return tuple(invalid)
