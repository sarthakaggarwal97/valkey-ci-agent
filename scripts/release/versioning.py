"""Deterministic version derivation from a release branch and existing tags.

Given a release branch (``M.m``), the operator's intent (rc/ga/patch), and the
repository's existing tags, exactly one next version and stage follow. An
optional policy-gated target may skip exactly one unpublished patch only when
the release branch source already records that skipped version.

Valkey's tag model (mirrored from the release-notes cut):

    M.m.p       final release (stage "ga")
    M.m.p-rcN   release candidate, N >= 1

Only tags on the requested release line are considered; tags in any other
format are ignored rather than guessed at.
"""

from __future__ import annotations

import re
from typing import Iterable

from scripts.release.models import DerivedRelease, ReleaseIntent

# No leading zeros: git treats 9.01 and 9.1 as distinct refs, so accepting
# a zero-padded component would derive versions for the wrong branch, and a
# zero-padded tag (9.01.0) would be counted onto the wrong release line.
_BRANCH_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)$")
_FINAL_TAG_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
_RC_TAG_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)-rc([1-9]\d*)$")


def parse_release_branch(branch: str) -> tuple[int, int]:
    """Return ``(major, minor)`` for an ``M.m`` release branch.

    Raises :class:`ValueError` for anything else (``unstable``, ``main``, a
    full ``M.m.p`` version, ...), so a wrong-branch dispatch fails before any
    GitHub state is touched.
    """
    m = _BRANCH_RE.match(branch.strip())
    if not m:
        raise ValueError(f"not a release branch: {branch!r} (expected MAJOR.MINOR, e.g. '9.1')")
    return int(m.group(1)), int(m.group(2))


def derive_version(branch: str, intent: ReleaseIntent, tags: Iterable[str]) -> DerivedRelease:
    """Derive the next version and stage for *branch* from existing *tags*.

    Pure and deterministic: the same branch, intent, and tag set always yield
    the same result. Raises :class:`ValueError` when the intent is impossible
    for the branch's tag state (e.g. an RC after the line already shipped a
    final release), so the impossibility is reported instead of guessed around.
    """
    major, minor = parse_release_branch(branch)

    finals: list[int] = []  # patch numbers of final releases on this line
    initial_rcs: list[int] = []  # rc numbers of M.m.0 release candidates
    for tag in tags:
        f = _FINAL_TAG_RE.match(tag)
        if f and (int(f.group(1)), int(f.group(2))) == (major, minor):
            finals.append(int(f.group(3)))
            continue
        r = _RC_TAG_RE.match(tag)
        if r and (int(r.group(1)), int(r.group(2)), int(r.group(3))) == (major, minor, 0):
            initial_rcs.append(int(r.group(4)))

    # Any final release on the line closes the rc/ga window: deriving M.m.0
    # while e.g. M.m.1 exists would produce a version *lower* than a shipped
    # release (reachable when the .0 tag was deleted or the line was seeded
    # at .1), so the guard is "any final", not "the .0 final".
    line_released = bool(finals)

    if intent is ReleaseIntent.RC:
        if line_released:
            raise ValueError(
                f"{branch} already has a final release ({branch}.{max(finals)}); "
                f"release candidates only precede the initial release of a line "
                f"(use intent 'patch')"
            )
        next_rc = max(initial_rcs) + 1 if initial_rcs else 1
        return DerivedRelease(version=f"{major}.{minor}.0", stage=f"rc{next_rc}")

    if intent is ReleaseIntent.GA:
        if line_released:
            raise ValueError(
                f"{branch} already has a final release ({branch}.{max(finals)}); "
                f"use intent 'patch' for the next release on this line"
            )
        return DerivedRelease(version=f"{major}.{minor}.0", stage="ga")

    if intent is ReleaseIntent.PATCH:
        if not finals:
            raise ValueError(
                f"no final release exists on {branch}; a patch requires an initial release (use intent 'rc' or 'ga')"
            )
        return DerivedRelease(version=f"{major}.{minor}.{max(finals) + 1}", stage="ga")

    raise ValueError(f"cannot derive a version for intent {intent.value!r}")


def resolve_target_version(
    branch: str,
    intent: ReleaseIntent,
    tags: Iterable[str],
    *,
    source_version: str,
    source_stage: str,
    target_version: str,
) -> DerivedRelease:
    """Validate an explicit target that skips one unpublished patch.

    Normal releases remain tag-derived. The escape hatch is deliberately
    narrow: patch intent only, exact unpadded version syntax, the same release
    line, source at the automatically derived unpublished patch, and a target
    exactly one patch later.
    """
    if target_version and intent is not ReleaseIntent.PATCH:
        raise ValueError("an explicit target version is allowed only for patch releases")
    derived = derive_version(branch, intent, tags)
    if not target_version:
        return derived

    target = _FINAL_TAG_RE.fullmatch(target_version)
    if not target:
        raise ValueError("target version must be exact unpadded MAJOR.MINOR.PATCH")
    target_parts = tuple(int(target.group(index)) for index in range(1, 4))
    if target_parts[:2] != parse_release_branch(branch):
        raise ValueError(f"target version {target_version} does not belong to branch {branch}")

    expected = _FINAL_TAG_RE.fullmatch(derived.version)
    source = _FINAL_TAG_RE.fullmatch(source_version)
    if expected is None or source is None:
        raise ValueError("derived and source versions must use exact unpadded MAJOR.MINOR.PATCH syntax")
    expected_parts = tuple(int(expected.group(index)) for index in range(1, 4))
    source_parts = tuple(int(source.group(index)) for index in range(1, 4))
    if source_stage != "ga" or source_parts != expected_parts:
        raise ValueError(
            f"branch source must record unpublished {derived.version} at stage ga before it can be skipped"
        )
    if target_parts != (*expected_parts[:2], expected_parts[2] + 1):
        raise ValueError(
            f"target version must be exactly one patch after unpublished {derived.version}"
        )
    return DerivedRelease(version=target_version, stage="ga")
