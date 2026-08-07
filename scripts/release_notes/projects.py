"""Per-repository release conventions for the release-notes cut.

Each supported repository has a ProjectProfile describing where its version
lives and how to rewrite it (the VersionBumper), how its changelog headings are
spelled (display_name), which note categories apply, and how the AI prompts
should describe the project. Profiles are keyed by repository NAME (the part
after the owner), so a cut against a personal fork such as
``someuser/valkey-search`` resolves the same conventions as the upstream repo.

Version layouts by repository:
  valkey        src/version.h  VALKEY_VERSION, VALKEY_VERSION_NUM, VALKEY_RELEASE_STAGE
  valkey-search src/version.h  kModuleVersion constexpr + MODULE_RELEASE_STAGE
  valkey-json   CMakeLists.txt project(<Name> VERSION M.m.p ...)
  valkey-bloom  Cargo.toml     [package] version = "M.m.p"

valkey-json and valkey-bloom record no release stage in their version file, so
their bumpers set ``records_stage = False``: the version-file progression gate
compares M.m.p only (allowing rc2 after rc1 of the same version through), and
the tag-based gate in discover.validate_target_release_tag remains the
authoritative check against re-cutting an already-tagged stage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from scripts.release_notes import release_format as rn
from scripts.release_notes import version_bump as bv


class VersionBumper(Protocol):
    """Reads and rewrites a repository's recorded release version."""

    #: Path of the version file, relative to the repository root.
    version_file: str
    #: Whether the version file records the release stage (ga/rcN/dev).
    #: When False, the progression gate compares M.m.p only.
    records_stage: bool

    def current_release_state(self, text: str) -> tuple[str, str]:
        """Return the canonical ``(version, stage)`` recorded in *text*."""
        ...

    def set_version(self, text: str, version: str, stage: str) -> str:
        """Return *text* with the recorded version (and stage) updated."""
        ...


class ValkeyVersionH:
    """valkey core: src/version.h with the three VALKEY_* macros."""

    version_file = "src/version.h"
    records_stage = True

    def current_release_state(self, text: str) -> tuple[str, str]:
        return bv.current_release_state(text)

    def set_version(self, text: str, version: str, stage: str) -> str:
        return bv.set_version(text, version, stage)


# valkey-search src/version.h:
#   constexpr auto kModuleVersion = vmsdk::ValkeyVersion(1, 2, 1);
#   #define MODULE_RELEASE_STAGE "ga"
_SEARCH_VERSION_RE = re.compile(
    r"^(?P<prefix>\s*constexpr\s+auto\s+kModuleVersion\s*=\s*"
    r"vmsdk::ValkeyVersion\()\s*(?P<major>\d+)\s*,\s*(?P<minor>\d+)\s*,"
    r"\s*(?P<patch>\d+)\s*(?P<suffix>\))",
    re.MULTILINE,
)
_SEARCH_STAGE_RE = re.compile(
    r'^(?P<prefix>#define\s+MODULE_RELEASE_STAGE\s+)"(?P<stage>[^"]*)"',
    re.MULTILINE,
)


class SearchVersionH:
    """valkey-search: src/version.h with kModuleVersion + MODULE_RELEASE_STAGE.

    The 1.0 line keeps its version inline in ``src/module_loader.cc`` instead
    and is deliberately unsupported; a cut against it fails here with a clear
    error rather than guessing at a second layout.
    """

    version_file = "src/version.h"
    records_stage = True

    def current_release_state(self, text: str) -> tuple[str, str]:
        versions = _SEARCH_VERSION_RE.findall(text)
        if len(versions) != 1:
            raise ValueError(
                "expected exactly one kModuleVersion definition in src/version.h, "
                f"found {len(versions)} (the valkey-search 1.0 line keeps its "
                "version in src/module_loader.cc and is not supported)"
            )
        stages = _SEARCH_STAGE_RE.findall(text)
        if len(stages) != 1:
            raise ValueError(
                "expected exactly one MODULE_RELEASE_STAGE definition in "
                f"src/version.h, found {len(stages)}"
            )
        _prefix, major, minor, patch, _suffix = versions[0]
        stage = bv._validate_stage(stages[0][1])
        return f"{int(major)}.{int(minor)}.{int(patch)}", stage

    def set_version(self, text: str, version: str, stage: str) -> str:
        self.current_release_state(text)  # validates both macros exist exactly once
        major, minor, patch = rn.parse_version(version)
        stage = bv._validate_stage(stage)
        text = _SEARCH_VERSION_RE.sub(
            lambda m: f"{m.group('prefix')}{major}, {minor}, {patch}{m.group('suffix')}",
            text,
        )
        return _SEARCH_STAGE_RE.sub(
            lambda m: f'{m.group("prefix")}"{stage}"', text
        )


# CMake: project(<Name> VERSION M.m.p ...). ``project`` is matched
# case-insensitively (CMake command names are case-insensitive);
# cmake_minimum_required(VERSION ...) cannot match because a project name must
# precede the VERSION keyword.
_CMAKE_PROJECT_VERSION_RE = re.compile(
    r"(?P<prefix>\bproject\s*\(\s*[A-Za-z0-9_-]+\s+VERSION\s+)"
    r"(?P<version>\d+\.\d+\.\d+)",
    re.IGNORECASE,
)


class CMakeProjectVersion:
    """valkey-json: the ``project(<Name> VERSION M.m.p ...)`` declaration.

    CMake's VERSION accepts only numeric components, so no release stage is
    recorded; ``current_release_state`` reports ``ga`` and the progression gate
    compares M.m.p only (records_stage is False).
    """

    version_file = "CMakeLists.txt"
    records_stage = False

    def current_release_state(self, text: str) -> tuple[str, str]:
        versions = _CMAKE_PROJECT_VERSION_RE.findall(text)
        if len(versions) != 1:
            raise ValueError(
                "expected exactly one project(<Name> VERSION M.m.p) declaration "
                f"in CMakeLists.txt, found {len(versions)}"
            )
        major, minor, patch = rn.parse_version(versions[0][1])
        return f"{major}.{minor}.{patch}", "ga"

    def set_version(self, text: str, version: str, stage: str) -> str:
        self.current_release_state(text)  # validates exactly one declaration
        major, minor, patch = rn.parse_version(version)
        return _CMAKE_PROJECT_VERSION_RE.sub(
            lambda m: f"{m.group('prefix')}{major}.{minor}.{patch}", text
        )


# The [package] section runs until the next top-level [section] header.
_CARGO_PACKAGE_SECTION_RE = re.compile(
    r"^\[package\]\s*$(?P<body>.*?)(?=^\[|\Z)", re.MULTILINE | re.DOTALL
)
_CARGO_VERSION_RE = re.compile(
    r'^(?P<prefix>version\s*=\s*")(?P<version>\d+\.\d+\.\d+)"',
    re.MULTILINE,
)


class CargoTomlVersion:
    """valkey-bloom: ``version = "M.m.p"`` in Cargo.toml's [package] section.

    Only the [package] section is rewritten, so dependency ``version = "..."``
    entries elsewhere in the file are never touched. No release stage is
    recorded (records_stage is False).
    """

    version_file = "Cargo.toml"
    records_stage = False

    def _package_version_match(self, text: str) -> tuple[re.Match[str], re.Match[str]]:
        section = _CARGO_PACKAGE_SECTION_RE.search(text)
        if section is None:
            raise ValueError("no [package] section found in Cargo.toml")
        body = section.group("body")
        versions = list(_CARGO_VERSION_RE.finditer(body))
        if len(versions) != 1:
            raise ValueError(
                "expected exactly one version = \"M.m.p\" entry in Cargo.toml's "
                f"[package] section, found {len(versions)}"
            )
        return section, versions[0]

    def current_release_state(self, text: str) -> tuple[str, str]:
        _section, version = self._package_version_match(text)
        major, minor, patch = rn.parse_version(version.group("version"))
        return f"{major}.{minor}.{patch}", "ga"

    def set_version(self, text: str, version: str, stage: str) -> str:
        section, match = self._package_version_match(text)
        major, minor, patch = rn.parse_version(version)
        start = section.start("body") + match.start("version")
        end = section.start("body") + match.end("version")
        return f"{text[:start]}{major}.{minor}.{patch}{text[end:]}"


# Category lists shared by the module repos: valkey core's canonical names,
# minus the core-only surfaces (Sentinel/CLI programs/module-API hosting). All
# names stay subsets of core's CATEGORIES so render's catch-all coercion and
# generate's observability guardrail behave identically across profiles.
_MODULE_CATEGORIES: tuple[str, ...] = (
    "Behavior Changes",
    "New Features and Enhanced Behavior",
    "Performance and Efficiency Improvements",
    "Bug Fixes",
    "Command and API Updates",
    "Configuration",
    "Observability and Logging",
    "Build and Tooling",
    "Other Changes",
)

# valkey-search coordinates queries across cluster nodes, so it keeps the
# cluster category.
_SEARCH_CATEGORIES: tuple[str, ...] = (
    "Behavior Changes",
    "New Features and Enhanced Behavior",
    "Performance and Efficiency Improvements",
    "Bug Fixes",
    "Command and API Updates",
    "Cluster and Replication",
    "Configuration",
    "Observability and Logging",
    "Build and Tooling",
    "Other Changes",
)

_CORE_CATEGORY_GUIDANCE = """\
- Prefer the category for the user-visible surface over the fact that the PR is
  a bug fix. `Bug Fixes` is the fallback, not the automatic category for every
  title beginning with "Fix".
- EXCEPTION: a crash, assertion, or memory-safety fix belongs in `Bug Fixes`
  even when the crash happens in an operator-output path (INFO, logging,
  metrics). The severity is the story, not the surface.
- `Observability and Logging` owns INFO fields, metrics, ACL LOG, server logs,
  diagnostics, process titles, and corrections to those outputs.
- `Command and API Updates` owns command arguments/results, wire reply schemas,
  and public APIs. `Module API Changes` owns third-party module APIs.
- `Cluster and Replication` owns cluster, Sentinel, failover, migration, and
  replication behavior unless Configuration or Observability is more specific.
- `Configuration` owns config parsing, validation, persistence, and defaults.
- `Build and Tooling` is for shipped build/packaging/tool changes; test-only and
  CI-only PRs should be skipped.
"""

_MODULE_CATEGORY_GUIDANCE = """\
- Prefer the category for the user-visible surface over the fact that the PR is
  a bug fix. `Bug Fixes` is the fallback, not the automatic category for every
  title beginning with "Fix".
- `Command and API Updates` owns the module's command arguments/results, reply
  schemas, and ACL categories.
- `Configuration` owns module config parsing, validation, and defaults.
- `Observability and Logging` owns INFO fields, metrics, logs, and corrections
  to those outputs.
- `Build and Tooling` is for shipped build/packaging changes; test-only and
  CI-only PRs should be skipped.
"""

_SEARCH_CATEGORY_GUIDANCE = _MODULE_CATEGORY_GUIDANCE + """\
- `Cluster and Replication` owns cluster-mode query fanout, coordinator
  behavior, and replication of index state.
"""


@dataclass(frozen=True)
class ProjectProfile:
    """Release conventions for one supported repository."""

    name: str                # repository name, e.g. "valkey-search"
    display_name: str        # changelog heading name, e.g. "Valkey Search"
    bumper: VersionBumper
    prompt_description: str  # names the project in the AI prompts ("<Name>, a ...")
    category_guidance: str   # category-boundary guidance for the generation prompt
    notes_file: str = "00-RELEASENOTES"
    categories: tuple[str, ...] = tuple(rn.CATEGORIES)


VALKEY_PROFILE = ProjectProfile(
    name="valkey",
    display_name="Valkey",
    bumper=ValkeyVersionH(),
    prompt_description="Valkey, a production key-value datastore",
    category_guidance=_CORE_CATEGORY_GUIDANCE,
)

_PROFILES: dict[str, ProjectProfile] = {
    profile.name: profile
    for profile in (
        VALKEY_PROFILE,
        ProjectProfile(
            name="valkey-search",
            display_name="Valkey Search",
            bumper=SearchVersionH(),
            prompt_description=(
                "Valkey Search, a Valkey module that provides vector similarity "
                "search, full-text search, and secondary indexing (the FT.* "
                "commands) for the Valkey key-value datastore"
            ),
            categories=_SEARCH_CATEGORIES,
            category_guidance=_SEARCH_CATEGORY_GUIDANCE,
        ),
        ProjectProfile(
            name="valkey-json",
            display_name="Valkey JSON",
            bumper=CMakeProjectVersion(),
            prompt_description=(
                "Valkey JSON, a Valkey module that provides a native JSON data "
                "type with JSONPath queries (the JSON.* commands) for the Valkey "
                "key-value datastore"
            ),
            categories=_MODULE_CATEGORIES,
            category_guidance=_MODULE_CATEGORY_GUIDANCE,
        ),
        ProjectProfile(
            name="valkey-bloom",
            display_name="Valkey Bloom",
            bumper=CargoTomlVersion(),
            prompt_description=(
                "Valkey Bloom, a Valkey module that provides bloom filter data "
                "types (the BF.* commands) for the Valkey key-value datastore"
            ),
            categories=_MODULE_CATEGORIES,
            category_guidance=_MODULE_CATEGORY_GUIDANCE,
        ),
    )
}


def profile_for(repo_full_name: str) -> ProjectProfile:
    """Resolve the ProjectProfile for ``owner/name`` (or a bare ``name``).

    Keyed by repository name only, so personal forks resolve the upstream
    conventions. Raises ValueError for an unsupported repository.
    """
    name = repo_full_name.strip().rstrip("/").rsplit("/", 1)[-1].lower()
    profile = _PROFILES.get(name)
    if profile is None:
        supported = ", ".join(sorted(_PROFILES))
        raise ValueError(
            f"unsupported repository {repo_full_name!r} for a release-notes cut; "
            f"supported repositories: {supported}"
        )
    return profile
