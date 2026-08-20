"""Tests for the per-repository project profiles and version bumpers."""

from __future__ import annotations

import inspect
import re

import pytest

from scripts.release_notes import generate, projects, render, triage
from scripts.release_notes import release_cut as cut_mod
from scripts.release_notes import release_format as rn

_SEARCH_VERSION_H = """\
#ifndef VALKEYSEARCH_SRC_VERSION_H_
#define VALKEYSEARCH_SRC_VERSION_H_

#include "utils.h"

//
// Set the module version to the current release
//
constexpr auto kModuleVersion = vmsdk::ValkeyVersion(1, 2, 1);

/* The release stage is used in order to provide release status information. */
#define MODULE_RELEASE_STAGE "ga"

constexpr auto kMinimumServerVersion = vmsdk::ValkeyVersion(9, 0, 1);

#endif
"""

_JSON_CMAKELISTS = """\
cmake_minimum_required(VERSION 3.17)

project(ValkeyJSONModule VERSION 1.0.2 LANGUAGES C CXX)

if(NOT VALKEY_VERSION)
    set(VALKEY_VERSION unstable)
endif()
message("Valkey version: ${VALKEY_VERSION}")
"""

_BLOOM_CARGO_TOML = """\
[package]
name = "valkey-bloom"
authors = ["Karthik Subbarao"]
version = "1.0.1"
edition = "2021"

[dependencies]
valkey-module = { version = "0.1.5", features = ["min-valkey-compatibility-version-8-0"]}
libc = "0.2"

[dev-dependencies]
version = "9.9.9"
"""


# ---------------------------------------------------------------------------
# profile_for


def test_profile_for_resolves_upstream_and_fork_owners() -> None:
    assert projects.profile_for("valkey-io/valkey").display_name == "Valkey"
    assert projects.profile_for("valkey-io/valkey-search").display_name == "Valkey Search"
    assert projects.profile_for("someuser/valkey-json").display_name == "Valkey JSON"
    assert projects.profile_for("valkey-bloom").display_name == "Valkey Bloom"


def test_profile_for_rejects_unsupported_repo() -> None:
    with pytest.raises(ValueError, match="unsupported repository"):
        projects.profile_for("valkey-io/valkey-ldap")


def test_valkey_profile_keeps_core_conventions() -> None:
    profile = projects.profile_for("valkey-io/valkey")
    assert profile.notes_file == "00-RELEASENOTES"
    assert profile.bumper.version_file == "src/version.h"
    assert profile.bumper.records_stage is True
    assert profile.categories == tuple(rn.CATEGORIES)
    assert "Cluster and Replication" in profile.category_guidance


def test_profile_guidance_names_only_profile_categories() -> None:
    # Guidance naming a category outside the profile's list would steer the
    # model toward bullets the renderer coerces into the catch-all.
    for name in ("valkey", "valkey-search", "valkey-json", "valkey-bloom"):
        profile = projects.profile_for(name)
        named = re.findall(r"`([^`]+)`", profile.category_guidance)
        for category in named:
            if category in rn.CATEGORIES:
                assert category in profile.categories, (
                    f"{name} guidance names {category!r}, absent from its categories"
                )


@pytest.mark.parametrize(
    ("function", "parameter"),
    [
        (generate.build_prompt, "project_description"),
        (generate.build_prompt, "category_guidance"),
        (generate.generate, "project_description"),
        (generate.generate, "category_guidance"),
        (triage.build_prompt, "project_description"),
        (triage.triage, "project_description"),
        (cut_mod.commit_title, "display_name"),
        (cut_mod.validate_release_progression, "bumper"),
        (rn.unrecognized_categories, "categories"),
        (rn.render_header, "display_name"),
        (rn.render_version_section, "display_name"),
        (rn.render_version_section, "categories"),
        (rn.render_release_notes, "display_name"),
        (rn.render_release_notes, "categories"),
        (render.group_bullets, "categories"),
        (projects.ProjectProfile, "categories"),
        (projects.ProjectProfile, "notes_file"),
    ],
)
def test_profile_sensitive_parameters_are_required(function, parameter) -> None:
    assert inspect.signature(function).parameters[parameter].default is inspect.Parameter.empty


def test_module_categories_are_subsets_of_core() -> None:
    core = set(rn.CATEGORIES)
    for name in ("valkey-search", "valkey-json", "valkey-bloom"):
        profile = projects.profile_for(name)
        assert set(profile.categories) <= core
        assert "Cluster and Replication" in profile.categories
        assert rn.CATCH_ALL_CATEGORY in profile.categories


# ---------------------------------------------------------------------------
# SearchVersionH


def test_search_bumper_reads_current_state() -> None:
    bumper = projects.SearchVersionH()
    assert bumper.current_release_state(_SEARCH_VERSION_H) == ("1.2.1", "ga")


def test_search_bumper_sets_version_and_stage() -> None:
    bumper = projects.SearchVersionH()
    updated = bumper.set_version(_SEARCH_VERSION_H, "1.2.2", "rc1")
    assert "constexpr auto kModuleVersion = vmsdk::ValkeyVersion(1, 2, 2);" in updated
    assert '#define MODULE_RELEASE_STAGE "rc1"' in updated
    # Untouched macros keep their values.
    assert "kMinimumServerVersion = vmsdk::ValkeyVersion(9, 0, 1)" in updated
    assert bumper.current_release_state(updated) == ("1.2.2", "rc1")


def test_search_bumper_rejects_missing_module_version() -> None:
    # The 1.0 line keeps its version in src/module_loader.cc, not version.h.
    bumper = projects.SearchVersionH()
    with pytest.raises(ValueError, match="module_loader.cc"):
        bumper.current_release_state("#define SOMETHING_ELSE 1\n")


def test_search_bumper_rejects_bad_stage() -> None:
    bumper = projects.SearchVersionH()
    with pytest.raises(ValueError, match="release stage"):
        bumper.set_version(_SEARCH_VERSION_H, "1.2.2", "beta")


# ---------------------------------------------------------------------------
# CMakeProjectVersion


def test_cmake_bumper_reads_current_state() -> None:
    bumper = projects.CMakeProjectVersion()
    assert bumper.current_release_state(_JSON_CMAKELISTS) == ("1.0.2", "ga")


def test_cmake_bumper_recognizes_json_unstable_sentinel() -> None:
    bumper = projects.CMakeProjectVersion()
    unstable = _JSON_CMAKELISTS.replace("VERSION 1.0.2", "VERSION 99.99.99")
    assert bumper.current_release_state(unstable) == ("99.99.99", "dev")
    cut_mod.validate_release_progression(unstable, "2.0.0", "rc1", bumper=bumper)


def test_cmake_bumper_sets_version_only() -> None:
    bumper = projects.CMakeProjectVersion()
    updated = bumper.set_version(_JSON_CMAKELISTS, "1.0.3", "rc1")
    assert "project(ValkeyJSONModule VERSION 1.0.3 LANGUAGES C CXX)" in updated
    # cmake_minimum_required(VERSION ...) is not a project version.
    assert "cmake_minimum_required(VERSION 3.17)" in updated


def test_cmake_bumper_rejects_missing_project_version() -> None:
    bumper = projects.CMakeProjectVersion()
    with pytest.raises(ValueError, match="exactly one project"):
        bumper.current_release_state("cmake_minimum_required(VERSION 3.17)\nproject(Thing)\n")


# ---------------------------------------------------------------------------
# CargoTomlVersion


def test_cargo_bumper_reads_current_state() -> None:
    bumper = projects.CargoTomlVersion()
    assert bumper.current_release_state(_BLOOM_CARGO_TOML) == ("1.0.1", "ga")


def test_cargo_bumper_reads_and_replaces_bloom_unstable_version() -> None:
    bumper = projects.CargoTomlVersion()
    unstable = _BLOOM_CARGO_TOML.replace('version = "1.0.1"', 'version = "99.99.99-dev"')
    assert bumper.current_release_state(unstable) == ("99.99.99", "dev")
    cut_mod.validate_release_progression(unstable, "2.0.0", "rc1", bumper=bumper)
    updated = bumper.set_version(unstable, "2.0.0", "rc1")
    assert 'version = "2.0.0"' in updated
    assert "99.99.99-dev" not in updated


def test_cargo_bumper_rewrites_only_the_package_version() -> None:
    bumper = projects.CargoTomlVersion()
    updated = bumper.set_version(_BLOOM_CARGO_TOML, "1.0.2", "ga")
    assert 'version = "1.0.2"' in updated
    # Dependency and other-section version keys are untouched.
    assert 'valkey-module = { version = "0.1.5"' in updated
    assert 'version = "9.9.9"' in updated
    assert bumper.current_release_state(updated) == ("1.0.2", "ga")


def test_cargo_bumper_rejects_missing_package_section() -> None:
    bumper = projects.CargoTomlVersion()
    with pytest.raises(ValueError, match=r"\[package\]"):
        bumper.current_release_state('[dependencies]\nversion = "1.0.0"\n')


@pytest.mark.parametrize(
    ("bumper", "text"),
    [
        (projects.CMakeProjectVersion(), _JSON_CMAKELISTS),
        (projects.CargoTomlVersion(), _BLOOM_CARGO_TOML),
    ],
)
def test_stage_less_bumpers_reject_invalid_stage(bumper, text: str) -> None:
    with pytest.raises(ValueError, match="release stage"):
        bumper.set_version(text, "1.0.3", "beta")


# ---------------------------------------------------------------------------
# Progression validation through a profile bumper


def test_progression_stageless_bumper_allows_equal_version() -> None:
    # rc2 after rc1 of the same version leaves CMakeLists/Cargo.toml unchanged;
    # the tag gate is the authoritative duplicate check for these repos.
    bumper = projects.CMakeProjectVersion()
    cut_mod.validate_release_progression(
        _JSON_CMAKELISTS, "1.0.2", "rc2", bumper=bumper
    )
    cut_mod.validate_release_progression(
        _JSON_CMAKELISTS, "1.0.3", "ga", bumper=bumper
    )


def test_progression_stageless_bumper_rejects_backward_version() -> None:
    bumper = projects.CargoTomlVersion()
    with pytest.raises(ValueError, match="backward"):
        cut_mod.validate_release_progression(
            _BLOOM_CARGO_TOML, "1.0.0", "ga", bumper=bumper
        )


def test_progression_search_bumper_orders_stages() -> None:
    bumper = projects.SearchVersionH()
    cut_mod.validate_release_progression(
        _SEARCH_VERSION_H, "1.2.2", "rc1", bumper=bumper
    )
    with pytest.raises(ValueError, match="must be newer"):
        cut_mod.validate_release_progression(
            _SEARCH_VERSION_H, "1.2.1", "ga", bumper=bumper
        )


def test_progression_search_dev_stage_allows_same_version_rc() -> None:
    # A fresh M.m branch forked from main carries stage "dev"; rc1 of that
    # version must pass the gate.
    dev_text = _SEARCH_VERSION_H.replace('"ga"', '"dev"').replace(
        "ValkeyVersion(1, 2, 1)", "ValkeyVersion(1, 3, 0)"
    )
    bumper = projects.SearchVersionH()
    assert bumper.current_release_state(dev_text) == ("1.3.0", "dev")
    cut_mod.validate_release_progression(dev_text, "1.3.0", "rc1", bumper=bumper)
