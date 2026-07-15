from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from scripts.backport.registry import (
    BranchEntry,
    Registry,
    ValidationWaiver,
    load_registry,
)

_IMAGE = "gcc@sha256:" + "a" * 64


def _validation() -> dict:
    return {
        "adapter": "container-argv-v1",
        "image": _IMAGE,
        "platform": "linux/amd64",
        "network": "none",
        "resources": {
            "cpus": 2,
            "memory_mb": 1024,
            "pids": 128,
            "output_bytes": 65536,
            "tmpfs_mb": 64,
        },
        "default_commands": ["build"],
        "commands": [
            {
                "id": "build",
                "argv": ["make", "-j2"],
                "working_directory": ".",
                "timeout_seconds": 600,
                "inputs": ["**"],
                "expected_artifacts": ["src/server"],
            },
            {
                "id": "cluster-tests",
                "argv": ["./runtest", "--single", "cluster"],
                "working_directory": ".",
                "timeout_seconds": 900,
                "inputs": ["src/**", "tests/**"],
                "expected_artifacts": [],
            },
        ],
        "rules": [
            {
                "paths": ["src/cluster*.c"],
                "command_ids": ["cluster-tests"],
            },
        ],
    }


def _waiver() -> dict:
    return {
        "reason": "Unit-test repository has no build system.",
        "approved_by": "test suite",
        "expires": "2099-01-01",
    }


def _repo(**overrides) -> dict:
    value = {
        "repo": "org/repo",
        "project_owner": "org",
        "project_owner_type": "organization",
        "language": "c",
        "validation_waiver": _waiver(),
        "branches": [{"branch": "1.0", "project_number": 1}],
    }
    if "validation" in overrides and "validation_waiver" not in overrides:
        value.pop("validation_waiver")
    value.update(overrides)
    return value


def _registry(**overrides) -> dict:
    value = {"schema_version": 2, "repos": [_repo()]}
    value.update(overrides)
    return value


def _write(tmp_path, value) -> str:
    path = tmp_path / "repos.yml"
    path.write_text(yaml.safe_dump(value), encoding="utf-8")
    return str(path)


def test_valid_waived_registry_and_lookup(tmp_path) -> None:
    registry = load_registry(_write(tmp_path, _registry()))
    assert isinstance(registry, Registry)
    entry, branch = registry.get_branch("org/repo", "1.0")
    assert branch == BranchEntry("1.0", 1)
    assert entry.validation is None
    assert entry.validation_waiver == ValidationWaiver(
        reason="Unit-test repository has no build system.",
        approved_by="test suite",
        expires=date(2099, 1, 1),
    )
    assert entry.effective_push_repo == "org/repo"
    with pytest.raises(KeyError):
        registry.get_repo("missing/repo")
    with pytest.raises(KeyError):
        registry.get_branch("org/repo", "2.0")


def test_full_typed_entry(tmp_path) -> None:
    registry = load_registry(_write(
        tmp_path,
        _registry(repos=[_repo(
            push_repo="fork/repo",
            validation=_validation(),
            backport_label="bp",
            llm_conflict_label="ai",
            max_conflicting_files=50,
            repair_validation_failures=True,
            branches=[
                {"branch": "1.0", "project_number": 1},
                {"branch": "2.0", "project_number": 2},
            ],
        )]),
    ))
    entry = registry.get_repo("org/repo")
    assert entry.effective_push_repo == "fork/repo"
    assert entry.validation is not None
    assert entry.validation.image == _IMAGE
    assert entry.validation.default_command_ids == ("build",)
    assert entry.validation.rules[0].command_ids == ("cluster-tests",)
    assert entry.max_conflicting_files == 50
    assert entry.repair_validation_failures is True


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.pop("schema_version"), "schema_version"),
        (lambda value: value.update(schema_version=1), "schema_version"),
        (lambda value: value.update(repos=[]), "non-empty"),
        (lambda value: value["repos"][0].pop("repo"), "valid 'owner/name'"),
        (
            lambda value: value["repos"][0].update(repo="invalid"),
            "valid 'owner/name'",
        ),
        (
            lambda value: value["repos"][0].update(project_owner_type="team"),
            "project_owner_type",
        ),
        (
            lambda value: value["repos"][0].update(branches=[]),
            "non-empty",
        ),
        (
            lambda value: value["repos"][0]["branches"][0].update(project_number=0),
            "positive integer",
        ),
        (
            lambda value: value["repos"][0].update(max_conflicting_files=0),
            "positive integer",
        ),
        (
            lambda value: value["repos"][0].update(
                repair_validation_failures="yes",
            ),
            "must be boolean",
        ),
        (
            lambda value: value["repos"][0].update(backport_label=""),
            "non-empty string",
        ),
        (
            lambda value: value["repos"][0].update(unknown=True),
            "unknown key",
        ),
    ],
)
def test_registry_rejects_invalid_structure(tmp_path, mutate, message) -> None:
    value = _registry()
    mutate(value)
    with pytest.raises(ValueError, match=message):
        load_registry(_write(tmp_path, value))


@pytest.mark.parametrize(
    "branch",
    ["-bad", "bad..name", "bad//name", "bad@{x}", "bad.lock"],
)
def test_branch_name_must_be_safe(tmp_path, branch) -> None:
    value = _registry()
    value["repos"][0]["branches"][0]["branch"] = branch
    with pytest.raises(ValueError, match="safe Git branch"):
        load_registry(_write(tmp_path, value))


def test_duplicate_repo_branch_and_project_are_rejected(tmp_path) -> None:
    value = _registry(repos=[_repo(), _repo()])
    with pytest.raises(ValueError, match="Duplicate repo"):
        load_registry(_write(tmp_path, value))

    for duplicate in (
        {"branch": "1.0", "project_number": 2},
        {"branch": "2.0", "project_number": 1},
    ):
        value = _registry()
        value["repos"][0]["branches"].append(duplicate)
        with pytest.raises(ValueError, match="Duplicate"):
            load_registry(_write(tmp_path, value))


@pytest.mark.parametrize("push_repo", ["invalid", "org/fork", "org/repo"])
def test_push_repo_requires_different_owner_fork(tmp_path, push_repo) -> None:
    value = _registry()
    value["repos"][0]["push_repo"] = push_repo
    with pytest.raises(ValueError):
        load_registry(_write(tmp_path, value))


def test_validation_and_waiver_are_mutually_exclusive(tmp_path) -> None:
    value = _registry(repos=[_repo(
        validation=_validation(),
        validation_waiver=_waiver(),
    )])
    with pytest.raises(ValueError, match="cannot be combined"):
        load_registry(_write(tmp_path, value))


def test_missing_or_expired_waiver_is_rejected(tmp_path) -> None:
    missing = _repo()
    missing.pop("validation_waiver")
    with pytest.raises(ValueError, match="validation_waiver"):
        load_registry(_write(tmp_path, _registry(repos=[missing])))

    expired = _repo(validation_waiver={
        "reason": "old",
        "approved_by": "test",
        "expires": "2000-01-01",
    })
    with pytest.raises(ValueError, match="expired"):
        load_registry(_write(tmp_path, _registry(repos=[expired])))


def test_old_shell_command_interface_is_rejected(tmp_path) -> None:
    value = _registry()
    value["repos"][0]["build_commands"] = ["make -j$(nproc)"]
    with pytest.raises(ValueError, match="unknown key.*build_commands"):
        load_registry(_write(tmp_path, value))


def test_live_registry_keeps_valkey_search_enabled_with_strict_validation() -> None:
    registry = load_registry(
        str(Path(__file__).resolve().parents[1] / "repos.yml"),
    )
    search = registry.get_repo("valkey-io/valkey-search")

    assert search.automation.enabled is True
    assert search.validation_waiver is None
    assert search.repair_validation_failures is True
    assert tuple(
        (branch.branch, branch.project_number)
        for branch in search.branches
    ) == (("1.0", 36), ("1.1", 34), ("1.2", 58))
    assert search.validation is not None
    assert search.validation.kind == "container-argv-v2"
    assert search.validation.network == "none"
    assert search.validation.image.startswith(
        "mcr.microsoft.com/devcontainers/cpp@sha256:",
    )
    assert search.validation.immutable_inputs[0].sha256 == (
        "2aba36cafd6a94c54ca11a15409e86db"
        "2705bfa8fe38fbd9c5c4c758dca3c69f"
    )
    command = search.validation.command("build-and-test")
    assert command.argv[-4:] == (
        "--use-system-modules",
        "--test-errors-stdout",
        "--run-tests",
        "--jobs=2",
    )
    assert not {"sudo", "docker", "--privileged"} & set(command.argv)
