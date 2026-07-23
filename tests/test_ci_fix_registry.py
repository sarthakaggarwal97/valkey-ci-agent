"""Tests for the typed CI-fix repository registry."""

from __future__ import annotations

import json

import yaml

from scripts.ci_fix.policy import (
    DEFAULT_AUTO_PUBLISH_PATTERNS,
    DEFAULT_PROTECTED_PATTERNS,
)
from scripts.ci_fix.registry import load_ci_fix_registry, main


def _write(tmp_path, repos) -> str:
    path = tmp_path / "repos.yml"
    path.write_text(yaml.safe_dump({"repos": repos}), encoding="utf-8")
    return str(path)


def _repo(name: str = "valkey-io/valkey", **ci_fix):
    return {
        "repo": name,
        "branches": [
            {"branch": "8.1", "project_number": 1},
            {"branch": "9.0", "project_number": 2},
        ],
        "ci_fix": ci_fix,
    }


def test_loads_enabled_repo_and_derives_release_history(tmp_path):
    path = _write(tmp_path, [_repo(enabled=True, poll_comments=True)])
    entry = load_ci_fix_registry(path).get_repo("valkey-io/valkey")

    assert entry.owner == "valkey-io"
    assert entry.name == "valkey"
    assert entry.authorization_org == "valkey-io"
    assert entry.authorization_team == "contributors"
    assert entry.allowed_branch_prefixes == ("agent/backport/",)
    assert entry.history_branches == ("8.1", "9.0")
    assert entry.baseline_runs == 3
    assert entry.flaky_verify_runs == 10
    assert entry.remote_parallelism == 5
    assert entry.remote_sample_timeout_minutes == 15
    assert entry.remote_budget_minutes == 45
    assert entry.minimum_confidence == 0.8
    assert entry.protected_paths == DEFAULT_PROTECTED_PATTERNS
    assert entry.auto_publish_paths == DEFAULT_AUTO_PUBLISH_PATTERNS


def test_full_policy_and_duplicate_values_are_normalized(tmp_path):
    path = _write(tmp_path, [_repo(
        enabled=True,
        authorization_org="maintainers",
        authorization_team="ci-drivers",
        allowed_branch_prefixes=["agent/backport/", "agent/fix/", "agent/fix/"],
        history_branches=["7.2", "8.0", "7.2"],
        baseline_runs=7,
        flaky_verify_runs=25,
        remote_parallelism=4,
        remote_sample_timeout_minutes=12,
        remote_budget_minutes=40,
        minimum_confidence=0.9,
        verification_workflow=".github/workflows/ci-fix-verify.yml",
        verification_ref="unstable",
        protected_paths=[".github/**"],
        auto_publish_paths=["tests/**"],
    )])
    entry = load_ci_fix_registry(path).get_repo("valkey-io/valkey")

    assert entry.authorization_org == "maintainers"
    assert entry.authorization_team == "ci-drivers"
    assert entry.allowed_branch_prefixes == ("agent/backport/", "agent/fix/")
    assert entry.history_branches == ("7.2", "8.0")
    assert entry.baseline_runs == 7
    assert entry.flaky_verify_runs == 25
    assert entry.remote_parallelism == 4
    assert entry.remote_sample_timeout_minutes == 12
    assert entry.remote_budget_minutes == 40
    assert entry.minimum_confidence == 0.9
    assert entry.verification_workflow.endswith("ci-fix-verify.yml")
    assert entry.verification_ref == "unstable"


def test_disabled_repo_is_not_resolvable_but_does_not_poll(tmp_path):
    path = _write(tmp_path, [_repo()])
    registry = load_ci_fix_registry(path)

    try:
        registry.get_repo("valkey-io/valkey")
    except KeyError as exc:
        assert "not enabled" in str(exc)
    else:
        raise AssertionError("disabled repository unexpectedly resolved")
    assert registry.poll_matrix() == {"include": []}


def test_poll_matrix_contains_only_enabled_poll_targets(tmp_path):
    path = _write(tmp_path, [
        _repo("valkey-io/valkey", enabled=True, poll_comments=True),
        _repo("valkey-io/valkey-search", enabled=True, poll_comments=False),
        _repo("valkey-io/valkey-bloom", enabled=False),
    ])

    assert load_ci_fix_registry(path).poll_matrix() == {
        "include": [
            {
                "repo": "valkey-io/valkey",
                "repo_slug": "valkey-io-valkey",
                "owner": "valkey-io",
                "name": "valkey",
                "authorization_org": "valkey-io",
                "authorization_team": "contributors",
            }
        ]
    }


def test_rejects_poll_without_enablement(tmp_path):
    path = _write(tmp_path, [_repo(poll_comments=True)])
    assert main(["poll-matrix", "--registry", path]) == 2


def test_rejects_partial_remote_verifier_configuration(tmp_path):
    path = _write(tmp_path, [_repo(
        enabled=True,
        verification_workflow="ci-fix-verify.yml",
    )])
    assert main(["resolve", "--registry", path, "--repo", "valkey-io/valkey"]) == 2


def test_resolve_writes_github_outputs(tmp_path):
    registry_path = _write(tmp_path, [_repo(enabled=True)])
    output = tmp_path / "output"

    assert main([
        "resolve", "--registry", registry_path,
        "--repo", "valkey-io/valkey", "--output-file", str(output),
    ]) == 0
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())

    assert values["owner"] == "valkey-io"
    assert values["name"] == "valkey"
    assert json.loads(values["history_branches_json"]) == ["8.1", "9.0"]
    assert json.loads(values["config_json"])["enabled"] is True


def test_poll_matrix_cli_writes_empty_signal(tmp_path):
    registry_path = _write(tmp_path, [_repo(enabled=True, poll_comments=False)])
    output = tmp_path / "output"

    assert main([
        "poll-matrix", "--registry", registry_path, "--output-file", str(output),
    ]) == 0
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["has_entries"] == "false"
    assert json.loads(values["matrix"]) == {"include": []}


def test_rejects_unsafe_ref_and_invalid_limits(tmp_path):
    bad_ref = _write(tmp_path, [_repo(
        enabled=True,
        allowed_branch_prefixes=["../../heads/main"],
    )])
    assert main(["resolve", "--registry", bad_ref, "--repo", "valkey-io/valkey"]) == 2

    bad_runs = _write(tmp_path, [_repo(enabled=True, baseline_runs=0)])
    assert main(["resolve", "--registry", bad_runs, "--repo", "valkey-io/valkey"]) == 2

    bad_parallelism = _write(tmp_path, [_repo(enabled=True, remote_parallelism=11)])
    assert main([
        "resolve", "--registry", bad_parallelism, "--repo", "valkey-io/valkey",
    ]) == 2
