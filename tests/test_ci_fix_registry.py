"""Tests for registry-driven CI-fix repository selection."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.backport.registry import CiFixConfig, Registry, RepoEntry
from scripts.ci_fix.registry import (
    CiFixRepositoryError,
    enabled_ci_fix_repositories,
    main,
    resolve_ci_fix_repository,
    token_repository_names,
)


def _entry(repo: str, *, enabled: bool) -> RepoEntry:
    return RepoEntry(
        repo=repo,
        project_owner=repo.split("/", 1)[0],
        project_owner_type="organization",
        language="c",
        branches=(),
        ci_fix=CiFixConfig(enabled=enabled),
    )


def _registry() -> Registry:
    return Registry(
        repos=(
            _entry("valkey-io/valkey", enabled=True),
            _entry("valkey-io/not-enabled", enabled=False),
            _entry("valkey-io/valkey-search", enabled=True),
        )
    )


def test_enabled_repositories_preserve_registry_order():
    assert tuple(entry.repo for entry in enabled_ci_fix_repositories(_registry())) == (
        "valkey-io/valkey",
        "valkey-io/valkey-search",
    )


def test_resolve_requires_registered_explicit_opt_in():
    assert resolve_ci_fix_repository(_registry(), "valkey-io/valkey").repo == "valkey-io/valkey"

    with pytest.raises(CiFixRepositoryError, match="not enabled"):
        resolve_ci_fix_repository(_registry(), "valkey-io/not-enabled")
    with pytest.raises(CiFixRepositoryError, match="not registered"):
        resolve_ci_fix_repository(_registry(), "valkey-io/missing")


def test_token_scope_includes_enabled_targets_and_agent_repo():
    assert token_repository_names(
        _registry(),
        owner="valkey-io",
        include_repositories=("valkey-io/valkey-ci-agent",),
    ) == ("valkey", "valkey-search", "valkey-ci-agent")


def test_token_scope_rejects_cross_owner_targets():
    registry = Registry(
        repos=(
            _entry("valkey-io/valkey", enabled=True),
            _entry("other/search", enabled=True),
        )
    )
    with pytest.raises(CiFixRepositoryError, match="must share the token owner"):
        token_repository_names(registry, owner="valkey-io")


def test_cli_resolve_emits_github_outputs(tmp_path, capsys):
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
repos:
  - repo: valkey-io/valkey-search
    project_owner: valkey-io
    project_owner_type: organization
    language: c++
    ci_fix:
      enabled: true
    branches:
      - branch: "1.0"
        project_number: 1
""".lstrip(),
        encoding="utf-8",
    )

    assert main(["--registry", str(registry), "resolve", "--repo", "valkey-io/valkey-search"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "owner=valkey-io",
        "name=valkey-search",
        "full_name=valkey-io/valkey-search",
    ]


def test_workflows_use_registry_outputs_instead_of_repo_allowlists():
    fix_workflow = Path(".github/workflows/ci-fix.yml").read_text(encoding="utf-8")
    poll_workflow = Path(".github/workflows/ci-fix-comment-poll.yml").read_text(
        encoding="utf-8"
    )

    assert "scripts.ci_fix.registry" in fix_workflow
    assert "owner: ${{ steps.target.outputs.owner }}" in fix_workflow
    assert "repositories: ${{ steps.target.outputs.name }}" in fix_workflow
    assert "format('issue-{0}', inputs.issue)" in fix_workflow
    assert "format('pr-{0}', inputs.pr)" in fix_workflow
    assert 'CI_FIX_REPO}" != "valkey-io/valkey"' not in fix_workflow

    assert "scripts.ci_fix.registry" in poll_workflow
    assert "repositories: ${{ steps.targets.outputs.repositories }}" in poll_workflow
    assert "valkey-search" not in poll_workflow
