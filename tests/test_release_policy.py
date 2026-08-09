"""Tests for the release policy loader and the shipped registry."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.release.policy import load_policy

_VALID = """
repos:
  - repo: valkey-io/valkey
    authorized_team: valkey-io/core-team
    branches: ["9.1", "8.0"]
    required_checks: [test-ubuntu-latest]
    checks_workflow: ci.yml
    check_timeout_minutes: 360
    downstream:
      automation_repo: valkey-io/valkey-release-automation
      build_workflow: build-release.yml
      qualification_workflow: qualify-release.yml
      qualification_x86_archive_jobs: 2
      qualification_arm_archive_jobs: 2
      qualification_rpm_jobs: 30
      qualification_deb_jobs: 10
      downloads_base_url: https://download.valkey.io/releases
      tarball_targets: [jammy/x86_64, noble/arm64]
      hashes_repo: valkey-io/valkey-hashes
      container_repo: valkey-io/valkey-container
      doc_repo: valkey-io/valkey-doc
      website_repo: valkey-io/valkey-io.github.io
      bundle_repo: valkey-io/valkey-bundle
      helm_repo: valkey-io/valkey-helm
      dockerhub_repo: valkey/valkey
      bundle_dockerhub_repo: valkey/valkey-bundle
      ghcr_image_repo: valkey-io/valkey
      ecr_namespace: valkey
      helm_index_url: https://valkey.io/valkey-helm/index.yaml
"""


def _write(tmp_path: Path, text: str) -> str:
    path = tmp_path / "release_policy.yml"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_valid_policy_parses(tmp_path: Path) -> None:
    policies = load_policy(_write(tmp_path, _VALID))
    policy = policies["valkey-io/valkey"]
    assert policy.branches == ("9.1", "8.0")
    assert policy.required_checks == ("test-ubuntu-latest",)
    assert policy.checks_workflow == "ci.yml"
    assert policy.check_timeout_minutes == 360
    assert policy.team_org == "valkey-io"
    assert policy.team_slug == "core-team"
    down = policy.downstream
    assert down.qualification_workflow == "qualify-release.yml"
    assert down.qualification_x86_archive_jobs == 2
    assert down.qualification_arm_archive_jobs == 2
    assert down.qualification_rpm_jobs == 30
    assert down.qualification_deb_jobs == 10
    assert down.build_workflow == "build-release.yml"
    assert down.helm_index_url.startswith("https://")
    assert down.tarball_targets == ("jammy/x86_64", "noble/arm64")
    assert down.downloads_base_url == "https://download.valkey.io/releases"
    assert down.ecr_namespace == "valkey"


def test_shipped_registry_is_valid() -> None:
    path = Path(__file__).resolve().parents[1] / "release_policy.yml"
    policies = load_policy(str(path))
    assert "valkey-io/valkey" in policies
    assert policies["valkey-io/valkey"].required_checks


def test_shipped_fork_registry_is_valid() -> None:
    path = Path(__file__).resolve().parents[1] / "release_policy.fork.yml"
    policies = load_policy(str(path))
    policy = next(iter(policies.values()))
    assert policy.authorized_team.startswith("user:")
    assert policy.mention == "@sarthakaggarwal97"


@pytest.mark.parametrize(
    "mutation, message",
    [
        ("required_checks: []", "required_checks"),
        ("required_checks: [test-ubuntu-latest, '']", "required_checks"),
        ('branches: ["unstable"]', "not a release branch"),
        ("branches: []", "branches"),
        ("authorized_team: core-team", "authorized_team"),
        ("checks_workflow: ''", "checks_workflow"),
        ("checks_workflow: .github/workflows/ci.yml", "checks_workflow"),
        ("check_timeout_minutes: 0", "check_timeout_minutes"),
        ("check_timeout_minutes: '360'", "check_timeout_minutes"),
        ("qualification_rpm_jobs: 0", "qualification_rpm_jobs"),
        ("qualification_x86_archive_jobs: 0", "qualification_x86_archive_jobs"),
        ("build_workflow: ''", "build_workflow"),
        ("helm_index_url: http://insecure.example", "helm_index_url"),
        ("downloads_base_url: http://insecure.example", "downloads_base_url"),
        ("tarball_targets: []", "tarball_targets"),
        ("automation_repo: not-a-repo", "automation_repo"),
        ("ecr_namespace: ''", "ecr_namespace"),
    ],
)
def test_invalid_entries_are_rejected(tmp_path: Path, mutation: str, message: str) -> None:
    key = mutation.split(":", 1)[0]
    lines = []
    for line in _VALID.splitlines():
        if line.strip().startswith(f"{key}:"):
            indent = line[: len(line) - len(line.lstrip())]
            lines.append(f"{indent}{mutation}")
        else:
            lines.append(line)
    with pytest.raises(ValueError, match=message):
        load_policy(_write(tmp_path, "\n".join(lines)))


def test_duplicate_repo_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        load_policy(_write(tmp_path, _VALID + _VALID.replace("repos:\n", "")))


def test_missing_repos_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="repos"):
        load_policy(_write(tmp_path, "other: 1\n"))
