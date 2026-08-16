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
    daily_workflow: daily.yml
    daily_max_age_hours: 30
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
    assert policy.daily_workflow == "daily.yml"
    assert policy.daily_max_age_hours == 30
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
    # Deliberately unconfigured: upstream daily.yml only runs on the default
    # branch, so there is no trustworthy per-release-branch producer yet.
    # The gate code stays and is covered by fixture-based tests.
    assert policies["valkey-io/valkey"].daily_workflow is None
    assert policies["valkey-io/valkey"].daily_max_age_hours is None


def test_shipped_fork_registry_is_valid() -> None:
    path = Path(__file__).resolve().parents[1] / "release_policy.fork.yml"
    policies = load_policy(str(path))
    policy = next(iter(policies.values()))
    assert policy.authorized_team.startswith("user:")
    assert policy.mention == "@sarthakaggarwal97"
    # Forks have no scheduled daily runs; the gate stays unconfigured.
    assert policy.daily_workflow is None
    assert policy.daily_max_age_hours is None
    # Public endpoints are either fork-owned or empty (not configured):
    # a fork E2E must never be able to false-VERIFY against upstream's
    # real registries, downloads, or chart index.
    down = policy.downstream
    fork_owner = policy.repo.split("/", 1)[0]
    assert down.dockerhub_repo == ""
    assert down.bundle_dockerhub_repo == ""
    assert down.ecr_namespace == ""
    assert down.helm_index_url == ""
    assert down.downloads_base_url == ""
    assert down.ghcr_image_repo.startswith(f"{fork_owner}/")


def test_empty_public_endpoint_fields_are_accepted(tmp_path: Path) -> None:
    # Empty means "not configured for this repository": the verifier
    # reports the output as informational instead of probing anything.
    text = _VALID
    for old, new in (
        # bundle_dockerhub_repo first: its line contains "dockerhub_repo:
        # valkey/valkey" as a substring.
        ("bundle_dockerhub_repo: valkey/valkey-bundle", "bundle_dockerhub_repo: ''"),
        ("dockerhub_repo: valkey/valkey", "dockerhub_repo: ''"),
        ("ghcr_image_repo: valkey-io/valkey", "ghcr_image_repo: ''"),
        ("ecr_namespace: valkey", "ecr_namespace: ''"),
        ("helm_index_url: https://valkey.io/valkey-helm/index.yaml",
         "helm_index_url: ''"),
        ("downloads_base_url: https://download.valkey.io/releases",
         "downloads_base_url: ''"),
    ):
        assert old in text
        text = text.replace(old, new)
    down = load_policy(_write(tmp_path, text))["valkey-io/valkey"].downstream
    assert down.dockerhub_repo == ""
    assert down.bundle_dockerhub_repo == ""
    assert down.ghcr_image_repo == ""
    assert down.ecr_namespace == ""
    assert down.helm_index_url == ""
    assert down.downloads_base_url == ""


def test_non_endpoint_repo_fields_still_reject_empty(tmp_path: Path) -> None:
    # Only the public-endpoint fields may be empty; the GitHub repos the
    # controller reads and writes are always required.
    text = _VALID.replace("bundle_repo: valkey-io/valkey-bundle",
                          "bundle_repo: ''")
    with pytest.raises(ValueError, match="bundle_repo"):
        load_policy(_write(tmp_path, text))


@pytest.mark.parametrize(
    "mutation, message",
    [
        ("required_checks: []", "required_checks"),
        ("required_checks: [test-ubuntu-latest, '']", "required_checks"),
        ('branches: ["unstable"]', "not a release branch"),
        ("branches: []", "branches"),
        ("authorized_team: core-team", "authorized_team"),
        ("authorized_team: 'user:'", "authorized_team"),
        ("checks_workflow: ''", "checks_workflow"),
        ("checks_workflow: .github/workflows/ci.yml", "checks_workflow"),
        ("check_timeout_minutes: 0", "check_timeout_minutes"),
        ("check_timeout_minutes: '360'", "check_timeout_minutes"),
        ("daily_workflow: ''", "daily_workflow"),
        ("daily_workflow: .github/workflows/daily.yml", "daily_workflow"),
        ("daily_max_age_hours: 0", "daily_max_age_hours"),
        # YAML quoting slips again: '30' is a string and true is a bool;
        # both would break the freshness comparison if accepted.
        ("daily_max_age_hours: '30'", "daily_max_age_hours"),
        ("daily_max_age_hours: true", "daily_max_age_hours"),
        ("qualification_rpm_jobs: 0", "qualification_rpm_jobs"),
        # YAML quoting slips: '30' is a string and true is a bool; both
        # would break the exact-count matrix comparison if accepted.
        ("qualification_rpm_jobs: '30'", "qualification_rpm_jobs"),
        ("qualification_deb_jobs: true", "qualification_deb_jobs"),
        ("qualification_x86_archive_jobs: 0", "qualification_x86_archive_jobs"),
        ("build_workflow: ''", "build_workflow"),
        ("helm_index_url: http://insecure.example", "helm_index_url"),
        ("downloads_base_url: http://insecure.example", "downloads_base_url"),
        ("tarball_targets: []", "tarball_targets"),
        ("automation_repo: not-a-repo", "automation_repo"),
        ("ecr_namespace: '   '", "ecr_namespace"),
        ("dockerhub_repo: not-a-repo", "dockerhub_repo"),
        ("ghcr_image_repo: not-a-repo", "ghcr_image_repo"),
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
@pytest.mark.parametrize("team", ["valkey-io/", "/core-team"])
def test_team_with_empty_org_or_slug_is_rejected(tmp_path: Path, team: str) -> None:
    text = _VALID.replace("authorized_team: valkey-io/core-team",
                          f"authorized_team: {team}")
    with pytest.raises(ValueError, match="authorized_team"):
        load_policy(_write(tmp_path, text))
def test_user_form_with_whitespace_only_login_is_rejected(tmp_path: Path) -> None:
    text = _VALID.replace("repo: valkey-io/valkey", "repo: o/valkey").replace(
        "authorized_team: valkey-io/core-team", "authorized_team: 'user:   '"
    )
    with pytest.raises(ValueError, match="authorized_team"):
        load_policy(_write(tmp_path, text))


def test_duplicate_required_checks_are_rejected(tmp_path: Path) -> None:
    text = _VALID.replace(
        "required_checks: [test-ubuntu-latest]",
        "required_checks: [test-ubuntu-latest, test-ubuntu-latest]",
    )
    with pytest.raises(ValueError, match="required_checks"):
        load_policy(_write(tmp_path, text))


@pytest.mark.parametrize("removed", ["daily_workflow", "daily_max_age_hours"])
def test_daily_gate_fields_must_be_set_together(tmp_path: Path, removed: str) -> None:
    # One field without the other is a half-configured gate (a workflow with
    # no freshness bound, or a bound with nothing to measure).
    lines = [line for line in _VALID.splitlines()
             if not line.strip().startswith(f"{removed}:")]
    with pytest.raises(ValueError, match="set together or omitted together"):
        load_policy(_write(tmp_path, "\n".join(lines)))


def test_daily_gate_omitted_entirely_leaves_the_gate_unconfigured(tmp_path: Path) -> None:
    lines = [line for line in _VALID.splitlines()
             if not line.strip().startswith(("daily_workflow:", "daily_max_age_hours:"))]
    policy = load_policy(_write(tmp_path, "\n".join(lines)))["valkey-io/valkey"]
    assert policy.daily_workflow is None
    assert policy.daily_max_age_hours is None


def test_unknown_keys_are_rejected_at_every_level(tmp_path: Path) -> None:
    # A stray key is a misspelled real one until proven otherwise: a typo'd
    # optional key (daly_workflow) would silently disable the gate it meant
    # to configure, so every mapping level rejects unknowns by name.
    text = _VALID.replace("check_timeout_minutes: 360",
                          "check_timeout_minutes: 360\n    surprise_key: true")
    with pytest.raises(ValueError, match="surprise_key"):
        load_policy(_write(tmp_path, text))


def test_typoed_daily_workflow_key_fails_loudly(tmp_path: Path) -> None:
    text = _VALID.replace("daily_workflow: daily.yml",
                          "daly_workflow: daily.yml")
    with pytest.raises(ValueError, match="daly_workflow"):
        load_policy(_write(tmp_path, text))


def test_unknown_downstream_key_is_rejected(tmp_path: Path) -> None:
    text = _VALID.replace("ecr_namespace: valkey",
                          "ecr_namespace: valkey\n      exr_namespace: valkey")
    with pytest.raises(ValueError, match="exr_namespace"):
        load_policy(_write(tmp_path, text))


def test_unknown_top_level_key_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="repositories"):
        load_policy(_write(tmp_path, "repositories: []\n" + _VALID))


def test_schema_version_1_is_accepted(tmp_path: Path) -> None:
    policies = load_policy(_write(tmp_path, "schema_version: 1\n" + _VALID))
    assert "valkey-io/valkey" in policies


def test_unsupported_schema_version_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="schema_version"):
        load_policy(_write(tmp_path, "schema_version: 2\n" + _VALID))


def test_duplicate_repo_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        load_policy(_write(tmp_path, _VALID + _VALID.replace("repos:\n", "")))


def test_missing_repos_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="repos"):
        load_policy(_write(tmp_path, "other: 1\n"))


class TestValidateReleaseBranch:
    """:func:`validate_release_branch` is the single choke point every
    branch-scoped entry point (plan_publication, publish_release, the
    reconcile entry points) funnels through. A branch not
    listed in the policy is refused BEFORE any API access; the shape check
    inherited from :func:`parse_release_branch` runs on the same call site
    so both wrong-shape and unconfigured-but-well-shaped inputs fail here.
    """

    def _policy(self, tmp_path: Path) -> object:
        from scripts.release.policy import load_policy
        return load_policy(_write(tmp_path, _VALID))["valkey-io/valkey"]

    def test_configured_branch_is_accepted(self, tmp_path: Path) -> None:
        from scripts.release.policy import validate_release_branch
        # Returns None (nothing to inspect); no exception is success.
        validate_release_branch(self._policy(tmp_path), "9.1")
        validate_release_branch(self._policy(tmp_path), "8.0")

    def test_unconfigured_numeric_branch_is_refused(self, tmp_path: Path) -> None:
        from scripts.release.policy import validate_release_branch
        with pytest.raises(ValueError,
                           match="not a configured release branch"):
            validate_release_branch(self._policy(tmp_path), "6.9")

    def test_error_message_lists_allowed_branches(self, tmp_path: Path) -> None:
        # The operator sees exactly which branches ARE configured so they
        # can correct the dispatch without inspecting the policy file.
        from scripts.release.policy import validate_release_branch
        with pytest.raises(ValueError,
                           match=r"policy allows: 9\.1, 8\.0"):
            validate_release_branch(self._policy(tmp_path), "10.5")

    @pytest.mark.parametrize("bad_shape", [
        "main", "unstable", "9", "9.1.0", "9.01", "release/9.1", "",
    ])
    def test_wrong_shape_branch_is_refused_by_the_shape_check(
        self, tmp_path: Path, bad_shape: str,
    ) -> None:
        # An operator-supplied branch that does not even match M.m is
        # refused with the parse error (same message as at policy load
        # time); nothing gets past this to touch GitHub.
        from scripts.release.policy import validate_release_branch
        with pytest.raises(ValueError):
            validate_release_branch(self._policy(tmp_path), bad_shape)
