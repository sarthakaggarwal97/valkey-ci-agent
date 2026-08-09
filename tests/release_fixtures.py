"""Shared fixtures for the release-controller test suite.

Builders for the policy and for listing-shaped PyGithub mocks. Defaults
model the happy path (merged notes PR at the branch head, all required
checks passed from the qualification workflow's suite, a passed
qualification run, no release published); tests override the dimension
under test.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from github.GithubException import GithubException

from scripts.release import issue as issue_mod
from scripts.release.policy import DownstreamPolicy, RepoReleasePolicy

MERGE_SHA = "a" * 40
MOVED_SHA = "b" * 40
CI_SUITE = 100        # check-suite id of the qualification workflow's run
DAILY_SUITE = 200     # check-suite id of an unrelated workflow's run
TRACKER_CREATED = datetime(2026, 8, 1, tzinfo=timezone.utc)
AFTER_TRACKER = TRACKER_CREATED + timedelta(hours=1)
BEFORE_TRACKER = TRACKER_CREATED - timedelta(days=3)


def make_downstream(**overrides: object) -> DownstreamPolicy:
    values: "dict[str, object]" = {
        "automation_repo": "valkey-io/valkey-release-automation",
        "build_workflow": "build-release.yml",
        "qualification_workflow": "qualify-release.yml",
        "qualification_rpm_jobs": 2,
        "qualification_deb_jobs": 1,
        "downloads_base_url": "https://download.valkey.io/releases",
        "tarball_targets": ("jammy/x86_64", "noble/arm64"),
        "hashes_repo": "valkey-io/valkey-hashes",
        "container_repo": "valkey-io/valkey-container",
        "doc_repo": "valkey-io/valkey-doc",
        "website_repo": "valkey-io/valkey-io.github.io",
        "bundle_repo": "valkey-io/valkey-bundle",
        "helm_repo": "valkey-io/valkey-helm",
        "dockerhub_repo": "valkey/valkey",
        "bundle_dockerhub_repo": "valkey/valkey-bundle",
        "ghcr_image_repo": "valkey-io/valkey",
        "ecr_namespace": "valkey",
        "helm_index_url": "https://valkey.io/valkey-helm/index.yaml",
    }
    values.update(overrides)
    return DownstreamPolicy(**values)  # type: ignore[arg-type]


def make_policy(**overrides: object) -> RepoReleasePolicy:
    values: "dict[str, object]" = {
        "repo": "valkey-io/valkey",
        "authorized_team": "valkey-io/core-team",
        "branches": ("9.1", "8.0"),
        "required_checks": ("test-ubuntu-latest", "build-macos-latest"),
        "checks_workflow": "ci.yml",
        "check_timeout_minutes": 360,
        "downstream": make_downstream(),
    }
    values.update(overrides)
    return RepoReleasePolicy(**values)  # type: ignore[arg-type]


def check_run(name: str, *, status: str = "completed",
              conclusion: "str | None" = "success", run_id: int = 1,
              started: "datetime | None" = None,
              created: "datetime | None" = None, suite: int = CI_SUITE) -> MagicMock:
    run = MagicMock(status=status, conclusion=conclusion, id=run_id,
                    html_url=f"https://x/runs/{run_id}", started_at=started,
                    created_at=created)
    run.name = name  # `name` is reserved in the MagicMock constructor
    run._rawData = {"check_suite": {"id": suite}}
    return run


def passing_runs() -> "list[MagicMock]":
    return [check_run("test-ubuntu-latest", run_id=1),
            check_run("build-macos-latest", run_id=2)]


def workflow_runs() -> "list[MagicMock]":
    ci = MagicMock(check_suite_id=CI_SUITE, path=".github/workflows/ci.yml")
    daily = MagicMock(check_suite_id=DAILY_SUITE, path=".github/workflows/daily.yml")
    return [ci, daily]


def qualification_run(sha: str = MERGE_SHA, *,
                      status: str = "completed", conclusion: str = "success",
                      tag: str = "9.1.1", head_branch: str = "main",
                      jobs: "list[MagicMock] | None" = None) -> MagicMock:
    run = MagicMock(id=900, status=status, conclusion=conclusion,
                    html_url="https://x/qruns/900",
                    head_branch=head_branch,
                    display_title=f"Qualify {tag} @ {sha}")
    if jobs is None:
        names = [
            "Qualify x86 archives / Build package ubuntu-22.04 x86_64",
            "Qualify x86 archives / Build package ubuntu-24.04 x86_64",
            "Qualify ARM archives / Build package ubuntu-22.04-arm arm64",
            "Qualify ARM archives / Build package ubuntu-24.04-arm arm64",
            "Qualify RPM/DEB packages / RPM · Rocky Linux 9 (x86_64) · v9",
            "Qualify RPM/DEB packages / RPM · AlmaLinux 9 (aarch64) · v9",
            "Qualify RPM/DEB packages / Test RPM · Rocky Linux 9 (x86_64) · v9",
            "Qualify RPM/DEB packages / DEB · Debian 12 (arm64) · v9",
        ]
        jobs = []
        for name in names:
            job = MagicMock(conclusion="success")
            job.name = name
            jobs.append(job)
    run.jobs.return_value = jobs
    artifact_names = ["qualify-a", "qualify-b", "qualify-c", "qualify-d",
                      "valkey-rpms-el9-x86_64", "valkey-rpms-alma9-aarch64",
                      "valkey-debs-debian12-arm64"]
    artifacts = []
    for name in artifact_names:
        artifact = MagicMock(expired=False, size_in_bytes=1024)
        artifact.name = name
        artifacts.append(artifact)
    run.get_artifacts.return_value = artifacts
    return run


def notes_pr(*, merged: bool = True, state: "str | None" = None,
             head_ref: str = "agent/release-cut/9.1.1-ga",
             head_repo: "str | None" = "valkey-io/valkey",
             number: int = 42, merge_sha: "str | None" = MERGE_SHA,
             created: datetime = AFTER_TRACKER) -> MagicMock:
    pr = MagicMock(number=number,
                   merged_at=AFTER_TRACKER if merged else None,
                   merge_commit_sha=merge_sha if merged else None,
                   state=state or ("closed" if merged else "open"),
                   created_at=created,
                   html_url=f"https://x/pull/{number}")
    pr.head.ref = head_ref
    pr.head.repo = None if head_repo is None else SimpleNamespace(full_name=head_repo)
    return pr


def tracker(branch: str = "9.1", comments: "list[MagicMock] | None" = None) -> MagicMock:
    issue = MagicMock(number=7, html_url="https://x/issues/7", title="Release 9.1.1",
                      created_at=TRACKER_CREATED, state="open")
    issue.body = issue_mod.identity_marker(branch)
    issue._rawData = {}
    issue.get_comments.return_value = comments or []
    return issue


def bot_adoption(sha: str) -> MagicMock:
    comment = MagicMock()
    comment.user.login = "valkeyrie-ops[bot]"
    comment.body = f"{issue_mod.adopt_marker(sha)}\nadopted"
    return comment


def repo_mock(*, branch_head: str = MERGE_SHA,
              pulls: "list[MagicMock] | None" = None,
              issues: "list[MagicMock] | None" = None,
              runs: "list[MagicMock] | None" = None,
              tags: "list[str] | None" = None,
              qual_runs: "list[MagicMock] | None" = None,
              released: bool = False) -> MagicMock:
    """A repo mock serving every repo the controller touches.

    ``released`` False makes ``get_release`` 404 (the not-published default);
    True serves a non-draft release whose tag resolves to ``branch_head``.
    """
    repo = MagicMock()
    repo.default_branch = "main"
    repo.get_branch.return_value.commit.sha = branch_head
    repo.get_pulls.return_value = pulls if pulls is not None else [notes_pr()]
    repo.get_issues.return_value = issues if issues is not None else []
    repo.get_commit.return_value.get_check_runs.return_value = (
        runs if runs is not None else passing_runs()
    )
    repo.get_workflow_runs.return_value = workflow_runs()
    repo.get_tags.return_value = [SimpleNamespace(name=t) for t in (tags or [])]
    repo.get_workflow.return_value.get_runs.return_value = (
        qual_runs if qual_runs is not None else [qualification_run()]
    )
    if released:
        release = MagicMock(prerelease=False, draft=False,
                            published_at=AFTER_TRACKER,
                            html_url="https://x/releases/9.1.1")
        repo.get_release.return_value = release
        ref = MagicMock()
        ref.object.type = "commit"
        ref.object.sha = MERGE_SHA
        repo.get_git_ref.return_value = ref
    else:
        repo.get_release.side_effect = GithubException(404, "no release", {})
        repo.get_git_ref.side_effect = GithubException(404, "no tag", {})
    return repo


def gh_mock(repo: MagicMock, *, member: bool = True) -> MagicMock:
    gh = MagicMock()
    gh.get_repo.return_value = repo
    gh.get_organization.return_value.get_team_by_slug.return_value.has_in_members.return_value = member
    return gh
