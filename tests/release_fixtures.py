"""Shared fixtures for the release-controller test suite.

Builders for the policy and for listing-shaped PyGithub mocks. Defaults
model the happy path (merged notes PR at the branch head, all required
checks passed from the qualification workflow's suite, a passed
qualification run, no release published); tests override the dimension
under test.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from github.GithubException import GithubException

from scripts.release import issue as issue_mod
from scripts.release.models import ReleaseBinding
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


def daily_run(*, run_id: int = 77, status: str = "completed",
              conclusion: "str | None" = "success", branch: str = "9.1",
              created: "datetime | None" = None) -> MagicMock:
    """A run of the branch-level daily workflow, fresh and green by default."""
    return MagicMock(
        id=run_id, status=status, conclusion=conclusion, head_branch=branch,
        html_url=f"https://x/druns/{run_id}",
        created_at=created or datetime.now(timezone.utc) - timedelta(hours=2),
    )


def build_manifest_payload(
    *, tag: str = "9.1.1", sha: str = MERGE_SHA,
    version: "str | None" = None,
    rpm_jobs: "int | None" = None, deb_jobs: "int | None" = None,
    archive_jobs: int = 4,
    schema: int = 1, nonce: str = "n" * 32,
    automation_sha: str = "c" * 40,
    created_at: str = "2026-08-01T00:00:00Z",
    extra: "dict | None" = None,
) -> "dict[str, object]":
    """A valid schema-1 qualification manifest, ready for tests to mutate.

    The default matches the ``qualification_run`` job/artifact inventory
    for GA (four archive legs, two RPM, one DEB); an RC tag zeroes the
    package counts to match the workflow's RC-skip behavior. Individual
    fields are overridden by attack tests (wrong tag, wrong SHA, wrong
    counts).
    """
    is_rc = "-rc" in tag
    if rpm_jobs is None:
        rpm_jobs = 0 if is_rc else 2
    if deb_jobs is None:
        deb_jobs = 0 if is_rc else 1
    payload: "dict[str, object]" = {
        "schema": schema,
        "nonce": nonce,
        # The -rcN strip mirrors the producer contract exactly: the fixed
        # qualify-release.yml (valkey-release-automation) emits the BASE
        # version (no -rcN suffix) in the manifest 'version' field and the
        # full dispatched tag in 'tag'. The consumer (qualification.py)
        # binds 'version' against tag.split("-rc", 1)[0], so this default
        # is the shape a real producer manifest carries.
        "version": version if version is not None else tag.split("-rc", 1)[0],
        "tag": tag,
        "source_sha": sha,
        "automation_sha": automation_sha,
        "rpm_jobs": rpm_jobs,
        "deb_jobs": deb_jobs,
        "archive_jobs": archive_jobs,
        "created_at": created_at,
    }
    if extra:
        payload.update(extra)
    return payload


def build_manifest_zip_bytes(payload: "dict[str, object] | None" = None,
                             *, filename: str = "manifest.json",
                             json_body: "str | None" = None) -> bytes:
    """A zip whose only file is the given JSON payload (or raw ``json_body``).

    Attack tests use ``json_body`` to inject exact malformed content (an
    unterminated brace, a top-level list, a plain string) without going
    through the well-formed builder.
    """
    if json_body is None:
        payload = payload if payload is not None else build_manifest_payload()
        json_body = json.dumps(payload)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, json_body)
    return buf.getvalue()


def _manifest_artifact_mock(*, expired: bool = False,
                            size_in_bytes: int = 1024,
                            zip_bytes: "bytes | None" = None,
                            requester: "MagicMock | None" = None) -> MagicMock:
    """A qualification-manifest artifact mock whose requester serves a zip.

    The default zip is a valid schema-1 manifest bound to ``(9.1.1,
    MERGE_SHA)``, matching the fixture's happy path. Tests wanting a
    mismatch pass a different ``zip_bytes`` value (built via
    :func:`build_manifest_zip_bytes` with overridden fields).
    """
    artifact = MagicMock(expired=expired, size_in_bytes=size_in_bytes)
    artifact.name = "qualification-manifest"
    artifact.archive_download_url = (
        "https://api.github.com/repos/o/r/actions/artifacts/1/zip"
    )
    if requester is None:
        blob = zip_bytes if zip_bytes is not None else build_manifest_zip_bytes()
        requester = MagicMock()
        requester.requestBlob.return_value = (200, {}, blob)
    artifact.requester = requester
    return artifact


def qualification_run(sha: str = MERGE_SHA, *,
                      status: str = "completed", conclusion: str = "success",
                      tag: str = "9.1.1", head_branch: str = "main",
                      jobs: "list[MagicMock] | None" = None,
                      manifest_payload: "dict[str, object] | None" = None,
                      manifest_zip: "bytes | None" = None) -> MagicMock:
    """A qualification run mock whose manifest artifact serves valid content.

    ``manifest_payload`` overrides the manifest JSON payload while keeping
    the zip envelope intact (default: a schema-1 payload matching this
    run's ``tag`` and ``sha``). ``manifest_zip`` overrides the entire
    envelope (for empty-upload / corrupt-zip attack tests).
    """
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
            job = MagicMock(status="completed", conclusion="success")
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
    # Manifest artifact carrying valid content by default. Attack tests
    # opt into malformed content by passing manifest_payload / manifest_zip.
    if manifest_zip is None:
        payload = manifest_payload if manifest_payload is not None else (
            build_manifest_payload(tag=tag, sha=sha)
        )
        manifest_zip = build_manifest_zip_bytes(payload)
    artifacts.append(_manifest_artifact_mock(zip_bytes=manifest_zip))
    run.get_artifacts.return_value = artifacts
    return run


def tag_ref(sha: str = MERGE_SHA) -> MagicMock:
    """A git-ref mock resolving to a commit at *sha* (as get_git_ref serves)."""
    ref = MagicMock()
    ref.object.type = "commit"
    ref.object.sha = sha
    return ref


def publish_run(*, head_sha: str, status: str = "waiting",
                branch: str = "9.1", run_id: int = 77,
                conclusion: "str | None" = None,
                tag: str = "", candidate_sha: str = "") -> MagicMock:
    """A publish-workflow run mock whose run-name carries the tag@sha binding.

    ``tag`` + ``candidate_sha`` both set = a bound run; either empty = an
    unbound (legacy / manual) run.
    """
    binding = f" · {tag} @ {candidate_sha}" if tag and candidate_sha else ""
    run = MagicMock(status=status, conclusion=conclusion, head_sha=head_sha,
                    id=run_id,
                    display_title=f"Publish Release on {branch}{binding} "
                                  f"(requested by x)",
                    html_url=f"https://x/actions/runs/{run_id}")
    run.cancel.return_value = True
    return run


def notes_pr(*, merged: bool = True, state: "str | None" = None,
             head_ref: str = "agent/release-cut/9.1.1-ga",
             head_repo: "str | None" = "valkey-io/valkey",
             number: int = 42, merge_sha: "str | None" = MERGE_SHA,
             created: datetime = AFTER_TRACKER,
             author_login: str = "valkeyrie-ops[bot]") -> MagicMock:
    pr = MagicMock(number=number,
                   merged_at=AFTER_TRACKER if merged else None,
                   merge_commit_sha=merge_sha if merged else None,
                   state=state or ("closed" if merged else "open"),
                   created_at=created,
                   html_url=f"https://x/pull/{number}")
    pr.head.ref = head_ref
    pr.head.repo = None if head_repo is None else SimpleNamespace(full_name=head_repo)
    # Trust the notes-PR author by default (the trusted-bot-author gate): tests
    # that model a lookalike PR override author_login to a non-bot value.
    pr.user.login = author_login
    return pr


def tracker(branch: str = "9.1", comments: "list[MagicMock] | None" = None) -> MagicMock:
    issue = MagicMock(number=7, html_url="https://x/issues/7", title="Release 9.1.1",
                      created_at=TRACKER_CREATED, state="open")
    issue.body = issue_mod.identity_marker(branch)
    issue._rawData = {}
    issue.get_comments.return_value = comments or []
    return issue


def bot_comment(body: str, *, author: str = "valkeyrie-ops[bot]",
                created: datetime = AFTER_TRACKER) -> MagicMock:
    comment = MagicMock()
    comment.user.login = author
    comment.body = body
    comment.created_at = created
    return comment


def bot_adoption(sha: str) -> MagicMock:
    return bot_comment(f"{issue_mod.adopt_marker(sha)}\nadopted")


def bot_binding(version: str = "9.1.1", stage: str = "ga", *,
                notes_pr_number: int = 0, merge_sha: str = "") -> MagicMock:
    """A trusted identity-binding receipt comment, as write_binding posts it."""
    binding = ReleaseBinding(version=version, stage=stage,
                             notes_pr_number=notes_pr_number,
                             merge_sha=merge_sha)
    return bot_comment(f"{issue_mod.binding_marker(binding)}\nreceipt")


def bot_receipt(tag: str = "9.1.1", sha: str = MERGE_SHA, *,
                author: str = "valkeyrie-ops[bot]") -> MagicMock:
    """A trusted publication receipt in the LEGACY field set (no plan
    digest, no controller lines): exactly what the pre-receipt-fields publish path
    posted, and what prior releases' trackers (8.0.10, 9.0.6, the live
    8.0.11) carry. Defaulting to this shape means every test using it also
    proves the migration acceptance: marker presence plus tag/SHA match is
    the whole requirement."""
    return bot_comment(
        f"{issue_mod.publication_receipt_marker()}\n"
        f"Published **{tag}** at `{sha}` (publication approved by "
        f"@madolson): https://x/releases/{tag}\n"
        f"Downstream outputs are now observed by reconciliation.",
        author=author,
    )


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
    pr_list = pulls if pulls is not None else [notes_pr()]
    repo.get_pulls.return_value = pr_list

    def _get_pull(number: int) -> MagicMock:
        for pr in pr_list:
            if pr.number == number:
                return pr
        raise GithubException(404, "no such PR", {})

    repo.get_pull.side_effect = _get_pull
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
        repo.get_git_ref.return_value = tag_ref()
    else:
        repo.get_release.side_effect = GithubException(404, "no release", {})
        repo.get_git_ref.side_effect = GithubException(404, "no tag", {})
    return repo


def gh_mock(repo: MagicMock, *, member: bool = True) -> MagicMock:
    gh = MagicMock()
    gh.get_repo.return_value = repo
    gh.get_organization.return_value.get_team_by_slug.return_value.has_in_members.return_value = member
    return gh
