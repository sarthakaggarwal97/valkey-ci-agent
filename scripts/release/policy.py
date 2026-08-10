"""Load and validate the release policy registry (``release_policy.yml``).

The policy is the controller's authority for *what is allowed*: which
repositories can be released, which branches are release lines, which check
runs must pass on the exact candidate SHA, and which GitHub team may start a
release or adopt a moved branch head. Everything else (versions, candidates,
readiness) is recomputed from live GitHub state.

Validation is strict and fails loudly: a policy entry that is missing required
fields, lists no required checks, or names a non-release branch is a
configuration bug, not something to work around at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass

import yaml

from scripts.release.versioning import parse_release_branch

# Label carried by every release tracking issue. Scopes issue listings
# server-side (mirroring issue_dedup's filter_label) and makes trackers
# discoverable by humans.
TRACKER_LABEL = "release-tracker"


@dataclass(frozen=True)
class DownstreamPolicy:
    """Where the release's public outputs live and how to verify them.

    ``automation_repo`` hosts the build/qualification workflows;
    ``qualification_workflow`` is the no-publish qualification entry point
    (workflow file basename). ``tarball_targets`` are the
    ``<platform>/<arch>`` pairs the tarball builds publish (must mirror the
    automation repo's build matrix). ``dockerhub_repo`` /
    ``bundle_dockerhub_repo`` are Docker Hub ``namespace/name`` image repos.
    """

    automation_repo: str
    build_workflow: str
    qualification_workflow: str
    qualification_rpm_jobs: int
    qualification_deb_jobs: int
    downloads_base_url: str
    tarball_targets: tuple[str, ...]
    hashes_repo: str
    container_repo: str
    doc_repo: str
    website_repo: str
    bundle_repo: str
    helm_repo: str
    dockerhub_repo: str
    bundle_dockerhub_repo: str
    ghcr_image_repo: str
    ecr_namespace: str
    helm_index_url: str
    # Exact archive-build leg counts of the qualification matrix, one per
    # architecture group (part of the reviewed inventory alongside the
    # RPM/DEB counts). Defaults exist for direct construction in tests; the
    # loader still requires both fields in the policy file.
    qualification_x86_archive_jobs: int = 2
    qualification_arm_archive_jobs: int = 2


@dataclass(frozen=True)
class RepoReleasePolicy:
    """Release policy for one repository.

    ``authorized_team`` is ``org/team-slug``; membership is checked live at
    every authorization point. ``required_checks`` are check-run names that
    must pass on the exact candidate SHA; the loader rejects an empty list so
    a repository can never be trivially "ready". ``checks_workflow`` is the
    workflow file (basename, e.g. ``ci.yml``) whose runs the required checks
    must come from: check-run names are not unique across workflows (valkey's
    ``ci.yml`` and ``daily.yml`` share job names), so a run from another
    workflow on the same SHA must never satisfy (or clobber) a requirement.
    ``check_timeout_minutes`` bounds how long a still-running required check
    may sit before it is reported STALLED instead of pending.
    """

    repo: str
    authorized_team: str
    branches: tuple[str, ...]
    required_checks: tuple[str, ...]
    checks_workflow: str
    check_timeout_minutes: int
    downstream: DownstreamPolicy

    @property
    def team_org(self) -> str:
        return self.authorized_team.split("/", 1)[0]

    @property
    def team_slug(self) -> str:
        return self.authorized_team.split("/", 1)[1]

    @property
    def mention(self) -> str:
        """The @-mention for notifications: a team or the fork user."""
        if self.authorized_team.startswith("user:"):
            return f"@{self.authorized_team[len('user:'):]}"
        return f"@{self.authorized_team}"


def load_policy(path: str) -> dict[str, RepoReleasePolicy]:
    """Parse *path* into a mapping of ``repo full name -> policy``.

    Raises :class:`ValueError` on structural problems so a bad policy fails
    the run instead of silently authorizing (or blocking) the wrong thing.
    """
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict) or not isinstance(raw.get("repos"), list):
        raise ValueError(f"{path}: expected a top-level 'repos' list")

    policies: dict[str, RepoReleasePolicy] = {}
    for entry in raw["repos"]:
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: every repos entry must be a mapping")
        policy = _parse_entry(path, entry)
        if policy.repo in policies:
            raise ValueError(f"{path}: duplicate policy for {policy.repo}")
        policies[policy.repo] = policy
    if not policies:
        raise ValueError(f"{path}: no repositories configured")
    return policies


def _parse_entry(path: str, entry: dict[str, object]) -> RepoReleasePolicy:
    repo = entry.get("repo")
    if not isinstance(repo, str) or repo.count("/") != 1:
        raise ValueError(f"{path}: 'repo' must be 'owner/name', got {repo!r}")

    team = entry.get("authorized_team")
    valid_team_form = isinstance(team, str) and (
        (team.count("/") == 1 and all(part.strip() for part in team.split("/")))
        or (team.startswith("user:") and team[len("user:"):].strip() != "" and "/" not in team)
    )
    if not isinstance(team, str) or not valid_team_form:
        raise ValueError(
            f"{path}: {repo}: 'authorized_team' must be 'org/team-slug' or "
            f"'user:<login>' (fork policies only), got {team!r}"
        )
    # The user: form bypasses team review and the environment-protection
    # check by design (a personal fork has neither); it must never be one
    # policy-file edit away from doing so upstream.
    if team.startswith("user:") and repo.startswith("valkey-io/"):
        raise ValueError(
            f"{path}: {repo}: 'user:' authorization is for forks only and is "
            f"refused for valkey-io repositories"
        )

    branches = entry.get("branches")
    if not isinstance(branches, list) or not branches:
        raise ValueError(f"{path}: {repo}: 'branches' must be a non-empty list")
    normalized: list[str] = []
    for raw_branch in branches:
        branch = str(raw_branch).strip()
        try:
            parse_release_branch(branch)
        except ValueError as exc:
            raise ValueError(f"{path}: {repo}: {exc}") from exc
        normalized.append(branch)

    checks = entry.get("required_checks")
    if not isinstance(checks, list) or not checks or not all(
        isinstance(c, str) and c.strip() for c in checks
    ):
        raise ValueError(
            f"{path}: {repo}: 'required_checks' must be a non-empty list of check "
            f"names; an empty list would make every candidate trivially ready"
        )
    if len(set(checks)) != len(checks):
        # A duplicate usually means a copy-paste error hid the check that
        # was actually meant; fail loudly at load time.
        raise ValueError(
            f"{path}: {repo}: 'required_checks' contains duplicate entries"
        )

    workflow = entry.get("checks_workflow")
    if not isinstance(workflow, str) or not workflow.strip() or "/" in workflow:
        raise ValueError(
            f"{path}: {repo}: 'checks_workflow' must be a workflow file basename "
            f"(e.g. 'ci.yml'), got {workflow!r}; without it, a same-named check "
            f"run from another workflow could satisfy or clobber a requirement"
        )

    timeout = entry.get("check_timeout_minutes")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError(
            f"{path}: {repo}: 'check_timeout_minutes' must be a positive integer, "
            f"got {timeout!r}"
        )

    return RepoReleasePolicy(
        repo=repo,
        authorized_team=team,
        branches=tuple(normalized),
        required_checks=tuple(c.strip() for c in checks),
        checks_workflow=workflow.strip(),
        check_timeout_minutes=timeout,
        downstream=_parse_downstream(path, repo, entry.get("downstream")),
    )


_DOWNSTREAM_REPO_FIELDS = (
    "automation_repo", "hashes_repo", "container_repo", "doc_repo",
    "website_repo", "bundle_repo", "helm_repo",
)
_DOWNSTREAM_IMAGE_FIELDS = ("dockerhub_repo", "bundle_dockerhub_repo")


def _parse_downstream(path: str, repo: str, raw: object) -> DownstreamPolicy:
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: {repo}: 'downstream' must be a mapping")

    values: dict[str, object] = {}
    for field in (*_DOWNSTREAM_REPO_FIELDS, *_DOWNSTREAM_IMAGE_FIELDS):
        value = raw.get(field)
        if not isinstance(value, str) or value.count("/") != 1:
            raise ValueError(
                f"{path}: {repo}: downstream.{field} must be 'owner/name', got {value!r}"
            )
        values[field] = value

    build_workflow = raw.get("build_workflow")
    if not isinstance(build_workflow, str) or not build_workflow.strip() or "/" in build_workflow:
        raise ValueError(
            f"{path}: {repo}: downstream.build_workflow must be a workflow file "
            f"basename (e.g. 'build-release.yml'), got {build_workflow!r}"
        )

    workflow = raw.get("qualification_workflow")
    if not isinstance(workflow, str) or not workflow.strip() or "/" in workflow:
        raise ValueError(
            f"{path}: {repo}: downstream.qualification_workflow must be a workflow "
            f"file basename, got {workflow!r}"
        )

    base_url = raw.get("downloads_base_url")
    if not isinstance(base_url, str) or not base_url.startswith("https://"):
        raise ValueError(
            f"{path}: {repo}: downstream.downloads_base_url must be an https URL, "
            f"got {base_url!r}"
        )

    targets = raw.get("tarball_targets")
    if not isinstance(targets, list) or not targets or not all(
        isinstance(t, str) and t.count("/") == 1 for t in targets
    ):
        raise ValueError(
            f"{path}: {repo}: downstream.tarball_targets must be a non-empty list "
            f"of '<platform>/<arch>' entries"
        )

    matrix_counts = {}
    for field in ("qualification_x86_archive_jobs", "qualification_arm_archive_jobs",
                  "qualification_rpm_jobs", "qualification_deb_jobs"):
        value = raw.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(
                f"{path}: {repo}: downstream.{field} must be the exact "
                f"qualification matrix leg count (updated deliberately when "
                f"platforms change), got {value!r}"
            )
        matrix_counts[field] = value

    ecr_namespace = raw.get("ecr_namespace")
    if not isinstance(ecr_namespace, str) or not ecr_namespace.strip() or "/" in ecr_namespace:
        raise ValueError(
            f"{path}: {repo}: downstream.ecr_namespace must be the ECR Public "
            f"registry alias (e.g. 'valkey'), got {ecr_namespace!r}"
        )

    ghcr_image_repo = raw.get("ghcr_image_repo")
    if not isinstance(ghcr_image_repo, str) or ghcr_image_repo.count("/") != 1:
        raise ValueError(
            f"{path}: {repo}: downstream.ghcr_image_repo must be 'owner/name', "
            f"got {ghcr_image_repo!r}"
        )
    helm_index_url = raw.get("helm_index_url")
    if not isinstance(helm_index_url, str) or not helm_index_url.startswith("https://"):
        raise ValueError(
            f"{path}: {repo}: downstream.helm_index_url must be an https URL, "
            f"got {helm_index_url!r}"
        )

    return DownstreamPolicy(
        automation_repo=str(values["automation_repo"]),
        build_workflow=build_workflow.strip(),
        qualification_workflow=workflow.strip(),
        qualification_x86_archive_jobs=matrix_counts["qualification_x86_archive_jobs"],
        qualification_arm_archive_jobs=matrix_counts["qualification_arm_archive_jobs"],
        qualification_rpm_jobs=matrix_counts["qualification_rpm_jobs"],
        qualification_deb_jobs=matrix_counts["qualification_deb_jobs"],
        downloads_base_url=base_url.rstrip("/"),
        tarball_targets=tuple(targets),
        hashes_repo=str(values["hashes_repo"]),
        container_repo=str(values["container_repo"]),
        doc_repo=str(values["doc_repo"]),
        website_repo=str(values["website_repo"]),
        bundle_repo=str(values["bundle_repo"]),
        helm_repo=str(values["helm_repo"]),
        dockerhub_repo=str(values["dockerhub_repo"]),
        bundle_dockerhub_repo=str(values["bundle_dockerhub_repo"]),
        ghcr_image_repo=ghcr_image_repo,
        ecr_namespace=ecr_namespace.strip(),
        helm_index_url=helm_index_url,
    )
