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
from typing import Any

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
    every authorization point. ``required_checks`` are check-run names
    evaluated on the exact candidate SHA for tracker display (informational;
    the release gates on qualification, never on these results); the loader
    rejects an empty list so the tracker always shows the candidate's CI
    state. ``checks_workflow`` is the
    workflow file (basename, e.g. ``ci.yml``) whose runs the required checks
    must come from: check-run names are not unique across workflows (valkey's
    ``ci.yml`` and ``daily.yml`` share job names), so a run from another
    workflow on the same SHA must never satisfy (or clobber) a displayed
    result.
    ``check_timeout_minutes`` bounds how long a still-running required check
    may sit before it is reported STALLED instead of pending.
    ``daily_workflow`` / ``daily_max_age_hours`` configure the optional
    branch-level daily-CI gate: the newest completed run of that workflow on
    the release branch must be green and no older than the bound for the
    release to reach READY. Both fields are set together or omitted
    together; omitted means the gate is skipped.
    """

    repo: str
    authorized_team: str
    branches: tuple[str, ...]
    required_checks: tuple[str, ...]
    checks_workflow: str
    check_timeout_minutes: int
    downstream: DownstreamPolicy
    daily_workflow: "str | None" = None
    daily_max_age_hours: "int | None" = None

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


def validate_release_branch(policy: RepoReleasePolicy, branch: str) -> None:
    """Refuse a branch that is not one of *policy*'s configured release lines.

    The one choke point every entry point that acts against a caller-supplied
    branch is expected to funnel through, so a policy load that never listed
    the branch (a typo, a mothballed line, an attempt to run the controller
    against a numeric non-release ref) is refused *before* any GitHub read
    or write is issued. Raises :class:`ValueError` with a message that names
    the offending branch and the allowed set so the operator can see what
    the policy actually configures. Numeric-looking branch strings still
    have to pass the same shape check that :func:`parse_release_branch`
    already enforces at policy load time (reused here so both a wrong-form
    branch and a right-form-but-unconfigured branch fail at the same place).
    """
    parse_release_branch(branch)  # right-shape check; raises on '6.9-nope', 'main', ...
    if branch not in policy.branches:
        raise ValueError(
            f"branch {branch!r} is not a configured release branch of "
            f"{policy.repo} (policy allows: {', '.join(policy.branches)})"
        )


def load_policy(path: str) -> dict[str, RepoReleasePolicy]:
    """Parse *path* into a mapping of ``repo full name -> policy``.

    Raises :class:`ValueError` on structural problems so a bad policy fails
    the run instead of silently authorizing (or blocking) the wrong thing.
    Unknown keys are rejected at every mapping level: a typo'd optional key
    (``daly_workflow``) would otherwise silently disable the gate it meant
    to configure.
    """
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict) or not isinstance(raw.get("repos"), list):
        raise ValueError(f"{path}: expected a top-level 'repos' list")
    _reject_unknown_keys(path, "top level", raw, _TOP_LEVEL_KEYS)
    schema_version = raw.get("schema_version", 1)
    if schema_version != 1:
        raise ValueError(
            f"{path}: unsupported schema_version {schema_version!r} (expected 1)"
        )

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


_TOP_LEVEL_KEYS = frozenset({"schema_version", "repos"})
_REPO_ENTRY_KEYS = frozenset({
    "repo", "authorized_team", "branches", "required_checks",
    "checks_workflow", "check_timeout_minutes", "daily_workflow",
    "daily_max_age_hours", "downstream",
})


def _reject_unknown_keys(path: str, where: str, mapping: "dict[str, object]",
                         allowed: "frozenset[str]") -> None:
    unknown = sorted(str(key) for key in mapping if key not in allowed)
    if unknown:
        raise ValueError(
            f"{path}: unknown key(s) at {where}: {', '.join(unknown)}; "
            f"a misspelled key would silently misconfigure the policy"
        )


def _workflow_basename(ctx: str, field: str, value: object,
                       example: str, why: str = "") -> str:
    """Validate a workflow file basename field, returning it stripped.

    A path (anything containing ``/``) is refused: runs are matched by the
    workflow's file basename, and a path would silently never match.
    """
    if not isinstance(value, str) or not value.strip() or "/" in value:
        raise ValueError(
            f"{ctx}: {field} must be a workflow file basename "
            f"(e.g. {example!r}), got {value!r}{why}"
        )
    return value.strip()


def _https_url(ctx: str, field: str, value: object) -> str:
    if not isinstance(value, str) or not value.startswith("https://"):
        raise ValueError(
            f"{ctx}: downstream.{field} must be an https URL, got {value!r}"
        )
    return value


def _parse_entry(path: str, entry: dict[str, object]) -> RepoReleasePolicy:
    repo = entry.get("repo")
    if not isinstance(repo, str) or repo.count("/") != 1:
        raise ValueError(f"{path}: 'repo' must be 'owner/name', got {repo!r}")
    _reject_unknown_keys(path, f"repos entry {repo}", entry, _REPO_ENTRY_KEYS)

    team = entry.get("authorized_team")
    if not isinstance(team, str) or not (
        (team.count("/") == 1 and all(part.strip() for part in team.split("/")))
        or (team.startswith("user:") and team[len("user:"):].strip() != "" and "/" not in team)
    ):
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
            f"names; an empty list would leave the tracker with no candidate "
            f"CI display"
        )
    if len(set(checks)) != len(checks):
        # A duplicate usually means a copy-paste error hid the check that
        # was actually meant; fail loudly at load time.
        raise ValueError(
            f"{path}: {repo}: 'required_checks' contains duplicate entries"
        )

    workflow = _workflow_basename(
        f"{path}: {repo}", "'checks_workflow'", entry.get("checks_workflow"),
        "ci.yml",
        why="; without it, a same-named check "
            "run from another workflow could satisfy or clobber a requirement",
    )

    timeout = entry.get("check_timeout_minutes")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError(
            f"{path}: {repo}: 'check_timeout_minutes' must be a positive integer, "
            f"got {timeout!r}"
        )

    raw_daily_workflow = entry.get("daily_workflow")
    raw_daily_age = entry.get("daily_max_age_hours")
    # The two fields configure one gate; one without the other is a
    # half-configured policy (a workflow with no freshness bound, or a bound
    # with nothing to measure), not something to default around.
    if (raw_daily_workflow is None) != (raw_daily_age is None):
        raise ValueError(
            f"{path}: {repo}: 'daily_workflow' and 'daily_max_age_hours' "
            f"configure one gate and must be set together or omitted together"
        )
    daily_workflow: "str | None" = None
    daily_max_age_hours: "int | None" = None
    if raw_daily_workflow is not None:
        daily_workflow = _workflow_basename(
            f"{path}: {repo}", "'daily_workflow'", raw_daily_workflow,
            "daily.yml",
        )
        if (not isinstance(raw_daily_age, int) or isinstance(raw_daily_age, bool)
                or raw_daily_age < 1):
            raise ValueError(
                f"{path}: {repo}: 'daily_max_age_hours' must be an integer >= 1, "
                f"got {raw_daily_age!r}"
            )
        daily_max_age_hours = raw_daily_age

    return RepoReleasePolicy(
        repo=repo,
        authorized_team=team,
        branches=tuple(normalized),
        required_checks=tuple(c.strip() for c in checks),
        checks_workflow=workflow,
        check_timeout_minutes=timeout,
        downstream=_parse_downstream(path, repo, entry.get("downstream")),
        daily_workflow=daily_workflow,
        daily_max_age_hours=daily_max_age_hours,
    )


_DOWNSTREAM_REPO_FIELDS = (
    "automation_repo", "hashes_repo", "container_repo", "doc_repo",
    "website_repo", "bundle_repo", "helm_repo",
)
# Public-endpoint fields may be EMPTY, meaning "not configured for this
# repository": the verifier then reports the corresponding output as
# informational (never VERIFIED, never failing the release). This exists
# for fork policies, where pointing these at the real upstream registries
# and downloads would let a fork E2E false-VERIFY artifacts it never
# produced. Non-empty values must still satisfy the full shape check.
_DOWNSTREAM_IMAGE_FIELDS = ("dockerhub_repo", "bundle_dockerhub_repo")
_DOWNSTREAM_COUNT_FIELDS = (
    "qualification_x86_archive_jobs", "qualification_arm_archive_jobs",
    "qualification_rpm_jobs", "qualification_deb_jobs",
)
_DOWNSTREAM_KEYS = frozenset({
    *_DOWNSTREAM_REPO_FIELDS, *_DOWNSTREAM_IMAGE_FIELDS,
    *_DOWNSTREAM_COUNT_FIELDS,
    "build_workflow", "qualification_workflow",
    "downloads_base_url", "tarball_targets",
    "ghcr_image_repo", "ecr_namespace", "helm_index_url",
})


def _parse_downstream(path: str, repo: str, raw: object) -> DownstreamPolicy:
    ctx = f"{path}: {repo}"
    if not isinstance(raw, dict):
        raise ValueError(f"{ctx}: 'downstream' must be a mapping")
    _reject_unknown_keys(path, f"{repo} downstream", raw, _DOWNSTREAM_KEYS)

    values: "dict[str, Any]" = {}
    for field in _DOWNSTREAM_REPO_FIELDS:
        value = raw.get(field)
        if not isinstance(value, str) or value.count("/") != 1:
            raise ValueError(
                f"{ctx}: downstream.{field} must be 'owner/name', got {value!r}"
            )
        values[field] = value

    # Public-endpoint fields: an empty string means "not configured for
    # this repository" (the verifier reports the output as informational);
    # a non-empty value must be 'owner/name'.
    for field in (*_DOWNSTREAM_IMAGE_FIELDS, "ghcr_image_repo"):
        value = raw.get(field)
        if not isinstance(value, str) or (value != "" and value.count("/") != 1):
            raise ValueError(
                f"{ctx}: downstream.{field} must be 'owner/name', or empty "
                f"for a repository without that public endpoint, got {value!r}"
            )
        values[field] = value

    values["build_workflow"] = _workflow_basename(
        ctx, "downstream.build_workflow", raw.get("build_workflow"),
        "build-release.yml",
    )
    values["qualification_workflow"] = _workflow_basename(
        ctx, "downstream.qualification_workflow",
        raw.get("qualification_workflow"), "qualify-release.yml",
    )
    # The two URL endpoints follow the same empty-means-unconfigured rule.
    raw_downloads = raw.get("downloads_base_url")
    values["downloads_base_url"] = ("" if raw_downloads == "" else _https_url(
        ctx, "downloads_base_url", raw_downloads,
    ).rstrip("/"))
    raw_helm_index = raw.get("helm_index_url")
    values["helm_index_url"] = ("" if raw_helm_index == "" else _https_url(
        ctx, "helm_index_url", raw_helm_index,
    ))

    targets = raw.get("tarball_targets")
    if not isinstance(targets, list) or not targets or not all(
        isinstance(t, str) and t.count("/") == 1 for t in targets
    ):
        raise ValueError(
            f"{ctx}: downstream.tarball_targets must be a non-empty list "
            f"of '<platform>/<arch>' entries"
        )
    values["tarball_targets"] = tuple(targets)

    for field in _DOWNSTREAM_COUNT_FIELDS:
        value = raw.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(
                f"{ctx}: downstream.{field} must be the exact "
                f"qualification matrix leg count (updated deliberately when "
                f"platforms change), got {value!r}"
            )
        values[field] = value

    ecr_namespace = raw.get("ecr_namespace")
    if not isinstance(ecr_namespace, str) or "/" in ecr_namespace or (
            ecr_namespace != "" and not ecr_namespace.strip()):
        raise ValueError(
            f"{ctx}: downstream.ecr_namespace must be the ECR Public "
            f"registry alias (e.g. 'valkey'), or empty for a repository "
            f"without that public endpoint, got {ecr_namespace!r}"
        )
    values["ecr_namespace"] = ecr_namespace.strip()

    return DownstreamPolicy(**values)
