"""Durable desired-label markers and idempotent GitHub reconciliation."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

from github import Auth, Github
from github.GithubException import GithubException

from scripts.backport.registry import load_registry
from scripts.common.desired_comments import reconcile_issue_desired_comments
from scripts.common.github_client import retry_github_call
from scripts.common.markdown import with_required_suffix
from scripts.common.operational_controls import (
    OperationalPolicy,
    enforce_operational_access,
)
from scripts.common.phase_artifact import (
    ArtifactError,
    canonical_json_bytes,
    sha256_bytes,
)

logger = logging.getLogger(__name__)

_MARKER_PREFIX = "valkey-ci-agent:desired-labels:v1"
_MARKER_RE = re.compile(
    rf"<!-- {re.escape(_MARKER_PREFIX)} "
    r"payload=(?P<payload>[A-Za-z0-9_-]+) "
    r"sha256=(?P<sha>[0-9a-f]{64}) -->",
)
_LABEL_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_LOGIN_RE = re.compile(r"^[A-Za-z0-9-]{1,100}(?:\[bot\])?$")
_PAYLOAD_KEYS = {"version", "labels"}
_MAX_ITEMS = 2_000
_FIXED_LABELS = {"test-failure", "possible-valkey-bug"}
_LABEL_DEFAULTS: dict[str, tuple[str, str]] = {
    "backport": ("0e8a16", "Backport PR opened by valkey-ci-agent"),
    "ai-resolved-conflicts": (
        "fbca04",
        "Cherry-pick conflicts resolved by AI; needs human review",
    ),
}


@dataclass(frozen=True)
class ReconcileSummary:
    repositories: int = 0
    scanned: int = 0
    marked: int = 0
    updated: int = 0
    comments_reconciled: int = 0


@dataclass(frozen=True)
class MetadataRepositoryPolicy:
    labels: frozenset[str]
    automation: OperationalPolicy
    registry_backed: bool


def with_desired_labels(body: str, labels: tuple[str, ...] | list[str]) -> str:
    """Attach a bounded marker that survives partial label-application failure."""
    normalized = _normalize_labels(labels)
    without_old = _MARKER_RE.sub("", body).rstrip()
    if not normalized:
        return without_old
    payload = canonical_json_bytes({"version": 1, "labels": list(normalized)})
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    marker = (
        f"<!-- {_MARKER_PREFIX} payload={encoded} "
        f"sha256={sha256_bytes(payload)} -->"
    )
    return with_required_suffix(without_old, marker)


def desired_labels_from_body(body: str) -> tuple[str, ...] | None:
    """Parse one exact desired-label marker, rejecting malformed duplicates."""
    matches = list(_MARKER_RE.finditer(body))
    if not matches:
        return None
    if len(matches) != 1:
        raise ArtifactError("metadata body contains multiple desired-label markers")
    match = matches[0]
    encoded = match.group("payload")
    padding = "=" * (-len(encoded) % 4)
    try:
        payload = base64.b64decode(
            encoded + padding,
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise ArtifactError("desired-label marker payload is invalid") from exc
    if not payload or len(payload) > 8 * 1024:
        raise ArtifactError("desired-label marker payload is empty or oversized")
    if sha256_bytes(payload) != match.group("sha"):
        raise ArtifactError("desired-label marker digest mismatch")
    try:
        raw = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ArtifactError("desired-label marker payload is not JSON") from exc
    if (
        not isinstance(raw, dict)
        or set(raw) != _PAYLOAD_KEYS
        or raw["version"] != 1
        or canonical_json_bytes(raw) != payload
    ):
        raise ArtifactError("desired-label marker schema is invalid")
    labels = raw["labels"]
    if not isinstance(labels, list):
        raise ArtifactError("desired-label marker labels are invalid")
    return _normalize_labels(labels)


def reconcile_labels(repo: Any, item: Any, labels: tuple[str, ...]) -> bool:
    """Ensure all desired labels exist and are attached; preserve other labels."""
    desired = _normalize_labels(labels)
    current = {
        str(getattr(label, "name", "") or "")
        for label in (getattr(item, "labels", None) or [])
    }
    missing = tuple(label for label in desired if label not in current)
    if not missing:
        return False
    for label in missing:
        _ensure_label(repo, label)
    retry_github_call(
        lambda: item.add_to_labels(*missing),
        retries=3,
        description=f"reconcile labels on item #{item.number}",
    )
    return True


def reconcile_repository(
    gh: Any,
    repository: str,
    *,
    writer_login: str,
    allowed_labels: set[str],
) -> ReconcileSummary:
    """Converge markers on bot-authored open issues and pull requests."""
    if not _LOGIN_RE.fullmatch(writer_login):
        raise ValueError("metadata writer login is invalid")
    allowed = set(_normalize_labels(sorted(allowed_labels)))
    repo = retry_github_call(
        lambda: gh.get_repo(repository),
        retries=3,
        description=f"get metadata repository {repository}",
    )
    scanned = marked = updated = comments_reconciled = 0
    for item in repo.get_issues(state="open"):
        scanned += 1
        if scanned > _MAX_ITEMS:
            raise RuntimeError(
                f"{repository} exceeds the metadata reconciliation item cap",
            )
        labels = desired_labels_from_body(str(getattr(item, "body", "") or ""))
        if labels is not None:
            marked += 1
            author = str(getattr(getattr(item, "user", None), "login", "") or "")
            if author != writer_login:
                logger.warning(
                    "Ignoring desired-label marker on non-bot item %s#%s",
                    repository,
                    getattr(item, "number", "?"),
                )
            else:
                unexpected = sorted(set(labels) - allowed)
                if unexpected:
                    raise RuntimeError(
                        f"{repository} desired labels are outside policy: {unexpected}",
                    )
                if reconcile_labels(repo, item, labels):
                    updated += 1
        if getattr(item, "pull_request", None) is not None:
            pull = retry_github_call(
                lambda: repo.get_pull(item.number),
                retries=3,
                description=f"get pull request #{item.number}",
            )
            head_sha = str(getattr(getattr(pull, "head", None), "sha", "") or "")
            if not re.fullmatch(r"[0-9a-f]{40}", head_sha):
                raise RuntimeError(
                    f"{repository} pull request #{item.number} has an invalid head SHA",
                )
            comments_reconciled += reconcile_issue_desired_comments(
                repo,
                item,
                current_head_sha=head_sha,
                writer_login=writer_login,
            )
    return ReconcileSummary(
        repositories=1,
        scanned=scanned,
        marked=marked,
        updated=updated,
        comments_reconciled=comments_reconciled,
    )


def _ensure_label(repo: Any, label: str) -> None:
    try:
        retry_github_call(
            lambda: repo.get_label(label),
            retries=3,
            description=f"get label {label!r}",
        )
        return
    except GithubException as exc:
        if exc.status != 404:
            raise
    color, description = _LABEL_DEFAULTS.get(
        label,
        ("ededed", f"Managed by valkey-ci-agent: {label}"),
    )
    try:
        retry_github_call(
            lambda: repo.create_label(
                name=label,
                color=color,
                description=description,
            ),
            retries=3,
            description=f"create label {label!r}",
        )
    except GithubException as exc:
        if exc.status != 422:
            raise


def _normalize_labels(labels: Any) -> tuple[str, ...]:
    if (
        not isinstance(labels, (list, tuple))
        or len(labels) > 20
        or not all(isinstance(label, str) and _LABEL_RE.fullmatch(label) for label in labels)
    ):
        raise ArtifactError("desired labels are invalid")
    normalized = tuple(labels)
    if len({label.casefold() for label in normalized}) != len(labels):
        raise ArtifactError("desired labels must be unique")
    return normalized


def metadata_repository_policies(
    registry_path: str,
) -> dict[str, MetadataRepositoryPolicy]:
    """Return the exact repository, label, and operational policy allowlist."""
    registry = load_registry(registry_path)
    labels_by_repo: dict[str, set[str]] = {}
    automation_by_repo: dict[str, OperationalPolicy] = {}
    for entry in registry.repos:
        labels_by_repo.setdefault(entry.repo, set()).update({
            entry.backport_label,
            entry.llm_conflict_label,
            "test-failure",
        })
        automation_by_repo[entry.repo] = entry.automation
    policies = {
        repository: MetadataRepositoryPolicy(
            labels=frozenset(labels),
            automation=automation_by_repo[repository],
            registry_backed=True,
        )
        for repository, labels in labels_by_repo.items()
    }
    policies["valkey-io/valkey-fuzzer"] = MetadataRepositoryPolicy(
        labels=frozenset(_FIXED_LABELS),
        automation=OperationalPolicy(),
        registry_backed=False,
    )
    return policies


def build_reconcile_matrix(
    registry_path: str,
    *,
    repositories: list[str] | tuple[str, ...] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Build a credentialless matrix containing only operationally enabled repos."""
    policies = metadata_repository_policies(registry_path)
    requested = set(repositories)
    if requested:
        unknown = requested - set(policies)
        if unknown:
            raise ValueError(
                f"repositories are outside metadata policy: {sorted(unknown)}"
            )
        policies = {
            name: policies[name]
            for name in sorted(requested)
        }
    include: list[dict[str, Any]] = []
    for repository, policy in sorted(policies.items()):
        try:
            enforce_operational_access(repository, policy.automation)
        except RuntimeError as exc:
            logger.warning(
                "Skipping metadata reconciliation for %s: %s",
                repository,
                exc,
            )
            continue
        include.append({
            "repo": repository,
            "repo_name": repository.split("/", 1)[1],
            "registry_backed": policy.registry_backed,
        })
    return {"include": include}


def _select_policies(
    registry_path: str,
    requested_repositories: list[str],
) -> dict[str, MetadataRepositoryPolicy]:
    policies = metadata_repository_policies(registry_path)
    requested = set(requested_repositories)
    if not requested:
        return policies
    unknown = requested - set(policies)
    if unknown:
        raise ValueError(
            f"repositories are outside metadata policy: {sorted(unknown)}"
        )
    return {name: policies[name] for name in sorted(requested)}


def _write_plan_outputs(
    path: str,
    matrix: dict[str, list[dict[str, Any]]],
) -> None:
    rendered = json.dumps(matrix, separators=(",", ":"), sort_keys=True)
    entries = matrix["include"]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"matrix={rendered}\n")
        handle.write(f"has_entries={'true' if entries else 'false'}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", action="append", default=[])
    parser.add_argument("--plan", action="store_true")
    parser.add_argument(
        "--github-output",
        default=os.environ.get("GITHUB_OUTPUT", ""),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("METADATA_GITHUB_TOKEN", ""),
    )
    parser.add_argument(
        "--writer-login",
        default=os.environ.get("PUBLISHER_IDENTITY", ""),
    )
    args = parser.parse_args(argv)
    if args.plan:
        try:
            matrix = build_reconcile_matrix(
                args.registry,
                repositories=args.repo,
            )
        except ValueError as exc:
            parser.error(str(exc))
        if args.github_output:
            _write_plan_outputs(args.github_output, matrix)
        else:
            print(json.dumps(matrix, sort_keys=True))
        return 0
    if not args.token:
        parser.error("METADATA_GITHUB_TOKEN is required")
    if not _LOGIN_RE.fullmatch(args.writer_login):
        parser.error("PUBLISHER_IDENTITY is required")

    try:
        policies = _select_policies(args.registry, args.repo)
    except ValueError as exc:
        parser.error(str(exc))

    gh = Github(auth=Auth.Token(args.token))
    total = ReconcileSummary()
    for repository, policy in sorted(policies.items()):
        enforce_operational_access(repository, policy.automation)
        result = reconcile_repository(
            gh,
            repository,
            writer_login=args.writer_login,
            allowed_labels=set(policy.labels),
        )
        total = ReconcileSummary(
            repositories=total.repositories + result.repositories,
            scanned=total.scanned + result.scanned,
            marked=total.marked + result.marked,
            updated=total.updated + result.updated,
            comments_reconciled=(
                total.comments_reconciled + result.comments_reconciled
            ),
        )
    print(json.dumps(total.__dict__, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
