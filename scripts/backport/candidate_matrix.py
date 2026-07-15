"""Discover bounded project-board candidates for phased backport workflows."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from github import Auth, Github

from scripts.backport.pr_creator import BackportPRCreator, find_existing_pr
from scripts.backport.project_discovery import ProjectBackportDiscovery
from scripts.backport.registry import load_registry
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_reporting import parse_previous_applied
from scripts.common.operational_controls import enforce_operational_access
from scripts.common.phase_artifact import ArtifactError, write_json

logger = logging.getLogger(__name__)

_HARD_MAX_TOTAL = 100
_SAFE_SUFFIX_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_LEGACY_SWEEP_BRANCH_PREFIX = "agent/backport/sweep"


def build_candidate_matrix(
    registry_path: str,
    token: str,
    *,
    repo_filter: str = "",
    project_number_filter: int | None = None,
    max_candidates: int = 2,
    github_client: Any | None = None,
    graphql_client: Any | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return bounded attempts for the aggregate's successful-application cap."""
    if not token:
        raise ArtifactError("candidate discovery token is required")
    if max_candidates < 0 or max_candidates > _HARD_MAX_TOTAL:
        raise ArtifactError(
            f"max_candidates must be between 0 and {_HARD_MAX_TOTAL}"
        )
    registry = load_registry(registry_path)
    gh = github_client or Github(auth=Auth.Token(token))
    gql = graphql_client or GitHubGraphQLClient(token)
    entries: list[dict[str, Any]] = []

    for repo_entry in registry.repos:
        if repo_filter and repo_entry.repo != repo_filter:
            continue
        try:
            enforce_operational_access(repo_entry.repo, repo_entry.automation)
        except RuntimeError as exc:
            logger.warning(
                "Skipping %s because operational policy denied it: %s",
                repo_entry.repo,
                exc,
            )
            continue
        queued_for_repo = 0
        for branch_entry in repo_entry.branches:
            if queued_for_repo >= repo_entry.automation.max_queue_depth:
                break
            if (
                project_number_filter is not None
                and branch_entry.project_number != project_number_filter
            ):
                continue
            discovery = ProjectBackportDiscovery(
                gql,
                project_owner=repo_entry.project_owner,
                project_number=branch_entry.project_number,
                source_repo=repo_entry.repo,
                project_owner_type=repo_entry.project_owner_type,
                implicit_target_branch=branch_entry.branch,
            )
            candidates = discovery.discover([branch_entry.branch]).get(
                branch_entry.branch,
                [],
            )
            candidates.sort(key=lambda candidate: candidate.merged_at or "")
            legacy_applied = _legacy_sweep_pr_numbers(
                gh,
                repo_entry.repo,
                repo_entry.effective_push_repo,
                branch_entry.branch,
            )
            creator = BackportPRCreator(
                gh,
                base_repo=repo_entry.repo,
                push_repo=(
                    repo_entry.effective_push_repo
                    if repo_entry.effective_push_repo != repo_entry.repo
                    else None
                ),
                backport_label=repo_entry.backport_label,
                llm_conflict_label=repo_entry.llm_conflict_label,
            )
            for candidate in candidates:
                if queued_for_repo >= repo_entry.automation.max_queue_depth:
                    break
                if candidate.source_pr_number in legacy_applied:
                    continue
                duplicate = creator.check_duplicate(
                    candidate.source_pr_number,
                    branch_entry.branch,
                )
                if duplicate:
                    continue
                suffix = _artifact_suffix(
                    repo_entry.repo,
                    branch_entry.branch,
                    candidate.source_pr_number,
                )
                entries.append(
                    {
                        "repo": repo_entry.repo,
                        "branch": branch_entry.branch,
                        "source_pr_number": candidate.source_pr_number,
                        "push_repo": repo_entry.push_repo or "",
                        "artifact_suffix": suffix,
                    }
                )
                queued_for_repo += 1
                if len(entries) > _HARD_MAX_TOTAL:
                    raise ArtifactError(
                        f"candidate matrix exceeds {_HARD_MAX_TOTAL} total entries"
                    )
    return {"include": entries}


def _legacy_sweep_pr_numbers(
    gh: Any,
    repository: str,
    push_repository: str,
    target_branch: str,
) -> set[int]:
    branch = f"{_LEGACY_SWEEP_BRANCH_PREFIX}/{target_branch}"
    existing = find_existing_pr(
        gh,
        repository,
        push_repository,
        branch,
    )
    if existing is None:
        return set()
    body = getattr(existing, "body", "")
    if not isinstance(body, str):
        return set()
    return {
        candidate.source_pr_number
        for candidate in parse_previous_applied(body)
    }


def _artifact_suffix(repository: str, branch: str, source_pr_number: int) -> str:
    value = f"{repository}-{branch}-pr-{source_pr_number}"
    value = _SAFE_SUFFIX_RE.sub("-", value).strip("-.")
    if not value or len(value) > 120:
        raise ArtifactError("candidate artifact suffix is empty or oversized")
    return value


def _write_workflow_outputs(path: Path, matrix: dict[str, list[dict[str, Any]]]) -> None:
    entries = matrix["include"]
    rendered = json.dumps(matrix, separators=(",", ":"), sort_keys=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"matrix={rendered}\n")
        handle.write(f"has_entries={'true' if entries else 'false'}\n")
        handle.write(f"candidate_count={len(entries)}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", default="")
    parser.add_argument("--project-number", type=int)
    parser.add_argument("--max-candidates", type=int, default=2)
    parser.add_argument(
        "--token",
        default=os.environ.get("DISCOVERY_GITHUB_TOKEN", ""),
    )
    parser.add_argument("--output", default="backport-candidates.json")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    matrix = build_candidate_matrix(
        args.registry,
        args.token,
        repo_filter=args.repo,
        project_number_filter=args.project_number,
        max_candidates=args.max_candidates,
    )
    write_json(Path(args.output), matrix)
    if args.github_output:
        _write_workflow_outputs(Path(args.github_output), matrix)
    else:
        print(json.dumps(matrix, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
