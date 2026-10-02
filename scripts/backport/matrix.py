"""Generate GitHub Actions matrix JSON from the backport registry.

Usage:
    python -m scripts.backport.matrix --registry repos.yml
    python -m scripts.backport.matrix --registry repos.yml --repo owner/repo
    python -m scripts.backport.matrix --registry repos.yml --repo owner/repo --project-number 14
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.backport.registry import load_registry
from scripts.common.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def build_matrix(
    registry_path: str,
    *,
    repo_filter: str | None = None,
    project_number_filter: int | None = None,
) -> dict:
    """Build a GitHub Actions matrix from the registry.

    Returns {"include": [...]} where each entry is one {repo, branch} job.
    """
    registry = load_registry(registry_path)
    entries = []

    for repo_entry in registry.repos:
        if repo_filter and repo_entry.repo != repo_filter:
            continue
        for branch_entry in repo_entry.branches:
            if project_number_filter is not None and branch_entry.project_number != project_number_filter:
                continue
            entries.append({
                "repo": repo_entry.repo,
                "repo_slug": repo_entry.repo.replace("/", "-"),
                "project_owner": repo_entry.project_owner,
                "project_owner_type": repo_entry.project_owner_type,
                "project_number": branch_entry.project_number,
                "branch": branch_entry.branch,
                "push_repo": repo_entry.effective_push_repo,
                "language": repo_entry.language,
                "build_commands_json": json.dumps(list(repo_entry.build_commands)),
                "validation_setup_commands_json": json.dumps(
                    list(repo_entry.validation_setup_commands)
                ),
                "repair_validation_failures": repo_entry.repair_validation_failures,
            })

    return {"include": entries}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True, help="Path to repos.yml")
    parser.add_argument("--repo", default="", help="Filter to this repo only")
    parser.add_argument("--project-number", type=int, default=None, help="Filter to this project number")
    parser.add_argument("--output-file", default="", help="Write to file instead of stdout (for GITHUB_OUTPUT)")
    args = parser.parse_args()
    configure_logging()

    matrix = build_matrix(
        args.registry,
        repo_filter=args.repo or None,
        project_number_filter=args.project_number,
    )

    has_entries = len(matrix["include"]) > 0
    # Logs go to stderr, so they never mix with the stdout matrix output.
    logger.info(
        "Backport matrix from %s (repo filter: %s, project filter: %s): %d entr%s: %s",
        args.registry, args.repo or "all",
        "all" if args.project_number is None else args.project_number,
        len(matrix["include"]), "y" if len(matrix["include"]) == 1 else "ies",
        ", ".join(f"{e['repo']}@{e['branch']}" for e in matrix["include"]) or "none",
    )
    if not has_entries:
        logger.warning("No registry entries matched the filters; every downstream job will be skipped.")
    matrix_json = json.dumps(matrix)

    if args.output_file:
        with open(args.output_file, "a") as f:
            f.write(f"matrix={matrix_json}\n")
            f.write(f"has_entries={'true' if has_entries else 'false'}\n")
    else:
        print(f"matrix={matrix_json}")
        print(f"has_entries={'true' if has_entries else 'false'}")


if __name__ == "__main__":
    main()
