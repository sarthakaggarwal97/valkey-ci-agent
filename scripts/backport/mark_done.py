"""Mark project items done from immutable merged-backport provenance."""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from github import Auth, Github

from scripts.backport.provenance import parse_provenance_commit
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.utils import build_branch_name
from scripts.common.phase_artifact import ArtifactError
from scripts.common.polling import (
    PollLoopError,
    add_poll_loop_args,
    format_poll_results,
    run_poll_loop_from_args,
)

logger = logging.getLogger(__name__)

_DEFAULT_STATUS_FIELD = "Status"
_DEFAULT_FROM_STATUS = "To be backported"
_DEFAULT_DONE_STATUS = "Done"

_PUBLISHER_LOGIN = "valkeyrie-bot[bot]"
_MAX_CLOSED_PULLS = 10_000


@dataclass
class BackportStatusUpdateResult:
    requested: list[int]
    updated: list[int] = field(default_factory=list)
    already_done: list[int] = field(default_factory=list)
    missing: list[int] = field(default_factory=list)
    skipped: dict[int, str] = field(default_factory=dict)
    unverified: list[int] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "updated": self.updated,
            "already_done": self.already_done,
            "missing": self.missing,
            "skipped": {str(k): v for k, v in sorted(self.skipped.items())},
            "unverified": self.unverified,
        }


def verify_prs_on_branch(
    repo_full_name: str,
    target_branch: str,
    pr_numbers: set[int],
    *,
    token: str = "",
    git_env: dict[str, str] | None = None,
    push_repo: str | None = None,
    github_client: Any | None = None,
    publisher_login: str = _PUBLISHER_LOGIN,
) -> set[int]:
    """Verify merged PR ancestry and the publisher's immutable provenance."""
    if not pr_numbers:
        return set()
    del git_env  # retained for API compatibility; verification is now REST-based.
    if github_client is None:
        if not token:
            raise ValueError("token is required for provenance verification")
        github_client = Github(auth=Auth.Token(token))
    repo = github_client.get_repo(repo_full_name)
    expected_push_repo = push_repo or repo_full_name
    expected_branches = {
        build_branch_name(number, target_branch): number
        for number in pr_numbers
    }
    verified: set[int] = set()
    pulls = repo.get_pulls(
        state="closed",
        base=target_branch,
        sort="updated",
        direction="desc",
    )
    for index, pull in enumerate(pulls):
        if index >= _MAX_CLOSED_PULLS:
            raise RuntimeError("closed backport PR scan exceeded its safety limit")
        branch = str(getattr(getattr(pull, "head", None), "ref", "") or "")
        number = expected_branches.get(branch)
        if number is None or number in verified:
            continue
        head_repo = str(
            getattr(
                getattr(getattr(pull, "head", None), "repo", None),
                "full_name",
                "",
            )
            or ""
        )
        author = str(getattr(getattr(pull, "user", None), "login", "") or "")
        if (
            head_repo != expected_push_repo
            or author != publisher_login
            or getattr(pull, "merged_at", None) is None
        ):
            continue
        try:
            _verify_merged_pull_provenance(
                repo,
                pull,
                repository=repo_full_name,
                target_branch=target_branch,
                source_pr_number=number,
            )
        except (ArtifactError, RuntimeError, ValueError) as exc:
            logger.warning(
                "Backport PR #%s failed provenance verification for source #%d: %s",
                getattr(pull, "number", "?"),
                number,
                exc,
            )
            continue
        verified.add(number)
        if verified == pr_numbers:
            break
    return verified


def _verify_merged_pull_provenance(
    repo: Any,
    pull: Any,
    *,
    repository: str,
    target_branch: str,
    source_pr_number: int,
) -> None:
    commits = _bounded_commits(pull.get_commits(), "backport PR")
    if len(commits) != 2:
        raise ArtifactError("backport PR must contain one target and one provenance commit")
    target_commit, attestation_commit = commits
    head_sha = str(getattr(getattr(pull, "head", None), "sha", "") or "").lower()
    if _commit_sha(attestation_commit) != head_sha:
        raise ArtifactError("provenance commit is not the immutable PR head")
    provenance = parse_provenance_commit(_commit_message(attestation_commit))
    expected = {
        "repository": repository,
        "target_branch": target_branch,
        "source_pr_number": source_pr_number,
        "target_commit": _commit_sha(target_commit),
    }
    for key, value in expected.items():
        if provenance[key] != value:
            raise ArtifactError(f"provenance {key} differs from merged PR")

    attestation_parents = _commit_parents(attestation_commit)
    target_parents = _commit_parents(target_commit)
    if attestation_parents != (provenance["target_commit"],):
        raise ArtifactError("provenance commit does not directly follow target commit")
    if target_parents != (provenance["base_commit"],):
        raise ArtifactError("target commit does not directly follow attested base")
    if (
        _commit_tree(target_commit) != provenance["validated_tree"]
        or _commit_tree(attestation_commit) != provenance["validated_tree"]
    ):
        raise ArtifactError("PR commit tree differs from validated provenance tree")

    source_pr = repo.get_pull(source_pr_number)
    if not bool(getattr(source_pr, "merged", False)):
        raise ArtifactError("source PR is no longer recorded as merged")
    merge_sha = str(getattr(source_pr, "merge_commit_sha", "") or "").lower() or None
    source_commits = tuple(
        _commit_sha(commit)
        for commit in _bounded_commits(source_pr.get_commits(), "source PR")
    )
    if (
        merge_sha != provenance["source_merge_commit"]
        or list(source_commits) != provenance["source_commits"]
    ):
        raise ArtifactError("source PR identity differs from published provenance")

    merge_commit = str(getattr(pull, "merge_commit_sha", "") or "").lower()
    if not merge_commit:
        raise ArtifactError("merged backport PR has no merge commit")
    comparison = repo.compare(merge_commit, target_branch)
    if str(getattr(comparison, "status", "") or "") not in {"ahead", "identical"}:
        raise ArtifactError("backport merge commit is not an ancestor of target branch")


def _bounded_commits(values: Any, label: str) -> list[Any]:
    commits: list[Any] = []
    for commit in values:
        commits.append(commit)
        if len(commits) > 1000:
            raise ArtifactError(f"{label} has more than 1000 commits")
    if not commits:
        raise ArtifactError(f"{label} has no commits")
    return commits


def _commit_sha(commit: Any) -> str:
    value = str(getattr(commit, "sha", "") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ArtifactError("GitHub returned a malformed commit SHA")
    return value


def _commit_message(commit: Any) -> str:
    value = getattr(getattr(commit, "commit", None), "message", None)
    if not isinstance(value, str) or not value:
        raise ArtifactError("GitHub returned an empty commit message")
    return value


def _commit_tree(commit: Any) -> str:
    value = str(
        getattr(getattr(getattr(commit, "commit", None), "tree", None), "sha", "")
        or ""
    ).lower()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ArtifactError("GitHub returned a malformed commit tree")
    return value


def _commit_parents(commit: Any) -> tuple[str, ...]:
    return tuple(_commit_sha(parent) for parent in (getattr(commit, "parents", None) or []))


def mark_backport_items_done(
    gql: GitHubGraphQLClient,
    *,
    project_owner: str,
    project_number: int,
    source_repo: str,
    source_pr_numbers: list[int],
    project_owner_type: str = "organization",
    status_field: str = _DEFAULT_STATUS_FIELD,
    from_status: str = _DEFAULT_FROM_STATUS,
    done_status: str = _DEFAULT_DONE_STATUS,
    verified_pr_numbers: set[int],
    project: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> BackportStatusUpdateResult:
    """Flip board items for ``source_pr_numbers`` from ``from_status`` to Done.

    Only PRs in ``verified_pr_numbers`` are eligible to be marked Done; the rest
    are recorded as ``unverified`` and left as-is.

    ``project`` may be a board already loaded by the caller (e.g. the poller),
    to avoid re-fetching it.

    When ``dry_run`` is true, no mutation is sent; ``updated`` lists the items
    that *would* be marked Done.
    """
    requested = sorted(set(source_pr_numbers))
    result = BackportStatusUpdateResult(requested=requested)
    if not requested:
        return result

    if project is None:
        project = _load_project(
            gql,
            project_owner=project_owner,
            project_number=project_number,
            project_owner_type=project_owner_type,
        )
    status_field_id, done_option_id = _find_status_field_and_option(
        project["fields"],
        status_field=status_field,
        done_status=done_status,
    )

    found: set[int] = set()
    requested_set = set(requested)
    for item in project["items"]:
        content = item.get("content") or {}
        if content.get("__typename") != "PullRequest":
            continue
        repo = (content.get("repository") or {}).get("nameWithOwner")
        number = content.get("number")
        if repo != source_repo or number not in requested_set:
            continue

        found.add(number)
        current_status = _item_single_select_value(item, status_field)
        if _normalize(current_status) == _normalize(done_status):
            result.already_done.append(number)
            continue
        if _normalize(current_status) != _normalize(from_status):
            result.skipped[number] = (
                f"{status_field} is {current_status!r}, not {from_status!r}"
            )
            continue
        if number not in verified_pr_numbers:
            result.unverified.append(number)
            continue

        if not dry_run:
            _set_project_item_status(
                gql,
                project_id=project["id"],
                item_id=item["id"],
                field_id=status_field_id,
                option_id=done_option_id,
            )
        result.updated.append(number)

    result.missing = sorted(requested_set - found)
    result.updated = sorted(set(result.updated))
    result.already_done = sorted(set(result.already_done))
    result.unverified = sorted(set(result.unverified))
    return result


def reconcile_project_board(
    gql: GitHubGraphQLClient,
    *,
    project_owner: str,
    project_number: int,
    source_repo: str,
    target_branch: str,
    project_owner_type: str = "organization",
    status_field: str = _DEFAULT_STATUS_FIELD,
    from_status: str = _DEFAULT_FROM_STATUS,
    done_status: str = _DEFAULT_DONE_STATUS,
    token: str = "",
    git_env: dict[str, str] | None = None,
    push_repo: str | None = None,
    dry_run: bool = False,
) -> BackportStatusUpdateResult:
    """Self-healing reconcile: mark Done every "To be backported" item that is
    genuinely on ``target_branch``.

    Unlike :func:`mark_backport_items_done`, this does not need a merged-PR body
    or a merge hook. It scans the board, clones the branch once, verifies each
    candidate by ``(#N)`` presence, and flips only the verified items. Items not
    yet on the branch are recorded as ``unverified`` and left untouched so a
    later run can pick them up.
    """
    project = _load_project(
        gql,
        project_owner=project_owner,
        project_number=project_number,
        project_owner_type=project_owner_type,
    )

    candidate_pr_numbers: set[int] = set()
    for item in project["items"]:
        content = item.get("content") or {}
        if content.get("__typename") != "PullRequest":
            continue
        if (content.get("repository") or {}).get("nameWithOwner") != source_repo:
            continue
        if _normalize(_item_single_select_value(item, status_field)) != _normalize(from_status):
            continue
        number = content.get("number")
        if not isinstance(number, int):
            continue
        candidate_pr_numbers.add(number)

    if not candidate_pr_numbers:
        return BackportStatusUpdateResult(requested=[])

    verified = verify_prs_on_branch(
        source_repo,
        target_branch,
        candidate_pr_numbers,
        token=token,
        git_env=git_env,
        push_repo=push_repo,
    )
    logger.info(
        "Branch %s: %d candidate(s) in %r, %d verified present",
        target_branch, len(candidate_pr_numbers), from_status, len(verified),
    )

    return mark_backport_items_done(
        gql,
        project_owner=project_owner,
        project_number=project_number,
        source_repo=source_repo,
        source_pr_numbers=sorted(candidate_pr_numbers),
        project_owner_type=project_owner_type,
        status_field=status_field,
        from_status=from_status,
        done_status=done_status,
        verified_pr_numbers=verified,
        project=project,
        dry_run=dry_run,
    )


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _load_project(
    gql: GitHubGraphQLClient,
    *,
    project_owner: str,
    project_number: int,
    project_owner_type: str,
) -> dict[str, Any]:
    owner_field = "user" if project_owner_type == "user" else "organization"
    query = _project_query(owner_field)
    cursor = None
    project_id = ""
    fields: list[dict[str, Any]] = []
    fields_page_info: dict[str, Any] = {}
    items: list[dict[str, Any]] = []

    while True:
        data = gql.execute(
            query,
            {"owner": project_owner, "number": project_number, "cursor": cursor},
        )
        project = (data.get(owner_field) or {}).get("projectV2")
        if not project:
            raise RuntimeError(f"Project {project_owner}/{project_number} not found")

        project_id = project_id or str(project.get("id") or "")
        if not fields:
            fields_connection = project.get("fields") or {}
            fields = list(fields_connection.get("nodes") or [])
            fields_page_info = fields_connection.get("pageInfo") or {}

        page = project.get("items") or {}
        items.extend(page.get("nodes") or [])
        page_info = page.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = _required_next_cursor(page_info, "project items")

    if not project_id:
        raise RuntimeError(f"Project {project_owner}/{project_number} has no id")
    fields.extend(
        _load_remaining_fields(
            gql,
            project_id=project_id,
            page_info=fields_page_info,
        )
    )
    for item in items:
        _load_remaining_item_field_values(gql, item)
    return {"id": project_id, "fields": fields, "items": items}


def _load_remaining_fields(
    gql: GitHubGraphQLClient,
    *,
    project_id: str,
    page_info: dict[str, Any],
) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    while page_info.get("hasNextPage"):
        cursor = _required_next_cursor(page_info, "project fields")
        if cursor in seen:
            raise RuntimeError("project fields pagination cursor repeated")
        seen.add(cursor)
        data = gql.execute(
            _project_fields_query(),
            {"id": project_id, "cursor": cursor},
        )
        connection = ((data.get("node") or {}).get("fields") or {})
        nodes.extend(connection.get("nodes") or [])
        page_info = connection.get("pageInfo") or {}
    return nodes


def _load_remaining_item_field_values(
    gql: GitHubGraphQLClient,
    item: dict[str, Any],
) -> None:
    connection = item.get("fieldValues") or {}
    nodes = list(connection.get("nodes") or [])
    page_info = connection.get("pageInfo") or {}
    seen: set[str] = set()
    while page_info.get("hasNextPage"):
        cursor = _required_next_cursor(page_info, "project item field values")
        if cursor in seen:
            raise RuntimeError("project item field-values cursor repeated")
        seen.add(cursor)
        data = gql.execute(
            _item_field_values_query(),
            {"id": item["id"], "cursor": cursor},
        )
        next_connection = (
            (data.get("node") or {}).get("fieldValues") or {}
        )
        nodes.extend(next_connection.get("nodes") or [])
        page_info = next_connection.get("pageInfo") or {}
    item["fieldValues"] = {"nodes": nodes, "pageInfo": page_info}


def _required_next_cursor(page_info: dict[str, Any], label: str) -> str:
    cursor = page_info.get("endCursor")
    if not isinstance(cursor, str) or not cursor:
        raise RuntimeError(f"{label} is truncated without an end cursor")
    return cursor


def _find_status_field_and_option(
    fields: list[dict[str, Any]],
    *,
    status_field: str,
    done_status: str,
) -> tuple[str, str]:
    for field_node in fields:
        if (
            field_node.get("__typename") != "ProjectV2SingleSelectField"
            or _normalize(field_node.get("name")) != _normalize(status_field)
        ):
            continue
        field_id = str(field_node.get("id") or "")
        for option in field_node.get("options") or []:
            if _normalize(option.get("name")) == _normalize(done_status):
                option_id = str(option.get("id") or "")
                if field_id and option_id:
                    return field_id, option_id
        raise RuntimeError(
            f"Project status field {status_field!r} has no {done_status!r} option"
        )
    raise RuntimeError(f"Project has no single-select status field {status_field!r}")


def _item_single_select_value(item: dict[str, Any], field_name: str) -> str:
    for field_value in (item.get("fieldValues") or {}).get("nodes") or []:
        if field_value.get("__typename") != "ProjectV2ItemFieldSingleSelectValue":
            continue
        if _normalize((field_value.get("field") or {}).get("name")) == _normalize(field_name):
            return str(field_value.get("name") or "")
    return ""


def _set_project_item_status(
    gql: GitHubGraphQLClient,
    *,
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
) -> None:
    mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId
    itemId: $itemId
    fieldId: $fieldId
    value: { singleSelectOptionId: $optionId }
  }) {
    projectV2Item { id }
  }
}
"""
    gql.execute(
        mutation,
        {
            "projectId": project_id,
            "itemId": item_id,
            "fieldId": field_id,
            "optionId": option_id,
        },
    )


def _project_query(owner_field: str) -> str:
    return f"""
query($owner: String!, $number: Int!, $cursor: String) {{
  {owner_field}(login: $owner) {{
    projectV2(number: $number) {{
      id
      fields(first: 100) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          __typename
          ... on ProjectV2SingleSelectField {{
            id
            name
            options {{ id name }}
          }}
        }}
      }}
      items(first: 100, after: $cursor) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          id
          content {{
            __typename
            ... on PullRequest {{
              number
              repository {{ nameWithOwner }}
            }}
          }}
          fieldValues(first: 100) {{
            pageInfo {{ hasNextPage endCursor }}
            nodes {{
              __typename
              ... on ProjectV2ItemFieldSingleSelectValue {{
                name
                field {{ ... on ProjectV2FieldCommon {{ name }} }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""


def _project_fields_query() -> str:
    return """
query($id: ID!, $cursor: String) {
  node(id: $id) {
    ... on ProjectV2 {
      fields(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on ProjectV2SingleSelectField {
            id
            name
            options { id name }
          }
        }
      }
    }
  }
}
"""


def _item_field_values_query() -> str:
    return """
query($id: ID!, $cursor: String) {
  node(id: $id) {
    ... on ProjectV2Item {
      fieldValues(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on ProjectV2ItemFieldSingleSelectValue {
            name
            field { ... on ProjectV2FieldCommon { name } }
          }
        }
      }
    }
  }
}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", required=True)
    parser.add_argument(
        "--target-branch",
        help="Release branch to reconcile. Omit to reconcile every branch "
        "configured for the repo.",
    )
    parser.add_argument("--target-token", required=True)
    parser.add_argument("--status-field", default=_DEFAULT_STATUS_FIELD)
    parser.add_argument("--from-status", default=_DEFAULT_FROM_STATUS)
    parser.add_argument("--done-status", default=_DEFAULT_DONE_STATUS)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be marked Done without mutating the board.",
    )
    parser.add_argument("--verbose", action="store_true")
    add_poll_loop_args(parser)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    from scripts.backport.registry import load_registry

    registry = load_registry(args.registry)
    gql = GitHubGraphQLClient(args.target_token)

    def _poll() -> dict[str, Any]:
        return _run_poll(
            registry, gql, repo=args.repo, target_branch=args.target_branch,
            status_field=args.status_field, from_status=args.from_status,
            done_status=args.done_status, token=args.target_token, dry_run=args.dry_run,
        )

    try:
        results = run_poll_loop_from_args(
            _poll,
            args,
            logger=logger,
        )
    except PollLoopError as exc:
        results = [
            *exc.results,
            {
                "repo": args.repo,
                "target_branch": args.target_branch,
                "action": "error",
                "error": str(exc.last_error),
            },
        ]
        print(json.dumps(format_poll_results(results), indent=2))
        raise SystemExit(1) from exc
    print(json.dumps(format_poll_results(results), indent=2))


def _run_poll(
    registry: Any,
    gql: GitHubGraphQLClient,
    *,
    repo: str,
    target_branch: str | None,
    status_field: str,
    from_status: str,
    done_status: str,
    token: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    repo_entry = registry.get_repo(repo)
    if target_branch:
        branches = [registry.get_branch(repo, target_branch)[1]]
    else:
        branches = list(repo_entry.branches)

    out: dict[str, Any] = {}
    for branch_entry in branches:
        result = reconcile_project_board(
            gql,
            project_owner=repo_entry.project_owner,
            project_number=branch_entry.project_number,
            source_repo=repo_entry.repo,
            target_branch=branch_entry.branch,
            project_owner_type=repo_entry.project_owner_type,
            status_field=status_field,
            from_status=from_status,
            done_status=done_status,
            token=token,
            push_repo=repo_entry.effective_push_repo,
            dry_run=dry_run,
        )
        out[branch_entry.branch] = result.as_dict()
    return out


if __name__ == "__main__":
    main()
