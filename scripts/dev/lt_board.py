"""Throwaway ProjectV2 helper for fork live-testing of the backport sweep.

Runs inside the fork's Actions with a PAT that has Projects read/write.
Creates a dedicated user project with the two fields the sweep and
mark-done poll read, and adds/updates PR items on it.

Not for upstream. Deleted with the test branch.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from scripts.backport.sweep_graphql import GitHubGraphQLClient

STATUS_FIELD = "LT Status"
BRANCH_FIELD = "Backport Branch"
STATUS_OPTIONS = ("To be backported", "Done", "Parked")


def _viewer(gql: GitHubGraphQLClient) -> dict:
    return gql.execute("query { viewer { id login } }", {})["viewer"]


def _project(gql: GitHubGraphQLClient, number: int) -> dict:
    data = gql.execute(
        """
query($number: Int!) {
  viewer {
    projectV2(number: $number) {
      id
      number
      title
      fields(first: 50) {
        nodes {
          __typename
          ... on ProjectV2FieldCommon { id name dataType }
          ... on ProjectV2SingleSelectField { options { id name } }
        }
      }
    }
  }
}
""",
        {"number": number},
    )
    project = (data.get("viewer") or {}).get("projectV2")
    if not project:
        raise SystemExit(f"project {number} not found for viewer")
    return project


def _field(project: dict, name: str) -> dict | None:
    for node in (project.get("fields") or {}).get("nodes") or []:
        if node.get("name") == name:
            return node
    return None


def cmd_setup(gql: GitHubGraphQLClient, args: argparse.Namespace) -> None:
    viewer = _viewer(gql)
    created = gql.execute(
        """
mutation($ownerId: ID!, $title: String!) {
  createProjectV2(input: { ownerId: $ownerId, title: $title }) {
    projectV2 { id number title }
  }
}
""",
        {"ownerId": viewer["id"], "title": args.title},
    )["createProjectV2"]["projectV2"]

    gql.execute(
        """
mutation($projectId: ID!, $name: String!, $options: [ProjectV2SingleSelectFieldOptionInput!]!) {
  createProjectV2Field(input: {
    projectId: $projectId
    dataType: SINGLE_SELECT
    name: $name
    singleSelectOptions: $options
  }) { projectV2Field { ... on ProjectV2FieldCommon { id name } } }
}
""",
        {
            "projectId": created["id"],
            "name": STATUS_FIELD,
            "options": [
                {"name": option, "color": "GRAY", "description": ""}
                for option in STATUS_OPTIONS
            ],
        },
    )
    gql.execute(
        """
mutation($projectId: ID!, $name: String!) {
  createProjectV2Field(input: {
    projectId: $projectId
    dataType: TEXT
    name: $name
  }) { projectV2Field { ... on ProjectV2FieldCommon { id name } } }
}
""",
        {"projectId": created["id"], "name": BRANCH_FIELD},
    )
    print(json.dumps({"login": viewer["login"], **created}))


def _pr_item(gql: GitHubGraphQLClient, project: dict, repo: str, pr: int) -> str | None:
    """Item id for PR #pr already on the board, or None."""
    cursor = None
    while True:
        data = gql.execute(
            """
query($number: Int!, $cursor: String) {
  viewer {
    projectV2(number: $number) {
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          content {
            ... on PullRequest { number repository { nameWithOwner } }
          }
        }
      }
    }
  }
}
""",
            {"number": project["number"], "cursor": cursor},
        )
        page = data["viewer"]["projectV2"]["items"]
        for node in page["nodes"]:
            content = node.get("content") or {}
            if (
                content.get("number") == pr
                and (content.get("repository") or {}).get("nameWithOwner") == repo
            ):
                return node["id"]
        if not page["pageInfo"]["hasNextPage"]:
            return None
        cursor = page["pageInfo"]["endCursor"]


def _set_status(gql: GitHubGraphQLClient, project: dict, item_id: str, value: str) -> None:
    field = _field(project, STATUS_FIELD)
    if field is None:
        raise SystemExit(f"field {STATUS_FIELD!r} missing")
    option_id = next(
        (o["id"] for o in field.get("options") or [] if o["name"] == value),
        None,
    )
    if option_id is None:
        raise SystemExit(f"option {value!r} missing on {STATUS_FIELD!r}")
    gql.execute(
        """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId, itemId: $itemId, fieldId: $fieldId,
    value: { singleSelectOptionId: $optionId }
  }) { projectV2Item { id } }
}
""",
        {
            "projectId": project["id"],
            "itemId": item_id,
            "fieldId": field["id"],
            "optionId": option_id,
        },
    )


def cmd_add(gql: GitHubGraphQLClient, args: argparse.Namespace) -> None:
    project = _project(gql, args.project_number)
    owner, name = args.repo.split("/", 1)
    pr_id = gql.execute(
        """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) { pullRequest(number: $number) { id } }
}
""",
        {"owner": owner, "name": name, "number": args.pr},
    )["repository"]["pullRequest"]["id"]

    item_id = _pr_item(gql, project, args.repo, args.pr)
    if item_id is None:
        item_id = gql.execute(
            """
mutation($projectId: ID!, $contentId: ID!) {
  addProjectV2ItemById(input: { projectId: $projectId, contentId: $contentId }) {
    item { id }
  }
}
""",
            {"projectId": project["id"], "contentId": pr_id},
        )["addProjectV2ItemById"]["item"]["id"]

    _set_status(gql, project, item_id, args.status)

    branch_field = _field(project, BRANCH_FIELD)
    if branch_field is None:
        raise SystemExit(f"field {BRANCH_FIELD!r} missing")
    gql.execute(
        """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $text: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId, itemId: $itemId, fieldId: $fieldId,
    value: { text: $text }
  }) { projectV2Item { id } }
}
""",
        {
            "projectId": project["id"],
            "itemId": item_id,
            "fieldId": branch_field["id"],
            "text": args.branch,
        },
    )
    print(json.dumps({"item": item_id, "pr": args.pr, "status": args.status, "branch": args.branch}))


def cmd_status(gql: GitHubGraphQLClient, args: argparse.Namespace) -> None:
    project = _project(gql, args.project_number)
    item_id = _pr_item(gql, project, args.repo, args.pr)
    if item_id is None:
        raise SystemExit(f"PR #{args.pr} not on project {args.project_number}")
    _set_status(gql, project, item_id, args.status)
    print(json.dumps({"item": item_id, "pr": args.pr, "status": args.status}))


def cmd_list(gql: GitHubGraphQLClient, args: argparse.Namespace) -> None:
    project = _project(gql, args.project_number)
    cursor = None
    items = []
    while True:
        data = gql.execute(
            """
query($number: Int!, $cursor: String) {
  viewer {
    projectV2(number: $number) {
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          content { ... on PullRequest { number title merged } }
          fieldValues(first: 20) {
            nodes {
              __typename
              ... on ProjectV2ItemFieldSingleSelectValue {
                name field { ... on ProjectV2FieldCommon { name } }
              }
              ... on ProjectV2ItemFieldTextValue {
                text field { ... on ProjectV2FieldCommon { name } }
              }
            }
          }
        }
      }
    }
  }
}
""",
            {"number": project["number"], "cursor": cursor},
        )
        page = data["viewer"]["projectV2"]["items"]
        for node in page["nodes"]:
            content = node.get("content") or {}
            fields = {}
            for fv in (node.get("fieldValues") or {}).get("nodes") or []:
                fname = (fv.get("field") or {}).get("name")
                if fname:
                    fields[fname] = fv.get("name") or fv.get("text")
            items.append({"pr": content.get("number"), "title": content.get("title"),
                          "merged": content.get("merged"), "fields": fields})
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
    print(json.dumps({"title": project["title"], "number": project["number"], "items": items}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="op", required=True)

    p = sub.add_parser("setup")
    p.add_argument("--title", required=True)

    p = sub.add_parser("add")
    p.add_argument("--project-number", type=int, required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", type=int, required=True)
    p.add_argument("--branch", required=True)
    p.add_argument("--status", default="To be backported")

    p = sub.add_parser("status")
    p.add_argument("--project-number", type=int, required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", type=int, required=True)
    p.add_argument("--status", required=True)

    p = sub.add_parser("list")
    p.add_argument("--project-number", type=int, required=True)

    args = parser.parse_args()
    token = os.environ.get("LT_TOKEN", "")
    if not token:
        print("LT_TOKEN is empty — secret missing on this repo", file=sys.stderr)
        raise SystemExit(2)
    gql = GitHubGraphQLClient(token)
    {"setup": cmd_setup, "add": cmd_add, "status": cmd_status, "list": cmd_list}[args.op](gql, args)


if __name__ == "__main__":
    main()
