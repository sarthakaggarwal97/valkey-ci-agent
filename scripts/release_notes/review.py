"""Shared contracts for release-note review polling and handling."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN
from scripts.release_notes import projects
from scripts.release_notes import release_format as rn

REPOSITORY_NAMES = ("valkey", "valkey-search", "valkey-json", "valkey-bloom")
REPOSITORIES = tuple(f"valkey-io/{name}" for name in REPOSITORY_NAMES)

_BRANCH_RE = re.compile(
    r"^agent/release-cut/"
    r"(?P<version>\d+\.\d+\.\d+)-(?P<stage>ga|rc[1-9]\d*)$"
)
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MARKER_RE = re.compile(
    r"<!-- valkey-ci-agent:release-review:"
    r"(?P<status>addressing|addressed|refused|failed):"
    r"(?P<head>[0-9a-f]{40}):(?P<batch>[0-9a-f]{16}):"
    r"(?P<count>[1-9]\d*) -->"
)
_CONTRIBUTORS_RE = re.compile(r"^###\s+Contributors\s*$", re.MULTILINE)

_THREAD_LIMIT = 100
_COMMENT_LIMIT = 100
_BATCH_LIMIT = 50
_COMMENT_CHARS = 12_000
_CONTEXT_CHARS = 8_000
_DIFF_CHARS = 6_000
_PAYLOAD_CHARS = 100_000
_NOTES_CHARS = 2_000_000


class ReviewRefused(ValueError):
    """The request no longer satisfies the release-review contract."""


@dataclass(frozen=True)
class ReviewRequest:
    repo_name: str
    repo_full_name: str
    pr_number: int
    head_sha: str
    status_comment_id: int


@dataclass(frozen=True)
class ReleasePR:
    repo_full_name: str
    number: int
    head_sha: str
    head_branch: str
    version: str
    stage: str
    notes_path: str
    version_path: str
    profile: projects.ProjectProfile


@dataclass(frozen=True)
class ReviewComment:
    database_id: int
    body: str
    created_at: str
    diff_hunk: str
    author_login: str
    author_type: str

    @property
    def is_human(self) -> bool:
        return (
            self.author_type == "User"
            and bool(self.author_login)
            and not self.author_login.endswith("[bot]")
        )


@dataclass(frozen=True)
class ReviewThread:
    node_id: str
    resolved: bool
    outdated: bool
    path: str
    line: int | None
    comments: tuple[ReviewComment, ...]

    @property
    def root_comment_id(self) -> int:
        return self.comments[0].database_id

    def latest_human_comment(self) -> ReviewComment | None:
        humans = [comment for comment in self.comments if comment.is_human]
        return max(
            humans,
            key=lambda comment: (comment.created_at, comment.database_id),
            default=None,
        )


@dataclass(frozen=True)
class SelectedReview:
    thread: ReviewThread
    comment: ReviewComment


@dataclass(frozen=True)
class BatchMarker:
    status: str
    head_sha: str
    batch_id: str
    count: int


def parse_request(
    repo_name: str,
    pr_number: str,
    head_sha: str,
    status_comment_id: str,
) -> ReviewRequest:
    repo_name = repo_name.strip()
    if repo_name not in REPOSITORY_NAMES:
        raise ReviewRefused(f"unsupported repository: {repo_name!r}")
    if not re.fullmatch(r"[1-9]\d*", pr_number):
        raise ReviewRefused(f"invalid pull request number: {pr_number!r}")
    if not _SHA_RE.fullmatch(head_sha):
        raise ReviewRefused("head SHA must contain 40 lowercase hex characters")
    if not re.fullmatch(r"[1-9]\d*", status_comment_id):
        raise ReviewRefused(f"invalid status comment id: {status_comment_id!r}")
    return ReviewRequest(
        repo_name=repo_name,
        repo_full_name=f"valkey-io/{repo_name}",
        pr_number=int(pr_number),
        head_sha=head_sha,
        status_comment_id=int(status_comment_id),
    )


def validate_release_pr(pr: Any, repo_full_name: str) -> ReleasePR:
    """Validate the small set of facts that identify an automated release PR."""

    if repo_full_name not in REPOSITORIES:
        raise ReviewRefused(f"unsupported repository: {repo_full_name}")
    profile = projects.profile_for(repo_full_name)
    if str(getattr(pr, "state", "")).lower() != "open":
        raise ReviewRefused("release PR is not open")
    if getattr(getattr(pr, "user", None), "login", "") != f"{APP_LOGIN}[bot]":
        raise ReviewRefused("release PR was not opened by the release bot")

    head = getattr(pr, "head", None)
    head_repo = getattr(getattr(head, "repo", None), "full_name", "")
    head_sha = str(getattr(head, "sha", "")).lower()
    head_branch = str(getattr(head, "ref", ""))
    if head_repo != repo_full_name or not _SHA_RE.fullmatch(head_sha):
        raise ReviewRefused("release PR has an unexpected head repository or SHA")
    match = _BRANCH_RE.fullmatch(head_branch)
    if match is None:
        raise ReviewRefused("release PR does not use an agent/release-cut branch")

    version = match.group("version")
    stage = match.group("stage")
    major, minor, _patch = rn.parse_version(version)
    if getattr(getattr(pr, "base", None), "ref", "") != f"{major}.{minor}":
        raise ReviewRefused("release PR targets the wrong release line")

    files = list(
        retry_github_call(
            lambda: pr.get_files(),
            retries=3,
            description=f"list files for {repo_full_name}#{pr.number}",
        )
    )
    paths = [str(getattr(item, "filename", "")) for item in files]
    allowed = {profile.notes_file, profile.bumper.version_file}
    if (
        profile.notes_file not in paths
        or not set(paths).issubset(allowed)
        or len(paths) != len(set(paths))
        or any(getattr(item, "status", "") != "modified" for item in files)
    ):
        raise ReviewRefused("release PR changed unexpected paths")

    number = getattr(pr, "number", 0)
    if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
        raise ReviewRefused("release PR has an invalid number")
    return ReleasePR(
        repo_full_name=repo_full_name,
        number=number,
        head_sha=head_sha,
        head_branch=head_branch,
        version=version,
        stage=stage,
        notes_path=profile.notes_file,
        version_path=profile.bumper.version_file,
        profile=profile,
    )


def status_body(
    status: str,
    head_sha: str,
    batch_id: str,
    count: int,
    *,
    reason: str = "",
    commit_sha: str = "",
    repo_full_name: str = "",
) -> str:
    if status not in {"addressing", "addressed", "refused", "failed"}:
        raise ReviewRefused(f"invalid review status: {status}")
    if (
        not _SHA_RE.fullmatch(head_sha)
        or not re.fullmatch(r"[0-9a-f]{16}", batch_id)
        or count < 1
    ):
        raise ReviewRefused("invalid release-review status values")
    noun = "comment" if count == 1 else "comments"
    if status == "addressing":
        visible = f"Addressing {count} release-note review {noun}."
    elif status == "addressed":
        if not _SHA_RE.fullmatch(commit_sha):
            raise ReviewRefused("addressed status requires a commit SHA")
        url = f"https://github.com/{repo_full_name}/commit/{commit_sha}"
        visible = (
            f"Addressed {count} release-note review {noun} in "
            f"[`{commit_sha[:12]}`]({url})."
        )
    else:
        detail = " ".join((reason or "unknown error").split())[:500].rstrip(".")
        prefix = "Could not safely address" if status == "refused" else "Failed to address"
        visible = f"{prefix} {count} release-note review {noun}: {detail}."
    marker = (
        f"<!-- valkey-ci-agent:release-review:"
        f"{status}:{head_sha}:{batch_id}:{count} -->"
    )
    return f"{visible}\n\n{marker}"


def parse_status(comment: Any, bot_login: str) -> BatchMarker | None:
    if getattr(getattr(comment, "user", None), "login", "") != bot_login:
        return None
    matches = _MARKER_RE.findall(str(getattr(comment, "body", "")))
    if len(matches) != 1:
        return None
    status, head_sha, batch_id, count = matches[0]
    return BatchMarker(
        status=status,
        head_sha=head_sha,
        batch_id=batch_id,
        count=int(count),
    )


_THREADS_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        pageInfo { hasNextPage }
        nodes {
          id isResolved isOutdated path line
          comments(first: 100) {
            pageInfo { hasNextPage }
            nodes {
              databaseId body createdAt diffHunk
              author { login __typename }
            }
          }
        }
      }
    }
  }
}
"""

_RESOLVE_MUTATION = """
mutation($threadId: ID!) {
  resolveReviewThread(input: {threadId: $threadId}) {
    thread { id isResolved }
  }
}
"""


def list_review_threads(
    gql: GitHubGraphQLClient,
    repo_full_name: str,
    pr_number: int,
) -> tuple[ReviewThread, ...]:
    owner, name = repo_full_name.split("/", 1)
    data = gql.execute(
        _THREADS_QUERY,
        {"owner": owner, "name": name, "number": pr_number},
    )
    try:
        connection = data["repository"]["pullRequest"]["reviewThreads"]
        if connection["pageInfo"]["hasNextPage"]:
            raise ReviewRefused(
                f"release PR has more than {_THREAD_LIMIT} review threads"
            )
        raw_threads = connection["nodes"]
    except (KeyError, TypeError) as exc:
        raise ReviewRefused("GitHub returned malformed review-thread data") from exc

    threads: list[ReviewThread] = []
    for raw in raw_threads:
        try:
            comments_connection = raw["comments"]
            if comments_connection["pageInfo"]["hasNextPage"]:
                raise ReviewRefused(
                    f"review thread has more than {_COMMENT_LIMIT} comments"
                )
            comments = tuple(
                sorted(
                    (_parse_comment(item) for item in comments_connection["nodes"]),
                    key=lambda item: (item.created_at, item.database_id),
                )
            )
            if not comments:
                raise ReviewRefused("review thread contains no comments")
            threads.append(
                ReviewThread(
                    node_id=str(raw["id"]),
                    resolved=_required_bool(raw["isResolved"]),
                    outdated=_required_bool(raw["isOutdated"]),
                    path=str(raw["path"]),
                    line=raw["line"] if isinstance(raw["line"], int) else None,
                    comments=comments,
                )
            )
        except (KeyError, TypeError) as exc:
            raise ReviewRefused("GitHub returned malformed review-thread data") from exc
    return tuple(threads)


def _parse_comment(raw: dict[str, Any]) -> ReviewComment:
    author = raw.get("author") or {}
    database_id = raw.get("databaseId")
    if (
        isinstance(database_id, bool)
        or not isinstance(database_id, int)
        or database_id <= 0
    ):
        raise ReviewRefused("review comment has no numeric id")
    return ReviewComment(
        database_id=database_id,
        body=str(raw.get("body", "")),
        created_at=str(raw.get("createdAt", "")),
        diff_hunk=str(raw.get("diffHunk", "")),
        author_login=str(author.get("login", "")),
        author_type=str(author.get("__typename", "")),
    )


def _required_bool(value: Any) -> bool:
    if not isinstance(value, bool):
        raise ReviewRefused("review thread has malformed state")
    return value


def resolve_review_thread(gql: GitHubGraphQLClient, thread_id: str) -> None:
    data = gql.execute(_RESOLVE_MUTATION, {"threadId": thread_id})
    thread = (data.get("resolveReviewThread") or {}).get("thread")
    if not isinstance(thread, dict) or thread.get("id") != thread_id or thread.get("isResolved") is not True:
        raise RuntimeError(f"GitHub did not resolve review thread {thread_id}")


def selected_reviews(
    threads: Iterable[ReviewThread],
    notes_path: str,
    authorized: Callable[[str], bool],
) -> tuple[SelectedReview, ...]:
    selected: list[SelectedReview] = []
    for thread in sorted(threads, key=lambda item: item.node_id):
        comment = thread.latest_human_comment()
        if (
            thread.resolved
            or thread.outdated
            or thread.path != notes_path
            or comment is None
            or not comment.body.strip()
            or len(comment.body) > _COMMENT_CHARS
            or not authorized(comment.author_login)
        ):
            continue
        selected.append(SelectedReview(thread=thread, comment=comment))
    if len(selected) > _BATCH_LIMIT:
        raise ReviewRefused(f"release-review batch exceeds {_BATCH_LIMIT} comments")
    return tuple(selected)


def review_batch_id(reviews: Iterable[SelectedReview]) -> str:
    identity = "\n".join(
        f"{review.thread.node_id}:{review.comment.database_id}"
        for review in reviews
    )
    if not identity:
        raise ReviewRefused("release-review batch is empty")
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def review_payload_json(reviews: Iterable[SelectedReview]) -> str:
    payload = []
    for review in reviews:
        payload.append(
            {
                "thread_id": review.thread.node_id,
                "line": review.thread.line,
                "diff_hunk": review.comment.diff_hunk[:_DIFF_CHARS],
                "selected_comment_id": review.comment.database_id,
                "conversation": [
                    {
                        "author": comment.author_login,
                        "body": (
                            comment.body
                            if comment == review.comment
                            else comment.body[:_CONTEXT_CHARS]
                        ),
                    }
                    for comment in review.thread.comments
                ],
            }
        )
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    if len(encoded) > _PAYLOAD_CHARS:
        raise ReviewRefused("release-review batch is too large for one edit pass")
    return encoded


def validate_notes_edit(original: str, candidate: str, release: ReleasePR) -> None:
    """Require an edit confined to the current dated release section."""

    if (
        candidate == original
        or not candidate.endswith("\n")
        or len(candidate) > _NOTES_CHARS
        or "\x00" in candidate
        or "\r" in candidate
    ):
        raise ReviewRefused("release-note edit is empty or malformed")
    before = _split_current_section(original, release)
    after = _split_current_section(candidate, release)
    if before[0] != after[0] or before[2] != after[2]:
        raise ReviewRefused(
            "release-note edit changed the header, dated heading, or older content"
        )


def _split_current_section(
    text: str,
    release: ReleasePR,
) -> tuple[str, str, str]:
    if not text or len(text) > _NOTES_CHARS:
        raise ReviewRefused("release-notes file is empty or too large")
    major, minor, _patch = rn.parse_version(release.version)
    header = f"{rn.render_header(major, minor, release.profile.display_name)}\n\n"
    if not text.startswith(header):
        raise ReviewRefused("release-notes header is not canonical")

    heading_end = text.find("\n", len(header))
    underline_end = text.find("\n", heading_end + 1)
    if heading_end < 0 or underline_end < 0:
        raise ReviewRefused("release-notes current heading is incomplete")
    heading = text[len(header):heading_end]
    expected = (
        f"{rn.stage_heading(release.version, release.stage, release.profile.display_name)}"
        "  -  Released "
    )
    underline = text[heading_end + 1:underline_end]
    if not heading.startswith(expected) or underline != "-" * len(heading):
        raise ReviewRefused("release-notes current heading does not match the PR")

    body_start = underline_end + 1
    rest = text[body_start:]
    boundaries = [len(rest)]
    next_release = rn.dated_section_start(rest, release.profile.display_name)
    if next_release is not None:
        boundaries.append(next_release)
    contributors = _CONTRIBUTORS_RE.search(rest)
    if contributors is not None:
        boundaries.append(contributors.start())
    body_end = body_start + min(boundaries)
    return text[:body_start], text[body_start:body_end], text[body_end:]
