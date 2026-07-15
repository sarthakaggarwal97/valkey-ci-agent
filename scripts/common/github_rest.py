"""Small boundary around PyGithub's private REST requester."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from scripts.common.github_client import retry_github_call

_REPOSITORY_RE = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,99})/"
    r"[A-Za-z0-9_.-]{1,100}$",
)
_MAX_ARTIFACT_NAME_BYTES = 1_024


@dataclass(frozen=True)
class RestWorkflowArtifact:
    artifact_id: int
    name: str
    size_in_bytes: int
    expired: bool


@dataclass(frozen=True)
class ArtifactPage:
    total_count: int
    artifacts: tuple[RestWorkflowArtifact, ...]


class GitHubRestClient:
    """Typed operations not exposed by the supported PyGithub objects."""

    def __init__(self, repository: Any, *, retries: int = 3) -> None:
        requester = getattr(repository, "_requester", None)
        if requester is None or not callable(
            getattr(requester, "requestJsonAndCheck", None),
        ):
            raise TypeError("repository does not expose a compatible REST requester")
        if (
            not isinstance(retries, int)
            or isinstance(retries, bool)
            or retries <= 0
        ):
            raise ValueError("retries must be a positive integer")
        self._requester = requester
        self._retries = retries

    def list_run_artifacts_page(
        self,
        repository: str,
        run_id: int,
        *,
        page: int,
        per_page: int = 100,
    ) -> ArtifactPage:
        """Retrieve one Actions artifact page through a validated contract."""
        _validate_repository(repository)
        _validate_positive_int(run_id, "run_id")
        _validate_positive_int(page, "page")
        _validate_positive_int(per_page, "per_page")
        if per_page > 100:
            raise ValueError("per_page cannot exceed 100")
        path = (
            f"/repos/{repository}/actions/runs/{run_id}/artifacts"
            f"?per_page={per_page}&page={page}"
        )
        response = retry_github_call(
            lambda: self._requester.requestJsonAndCheck("GET", path),
            retries=self._retries,
            description=f"list artifacts {run_id} page {page}",
        )
        if not isinstance(response, tuple) or len(response) != 2:
            raise RuntimeError("GitHub REST requester returned an invalid response")
        return _parse_artifact_page(response[1], per_page=per_page)

    def add_issue_comment_reaction(
        self,
        repository: str,
        comment_id: int,
        content: str,
    ) -> None:
        _validate_repository(repository)
        _validate_positive_int(comment_id, "comment_id")
        if content not in {"+1", "-1", "eyes"}:
            raise ValueError("unsupported issue-comment reaction")
        path = f"/repos/{repository}/issues/comments/{comment_id}/reactions"
        retry_github_call(
            lambda: self._requester.requestJsonAndCheck(
                "POST",
                path,
                input={"content": content},
            ),
            retries=self._retries,
            description=f"add {content} reaction to comment {comment_id}",
        )


def _parse_artifact_page(payload: Any, *, per_page: int) -> ArtifactPage:
    if not isinstance(payload, dict):
        raise RuntimeError("GitHub artifact listing is not an object")
    total_count = payload.get("total_count")
    if (
        not isinstance(total_count, int)
        or isinstance(total_count, bool)
        or total_count < 0
    ):
        raise RuntimeError("GitHub artifact listing has an invalid total_count")
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise RuntimeError("GitHub artifact listing lacks an artifacts list")
    if len(raw_artifacts) > per_page:
        raise RuntimeError("GitHub artifact listing exceeds the requested page size")
    if len(raw_artifacts) > total_count:
        raise RuntimeError("GitHub artifact listing exceeds total_count")
    artifacts = tuple(
        _parse_artifact(raw, index=index)
        for index, raw in enumerate(raw_artifacts)
    )
    if len({artifact.artifact_id for artifact in artifacts}) != len(artifacts):
        raise RuntimeError("GitHub artifact listing contains duplicate artifact IDs")
    return ArtifactPage(total_count=total_count, artifacts=artifacts)


def _parse_artifact(raw: Any, *, index: int) -> RestWorkflowArtifact:
    prefix = f"GitHub artifact at index {index}"
    if not isinstance(raw, dict):
        raise RuntimeError(f"{prefix} is not an object")
    artifact_id = raw.get("id")
    if (
        not isinstance(artifact_id, int)
        or isinstance(artifact_id, bool)
        or artifact_id <= 0
    ):
        raise RuntimeError(f"{prefix} has an invalid id")
    name = raw.get("name")
    if (
        not isinstance(name, str)
        or not name
        or "\x00" in name
        or len(name.encode("utf-8")) > _MAX_ARTIFACT_NAME_BYTES
    ):
        raise RuntimeError(f"{prefix} has an invalid name")
    size = raw.get("size_in_bytes")
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
    ):
        raise RuntimeError(f"{prefix} has an invalid size_in_bytes")
    expired = raw.get("expired")
    if not isinstance(expired, bool):
        raise RuntimeError(f"{prefix} has an invalid expired flag")
    return RestWorkflowArtifact(
        artifact_id=artifact_id,
        name=name,
        size_in_bytes=size,
        expired=expired,
    )


def _validate_repository(repository: str) -> None:
    if (
        not isinstance(repository, str)
        or not _REPOSITORY_RE.fullmatch(repository)
        or repository.endswith(("/.", "/.."))
    ):
        raise ValueError("repository must be an owner/name slug")


def _validate_positive_int(value: int, name: str) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive")
