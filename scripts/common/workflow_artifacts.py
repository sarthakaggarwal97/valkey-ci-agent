"""Bounded, typed GitHub Actions artifact and log retrieval."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import stat
import tempfile
import time
import zipfile
from collections.abc import Callable, Collection
from dataclasses import dataclass
from enum import Enum
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.github_client import (
    RETRYABLE_HTTP_STATUS,
    retry_github_call,
    transient_backoff_delay,
)
from scripts.common.github_rest import GitHubRestClient

if TYPE_CHECKING:
    from github import Github

logger = logging.getLogger(__name__)

MAX_COMPRESSED_BYTES = 128 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_MEMBER_BYTES = 32 * 1024 * 1024
MAX_MEMBERS = 1_000
MAX_PATH_BYTES = 4_096
MAX_COMPRESSION_RATIO = 250
_CHUNK_BYTES = 1024 * 1024
_MAX_ARTIFACT_PAGES = 100


class ArtifactState(str, Enum):
    AVAILABLE = "available"
    NOT_FOUND = "not-found"
    EXPIRED = "expired"
    OVERSIZED = "oversized"
    CORRUPT = "corrupt"
    TRANSPORT_FAILED = "transport-failed"
    MEMBER_MISSING = "member-missing"


@dataclass(frozen=True)
class WorkflowArtifact:
    artifact_id: int
    name: str
    size_in_bytes: int
    expired: bool


@dataclass(frozen=True)
class DownloadedMember:
    name: str
    path: Path
    size: int
    sha256: str


@dataclass(frozen=True)
class ArtifactDownloadResult:
    state: ArtifactState
    members: tuple[DownloadedMember, ...] = ()
    detail: str = ""
    compressed_bytes: int = 0

    @property
    def available(self) -> bool:
        return self.state is ArtifactState.AVAILABLE

    @property
    def retryable(self) -> bool:
        return self.state is ArtifactState.TRANSPORT_FAILED

    def member(self, name: str) -> DownloadedMember | None:
        return next((item for item in self.members if item.name == name), None)


RequestedMembers = Optional[Union[Collection[str], Callable[[str], bool]]]


class ArtifactClient:
    """Fetch workflow artifacts into caller-owned bounded directories."""

    def __init__(self, github_client: Github, *, token: str, retries: int = 3) -> None:
        if not token:
            raise ValueError("GitHub token is required")
        self._gh = github_client
        self._token = token
        self._retries = retries

    def list_recent_runs(
        self,
        repo_full_name: str,
        workflow_file: str,
        *,
        event: str = "schedule",
        max_runs: int = 1,
    ) -> list[Any]:
        def _fetch() -> list[Any]:
            repo = self._gh.get_repo(repo_full_name)
            workflow = repo.get_workflow(workflow_file)
            return list(islice(workflow.get_runs(event=event, status="completed"), max_runs))

        return retry_github_call(
            _fetch,
            retries=self._retries,
            description=f"list runs {workflow_file}",
        )

    def list_run_artifacts(
        self,
        repo_full_name: str,
        run_id: int,
    ) -> list[WorkflowArtifact]:
        """List every artifact page and reject conflicting duplicate IDs."""
        repo = self._gh.get_repo(repo_full_name)
        rest = GitHubRestClient(repo, retries=self._retries)
        artifacts: list[WorkflowArtifact] = []
        seen_ids: set[int] = set()
        expected_total: int | None = None
        for page in range(1, _MAX_ARTIFACT_PAGES + 1):
            artifact_page = rest.list_run_artifacts_page(
                repo_full_name,
                run_id,
                page=page,
            )
            if expected_total is None:
                expected_total = artifact_page.total_count
            elif artifact_page.total_count != expected_total:
                raise RuntimeError("artifact total_count changed during pagination")
            for raw in artifact_page.artifacts:
                parsed = WorkflowArtifact(
                    artifact_id=raw.artifact_id,
                    name=raw.name,
                    size_in_bytes=raw.size_in_bytes,
                    expired=raw.expired,
                )
                if parsed.artifact_id in seen_ids:
                    raise RuntimeError(
                        f"duplicate artifact ID {parsed.artifact_id} across pages",
                    )
                seen_ids.add(parsed.artifact_id)
                artifacts.append(parsed)
            if len(artifacts) == expected_total:
                return artifacts
            if len(artifacts) > expected_total:
                raise RuntimeError("artifact pages exceed total_count")
            if len(artifact_page.artifacts) < 100:
                raise RuntimeError("artifact listing ended before total_count")
        raise RuntimeError("artifact listing exceeds the 100-page safety limit")

    def download_artifact(
        self,
        repo_full_name: str,
        artifact_id: int,
        *,
        destination: Path,
        requested: RequestedMembers = None,
    ) -> ArtifactDownloadResult:
        return self._download_and_extract(
            f"/repos/{repo_full_name}/actions/artifacts/{artifact_id}/zip",
            destination=destination,
            requested=requested,
        )

    def download_run_logs(
        self,
        repo_full_name: str,
        run_id: int,
        *,
        destination: Path,
        requested: RequestedMembers = None,
    ) -> ArtifactDownloadResult:
        return self._download_and_extract(
            f"/repos/{repo_full_name}/actions/runs/{run_id}/logs",
            destination=destination,
            requested=requested,
        )

    def _download_and_extract(
        self,
        path: str,
        *,
        destination: Path,
        requested: RequestedMembers,
    ) -> ArtifactDownloadResult:
        destination = destination.resolve()
        if destination.exists() and any(destination.iterdir()):
            raise ValueError("artifact destination must be empty")
        destination.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="workflow-artifact-") as temporary:
            archive = Path(temporary) / "download.zip"
            fetched = self._download_to(path, archive)
            if fetched.state is not ArtifactState.AVAILABLE:
                return fetched
            return _extract_zip_to(
                archive,
                destination,
                requested=requested,
                compressed_bytes=fetched.compressed_bytes,
            )

    def _download_to(self, path: str, destination: Path) -> ArtifactDownloadResult:
        url = f"https://api.github.com{path}"
        request = Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "valkey-ci-agent",
            },
        )
        # This header is not forwarded to GitHub's cross-host signed redirect.
        request.add_unredirected_header("Authorization", f"Bearer {self._token}")
        for attempt in range(self._retries + 1):
            try:
                with urlopen(request, timeout=120) as response:
                    declared = _content_length(response)
                    if declared is not None and declared > MAX_COMPRESSED_BYTES:
                        return ArtifactDownloadResult(
                            ArtifactState.OVERSIZED,
                            detail=(
                                f"compressed content length {declared} exceeds "
                                f"{MAX_COMPRESSED_BYTES}"
                            ),
                        )
                    total = 0
                    digest = hashlib.sha256()
                    with destination.open("wb") as handle:
                        while chunk := response.read(_CHUNK_BYTES):
                            total += len(chunk)
                            if total > MAX_COMPRESSED_BYTES:
                                handle.close()
                                destination.unlink(missing_ok=True)
                                return ArtifactDownloadResult(
                                    ArtifactState.OVERSIZED,
                                    detail=(
                                        f"compressed download exceeds "
                                        f"{MAX_COMPRESSED_BYTES} bytes"
                                    ),
                                    compressed_bytes=total,
                                )
                            digest.update(chunk)
                            handle.write(chunk)
                    logger.debug(
                        "Downloaded %d bytes from %s (sha256=%s)",
                        total,
                        path,
                        digest.hexdigest(),
                    )
                    return ArtifactDownloadResult(
                        ArtifactState.AVAILABLE,
                        compressed_bytes=total,
                    )
            except HTTPError as exc:
                if exc.code == 404:
                    return ArtifactDownloadResult(
                        ArtifactState.NOT_FOUND,
                        detail="GitHub returned 404; artifact or logs may have expired",
                    )
                if exc.code in RETRYABLE_HTTP_STATUS and attempt < self._retries:
                    time.sleep(transient_backoff_delay(attempt))
                    continue
                return ArtifactDownloadResult(
                    ArtifactState.TRANSPORT_FAILED,
                    detail=f"GitHub download failed with HTTP {exc.code}",
                )
            except (URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt < self._retries:
                    time.sleep(transient_backoff_delay(attempt))
                    continue
                return ArtifactDownloadResult(
                    ArtifactState.TRANSPORT_FAILED,
                    detail=f"artifact transport failed: {exc}",
                )
        raise AssertionError("unreachable: retry loop must return")


def _extract_zip_to(
    archive: Path,
    destination: Path,
    *,
    requested: RequestedMembers,
    compressed_bytes: int,
) -> ArtifactDownloadResult:
    try:
        with zipfile.ZipFile(archive) as zip_file:
            infos = [item for item in zip_file.infolist() if not item.is_dir()]
            invalid = _validate_members(infos)
            if invalid is not None:
                state, detail = invalid
                return ArtifactDownloadResult(
                    state,
                    detail=detail,
                    compressed_bytes=compressed_bytes,
                )
            selected = [
                info for info in infos
                if _is_requested(info.filename, requested)
            ]
            members: list[DownloadedMember] = []
            for info in selected:
                extracted = _extract_member(zip_file, info, destination)
                if isinstance(extracted, ArtifactDownloadResult):
                    return ArtifactDownloadResult(
                        extracted.state,
                        detail=extracted.detail,
                        compressed_bytes=compressed_bytes,
                    )
                members.append(extracted)
            return ArtifactDownloadResult(
                ArtifactState.AVAILABLE,
                members=tuple(members),
                compressed_bytes=compressed_bytes,
            )
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, RuntimeError) as exc:
        return ArtifactDownloadResult(
            ArtifactState.CORRUPT,
            detail=f"artifact ZIP is unreadable: {exc}",
            compressed_bytes=compressed_bytes,
        )


def _validate_members(
    infos: list[zipfile.ZipInfo],
) -> tuple[ArtifactState, str] | None:
    if len(infos) > MAX_MEMBERS:
        return (
            ArtifactState.OVERSIZED,
            f"archive contains more than {MAX_MEMBERS} members",
        )
    total = 0
    names: set[str] = set()
    for info in infos:
        name_error = _validate_member_name(info.filename)
        if name_error:
            return ArtifactState.CORRUPT, name_error
        canonical = PurePosixPath(info.filename).as_posix()
        if canonical in names:
            return ArtifactState.CORRUPT, f"duplicate ZIP member {canonical!r}"
        names.add(canonical)
        if info.flag_bits & 0x1:
            return ArtifactState.CORRUPT, f"encrypted ZIP member {canonical!r}"
        mode = (info.external_attr >> 16) & 0xFFFF
        if mode and stat.S_IFMT(mode) not in {0, stat.S_IFREG}:
            return ArtifactState.CORRUPT, f"non-regular ZIP member {canonical!r}"
        if info.file_size < 0 or info.compress_size < 0:
            return ArtifactState.CORRUPT, f"negative ZIP member size for {canonical!r}"
        if info.file_size > MAX_MEMBER_BYTES:
            return (
                ArtifactState.OVERSIZED,
                f"ZIP member {canonical!r} exceeds {MAX_MEMBER_BYTES} bytes",
            )
        if (
            info.compress_size > 0
            and info.file_size > info.compress_size * MAX_COMPRESSION_RATIO
        ):
            return (
                ArtifactState.OVERSIZED,
                f"ZIP member {canonical!r} exceeds compression-ratio limit",
            )
        total += info.file_size
        if total > MAX_UNCOMPRESSED_BYTES:
            return (
                ArtifactState.OVERSIZED,
                f"archive exceeds {MAX_UNCOMPRESSED_BYTES} uncompressed bytes",
            )
    return None


def _validate_member_name(name: str) -> str | None:
    if (
        not name
        or "\x00" in name
        or "\\" in name
        or len(name.encode("utf-8")) > MAX_PATH_BYTES
    ):
        return "ZIP member has an invalid path"
    pure = PurePosixPath(name)
    if pure.is_absolute() or ".." in pure.parts or "." in pure.parts:
        return f"unsafe ZIP member path {name!r}"
    # Drive-letter paths are not absolute to PurePosixPath.
    if re.match(r"^[A-Za-z]:", name):
        return f"unsafe ZIP member path {name!r}"
    return None


def _extract_member(
    zip_file: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    destination: Path,
) -> DownloadedMember | ArtifactDownloadResult:
    pure = PurePosixPath(info.filename)
    output = (destination / Path(*pure.parts)).resolve()
    try:
        output.relative_to(destination)
    except ValueError:
        return ArtifactDownloadResult(
            ArtifactState.CORRUPT,
            detail=f"ZIP member escapes destination: {info.filename!r}",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    digest = hashlib.sha256()
    total = 0
    try:
        with zip_file.open(info) as source, temporary.open("xb") as target:
            while chunk := source.read(_CHUNK_BYTES):
                total += len(chunk)
                if total > MAX_MEMBER_BYTES or total > info.file_size:
                    return ArtifactDownloadResult(
                        ArtifactState.OVERSIZED,
                        detail=f"ZIP member {info.filename!r} expanded beyond its bound",
                    )
                digest.update(chunk)
                target.write(chunk)
        if total != info.file_size:
            return ArtifactDownloadResult(
                ArtifactState.CORRUPT,
                detail=f"ZIP member {info.filename!r} size does not match metadata",
            )
        temporary.replace(output)
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        return ArtifactDownloadResult(
            ArtifactState.CORRUPT,
            detail=f"could not extract ZIP member {info.filename!r}: {exc}",
        )
    finally:
        temporary.unlink(missing_ok=True)
    return DownloadedMember(
        name=pure.as_posix(),
        path=output,
        size=total,
        sha256=digest.hexdigest(),
    )


def _is_requested(name: str, requested: RequestedMembers) -> bool:
    if requested is None:
        return True
    if callable(requested):
        return bool(requested(name))
    return name in requested


def _content_length(response: Any) -> int | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    raw = headers.get("Content-Length")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None
