"""Fetches job logs from the GitHub Actions API."""

from __future__ import annotations

import io
import logging
import zipfile
from typing import TYPE_CHECKING
from urllib.request import HTTPRedirectHandler, Request, build_opener

if TYPE_CHECKING:
    from github import Github

logger = logging.getLogger(__name__)


class LogRetriever:
    """Retrieves raw log output for a failed GitHub Actions job."""

    def __init__(self, github_client: Github, *, token: str | None = None) -> None:
        self._gh = github_client
        self._token = token

    def get_job_log(self, repo_full_name: str, job_id: int) -> str:
        """Fetch the full log for a job via the GitHub API.

        Tries the individual job log endpoint first. If that fails (404 or
        unexpected payload), falls back to downloading the run-level log zip
        and extracting the matching job entry.

        Returns the log content as a string, or empty string on failure.
        """
        log = self._try_job_log(repo_full_name, job_id)
        if log:
            return log

        # Fallback: try run-level log zip
        return self._try_run_log_zip(repo_full_name, job_id)

    def _try_job_log(self, repo_full_name: str, job_id: int) -> str:
        """Attempt to fetch the individual job log."""
        try:
            repo = self._gh.get_repo(repo_full_name)
            url = f"/repos/{repo_full_name}/actions/jobs/{job_id}/logs"
            if self._token:
                return _download_text_via_http(url, self._token)
            _headers, data = repo._requester.requestBlobAndCheck("GET", url)
            if isinstance(data, bytes):
                return data.decode("utf-8", errors="replace")
            if isinstance(data, str):
                return data
            logger.warning(
                "Unexpected log payload type for job %d: %s, will try run-level fallback.",
                job_id, type(data).__name__,
            )
            return ""
        except Exception as exc:
            logger.warning(
                "Job-level log unavailable for job %d: %s, will try run-level fallback.",
                job_id, exc,
            )
            return ""

    def _try_run_log_zip(self, repo_full_name: str, job_id: int) -> str:
        """Download the run-level log zip and extract the matching job's log."""
        try:
            repo = self._gh.get_repo(repo_full_name)
            # Look up the run ID from the job
            job_obj = repo.get_workflow_run_attempt  # not available; use REST
            job_url = f"/repos/{repo_full_name}/actions/jobs/{job_id}"
            if self._token:
                import json
                import urllib.request
                req = urllib.request.Request(
                    f"https://api.github.com{job_url}",
                    headers={
                        "Authorization": f"Bearer {self._token}",
                        "Accept": "application/vnd.github+json",
                        "User-Agent": "valkey-ci-agent",
                    },
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    job_data = json.loads(resp.read())
            else:
                _headers, job_data = repo._requester.requestJsonAndCheck("GET", job_url)

            run_id = job_data.get("run_id")
            job_name = job_data.get("name", "")
            if not run_id:
                logger.warning("Could not determine run_id for job %d.", job_id)
                return ""

            # Download the run-level log zip
            log_url = f"/repos/{repo_full_name}/actions/runs/{run_id}/logs"
            if self._token:
                zip_bytes = _download_bytes_via_http(log_url, self._token)
            else:
                _headers, zip_data = repo._requester.requestBlobAndCheck("GET", log_url)
                zip_bytes = zip_data if isinstance(zip_data, bytes) else b""

            if not zip_bytes:
                logger.warning("Empty run-level log zip for run %d.", run_id)
                return ""

            return _extract_job_from_zip(zip_bytes, job_name, job_id)
        except Exception as exc:
            logger.error("Run-level log fallback failed for job %d: %s", job_id, exc)
            return ""


def _extract_job_from_zip(zip_bytes: bytes, job_name: str, job_id: int) -> str:
    """Extract a job's log from a run-level log zip archive.

    GitHub structures the zip as: ``<job_name>/<step_number>_<step_name>.txt``
    We concatenate all files under the matching job folder.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find entries matching the job name prefix
            prefix = f"{job_name}/"
            matching = sorted(
                name for name in zf.namelist()
                if name.startswith(prefix)
            )
            if not matching:
                # Try fuzzy match — job names in zip may differ slightly
                # (e.g. matrix params in parens)
                for name in sorted(zf.namelist()):
                    parts = name.split("/", 1)
                    if len(parts) >= 1 and job_name.lower() in parts[0].lower():
                        matching.append(name)
                matching.sort()

            if not matching:
                logger.warning(
                    "No log entries found for job '%s' (id=%d) in run zip. "
                    "Available: %s",
                    job_name, job_id,
                    [n.split("/")[0] for n in zf.namelist()[:10]],
                )
                return ""

            parts = []
            for entry in matching:
                parts.append(zf.read(entry).decode("utf-8", errors="replace"))

            log = "\n".join(parts)
            logger.info(
                "Extracted %d log entries (%d chars) for job '%s' from run zip.",
                len(matching), len(log), job_name,
            )
            return log
    except Exception as exc:
        logger.error("Failed to extract job log from zip: %s", exc)
        return ""


class _StripAuthRedirectHandler(HTTPRedirectHandler):
    """Drop the Authorization header when following redirects.

    GitHub's log download endpoints return a 302 to Azure Blob Storage.
    If the ``Authorization: Bearer`` header is forwarded, Azure rejects
    the request with HTTP 401.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is not None and new_req.host != req.host:
            new_req.remove_header("Authorization")
        return new_req


def _download_text_via_http(path: str, token: str) -> str:
    request = Request(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "valkey-ci-agent",
        },
    )
    opener = build_opener(_StripAuthRedirectHandler)
    with opener.open(request, timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def _download_bytes_via_http(path: str, token: str) -> bytes:
    """Download binary content (e.g. zip) from the GitHub API."""
    request = Request(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "valkey-ci-agent",
        },
    )
    opener = build_opener(_StripAuthRedirectHandler)
    with opener.open(request, timeout=120) as response:
        return response.read()
