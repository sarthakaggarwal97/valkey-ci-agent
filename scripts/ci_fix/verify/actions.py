"""Shared transport for correlated GitHub Actions verification runs."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from scripts.ci_fix.verify.base import VerificationResult
from scripts.common.github_client import (
    is_retryable_github_error,
    retry_github_call,
)
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

_CREATED_AT_TOLERANCE_SECONDS = 120
_MAX_LOG_TAIL_CHARS = 4000
_POLL_INTERVAL_SECONDS = 20
_MAX_RECENT_RUNS = 50


class WorkflowDispatchTransport:
    """Dispatch one workflow and resolve its correlated run.

    Verification backends own their input contract and verdict wording. This
    component owns the GitHub mechanics they share: dispatch, correlation,
    polling, reloads, and bounded log feedback.
    """

    def __init__(
        self,
        github_client: Any,
        *,
        repo_full_name: str,
        workflow: str,
        ref: str,
        timeout: int,
        artifact_client: ArtifactClient | None = None,
        log_markers: tuple[str, ...] = (),
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._gh = github_client
        self._repo = repo_full_name
        self._workflow = workflow
        self._ref = ref
        self._timeout = max(1, timeout)
        self._artifact_client = artifact_client
        self._log_markers = log_markers
        self._clock = clock
        self._sleep = sleep

    def effective_timeout(self, requested: int) -> int:
        """Apply the orchestration cap without allowing a zero-length poll."""
        return max(1, min(self._timeout, requested)) if requested > 0 else self._timeout

    def dispatch(self, inputs: dict[str, str]) -> bool:
        """Dispatch once; reconcile an ambiguous response by correlation.

        Retrying a state-changing POST can create two runs when GitHub accepted
        the first request but returned a transient error. A retryable error is
        therefore treated as uncertain acceptance: the caller proceeds to
        ``wait_for_run`` and trusts only a fresh run carrying its UUID marker.
        """
        try:
            workflow = self._gh.get_repo(self._repo).get_workflow(self._workflow)
            return bool(workflow.create_dispatch(self._ref, inputs))
        except Exception as exc:  # noqa: BLE001 - unavailable verification is not fatal
            status = getattr(exc, "status", 0)
            if (
                is_retryable_github_error(exc)
                and isinstance(status, int)
                and status >= 500
            ):
                logger.warning(
                    "dispatch response for %s in %s was ambiguous; reconciling "
                    "the correlation marker: %s",
                    self._workflow,
                    self._repo,
                    exc,
                )
                return True
            logger.warning(
                "dispatching %s in %s failed: %s",
                self._workflow,
                self._repo,
                exc,
            )
            return False

    def wait_for_run(
        self,
        correlation: str,
        *,
        since: float,
        timeout: int,
    ) -> Any | None:
        """Find the correlated workflow_dispatch run and wait for completion."""
        deadline = self._clock() + timeout
        run = None
        while self._clock() < deadline:
            if run is None:
                run = self._find_run(correlation, since=since)
            else:
                run = self._reload(run)
            if run is not None and _run_status(run) == "completed":
                return run
            remaining = deadline - self._clock()
            if remaining > 0:
                self._sleep(min(_POLL_INTERVAL_SECONDS, remaining))

        if run is not None:
            run = self._reload(run)
            if _run_status(run) == "completed":
                return run
        return None

    def step_conclusion(self, run: Any, step_name: str) -> str:
        """Return one exact completed step's conclusion, or fail closed.

        A workflow-level ``failure`` also covers checkout, patch application,
        setup, and runner failures. Agent-owned verifiers therefore use the
        targeted command step as the factual test verdict instead.
        """
        run_id = getattr(run, "id", None)
        if not isinstance(run_id, int):
            return ""

        def _list_jobs() -> list[Any]:
            refreshed = self._gh.get_repo(self._repo).get_workflow_run(run_id)
            return list(refreshed.jobs())

        try:
            jobs = retry_github_call(
                _list_jobs,
                retries=2,
                description=f"list {self._workflow} run jobs",
            )
        except Exception as exc:  # noqa: BLE001 - missing evidence is not a verdict
            logger.warning(
                "listing jobs for %s run %s failed: %s",
                self._workflow,
                run_id,
                exc,
            )
            return ""

        matches = [
            _field(step, "conclusion")
            for job in jobs
            for step in (getattr(job, "steps", None) or ())
            if _field(step, "name") == step_name
        ]
        if len(matches) != 1:
            logger.warning(
                "expected one %r step in %s run %s, found %d",
                step_name,
                self._workflow,
                run_id,
                len(matches),
            )
            return ""
        return matches[0]

    def log_tail(self, run: Any) -> str:
        """Return best-effort bounded feedback from the relevant workflow step."""
        if self._artifact_client is None:
            return ""
        run_id = getattr(run, "id", None)
        if not isinstance(run_id, int):
            return ""
        try:
            logs = self._artifact_client.download_run_logs(self._repo, run_id)
        except Exception as exc:  # noqa: BLE001 - logs aid retries but do not define verdict
            logger.warning(
                "downloading logs for %s run %s failed: %s",
                self._workflow,
                run_id,
                exc,
            )
            return ""
        return _tail_log_map(logs, self._log_markers)

    def _find_run(self, correlation: str, *, since: float) -> Any | None:
        marker = f"[token:{correlation}]"

        def _list() -> list[Any]:
            workflow = self._gh.get_repo(self._repo).get_workflow(self._workflow)
            return list(
                workflow.get_runs(
                    branch=self._ref,
                    event="workflow_dispatch",
                )[:_MAX_RECENT_RUNS]
            )

        try:
            runs = retry_github_call(
                _list,
                retries=2,
                description=f"list {self._workflow} runs in {self._repo}",
            )
        except Exception as exc:  # noqa: BLE001 - keep polling after transient failures
            logger.warning(
                "listing %s runs in %s failed: %s",
                self._workflow,
                self._repo,
                exc,
            )
            return None

        for run in runs:
            title = (
                f"{getattr(run, 'display_title', '') or ''} "
                f"{getattr(run, 'name', '') or ''}"
            )
            if marker not in title or not _run_created_after(run, since):
                continue
            branch = str(getattr(run, "head_branch", "") or "")
            if branch and branch != self._ref:
                continue
            event = str(getattr(run, "event", "") or "")
            if event and event != "workflow_dispatch":
                continue
            return run
        return None

    def _reload(self, run: Any) -> Any:
        def _get() -> Any:
            return self._gh.get_repo(self._repo).get_workflow_run(run.id)

        try:
            return retry_github_call(
                _get,
                retries=2,
                description=f"reload {self._workflow} run",
            )
        except Exception as exc:  # noqa: BLE001 - retain last known state while polling
            logger.warning("reloading %s run failed: %s", self._workflow, exc)
            return run


def _run_status(run: Any) -> str:
    return str(getattr(run, "status", "") or "")


def completed_workflow_result(
    transport: WorkflowDispatchTransport,
    run: Any,
    *,
    success_detail: str,
    failure_detail: str,
    unavailable_detail: str,
    verdict_step: str = "",
) -> VerificationResult:
    """Map one completed workflow to a factual verification result.

    Target-owned workflows define the whole run as their verdict. Agent-owned
    workflows pass ``verdict_step`` so setup failures cannot masquerade as a
    targeted test failure.
    """
    url = str(getattr(run, "html_url", "") or "")
    run_conclusion = str(getattr(run, "conclusion", "") or "")
    conclusion = (
        transport.step_conclusion(run, verdict_step)
        if verdict_step
        else run_conclusion
    )

    if conclusion == "success" and run_conclusion == "success":
        return VerificationResult(
            verified=True,
            ran=True,
            detail=success_detail,
            run_url=url,
        )

    output_tail = transport.log_tail(run)
    if conclusion == "failure":
        return VerificationResult(
            verified=False,
            ran=True,
            detail=failure_detail,
            run_url=url,
            output_tail=output_tail,
        )

    observed = conclusion or run_conclusion or "unknown"
    return VerificationResult(
        verified=False,
        ran=False,
        detail=f"{unavailable_detail} ({observed})",
        run_url=url,
        output_tail=output_tail,
    )


def _field(value: Any, name: str) -> str:
    raw = value.get(name) if isinstance(value, dict) else getattr(value, name, "")
    return str(raw or "")


def _run_created_after(run: Any, since: float) -> bool:
    created = getattr(run, "created_at", None)
    if created is None:
        return False
    try:
        return created.timestamp() >= since - _CREATED_AT_TOLERANCE_SECONDS
    except (AttributeError, TypeError, ValueError):
        return False


def _tail_log_map(logs: dict[str, bytes], markers: tuple[str, ...]) -> str:
    if not logs:
        return ""
    preferred = next(
        (
            name
            for name in sorted(logs)
            if any(marker in name for marker in markers)
        ),
        None,
    )
    if preferred is not None:
        return _tail_text(logs[preferred].decode("utf-8", errors="replace"))

    combined = "\n".join(
        f"===== {name} =====\n{logs[name].decode('utf-8', errors='replace')}"
        for name in sorted(logs)
    )
    return _tail_text(combined)


def _tail_text(text: str) -> str:
    if len(text) <= _MAX_LOG_TAIL_CHARS:
        return text
    return "[truncated]\n" + text[-_MAX_LOG_TAIL_CHARS:]
