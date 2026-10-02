"""Shared build/test command runner for backport validation."""

from __future__ import annotations

import logging
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def run_build_commands(
    repo_dir: str,
    commands: list[str],
    log_path: str | None = None,
) -> tuple[bool, str]:
    """Run validation commands sequentially.

    Returns (success, summary). The summary is a tail-trimmed combination of
    stdout/stderr suitable for log lines and PR descriptions. Empty commands
    list returns (True, "") — build validation is skipped when no commands
    are configured.

    When ``log_path`` is provided, the full (untruncated) stdout+stderr of
    every command is written to that path. The file is overwritten on each
    call. This lets agent-driven consumers (e.g. validation repair) read
    the complete log via tools like ``Read`` and ``Grep`` instead of being
    starved by a small embedded tail.
    """
    if not commands:
        logger.info("No validation commands configured for this step; nothing to run.")
        if log_path:
            Path(log_path).write_text("", encoding="utf-8")
        return True, ""
    full_log_parts: list[str] = []
    for index, command in enumerate(commands, start=1):
        logger.info("Running backport validation command %d/%d: %s", index, len(commands), command)
        started = time.monotonic()
        try:
            # Registry build commands are operator-controlled repo config, not
            # user input from PRs or issues; shell=True is intentional so repos
            # can express normal build pipelines.
            result = subprocess.run(
                command,
                cwd=repo_dir,
                shell=True,
                capture_output=True,
                text=True,
                timeout=1800,
            )
        except subprocess.TimeoutExpired as exc:
            logger.error(
                "Validation command %d/%d timed out after %.0fs: %s%s",
                index, len(commands), time.monotonic() - started, command,
                _skipped_note(len(commands) - index),
            )
            full_stdout = _decode(exc.stdout)
            full_stderr = _decode(exc.stderr)
            full_log_parts.append(_full_log_section(command, None, full_stdout, full_stderr))
            if log_path:
                Path(log_path).write_text("\n".join(full_log_parts), encoding="utf-8")
            summary = "\n".join(
                part for part in [_tail_text(full_stdout), _tail_text(full_stderr)]
                if part
            ).strip()
            return False, summary or f"`{command}` timed out after 1800 seconds"
        full_log_parts.append(
            _full_log_section(command, result.returncode, result.stdout, result.stderr)
        )
        elapsed = time.monotonic() - started
        if result.returncode != 0:
            logger.error(
                "Validation command %d/%d failed with exit code %d after %.1fs: %s%s",
                index, len(commands), result.returncode, elapsed, command,
                _skipped_note(len(commands) - index),
            )
            if log_path:
                Path(log_path).write_text("\n".join(full_log_parts), encoding="utf-8")
            summary = "\n".join(
                part for part in [result.stdout[-2000:], result.stderr[-2000:]]
                if part
            ).strip()
            return False, summary or f"`{command}` failed with exit code {result.returncode}"
        logger.info(
            "Validation command %d/%d passed in %.1fs: %s", index, len(commands), elapsed, command,
        )
    if log_path:
        Path(log_path).write_text("\n".join(full_log_parts), encoding="utf-8")
    return True, ""


def _skipped_note(remaining: int) -> str:
    return f" ({remaining} remaining command(s) not run)" if remaining else ""


def _tail_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value[-2000:]


def _decode(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _full_log_section(command: str, returncode: int | None, stdout: str, stderr: str) -> str:
    rc_label = "timeout" if returncode is None else str(returncode)
    parts = [f"$ {command}", f"# exit code: {rc_label}"]
    if stdout:
        parts.append("# stdout:")
        parts.append(stdout)
    if stderr:
        parts.append("# stderr:")
        parts.append(stderr)
    return "\n".join(parts)
