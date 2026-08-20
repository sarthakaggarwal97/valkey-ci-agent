"""Shared build/test command runner for backport validation."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from scripts.common.proc import NETWORK_ENV, PROCESS_BASICS, filter_env

logger = logging.getLogger(__name__)


# Validation executes code from the target branch and freshly applied
# backports. Give it only the process basics needed to run local tools plus CA
# bundle locations for repositories whose build setup downloads dependencies.
# In particular, never pass AWS credentials or GitHub Actions control-file
# paths: a validation command that can write GITHUB_ENV or GITHUB_PATH could
# otherwise compromise the later publication step and its fresh GitHub token.
_BUILD_ENV_ALLOWLIST = PROCESS_BASICS + NETWORK_ENV


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

    Commands receive a scrubbed environment containing only process basics
    and CA-bundle locations. Credentials and GitHub Actions control-file paths
    are intentionally excluded because the checked-out code is untrusted.
    """
    if not commands:
        if log_path:
            Path(log_path).write_text("", encoding="utf-8")
        return True, ""
    full_log_parts: list[str] = []
    for command in commands:
        logger.info("Running backport validation command: %s", command)
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
                env=filter_env(_BUILD_ENV_ALLOWLIST),
            )
        except subprocess.TimeoutExpired as exc:
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
        if result.returncode != 0:
            if log_path:
                Path(log_path).write_text("\n".join(full_log_parts), encoding="utf-8")
            summary = "\n".join(
                part for part in [result.stdout[-2000:], result.stderr[-2000:]]
                if part
            ).strip()
            return False, summary or f"`{command}` failed with exit code {result.returncode}"
    if log_path:
        Path(log_path).write_text("\n".join(full_log_parts), encoding="utf-8")
    return True, ""


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
