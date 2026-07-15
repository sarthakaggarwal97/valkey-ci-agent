"""Wrapper around the Claude Code CLI."""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import threading
from pathlib import Path
from typing import Any

from scripts.common.proc import NETWORK_ENV, PROCESS_BASICS, filter_env

logger = logging.getLogger(__name__)

_DEFAULT_CLAUDE_MODEL = "opus"
_CLAUDE_MODEL_ENV = "CI_AGENT_CLAUDE_MODEL"
_DEFAULT_TIMEOUT_SECONDS = 60 * 60
_MAX_TRANSCRIPT_BYTES = 32 * 1024 * 1024
_STREAM_READ_CHARS = 64 * 1024
_MAX_LOG_BUFFER_CHARS = 256 * 1024
_DOCKER_ENV_VARS = (
    "DOCKER_HOST",
    "DOCKER_TLS_VERIFY",
    "DOCKER_CERT_PATH",
    "CI_AGENT_AI_CONTAINER_IMAGE",
    "CI_AGENT_AI_CONTAINER_IDENTITY",
    "CI_AGENT_AI_DOCKER_NETWORK",
)
DEFAULT_CLAUDE_ENV_ALLOWLIST = tuple(
    sorted(set(PROCESS_BASICS + NETWORK_ENV + _DOCKER_ENV_VARS))
)


def run_claude_code(
    prompt: str,
    *,
    cwd: str | None = None,
    timeout: int = _DEFAULT_TIMEOUT_SECONDS,
    model: str | None = _DEFAULT_CLAUDE_MODEL,
    effort: str | None = "max",
    max_turns: int = 200,
    allowed_tools: str = "Read,Edit,MultiEdit,Write,Bash,Glob,Grep",
    disallowed_tools: str | None = None,
    env_allowlist: tuple[str, ...] | None = None,
    dangerously_skip_permissions: bool = False,
) -> tuple[str, str, int]:
    """Run claude CLI and return (stdout, stderr, exit_code).

    Requires the pinned AI runtime image and internal Docker network created by
    ``setup-ai-runtime``. The Claude container receives no cloud credential,
    host home directory, Docker socket, or filesystem outside ``cwd``.
    """
    env = _build_claude_env(env_allowlist)
    # Resolve once here so the env-var override (CI_AGENT_CLAUDE_MODEL)
    # always wins, regardless of whether the caller pre-resolved.
    # runtime.run_agent intentionally calls _resolve_claude_model too so
    # it can capture the resolved value in the audit record - the two
    # calls are idempotent by design (override wins each time).
    resolved_model = _resolve_claude_model(model)
    claude_args = [
        "--print",
        "--max-turns", str(max_turns),
        "--tools", allowed_tools,
        # cwd is an untrusted checkout, so ignore any .mcp.json it carries:
        # --tools gates the model's tools but not an MCP server the project
        # config could otherwise auto-register inside the isolated tool
        # process.
        "--strict-mcp-config",
        "--output-format", "stream-json",
        "--verbose",
    ]
    if dangerously_skip_permissions:
        # Edit-only profiles need to bypass interactive approval in headless
        # mode. Read-only profiles never receive this broad permission switch.
        claude_args.append("--dangerously-skip-permissions")
    denied = (
        _default_disallowed_tools(allowed_tools)
        if disallowed_tools is None
        else disallowed_tools
    )
    if denied:
        claude_args.extend(["--disallowedTools", denied])
    if resolved_model:
        claude_args.extend(["--model", resolved_model])
    if effort:
        claude_args.extend(["--effort", effort])

    try:
        cmd = _containerized_command(
            claude_args,
            cwd=cwd,
            writes_allowed=dangerously_skip_permissions,
            env=env,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Claude isolation unavailable: %s", exc)
        return "", str(exc), 127

    logger.info("Running claude: cwd=%s, timeout=%d, prompt=%s…", cwd, timeout, prompt[:120])
    stdout_parts: list[str] = []
    transcript_bytes = 0
    transcript_exceeded = threading.Event()
    process = None
    try:
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=None,
            env=env,
            bufsize=1,
            start_new_session=True,
        )

        def _read_stdout() -> None:
            nonlocal transcript_bytes
            if process.stdout is None:
                return
            log_buffer = ""
            while chunk := process.stdout.read(_STREAM_READ_CHARS):
                encoded = chunk.encode("utf-8")
                remaining = _MAX_TRANSCRIPT_BYTES - transcript_bytes
                if len(encoded) > remaining:
                    prefix = encoded[: max(remaining, 0)].decode(
                        "utf-8",
                        errors="ignore",
                    )
                    if prefix:
                        stdout_parts.append(prefix)
                        transcript_bytes += len(prefix.encode("utf-8"))
                    transcript_exceeded.set()
                    _terminate_process(process)
                    break
                stdout_parts.append(chunk)
                transcript_bytes += len(encoded)
                log_buffer += chunk
                lines = log_buffer.splitlines(keepends=True)
                if lines and not lines[-1].endswith(("\n", "\r")):
                    log_buffer = lines.pop()
                else:
                    log_buffer = ""
                for line in lines:
                    _log_stream_event(line)
                if len(log_buffer) > _MAX_LOG_BUFFER_CHARS:
                    _log_stream_event(log_buffer)
                    log_buffer = ""
            if log_buffer:
                _log_stream_event(log_buffer)

        reader = threading.Thread(target=_read_stdout, daemon=True)
        reader.start()
        if process.stdin is not None:
            process.stdin.write(prompt)
            process.stdin.close()

        returncode = process.wait(timeout=timeout)
        reader.join(timeout=5)
        stdout = "".join(stdout_parts)
        if transcript_exceeded.is_set():
            logger.error(
                "Claude transcript exceeded %d bytes.",
                _MAX_TRANSCRIPT_BYTES,
            )
            return (
                stdout,
                f"transcript exceeded {_MAX_TRANSCRIPT_BYTES} bytes",
                1,
            )
        logger.info("Claude exited %d (%d chars stdout).", returncode, len(stdout))
        return stdout, "", returncode
    except subprocess.TimeoutExpired:
        if process is not None:
            _terminate_process(process)
        # Let the reader thread flush buffered output before we read it.
        reader.join(timeout=5)
        stdout = "".join(stdout_parts)
        logger.error("Claude timed out after %ds.", timeout)
        return stdout, f"timeout after {timeout}s", 1
    except FileNotFoundError:
        logger.error("claude CLI not found on PATH.")
        return "", "claude not found", 127


def _terminate_process(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError, AttributeError):
        try:
            process.kill()
        except ProcessLookupError:
            pass


def _build_claude_env(env_allowlist: tuple[str, ...] | None = None) -> dict[str, str]:
    """Return only the environment needed by the host-side Docker client."""
    allowed = set(env_allowlist or DEFAULT_CLAUDE_ENV_ALLOWLIST)
    return filter_env(tuple(allowed))


def _containerized_command(
    claude_args: list[str],
    *,
    cwd: str | None,
    writes_allowed: bool,
    env: dict[str, str],
) -> list[str]:
    if not cwd:
        raise ValueError("Claude requires an explicit isolated workspace")
    workspace = Path(cwd).resolve()
    if not workspace.is_dir():
        raise ValueError(f"Claude workspace does not exist: {workspace}")
    image = env.get("CI_AGENT_AI_CONTAINER_IMAGE", "").strip()
    network = env.get("CI_AGENT_AI_DOCKER_NETWORK", "").strip()
    if not image or not network:
        raise ValueError(
            "isolated AI runtime is not configured "
            "(CI_AGENT_AI_CONTAINER_IMAGE/CI_AGENT_AI_DOCKER_NETWORK)"
        )
    mount_mode = "rw" if writes_allowed else "ro"
    return [
        "docker",
        "run",
        "--rm",
        "--network", network,
        "--read-only",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "256",
        "--memory", "2g",
        "--cpus", "2",
        "--user", "1000:1000",
        "--volume", f"{workspace}:/workspace:{mount_mode}",
        "--workdir", "/workspace",
        "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=256m",
        "--tmpfs", "/home/node:rw,noexec,nosuid,nodev,size=128m",
        "--env", "HOME=/home/node",
        "--env", "ANTHROPIC_BASE_URL=http://ai-gateway:8080",
        "--env", "ANTHROPIC_AUTH_TOKEN=credential-held-by-gateway",
        "--env", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1",
        image,
        *claude_args,
    ]


def _resolve_claude_model(model: str | None) -> str | None:
    """Resolve the Claude Code model alias, honoring operator override."""
    override = os.environ.get(_CLAUDE_MODEL_ENV, "").strip()
    if override:
        return override
    return model or _DEFAULT_CLAUDE_MODEL


def _default_disallowed_tools(allowed_tools: str) -> str:
    """Deny dangerous tools unless the profile explicitly allowed them."""
    allowed = {
        token.split("(", 1)[0]
        for token in re.split(r"[\s,]+", allowed_tools.strip())
        if token
    }
    return ",".join(tool for tool in ("Bash", "Write") if tool not in allowed)


def _log_stream_event(raw_line: str) -> None:
    raw_line = raw_line.strip()
    if not raw_line:
        return
    try:
        event = json.loads(raw_line)
    except json.JSONDecodeError:
        logger.info("Claude stream: %s", _truncate(raw_line, 500))
        return

    summary = _summarize_stream_event(event)
    if summary:
        logger.info("Claude stream: %s", summary)
    else:
        logger.debug("Claude stream event: %s", _truncate(raw_line, 1000))


def _summarize_stream_event(event: dict[str, Any]) -> str:
    event_type = str(event.get("type") or event.get("event") or "")
    subtype = str(event.get("subtype") or "")

    if event_type == "system":
        session_id = event.get("session_id") or event.get("sessionId") or ""
        model = event.get("model") or ""
        cwd = event.get("cwd") or ""
        parts = ["system"]
        if subtype:
            parts.append(subtype)
        if model:
            parts.append(f"model={model}")
        if session_id:
            parts.append(f"session={session_id}")
        if cwd:
            parts.append(f"cwd={cwd}")
        return " ".join(parts)

    if event_type == "assistant":
        message = event.get("message")
        if not isinstance(message, dict):
            return "assistant event"
        content = message.get("content")
        summaries: list[str] = []
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    text = str(block.get("text") or "").strip()
                    if text:
                        summaries.append(f"text={_truncate(text, 240)}")
                elif block_type == "tool_use":
                    name = str(block.get("name") or "tool")
                    summaries.append(f"tool={name} {_summarize_tool_input(block.get('input'))}")
        return "assistant " + "; ".join(summaries) if summaries else "assistant event"

    if event_type == "user":
        message = event.get("message")
        if not isinstance(message, dict):
            return "user event"
        content = message.get("content")
        if isinstance(content, list):
            result_count = sum(
                1 for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
            if result_count:
                return f"tool_result count={result_count}"
        return "user event"

    if event_type == "result":
        duration = event.get("duration_ms")
        cost = event.get("total_cost_usd")
        turns = event.get("num_turns")
        result = str(event.get("result") or "").strip()
        parts = ["result"]
        if subtype:
            parts.append(subtype)
        if turns is not None:
            parts.append(f"turns={turns}")
        if duration is not None:
            parts.append(f"duration_ms={duration}")
        if cost is not None:
            parts.append(f"cost_usd={cost}")
        if result:
            parts.append(f"text={_truncate(result, 300)}")
        return " ".join(parts)

    return f"{event_type or 'unknown'} event"


def _summarize_tool_input(tool_input: Any) -> str:
    if not isinstance(tool_input, dict):
        return ""
    for key in ("file_path", "path", "pattern", "command"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return f"{key}={_truncate(value, 180)}"
    return _truncate(json.dumps(tool_input, sort_keys=True, default=str), 180)


def _truncate(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"
