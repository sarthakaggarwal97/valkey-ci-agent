"""Central capability profiles for tool-using AI subprocesses."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Literal

from scripts.ai.claude_code import (
    DEFAULT_CLAUDE_ENV_ALLOWLIST,
    _resolve_claude_model,
    run_claude_code,
)
from scripts.common.ai_evidence import write_ai_run_evidence
from scripts.common.proc import run_git

AgentProfileName = Literal[
    "conflict_resolve_edit_only",
    "validation_repair_edit_only",
    "fuzzer_analysis_readonly",
    "ci_fix_diagnose_readonly",
]


@dataclass(frozen=True)
class AgentProfile:
    """Execution contract for one kind of AI task."""

    name: AgentProfileName
    allowed_tools: str
    timeout: int
    effort: str | None = "high"
    max_turns: int = 200
    writes_allowed: bool = False
    output_schema: str = "text"
    failure_policy: str = "fail-closed"
    disallowed_tools: str = ""
    env_allowlist: tuple[str, ...] = DEFAULT_CLAUDE_ENV_ALLOWLIST


@dataclass(frozen=True)
class AgentRunResult:
    """Result and audit metadata for one AI subprocess call."""

    profile: AgentProfileName
    stdout: str
    stderr: str
    returncode: int
    prompt_sha256: str
    cwd: str
    allowed_tools: str
    model: str
    repository_tree_sha: str
    runtime_image: str
    runtime_image_identity: str
    started_at: str
    finished_at: str
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int
    turns: int
    cost_microusd: int


AGENT_PROFILES: dict[AgentProfileName, AgentProfile] = {
    "conflict_resolve_edit_only": AgentProfile(
        name="conflict_resolve_edit_only",
        allowed_tools="Read,Edit,MultiEdit,Grep,Glob",
        timeout=3600,
        effort="max",
        max_turns=240,
        writes_allowed=True,
        output_schema="edited-files",
        disallowed_tools="Bash,Write",
    ),
    "validation_repair_edit_only": AgentProfile(
        name="validation_repair_edit_only",
        allowed_tools="Read,Edit,MultiEdit,Grep,Glob",
        timeout=1800,
        effort="max",
        max_turns=160,
        writes_allowed=True,
        output_schema="edited-files",
        disallowed_tools="Bash,Write",
    ),
    "fuzzer_analysis_readonly": AgentProfile(
        name="fuzzer_analysis_readonly",
        allowed_tools="Read,Grep,Glob",
        timeout=1800,
        effort="max",
        max_turns=200,
        writes_allowed=False,
        output_schema="text",
    ),
    "ci_fix_diagnose_readonly": AgentProfile(
        name="ci_fix_diagnose_readonly",
        allowed_tools="Read,Grep,Glob",
        timeout=3600,
        effort="high",
        max_turns=200,
        writes_allowed=False,
        output_schema="text",
    ),
}


def get_agent_profile(name: AgentProfileName) -> AgentProfile:
    return AGENT_PROFILES[name]


def run_agent(
    profile_name: AgentProfileName,
    prompt: str,
    *,
    cwd: str | None = None,
    timeout: int | None = None,
    model: str | None = None,
    evidence_dir: str | Path | None = None,
) -> AgentRunResult:
    """Run Claude Code under a named capability profile.

    The profile controls tool permissions, timeout, effort, and audit labels.
    Optional evidence is written after the process exits so generated files in
    the working tree cannot influence the prompt that just ran.
    """
    profile = get_agent_profile(profile_name)
    started_at = datetime.now(timezone.utc).isoformat()
    resolved_model = _resolve_claude_model(model)
    repository_tree_sha = _repository_tree_sha(cwd)
    stdout, stderr, rc = run_claude_code(
        prompt,
        cwd=cwd,
        timeout=timeout if timeout is not None else profile.timeout,
        model=resolved_model,
        effort=profile.effort,
        max_turns=profile.max_turns,
        allowed_tools=profile.allowed_tools,
        disallowed_tools=profile.disallowed_tools,
        env_allowlist=profile.env_allowlist,
        dangerously_skip_permissions=profile.writes_allowed,
    )
    finished_at = datetime.now(timezone.utc).isoformat()
    usage = _usage_from_stdout(stdout)
    result = AgentRunResult(
        profile=profile_name,
        stdout=stdout,
        stderr=stderr,
        returncode=rc,
        prompt_sha256=hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        cwd=str(cwd or ""),
        allowed_tools=profile.allowed_tools,
        model=resolved_model or "",
        repository_tree_sha=repository_tree_sha,
        runtime_image=os.environ.get("CI_AGENT_AI_CONTAINER_IMAGE", ""),
        runtime_image_identity=os.environ.get(
            "CI_AGENT_AI_CONTAINER_IDENTITY",
            "",
        ),
        started_at=started_at,
        finished_at=finished_at,
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        cache_read_input_tokens=usage["cache_read_input_tokens"],
        cache_creation_input_tokens=usage["cache_creation_input_tokens"],
        turns=usage["turns"],
        cost_microusd=usage["cost_microusd"],
    )
    _write_evidence(result, profile, evidence_dir, prompt)
    return result


def _write_evidence(
    result: AgentRunResult,
    profile: AgentProfile,
    evidence_dir: str | Path | None,
    prompt: str,
) -> None:
    configured_dir = evidence_dir or os.environ.get("CI_AGENT_EVIDENCE_DIR", "")
    if not configured_dir and os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        configured_dir = "agent-evidence"
    if not configured_dir:
        return
    target_dir = Path(configured_dir)
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        result_data = {
            key: value
            for key, value in asdict(result).items()
            if key not in {"stdout", "stderr", "cwd"}
        }
        run_id = (
            f"{result.started_at.replace(':', '').replace('+', 'Z')}-"
            f"{result.profile}-{result.prompt_sha256[:12]}"
        )
        write_ai_run_evidence(
            target_dir,
            run_id=run_id,
            profile=asdict(profile),
            result=result_data,
            repository_tree_sha=result.repository_tree_sha,
            runtime={
                "container_image": result.runtime_image,
                "container_image_identity": result.runtime_image_identity,
                "network_policy": "internal-model-gateway-only",
                "filesystem": "explicit-workspace-only",
                "user": "1000:1000",
                "read_only_root": True,
                "capabilities": "none",
                "no_new_privileges": True,
                "resources": {
                    "cpus": 2,
                    "memory_mb": 2048,
                    "pids": 256,
                    "tmpfs_mb": 384,
                },
            },
            prompt=prompt,
            stdout=result.stdout,
            stderr=result.stderr,
        )
    except OSError as exc:
        raise RuntimeError(f"could not write AI evidence: {exc}") from exc


def _repository_tree_sha(cwd: str | None) -> str:
    if not cwd or not Path(cwd).is_dir():
        return ""
    try:
        result = run_git(cwd, "rev-parse", "HEAD^{tree}", check=False)
    except OSError:
        return ""
    value = result.stdout.strip().lower()
    if result.returncode == 0 and len(value) == 40:
        return value
    return ""


def _usage_from_stdout(stdout: str) -> dict[str, int]:
    usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "turns": 0,
        "cost_microusd": 0,
    }
    result_event: dict[str, object] | None = None
    for line in stdout.splitlines():
        if len(line) > 2 * 1024 * 1024:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("type") == "result":
            result_event = event
    if result_event is None:
        return usage
    raw_usage = result_event.get("usage")
    if isinstance(raw_usage, dict):
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ):
            usage[name] = _bounded_nonnegative_int(raw_usage.get(name))
    usage["turns"] = _bounded_nonnegative_int(result_event.get("num_turns"))
    usage["cost_microusd"] = _cost_microusd(
        result_event.get("total_cost_usd"),
    )
    return usage


def _bounded_nonnegative_int(value: object) -> int:
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= 1_000_000_000
    ):
        return value
    return 0


def _cost_microusd(value: object) -> int:
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        return 0
    try:
        cost = Decimal(str(value))
    except InvalidOperation:
        return 0
    if not cost.is_finite() or cost < 0 or cost > Decimal("10000"):
        return 0
    return int((cost * 1_000_000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
