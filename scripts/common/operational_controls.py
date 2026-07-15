"""Strict operational limits and kill switches for automated work."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import ssl
import urllib.parse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from scripts.common.phase_artifact import canonical_json_bytes, sha256_bytes

_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_OPERATION_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")
_PUBLICATION_KEY_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,300}$")
_RESERVATION_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
_CONTROL_ADMISSION_PATH = "/v1/controls/admit"
_PUBLICATION_PATH = "/v1/publications"
_MAX_CONTROL_RESPONSE_BYTES = 64 * 1024
_POLICY_KEYS = {
    "enabled",
    "daily_ai_requests",
    "daily_input_tokens",
    "daily_output_tokens",
    "daily_cost_microusd",
    "max_queue_depth",
    "failure_threshold",
    "circuit_cooldown_seconds",
    "max_publications_per_day",
    "run_ai_requests",
}
_BOUNDS = {
    "daily_ai_requests": (1, 10_000),
    "daily_input_tokens": (1, 1_000_000_000),
    "daily_output_tokens": (1, 100_000_000),
    "daily_cost_microusd": (1, 10_000_000_000),
    "max_queue_depth": (1, 32),
    "failure_threshold": (1, 100),
    "circuit_cooldown_seconds": (60, 604_800),
    "max_publications_per_day": (1, 10_000),
    "run_ai_requests": (1, 100),
}
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"", "0", "false", "no", "off"}


@dataclass(frozen=True)
class OperationalPolicy:
    """Repository-level limits consumed by discovery and the model gateway."""

    enabled: bool = True
    daily_ai_requests: int = 24
    daily_input_tokens: int = 2_000_000
    daily_output_tokens: int = 500_000
    daily_cost_microusd: int = 20_000_000
    max_queue_depth: int = 4
    failure_threshold: int = 3
    circuit_cooldown_seconds: int = 21_600
    max_publications_per_day: int = 20
    run_ai_requests: int = 8


def parse_operational_policy(
    raw: Any,
    *,
    field: str = "automation",
) -> OperationalPolicy:
    """Parse a strict policy mapping, applying bounded defaults."""
    if raw is None:
        return OperationalPolicy()
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be a mapping")
    unknown = sorted(set(raw) - _POLICY_KEYS)
    if unknown:
        raise ValueError(f"{field} contains unknown key(s): {', '.join(unknown)}")
    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError(f"{field}.enabled must be a boolean")
    values: dict[str, Any] = {"enabled": enabled}
    defaults = OperationalPolicy()
    for name, (minimum, maximum) in _BOUNDS.items():
        value = raw.get(name, getattr(defaults, name))
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or not minimum <= value <= maximum
        ):
            raise ValueError(
                f"{field}.{name} must be an integer between "
                f"{minimum} and {maximum}",
            )
        values[name] = value
    if values["run_ai_requests"] > values["daily_ai_requests"]:
        raise ValueError(
            f"{field}.run_ai_requests cannot exceed daily_ai_requests",
        )
    return OperationalPolicy(**values)


def operational_policy_to_dict(policy: OperationalPolicy) -> dict[str, Any]:
    return asdict(policy)


def enforce_operational_access(
    repository: str,
    policy: OperationalPolicy,
    *,
    global_kill_switch: str | bool | None = None,
    disabled_repositories: str | None = None,
) -> None:
    """Fail closed when an operator or repository policy disables automation."""
    if not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError("repository must be an owner/name slug")
    if not policy.enabled:
        raise RuntimeError(f"automation is disabled for {repository}")
    switch = (
        os.environ.get("VALKEY_CI_AGENT_KILL_SWITCH", "")
        if global_kill_switch is None
        else global_kill_switch
    )
    if _parse_switch(switch, "global kill switch"):
        raise RuntimeError("organization-wide automation kill switch is active")
    disabled_value = (
        os.environ.get("VALKEY_CI_AGENT_DISABLED_REPOSITORIES", "")
        if disabled_repositories is None
        else disabled_repositories
    )
    disabled = _parse_disabled_repositories(disabled_value)
    if repository.casefold() in disabled:
        raise RuntimeError(f"operator disabled automation for {repository}")


def policy_environment(
    repository: str,
    operation: str,
    policy: OperationalPolicy,
) -> dict[str, str]:
    """Render the trusted environment consumed by the local gateway."""
    if not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError("repository must be an owner/name slug")
    if not _OPERATION_RE.fullmatch(operation):
        raise ValueError("operation must be a bounded identifier")
    payload = operational_policy_to_dict(policy)
    return {
        "AI_CONTROL_REPOSITORY": repository,
        "AI_CONTROL_OPERATION": operation,
        "AI_CONTROL_ENABLED": "true" if policy.enabled else "false",
        "AI_CONTROL_DAILY_REQUEST_LIMIT": str(policy.daily_ai_requests),
        "AI_CONTROL_DAILY_INPUT_TOKEN_LIMIT": str(policy.daily_input_tokens),
        "AI_CONTROL_DAILY_OUTPUT_TOKEN_LIMIT": str(policy.daily_output_tokens),
        "AI_CONTROL_DAILY_COST_MICROUSD": str(policy.daily_cost_microusd),
        "AI_CONTROL_MAX_QUEUE_DEPTH": str(policy.max_queue_depth),
        "AI_CONTROL_FAILURE_THRESHOLD": str(policy.failure_threshold),
        "AI_CONTROL_CIRCUIT_COOLDOWN_SECONDS": str(
            policy.circuit_cooldown_seconds,
        ),
        "AI_CONTROL_MAX_PUBLICATIONS_PER_DAY": str(
            policy.max_publications_per_day,
        ),
        "AI_CONTROL_RUN_REQUEST_LIMIT": str(policy.run_ai_requests),
        "AI_CONTROL_POLICY_SHA256": sha256_bytes(canonical_json_bytes(payload)),
    }


def write_github_environment(path: str | Path, values: dict[str, str]) -> None:
    target = Path(path)
    with target.open("a", encoding="utf-8") as handle:
        for name, value in sorted(values.items()):
            if not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
                raise ValueError("invalid environment variable name")
            if "\n" in value or "\r" in value:
                raise ValueError("environment values must be single-line")
            handle.write(f"{name}={value}\n")


def request_publication_admission(
    repository: str,
    operation: str,
    publication_key: str,
    policy: OperationalPolicy,
    *,
    upstream_url: str,
    token: str,
) -> str:
    """Atomically reserve one centrally budgeted publication."""
    if not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError("repository must be an owner/name slug")
    if not _OPERATION_RE.fullmatch(operation):
        raise ValueError("operation must be a bounded identifier")
    if not _PUBLICATION_KEY_RE.fullmatch(publication_key):
        raise ValueError("publication key must be a bounded identifier")
    host, port = _control_origin(upstream_url)
    if not token or "\n" in token or "\r" in token:
        raise ValueError("control token must be a non-empty single-line value")

    policy_payload = operational_policy_to_dict(policy)
    policy_sha = sha256_bytes(canonical_json_bytes(policy_payload))
    request_sha = sha256_bytes(
        canonical_json_bytes({
            "repository": repository,
            "operation": operation,
            "publication_key": publication_key,
        })
    )
    payload = canonical_json_bytes({
        "version": 1,
        "path": _PUBLICATION_PATH,
        "request_sha256": request_sha,
        "estimated_input_tokens": 0,
        "reserved_output_tokens": 0,
        "policy": {
            "repository": repository,
            "operation": operation,
            "policy_sha256": policy_sha,
            "daily_request_limit": policy.daily_ai_requests,
            "daily_input_token_limit": policy.daily_input_tokens,
            "daily_output_token_limit": policy.daily_output_tokens,
            "daily_cost_microusd": policy.daily_cost_microusd,
            "max_queue_depth": policy.max_queue_depth,
            "failure_threshold": policy.failure_threshold,
            "circuit_cooldown_seconds": policy.circuit_cooldown_seconds,
            "max_publications_per_day": policy.max_publications_per_day,
            "run_request_limit": policy.run_ai_requests,
        },
    })
    connection = http.client.HTTPSConnection(
        host,
        port,
        timeout=30,
        context=ssl.create_default_context(),
    )
    try:
        connection.request(
            "POST",
            _CONTROL_ADMISSION_PATH,
            body=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
            },
        )
        response = connection.getresponse()
        raw = response.read(_MAX_CONTROL_RESPONSE_BYTES + 1)
        if len(raw) > _MAX_CONTROL_RESPONSE_BYTES:
            raise RuntimeError("central publication admission response is oversized")
        if response.status != 200:
            raise RuntimeError(
                f"central publication admission denied with status {response.status}"
            )
        try:
            result = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                "central publication admission response is invalid"
            ) from exc
        if (
            not isinstance(result, dict)
            or set(result) != {"decision", "reservation_id"}
            or result.get("decision") != "admit"
            or not isinstance(result.get("reservation_id"), str)
            or not _RESERVATION_RE.fullmatch(result["reservation_id"])
        ):
            raise RuntimeError("central publication admission response is invalid")
        return result["reservation_id"]
    except (OSError, http.client.HTTPException) as exc:
        raise RuntimeError("central publication admission is unavailable") from exc
    finally:
        connection.close()


def _control_origin(value: str) -> tuple[str, int]:
    parsed = urllib.parse.urlsplit(value.strip().rstrip("/"))
    if parsed.scheme != "https" or not parsed.hostname or parsed.username:
        raise ValueError("control upstream URL must be an HTTPS origin")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError(
            "control upstream URL must not contain a path, query, or fragment"
        )
    return parsed.hostname, parsed.port or 443


def _parse_switch(value: str | bool, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a boolean string")
    normalized = value.strip().lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ValueError(f"{label} has an invalid boolean value")


def _parse_disabled_repositories(value: str) -> set[str]:
    if not isinstance(value, str):
        raise ValueError("disabled repository list must be a string")
    disabled: set[str] = set()
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        if not _REPOSITORY_RE.fullmatch(item):
            raise ValueError(
                "disabled repository list contains an invalid owner/name slug",
            )
        disabled.add(item.casefold())
    return disabled


def _load_policy(registry_path: str, repository: str) -> OperationalPolicy:
    if not registry_path:
        return OperationalPolicy()
    from scripts.backport.registry import load_registry

    return load_registry(registry_path).get_repo(repository).automation


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--registry", default="")
    parser.add_argument(
        "--github-env",
        default=os.environ.get("GITHUB_ENV", ""),
    )
    parser.add_argument(
        "--kill-switch",
        default=os.environ.get("VALKEY_CI_AGENT_KILL_SWITCH", ""),
    )
    parser.add_argument(
        "--disabled-repositories",
        default=os.environ.get("VALKEY_CI_AGENT_DISABLED_REPOSITORIES", ""),
    )
    parser.add_argument("--admit-publication-key", default="")
    parser.add_argument(
        "--control-upstream-url",
        default=os.environ.get("AI_GATEWAY_UPSTREAM_URL", ""),
    )
    parser.add_argument(
        "--control-token",
        default=os.environ.get("AI_GATEWAY_TOKEN", ""),
    )
    args = parser.parse_args(argv)
    policy = _load_policy(args.registry, args.repo)
    enforce_operational_access(
        args.repo,
        policy,
        global_kill_switch=args.kill_switch,
        disabled_repositories=args.disabled_repositories,
    )
    if args.admit_publication_key:
        request_publication_admission(
            args.repo,
            args.operation,
            args.admit_publication_key,
            policy,
            upstream_url=args.control_upstream_url,
            token=args.control_token,
        )
    values = policy_environment(args.repo, args.operation, policy)
    if args.github_env:
        write_github_environment(args.github_env, values)
    else:
        print(json.dumps(values, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
