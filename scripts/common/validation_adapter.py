"""Typed, container-isolated validation adapters."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import select
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.common.proc import filter_env, terminate_process_group

logger = logging.getLogger(__name__)

_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_IMAGE_RE = re.compile(
    r"^[a-z0-9]+(?:[._/-][a-z0-9]+)*"
    r"@sha256:[0-9a-f]{64}$"
)
_PLATFORMS = {"linux/amd64", "linux/arm64"}
_MAX_COMMANDS = 64
_MAX_ARGV = 128
_MAX_ARG_BYTES = 4096
_MAX_PATTERNS = 128
_MAX_IMMUTABLE_INPUTS = 8
_MAX_IMMUTABLE_INPUT_BYTES = 128 * 1024 * 1024
_MAX_TOTAL_IMMUTABLE_INPUT_BYTES = 256 * 1024 * 1024
_IMMUTABLE_INPUT_SOURCE_HOSTS = {"github.com"}
_IMMUTABLE_INPUT_REDIRECT_HOSTS = {
    "github.com",
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
}


@dataclass(frozen=True)
class ValidationResources:
    cpus: int
    memory_mb: int
    pids: int
    output_bytes: int
    tmpfs_mb: int


@dataclass(frozen=True)
class ValidationCommand:
    id: str
    argv: tuple[str, ...]
    working_directory: str
    timeout_seconds: int
    inputs: tuple[str, ...]
    expected_artifacts: tuple[str, ...]


@dataclass(frozen=True)
class ValidationImmutableInput:
    id: str
    url: str
    sha256: str
    size_bytes: int
    format: str


@dataclass(frozen=True)
class ValidationRule:
    paths: tuple[str, ...]
    command_ids: tuple[str, ...]


@dataclass(frozen=True)
class ValidationAdapter:
    kind: str
    image: str
    platform: str
    network: str
    resources: ValidationResources
    immutable_inputs: tuple[ValidationImmutableInput, ...]
    default_command_ids: tuple[str, ...]
    commands: tuple[ValidationCommand, ...]
    rules: tuple[ValidationRule, ...]

    def command(self, command_id: str) -> ValidationCommand:
        for command in self.commands:
            if command.id == command_id:
                return command
        raise KeyError(command_id)


@dataclass(frozen=True)
class ValidationRunResult:
    success: bool
    summary: str
    command_ids: tuple[str, ...]
    environment: dict[str, Any]


def parse_validation_adapter(raw: Any, *, field: str) -> ValidationAdapter:
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be a mapping")
    adapter_kind = raw.get("adapter")
    common_keys = {
        "adapter",
        "image",
        "platform",
        "network",
        "resources",
        "default_commands",
        "commands",
        "rules",
    }
    if adapter_kind == "container-argv-v1":
        adapter_keys = common_keys
    elif adapter_kind == "container-argv-v2":
        adapter_keys = common_keys | {"immutable_inputs"}
    else:
        raise ValueError(
            f"{field}.adapter must be 'container-argv-v1' or "
            "'container-argv-v2'",
        )
    data = _exact(
        raw,
        adapter_keys,
        field,
    )
    image = _text(data["image"], f"{field}.image", 512)
    if not _IMAGE_RE.fullmatch(image):
        raise ValueError(f"{field}.image must be pinned by sha256 digest")
    platform = data["platform"]
    if platform not in _PLATFORMS:
        raise ValueError(f"{field}.platform must be one of {sorted(_PLATFORMS)}")
    if data["network"] != "none":
        raise ValueError(f"{field}.network must be 'none'")
    resources = _parse_resources(data["resources"], f"{field}.resources")
    immutable_inputs = (
        _parse_immutable_inputs(
            data["immutable_inputs"],
            f"{field}.immutable_inputs",
        )
        if adapter_kind == "container-argv-v2"
        else ()
    )

    raw_commands = data["commands"]
    if not isinstance(raw_commands, list) or not 1 <= len(raw_commands) <= _MAX_COMMANDS:
        raise ValueError(f"{field}.commands must contain 1-{_MAX_COMMANDS} entries")
    commands = tuple(
        _parse_command(value, f"{field}.commands[{index}]")
        for index, value in enumerate(raw_commands)
    )
    ids = [command.id for command in commands]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{field}.commands contains duplicate IDs")

    defaults = _id_list(
        data["default_commands"],
        f"{field}.default_commands",
        require_nonempty=True,
    )
    _require_known_ids(defaults, set(ids), f"{field}.default_commands")

    raw_rules = data["rules"]
    if not isinstance(raw_rules, list) or len(raw_rules) > 64:
        raise ValueError(f"{field}.rules must be a list with at most 64 entries")
    rules = tuple(
        _parse_rule(value, f"{field}.rules[{index}]", set(ids))
        for index, value in enumerate(raw_rules)
    )
    return ValidationAdapter(
        kind=adapter_kind,
        image=image,
        platform=platform,
        network="none",
        resources=resources,
        immutable_inputs=immutable_inputs,
        default_command_ids=tuple(defaults),
        commands=commands,
        rules=rules,
    )


def validation_adapter_to_dict(adapter: ValidationAdapter) -> dict[str, Any]:
    result = {
        "adapter": adapter.kind,
        "image": adapter.image,
        "platform": adapter.platform,
        "network": adapter.network,
        "resources": {
            "cpus": adapter.resources.cpus,
            "memory_mb": adapter.resources.memory_mb,
            "pids": adapter.resources.pids,
            "output_bytes": adapter.resources.output_bytes,
            "tmpfs_mb": adapter.resources.tmpfs_mb,
        },
        "default_commands": list(adapter.default_command_ids),
        "commands": [
            {
                "id": command.id,
                "argv": list(command.argv),
                "working_directory": command.working_directory,
                "timeout_seconds": command.timeout_seconds,
                "inputs": list(command.inputs),
                "expected_artifacts": list(command.expected_artifacts),
            }
            for command in adapter.commands
        ],
        "rules": [
            {
                "paths": list(rule.paths),
                "command_ids": list(rule.command_ids),
            }
            for rule in adapter.rules
        ],
    }
    if adapter.kind == "container-argv-v2":
        result["immutable_inputs"] = [
            {
                "id": item.id,
                "url": item.url,
                "sha256": item.sha256,
                "size_bytes": item.size_bytes,
                "format": item.format,
            }
            for item in adapter.immutable_inputs
        ]
    return result


def select_validation_commands(
    adapter: ValidationAdapter,
    changed_paths: list[str] | tuple[str, ...],
) -> tuple[ValidationCommand, ...]:
    selected = list(adapter.default_command_ids)
    seen = set(selected)
    for rule in adapter.rules:
        if not any(
            fnmatch(path, pattern)
            for path in changed_paths
            for pattern in rule.paths
        ):
            continue
        for command_id in rule.command_ids:
            if command_id not in seen:
                selected.append(command_id)
                seen.add(command_id)
    return tuple(adapter.command(command_id) for command_id in selected)


def validation_policy_digest_payload(adapter: ValidationAdapter) -> dict[str, Any]:
    return validation_adapter_to_dict(adapter)


def command_plan_payload(
    adapter: ValidationAdapter,
    commands: tuple[ValidationCommand, ...],
) -> dict[str, Any]:
    command_map = {
        item["id"]: item
        for item in validation_adapter_to_dict(adapter)["commands"]
    }
    result = {
        "adapter": adapter.kind,
        "image": adapter.image,
        "platform": adapter.platform,
        "network": adapter.network,
        "resources": validation_adapter_to_dict(adapter)["resources"],
        "commands": [command_map[command.id] for command in commands],
    }
    if adapter.kind == "container-argv-v2":
        result["immutable_inputs"] = validation_adapter_to_dict(adapter)[
            "immutable_inputs"
        ]
    return result


def run_validation_adapter(
    repo_dir: str,
    adapter: ValidationAdapter,
    commands: tuple[ValidationCommand, ...],
    *,
    log_path: str | None = None,
) -> ValidationRunResult:
    """Run a typed command plan in fresh, networkless containers."""
    if not commands:
        raise ValueError("validation command plan is empty")
    if shutil.which("docker") is None:
        raise RuntimeError("Docker is required for container validation")

    log_parts: list[str] = [
        "adapter=" + json.dumps(
            command_plan_payload(adapter, commands),
            sort_keys=True,
            separators=(",", ":"),
        ),
    ]
    with tempfile.TemporaryDirectory(prefix="validation-workspace-") as temporary:
        workspace = Path(temporary, "workspace")
        shutil.copytree(
            repo_dir,
            workspace,
            symlinks=True,
            ignore=shutil.ignore_patterns(".git"),
        )
        immutable_mounts = _materialize_immutable_inputs(
            adapter,
            Path(temporary, "immutable-inputs"),
        )
        for command in commands:
            result = _run_container_command(
                workspace,
                adapter,
                command,
                immutable_mounts,
            )
            log_parts.extend([
                f"command={command.id}",
                "argv=" + json.dumps(list(command.argv)),
                f"exit_code={result.returncode}",
                f"timed_out={str(result.timed_out).lower()}",
                result.output,
            ])
            if result.timed_out or result.returncode != 0:
                _write_log(log_path, log_parts)
                summary = result.output[-4000:].strip()
                if result.timed_out:
                    summary = summary or (
                        f"{command.id} timed out after "
                        f"{command.timeout_seconds} seconds"
                    )
                else:
                    summary = summary or (
                        f"{command.id} exited with {result.returncode}"
                    )
                return ValidationRunResult(
                    False,
                    summary,
                    tuple(item.id for item in commands),
                    command_plan_payload(adapter, commands),
                )
            missing = [
                artifact
                for artifact in command.expected_artifacts
                if not _artifact_exists(workspace, artifact)
            ]
            if missing:
                log_parts.append(
                    "missing_expected_artifacts=" + json.dumps(missing),
                )
                _write_log(log_path, log_parts)
                return ValidationRunResult(
                    False,
                    f"{command.id} did not produce: {', '.join(missing)}",
                    tuple(item.id for item in commands),
                    command_plan_payload(adapter, commands),
                )
    _write_log(log_path, log_parts)
    return ValidationRunResult(
        True,
        "",
        tuple(item.id for item in commands),
        command_plan_payload(adapter, commands),
    )


@dataclass(frozen=True)
class _ProcessResult:
    returncode: int
    output: str
    timed_out: bool


def _run_container_command(
    workspace: Path,
    adapter: ValidationAdapter,
    command: ValidationCommand,
    immutable_mounts: tuple[tuple[str, Path], ...] = (),
) -> _ProcessResult:
    name = f"valkey-ci-validation-{uuid.uuid4().hex[:16]}"
    workdir = "/workspace"
    if command.working_directory != ".":
        workdir += f"/{command.working_directory}"
    resources = adapter.resources
    args = [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--pull",
        "always",
        "--platform",
        adapter.platform,
        "--network",
        "none",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",
        "--pids-limit",
        str(resources.pids),
        "--cpus",
        str(resources.cpus),
        "--memory",
        f"{resources.memory_mb}m",
        "--tmpfs",
        f"/tmp:rw,nosuid,nodev,size={resources.tmpfs_mb}m",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--env",
        "CI=1",
        "--env",
        "HOME=/tmp",
        "--volume",
        f"{workspace}:/workspace:rw,Z",
        "--workdir",
        workdir,
    ]
    for input_id, source in immutable_mounts:
        args.extend([
            "--volume",
            f"{source}:/validation-inputs/{input_id}:ro,Z",
        ])
    args.extend([adapter.image, *command.argv])
    try:
        return _run_capped(
            args,
            timeout=command.timeout_seconds,
            output_bytes=resources.output_bytes,
        )
    finally:
        _remove_container(name)


def _materialize_immutable_inputs(
    adapter: ValidationAdapter,
    root: Path,
) -> tuple[tuple[str, Path], ...]:
    if not adapter.immutable_inputs:
        return ()
    downloads = root / "downloads"
    mounts = root / "mounts"
    downloads.mkdir(parents=True)
    mounts.mkdir()
    result: list[tuple[str, Path]] = []
    for item in adapter.immutable_inputs:
        archive = downloads / f"{item.id}.{item.format}"
        output = mounts / item.id
        output.mkdir()
        _download_immutable_input(item, archive)
        _extract_immutable_input(adapter, item, archive, output)
        result.append((item.id, output))
    return tuple(result)


class _ImmutableInputRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        _validate_immutable_input_url(newurl, redirect=True)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _open_immutable_input(url: str) -> Any:
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _ImmutableInputRedirectHandler(),
    )
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": "valkey-ci-agent-validation/2",
        },
    )
    return opener.open(request, timeout=60)


def _download_immutable_input(
    item: ValidationImmutableInput,
    destination: Path,
) -> None:
    digest = hashlib.sha256()
    size = 0
    try:
        with (
            _open_immutable_input(item.url) as response,
            destination.open("xb") as output,
        ):
            _validate_immutable_input_url(response.geturl(), redirect=True)
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > item.size_bytes:
                    raise RuntimeError(
                        f"immutable input {item.id} exceeded its declared size",
                    )
                digest.update(chunk)
                output.write(chunk)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    if size != item.size_bytes:
        destination.unlink(missing_ok=True)
        raise RuntimeError(
            f"immutable input {item.id} size mismatch: "
            f"expected {item.size_bytes}, got {size}",
        )
    actual_digest = digest.hexdigest()
    if actual_digest != item.sha256:
        destination.unlink(missing_ok=True)
        raise RuntimeError(
            f"immutable input {item.id} SHA-256 mismatch",
        )


def _extract_immutable_input(
    adapter: ValidationAdapter,
    item: ValidationImmutableInput,
    archive: Path,
    output: Path,
) -> None:
    if item.format != "deb":
        raise RuntimeError(f"unsupported immutable input format: {item.format}")
    name = f"valkey-ci-input-{uuid.uuid4().hex[:16]}"
    resources = adapter.resources
    args = [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--pull",
        "always",
        "--platform",
        adapter.platform,
        "--network",
        "none",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",
        "--pids-limit",
        str(resources.pids),
        "--cpus",
        str(resources.cpus),
        "--memory",
        f"{resources.memory_mb}m",
        "--tmpfs",
        f"/tmp:rw,nosuid,nodev,size={resources.tmpfs_mb}m",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        f"{archive}:/validation-package.deb:ro,Z",
        "--volume",
        f"{output}:/validation-output:rw,Z",
        "--workdir",
        "/validation-output",
        "--entrypoint",
        "/usr/bin/dpkg-deb",
        adapter.image,
        "--extract",
        "/validation-package.deb",
        "/validation-output",
    ]
    try:
        result = _run_capped(
            args,
            timeout=300,
            output_bytes=min(resources.output_bytes, 4 * 1024 * 1024),
        )
        if result.timed_out:
            raise RuntimeError(
                f"immutable input {item.id} extraction timed out",
            )
        if result.returncode != 0:
            detail = result.output[-4000:].strip()
            raise RuntimeError(
                f"immutable input {item.id} extraction failed: {detail}",
            )
    finally:
        _remove_container(name)


def _remove_container(name: str) -> None:
    """Best-effort cleanup that never replaces the validation result."""
    try:
        subprocess.run(
            ["docker", "rm", "--force", name],
            capture_output=True,
            check=False,
            timeout=30,
            env=filter_env(("PATH", "HOME")),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Could not remove validation container %s: %s", name, exc)


def _run_capped(
    args: list[str],
    *,
    timeout: int,
    output_bytes: int,
) -> _ProcessResult:
    process = subprocess.Popen(
        args,
        env=filter_env(("PATH", "HOME")),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    assert process.stdout is not None
    output = bytearray()
    deadline = time.monotonic() + timeout
    timed_out = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            readable, _, _ = select.select(
                [process.stdout],
                [],
                [],
                min(remaining, 1.0),
            )
            if not readable:
                if process.poll() is not None:
                    break
                continue
            read = getattr(process.stdout, "read1", process.stdout.read)
            chunk = read(65_536)
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > output_bytes:
                del output[:-output_bytes]
    finally:
        process.stdout.close()
        if timed_out:
            terminate_process_group(process)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            terminate_process_group(process)
            process.wait()
    return _ProcessResult(
        returncode=process.returncode if process.returncode is not None else -1,
        output=output.decode("utf-8", errors="replace"),
        timed_out=timed_out,
    )


def _parse_resources(raw: Any, field: str) -> ValidationResources:
    data = _exact(
        raw,
        {"cpus", "memory_mb", "pids", "output_bytes", "tmpfs_mb"},
        field,
    )
    limits = {
        "cpus": (1, 16),
        "memory_mb": (256, 32 * 1024),
        "pids": (16, 4096),
        "output_bytes": (1024, 32 * 1024 * 1024),
        "tmpfs_mb": (16, 4096),
    }
    values: dict[str, int] = {}
    for key, (minimum, maximum) in limits.items():
        value = data[key]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or not minimum <= value <= maximum
        ):
            raise ValueError(
                f"{field}.{key} must be between {minimum} and {maximum}",
            )
        values[key] = value
    return ValidationResources(**values)


def _parse_immutable_inputs(
    raw: Any,
    field: str,
) -> tuple[ValidationImmutableInput, ...]:
    if (
        not isinstance(raw, list)
        or not 1 <= len(raw) <= _MAX_IMMUTABLE_INPUTS
    ):
        raise ValueError(
            f"{field} must contain 1-{_MAX_IMMUTABLE_INPUTS} entries",
        )
    inputs: list[ValidationImmutableInput] = []
    total_size = 0
    for index, value in enumerate(raw):
        item_field = f"{field}[{index}]"
        data = _exact(
            value,
            {"id", "url", "sha256", "size_bytes", "format"},
            item_field,
        )
        input_id = data["id"]
        if not isinstance(input_id, str) or not _ID_RE.fullmatch(input_id):
            raise ValueError(f"{item_field}.id is invalid")
        url = _text(data["url"], f"{item_field}.url", 2048)
        _validate_immutable_input_url(url, redirect=False)
        sha256 = data["sha256"]
        if (
            not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise ValueError(
                f"{item_field}.sha256 must be a lowercase SHA-256 digest",
            )
        size_bytes = data["size_bytes"]
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or not 1 <= size_bytes <= _MAX_IMMUTABLE_INPUT_BYTES
        ):
            raise ValueError(
                f"{item_field}.size_bytes must be between 1 and "
                f"{_MAX_IMMUTABLE_INPUT_BYTES}",
            )
        if data["format"] != "deb":
            raise ValueError(f"{item_field}.format must be 'deb'")
        inputs.append(
            ValidationImmutableInput(
                id=input_id,
                url=url,
                sha256=sha256,
                size_bytes=size_bytes,
                format="deb",
            ),
        )
        total_size += size_bytes
    ids = [item.id for item in inputs]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{field} contains duplicate IDs")
    if total_size > _MAX_TOTAL_IMMUTABLE_INPUT_BYTES:
        raise ValueError(
            f"{field} exceeds the total immutable input size limit",
        )
    return tuple(inputs)


def _validate_immutable_input_url(url: str, *, redirect: bool) -> None:
    parsed = urllib.parse.urlsplit(url)
    allowed_hosts = (
        _IMMUTABLE_INPUT_REDIRECT_HOSTS
        if redirect
        else _IMMUTABLE_INPUT_SOURCE_HOSTS
    )
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("immutable input URL has an invalid port") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname not in allowed_hosts
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or bool(parsed.fragment)
    ):
        raise ValueError("immutable input URL is not an approved HTTPS URL")
    if not redirect and (
        bool(parsed.query)
        or re.fullmatch(
            r"/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/releases/download/"
            r"[^/?#]+/[^/?#]+",
            parsed.path,
        )
        is None
    ):
        raise ValueError(
            "immutable input URL must identify a GitHub release asset",
        )


def _parse_command(raw: Any, field: str) -> ValidationCommand:
    data = _exact(
        raw,
        {
            "id",
            "argv",
            "working_directory",
            "timeout_seconds",
            "inputs",
            "expected_artifacts",
        },
        field,
    )
    command_id = data["id"]
    if not isinstance(command_id, str) or not _ID_RE.fullmatch(command_id):
        raise ValueError(f"{field}.id is invalid")
    argv = data["argv"]
    if not isinstance(argv, list) or not 1 <= len(argv) <= _MAX_ARGV:
        raise ValueError(f"{field}.argv must contain 1-{_MAX_ARGV} strings")
    parsed_argv = tuple(
        _text(value, f"{field}.argv[{index}]", _MAX_ARG_BYTES)
        for index, value in enumerate(argv)
    )
    if any("\x00" in value for value in parsed_argv):
        raise ValueError(f"{field}.argv must not contain NUL")
    workdir = _relative_path(
        data["working_directory"],
        f"{field}.working_directory",
        allow_dot=True,
    )
    timeout = data["timeout_seconds"]
    if (
        not isinstance(timeout, int)
        or isinstance(timeout, bool)
        or not 1 <= timeout <= 3600
    ):
        raise ValueError(f"{field}.timeout_seconds must be between 1 and 3600")
    inputs = _patterns(data["inputs"], f"{field}.inputs", require_nonempty=True)
    artifacts = _path_list(
        data["expected_artifacts"],
        f"{field}.expected_artifacts",
    )
    return ValidationCommand(
        id=command_id,
        argv=parsed_argv,
        working_directory=workdir,
        timeout_seconds=timeout,
        inputs=tuple(inputs),
        expected_artifacts=tuple(artifacts),
    )


def _parse_rule(raw: Any, field: str, known_ids: set[str]) -> ValidationRule:
    data = _exact(raw, {"paths", "command_ids"}, field)
    paths = _patterns(data["paths"], f"{field}.paths", require_nonempty=True)
    command_ids = _id_list(
        data["command_ids"],
        f"{field}.command_ids",
        require_nonempty=True,
    )
    _require_known_ids(command_ids, known_ids, f"{field}.command_ids")
    return ValidationRule(tuple(paths), tuple(command_ids))


def _patterns(raw: Any, field: str, *, require_nonempty: bool) -> list[str]:
    if (
        not isinstance(raw, list)
        or len(raw) > _MAX_PATTERNS
        or (require_nonempty and not raw)
    ):
        raise ValueError(f"{field} is invalid")
    return [
        _text(value, f"{field}[{index}]", 4096)
        for index, value in enumerate(raw)
    ]


def _path_list(raw: Any, field: str) -> list[str]:
    if not isinstance(raw, list) or len(raw) > _MAX_PATTERNS:
        raise ValueError(f"{field} is invalid")
    return [
        _relative_path(value, f"{field}[{index}]", allow_dot=False)
        for index, value in enumerate(raw)
    ]


def _id_list(raw: Any, field: str, *, require_nonempty: bool) -> list[str]:
    if (
        not isinstance(raw, list)
        or len(raw) > _MAX_COMMANDS
        or (require_nonempty and not raw)
    ):
        raise ValueError(f"{field} is invalid")
    values: list[str] = []
    for index, value in enumerate(raw):
        if not isinstance(value, str) or not _ID_RE.fullmatch(value):
            raise ValueError(f"{field}[{index}] is not a command ID")
        values.append(value)
    if len(values) != len(set(values)):
        raise ValueError(f"{field} contains duplicate command IDs")
    return values


def _require_known_ids(values: list[str], known: set[str], field: str) -> None:
    unknown = sorted(set(values) - known)
    if unknown:
        raise ValueError(f"{field} references unknown command IDs: {unknown}")


def _relative_path(value: Any, field: str, *, allow_dot: bool) -> str:
    text = _text(value, field, 4096)
    pure = PurePosixPath(text)
    if (
        pure.is_absolute()
        or ".." in pure.parts
        or (text == "." and not allow_dot)
        or (text != "." and (not pure.parts or text.endswith("/")))
    ):
        raise ValueError(f"{field} must be a contained relative path")
    return text


def _artifact_exists(workspace: Path, relative: str) -> bool:
    candidate = (workspace / relative).resolve()
    if workspace.resolve() != candidate and workspace.resolve() not in candidate.parents:
        return False
    return candidate.exists()


def _write_log(path: str | None, lines: list[str]) -> None:
    if path is None:
        return
    output = "\n".join(lines)
    Path(path).write_text(output[-32 * 1024 * 1024 :], encoding="utf-8")


def _text(value: Any, field: str, max_bytes: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > max_bytes
    ):
        raise ValueError(f"{field} must be a non-empty bounded string")
    return value


def _exact(raw: Any, keys: set[str], field: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be a mapping")
    unknown = sorted(set(raw) - keys)
    missing = sorted(keys - set(raw))
    if unknown or missing:
        raise ValueError(
            f"{field} keys invalid: unknown={unknown}, missing={missing}",
        )
    return raw
