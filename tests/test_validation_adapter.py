from __future__ import annotations

import hashlib
import io
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts.common import validation_adapter
from scripts.common.validation_adapter import (
    ValidationCommand,
    ValidationRunResult,
    command_plan_payload,
    parse_validation_adapter,
    run_validation_adapter,
    select_validation_commands,
    validation_adapter_to_dict,
)

_IMAGE = "gcc@sha256:" + "a" * 64
_REAL_IMAGE = (
    "mcr.microsoft.com/devcontainers/cpp@sha256:"
    "d51703c4fcbe93cd889d38005847521d87cca4d304f33423430daf10a384a332"
)


def _raw_adapter() -> dict:
    return {
        "adapter": "container-argv-v1",
        "image": _IMAGE,
        "platform": "linux/amd64",
        "network": "none",
        "resources": {
            "cpus": 2,
            "memory_mb": 1024,
            "pids": 128,
            "output_bytes": 65536,
            "tmpfs_mb": 64,
        },
        "default_commands": ["build"],
        "commands": [
            {
                "id": "build",
                "argv": ["make", "-j2"],
                "working_directory": ".",
                "timeout_seconds": 600,
                "inputs": ["**"],
                "expected_artifacts": [],
            },
            {
                "id": "cluster-tests",
                "argv": ["./runtest", "--single", "cluster"],
                "working_directory": ".",
                "timeout_seconds": 900,
                "inputs": ["tests/**", "src/**"],
                "expected_artifacts": [],
            },
        ],
        "rules": [
            {
                "paths": ["src/cluster*.c"],
                "command_ids": ["cluster-tests"],
            },
        ],
    }


def _raw_v2_adapter() -> dict:
    raw = _raw_adapter()
    raw["adapter"] = "container-argv-v2"
    raw["immutable_inputs"] = [
        {
            "id": "search-deps",
            "url": (
                "https://github.com/valkey-io/valkey-search/releases/"
                "download/1.0.0-rc1/search-deps.deb"
            ),
            "sha256": hashlib.sha256(b"dependency").hexdigest(),
            "size_bytes": len(b"dependency"),
            "format": "deb",
        },
    ]
    return raw


def test_adapter_round_trip_and_path_selection() -> None:
    adapter = parse_validation_adapter(_raw_adapter(), field="validation")
    assert validation_adapter_to_dict(adapter) == _raw_adapter()
    selected = select_validation_commands(
        adapter,
        ["src/cluster_legacy.c"],
    )
    assert [command.id for command in selected] == ["build", "cluster-tests"]


def test_v2_adapter_round_trip_binds_immutable_inputs() -> None:
    raw = _raw_v2_adapter()
    adapter = parse_validation_adapter(raw, field="validation")

    assert validation_adapter_to_dict(adapter) == raw
    assert adapter.immutable_inputs[0].id == "search-deps"
    assert adapter.immutable_inputs[0].size_bytes == len(b"dependency")
    plan = command_plan_payload(adapter, (adapter.command("build"),))
    assert plan["immutable_inputs"] == raw["immutable_inputs"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(image="gcc:latest"), "sha256"),
        (lambda value: value.update(network="bridge"), "network"),
        (
            lambda value: value["commands"][0].update(argv="make -j2"),
            "argv",
        ),
        (
            lambda value: value.update(default_commands=["missing"]),
            "unknown command",
        ),
        (
            lambda value: value["commands"][0].update(working_directory="../x"),
            "contained relative path",
        ),
    ],
)
def test_adapter_rejects_untyped_or_unsafe_policy(mutation, message) -> None:
    raw = _raw_adapter()
    mutation(raw)
    with pytest.raises(ValueError, match=message):
        parse_validation_adapter(raw, field="validation")


@pytest.mark.parametrize(
    "url",
    [
        "http://github.com/org/repo/releases/download/v1/deps.deb",
        "https://example.com/org/repo/releases/download/v1/deps.deb",
        "https://github.com/org/repo/archive/v1.tar.gz",
        "https://github.com/org/repo/releases/download/v1/deps.deb?token=x",
        "https://user@github.com/org/repo/releases/download/v1/deps.deb",
    ],
)
def test_v2_adapter_rejects_unsafe_immutable_input_urls(url: str) -> None:
    raw = _raw_v2_adapter()
    raw["immutable_inputs"][0]["url"] = url

    with pytest.raises(ValueError, match="immutable input URL"):
        parse_validation_adapter(raw, field="validation")


def test_immutable_input_download_enforces_size_and_digest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = parse_validation_adapter(
        _raw_v2_adapter(),
        field="validation",
    )
    item = adapter.immutable_inputs[0]

    class Response(io.BytesIO):
        def geturl(self) -> str:
            return item.url

    monkeypatch.setattr(
        validation_adapter,
        "_open_immutable_input",
        lambda _url: Response(b"dependency"),
    )
    destination = tmp_path / "dependency.deb"
    validation_adapter._download_immutable_input(item, destination)
    assert destination.read_bytes() == b"dependency"

    destination.unlink()
    monkeypatch.setattr(
        validation_adapter,
        "_open_immutable_input",
        lambda _url: Response(b"tampered!!"),
    )
    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        validation_adapter._download_immutable_input(item, destination)
    assert not destination.exists()


def test_immutable_input_extraction_uses_locked_container(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = parse_validation_adapter(
        _raw_v2_adapter(),
        field="validation",
    )
    item = adapter.immutable_inputs[0]
    archive = tmp_path / "input.deb"
    archive.write_bytes(b"dependency")
    output = tmp_path / "output"
    output.mkdir()
    captured: list[str] = []

    def fake_run(args, *, timeout, output_bytes):
        del timeout, output_bytes
        captured.extend(args)
        return validation_adapter._ProcessResult(0, "", False)

    monkeypatch.setattr(validation_adapter, "_run_capped", fake_run)
    monkeypatch.setattr(validation_adapter.subprocess, "run", MagicMock())

    validation_adapter._extract_immutable_input(
        adapter,
        item,
        archive,
        output,
    )

    assert captured[captured.index("--network") + 1] == "none"
    assert captured[captured.index("--cap-drop") + 1] == "ALL"
    assert "--read-only" in captured
    assert "no-new-privileges" in captured
    assert captured[captured.index("--entrypoint") + 1] == "/usr/bin/dpkg-deb"
    assert captured[-3:] == [
        "--extract",
        "/validation-package.deb",
        "/validation-output",
    ]


def test_executor_builds_networkless_nonprivileged_argv_without_git(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    (repo / "Makefile").write_text("all:\n\t@true\n", encoding="utf-8")
    captured: dict[str, object] = {}

    monkeypatch.setattr(validation_adapter.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        validation_adapter.subprocess,
        "run",
        MagicMock(),
    )

    def fake_run(args, *, timeout, output_bytes):
        del timeout, output_bytes
        captured["args"] = args
        mount = args[args.index("--volume") + 1].split(":", 1)[0]
        assert not Path(mount, ".git").exists()
        return validation_adapter._ProcessResult(0, "ok", False)

    monkeypatch.setattr(validation_adapter, "_run_capped", fake_run)
    adapter = parse_validation_adapter(_raw_adapter(), field="validation")
    result = run_validation_adapter(
        str(repo),
        adapter,
        (adapter.command("build"),),
    )

    assert result == ValidationRunResult(
        True,
        "",
        ("build",),
        result.environment,
    )
    args = captured["args"]
    assert isinstance(args, list)
    assert args[args.index("--network") + 1] == "none"
    assert "ALL" in args
    assert "no-new-privileges" in args
    assert args[-2:] == ["make", "-j2"]
    assert not any(value in {"sh", "bash", "-c"} for value in args[-2:])


def test_expected_artifact_is_enforced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setattr(validation_adapter.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        validation_adapter,
        "_run_container_command",
        lambda *_args: validation_adapter._ProcessResult(0, "", False),
    )
    adapter = parse_validation_adapter(_raw_adapter(), field="validation")
    command = ValidationCommand(
        id="build",
        argv=("make",),
        working_directory=".",
        timeout_seconds=60,
        inputs=("**",),
        expected_artifacts=("src/server",),
    )
    result = run_validation_adapter(str(repo), adapter, (command,))
    assert result.success is False
    assert "src/server" in result.summary


def test_cleanup_failure_does_not_mask_validation_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        validation_adapter,
        "_run_capped",
        lambda *_args, **_kwargs: validation_adapter._ProcessResult(
            1,
            "compile failed",
            False,
        ),
    )
    monkeypatch.setattr(
        validation_adapter.subprocess,
        "run",
        MagicMock(side_effect=TimeoutError("docker daemon hung")),
    )
    adapter = parse_validation_adapter(_raw_adapter(), field="validation")

    result = validation_adapter._run_container_command(
        tmp_path,
        adapter,
        adapter.command("build"),
    )

    assert result.returncode == 1
    assert result.output == "compile failed"


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker is unavailable")
def test_real_adapter_denies_parent_host_network_device_and_root_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "tracked.txt").write_text("safe\n", encoding="utf-8")
    host_secret = tmp_path / "host-secret"
    host_secret.write_text("adapter-parent-secret\n", encoding="utf-8")
    monkeypatch.setenv("AUDIT_ADAPTER_PARENT_SECRET", "must-not-cross")

    raw = _raw_adapter()
    raw["image"] = _REAL_IMAGE
    raw["commands"] = [
        {
            "id": "build",
            "argv": [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-euo",
                "pipefail",
                "-c",
                " && ".join(
                    [
                        "test ! -e .git",
                        f"test ! -e '{host_secret}'",
                        "test -z \"${AUDIT_ADAPTER_PARENT_SECRET:-}\"",
                        (
                            "! grep -a -s 'AUDIT_ADAPTER_PARENT_SECRET=' "
                            "/proc/[0-9]*/environ"
                        ),
                        "test ! -S /var/run/docker.sock",
                        "! touch /adapter-root-write",
                        "test ! -e /dev/mem",
                        (
                            "! timeout 3 /bin/bash -c "
                            "'exec 3<>/dev/tcp/169.254.169.254/80'"
                        ),
                    ]
                ),
            ],
            "working_directory": ".",
            "timeout_seconds": 30,
            "inputs": ["**"],
            "expected_artifacts": [],
        }
    ]
    raw["rules"] = []
    adapter = parse_validation_adapter(raw, field="validation")

    result = run_validation_adapter(
        str(repo),
        adapter,
        (adapter.command("build"),),
    )

    assert result.success is True, result.summary
