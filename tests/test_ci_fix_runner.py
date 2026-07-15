"""Security-contract tests for isolated CI-fix verification."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from scripts.ci_fix import runner


def _completed(
    args: list[str],
    *,
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args, returncode, stdout, stderr)


def test_direct_host_execution_is_disabled(tmp_path: Path) -> None:
    marker = tmp_path / "ran"

    result = runner.run_verification_command(
        str(tmp_path),
        f"touch {marker}",
    )

    assert result.ran is False
    assert result.passed is False
    assert "isolated runtime required" in result.output_tail
    assert not marker.exists()


def test_linux_runtime_contract_records_complete_isolation() -> None:
    identity = "sha256:" + "a" * 64

    contract = runner.verification_runtime_contract(
        platform="linux",
        image_identity=identity,
    )

    assert contract["executor"] == "ephemeral-docker-cgroup"
    assert contract["network"] == "none"
    assert contract["filesystem"] == "quota-tmpfs-checkout-rw-without-git"
    assert contract["host_mounts"] == "none"
    assert contract["docker_socket"] == "absent"
    assert contract["capabilities"] == "none"
    assert contract["read_only_root"] is True
    assert contract["separate_pid_namespace"] is True
    resources = contract["resources"]
    assert isinstance(resources, dict)
    for key in (
        "timeout_seconds",
        "captured_output_bytes",
        "cpu_limit",
        "memory_mb",
        "pid_limit",
        "workspace_mb",
        "tmpfs_mb",
    ):
        assert isinstance(resources[key], int)
        assert resources[key] > 0


def test_macos_runtime_contract_records_vm_uid_and_seatbelt() -> None:
    contract = runner.verification_runtime_contract(
        platform="macos",
        sandbox_uid=550,
    )

    assert contract["executor"] == "credentialless-macos-vm-seatbelt"
    assert contract["network"] == "none-seatbelt-default-deny"
    assert contract["filesystem"] == "quota-apfs-checkout-rw-without-git"
    assert contract["sandbox_uid"] == 550
    assert contract["read_only_root"] is True
    assert contract["separate_pid_namespace"] == "ephemeral-github-hosted-vm"


def test_linux_runtime_pulls_then_binds_to_content_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    identity = "sha256:" + "b" * 64
    monkeypatch.setattr(runner.shutil, "which", lambda _name: "/usr/bin/docker")

    def fake_control(
        args: list[str],
        *,
        timeout: int,
        check: bool = True,
        stdin=None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check, stdin
        calls.append(args)
        stdout = identity + "\n" if args[1:3] == ["image", "inspect"] else ""
        return _completed(args, stdout=stdout)

    monkeypatch.setattr(runner, "_control_run", fake_control)

    runtime = runner.prepare_verification_runtime(platform="linux")

    assert calls[0] == [
        "docker",
        "pull",
        "--platform",
        "linux/amd64",
        runner.DEFAULT_LINUX_VERIFIER_IMAGE,
    ]
    assert runtime.requested_image == ""
    assert runtime.effective_image == identity
    assert runtime.image_identity == identity


def test_macos_runtime_requires_preprovisioned_dedicated_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI_FIX_MACOS_SANDBOX_USER", raising=False)

    with pytest.raises(RuntimeError, match="dedicated"):
        runner.prepare_verification_runtime(platform="macos")


def test_linux_container_has_no_host_mount_and_enforces_cgroup_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    identity = "sha256:" + "c" * 64
    runtime = runner.VerificationRuntime(
        platform="linux",
        requested_image="",
        effective_image=identity,
        image_identity=identity,
        sandbox_user="numeric-nonroot",
        sandbox_uid=os.getuid(),
    )
    sandbox = runner._VerificationSandbox(runtime, str(tmp_path))
    sandbox._staging = tmp_path
    calls: list[list[str]] = []
    streamed_inputs: list[object] = []
    inspect = json.dumps(
        [
            {
                "HostConfig": {
                    "NetworkMode": "none",
                    "ReadonlyRootfs": True,
                    "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges:true"],
                    "PidsLimit": runner._PID_LIMIT,
                    "Memory": runner._MEMORY_MB * 1024 * 1024,
                    "NanoCpus": runner._CPU_LIMIT * 1_000_000_000,
                    "Tmpfs": {
                        "/workspace": "rw,nosuid,nodev",
                        "/tmp": "rw,nosuid,nodev,noexec",
                    },
                },
                "Mounts": [
                    {"Type": "tmpfs", "Destination": "/workspace"},
                    {"Type": "tmpfs", "Destination": "/tmp"},
                ],
            }
        ]
    )

    def fake_control(
        args: list[str],
        *,
        timeout: int,
        check: bool = True,
        stdin=None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check
        calls.append(args)
        if stdin is not None:
            streamed_inputs.append(stdin)
        return _completed(args, stdout=inspect if args[1] == "inspect" else "")

    monkeypatch.setattr(runner, "_control_run", fake_control)

    sandbox._start_linux_container()
    create = calls[0]
    assert create[:2] == ["docker", "create"]
    assert "--network" in create
    assert create[create.index("--network") + 1] == "none"
    assert "--read-only" in create
    assert "--pids-limit" in create
    assert "--cpus" in create
    assert "--memory" in create
    assert "--volume" not in create
    assert "--mount" not in create
    stream = next(
        call
        for call in calls
        if call[:2] == ["docker", "exec"] and "/bin/tar" in call
    )
    assert "-i" in stream
    assert stream[stream.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert streamed_inputs


def test_linux_target_exec_is_numeric_nonroot_and_environment_bounded(
    tmp_path: Path,
) -> None:
    identity = "sha256:" + "d" * 64
    runtime = runner.VerificationRuntime(
        platform="linux",
        requested_image="",
        effective_image=identity,
        image_identity=identity,
        sandbox_user="numeric-nonroot",
        sandbox_uid=os.getuid(),
    )
    sandbox = runner._VerificationSandbox(runtime, str(tmp_path))
    sandbox._container_name = "sandbox"

    args = sandbox._linux_exec_args("make test", Path("."))

    assert args[:2] == ["docker", "exec"]
    assert args[args.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert args[args.index("--workdir") + 1] == "/workspace"
    assert "GITHUB_TOKEN" not in " ".join(args)
    assert args[-9:] == [
        "/bin/bash",
        "--noprofile",
        "--norc",
        "-O",
        "inherit_errexit",
        "-euo",
        "pipefail",
        "-c",
        "make test",
    ]


def test_macos_profile_is_default_deny_and_write_scoped(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    profile = runner._macos_profile(workspace)

    assert "(deny default)" in profile
    assert "(allow network" not in profile
    assert "(allow file-read*)" not in profile
    assert f'(subpath "{workspace}")' in profile
    assert '(subpath "/System")' in profile
    assert "/Users" not in profile
    assert f'(allow file-write* (subpath "{workspace}"))' in profile
    assert str(tmp_path / "outside") not in profile


def test_macos_command_uses_sudo_env_i_seatbelt_and_limits(
    tmp_path: Path,
) -> None:
    runtime = runner.VerificationRuntime(
        platform="macos",
        requested_image="",
        effective_image="",
        image_identity="",
        sandbox_user=runner.MACOS_SANDBOX_USER,
        sandbox_uid=550,
    )
    sandbox = runner._VerificationSandbox(runtime, str(tmp_path))
    mount = tmp_path / "volume"
    (mount / "workspace").mkdir(parents=True)
    sandbox._macos_mount = mount
    sandbox._macos_profile = runner._macos_profile(mount / "workspace")

    args = sandbox._macos_exec_args("make test", Path("."), 60)
    joined = " ".join(args)

    assert args[:4] == ["/usr/bin/sudo", "-n", "-u", runner.MACOS_SANDBOX_USER]
    assert "/usr/bin/env" in args
    assert "-i" in args
    assert "sandbox-exec" in joined
    assert "ulimit -u" in joined
    assert "ulimit -v" in joined
    assert "ulimit -t" in joined
    assert "GITHUB_TOKEN" not in joined


def test_sandbox_rejects_workdir_escape_before_execution(tmp_path: Path) -> None:
    runtime = runner.VerificationRuntime(
        platform="linux",
        requested_image="",
        effective_image="sha256:" + "e" * 64,
        image_identity="sha256:" + "e" * 64,
        sandbox_user="numeric-nonroot",
        sandbox_uid=os.getuid(),
    )
    sandbox = runner._VerificationSandbox(runtime, str(tmp_path))

    result = sandbox.run(str(tmp_path), "true", workdir="../")

    assert result.ran is False
    assert "escapes" in result.output_tail


def test_low_level_timeout_kills_descendant_processes(tmp_path: Path) -> None:
    pid_file = tmp_path / "child.pid"
    command = f"sleep 30 & child=$!; printf %s \"$child\" > {pid_file}; wait"

    _, _, _, timed_out = runner._run_capped(
        command,
        tmp_path,
        {"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        1,
    )

    assert timed_out is True
    child_pid = int(pid_file.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 2
    while _pid_is_running(child_pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not _pid_is_running(child_pid)


@pytest.mark.parametrize(
    "command",
    [
        "false | true",
        "(false)",
        "false > redirected.log",
        'result="$(false)"',
        "true\nfalse\ntrue",
    ],
)
def test_fail_fast_shell_forms_cannot_report_success(
    tmp_path: Path,
    command: str,
) -> None:
    _, exit_code, _, timed_out = runner._run_capped(
        command,
        tmp_path,
        {"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        5,
    )

    assert timed_out is False
    assert exit_code != 0


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker is unavailable")
def test_real_linux_sandbox_denies_host_proc_network_device_and_root_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "tracked.txt").write_text("safe\n", encoding="utf-8")
    host_secret = tmp_path / "host-secret"
    host_credentials = tmp_path / "host-home" / ".aws" / "credentials"
    sibling_workflow = (
        tmp_path / "sibling-workspace" / ".github" / "workflows" / "ci.yml"
    )
    host_credentials.parent.mkdir(parents=True)
    sibling_workflow.parent.mkdir(parents=True)
    marker = "audit-parent-secret-must-not-cross"
    host_secret.write_text(marker, encoding="utf-8")
    host_credentials.write_text(
        "[default]\naws_access_key_id=host-only\n",
        encoding="utf-8",
    )
    sibling_workflow.write_text("name: host-only\n", encoding="utf-8")
    monkeypatch.setenv("AUDIT_PARENT_SECRET", marker)

    runtime = runner.prepare_verification_runtime(platform="linux")
    with runtime.sandbox(str(repo)) as sandbox:
        result = sandbox.run(
            str(repo),
            " && ".join(
                [
                    "test ! -e .git",
                    'test ! -e "${HOME}/.aws/credentials"',
                    f"test ! -e {runner._shell_quote(str(host_secret))}",
                    f"test ! -e {runner._shell_quote(str(host_credentials))}",
                    f"test ! -e {runner._shell_quote(str(sibling_workflow))}",
                    (
                        f"! grep -a -s {runner._shell_quote(marker)} "
                        "/proc/[0-9]*/environ"
                    ),
                    "! touch /host-root-write",
                    "test ! -e /dev/mem",
                    (
                        "! timeout 3 /bin/bash -c "
                        "'exec 3<>/dev/tcp/169.254.169.254/80'"
                    ),
                ]
            ),
            timeout=30,
        )

    assert result.ran is True
    assert result.passed is True, result.output_tail


@pytest.mark.skipif(
    sys.platform != "linux" or shutil.which("docker") is None,
    reason="Linux Docker is unavailable",
)
def test_real_linux_sandbox_timeout_removes_container_descendants(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "tracked.txt").write_text("safe\n", encoding="utf-8")
    marker = "ci-fix-timeout-" + uuid.uuid4().hex
    result_box: list[object] = []

    runtime = runner.prepare_verification_runtime(platform="linux")
    with runtime.sandbox(str(repo)) as sandbox:
        container_name = sandbox._container_name

        def run_until_timeout() -> None:
            result_box.append(
                sandbox.run(
                    str(repo),
                    f"bash -c 'exec -a {marker} sleep 300' & wait",
                    timeout=3,
                )
            )

        thread = threading.Thread(target=run_until_timeout)
        thread.start()
        descendant_pid = _wait_for_container_process(container_name, marker)
        thread.join(timeout=15)

        assert not thread.is_alive()
        assert len(result_box) == 1
        result = result_box[0]
        assert isinstance(result, runner.RunResult)
        assert result.timed_out is True

    deadline = time.monotonic() + 5
    while _pid_is_running(descendant_pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _pid_is_running(descendant_pid)
    inspect = subprocess.run(
        ["docker", "inspect", container_name],
        check=False,
        capture_output=True,
        text=True,
    )
    assert inspect.returncode != 0


@pytest.mark.skipif(
    sys.platform != "darwin"
    or os.environ.get("CI_FIX_MACOS_SANDBOX_USER") != runner.MACOS_SANDBOX_USER,
    reason="dedicated macOS verifier identity is unavailable",
)
def test_real_macos_sandbox_denies_network_and_sibling_files(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "tracked.txt").write_text("safe\n", encoding="utf-8")

    runtime = runner.prepare_verification_runtime(platform="macos")
    with runtime.sandbox(str(repo)) as sandbox:
        result = sandbox.run(
            str(repo),
            'test "$(cat tracked.txt)" = safe && test ! -e .git',
            timeout=30,
        )

    assert result.ran is True
    assert result.passed is True, result.output_tail


def _pid_is_running(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return stat.split(")", 1)[1].strip().split(" ", 1)[0] != "Z"


def _wait_for_container_process(container_name: str, marker: str) -> int:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "top", container_name, "-eo", "pid,args"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if marker not in line:
                    continue
                pid, _, _ = line.strip().partition(" ")
                if pid.isdigit():
                    return int(pid)
        time.sleep(0.05)
    pytest.fail("timed verifier descendant was not observed")
