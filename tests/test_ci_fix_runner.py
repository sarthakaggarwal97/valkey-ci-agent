"""Tests for the sanitized verification command-runner.

These guard the trust anchor: a passing verdict comes only from a real
subprocess exit code, the environment is scrubbed, and the working directory
cannot escape the repo clone.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from scripts.ci_fix import runner as runner_mod
from scripts.ci_fix.runner import VERIFY_USER_ENV, run_verification_command


@pytest.fixture(autouse=True)
def _run_as_the_test_user(monkeypatch):
    monkeypatch.delenv(VERIFY_USER_ENV, raising=False)


def test_passed_derived_from_exit_zero(tmp_path):
    result = run_verification_command(str(tmp_path), "true")
    assert result.ran is True
    assert result.passed is True
    assert result.exit_code == 0


def test_failed_derived_from_nonzero_exit(tmp_path):
    result = run_verification_command(str(tmp_path), "exit 7")
    assert result.ran is True
    assert result.passed is False
    assert result.exit_code == 7


def test_empty_command_does_not_run(tmp_path):
    result = run_verification_command(str(tmp_path), "   ")
    assert result.ran is False
    assert result.passed is False


def test_missing_repo_dir_does_not_run(tmp_path):
    missing = tmp_path / "nope"
    result = run_verification_command(str(missing), "true")
    assert result.ran is False
    assert result.passed is False


def test_workdir_escape_is_rejected(tmp_path):
    result = run_verification_command(str(tmp_path), "true", workdir="../..")
    assert result.ran is False
    assert "escapes" in result.output_tail


def test_workdir_inside_repo_is_allowed(tmp_path):
    (tmp_path / "src").mkdir()
    result = run_verification_command(str(tmp_path), "true", workdir="src")
    assert result.ran is True
    assert result.passed is True


def test_timeout_marks_not_passed(tmp_path):
    result = run_verification_command(str(tmp_path), "sleep 5", timeout=1)
    assert result.ran is True
    assert result.passed is False
    assert result.timed_out is True


def test_environment_is_scrubbed(tmp_path, monkeypatch):
    """Tokens of any name in the parent env must not reach the command."""
    monkeypatch.setenv("GITHUB_TOKEN", "secret1")
    monkeypatch.setenv("GH_TOKEN", "secret2")
    monkeypatch.setenv("ACTIONS_RUNTIME_TOKEN", "secret3")
    monkeypatch.setenv("PATH", os.environ.get("PATH", "/usr/bin:/bin"))
    out = tmp_path / "leak.txt"
    result = run_verification_command(
        str(tmp_path),
        f'echo "[${{GITHUB_TOKEN}}][${{GH_TOKEN}}][${{ACTIONS_RUNTIME_TOKEN}}]" > {out}',
    )
    assert result.passed is True
    assert out.read_text().strip() == "[][][]"


def test_output_tail_truncates_large_output(tmp_path):
    result = run_verification_command(str(tmp_path), "yes x | head -c 100000")
    assert result.passed is True
    assert "[truncated]" in result.output_tail
    assert len(result.output_tail) < 100000


def test_aws_credentials_never_reach_command(tmp_path, monkeypatch):
    """The verification command must not see AWS/Bedrock credentials (P0)."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "shhh")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "tok")
    monkeypatch.setenv("PATH", os.environ.get("PATH", "/usr/bin:/bin"))
    out = tmp_path / "aws.txt"
    result = run_verification_command(
        str(tmp_path),
        f'echo "[${{AWS_ACCESS_KEY_ID}}][${{AWS_SECRET_ACCESS_KEY}}][${{AWS_SESSION_TOKEN}}]" > {out}',
    )
    assert result.passed is True
    assert out.read_text().strip() == "[][][]"


def test_output_tail_is_captured(tmp_path):
    result = run_verification_command(str(tmp_path), "echo hello-from-test")
    assert "hello-from-test" in result.output_tail


def test_chained_command_runs_via_shell(tmp_path):
    result = run_verification_command(str(tmp_path), "true && echo ok && exit 0")
    assert result.passed is True
    assert "ok" in result.output_tail


def test_local_verification_supports_bash_conditionals(tmp_path):
    result = run_verification_command(
        str(tmp_path),
        'if [[ -n "x" ]]; then echo bash-ok; else exit 1; fi',
    )
    assert result.passed is True
    assert "bash-ok" in result.output_tail


def test_docker_image_wraps_command(tmp_path, monkeypatch):
    """When a container image is given, the command runs via a named docker run that is removed afterwards."""
    captured = {}
    removed = []

    def fake_run_capped(argv, cwd, env, timeout):
        captured["argv"] = argv
        return True, 0, "ok", False

    def fake_run(argv, **_kwargs):
        removed.append(argv)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setenv(VERIFY_USER_ENV, "verifier")  # a container never runs as the verify user
    monkeypatch.setattr(runner_mod, "_run_capped", fake_run_capped)
    monkeypatch.setattr(runner_mod.subprocess, "run", fake_run)
    result = run_verification_command(
        str(tmp_path), "make test", container_image="almalinux:8",
    )
    assert result.passed is True
    docker = captured["argv"][-1]
    assert docker.startswith("docker run --rm --name ci-fix-verify-")
    assert "--network none" in docker and "almalinux:8" in docker and "make test" in docker
    name = docker.split("--name ", 1)[1].split()[0]
    assert removed == [["docker", "rm", "-f", name]]
    # The user-facing command stays the plain one, not the docker wrapper.
    assert result.command == "make test"


def test_a_background_process_does_not_outlive_the_command(tmp_path):
    marker = tmp_path / "late"
    result = run_verification_command(
        str(tmp_path), f"(sleep 2; touch {marker}) >/dev/null 2>&1 & echo started",
    )
    assert result.passed is True
    time.sleep(3)
    assert not marker.exists()


def test_the_verify_user_runs_the_command_with_its_own_home(monkeypatch):
    monkeypatch.setattr(runner_mod.pwd, "getpwnam", lambda _user: type("P", (), {"pw_dir": "/home/v"})())
    argv = runner_mod._as_user("v", {"PATH": "/usr/bin", "HOME": "/home/runner", "TMPDIR": "/runner/tmp"},
                               ["bash", "-c", "make"])
    assert argv[:8] == ["sudo", "-n", "runuser", "-u", "v", "--", "env", "-i"]
    assert "HOME=/home/v" in argv and "USER=v" in argv and "PATH=/usr/bin" in argv
    assert not any(arg.startswith("TMPDIR=") or arg == "HOME=/home/runner" for arg in argv)
    assert argv[-3:] == ["bash", "-c", "make"]


def test_an_unreachable_checkout_refuses_to_run(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setenv(VERIFY_USER_ENV, "verifier")
    monkeypatch.setattr(runner_mod, "_sudo", lambda *argv: calls.append(argv))
    private = tmp_path / "private"
    (private / "repo").mkdir(parents=True)
    private.chmod(0o700)
    try:
        result = run_verification_command(str(private / "repo"), "true")
    finally:
        private.chmod(0o755)
    assert result.ran is False
    assert "is not traversable" in result.output_tail
    assert calls == []  # nothing was handed over


@pytest.fixture
def reachable():
    """A directory every user can traverse, as the pipeline's workdir is (pytest's tmp_path is 0700)."""
    path = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    path.chmod(0o711)
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_the_checkout_is_taken_back_even_when_the_command_fails_to_start(reachable, monkeypatch):
    calls = []
    tmp_path = reachable
    monkeypatch.setenv(VERIFY_USER_ENV, "verifier")
    monkeypatch.setattr(runner_mod, "_sudo", lambda *argv: calls.append(argv))
    monkeypatch.setattr(runner_mod, "_kill_all", lambda user: calls.append(("kill", user)))
    monkeypatch.setattr(runner_mod, "_as_user", lambda *_a: ["/nonexistent/binary"])
    result = run_verification_command(str(tmp_path), "true")
    assert result.ran is False
    root = str(tmp_path.resolve())
    assert calls == [
        ("chown", "-R", "-h", "-P", "--", "verifier:", root),
        ("kill", "verifier"),
        ("chown", "-R", "-h", "-P", "--", f"{os.getuid()}:{os.getgid()}", root),
    ]


# --- isolation, against a real account ------------------------------------------------
# CI creates the account and names it in CI_FIX_TEST_VERIFY_USER (see ci.yml);
# elsewhere these skip.

_USER = os.environ.get("CI_FIX_TEST_VERIFY_USER", "")
needs_verify_user = pytest.mark.skipif(
    not _USER or shutil.which("sudo") is None
    or subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0,
    reason="CI_FIX_TEST_VERIFY_USER names no account this test can sudo to",
)


@pytest.fixture
def as_verify_user(monkeypatch):
    monkeypatch.setenv(VERIFY_USER_ENV, _USER)


def _checkout(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "file").write_text("x")
    return repo


@needs_verify_user
@pytest.mark.usefixtures("as_verify_user")
def test_the_command_cannot_read_the_parents_environment(reachable, monkeypatch):
    tmp_path = reachable
    monkeypatch.setenv("CI_FIX_PARENT_SECRET", "s3cret")
    repo = _checkout(tmp_path)
    result = run_verification_command(
        str(repo), f"cat /proc/{os.getpid()}/environ /proc/$PPID/environ 2>&1; id -un",
    )
    assert result.ran is True
    assert "s3cret" not in result.output_tail
    assert _USER in result.output_tail


@needs_verify_user
@pytest.mark.usefixtures("as_verify_user")
def test_the_command_cannot_write_outside_the_checkout(reachable):
    tmp_path = reachable
    repo = _checkout(tmp_path)
    state = tmp_path / "state.json"
    state.write_text("{}")
    state.chmod(0o644)
    result = run_verification_command(str(repo), f"echo forged > {state}")
    assert result.passed is False
    assert state.read_text() == "{}"


@needs_verify_user
@pytest.mark.usefixtures("as_verify_user")
def test_the_checkout_comes_back_owned_by_the_runner_with_nothing_left_running(reachable):
    tmp_path = reachable
    repo = _checkout(tmp_path)
    marker = repo / "late"
    result = run_verification_command(
        str(repo), f"touch made; ln -s /etc/passwd link; (setsid sleep 3; touch {marker}) >/dev/null 2>&1 &",
    )
    assert result.passed is True
    time.sleep(4)
    assert not marker.exists()
    for path in (repo, repo / "file", repo / "made"):
        assert path.stat().st_uid == os.getuid()
    assert (repo / "link").lstat().st_uid == os.getuid()
    assert os.stat("/etc/passwd").st_uid == 0
