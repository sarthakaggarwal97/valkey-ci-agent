"""The target setup cannot reach runner credentials or trusted control files."""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ci_fix import setup_job


def _layout(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    repo = tmp_path / "work" / "repo"
    repo.mkdir(parents=True)
    runner_temp = tmp_path / "runner-temp"
    plan = runner_temp / "ci-fix-plan"
    plan.mkdir(parents=True)
    setup = plan / "setup.sh"
    setup.write_text("echo setup\n", encoding="utf-8")
    home = tmp_path / "private-home"
    home.mkdir()
    return repo, runner_temp, setup, home


def test_setup_gets_private_command_files_and_no_oidc_or_runner_environment(tmp_path, monkeypatch):
    repo, runner_temp, setup, home = _layout(tmp_path)
    seen = {}

    @contextmanager
    def handed_to(user, root):
        seen["handoff"] = (user, root)
        yield str(home)

    def as_user(user, env, argv, *, home):
        seen.update(user=user, env=env, argv=argv, home=home)
        return SimpleNamespace(argv=("isolated-setup",), env={"ISOLATED": "1"})

    def run(argv, **kwargs):
        seen["run"] = (argv, kwargs)
        seen["stdout_fd"] = kwargs["stdout"].fileno()
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "oidc-secret")
    monkeypatch.setenv("GITHUB_ENV", "/real/github-env")
    monkeypatch.setenv("GITHUB_REPOSITORY", "valkey-io/valkey-ci-agent")
    monkeypatch.setenv("GITHUB_SHA", "outer-sha")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_JOB", "fix-in-job")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_call")
    monkeypatch.setenv("RUNNER_TEMP", str(runner_temp))
    monkeypatch.setenv("PATH", f"{repo}/bin:{runner_temp}/bin:/usr/bin:/bin")
    monkeypatch.setattr(setup_job, "handed_to", handed_to)
    monkeypatch.setattr(setup_job, "as_user", as_user)
    monkeypatch.setattr(setup_job.shutil, "which", lambda name, path: "/bin/sh")
    monkeypatch.setattr(setup_job.subprocess, "run", run)

    setup_job.replay_setup(repo, setup, user="ci-fix-setup", runner_temp=runner_temp)

    env = seen["env"]
    assert "ACTIONS_ID_TOKEN_REQUEST_TOKEN" not in env
    for name in ("GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_REF", "GITHUB_JOB", "GITHUB_EVENT_NAME"):
        assert name not in env
    assert env["GITHUB_ENV"] == str(home / "github-env")
    assert env["GITHUB_PATH"] == str(home / "github-path")
    assert env["GITHUB_OUTPUT"] == str(home / "github-output")
    assert env["GITHUB_STATE"] == str(home / "github-state")
    assert env["RUNNER_TEMP"] == str(home)
    assert str(repo) not in env["PATH"] and str(runner_temp) not in env["PATH"]
    assert seen["argv"] == ["/bin/sh", "-s"]
    argv, kwargs = seen["run"]
    assert argv == ("isolated-setup",)
    assert kwargs == {
        "cwd": repo.resolve(),
        "env": {"ISOLATED": "1"},
        "input": "echo setup\n",
        "text": True,
        "errors": "replace",
        "check": False,
        "timeout": setup_job.SETUP_TIMEOUT_S,
        "stdout": kwargs["stdout"],
        "stderr": subprocess.STDOUT,
    }
    assert seen["stdout_fd"] >= 0
    assert kwargs["stdout"].closed


def test_setup_output_is_captured_and_workflow_commands_are_neutralized(
    tmp_path,
    monkeypatch,
    capsys,
):
    repo, runner_temp, setup, home = _layout(tmp_path)

    @contextmanager
    def handed_to(_user, _root):
        yield str(home)

    monkeypatch.setattr(setup_job, "handed_to", handed_to)
    monkeypatch.setattr(
        setup_job,
        "as_user",
        lambda *_args, **_kwargs: SimpleNamespace(argv=("isolated-setup",), env={}),
    )
    monkeypatch.setattr(setup_job.shutil, "which", lambda name, path: "/bin/sh")
    def run(argv, **kwargs):
        kwargs["stdout"].write(
            b"setup started\n::add-mask::secret\r::stop-commands::TOKEN\n"
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(setup_job.subprocess, "run", run)

    setup_job.replay_setup(repo, setup, user="ci-fix-setup", runner_temp=runner_temp)

    output = capsys.readouterr().err.splitlines()
    assert output == [
        "[target setup] setup started",
        "[target setup] : :add-mask: :secret",
        "[target setup] : :stop-commands: :TOKEN",
    ]
    assert all("::" not in line and not line.startswith("::") for line in output)


def test_setup_failure_is_generic_and_fails_closed(tmp_path, monkeypatch):
    repo, runner_temp, setup, home = _layout(tmp_path)

    @contextmanager
    def handed_to(_user, _root):
        yield str(home)

    monkeypatch.setattr(setup_job, "handed_to", handed_to)
    monkeypatch.setattr(
        setup_job,
        "as_user",
        lambda *_args, **_kwargs: SimpleNamespace(argv=("isolated-setup",), env={}),
    )
    monkeypatch.setattr(setup_job.shutil, "which", lambda name, path: "/bin/sh")
    monkeypatch.setattr(
        setup_job.subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(returncode=23),
    )

    with pytest.raises(RuntimeError, match="isolated job setup failed with exit status 23"):
        setup_job.replay_setup(repo, setup, user="ci-fix-setup", runner_temp=runner_temp)


def test_setup_timeout_is_bounded_and_fails_closed(tmp_path, monkeypatch):
    repo, runner_temp, setup, home = _layout(tmp_path)

    @contextmanager
    def handed_to(_user, _root):
        yield str(home)

    monkeypatch.setattr(setup_job, "handed_to", handed_to)
    monkeypatch.setattr(
        setup_job,
        "as_user",
        lambda *_args, **_kwargs: SimpleNamespace(argv=("isolated-setup",), env={}),
    )
    monkeypatch.setattr(setup_job.shutil, "which", lambda name, path: "/bin/sh")
    monkeypatch.setattr(
        setup_job.subprocess,
        "run",
        lambda argv, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(argv, kwargs["timeout"])
        ),
    )

    with pytest.raises(RuntimeError, match="exceeded its time limit"):
        setup_job.replay_setup(repo, setup, user="ci-fix-setup", runner_temp=runner_temp)


def test_setup_plan_must_be_the_real_authoritative_file(tmp_path):
    repo, runner_temp, _setup, _home = _layout(tmp_path)
    outside = tmp_path / "outside.sh"
    outside.write_text("echo no\n", encoding="utf-8")

    with pytest.raises(ValueError, match="outside the authoritative plan"):
        setup_job.replay_setup(
            repo,
            outside,
            user="ci-fix-setup",
            runner_temp=runner_temp,
        )
