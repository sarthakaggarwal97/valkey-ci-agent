from __future__ import annotations

import io
import logging
import subprocess

import pytest

from scripts.ai import claude_code


class _RecordingStdin(io.StringIO):
    def close(self):
        pass


class _FakeProcess:
    def __init__(
        self,
        cmd,
        *,
        stdout_text: str = "",
        returncode: int = 0,
        timeout: bool = False,
        **kwargs,
    ):
        self.cmd = cmd
        self.kwargs = kwargs
        self.stdin = _RecordingStdin()
        self.stdout = io.StringIO(stdout_text)
        self.returncode = returncode
        self.timeout = timeout
        self.killed = False
        self.pid = 12345

    def wait(self, timeout=None):
        if self.timeout:
            raise subprocess.TimeoutExpired(cmd=self.cmd, timeout=timeout)
        return self.returncode

    def kill(self):
        self.killed = True


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch):
    monkeypatch.setenv("CI_AGENT_AI_CONTAINER_IMAGE", "ai-runtime@sha256:test")
    monkeypatch.setenv("CI_AGENT_AI_DOCKER_NETWORK", "ai-internal")
    monkeypatch.setattr(claude_code.Path, "is_dir", lambda _path: True)


def test_run_claude_code_streams_json_in_isolated_container(monkeypatch, caplog):
    captured = {}
    stream = (
        '{"type":"system","subtype":"init","session_id":"abc","model":"opus","cwd":"/tmp/checkout"}\n'
        '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"src/a.c"}}]}}\n'
        '{"type":"result","subtype":"success","num_turns":2,"duration_ms":123,"total_cost_usd":0.01,"result":"done"}\n'
    )

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        captured["process"] = _FakeProcess(cmd, stdout_text=stream, **kwargs)
        return captured["process"]

    monkeypatch.delenv("CI_AGENT_CLAUDE_MODEL", raising=False)
    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    with caplog.at_level(logging.INFO, logger="scripts.ai.claude_code"):
        stdout, stderr, rc = claude_code.run_claude_code("fix this", cwd="/tmp/checkout")

    assert stdout == stream
    assert stderr == ""
    assert rc == 0
    assert captured["process"].stdin.getvalue() == "fix this"
    assert captured["cmd"][:4] == ["docker", "run", "--rm", "--network"]
    assert captured["cmd"][captured["cmd"].index("--network") + 1] == "ai-internal"
    assert "--read-only" in captured["cmd"]
    assert ["--cap-drop", "ALL"] == captured["cmd"][
        captured["cmd"].index("--cap-drop"):captured["cmd"].index("--cap-drop") + 2
    ]
    assert "/tmp/checkout:/workspace:ro" in captured["cmd"]
    assert "/var/run/docker.sock" not in " ".join(captured["cmd"])
    assert "ai-runtime@sha256:test" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"
    assert captured["cmd"][captured["cmd"].index("--effort") + 1] == "max"
    assert captured["cmd"][captured["cmd"].index("--output-format") + 1] == "stream-json"
    assert "--verbose" in captured["cmd"]
    tools = captured["cmd"][captured["cmd"].index("--tools") + 1]
    assert "Edit" in tools
    assert "MultiEdit" in tools
    assert "--dangerously-skip-permissions" not in captured["cmd"]
    assert "--strict-mcp-config" in captured["cmd"]
    assert "--disallowedTools" not in captured["cmd"]
    assert captured["kwargs"]["cwd"] is None
    assert "AWS_ACCESS_KEY_ID" not in captured["kwargs"]["env"]
    assert "AWS_REGION" not in captured["kwargs"]["env"]
    assert "ANTHROPIC_AUTH_TOKEN" not in captured["kwargs"]["env"]
    assert "Claude stream: system init model=opus session=abc cwd=/tmp/checkout" in caplog.text
    assert "Claude stream: assistant tool=Read file_path=src/a.c" in caplog.text
    assert "Claude stream: result success turns=2 duration_ms=123 cost_usd=0.01 text=done" in caplog.text


def test_run_claude_code_drops_existing_cloud_region_and_keeps_model(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.delenv("CI_AGENT_CLAUDE_MODEL", raising=False)
    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt", model="model-id", cwd="/tmp/checkout"
    )

    assert (stdout, stderr, rc) == ('{"type":"result","result":"ok"}\n', "", 0)
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "model-id"
    assert "AWS_REGION" not in captured["env"]


def test_run_claude_code_does_not_inherit_github_tokens(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["env"] = kwargs["env"]
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setenv("GITHUB_TOKEN", "github-secret")
    monkeypatch.setenv("GH_TOKEN", "gh-secret")
    monkeypatch.setenv("BACKPORT_GITHUB_TOKEN", "backport-secret")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code("prompt", cwd="/tmp/checkout")

    assert (stdout, stderr, rc) == ('{"type":"result","result":"ok"}\n', "", 0)
    assert "AWS_REGION" not in captured["env"]
    assert "GITHUB_TOKEN" not in captured["env"]
    assert "GH_TOKEN" not in captured["env"]
    assert "BACKPORT_GITHUB_TOKEN" not in captured["env"]


def test_run_claude_code_denies_bash_and_write_when_not_allowed(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt",
        cwd="/tmp/checkout",
        allowed_tools="Read,Edit,MultiEdit,Grep,Glob",
    )

    assert (stdout, stderr, rc) == ('{"type":"result","result":"ok"}\n', "", 0)
    assert captured["cmd"][captured["cmd"].index("--tools") + 1] == "Read,Edit,MultiEdit,Grep,Glob"
    assert "--dangerously-skip-permissions" not in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--disallowedTools") + 1] == "Bash,Write"


def test_run_claude_code_respects_explicit_empty_disallowed_tools(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt",
        cwd="/tmp/checkout",
        allowed_tools="Read,Edit,MultiEdit,Grep,Glob",
        disallowed_tools="",
    )

    assert (stdout, stderr, rc) == ('{"type":"result","result":"ok"}\n', "", 0)
    assert "--disallowedTools" not in captured["cmd"]


def test_run_claude_code_only_skips_permissions_when_explicit(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    result = claude_code.run_claude_code(
        "prompt",
        cwd="/tmp/checkout",
        allowed_tools="Read,Edit,MultiEdit,Grep,Glob",
        dangerously_skip_permissions=True,
    )

    assert result == ('{"type":"result","result":"ok"}\n', "", 0)
    assert "--dangerously-skip-permissions" in captured["cmd"]
    assert "/tmp/checkout:/workspace:rw" in captured["cmd"]


def test_run_claude_code_honors_model_env_overrides(monkeypatch):
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return _FakeProcess(cmd, stdout_text='{"type":"result","result":"ok"}\n', **kwargs)

    monkeypatch.setenv("CI_AGENT_CLAUDE_MODEL", "custom-opus")
    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt", model="ignored", cwd="/tmp/checkout"
    )

    assert (stdout, stderr, rc) == ('{"type":"result","result":"ok"}\n', "", 0)
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "custom-opus"
    assert "ANTHROPIC_DEFAULT_OPUS_MODEL" not in captured["env"]


def test_run_claude_code_reports_timeout(monkeypatch):
    fake_processes = []

    def fake_popen(cmd, **kwargs):
        process = _FakeProcess(
            cmd,
            stdout_text='{"type":"assistant","message":{"content":[]}}\n',
            timeout=True,
            **kwargs,
        )
        fake_processes.append(process)
        return process

    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt", timeout=3, cwd="/tmp/checkout"
    )

    assert stdout == '{"type":"assistant","message":{"content":[]}}\n'
    assert stderr == "timeout after 3s"
    assert rc == 1
    assert fake_processes[0].killed is True


def test_run_claude_code_fails_closed_at_transcript_limit(monkeypatch):
    fake_processes = []

    def fake_popen(cmd, **kwargs):
        process = _FakeProcess(cmd, stdout_text="123456789", **kwargs)
        fake_processes.append(process)
        return process

    monkeypatch.setattr(claude_code, "_MAX_TRANSCRIPT_BYTES", 8)
    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    stdout, stderr, rc = claude_code.run_claude_code(
        "prompt",
        cwd="/tmp/checkout",
    )

    assert stdout == "12345678"
    assert stderr == "transcript exceeded 8 bytes"
    assert rc == 1
    assert fake_processes[0].killed is True


def test_run_claude_code_reports_missing_cli(monkeypatch):
    def fake_popen(_cmd, **_kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(claude_code.subprocess, "Popen", fake_popen)

    assert claude_code.run_claude_code(
        "prompt", cwd="/tmp/checkout"
    ) == ("", "claude not found", 127)


def test_run_claude_code_fails_closed_without_explicit_workspace():
    stdout, stderr, rc = claude_code.run_claude_code("prompt")

    assert stdout == ""
    assert "explicit isolated workspace" in stderr
    assert rc == 127
