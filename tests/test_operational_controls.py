from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts.common.operational_controls import (
    OperationalPolicy,
    enforce_operational_access,
    main,
    parse_operational_policy,
    policy_environment,
    request_publication_admission,
)


def test_policy_is_strict_and_bounded() -> None:
    policy = parse_operational_policy({
        "enabled": False,
        "daily_ai_requests": 10,
        "run_ai_requests": 2,
        "max_queue_depth": 1,
    })
    assert policy.enabled is False
    assert policy.daily_ai_requests == 10
    assert policy.max_queue_depth == 1

    with pytest.raises(ValueError, match="unknown key"):
        parse_operational_policy({"daily_tokens": 1})
    with pytest.raises(ValueError, match="cannot exceed"):
        parse_operational_policy({
            "daily_ai_requests": 2,
            "run_ai_requests": 3,
        })
    with pytest.raises(ValueError, match="between"):
        parse_operational_policy({"max_queue_depth": 0})


def test_kill_switches_fail_closed() -> None:
    policy = OperationalPolicy()
    enforce_operational_access(
        "org/repo",
        policy,
        global_kill_switch="false",
        disabled_repositories="org/other",
    )
    with pytest.raises(RuntimeError, match="kill switch"):
        enforce_operational_access(
            "org/repo",
            policy,
            global_kill_switch="true",
        )
    with pytest.raises(RuntimeError, match="operator disabled"):
        enforce_operational_access(
            "org/repo",
            policy,
            global_kill_switch=False,
            disabled_repositories="org/repo",
        )
    with pytest.raises(ValueError, match="invalid boolean"):
        enforce_operational_access(
            "org/repo",
            policy,
            global_kill_switch="maybe",
        )


def test_policy_environment_is_content_addressed() -> None:
    values = policy_environment("org/repo", "backport:prepare", OperationalPolicy())
    assert values["AI_CONTROL_REPOSITORY"] == "org/repo"
    assert values["AI_CONTROL_MAX_QUEUE_DEPTH"] == "4"
    assert len(values["AI_CONTROL_POLICY_SHA256"]) == 64


def test_cli_exports_repository_policy(tmp_path, capsys) -> None:
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
schema_version: 2
repos:
  - repo: org/repo
    project_owner: org
    language: c
    automation:
      daily_ai_requests: 5
      run_ai_requests: 2
    validation_waiver:
      reason: test
      approved_by: test
      expires: "2099-01-01"
    branches:
      - branch: "1.0"
        project_number: 1
""",
        encoding="utf-8",
    )

    assert main([
        "--repo", "org/repo",
        "--operation", "test:run",
        "--registry", str(registry),
    ]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["AI_CONTROL_DAILY_REQUEST_LIMIT"] == "5"
    assert rendered["AI_CONTROL_RUN_REQUEST_LIMIT"] == "2"


def test_publication_admission_is_content_addressed_and_fail_closed(
    monkeypatch,
) -> None:
    calls = []

    class Connection:
        def __init__(self, host, port, **_kwargs) -> None:
            assert (host, port) == ("controls.example", 443)

        def request(self, method, path, *, body, headers) -> None:
            calls.append((method, path, json.loads(body), headers))

        def getresponse(self):
            return SimpleNamespace(
                status=200,
                read=lambda _size: (
                    b'{"decision":"admit","reservation_id":"publication-1"}'
                ),
            )

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "scripts.common.operational_controls.http.client.HTTPSConnection",
        Connection,
    )
    reservation = request_publication_admission(
        "org/repo",
        "backport:publish",
        "run:123:attempt:1",
        OperationalPolicy(max_publications_per_day=7),
        upstream_url="https://controls.example",
        token="control-token",
    )

    assert reservation == "publication-1"
    method, path, payload, headers = calls[0]
    assert (method, path) == ("POST", "/v1/controls/admit")
    assert payload["path"] == "/v1/publications"
    assert payload["policy"]["max_publications_per_day"] == 7
    assert len(payload["request_sha256"]) == 64
    assert headers["Authorization"] == "Bearer control-token"


def test_publication_admission_rejects_denial(monkeypatch) -> None:
    class Connection:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def request(self, *_args, **_kwargs) -> None:
            pass

        def getresponse(self):
            return SimpleNamespace(status=429, read=lambda _size: b"denied")

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "scripts.common.operational_controls.http.client.HTTPSConnection",
        Connection,
    )
    with pytest.raises(RuntimeError, match="denied"):
        request_publication_admission(
            "org/repo",
            "ci-fix:publish",
            "run:123",
            OperationalPolicy(),
            upstream_url="https://controls.example",
            token="control-token",
        )
