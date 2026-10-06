from __future__ import annotations

import logging

import pytest
from github.GithubException import GithubException

from scripts.release import main as main_mod
from scripts.release.models import ReleasePolicy

POLICY = ReleasePolicy(
    repo="valkey-io/valkey",
    authorized_teams=("valkey-io/core-team",),
    branches=("9.1",),
    checks_workflow="ci.yml",
    required_checks=("test",),
)


def test_github_api_failure_is_a_named_cli_refusal(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(main_mod, "load_policy", lambda path: POLICY)
    monkeypatch.setattr(
        main_mod,
        "prepare_release",
        lambda *args, **kwargs: (_ for _ in ()).throw(GithubException(401, "bad credentials")),
    )

    with caplog.at_level(logging.ERROR):
        result = main_mod.main(
            [
                "--token",
                "invalid",
                "prepare",
                "--branch",
                "9.1",
                "--intent",
                "rc",
                "--actor",
                "maintainer",
            ]
        )

    assert result == 1
    assert "bad credentials" in caplog.text


def test_connection_failure_is_a_named_cli_refusal(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(main_mod, "load_policy", lambda path: POLICY)
    monkeypatch.setattr(
        main_mod,
        "prepare_release",
        lambda *args, **kwargs: (_ for _ in ()).throw(ConnectionError("offline")),
    )

    with caplog.at_level(logging.ERROR):
        result = main_mod.main(
            [
                "--token",
                "invalid",
                "prepare",
                "--branch",
                "9.1",
                "--intent",
                "rc",
                "--actor",
                "maintainer",
            ]
        )

    assert result == 1
    assert "offline" in caplog.text


def test_refusal_is_pinned_to_the_run_page(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(main_mod, "load_policy", lambda path: POLICY)
    monkeypatch.setattr(
        main_mod, "prepare_release",
        lambda *a, **k: (_ for _ in ()).throw(main_mod.ReleaseError("line\n::error::forged")),
    )
    assert main_mod.main(["--token", "t", "prepare", "--branch", "9.1", "--intent", "rc",
                          "--actor", "maintainer"]) == 1
    err = capsys.readouterr().err.splitlines()
    # One escaped annotation: a newline in the reason cannot open a second command.
    assert "::error::Release prepare refused: line ::error::forged" in err
