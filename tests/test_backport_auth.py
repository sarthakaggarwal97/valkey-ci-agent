from __future__ import annotations

import argparse
import os

import pytest

from scripts.backport.auth import BACKPORT_GITHUB_TOKEN_ENV, consume_github_token


def test_consume_github_token_removes_credential_from_ambient_env(monkeypatch) -> None:
    monkeypatch.setenv(BACKPORT_GITHUB_TOKEN_ENV, " secret-token ")

    token = consume_github_token(argparse.ArgumentParser())

    assert token == "secret-token"
    assert BACKPORT_GITHUB_TOKEN_ENV not in os.environ


def test_consume_github_token_fails_without_dedicated_env(monkeypatch, capsys) -> None:
    monkeypatch.delenv(BACKPORT_GITHUB_TOKEN_ENV, raising=False)

    with pytest.raises(SystemExit) as raised:
        consume_github_token(argparse.ArgumentParser(prog="backport"))

    assert raised.value.code == 2
    assert BACKPORT_GITHUB_TOKEN_ENV in capsys.readouterr().err
