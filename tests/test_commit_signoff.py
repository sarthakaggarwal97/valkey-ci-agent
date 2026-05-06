"""Tests for DCO signoff helpers."""

from __future__ import annotations

import pytest

from scripts.common.commit_signoff import (
    CommitSigner,
    load_signer_from_env,
    require_dco_signoff_from_env,
)


def test_load_signer_from_env_reads_expected_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CI_BOT_COMMIT_NAME", "Val Key")
    monkeypatch.setenv("CI_BOT_COMMIT_EMAIL", "valkey@example.com")

    signer = load_signer_from_env()

    assert signer == CommitSigner(name="Val Key", email="valkey@example.com")


def test_require_dco_signoff_from_env_parses_truthy_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI_BOT_REQUIRE_DCO_SIGNOFF", "true")

    assert require_dco_signoff_from_env() is True


def test_require_dco_signoff_from_env_defaults_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CI_BOT_REQUIRE_DCO_SIGNOFF", raising=False)

    assert require_dco_signoff_from_env() is False
