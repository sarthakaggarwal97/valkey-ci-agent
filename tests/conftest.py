"""Shared test fixtures."""

import pytest


@pytest.fixture(autouse=True)
def allow_upstream_publish_in_tests(
    monkeypatch: pytest.MonkeyPatch, request
) -> None:
    """Set VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH=1 unless test opts out."""
    if "disable_publish_autouse" in request.keywords:
        return
    monkeypatch.setenv("VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH", "1")
