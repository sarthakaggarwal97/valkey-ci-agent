"""Shared test fixtures."""

import pytest

from scripts.common.publish_guard import configure_publish_guard


@pytest.fixture(autouse=True)
def allow_upstream_publish_in_tests(
    monkeypatch: pytest.MonkeyPatch, request
) -> None:
    """Set VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH=1 unless test opts out.

    Also configures publish_guard with the standard protected repos.
    """
    configure_publish_guard({"valkey-io/valkey"})
    if "disable_publish_autouse" in request.keywords:
        return
    monkeypatch.setenv("VALKEY_CI_AGENT_ALLOW_VALKEY_IO_PUBLISH", "1")
