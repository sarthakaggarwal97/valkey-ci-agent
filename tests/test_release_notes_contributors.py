"""Tests for source-PR-author contributor attribution."""

from __future__ import annotations

import urllib.error

from scripts.release_notes import contributors as contrib


def test_resolves_profile_names_and_sorts(monkeypatch) -> None:
    names = {"zoe": "Zoe Q", "amy": "Amy P"}
    monkeypatch.setattr(contrib, "_display_name", lambda login, token: names.get(login))

    assert contrib.list_contributors(["zoe", "amy"], token="t") == [
        "Amy P @amy",
        "Zoe Q @zoe",
    ]


def test_login_is_stable_fallback_when_profile_name_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(contrib, "_display_name", lambda login, token: None)

    assert contrib.list_contributors(["ghost"]) == ["ghost @ghost"]


def test_deduplicates_logins_case_insensitively(monkeypatch) -> None:
    monkeypatch.setattr(contrib, "_display_name", lambda login, token: "Alice")

    assert contrib.list_contributors(["AliceDev", "alicedev"]) == ["Alice @AliceDev"]


def test_filters_bots_and_empty_values(monkeypatch) -> None:
    monkeypatch.setattr(contrib, "_display_name", lambda login, token: login)

    assert contrib.list_contributors(["", "valkeyrie-ops[bot]", "human"]) == ["human @human"]


def test_empty_original_author_list_does_not_guess_from_commits() -> None:
    assert contrib.list_contributors([]) == []


def test_profile_lookup_failure_keeps_original_login(monkeypatch) -> None:
    def _offline(url, token, label):
        raise urllib.error.URLError("offline")

    monkeypatch.setattr(contrib, "_api_get_with_retry", _offline)

    assert contrib.list_contributors(["source-author"]) == ["source-author @source-author"]


def test_transient_profile_lookup_is_retried(monkeypatch) -> None:
    calls = {"count": 0}

    def _flaky(url, token):
        calls["count"] += 1
        if calls["count"] == 1:
            raise urllib.error.HTTPError(url, 503, "busy", {}, None)
        return {"name": "Recovered Name"}

    monkeypatch.setattr(contrib, "_api_get", _flaky)
    monkeypatch.setattr(contrib.time, "sleep", lambda delay: None)

    assert contrib.list_contributors(["recovered"]) == ["Recovered Name @recovered"]
    assert calls["count"] == 2
