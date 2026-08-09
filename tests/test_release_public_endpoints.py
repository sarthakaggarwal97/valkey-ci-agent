"""Tests for anonymous public-endpoint verification.

Mocked at the HTTP boundary — ``urllib.request.urlopen``, the module's HTTP
layer (it uses urllib, not requests) — so no real network is ever touched.
These pin the URL contracts against each registry and the
absent/transient/present decision logic.
"""

from __future__ import annotations

import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from scripts.release import public_endpoints as pub


def _response(status: int = 200, body: bytes = b"") -> MagicMock:
    response = MagicMock()
    response.status = status
    response.read.return_value = body
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "err", None, None)


def _token_response(token: str = "tok") -> MagicMock:
    return _response(200, json.dumps({"token": token}).encode("utf-8"))


class TestUrlExists:
    def test_200_is_true_and_uses_head_by_default(self) -> None:
        with patch("urllib.request.urlopen", return_value=_response(200)) as opened:
            assert pub.url_exists("https://x/file")
        request = opened.call_args.args[0]
        assert request.get_method() == "HEAD"
        assert request.full_url == "https://x/file"

    def test_404_is_false(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(404)):
            assert not pub.url_exists("https://x/file")

    def test_405_raises_as_transient(self) -> None:
        # An endpoint rejecting the HEAD method is not a missing artifact.
        with patch("urllib.request.urlopen", side_effect=_http_error(405)), \
             pytest.raises(urllib.error.HTTPError):
            pub.url_exists("https://x/file")

    def test_429_raises_as_transient(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(429)), \
             pytest.raises(urllib.error.HTTPError):
            pub.url_exists("https://x/file")

    def test_5xx_raises(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(503)), \
             pytest.raises(urllib.error.HTTPError):
            pub.url_exists("https://x/file")


class TestDockerhub:
    def test_found_builds_the_tags_url_with_get(self) -> None:
        with patch("urllib.request.urlopen", return_value=_response(200)) as opened:
            assert pub.dockerhub_tag_exists("valkey/valkey", "9.1.1")
        request = opened.call_args.args[0]
        assert request.full_url == \
            "https://hub.docker.com/v2/repositories/valkey/valkey/tags/9.1.1"
        assert request.get_method() == "GET"

    def test_not_found_is_false(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(404)):
            assert not pub.dockerhub_tag_exists("valkey/valkey", "9.1.1")


class TestGhcr:
    def test_found_fetches_anonymous_token_then_manifest(self) -> None:
        with patch("urllib.request.urlopen",
                   side_effect=[_token_response(), _response(200)]) as opened:
            assert pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")
        token_request, manifest_request = (c.args[0] for c in opened.call_args_list)
        assert token_request.full_url == \
            "https://ghcr.io/token?scope=repository:valkey-io/valkey:pull"
        assert manifest_request.full_url == \
            "https://ghcr.io/v2/valkey-io/valkey/manifests/9.1.1"
        assert manifest_request.get_header("Authorization") == "Bearer tok"

    def test_missing_manifest_is_false(self) -> None:
        with patch("urllib.request.urlopen",
                   side_effect=[_token_response(), _http_error(404)]):
            assert not pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")

    def test_denied_token_grant_reads_as_not_public(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(403)):
            assert not pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")


class TestEcrPublic:
    def test_found_fetches_anonymous_token_then_manifest(self) -> None:
        with patch("urllib.request.urlopen",
                   side_effect=[_token_response(), _response(200)]) as opened:
            assert pub.ecr_public_tag_exists("valkey/valkey", "9.1.1")
        token_request, manifest_request = (c.args[0] for c in opened.call_args_list)
        assert token_request.full_url == "https://public.ecr.aws/token/"
        assert manifest_request.full_url == \
            "https://public.ecr.aws/v2/valkey/valkey/manifests/9.1.1"
        assert manifest_request.get_header("Authorization") == "Bearer tok"

    def test_missing_manifest_is_false(self) -> None:
        with patch("urllib.request.urlopen",
                   side_effect=[_token_response(), _http_error(404)]):
            assert not pub.ecr_public_tag_exists("valkey/valkey", "9.1.1")


class TestFetchText:
    def test_returns_the_decoded_body(self) -> None:
        body = "entries:\n  valkey:\n  - version: 0.12.0\n".encode("utf-8")
        with patch("urllib.request.urlopen", return_value=_response(200, body)) as opened:
            text = pub.fetch_text("https://valkey.io/valkey-helm/index.yaml")
        assert text == "entries:\n  valkey:\n  - version: 0.12.0\n"
        assert opened.call_args.args[0].full_url == \
            "https://valkey.io/valkey-helm/index.yaml"
