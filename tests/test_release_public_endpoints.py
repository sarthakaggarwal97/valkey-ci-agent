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

    @pytest.mark.parametrize("code", [
        405,  # endpoint rejecting the HEAD method is not a missing artifact
        429,  # rate limiting is not absence
        500,
        503,
    ])
    def test_transient_codes_raise_instead_of_reporting_absence(self, code: int) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(code)), \
             pytest.raises(urllib.error.HTTPError):
            pub.url_exists("https://x/file")

    @pytest.mark.parametrize("status, exists", [
        (204, True),   # any 2xx is presence
        (302, False),  # an unfollowed redirect must not read as presence
    ])
    def test_non_200_statuses_resolve_by_the_2xx_boundary(
            self, status: int, exists: bool) -> None:
        with patch("urllib.request.urlopen", return_value=_response(status)):
            assert pub.url_exists("https://x/file") is exists


class TestDockerhub:
    def test_found_builds_the_tags_url_with_get(self) -> None:
        with patch("urllib.request.urlopen", return_value=_response(200)) as opened:
            assert pub.dockerhub_tag_exists("valkey/valkey", "9.1.1")
        request = opened.call_args.args[0]
        assert request.full_url == \
            "https://hub.docker.com/v2/repositories/valkey/valkey/tags/9.1.1"
        assert request.get_method() == "GET"


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

    def test_denied_token_grant_reads_as_not_public(self) -> None:
        with patch("urllib.request.urlopen", side_effect=_http_error(403)):
            assert not pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")

    @pytest.mark.parametrize("body", [
        b"{}",                                 # 200 with no token key
        b'{"errors": [{"code": "DENIED"}]}',   # 200 with an error-shaped body
        b'{"token": null}',                    # 200 with a null token
        b'{"token": ""}',                      # 200 with an empty token
        b'{"token": 12345}',                   # 200 with a numeric token
    ])
    def test_hostile_token_body_never_reads_as_public(self, body: bytes) -> None:
        # A 200 token response without a usable token must never turn into
        # "tag exists". F26: every deviation surfaces as ValueError so the
        # caller's _guarded degradation catches it, instead of a raw
        # KeyError (missing 'token') or TypeError (non-string coerced into
        # a header) or a silently-empty Authorization header.
        with patch("urllib.request.urlopen",
                   return_value=_response(200, body)), \
             pytest.raises(ValueError):
            pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")

    def test_non_object_json_body_is_refused_by_shape_check(self) -> None:
        # A registry answering 200 with a JSON list or string (broken proxy
        # rewriting, API drift) would raise TypeError on subscript access
        # before F26; must now surface as a clear ValueError.
        with patch("urllib.request.urlopen",
                   return_value=_response(200, b'["not", "an", "object"]')), \
             pytest.raises(ValueError, match="not a JSON object"):
            pub.ghcr_tag_exists("valkey-io/valkey", "9.1.1")

    def test_ecr_shape_check_applies_uniformly(self) -> None:
        # Same shape-check discipline as ghcr; F26 factored both through
        # _registry_token so a hostile ECR grant fails just as loudly.
        with patch("urllib.request.urlopen",
                   return_value=_response(200, b'{"unrelated": true}')), \
             pytest.raises(ValueError, match="missing the 'token' field"):
            pub.ecr_public_tag_exists("valkey/valkey", "9.1.1")


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


class TestFetchText:
    def test_returns_the_decoded_body(self) -> None:
        body = "entries:\n  valkey:\n  - version: 0.12.0\n".encode("utf-8")
        with patch("urllib.request.urlopen", return_value=_response(200, body)) as opened:
            text = pub.fetch_text("https://valkey.io/valkey-helm/index.yaml")
        assert text == "entries:\n  valkey:\n  - version: 0.12.0\n"
        assert opened.call_args.args[0].full_url == \
            "https://valkey.io/valkey-helm/index.yaml"

    def test_invalid_utf8_body_degrades_instead_of_crashing(self) -> None:
        # A corrupt or truncated index must never crash the reconcile; the
        # replacement characters simply fail the version-entry regex.
        body = b"\xff\xfeversion: 0.12.0\n"
        with patch("urllib.request.urlopen", return_value=_response(200, body)):
            text = pub.fetch_text("https://valkey.io/valkey-helm/index.yaml")
        assert "version: 0.12.0" in text
        assert "\ufffd" in text
