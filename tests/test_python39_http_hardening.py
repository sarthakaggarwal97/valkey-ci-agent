from __future__ import annotations

import os
import sys
import tempfile
import zipfile
import zlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import requests.utils
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry

from scripts.common import python39_http_hardening as hardening


def test_requests_zip_extraction_uses_an_unpredictable_new_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "bundle.zip"
    with zipfile.ZipFile(archive, "w") as value:
        value.writestr("certs/ca.pem", b"trusted")

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    predictable = Path(os.path.join(tempfile.gettempdir(), "ca.pem"))
    predictable.write_bytes(b"attacker")
    try:
        extracted = Path(
            requests.utils.extract_zipped_paths(f"{archive}/certs/ca.pem"),
        )
    finally:
        predictable.unlink(missing_ok=True)

    try:
        assert extracted != predictable
        assert extracted.read_bytes() == b"trusted"
    finally:
        extracted.unlink(missing_ok=True)


def test_urllib3_drain_discards_raw_data_without_decompressing() -> None:
    response = HTTPResponse(
        BytesIO(zlib.compress(b"x" * 100_000)),
        headers={"content-encoding": "deflate"},
        preload_content=False,
    )
    assert response.read(8, decode_content=True) == b"x" * 8

    response.drain_conn()

    if sys.version_info < (3, 10):
        assert response._decoder is None
        assert len(response._decoded_buffer) == 0


def test_low_level_cross_origin_redirect_strips_sensitive_headers() -> None:
    pool = SimpleNamespace(is_same_host=lambda url: url.startswith("https://one/"))
    headers = {
        "Authorization": "secret",
        "Cookie": "session=secret",
        "Proxy-Authorization": "proxy-secret",
        "X-Trace": "keep",
    }

    hardened = hardening._redirect_headers(
        pool,
        "https://one/start",
        "https://two/finish",
        headers,
        Retry(),
        redirect=True,
        assert_same_host=False,
    )

    assert hardened == {"X-Trace": "keep"}
    assert headers["Authorization"] == "secret"


def test_urlopen_wrapper_hardens_recursive_redirect_call() -> None:
    calls = []

    class Pool:
        def is_same_host(self, url):
            return url.startswith("https://one/")

    def original(
        self,
        method,
        url,
        body=None,
        headers=None,
        retries=None,
        redirect=True,
        assert_same_host=True,
        **kwargs,
    ):
        calls.append((url, dict(headers or {})))
        if url == "https://one/start":
            return self.urlopen(
                method,
                "https://two/finish",
                body,
                headers,
                Retry(),
                redirect,
                assert_same_host,
                **kwargs,
            )
        return "done"

    Pool.urlopen = hardening._harden_urlopen(original)
    pool = Pool()
    result = pool.urlopen(
        "GET",
        "https://one/start",
        headers={"Authorization": "secret", "X-Trace": "keep"},
        redirect=True,
        assert_same_host=False,
    )

    assert result == "done"
    assert calls == [
        (
            "https://one/start",
            {"Authorization": "secret", "X-Trace": "keep"},
        ),
        ("https://two/finish", {"X-Trace": "keep"}),
    ]


def test_python39_runtime_applies_guarded_dependency_patches() -> None:
    if sys.version_info < (3, 10):
        assert hardening._applied is True
        assert requests.utils.extract_zipped_paths is hardening._extract_zipped_paths
        assert HTTPResponse.drain_conn is hardening._drain_conn
