"""Backport PYSEC-2026-2275, PYSEC-2026-141, and PYSEC-2026-142 for Python 3.9."""

from __future__ import annotations

import functools
import os
import sys
import tempfile
import threading
import zipfile
from http.client import HTTPException
from typing import Any, Mapping

_REQUESTS_VERSION = "2.32.5"
_URLLIB3_VERSION = "2.6.3"
_redirect_state = threading.local()
_applied = False


def apply_python39_http_hardening() -> None:
    """Apply reviewed upstream fixes unavailable in Python 3.9 package releases."""
    global _applied
    if _applied or sys.version_info >= (3, 10):
        return

    import requests
    import requests.utils
    import urllib3
    import urllib3.connectionpool
    import urllib3.response

    if requests.__version__ != _REQUESTS_VERSION:
        raise RuntimeError(
            f"Python 3.9 HTTP hardening expects requests {_REQUESTS_VERSION}, "
            f"found {requests.__version__}",
        )
    if urllib3.__version__ != _URLLIB3_VERSION:
        raise RuntimeError(
            f"Python 3.9 HTTP hardening expects urllib3 {_URLLIB3_VERSION}, "
            f"found {urllib3.__version__}",
        )
    brotli = getattr(urllib3.response, "brotli", None)
    if brotli is not None and getattr(brotli, "__name__", "") == "brotli":
        raise RuntimeError(
            "Python 3.9 HTTP hardening requires brotlicffi or no Brotli decoder",
        )

    setattr(requests.utils, "extract_zipped_paths", _extract_zipped_paths)
    setattr(urllib3.response.HTTPResponse, "drain_conn", _drain_conn)
    setattr(
        urllib3.connectionpool.HTTPConnectionPool,
        "urlopen",
        _harden_urlopen(urllib3.connectionpool.HTTPConnectionPool.urlopen),
    )
    _applied = True


def _extract_zipped_paths(path: str) -> str:
    """Requests 2.33.0's unpredictable extraction fix, kept Python 3.9-compatible."""
    if os.path.exists(path):
        return path

    archive, member = os.path.split(path)
    while archive and not os.path.exists(archive):
        archive, prefix = os.path.split(archive)
        if not prefix:
            break
        member = "/".join([prefix, member])

    if not zipfile.is_zipfile(archive):
        return path

    with zipfile.ZipFile(archive) as zip_file:
        if member not in zip_file.namelist():
            return path
        content = zip_file.read(member)

    suffix = os.path.splitext(member.split("/")[-1])[-1]
    descriptor, extracted_path = tempfile.mkstemp(suffix=suffix)
    try:
        os.write(descriptor, content)
    finally:
        os.close(descriptor)
    return extracted_path


def _drain_conn(self: Any) -> None:
    """urllib3 2.7.0's drain-without-decompression security fix."""
    from urllib3.connection import BaseSSLError
    from urllib3.exceptions import HTTPError
    from urllib3.response import BytesQueueBuffer

    try:
        self._raw_read()
    except (HTTPError, OSError, BaseSSLError, HTTPException):
        pass
    if self._has_decoded_content:
        self._decoded_buffer = BytesQueueBuffer()
        self._decoder = None


def _redirect_headers(
    pool: Any,
    previous_url: str | None,
    url: str,
    headers: Mapping[str, str] | None,
    retries: Any,
    *,
    redirect: bool,
    assert_same_host: bool,
) -> Mapping[str, str] | None:
    """Strip sensitive headers on low-level cross-origin redirect recursion."""
    if (
        previous_url is None
        or previous_url == url
        or not redirect
        or assert_same_host
        or headers is None
        or pool.is_same_host(url)
    ):
        return headers

    from urllib3.util.retry import Retry

    remove = getattr(
        retries,
        "remove_headers_on_redirect",
        Retry.DEFAULT_REMOVE_HEADERS_ON_REDIRECT,
    )
    copy_headers = getattr(headers, "copy", None)
    hardened: Any = copy_headers() if callable(copy_headers) else dict(headers)
    for header in tuple(headers):
        if header.lower() in remove:
            hardened.pop(header, None)
    return hardened


def _harden_urlopen(original: Any) -> Any:
    @functools.wraps(original)
    def urlopen(
        self: Any,
        method: str,
        url: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        positional = list(args)
        headers = (
            positional[1]
            if len(positional) > 1
            else kwargs.get("headers")
        )
        retries = (
            positional[2]
            if len(positional) > 2
            else kwargs.get("retries")
        )
        redirect = (
            positional[3]
            if len(positional) > 3
            else kwargs.get("redirect", True)
        )
        assert_same_host = (
            positional[4]
            if len(positional) > 4
            else kwargs.get("assert_same_host", True)
        )
        stack = getattr(_redirect_state, "urls", None)
        if stack is None:
            stack = []
            _redirect_state.urls = stack
        previous_url = stack[-1] if stack else None
        headers = _redirect_headers(
            self,
            previous_url,
            url,
            headers,
            retries,
            redirect=redirect,
            assert_same_host=assert_same_host,
        )
        if len(positional) > 1:
            positional[1] = headers
        else:
            kwargs["headers"] = headers
        stack.append(url)
        try:
            return original(self, method, url, *positional, **kwargs)
        finally:
            stack.pop()

    return urlopen
