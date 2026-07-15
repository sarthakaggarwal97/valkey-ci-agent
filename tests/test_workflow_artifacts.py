"""Tests for bounded, typed workflow artifact retrieval."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import MagicMock
from urllib.error import HTTPError

import pytest

from scripts.common.workflow_artifacts import (
    ArtifactClient,
    ArtifactState,
    _extract_zip_to,
)


def _zip(tmp_path, members):
    path = tmp_path / "input.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return path


def test_extracts_requested_members_to_files(tmp_path):
    archive = _zip(tmp_path, [("file.txt", b"hello"), ("skip.txt", b"no")])
    result = _extract_zip_to(
        archive,
        tmp_path / "out",
        requested={"file.txt"},
        compressed_bytes=archive.stat().st_size,
    )
    assert result.state is ArtifactState.AVAILABLE
    assert [member.name for member in result.members] == ["file.txt"]
    assert result.members[0].path.read_bytes() == b"hello"
    assert not (tmp_path / "out/skip.txt").exists()


@pytest.mark.parametrize("blob", [b"", b"not a zip"])
def test_corrupt_zip_has_typed_state(tmp_path, blob):
    archive = tmp_path / "input.zip"
    archive.write_bytes(blob)
    result = _extract_zip_to(
        archive,
        tmp_path / "out",
        requested=None,
        compressed_bytes=len(blob),
    )
    assert result.state is ArtifactState.CORRUPT


def test_refuses_oversized_member_before_extraction(monkeypatch, tmp_path):
    from scripts.common import workflow_artifacts as artifacts_mod

    monkeypatch.setattr(artifacts_mod, "MAX_MEMBER_BYTES", 512)
    archive = _zip(tmp_path, [("big.bin", b"a" * 1024)])
    result = _extract_zip_to(
        archive,
        tmp_path / "out",
        requested=None,
        compressed_bytes=archive.stat().st_size,
    )
    assert result.state is ArtifactState.OVERSIZED
    assert not (tmp_path / "out/big.bin").exists()


def test_rejects_duplicate_member_names(tmp_path):
    archive = tmp_path / "duplicate.zip"
    with zipfile.ZipFile(archive, "w") as zip_file:
        zip_file.writestr("same.txt", b"one")
        with pytest.warns(UserWarning):
            zip_file.writestr("same.txt", b"two")
    result = _extract_zip_to(
        archive,
        tmp_path / "out",
        requested=None,
        compressed_bytes=archive.stat().st_size,
    )
    assert result.state is ArtifactState.CORRUPT
    assert "duplicate" in result.detail


@pytest.mark.parametrize("name", ["../escape", "/absolute", r"a\\b", "C:/drive"])
def test_rejects_unsafe_member_paths(tmp_path, name):
    archive = _zip(tmp_path, [(name, b"x")])
    result = _extract_zip_to(
        archive,
        tmp_path / "out",
        requested=None,
        compressed_bytes=archive.stat().st_size,
    )
    assert result.state is ArtifactState.CORRUPT


class _Response:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._offset = 0
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size=-1):
        if self._offset >= len(self._payload):
            return b""
        end = len(self._payload) if size < 0 else self._offset + size
        chunk = self._payload[self._offset:end]
        self._offset += len(chunk)
        return chunk


def _zip_bytes(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return buffer.getvalue()


def test_download_keeps_token_off_cross_host_redirects(monkeypatch, tmp_path):
    from scripts.common import workflow_artifacts as artifacts_mod

    captured = {}

    def fake_urlopen(request, timeout=0):
        captured["request"] = request
        return _Response(_zip_bytes([("x", b"y")]))

    monkeypatch.setattr(artifacts_mod, "urlopen", fake_urlopen)
    client = ArtifactClient(MagicMock(), token="secret")
    result = client.download_artifact(
        "o/r",
        1,
        destination=tmp_path / "out",
    )

    assert result.state is ArtifactState.AVAILABLE
    request = captured["request"]
    assert "Authorization" not in request.headers
    assert request.unredirected_hdrs.get("Authorization") == "Bearer secret"


def test_streaming_download_enforces_compressed_cap(monkeypatch, tmp_path):
    from scripts.common import workflow_artifacts as artifacts_mod

    monkeypatch.setattr(artifacts_mod, "MAX_COMPRESSED_BYTES", 10)
    monkeypatch.setattr(
        artifacts_mod,
        "urlopen",
        lambda request, timeout=0: _Response(b"x" * 11),
    )
    result = ArtifactClient(MagicMock(), token="t").download_artifact(
        "o/r",
        1,
        destination=tmp_path / "out",
    )
    assert result.state is ArtifactState.OVERSIZED


def test_download_404_is_not_found(monkeypatch, tmp_path):
    from scripts.common import workflow_artifacts as artifacts_mod

    def not_found(request, timeout=0):
        raise HTTPError(request.full_url, 404, "missing", {}, None)

    monkeypatch.setattr(artifacts_mod, "urlopen", not_found)
    result = ArtifactClient(MagicMock(), token="t").download_run_logs(
        "o/r",
        1,
        destination=tmp_path / "out",
    )
    assert result.state is ArtifactState.NOT_FOUND
    assert result.retryable is False


def test_download_run_logs_extracts_per_step_logs(monkeypatch, tmp_path):
    from scripts.common import workflow_artifacts as artifacts_mod

    payload = _zip_bytes([
        ("1_build.txt", b"make output"),
        ("2_test.txt", b"[err]: NAN score"),
    ])
    captured = {}

    def fake_urlopen(request, timeout=0):
        captured["url"] = request.full_url
        return _Response(payload)

    monkeypatch.setattr(artifacts_mod, "urlopen", fake_urlopen)
    result = ArtifactClient(MagicMock(), token="t").download_run_logs(
        "valkey-io/valkey",
        27559908167,
        destination=tmp_path / "out",
    )
    assert result.state is ArtifactState.AVAILABLE
    assert {item.name for item in result.members} == {
        "1_build.txt",
        "2_test.txt",
    }
    assert captured["url"].endswith("/actions/runs/27559908167/logs")


def test_list_run_artifacts_paginates():
    page_one = [
        {
            "id": index,
            "name": f"artifact-{index}",
            "size_in_bytes": 1,
            "expired": False,
        }
        for index in range(1, 101)
    ]
    page_two = [{
        "id": 101,
        "name": "last",
        "size_in_bytes": 1,
        "expired": False,
    }]
    repo = MagicMock()
    repo._requester.requestJsonAndCheck.side_effect = [
        ({}, {"total_count": 101, "artifacts": page_one}),
        ({}, {"total_count": 101, "artifacts": page_two}),
    ]
    gh = MagicMock()
    gh.get_repo.return_value = repo

    artifacts = ArtifactClient(gh, token="t").list_run_artifacts("o/r", 99)
    assert len(artifacts) == 101
    paths = [
        call.args[1]
        for call in repo._requester.requestJsonAndCheck.call_args_list
    ]
    assert paths[0].endswith("per_page=100&page=1")
    assert paths[1].endswith("per_page=100&page=2")


def test_list_run_artifacts_rejects_malformed_entries():
    repo = MagicMock()
    repo._requester.requestJsonAndCheck.return_value = (
        {},
        {"total_count": 4, "artifacts": [
            {
                "id": 1,
                "name": "good",
                "size_in_bytes": 1,
                "expired": False,
            },
            {"id": 2},
            {"name": "no-id"},
            "not-a-dict",
        ]},
    )
    gh = MagicMock()
    gh.get_repo.return_value = repo
    with pytest.raises(RuntimeError, match="index 1"):
        ArtifactClient(gh, token="t").list_run_artifacts("o/r", 99)


def test_client_requires_token():
    with pytest.raises(ValueError, match="token is required"):
        ArtifactClient(MagicMock(), token="")
