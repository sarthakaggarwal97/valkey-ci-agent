"""Tests for the shared atomic JSON writer."""

from __future__ import annotations

import json
import os

from scripts.common.atomic_json import write_json_atomic


def test_writes_private_json_and_replaces_the_old_file(tmp_path):
    path = tmp_path / "state" / "state.json"
    write_json_atomic(path, {"b": 1, "a": [1, 2]})
    write_json_atomic(path, {"v": 2})
    assert json.loads(path.read_text()) == {"v": 2}
    assert path.read_text().endswith("\n")
    assert os.stat(path).st_mode & 0o777 == 0o600
    assert [p.name for p in path.parent.iterdir()] == ["state.json"]


def test_a_failed_write_leaves_the_old_file_and_no_temporary(tmp_path):
    path = tmp_path / "state.json"
    write_json_atomic(path, {"v": 1})
    try:
        write_json_atomic(path, {"bad": object()})
    except TypeError:
        pass
    assert json.loads(path.read_text()) == {"v": 1}
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]
