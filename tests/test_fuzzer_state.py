"""Tests for the fuzzer monitor's durable high-water issue."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.fuzzer.state import (
    STATE_MARKER,
    STATE_NAMESPACE,
    FuzzerStateStore,
)


def _state_issue(cursor: int, *, number: int = 7) -> MagicMock:
    return MagicMock(
        number=number,
        body=(
            f"{STATE_MARKER}\n"
            f"<!-- {STATE_NAMESPACE}:cursor:{cursor} -->"
        ),
    )


def test_reads_zero_when_state_has_not_been_created():
    gh = MagicMock()
    gh.search_issues.return_value = []
    assert FuzzerStateStore(gh).read("o/r").cursor == 0


def test_reads_exact_cursor_marker():
    gh = MagicMock()
    gh.search_issues.return_value = [_state_issue(123)]
    state = FuzzerStateStore(gh).read("o/r")
    assert state.cursor == 123
    assert state.issue_number == 7


def test_rejects_ambiguous_state_issues():
    gh = MagicMock()
    gh.search_issues.return_value = [_state_issue(1), _state_issue(2, number=8)]
    with pytest.raises(RuntimeError, match="expected exactly one"):
        FuzzerStateStore(gh).read("o/r")


def test_advance_is_compare_and_set():
    gh = MagicMock()
    gh.search_issues.return_value = [_state_issue(123)]
    with pytest.raises(RuntimeError, match="changed"):
        FuzzerStateStore(gh).advance(
            "o/r",
            expected_cursor=122,
            run_id=124,
            run_url="https://github.com/o/r/actions/runs/124",
            analysis_sha256="a" * 64,
        )
    gh.get_repo.assert_not_called()


def test_advance_updates_existing_state_after_cursor_check():
    search_issue = _state_issue(123)
    mutable_issue = MagicMock()
    repo = MagicMock()
    repo.get_issue.return_value = mutable_issue
    gh = MagicMock()
    gh.search_issues.return_value = [search_issue]
    gh.get_repo.return_value = repo

    state = FuzzerStateStore(gh).advance(
        "o/r",
        expected_cursor=123,
        run_id=124,
        run_url="https://github.com/o/r/actions/runs/124",
        analysis_sha256="a" * 64,
    )
    assert state.cursor == 124
    body = mutable_issue.edit.call_args.kwargs["body"]
    assert f"{STATE_NAMESPACE}:cursor:124" in body
    assert "a" * 64 in body
