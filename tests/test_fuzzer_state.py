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
        body=(f"{STATE_MARKER}\n<!-- {STATE_NAMESPACE}:cursor:{cursor} -->"),
    )


def test_reads_zero_when_state_has_not_been_created():
    gh = MagicMock()
    gh.search_issues.return_value = []

    state = FuzzerStateStore(gh).read("o/r")

    assert state.cursor == 0
    assert state.issue_number is None


def test_reads_exact_cursor_marker():
    gh = MagicMock()
    gh.search_issues.return_value = [_state_issue(123)]

    state = FuzzerStateStore(gh).read("o/r")

    assert state.cursor == 123
    assert state.issue_number == 7


def test_rejects_cursor_without_state_marker():
    issue = MagicMock(
        number=7,
        body=f"<!-- {STATE_NAMESPACE}:cursor:123 -->",
    )

    with pytest.raises(RuntimeError, match="no state marker"):
        FuzzerStateStore._parse_issue(issue)


def test_rejects_ambiguous_state_issues():
    gh = MagicMock()
    gh.search_issues.return_value = [
        _state_issue(1),
        _state_issue(2, number=8),
    ]

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
            result_sha256="a" * 64,
        )

    gh.get_repo.assert_not_called()


def test_advance_creates_initial_state_issue():
    created = MagicMock(number=9)
    repo = MagicMock()
    repo.create_issue.return_value = created
    gh = MagicMock()
    gh.search_issues.return_value = []
    gh.get_repo.return_value = repo

    state = FuzzerStateStore(gh).advance(
        "o/r",
        expected_cursor=0,
        run_id=124,
        run_url="https://github.com/o/r/actions/runs/124",
        result_sha256="a" * 64,
    )

    assert state.cursor == 124
    assert state.issue_number == 9
    body = repo.create_issue.call_args.kwargs["body"]
    assert f"{STATE_NAMESPACE}:cursor:124" in body
    assert "a" * 64 in body


def test_initialize_anchors_immediately_before_first_run():
    repo = MagicMock()
    repo.create_issue.return_value = MagicMock(number=9)
    gh = MagicMock()
    gh.search_issues.return_value = []
    gh.get_repo.return_value = repo

    state = FuzzerStateStore(gh).initialize(
        "o/r",
        first_run_id=124,
        first_run_url="https://github.com/o/r/actions/runs/124",
    )

    assert state.cursor == 123
    body = repo.create_issue.call_args.kwargs["body"]
    assert f"{STATE_NAMESPACE}:cursor:123" in body
    assert "Bootstrap pending run" in body


def test_advance_updates_existing_state_after_cursor_check():
    mutable_issue = _state_issue(123)
    repo = MagicMock()
    repo.get_issue.return_value = mutable_issue
    gh = MagicMock()
    gh.search_issues.return_value = [_state_issue(123)]
    gh.get_repo.return_value = repo

    state = FuzzerStateStore(gh).advance(
        "o/r",
        expected_cursor=123,
        run_id=124,
        run_url="https://github.com/o/r/actions/runs/124",
        result_sha256="b" * 64,
    )

    assert state.cursor == 124
    body = mutable_issue.edit.call_args.kwargs["body"]
    assert f"{STATE_NAMESPACE}:cursor:124" in body
    assert "b" * 64 in body


def test_second_advance_reads_known_issue_directly():
    current_body = f"{STATE_MARKER}\n<!-- {STATE_NAMESPACE}:cursor:100 -->"
    issue = MagicMock(number=7, body=current_body)

    def edit_issue(*, title, body, state):
        issue.body = body

    issue.edit.side_effect = edit_issue
    repo = MagicMock()
    repo.get_issue.return_value = issue
    gh = MagicMock()
    gh.search_issues.return_value = [issue]
    gh.get_repo.return_value = repo
    store = FuzzerStateStore(gh)
    assert store.read("o/r").cursor == 100

    store.advance(
        "o/r",
        expected_cursor=100,
        run_id=101,
        run_url="https://github.com/o/r/actions/runs/101",
        result_sha256="a" * 64,
    )
    store.advance(
        "o/r",
        expected_cursor=101,
        run_id=102,
        run_url="https://github.com/o/r/actions/runs/102",
        result_sha256="b" * 64,
    )

    assert gh.search_issues.call_count == 1
    assert repo.get_issue.call_count == 2
    assert f"{STATE_NAMESPACE}:cursor:102" in issue.body


@pytest.mark.parametrize(
    "expected_cursor,run_id,run_url,digest",
    [
        (0, 1, "not-a-url", "a" * 64),
        (0, 1, "https://github.com/o/r/actions/runs/1\ninjected", "a" * 64),
        (0, 1, "https://github.com/o/r/actions/runs/1", "short"),
        (False, 1, "https://github.com/o/r/actions/runs/1", "a" * 64),
        (0, True, "https://github.com/o/r/actions/runs/1", "a" * 64),
    ],
)
def test_advance_rejects_invalid_state_metadata(
    expected_cursor, run_id, run_url, digest
):
    with pytest.raises(ValueError):
        FuzzerStateStore(MagicMock()).advance(
            "o/r",
            expected_cursor=expected_cursor,
            run_id=run_id,
            run_url=run_url,
            result_sha256=digest,
        )
