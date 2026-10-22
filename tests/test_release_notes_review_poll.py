from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.release_notes.review import (
    ReviewComment,
    ReviewRefused,
    ReviewThread,
    SelectedReview,
    parse_status,
    review_batch_id,
    review_targets,
    status_body,
    validate_release_pr,
)
from scripts.release_notes.review_poll import (
    dispatch_release_review,
    ensure_watcher,
    live_watcher_runs,
    poll_once,
    poll_pr,
    stop_watcher,
)
from scripts.release_notes.review_poll import (
    main as poll_main,
)

_BOT = "valkeyrie-ops[bot]"
_HEAD = "a" * 40


def _batch_id(thread_id: str = "thread-one", comment_id: int = 1) -> str:
    thread = _thread(thread_id, comment_id)
    return review_batch_id((SelectedReview(thread, thread.comments[0]),))


class StatusComment:
    def __init__(
        self,
        body: str,
        comment_id: int = 99,
        *,
        updated_at: datetime | None = None,
    ) -> None:
        self.id = comment_id
        self.body = body
        self.user = SimpleNamespace(login=_BOT)
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def edit(self, body: str) -> None:
        self.body = body


def _comment(comment_id: int, login: str = "alice") -> ReviewComment:
    return ReviewComment(
        database_id=comment_id,
        body="Please improve this wording.",
        created_at=f"2026-08-21T00:00:{comment_id:02d}Z",
        diff_hunk="@@ -10 +10 @@",
        author_login=login,
        author_type="User",
    )


def _thread(thread_id: str, comment_id: int, login: str = "alice") -> ReviewThread:
    return ReviewThread(
        node_id=thread_id,
        resolved=False,
        outdated=False,
        path="00-RELEASENOTES",
        line=20,
        comments=(_comment(comment_id, login),),
    )


def _pr(issue_comments: list[StatusComment] | None = None) -> SimpleNamespace:
    comments = issue_comments or []

    def create_issue_comment(body: str) -> StatusComment:
        comment = StatusComment(body, 100 + len(comments))
        comments.append(comment)
        return comment

    return SimpleNamespace(
        number=42,
        state="open",
        user=SimpleNamespace(login=_BOT),
        head=SimpleNamespace(
            repo=SimpleNamespace(full_name="valkey-io/valkey"),
            ref="agent/release-cut/9.1.1-ga",
            sha=_HEAD,
        ),
        base=SimpleNamespace(ref="9.1"),
        get_files=lambda: [
            SimpleNamespace(filename="00-RELEASENOTES", status="modified"),
            SimpleNamespace(filename="src/version.h", status="modified"),
        ],
        get_issue_comments=lambda: list(comments),
        create_issue_comment=create_issue_comment,
        issue_comments=comments,
    )


def _github(pr: SimpleNamespace) -> MagicMock:
    gh = MagicMock()
    gh.get_repo.return_value = SimpleNamespace(
        get_pulls=lambda **_kwargs: [pr],
    )
    return gh


def _poll(
    monkeypatch: pytest.MonkeyPatch,
    pr: SimpleNamespace,
    threads: tuple[ReviewThread, ...],
    dispatch: MagicMock,
) -> int:
    monkeypatch.setattr(
        "scripts.release_notes.review_poll.list_review_threads",
        lambda *_args: threads,
    )
    return poll_once(
        _github(pr),
        MagicMock(),
        repositories=("valkey-io/valkey",),
        bot_login=_BOT,
        dispatch=dispatch,
        authorize=lambda login: login in {"alice", "bob"},
    )


def test_poller_batches_every_actionable_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _pr()
    dispatch = MagicMock()

    count = _poll(
        monkeypatch,
        pr,
        (_thread("thread-two", 2, "bob"), _thread("thread-one", 1)),
        dispatch,
    )

    assert count == 1
    release, status_id = dispatch.call_args.args
    assert release.number == 42
    assert status_id == 100
    assert pr.issue_comments[0].body.startswith(
        "Addressing 2 release-note review comments."
    )


@pytest.mark.parametrize("marker_status", ["addressing", "addressed", "refused"])
def test_existing_status_blocks_same_head(
    monkeypatch: pytest.MonkeyPatch,
    marker_status: str,
) -> None:
    kwargs = (
        {
            "commit_sha": "c" * 40,
            "repo_full_name": "valkey-io/valkey",
        }
        if marker_status == "addressed"
        else {"reason": "test"}
        if marker_status == "refused"
        else {}
    )
    pr = _pr(
        [
            StatusComment(
                status_body(
                    marker_status,
                    _HEAD,
                    _batch_id(),
                    1,
                    **kwargs,
                )
            )
        ]
    )
    dispatch = MagicMock()

    assert _poll(
        monkeypatch,
        pr,
        (_thread("thread-one", 1),),
        dispatch,
    ) == 0
    dispatch.assert_not_called()


def test_failed_status_is_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    pr = _pr(
        [
            StatusComment(
                status_body(
                    "failed",
                    _HEAD,
                    _batch_id(),
                    1,
                    reason="temporary",
                )
            )
        ]
    )
    dispatch = MagicMock()

    assert _poll(
        monkeypatch,
        pr,
        (_thread("thread-one", 1),),
        dispatch,
    ) == 1
    dispatch.assert_called_once()


def test_expired_addressing_status_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _pr(
        [
            StatusComment(
                status_body("addressing", _HEAD, _batch_id(), 1),
                updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]
    )
    dispatch = MagicMock()

    assert _poll(
        monkeypatch,
        pr,
        (_thread("thread-one", 1),),
        dispatch,
    ) == 1
    dispatch.assert_called_once()


def test_poller_recovers_bookkeeping_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thread = _thread("thread-one", 1)
    selected = (SelectedReview(thread, thread.comments[0]),)
    status = StatusComment(
        status_body(
            "addressing",
            "b" * 40,
            review_batch_id(selected),
            1,
            commit_sha=_HEAD,
            targets=review_targets(selected),
        )
    )
    pr = _pr([status])
    reconcile = MagicMock(return_value=(1, 1, []))
    monkeypatch.setattr(
        "scripts.release_notes.review_poll.reconcile_review_targets",
        reconcile,
    )

    assert _poll(monkeypatch, pr, (thread,), MagicMock()) == 0
    marker = parse_status(status, _BOT)
    assert marker is not None
    assert marker.status == "addressed"
    assert marker.head_sha == marker.commit_sha == _HEAD
    reconcile.assert_called_once()


def test_refusal_does_not_block_a_new_comment_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refused = StatusComment(
        status_body(
            "refused",
            _HEAD,
            _batch_id(),
            1,
            reason="no valid edit",
        )
    )
    pr = _pr([refused])
    dispatch = MagicMock()

    assert _poll(
        monkeypatch,
        pr,
        (_thread("thread-one", 2),),
        dispatch,
    ) == 1
    dispatch.assert_called_once()


def test_dispatch_failure_updates_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _pr()
    dispatch = MagicMock(side_effect=RuntimeError("dispatch unavailable"))

    assert _poll(
        monkeypatch,
        pr,
        (_thread("thread-one", 1),),
        dispatch,
    ) == 0
    assert "Failed to address" in pr.issue_comments[0].body
    assert ":failed:" in pr.issue_comments[0].body


def test_dispatcher_passes_only_pr_identity() -> None:
    workflow = MagicMock()
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow.return_value = workflow
    pr = _pr()
    release = validate_release_pr(pr, "valkey-io/valkey")

    dispatch_release_review(gh)(release, 99)

    assert workflow.create_dispatch.call_args.args == (
        "main",
        {
            "repo": "valkey",
            "pr": "42",
            "head_sha": _HEAD,
            "status_comment_id": "99",
        },
    )


def _github_pr(pr: SimpleNamespace) -> MagicMock:
    gh = MagicMock()
    gh.get_repo.return_value = SimpleNamespace(get_pull=lambda number: pr)
    return gh


def _watch(
    monkeypatch: pytest.MonkeyPatch,
    pr: SimpleNamespace,
    threads: tuple[ReviewThread, ...],
    dispatch: MagicMock,
) -> tuple[int, bool]:
    monkeypatch.setattr(
        "scripts.release_notes.review_poll.list_review_threads",
        lambda *_args: threads,
    )
    return poll_pr(
        _github_pr(pr),
        MagicMock(),
        repo_full_name="valkey-io/valkey",
        number=42,
        bot_login=_BOT,
        dispatch=dispatch,
        authorize=lambda login: login in {"alice", "bob"},
    )


def test_watcher_dispatches_for_its_one_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    dispatch = MagicMock()
    pr = _pr()
    dispatched, still_open = _watch(monkeypatch, pr, (_thread("thread-one", 1),), dispatch)
    assert (dispatched, still_open) == (1, True)
    dispatch.assert_called_once()
    assert dispatch.call_args.args[0].number == 42


def test_watcher_ends_when_the_pr_is_merged_or_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    dispatch = MagicMock()
    pr = _pr()
    pr.state = "closed"
    dispatched, still_open = _watch(monkeypatch, pr, (_thread("thread-one", 1),), dispatch)
    assert (dispatched, still_open) == (0, False)
    dispatch.assert_not_called()


def test_watcher_refuses_a_pr_that_is_not_a_release_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pointed at the wrong PR, the watcher must fail where someone sees it,
    # not poll quietly for an hour.
    pr = _pr()
    pr.head.ref = "feature/not-a-release"
    with pytest.raises(ReviewRefused):
        _watch(monkeypatch, pr, (), MagicMock())


def _run(run_id: int, status: str, title: str = "Watch valkey#42 reviews") -> MagicMock:
    run = MagicMock()
    run.id = run_id
    run.status = status
    run.display_title = title
    return run


def _agent(runs: list[MagicMock]) -> tuple[MagicMock, MagicMock]:
    """A Github whose agent repository has the watcher workflow with *runs*."""
    workflow = MagicMock()
    workflow.get_runs.return_value = runs
    gh = MagicMock()
    gh.get_repo.return_value.get_workflow.return_value = workflow
    return gh, workflow


def test_live_watcher_runs_match_this_prs_title_and_live_states() -> None:
    gh, _ = _agent([
        _run(1, "in_progress"),
        _run(2, "pending"),
        _run(3, "completed"),
        _run(4, "in_progress", title="Watch valkey#43 reviews"),
        _run(5, "queued", title="Stop watching valkey#42 reviews"),
    ])
    assert [run.id for run in live_watcher_runs(gh, "valkey", 42)] == [1, 2]


def test_cut_start_skips_a_watcher_that_is_running_or_waiting() -> None:
    gh, workflow = _agent([_run(1, "in_progress")])
    assert ensure_watcher(gh, "valkey", 42, successor=False) is False
    workflow.create_dispatch.assert_not_called()

    gh, workflow = _agent([_run(3, "completed")])
    assert ensure_watcher(gh, "valkey", 42, successor=False) is True
    workflow.create_dispatch.assert_called_once_with("main", {"repo": "valkey", "pr": "42"})


def test_running_watcher_queues_exactly_one_successor() -> None:
    # The run itself is in progress; that must not stop it queueing the next
    # hour, but a run already waiting on the concurrency group must.
    gh, workflow = _agent([_run(1, "in_progress")])
    assert ensure_watcher(gh, "valkey", 42, successor=True) is True
    workflow.create_dispatch.assert_called_once()

    gh, workflow = _agent([_run(1, "in_progress"), _run(2, "pending")])
    assert ensure_watcher(gh, "valkey", 42, successor=True) is False
    workflow.create_dispatch.assert_not_called()


def test_stop_cancels_every_live_run_for_the_pr_only() -> None:
    mine = [_run(1, "in_progress"), _run(2, "pending")]
    other = _run(4, "in_progress", title="Watch valkey#43 reviews")
    gh, _ = _agent([*mine, _run(3, "completed"), other])
    assert stop_watcher(gh, "valkey", 42) == 2
    for run in mine:
        run.cancel.assert_called_once()
    other.cancel.assert_not_called()


def _watch_main(monkeypatch: pytest.MonkeyPatch, *, still_open: bool) -> tuple[list, MagicMock]:
    calls: list[tuple[str, int]] = []

    def fake_poll_pr(_gh, _gql, *, repo_full_name, number, **_kwargs):
        calls.append((repo_full_name, number))
        return 0, still_open

    successor = MagicMock(return_value=True)
    monkeypatch.setattr("scripts.release_notes.review_poll.poll_pr", fake_poll_pr)
    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher", successor)
    monkeypatch.setattr("scripts.release_notes.review_poll.run_poll_loop", lambda once, **_kw: [once()])
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("scripts.release_notes.review_poll.GitHubGraphQLClient", lambda _token: MagicMock())
    monkeypatch.setattr("scripts.release_notes.review_poll.dispatch_release_review", lambda _gh: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_APP_SLUG", "valkeyrie-ops")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS", "300")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS", "3300")
    assert poll_main(["--repo", "valkey", "--pr", "42"]) == 0
    return calls, successor


def test_watch_main_queues_the_next_hour_then_keeps_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, successor = _watch_main(monkeypatch, still_open=True)
    # First pass up front (fail fast), successor queued, then the sustained loop.
    assert calls == [("valkey-io/valkey", 42)] * 2
    successor.assert_called_once()
    assert successor.call_args.kwargs == {"successor": True}


def test_watch_main_ends_without_a_successor_when_the_pr_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, successor = _watch_main(monkeypatch, still_open=False)
    assert calls == [("valkey-io/valkey", 42)]
    successor.assert_not_called()


def test_start_and_stop_modes_touch_only_the_watcher(monkeypatch: pytest.MonkeyPatch) -> None:
    started = MagicMock(return_value=True)
    stopped = MagicMock(return_value=1)
    polled = MagicMock()
    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher", started)
    monkeypatch.setattr("scripts.release_notes.review_poll.stop_watcher", stopped)
    monkeypatch.setattr("scripts.release_notes.review_poll.poll_pr", polled)
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.delenv("RELEASE_NOTES_REVIEW_APP_SLUG", raising=False)  # not needed to start or stop

    assert poll_main(["--repo", "valkey", "--pr", "42", "--start"]) == 0
    assert started.call_args.args[1:] == ("valkey", 42)
    assert started.call_args.kwargs == {"successor": False}
    assert poll_main(["--repo", "valkey-io/valkey-json", "--pr", "7", "--stop"]) == 0
    assert stopped.call_args.args[1:] == ("valkey-json", 7)
    polled.assert_not_called()


def test_watch_main_rejects_half_an_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    with pytest.raises(SystemExit):
        poll_main(["--pr", "42"])
    with pytest.raises(SystemExit):
        poll_main(["--repo", "not-a-valkey-repo", "--pr", "42"])
    with pytest.raises(SystemExit):
        poll_main(["--repo", "valkey", "--pr", "42", "--start", "--stop"])
