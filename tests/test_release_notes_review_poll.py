from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.common.polling import PollLoopError
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
    chain_allowed,
    dispatch_release_review,
    ensure_watcher,
    live_watcher_runs,
    poll_once,
    poll_pr,
    restart_missing_watchers,
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


def _watch_main(
    monkeypatch: pytest.MonkeyPatch,
    *,
    still_open: bool,
    created_at: datetime | None = None,
    first_pass_fails: bool = False,
    argv: list[str] | None = None,
) -> tuple[list, MagicMock]:
    calls: list[tuple[str, int]] = []
    pr = SimpleNamespace(created_at=created_at or datetime.now(timezone.utc))

    def fake_load(_gh, repo_full_name, number):
        return (pr, MagicMock()) if still_open else None

    def fake_poll_pr(_gh, _gql, *, repo_full_name, number, **_kwargs):
        calls.append((repo_full_name, number))
        if first_pass_fails and len(calls) == 1:
            raise RuntimeError("GitHub hiccup")
        return 0, still_open

    def fake_loop(once, *, until=None, **_kwargs):
        # Two passes, like the real loop: a failed pass is swallowed and
        # reported at the end, and `until` ends the loop early.
        results = []
        failed = False
        for _ in range(2):
            try:
                results.append(once())
            except Exception:  # noqa: BLE001 - mirrors run_poll_loop
                failed = True
                continue
            if until is not None and until(results[-1]):
                break
        if failed:
            raise PollLoopError(results=results, last_error=RuntimeError("GitHub hiccup"))
        return results

    successor = MagicMock(return_value=True)
    monkeypatch.setattr("scripts.release_notes.review_poll.load_release_pr", fake_load)
    monkeypatch.setattr("scripts.release_notes.review_poll.poll_pr", fake_poll_pr)
    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher", successor)
    monkeypatch.setattr("scripts.release_notes.review_poll.run_poll_loop", fake_loop)
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    monkeypatch.setattr("scripts.release_notes.review_poll.GitHubGraphQLClient", lambda _token: MagicMock())
    monkeypatch.setattr("scripts.release_notes.review_poll.dispatch_release_review", lambda _gh: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_APP_SLUG", "valkeyrie-ops")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS", "300")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS", "3300")
    if first_pass_fails:
        with pytest.raises(PollLoopError):
            poll_main(argv or ["--repo", "valkey", "--pr", "42"])
    else:
        assert poll_main(argv or ["--repo", "valkey", "--pr", "42"]) == 0
    return calls, successor


def test_watch_main_queues_the_next_hour_before_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, successor = _watch_main(monkeypatch, still_open=True)
    assert calls == [("valkey-io/valkey", 42)] * 2
    successor.assert_called_once()
    assert successor.call_args.kwargs == {"successor": True}


def test_watch_main_queues_the_next_hour_even_when_a_pass_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # A transient API error must not break the chain silently: the successor
    # is queued as soon as the PR is confirmed open, before any pass runs.
    calls, successor = _watch_main(monkeypatch, still_open=True, first_pass_fails=True)
    assert len(calls) == 2
    successor.assert_called_once()


def test_watch_main_ends_without_a_successor_when_the_pr_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, successor = _watch_main(monkeypatch, still_open=False)
    assert calls == []
    successor.assert_not_called()


def test_watch_main_stops_queueing_once_the_pr_is_old(monkeypatch: pytest.MonkeyPatch) -> None:
    old = datetime.now(timezone.utc) - timedelta(hours=73)
    calls, successor = _watch_main(monkeypatch, still_open=True, created_at=old)
    assert calls  # this hour still runs
    successor.assert_not_called()
    calls, successor = _watch_main(
        monkeypatch, still_open=True, created_at=old,
        argv=["--repo", "valkey", "--pr", "42", "--max-age-hours", "100"],
    )
    successor.assert_called_once()


def test_watch_main_fails_fast_on_a_pr_that_is_not_a_release_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(_gh, _repo, _number):
        raise ReviewRefused("release PR does not use an agent/release-cut branch")

    successor = MagicMock()
    monkeypatch.setattr("scripts.release_notes.review_poll.load_release_pr", refuse)
    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher", successor)
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS", "300")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS", "3300")
    assert poll_main(["--repo", "valkey", "--pr", "42"]) == 1
    successor.assert_not_called()


def test_chain_age_is_measured_from_the_pr_creation() -> None:
    now = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
    young = SimpleNamespace(created_at=now - timedelta(hours=71))
    old = SimpleNamespace(created_at=now - timedelta(hours=73))
    naive = SimpleNamespace(created_at=(now - timedelta(hours=1)).replace(tzinfo=None))
    assert chain_allowed(young, max_age_hours=72, now=now)
    assert not chain_allowed(old, max_age_hours=72, now=now)
    assert chain_allowed(naive, max_age_hours=72, now=now)
    assert chain_allowed(SimpleNamespace(), max_age_hours=72, now=now)  # unknown age: keep watching


def test_restart_starts_watchers_only_for_unwatched_young_release_prs(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    watched = _pr()
    watched.number = 41
    watched.created_at = now
    unwatched = _pr()
    unwatched.number = 42
    unwatched.created_at = now
    old = _pr()
    old.number = 43
    old.created_at = now - timedelta(hours=80)
    not_a_release = _pr()
    not_a_release.number = 44
    not_a_release.created_at = now
    not_a_release.head.ref = "feature/not-a-release"

    gh = MagicMock()
    gh.get_repo.return_value = SimpleNamespace(get_pulls=lambda **_kw: [watched, unwatched, old, not_a_release])
    started: list[tuple[str, int]] = []

    def fake_ensure(_gh, repo_name, number, *, successor, agent_repo):
        assert successor is False
        started.append((repo_name, number))
        return number != 41  # 41 already has a watcher

    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher", fake_ensure)
    assert restart_missing_watchers(gh, repositories=("valkey-io/valkey",)) == 1
    assert started == [("valkey", 41), ("valkey", 42)]


def test_restart_mode_takes_no_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    restarted = MagicMock(return_value=0)
    monkeypatch.setattr("scripts.release_notes.review_poll.restart_missing_watchers", restarted)
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    assert poll_main(["--restart-missing"]) == 0
    assert restarted.call_args.kwargs["max_age_hours"] == 72
    with pytest.raises(SystemExit):
        poll_main(["--restart-missing", "--repo", "valkey", "--pr", "1"])


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


def test_watch_main_dispatches_with_the_agent_token_and_reads_with_the_target_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: dict[str, MagicMock] = {}

    def fake_github(*, auth):
        clients[auth.token] = MagicMock(name=auth.token)
        return clients[auth.token]

    pr = SimpleNamespace(created_at=datetime.now(timezone.utc))
    loads: list[MagicMock] = []
    successors: list[MagicMock] = []
    dispatchers: list[MagicMock] = []
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", fake_github)
    monkeypatch.setattr("scripts.release_notes.review_poll.Auth.Token", lambda token: SimpleNamespace(token=token))
    monkeypatch.setattr("scripts.release_notes.review_poll.load_release_pr",
                        lambda gh, _repo, _n: loads.append(gh) or (pr, MagicMock()))
    monkeypatch.setattr("scripts.release_notes.review_poll.ensure_watcher",
                        lambda gh, _r, _n, **_kw: successors.append(gh) or True)
    monkeypatch.setattr("scripts.release_notes.review_poll.dispatch_release_review",
                        lambda gh: dispatchers.append(gh) or MagicMock())
    monkeypatch.setattr("scripts.release_notes.review_poll.poll_pr", lambda *_a, **_kw: (0, False))
    monkeypatch.setattr("scripts.release_notes.review_poll.GitHubGraphQLClient", lambda _token: MagicMock())
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "target-token")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_AGENT_TOKEN", "agent-token")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_APP_SLUG", "valkeyrie-ops")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_INTERVAL_SECONDS", "300")
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_POLL_DURATION_SECONDS", "3300")

    assert poll_main(["--repo", "valkey", "--pr", "42"]) == 0
    assert loads == [clients["target-token"]]
    assert successors == [clients["agent-token"]]
    assert dispatchers == [clients["agent-token"]]


def test_watch_main_rejects_half_an_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELEASE_NOTES_REVIEW_TOKEN", "t")
    monkeypatch.setattr("scripts.release_notes.review_poll.Github", lambda **_kwargs: MagicMock())
    with pytest.raises(SystemExit):
        poll_main(["--pr", "42"])
    with pytest.raises(SystemExit):
        poll_main(["--repo", "not-a-valkey-repo", "--pr", "42"])
    with pytest.raises(SystemExit):
        poll_main(["--repo", "valkey", "--pr", "42", "--start", "--stop"])
