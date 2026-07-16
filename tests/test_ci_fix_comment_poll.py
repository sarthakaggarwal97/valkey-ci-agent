"""Tests for the comment poller orchestration.

The poller is a trigger, so the tests pin down exactly when it dispatches and,
just as importantly, when it does not: a non-command, a bot author, an issue
(not a PR), an unauthorized commenter, an already-claimed comment, and a lost
claim race all skip without dispatching. ``poll_once`` takes injected ``claim``
and ``dispatch``, so the orchestration is exercised without real GitHub calls.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.ci_fix import comment_poll
from scripts.ci_fix.comment_poll import (
    AllRepositoriesUnavailable,
    Claim,
    WorkflowDispatchRejected,
    poll_once,
    poll_repositories,
)

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"


def _comment(*, body, login="alice", user_type="User", comment_id=1, is_pr=True):
    # A real listed IssueComment exposes url and issue_url but NOT a repository
    # attribute. The repo is supplied to poll_once separately, so the fake must
    # not carry one or it would hide a real bug.
    return SimpleNamespace(
        id=comment_id,
        body=body,
        user=SimpleNamespace(login=login, type=user_type),
        url=f"https://api.github.com/repos/valkey-io/valkey/issues/comments/{comment_id}",
        issue_url="https://api.github.com/repos/valkey-io/valkey/issues/42",
        _is_pr=is_pr,
    )


def _gh(comments):
    """Fake gh whose target repo lists ``comments`` and resolves the parent
    issue to a PR or plain issue based on the comment's ``_is_pr`` flag."""
    listed = list(comments)

    def get_issue(_n):
        is_pr = listed[0]._is_pr if listed else True
        return SimpleNamespace(pull_request={"url": "..."} if is_pr else None)

    repo = SimpleNamespace(
        full_name="valkey-io/valkey",
        get_issues_comments=lambda **kw: listed,
        get_issue=get_issue,
    )
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def _claim(_comment):
    return Claim(release=MagicMock())


def _run(gh, *, claim=_claim, dispatch=None, authorized=True):
    dispatch = dispatch or MagicMock()
    # Patch is_authorized and the already-claimed check at module scope.
    orig_auth = comment_poll.is_authorized
    orig_claimed = comment_poll._already_claimed
    comment_poll.is_authorized = lambda *a, **k: authorized
    comment_poll._already_claimed = lambda c, bot_login: getattr(c, "_claimed", False)
    try:
        n = poll_once(
            gh, target_repo="valkey-io/valkey", org="valkey-io",
            team_slug="contributors", bot_login="valkeyrie-ops[bot]", lookback_minutes=30,
            dispatch=dispatch, claim=claim,
        )
    finally:
        comment_poll.is_authorized = orig_auth
        comment_poll._already_claimed = orig_claimed
    return n, dispatch


def test_dispatches_valid_command():
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    n, dispatch = _run(gh)
    assert n == 1
    dispatch.assert_called_once()
    repo, pr, command, commenter, comment_id = dispatch.call_args.args
    assert repo == "valkey-io/valkey"
    assert pr == 42
    assert command.run_id == 27559908167
    assert commenter == "alice"
    assert comment_id == 1


def test_skips_non_command():
    gh = _gh([_comment(body="just a normal comment")])
    n, dispatch = _run(gh)
    assert n == 0
    dispatch.assert_not_called()


def test_skips_bot_author():
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}", user_type="Bot")])
    n, dispatch = _run(gh)
    assert n == 0
    dispatch.assert_not_called()


def test_skips_issue_comment_not_pr():
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}", is_pr=False)])
    n, dispatch = _run(gh)
    assert n == 0
    dispatch.assert_not_called()


def test_skips_unauthorized():
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    n, dispatch = _run(gh, authorized=False)
    assert n == 0
    dispatch.assert_not_called()


def test_skips_already_claimed():
    c = _comment(body=f"@valkeyrie-ops fix {_RUN_URL}")
    c._claimed = True
    gh = _gh([c])
    claim = MagicMock()
    n, dispatch = _run(gh, claim=claim)
    assert n == 0
    claim.assert_not_called()
    dispatch.assert_not_called()


def test_lost_claim_race_does_not_dispatch():
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    n, dispatch = _run(gh, claim=lambda c: None)  # another tick won
    assert n == 0
    dispatch.assert_not_called()


def test_claim_precedes_dispatch():
    order = []
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])
    def claim(c):
        order.append("claim")
        return Claim(release=lambda: order.append("release"))
    def dispatch(*a):
        order.append("dispatch")
    _run(gh, claim=claim, dispatch=dispatch)
    assert order == ["claim", "dispatch"]


def test_dispatch_failure_releases_claim_for_retry():
    order = []
    gh = _gh([_comment(body=f"@valkeyrie-ops fix {_RUN_URL}")])

    def claim(_comment):
        order.append("claim")
        return Claim(release=lambda: order.append("release"))

    def dispatch(*_args):
        order.append("dispatch")
        raise RuntimeError("dispatch rejected")

    n, _dispatch = _run(gh, claim=claim, dispatch=dispatch)
    assert n == 0
    assert order == ["claim", "dispatch", "release"]


# --- claim_via_status: the atomic 201-vs-200 win condition ---

def _comment_with_requester(status, data='{"id": 123}'):
    requester = MagicMock()
    requester.requestJson.return_value = (status, {}, data)
    comment = SimpleNamespace(
        id=7,
        url="https://api.github.com/repos/valkey-io/valkey/issues/comments/7",
        _requester=requester,
    )
    return comment


def test_claim_wins_on_201():
    comment = _comment_with_requester(201)
    comment._requester.requestJson.side_effect = [
        (201, {}, '{"id": 123}'),
        (204, {}, ""),
    ]
    claim = comment_poll.claim_via_status(comment)
    assert claim is not None

    claim.release()
    assert comment._requester.requestJson.call_args_list == [
        (
            ("POST", f"{comment.url}/reactions"),
            {"input": {"content": "eyes"}},
        ),
        (("DELETE", f"{comment.url}/reactions/123"),),
    ]


def test_claim_loses_on_200():
    assert comment_poll.claim_via_status(_comment_with_requester(200)) is None


def test_claim_without_reaction_id_does_not_dispatch():
    assert comment_poll.claim_via_status(_comment_with_requester(201, "{}")) is None


def test_claim_failure_is_not_a_win():
    requester = SimpleNamespace(
        requestJson=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    comment = SimpleNamespace(
        id=7,
        url="https://api.github.com/repos/valkey-io/valkey/issues/comments/7",
        _requester=requester,
    )
    assert comment_poll.claim_via_status(comment) is None


def test_claim_release_retries_transient_delete(monkeypatch):
    comment = _comment_with_requester(201)
    comment._requester.requestJson.side_effect = [
        (201, {}, '{"id": 123}'),
        (500, {}, '{"message": "temporary"}'),
        (204, {}, ""),
    ]
    monkeypatch.setattr(
        "scripts.common.github_client.transient_backoff_delay",
        lambda _attempt: 0,
    )

    claim = comment_poll.claim_via_status(comment)
    assert claim is not None
    claim.release()
    assert comment._requester.requestJson.call_count == 3


def test_dispatch_raises_on_rejected_github_request():
    gh = MagicMock()
    workflow = gh.get_repo.return_value.get_workflow.return_value
    workflow.create_dispatch.return_value = False
    dispatch = comment_poll.dispatch_ci_fix(
        gh,
        agent_repo="valkey-io/valkey-ci-agent",
        workflow="ci-fix.yml",
        ref="main",
    )
    command = comment_poll.parse_command(f"@valkeyrie-ops fix {_RUN_URL}")
    assert command is not None

    with pytest.raises(WorkflowDispatchRejected, match="GitHub rejected"):
        dispatch("valkey-io/valkey", 42, command, "alice", 7)

    workflow.create_dispatch.assert_called_once_with(
        "main",
        {
            "repo": "valkey-io/valkey",
            "pr": "42",
            "run_url": _RUN_URL,
            "hint": "",
            "commenter": "alice",
            "comment_id": "7",
        },
    )


# --- lookback clamping ---

def test_lookback_default_and_clamp(monkeypatch):
    from scripts.ci_fix.comment_poll import (
        _DEFAULT_LOOKBACK_MINUTES,
        _MAX_LOOKBACK_MINUTES,
        _lookback_minutes,
    )

    monkeypatch.delenv("CI_FIX_POLL_LOOKBACK_MINUTES", raising=False)
    assert _lookback_minutes() == _DEFAULT_LOOKBACK_MINUTES

    monkeypatch.setenv("CI_FIX_POLL_LOOKBACK_MINUTES", "120")
    assert _lookback_minutes() == 120

    monkeypatch.setenv("CI_FIX_POLL_LOOKBACK_MINUTES", "0")
    assert _lookback_minutes() == 1

    monkeypatch.setenv("CI_FIX_POLL_LOOKBACK_MINUTES", str(_MAX_LOOKBACK_MINUTES * 10))
    assert _lookback_minutes() == _MAX_LOOKBACK_MINUTES

    monkeypatch.setenv("CI_FIX_POLL_LOOKBACK_MINUTES", "garbage")
    assert _lookback_minutes() == _DEFAULT_LOOKBACK_MINUTES


def test_target_repos_come_from_registry_and_override(tmp_path, monkeypatch):
    registry = tmp_path / "repos.yml"
    registry.write_text(
        """
repos:
  - repo: valkey-io/valkey
    project_owner: valkey-io
    language: c
    ci_fix:
      enabled: true
    branches:
      - branch: "9.0"
        project_number: 1
  - repo: valkey-io/disabled
    project_owner: valkey-io
    language: c
    branches:
      - branch: "1.0"
        project_number: 2
  - repo: valkey-io/valkey-search
    project_owner: valkey-io
    language: c++
    ci_fix:
      enabled: true
    branches:
      - branch: "1.0"
        project_number: 3
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.delenv("CI_FIX_POLL_TARGET_REPO", raising=False)
    monkeypatch.setenv("CI_FIX_POLL_REGISTRY", str(registry))
    assert comment_poll._target_repos() == (
        "valkey-io/valkey",
        "valkey-io/valkey-search",
    )

    monkeypatch.setenv(
        "CI_FIX_POLL_TARGET_REPO",
        " fork/one, fork/two, fork/one ",
    )
    assert comment_poll._target_repos() == ("fork/one", "fork/two")


def test_target_repo_override_rejects_empty_or_malformed(monkeypatch):
    monkeypatch.setenv("CI_FIX_POLL_TARGET_REPO", " , ")
    with pytest.raises(ValueError, match="did not contain"):
        comment_poll._target_repos()

    monkeypatch.setenv("CI_FIX_POLL_TARGET_REPO", "not-a-repo")
    with pytest.raises(ValueError, match="malformed"):
        comment_poll._target_repos()


def _poll_repositories(monkeypatch, side_effect):
    fake_poll = MagicMock(side_effect=side_effect)
    monkeypatch.setattr(comment_poll, "poll_once", fake_poll)
    dispatched = poll_repositories(
        MagicMock(),
        target_repos=("valkey-io/valkey", "valkey-io/valkey-search"),
        org="valkey-io",
        team_slug="contributors",
        bot_login="valkeyrie-ops[bot]",
        lookback_minutes=30,
        dispatch=MagicMock(),
        claim=MagicMock(),
    )
    return dispatched, fake_poll


def test_one_repository_api_failure_does_not_abort_others(monkeypatch):
    api_error = GithubException(403, {"message": "not installed"}, None)
    dispatched, fake_poll = _poll_repositories(monkeypatch, [api_error, 2])
    assert dispatched == 2
    assert fake_poll.call_count == 2


def test_all_repository_api_failures_fail_poll_iteration(monkeypatch):
    api_error = GithubException(403, {"message": "bad installation"}, None)
    with pytest.raises(AllRepositoriesUnavailable, match="every CI-fix repository"):
        _poll_repositories(monkeypatch, [api_error, api_error])


def test_unexpected_repository_error_is_not_hidden(monkeypatch):
    with pytest.raises(RuntimeError, match="programming error"):
        _poll_repositories(monkeypatch, RuntimeError("programming error"))


# --- helpers exercised directly (these would catch the PyGithub-shape bugs) ---

def test_pull_request_number_uses_passed_repo_and_confirms_pr():
    """_pull_request_number must not read comment.repository (it does not exist);
    it takes the repo and requires the parent issue to be a PR."""
    comment = _comment(body="x", comment_id=9)  # has issue_url, no repository
    pr_repo = SimpleNamespace(get_issue=lambda n: SimpleNamespace(pull_request={"url": "..."}))
    issue_repo = SimpleNamespace(get_issue=lambda n: SimpleNamespace(pull_request=None))
    assert comment_poll._pull_request_number(pr_repo, comment) == 42
    assert comment_poll._pull_request_number(issue_repo, comment) is None


def test_already_claimed_matches_bot_reaction():
    """_already_claimed compares against the passed bot_login, not a get_user call."""
    eyes_bot = SimpleNamespace(content="eyes", user=SimpleNamespace(login="valkeyrie-ops[bot]"))
    eyes_human = SimpleNamespace(content="eyes", user=SimpleNamespace(login="alice"))
    claimed = SimpleNamespace(id=1, get_reactions=lambda: [eyes_human, eyes_bot])
    unclaimed = SimpleNamespace(id=2, get_reactions=lambda: [eyes_human])
    assert comment_poll._already_claimed(claimed, "valkeyrie-ops[bot]") is True
    assert comment_poll._already_claimed(unclaimed, "valkeyrie-ops[bot]") is False


def test_one_bad_comment_does_not_abort_the_tick():
    """An error processing one comment must not stop the others in the tick."""
    good = _comment(body=f"@valkeyrie-ops fix {_RUN_URL}", comment_id=1)
    later = _comment(body=f"@valkeyrie-ops fix {_RUN_URL}", comment_id=2)
    gh = _gh([good, later])

    # The first comment's claim raises; the second should still dispatch.
    def claim(c):
        if c.id == 1:
            raise RuntimeError("transient API error")
        return Claim(release=MagicMock())

    n, dispatch = _run(gh, claim=claim)
    assert n == 1
    assert dispatch.call_count == 1
    _repo, pr, _cmd, _commenter, _comment_id = dispatch.call_args.args
    assert pr == 42


def test_bot_login_override_and_app_slug(monkeypatch):
    """Explicit CI_FIX_POLL_BOT_LOGIN wins; otherwise derive <slug>[bot]."""
    from scripts.ci_fix.comment_poll import _bot_login

    monkeypatch.delenv("CI_FIX_POLL_BOT_LOGIN", raising=False)
    monkeypatch.setenv("CI_FIX_POLL_APP_SLUG", "valkeyrie-ops")
    assert _bot_login() == "valkeyrie-ops[bot]"

    monkeypatch.setenv("CI_FIX_POLL_BOT_LOGIN", "sarthakaggarwal97")
    assert _bot_login() == "sarthakaggarwal97"


def test_poll_loop_env_clamped_below_token_ttl(monkeypatch):
    from scripts.ci_fix.comment_poll import (
        _MAX_LOOP_SECONDS,
        _poll_duration_seconds,
        _poll_interval_seconds,
    )

    monkeypatch.delenv("CI_FIX_POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("CI_FIX_POLL_DURATION_SECONDS", raising=False)
    assert _poll_interval_seconds() == 0
    assert _poll_duration_seconds() == 0

    monkeypatch.setenv("CI_FIX_POLL_INTERVAL_SECONDS", "1800")
    monkeypatch.setenv("CI_FIX_POLL_DURATION_SECONDS", str(_MAX_LOOP_SECONDS * 10))
    assert _poll_interval_seconds() == 1800
    assert _poll_duration_seconds() == _MAX_LOOP_SECONDS
