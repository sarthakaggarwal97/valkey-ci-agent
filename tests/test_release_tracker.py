from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.release import tracker as tracker_mod
from scripts.release.checks import CandidateCheck, CandidateCI
from scripts.release.models import ReleasePolicy

SHA = "a" * 40
TRACKER = tracker_mod.Tracker(
    repo="valkey-io/valkey",
    branch="9.1",
    version="9.1.2",
    stage="ga",
    tag="9.1.2",
    prep_branch="agent/release-cut/9.1.2-ga",
    prepare_run_id=123,
)


POLICY = ReleasePolicy(
    repo=TRACKER.repo,
    authorized_team="valkey-io/core-team",
    branches=(TRACKER.branch,),
    checks_workflow="ci.yml",
    required_checks=("linux", "macos"),
)


def _run(*, status: str = "completed", conclusion: str | None = "success"):
    return SimpleNamespace(
        id=123,
        status=status,
        conclusion=conclusion,
        html_url="https://example/actions/runs/123",
    )


def _candidate_ci(*, linux: str = "success", macos: str = "success", status: str = "completed") -> CandidateCI:
    checks = tuple(
        CandidateCheck(
            name=name,
            status="completed" if conclusion in {"success", "failure"} else conclusion,
            conclusion=conclusion if conclusion in {"success", "failure"} else None,
            url=f"https://example/checks/{name}",
        )
        for name, conclusion in (("linux", linux), ("macos", macos))
    )
    return CandidateCI(
        workflow_url="https://example/actions/runs/456",
        workflow_status=status,
        workflow_conclusion="success" if all(check.passed for check in checks) else None,
        suite_id=7,
        checks=checks,
    )


def _issue() -> MagicMock:
    issue = MagicMock()
    issue.number = 42
    issue.user.login = "release-app[bot]"
    issue.get_comments.return_value = []
    return issue


def test_tracker_marker_round_trips_and_rejects_invalid_metadata() -> None:
    assert tracker_mod.parse_tracker(f"hello\n{TRACKER.marker()}\n") == TRACKER
    assert tracker_mod.parse_tracker("<!-- valkey-release-tracker:v1 {} -->") is None


@pytest.mark.parametrize(
    "payload",
    [
        '{"repo":null}',
        '{"repo":7}',
        '{"repo":"valkey-io/valkey --> forged"}',
        '{"repo":"valkey-io/valkey","branch":"9.1","version":"9.1.2","stage":"ga",'
        '"tag":"9.1.2","prep_branch":"agent/release-cut/9.1.2-ga","prepare_run_id":true}',
        "[" * 5000,
    ],
)
def test_poisoned_tracker_markers_are_contained(payload: str) -> None:
    assert tracker_mod.parse_tracker(f"{tracker_mod._TRACKER_PREFIX}{payload} -->") is None


def test_ensure_checks_issue_ownership_before_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    human = _issue()
    human.user.login = "maintainer"
    bot = _issue()
    bot.state = "open"
    bot.title = f"Release {TRACKER.tag}"
    bot.body = tracker_mod._issue_body(TRACKER, "valkey-io/valkey-ci-agent", include_marker=False)
    repo = MagicMock()
    repo.get_issues.return_value = [human, bot]
    gh = MagicMock()
    gh.get_repo.return_value = repo
    parsed = MagicMock(return_value=TRACKER)
    monkeypatch.setattr(tracker_mod, "_tracker_from_issue", parsed)

    assert tracker_mod.ensure_tracker(gh, TRACKER, agent_repo="valkey-io/valkey-ci-agent") is bot
    parsed.assert_called_once_with(bot)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"repo": "valkey"}, "owner/name"),
        ({"branch": "unstable"}, "MAJOR.MINOR"),
        ({"version": "9.1"}, "MAJOR.MINOR.PATCH"),
        ({"stage": "rc0"}, "ga or rcN"),
        ({"tag": "9.1.3"}, "does not match"),
        ({"prep_branch": "agent/release-cut/other"}, "not canonical"),
        ({"prepare_run_id": True}, "must be positive"),
        ({"prepare_run_id": 0}, "must be positive"),
    ],
)
def test_tracker_validation_refuses_each_invalid_identity(
    changes: dict[str, object],
    message: str,
) -> None:
    candidate = tracker_mod.Tracker(**{**TRACKER.__dict__, **changes})
    with pytest.raises(ValueError, match=message):
        tracker_mod._validate_tracker(candidate)


def test_bot_status_comment_is_authoritative_over_edited_issue_body() -> None:
    issue = _issue()
    issue.body = tracker_mod.Tracker(
        **{**TRACKER.__dict__, "version": "9.1.3", "tag": "9.1.3", "prep_branch": "agent/release-cut/9.1.3-ga"}
    ).marker()
    comment = SimpleNamespace(
        user=SimpleNamespace(login=issue.user.login),
        body=f"{tracker_mod._STATUS_MARKER}\n{TRACKER.marker()}\nlive status",
    )
    issue.get_comments.return_value = [comment]
    assert tracker_mod._tracker_from_issue(issue) == TRACKER


def test_only_bot_owned_issues_are_accepted_as_dashboards() -> None:
    bot_issue = SimpleNamespace(user=SimpleNamespace(login="release-app[bot]"))
    owner_issue = SimpleNamespace(user=SimpleNamespace(login="SarthakAggarwal97"))
    other_issue = SimpleNamespace(user=SimpleNamespace(login="maintainer"))

    assert tracker_mod._is_bot_owned(bot_issue)
    assert not tracker_mod._is_bot_owned(owner_issue)
    assert not tracker_mod._is_bot_owned(other_issue)


def test_prep_pr_fallback_survives_deleted_head_branch() -> None:
    repo = MagicMock()
    expected = SimpleNamespace(
        head=SimpleNamespace(
            ref=TRACKER.prep_branch,
            repo=SimpleNamespace(full_name=TRACKER.repo),
        )
    )
    repo.get_pulls.side_effect = [[], [expected]]
    assert tracker_mod._find_prep_pr(repo, TRACKER) is expected
    assert repo.get_pulls.call_count == 2


def test_prepare_failure_is_visible_with_a_direct_next_action() -> None:
    body, summary = tracker_mod._render_status(
        TRACKER,
        prepare_run=_run(conclusion="failure"),
        pr=None,
        branch_head=SHA,
        candidate_sha="",
        candidate_ci=None,
        publish_run=None,
        release=None,
        production_run=None,
        agent_repo="valkey-io/valkey-ci-agent",
        dispatched=False,
    )
    assert summary == "preparation failed"
    assert "Release preparation failed" in body
    assert "rerun Prepare Release" in body
    assert "> [!CAUTION]" in body
    assert "img.shields.io/badge/-Prepare-cf222e" in body
    assert not any(symbol in body for symbol in "✅❌⏳⛔🛑⚠️🟦🟥🟩⬜")
    assert "—" not in body


def test_downstream_follow_up_links_cover_ga_outputs_without_new_api_access() -> None:
    body, summary = tracker_mod._render_status(
        TRACKER,
        prepare_run=_run(),
        pr=None,
        branch_head=SHA,
        candidate_sha="",
        candidate_ci=None,
        publish_run=None,
        release=SimpleNamespace(html_url="https://example/releases/9.1.2"),
        production_run=_run(),
        agent_repo="valkey-io/valkey-ci-agent",
        dispatched=False,
    )

    assert summary == "production automation completed"
    assert "valkey-hashes/blob/main/README" in body
    assert "valkey-container/pulls?q=is%3Apr+head%3Aupdate-9.1.2" in body
    assert "valkey-doc/tree/9.1.2" in body
    assert "valkey-io.github.io/pulls?q=is%3Apr+head%3Aupdate-website-9.1.2" in body
    assert "valkey-helm/pulls?q=is%3Apr+head%3Aupdate-valkey-9.1.2" in body
    assert "valkey-bundle/pulls?q=is%3Apr+head%3Avalkey-bundle-update" in body
    assert "**Manual follow-up:**" in body
    assert "Release owner review" in body
    assert "review and merge every linked downstream PR" in body


def test_8_0_follow_up_omits_bundle_and_requires_manual_pr_merges() -> None:
    tracker = tracker_mod.Tracker(
        **{
            **TRACKER.__dict__,
            "branch": "8.0",
            "version": "8.0.12",
            "tag": "8.0.12",
            "prep_branch": "agent/release-cut/8.0.12-ga",
        }
    )
    body, summary = tracker_mod._render_status(
        tracker,
        prepare_run=_run(),
        pr=None,
        branch_head=SHA,
        candidate_sha="",
        candidate_ci=None,
        publish_run=None,
        release=SimpleNamespace(html_url="https://example/releases/8.0.12"),
        production_run=_run(),
        agent_repo="valkey-io/valkey-ci-agent",
        dispatched=False,
    )

    assert summary == "production automation completed"
    assert "**Manual follow-up:**" in body
    assert "review and merge every linked downstream PR" in body
    assert "valkey-bundle" not in body
    assert "Bundle" not in body


def test_rc_follow_up_omits_ga_only_outputs() -> None:
    rc = tracker_mod.Tracker(
        **{
            **TRACKER.__dict__,
            "version": "9.1.0",
            "stage": "rc1",
            "tag": "9.1.0-rc1",
            "prep_branch": "agent/release-cut/9.1.0-rc1",
        }
    )
    links = tracker_mod._downstream_links(rc)
    assert "Container PR search" in links
    assert "Bundle PR search" in links
    assert "Documentation" not in links
    assert "Website" not in links
    assert "Helm" not in links


def test_issue_body_is_a_compact_maintainer_control_center() -> None:
    body = tracker_mod._issue_body(TRACKER, "valkey-io/valkey-ci-agent")
    assert '<div align="center">' in body
    assert "stable release identity and operator guidance" in body
    assert "Prepare` → `Review notes` → `Candidate CI` → `Qualification" in body
    assert f"[`{TRACKER.prep_branch}`](https://github.com/{TRACKER.repo}/tree/{TRACKER.prep_branch})" in body
    assert "Prepare run 123" in body
    assert "## Human checkpoints" in body
    assert "- [ ]" not in body
    assert "review and merge every linked downstream PR" in body
    assert "confirm Bundle" not in body
    assert "Editing this issue never authorizes" in body
    assert "—" not in body


def test_merged_pr_at_branch_head_dispatches_publication_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    repo = MagicMock()
    agent = MagicMock()
    agent.default_branch = "main"
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    workflow.create_dispatch.return_value = None  # PyGithub may return no body on HTTP 204.
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: SHA)
    monkeypatch.setattr(tracker_mod, "_find_run", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "evaluate_candidate_ci", lambda *a: _candidate_ci())
    monkeypatch.setattr(tracker_mod, "_find_production_run", lambda *a: None)

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        repo,
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    workflow.create_dispatch.assert_called_once_with(
        "main",
        inputs={"branch": "9.1", "candidate_sha": SHA},
    )
    assert result == "#42: publication dispatched"
    assert "| Qualification |" in issue.create_comment.call_args.args[0]
    assert "img.shields.io/badge/-Starting-0969da" in issue.create_comment.call_args.args[0]


def test_moved_branch_blocks_automatic_publication(monkeypatch: pytest.MonkeyPatch) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: "b" * 40)
    monkeypatch.setattr(tracker_mod, "_find_run", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "evaluate_candidate_ci", lambda *a: _candidate_ci())

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        MagicMock(),
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    workflow.create_dispatch.assert_not_called()
    assert result == "#42: candidate invalidated by branch movement"
    assert "Rerun Prepare Release" in issue.create_comment.call_args.args[0]


def test_existing_exact_publication_run_prevents_duplicate_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    publication = _run(status="in_progress", conclusion=None)
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: SHA)
    find_run = MagicMock(return_value=publication)
    monkeypatch.setattr(tracker_mod, "_find_run", find_run)
    monkeypatch.setattr(tracker_mod, "evaluate_candidate_ci", lambda *a: _candidate_ci())

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        MagicMock(),
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    title = f"Publish release on {TRACKER.branch} @ {SHA}"
    find_run.assert_called_once_with(workflow, title, SHA)
    workflow.create_dispatch.assert_not_called()
    assert result == "#42: validating and qualifying"


def test_stale_waiting_publication_is_cancelled_before_redispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    repo = MagicMock()
    agent = MagicMock()
    agent.default_branch = "main"
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    workflow.create_dispatch.return_value = True
    stale = _run(status="waiting", conclusion=None)
    stale.head_sha = "b" * 40
    stale.cancel = MagicMock(return_value=True)
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", MagicMock(side_effect=[SHA, "c" * 40]))
    monkeypatch.setattr(tracker_mod, "_find_run", MagicMock(side_effect=[None, stale]))
    monkeypatch.setattr(tracker_mod, "evaluate_candidate_ci", lambda *a: _candidate_ci())
    monkeypatch.setattr(tracker_mod, "_find_production_run", lambda *a: None)

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        repo,
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    stale.cancel.assert_called_once_with()
    workflow.create_dispatch.assert_called_once_with(
        "main",
        inputs={"branch": "9.1", "candidate_sha": SHA},
    )
    assert result == "#42: publication dispatched"


def test_find_run_prefers_active_match_over_newer_completed_duplicate() -> None:
    title = f"Publish release on {TRACKER.branch} @ {SHA}"
    workflow = MagicMock()
    workflow.name = "Publish Release"
    cancelled = SimpleNamespace(display_title=title, status="completed", conclusion="cancelled")
    active = SimpleNamespace(display_title=title, status="in_progress", conclusion=None)
    workflow.get_runs.return_value = [cancelled, active]

    assert tracker_mod._find_run(workflow, title) is active


def test_release_must_be_published_at_the_exact_candidate() -> None:
    repo = MagicMock()
    repo.get_releases.return_value = [SimpleNamespace(tag_name=TRACKER.tag, draft=False, prerelease=False)]
    repo.get_git_ref.return_value.object = SimpleNamespace(type="commit", sha="b" * 40)
    with pytest.raises(RuntimeError, match="expected candidate"):
        tracker_mod._find_release(repo, TRACKER.tag, SHA, False)


def test_draft_or_wrong_kind_release_is_not_accepted() -> None:
    repo = MagicMock()
    repo.get_releases.return_value = [SimpleNamespace(tag_name=TRACKER.tag, draft=True, prerelease=False)]
    with pytest.raises(RuntimeError, match="draft"):
        tracker_mod._find_release(repo, TRACKER.tag, SHA, False)
    repo.get_releases.return_value = [SimpleNamespace(tag_name=TRACKER.tag, draft=False, prerelease=True)]
    with pytest.raises(RuntimeError, match="prerelease"):
        tracker_mod._find_release(repo, TRACKER.tag, SHA, False)


def test_successful_prepare_waits_truthfully_for_delayed_pr() -> None:
    body, summary = tracker_mod._render_status(
        TRACKER,
        prepare_run=_run(),
        pr=None,
        branch_head=SHA,
        candidate_sha="",
        candidate_ci=None,
        publish_run=None,
        release=None,
        production_run=None,
        agent_repo="valkey-io/valkey-ci-agent",
        dispatched=False,
    )

    assert summary == "preparation completed"
    assert "Release preparation completed and the release-notes PR is pending" in body
    assert "Wait for the release-notes PR to appear" in body


def test_failed_candidate_ci_is_visible_but_does_not_block_qualification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.default_branch = "main"
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: SHA)
    monkeypatch.setattr(tracker_mod, "_find_run", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "evaluate_candidate_ci", lambda *a: _candidate_ci(macos="failure"))
    monkeypatch.setattr(tracker_mod, "_find_production_run", lambda *a: None)

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        MagicMock(),
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    workflow.create_dispatch.assert_called_once_with(
        "main",
        inputs={"branch": "9.1", "candidate_sha": SHA},
    )
    assert result == "#42: publication dispatched"
    body = issue.create_comment.call_args.args[0]
    assert "[Candidate CI run 456](https://example/actions/runs/456)" in body
    assert "Advisory only; inspect if unexpected: macos." in body
    assert "[`macos` check](https://example/checks/macos)" in body


def test_unavailable_candidate_ci_does_not_strand_qualification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.default_branch = "main"
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: SHA)
    monkeypatch.setattr(tracker_mod, "_find_run", lambda *a: None)
    monkeypatch.setattr(
        tracker_mod,
        "evaluate_candidate_ci",
        MagicMock(side_effect=RuntimeError("checks API unavailable")),
    )
    monkeypatch.setattr(tracker_mod, "_find_production_run", lambda *a: None)

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        MagicMock(),
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    workflow.create_dispatch.assert_called_once()
    assert result == "#42: publication dispatched"
    body = issue.create_comment.call_args.args[0]
    assert "img.shields.io/badge/-Unavailable-9a6700" in body
    assert "Advisory only; inspect progress logs if this persists." in body


def test_sync_rejects_policy_for_a_different_repository() -> None:
    wrong_policy = ReleasePolicy(
        repo="valkey-io/other",
        authorized_team=POLICY.authorized_team,
        branches=POLICY.branches,
        checks_workflow=POLICY.checks_workflow,
        required_checks=POLICY.required_checks,
    )

    with pytest.raises(ValueError, match="does not match tracker target"):
        tracker_mod.sync_trackers(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            target_repo=TRACKER.repo,
            agent_repo="valkey-io/valkey-ci-agent",
            automation_repo="valkey-io/valkey-release-automation",
            policy=wrong_policy,
        )


def test_sync_logs_invalid_marker_and_continues_to_healthy_tracker(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    poisoned = _issue()
    poisoned.number = 40
    healthy = _issue()
    healthy.number = 42
    repo = MagicMock()
    repo.get_issues.return_value = [poisoned, healthy]
    agent = MagicMock()
    automation = MagicMock()
    monkeypatch.setattr(tracker_mod, "_repo", MagicMock(side_effect=[repo, agent, automation]))
    monkeypatch.setattr(tracker_mod, "_ensure_label", lambda *a: object())
    monkeypatch.setattr(tracker_mod, "_tracker_from_issue", MagicMock(side_effect=[None, TRACKER]))
    sync_one = MagicMock(return_value="#42: refreshed")
    monkeypatch.setattr(tracker_mod, "_sync_one", sync_one)

    results = tracker_mod.sync_trackers(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        target_repo=TRACKER.repo,
        agent_repo="valkey-io/valkey-ci-agent",
        automation_repo="valkey-io/valkey-release-automation",
        policy=POLICY,
    )

    assert results == ["#40: invalid tracker metadata", "#42: refreshed"]
    assert "invalid metadata" in caplog.text
    sync_one.assert_called_once()


def test_off_policy_tracker_is_rejected_before_issue_or_dispatch_mutation() -> None:
    off_policy = tracker_mod.Tracker(
        **{
            **TRACKER.__dict__,
            "branch": "8.0",
            "version": "8.0.12",
            "tag": "8.0.12",
            "prep_branch": "agent/release-cut/8.0.12-ga",
        }
    )
    issue = _issue()
    workflow = MagicMock()

    with pytest.raises(ValueError, match="not allowed by release policy"):
        tracker_mod._sync_one(
            issue,
            off_policy,
            MagicMock(),
            MagicMock(),
            MagicMock(),
            workflow,
            agent_repo="valkey-io/valkey-ci-agent",
            policy=POLICY,
            dispatch=True,
        )

    issue.edit.assert_not_called()
    workflow.create_dispatch.assert_not_called()


def test_sync_cli_uses_the_shared_bounded_poll_loop(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in ("TARGET_GITHUB_TOKEN", "AGENT_GITHUB_TOKEN", "AUTOMATION_GITHUB_TOKEN"):
        monkeypatch.setenv(name, "token")
    monkeypatch.setattr(tracker_mod, "Github", lambda **kwargs: SimpleNamespace(auth=kwargs["auth"]))
    monkeypatch.setattr(tracker_mod, "load_policy", lambda path: POLICY)
    sync = MagicMock(return_value=["#42: refreshed"])
    monkeypatch.setattr(tracker_mod, "sync_trackers", sync)

    def run_twice(poll, args, **kwargs):
        assert args.poll_interval_seconds == 300
        assert args.poll_duration_seconds == 3300
        assert kwargs["logger"] is tracker_mod.logger
        return [poll(), poll()]

    monkeypatch.setattr(tracker_mod, "run_poll_loop_from_args", run_twice)

    assert tracker_mod.main([
        "sync",
        "--poll-interval-seconds",
        "300",
        "--poll-duration-seconds",
        "3300",
    ]) == 0

    assert sync.call_count == 2
    assert capsys.readouterr().out.splitlines() == ["#42: refreshed", "#42: refreshed"]


def test_tracker_outputs_refuse_multiline_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))

    with pytest.raises(ValueError, match="multiline workflow output refused"):
        tracker_mod._write_outputs({"issue_url": "safe\nforged=true"})


def test_unchanged_status_does_not_churn_comment_for_timestamp_only() -> None:
    issue = _issue()
    comment = MagicMock()
    comment.user.login = issue.user.login
    comment.body = "<!-- valkey-release-tracker:status -->\nsame\nStatus last changed 2026-08-20 08:25 UTC\n"
    issue.get_comments.return_value = [comment]

    tracker_mod._upsert_status(issue, "same\nStatus last changed 2026-08-20 09:30 UTC")

    comment.edit.assert_not_called()
    issue.create_comment.assert_not_called()


def test_pending_candidate_ci_is_linked_without_blocking_qualification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.default_branch = "main"
    agent.get_workflow_run.return_value = _run()
    workflow = MagicMock()
    pr = SimpleNamespace(
        merged=True,
        merge_commit_sha=SHA,
        number=7,
        html_url="https://example/pull/7",
        state="closed",
        draft=False,
    )
    monkeypatch.setattr(tracker_mod, "_find_prep_pr", lambda *a: pr)
    monkeypatch.setattr(tracker_mod, "_find_release", lambda *a: None)
    monkeypatch.setattr(tracker_mod, "_branch_head", lambda *a: SHA)
    monkeypatch.setattr(tracker_mod, "_find_run", lambda *a: None)
    monkeypatch.setattr(
        tracker_mod,
        "evaluate_candidate_ci",
        lambda *a: _candidate_ci(macos="in_progress", status="in_progress"),
    )
    monkeypatch.setattr(tracker_mod, "_find_production_run", lambda *a: None)

    result = tracker_mod._sync_one(
        issue,
        TRACKER,
        MagicMock(),
        agent,
        MagicMock(),
        workflow,
        agent_repo="valkey-io/valkey-ci-agent",
        policy=POLICY,
        dispatch=True,
    )

    workflow.create_dispatch.assert_called_once_with(
        "main",
        inputs={"branch": "9.1", "candidate_sha": SHA},
    )
    assert result == "#42: publication dispatched"
    body = issue.create_comment.call_args.args[0]
    assert "[PR #7](https://example/pull/7)" in body
    assert f"[Candidate `{SHA[:12]}`](https://github.com/{TRACKER.repo}/commit/{SHA})" in body
    assert "[Candidate CI run 456](https://example/actions/runs/456)" in body
    assert "1 of 2 configured checks passed" in body
    assert "Advisory only; still running: macos." in body
    assert "| Candidate CI |" in body
    assert "—" not in body
    assert not any(symbol in body for symbol in "✅❌⏳⛔🛑⚠️🟦🟥🟩⬜")


def test_refresh_issue_body_migrates_legacy_dashboard_idempotently() -> None:
    issue = _issue()
    issue.title = f"Release {TRACKER.tag}"
    issue.body = "## Maintainer checklist\n\n- [ ] Legacy action"

    tracker_mod._refresh_issue_body(issue, TRACKER, "valkey-io/valkey-ci-agent")

    rendered = issue.edit.call_args.kwargs["body"]
    assert issue.edit.call_args.kwargs["title"] == f"Release {TRACKER.tag}"
    assert "## Maintainer checklist" not in rendered
    assert "## Human checkpoints" in rendered
    assert TRACKER.marker() not in rendered
    assert f"[{TRACKER.prep_branch!r}]" not in rendered
    assert f"https://github.com/{TRACKER.repo}/tree/{TRACKER.prep_branch}" in rendered

    issue.body = rendered
    issue.edit.reset_mock()
    tracker_mod._refresh_issue_body(issue, TRACKER, "valkey-io/valkey-ci-agent")
    issue.edit.assert_not_called()
