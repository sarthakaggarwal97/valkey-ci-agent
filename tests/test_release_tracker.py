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


def test_only_bot_or_explicitly_trusted_owner_issues_are_accepted_as_dashboards() -> None:
    bot_issue = SimpleNamespace(user=SimpleNamespace(login="release-app[bot]"))
    owner_issue = SimpleNamespace(user=SimpleNamespace(login="SarthakAggarwal97"))
    other_issue = SimpleNamespace(user=SimpleNamespace(login="maintainer"))

    assert tracker_mod._is_trusted_owner(bot_issue)
    assert not tracker_mod._is_trusted_owner(owner_issue)
    assert tracker_mod._is_trusted_owner(owner_issue, "sarthakaggarwal97")
    assert not tracker_mod._is_trusted_owner(other_issue, "sarthakaggarwal97")


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


def test_issue_body_is_a_compact_maintainer_control_center() -> None:
    body = tracker_mod._issue_body(TRACKER, "valkey-io/valkey-ci-agent")
    assert '<div align="center">' in body
    assert "stable release identity and operator guidance" in body
    assert "Prepare` → `Review notes` → `Candidate CI` → `Qualification" in body
    assert f"[`{TRACKER.prep_branch}`](https://github.com/{TRACKER.repo}/tree/{TRACKER.prep_branch})" in body
    assert "Prepare run 123" in body
    assert "## Human checkpoints" in body
    assert "- [ ]" not in body
    assert "Editing this issue never authorizes" in body
    assert "—" not in body


def test_merged_pr_at_branch_head_dispatches_publication_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    repo = MagicMock()
    agent = MagicMock()
    agent.default_branch = "e2e/latest-9.1.4-release-controller"
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
        "e2e/latest-9.1.4-release-controller",
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


@pytest.mark.parametrize(
    ("trusted_owner", "expected_head_sha"),
    (("", SHA), ("sarthakaggarwal97", "")),
)
def test_existing_exact_publication_run_prevents_duplicate_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    trusted_owner: str,
    expected_head_sha: str,
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
        trusted_owner=trusted_owner,
        dispatch=True,
    )

    find_run.assert_called_once_with(
        workflow,
        f"Publish release on {TRACKER.branch} @ {SHA}",
        expected_head_sha,
    )
    workflow.create_dispatch.assert_not_called()
    assert result == "#42: validating and qualifying"


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


def test_failed_candidate_ci_is_visible_and_blocks_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.default_branch = "e2e/latest-9.1.4-release-controller"
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

    workflow.create_dispatch.assert_not_called()
    assert result == "#42: candidate CI failed"
    body = issue.create_comment.call_args.args[0]
    assert "[Candidate CI run 456](https://example/actions/runs/456)" in body
    assert "Inspect and rerun: macos." in body
    assert "[`macos` check](https://example/checks/macos)" in body


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


def test_unchanged_status_does_not_churn_comment_for_timestamp_only() -> None:
    issue = _issue()
    comment = MagicMock()
    comment.user.login = issue.user.login
    comment.body = "<!-- valkey-release-tracker:status -->\nsame\nStatus last changed 2026-08-20 08:25 UTC\n"
    issue.get_comments.return_value = [comment]

    tracker_mod._upsert_status(issue, "same\nStatus last changed 2026-08-20 09:30 UTC")

    comment.edit.assert_not_called()
    issue.create_comment.assert_not_called()


def test_pending_candidate_ci_is_linked_without_premature_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = _issue()
    agent = MagicMock()
    agent.default_branch = "e2e/latest-9.1.4-release-controller"
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

    workflow.create_dispatch.assert_not_called()
    assert result == "#42: candidate CI running"
    body = issue.create_comment.call_args.args[0]
    assert "[PR #7](https://example/pull/7)" in body
    assert f"[Candidate `{SHA[:12]}`](https://github.com/{TRACKER.repo}/commit/{SHA})" in body
    assert "[Candidate CI run 456](https://example/actions/runs/456)" in body
    assert "1 of 2 required checks passed" in body
    assert "Wait for: macos." in body
    assert "| Candidate CI |" in body
    assert "—" not in body
    assert not any(symbol in body for symbol in "✅❌⏳⛔🛑⚠️🟦🟥🟩⬜")
