"""Tests for issue-triggered CI-fix authorization and evidence gating."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.ci_fix.issue_gate import (
    IssueFixInvocation,
    IssueFixRequest,
    IssueGateRejection,
    build_issue_fix_request,
    parse_issue_fix_invocation,
)

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/29132625592"
_JOB_URL = f"{_RUN_URL}/job/86490784462"


def _github(*, issue_body: str | None = None, run_branch: str = "unstable"):
    issue_body = issue_body or (
        "<!-- valkey-ci-agent:test-failure:abc -->\n"
        f"- `test-job`: [CI link]({_JOB_URL})"
    )
    comments = [
        SimpleNamespace(
            body=(
                "Test failed again.\n"
                "https://github.com/valkey-io/valkey/actions/runs/30000000000/job/2"
            ),
            user=SimpleNamespace(login="valkeyrie-ops[bot]", type="Bot"),
        )
    ]
    issue = SimpleNamespace(
        number=4149,
        title="[TEST-FAILURE] COPY Preserves TTLs",
        body=issue_body,
        state="open",
        pull_request=None,
        labels=[SimpleNamespace(name="test-failure")],
        html_url="https://github.com/valkey-io/valkey/issues/4149",
        user=SimpleNamespace(login="valkeyrie-ops[bot]", type="Bot"),
        get_comments=lambda: comments,
    )
    run = SimpleNamespace(
        id=30000000000,
        status="completed",
        conclusion="failure",
        head_branch=run_branch,
        head_sha="a" * 40,
        head_repository=SimpleNamespace(full_name="valkey-io/valkey"),
    )
    repo = MagicMock(
        default_branch="unstable",
        get_issue=MagicMock(return_value=issue),
        get_workflow_run=MagicMock(return_value=run),
        get_branch=MagicMock(
            return_value=SimpleNamespace(commit=SimpleNamespace(sha="b" * 40))
        ),
        compare=MagicMock(return_value=SimpleNamespace(status="ahead")),
        get_pulls=MagicMock(return_value=[]),
    )
    team = MagicMock()
    team.get_team_membership.return_value = SimpleNamespace(state="active")
    org = MagicMock()
    org.get_team_by_slug.return_value = team
    gh = MagicMock()
    gh.get_repo.return_value = repo
    gh.get_organization.return_value = org
    return gh, repo, issue, run


def test_parse_issue_invocation_supports_embedded_or_fallback_run():
    parsed = parse_issue_fix_invocation(
        f"@valkeyrie-ops fix {_JOB_URL} inspect timing\nmore discussion"
    )
    assert parsed == IssueFixInvocation(run_url=_JOB_URL, hint="inspect timing")
    assert parse_issue_fix_invocation("@valkeyrie-ops fix inspect timing") == (
        IssueFixInvocation(hint="inspect timing")
    )
    assert parse_issue_fix_invocation("@valkeyrie-ops fix https://example.com/no") is None
    assert parse_issue_fix_invocation("@valkeyrie-ops fixup") is None
    assert parse_issue_fix_invocation("please fix this") is None
    assert parse_issue_fix_invocation(
        f"@valkeyrie-ops fix\t{_JOB_URL}\tinspect timing"
    ) == IssueFixInvocation(run_url=_JOB_URL, hint="inspect timing")


def test_gate_uses_latest_issue_ci_link_and_builds_default_branch_request():
    gh, repo, _issue, _run = _github()
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(hint="inspect timing"),
    )

    assert isinstance(result, IssueFixRequest)
    assert result.detector_issue is True
    assert result.fix_request.run_id == 30000000000
    assert result.fix_request.head_sha == "a" * 40
    assert result.fix_request.head_branch == "unstable"
    assert result.branch_name == "agent/ci-fix/issue-4149-run-30000000000"
    assert "Comment by @valkeyrie-ops[bot]" in result.issue_evidence
    repo.compare.assert_called_once_with("a" * 40, "b" * 40)


def test_gate_honors_explicit_run_url():
    gh, repo, _issue, run = _github()
    run.id = 29132625592
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(run_url=_JOB_URL),
    )

    assert isinstance(result, IssueFixRequest)
    assert result.fix_request.run_id == 29132625592
    repo.get_workflow_run.assert_called_once_with(29132625592)


def test_gate_rejects_non_detector_issue():
    gh, _repo, issue, _run = _github(issue_body="ordinary issue")
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )
    assert isinstance(result, IssueGateRejection)
    assert "marker" in result.reason
    issue.labels = []
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )
    assert isinstance(result, IssueGateRejection)
    assert "label" in result.reason


def test_gate_accepts_explicit_run_link_on_ordinary_open_issue():
    gh, _repo, issue, _run = _github(issue_body="ordinary issue")
    issue.labels = []
    issue.user = SimpleNamespace(login="reporter", type="User")

    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(run_url=_RUN_URL),
    )

    assert isinstance(result, IssueFixRequest)
    assert result.detector_issue is False
    assert result.fix_request.run_id == 29132625592


def test_gate_requires_detector_bot_issue_author():
    gh, _repo, issue, _run = _github()
    issue.user = SimpleNamespace(login="mallory", type="User")

    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )

    assert isinstance(result, IssueGateRejection)
    assert "configured detector App" in result.reason


def test_gate_rejects_different_github_app_bot_as_detector():
    gh, _repo, issue, _run = _github()
    issue.user = SimpleNamespace(login="unrelated-app[bot]", type="Bot")

    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )

    assert isinstance(result, IssueGateRejection)
    assert "valkeyrie-ops[bot]" in result.reason


def test_gate_rejects_non_default_or_diverged_run():
    gh, repo, _issue, _run = _github(run_branch="feature")
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(run_url=_RUN_URL),
    )
    assert isinstance(result, IssueGateRejection)
    assert "default branch" in result.reason

    gh, repo, _issue, _run = _github()
    repo.compare.return_value.status = "diverged"
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(run_url=_RUN_URL),
    )
    assert isinstance(result, IssueGateRejection)
    assert "no longer an ancestor" in result.reason


def test_gate_rejects_completed_successful_run():
    gh, _repo, _issue, run = _github()
    run.conclusion = "success"
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(run_url=_RUN_URL),
    )
    assert isinstance(result, IssueGateRejection)
    assert "did not end in a failure" in result.reason


def test_gate_ignores_run_links_from_non_detector_comments():
    gh, repo, issue, run = _github()
    detector_comments = issue.get_comments()
    issue.get_comments = lambda: [
        *detector_comments,
        SimpleNamespace(
            body=(
                "try this instead "
                "https://github.com/valkey-io/valkey/actions/runs/39999999999"
            ),
            user=SimpleNamespace(login="mallory", type="User"),
        ),
    ]
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )
    assert isinstance(result, IssueFixRequest)
    assert result.fix_request.run_id == run.id
    repo.get_workflow_run.assert_called_once_with(run.id)


def test_gate_ignores_run_links_from_different_bot_comments():
    gh, repo, issue, run = _github()
    detector_comments = issue.get_comments()
    issue.get_comments = lambda: [
        *detector_comments,
        SimpleNamespace(
            body=(
                "try this instead "
                "https://github.com/valkey-io/valkey/actions/runs/39999999999"
            ),
            user=SimpleNamespace(login="unrelated-app[bot]", type="Bot"),
        ),
    ]

    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )

    assert isinstance(result, IssueFixRequest)
    assert result.fix_request.run_id == run.id
    repo.get_workflow_run.assert_called_once_with(run.id)


def test_gate_rejects_existing_issue_fix_pr():
    gh, repo, _issue, _run = _github()
    repo.get_pulls.return_value = [
        SimpleNamespace(html_url="https://github.com/valkey-io/valkey/pull/9999")
    ]
    result = build_issue_fix_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        commenter="alice",
        invocation=IssueFixInvocation(),
    )
    assert isinstance(result, IssueGateRejection)
    assert "already exists" in result.reason
    assert repo.get_pulls.call_args.kwargs["state"] == "all"
