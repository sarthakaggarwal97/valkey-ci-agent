"""Tests for the authorization and integrity gate.

The gate is the security boundary, so the tests lean on the refusal paths:
malformed commands, non-members, cross-repo runs, and moved branches must all
fail closed.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import gate as gate_mod
from scripts.ci_fix.gate import (
    GateRejection,
    ParsedCommand,
    build_fix_request,
    is_authorized,
    parse_command,
)
from scripts.ci_fix.models import FixRequest, Policy, Publication, request_from_dict, to_dict
from scripts.ci_fix.verify.base import FailedJob

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"


# --- parse_command ---

def test_parse_command_basic():
    cmd = parse_command(f"@valkeyrie-bot fix {_RUN_URL}")
    assert cmd is not None
    assert cmd.run_owner == "valkey-io"
    assert cmd.run_repo == "valkey"
    assert cmd.run_id == 27559908167
    assert cmd.hint == ""


def test_parse_command_with_hint():
    cmd = parse_command(f"@valkeyrie-bot fix {_RUN_URL} look at the NAN payload")
    assert cmd is not None
    assert cmd.hint == "look at the NAN payload"


def test_parse_command_accepts_valkeyrie_ops():
    cmd = parse_command(f"@valkeyrie-ops fix {_RUN_URL}")
    assert cmd is not None
    assert cmd.run_owner == "valkey-io"
    assert cmd.run_id == 27559908167


def test_parse_command_case_insensitive():
    cmd = parse_command(f"@Valkeyrie-Bot FIX {_RUN_URL}")
    assert cmd is not None


def test_parse_command_ignores_unrelated_comment():
    assert parse_command("thanks, lgtm!") is None
    assert parse_command("@valkeyrie-bot please review") is None


def test_parse_command_ignores_quoted_mention_mid_comment():
    # The command must start the comment; quoting it in discussion must not fire.
    body = f"Did you try `@valkeyrie-bot fix {_RUN_URL}`? It worked for me."
    assert parse_command(body) is None


def test_parse_command_requires_run_url():
    assert parse_command("@valkeyrie-bot fix https://example.com/not-a-run") is None


# --- is_authorized (fail closed) ---

def _team_membership_gh(state):
    membership = SimpleNamespace(state=state)
    team = MagicMock()
    team.get_team_membership.return_value = membership
    org = MagicMock()
    org.get_team_by_slug.return_value = team
    gh = MagicMock()
    gh.get_organization.return_value = org
    return gh


def test_authorized_active_member():
    gh = _team_membership_gh("active")
    assert is_authorized(gh, "valkey-io", "contributors", "alice") is True


def test_pending_member_not_authorized():
    gh = _team_membership_gh("pending")
    assert is_authorized(gh, "valkey-io", "contributors", "bob") is False


def test_membership_read_error_fails_closed():
    gh = MagicMock()
    gh.get_organization.side_effect = RuntimeError("403 no permission")
    assert is_authorized(gh, "valkey-io", "contributors", "carol") is False


def test_empty_username_not_authorized():
    assert is_authorized(MagicMock(), "valkey-io", "contributors", "") is False


def test_allowlist_authorizes_without_team(monkeypatch):
    """An allowlisted login is authorized without a team read (fork testing)."""
    monkeypatch.setenv("CI_FIX_AUTH_ALLOWLIST", "alice, bob")
    gh = MagicMock()
    gh.get_organization.side_effect = AssertionError("team should not be queried")
    assert is_authorized(gh, "valkey-io", "contributors", "bob") is True


def test_allowlist_empty_by_default(monkeypatch):
    """With no allowlist set, only team membership authorizes."""
    monkeypatch.delenv("CI_FIX_AUTH_ALLOWLIST", raising=False)
    gh = _team_membership_gh("pending")
    assert is_authorized(gh, "valkey-io", "contributors", "carol") is False


# --- build_fix_request ---

def _gh_for_request(*, member_state="active", pr_head_sha="abc123",
                    pr_head_ref="agent/backport/sweep/8.0",
                    pr_head_repo="valkey-io/valkey",
                    run_head_sha="abc123", run_head_branch="agent/backport/sweep/8.0",
                    run_status="completed", run_conclusion="failure"):
    gh = _team_membership_gh(member_state)
    pr = SimpleNamespace(
        head=SimpleNamespace(
            sha=pr_head_sha, ref=pr_head_ref,
            repo=SimpleNamespace(full_name=pr_head_repo),
        ),
        base=SimpleNamespace(ref="8.0"),
    )
    run = SimpleNamespace(id=27559908167, head_sha=run_head_sha, head_branch=run_head_branch,
                          status=run_status, conclusion=run_conclusion)
    repo = MagicMock()
    repo.get_pull.return_value = pr
    repo.get_workflow_run.return_value = run
    gh.get_repo.return_value = repo
    return gh


def _cmd():
    return parse_command(f"@valkeyrie-bot fix {_RUN_URL}")


def test_build_fix_request_happy_path():
    gh = _gh_for_request()
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, FixRequest)
    assert result.head_sha == "abc123"
    assert result.run_id == 27559908167
    assert result.requested_by == "alice"


def test_build_fix_request_rejects_non_member():
    gh = _gh_for_request(member_state="pending")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="stranger",
    )
    assert isinstance(result, GateRejection)
    assert "not an active member" in result.reason


def test_build_fix_request_rejects_cross_repo_run():
    gh = _gh_for_request()
    other_cmd = parse_command(
        "@valkeyrie-bot fix https://github.com/someone/else/actions/runs/123"
    )
    result = build_fix_request(
        gh, command=other_cmd, pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "not this PR's repository" in result.reason


def _request_for(**kwargs) -> FixRequest:
    result = build_fix_request(
        _gh_for_request(**kwargs), command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, FixRequest)
    return result


def test_bot_backport_branch_is_pushed_to_under_the_backport_policy():
    result = _request_for(pr_head_ref="agent/backport/sweep/8.0")
    assert (result.publication, result.policy, result.execute) == (Publication.PUSH, Policy.BACKPORT, True)
    assert result.base_branch == "8.0"


def test_other_bot_branches_are_pushed_to_under_the_fix_policy():
    result = _request_for(pr_head_ref="agent/ci-fix/issue-1-2", run_head_branch="agent/ci-fix/issue-1-2")
    assert (result.publication, result.policy, result.execute) == (Publication.PUSH, Policy.FIX, True)


def test_contributor_branch_gets_a_suggestion_never_a_push():
    result = _request_for(pr_head_ref="fix-flaky-test", run_head_branch="fix-flaky-test")
    assert (result.publication, result.policy, result.execute) == (Publication.SUGGEST, Policy.FIX, True)


def test_fork_head_is_diagnosed_without_running_its_code():
    """A fork PR is never pushed to, and its code never runs on the bot's runner."""
    result = _request_for(pr_head_repo="someoneelse/valkey", pr_head_ref="agent/backport/x",
                          run_head_branch="agent/backport/x")
    assert result.head_repo_full_name == "someoneelse/valkey"
    assert (result.publication, result.execute) == (Publication.SUGGEST, False)


def test_build_fix_request_rejects_moved_branch():
    gh = _gh_for_request(pr_head_sha="newsha999", run_head_sha="oldsha111")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "moved" in result.reason


def test_build_fix_request_rejects_branch_mismatch():
    gh = _gh_for_request(
        pr_head_ref="agent/backport/sweep/8.0",
        run_head_branch="some-other-branch",
    )
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "does not match" in result.reason


def test_build_fix_request_rejects_empty_sha():
    """A missing PR or run head SHA must fail closed, not compare equal as ''."""
    gh = _gh_for_request(pr_head_sha="", run_head_sha="")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "head commit" in result.reason


def test_build_fix_request_rejects_in_progress_run():
    """A run still in progress has no downloadable logs yet - refuse with a retry hint."""
    gh = _gh_for_request(run_status="in_progress", run_conclusion="")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "not finished" in result.reason


def test_build_fix_request_accepts_completed_run_regardless_of_conclusion():
    """The gate no longer judges the run's overall conclusion.

    A completed run is accepted (even 'success' or 'cancelled'); whether there
    is a real failure to act on is decided per-job downstream in the pipeline.
    """
    gh = _gh_for_request(run_conclusion="cancelled")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, FixRequest)


def test_build_fix_request_refuses_unknown_head_repo():
    # If the PR head repo can't be determined, fail closed.
    gh = _gh_for_request(pr_head_repo="")
    result = build_fix_request(
        gh, command=_cmd(), pr_repo_full_name="valkey-io/valkey",
        pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection)
    assert "head repository" in result.reason


def test_hint_is_limited_to_the_invocation_line():
    url = "https://github.com/o/r/actions/runs/5"
    cmd = parse_command(f"@valkeyrie-bot fix {url} only the NAN test\n\nthanks all!")
    assert cmd is not None
    assert cmd.hint == "only the NAN test"


# --- run link optional ---

def test_bare_fix_and_hint_only_commands_parse_without_a_run():
    assert parse_command("@valkeyrie-ops fix") == gate_mod.ParsedCommand("", "", 0, "")
    cmd = parse_command("@valkeyrie-ops fix look at the valgrind timeout")
    assert (cmd.run_id, cmd.hint) == (0, "look at the valgrind timeout")


def test_a_non_run_url_is_rejected_rather_than_read_as_a_hint():
    assert parse_command("@valkeyrie-ops fix https://github.com/valkey-io/valkey/pull/12") is None
    assert parse_command("@valkeyrie-ops fixed it") is None


def _run(run_id, *, conclusion="failure", status="completed", head_sha="abc123"):
    return SimpleNamespace(id=run_id, head_sha=head_sha, head_branch="agent/backport/sweep/8.0",
                           status=status, conclusion=conclusion)


def test_without_a_link_the_most_deterministic_failure_on_the_head_is_chosen(monkeypatch):
    gh = _gh_for_request()
    gh.get_repo.return_value.get_workflow_runs.return_value = [
        _run(1), _run(2), _run(3, conclusion="success"), _run(4, status="in_progress", conclusion=""),
        _run(5, head_sha="stale"),
    ]
    # Runs 3-5 would win on job priority if their filters were dropped.
    jobs = {1: [FailedJob("test-sanitizer-address", "failure", 11)],
            2: [FailedJob("build-macos", "failure", 21)],
            3: [FailedJob("lint", "failure", 31)], 4: [FailedJob("lint", "failure", 41)],
            5: [FailedJob("lint", "failure", 51)]}
    monkeypatch.setattr(gate_mod, "failed_jobs_for_run", lambda _gh, _repo, run_id, **_k: jobs.get(run_id, []))

    result = build_fix_request(gh, command=parse_command("@valkeyrie-ops fix"),
                               pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice")
    assert isinstance(result, FixRequest)
    assert result.run_id == 2
    assert result.target == "the failure in job `build-macos`"
    assert result.job == "build-macos"


def test_without_a_link_and_no_failed_run_the_gate_explains(monkeypatch):
    gh = _gh_for_request()
    gh.get_repo.return_value.get_workflow_runs.return_value = [_run(1, conclusion="success")]
    monkeypatch.setattr(gate_mod, "failed_jobs_for_run", lambda *a, **k: [])
    result = build_fix_request(gh, command=parse_command("@valkeyrie-ops fix"),
                               pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice")
    assert isinstance(result, GateRejection)
    assert "no completed, failed CI run" in result.reason



def test_without_a_link_a_failed_run_listing_is_a_clear_refusal(monkeypatch):
    gh = _gh_for_request()
    gh.get_repo.return_value.get_workflow_runs.side_effect = RuntimeError("API down")
    result = build_fix_request(gh, command=parse_command("@valkeyrie-ops fix"),
                               pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice")
    assert isinstance(result, GateRejection)
    assert "no completed, failed CI run" in result.reason


@pytest.mark.parametrize("body, run_id, hint", [
    ("@valkeyrie-ops fix <https://github.com/valkey-io/valkey/actions/runs/123>", 123, ""),
    ("@valkeyrie-ops fix [run](https://github.com/valkey-io/valkey/actions/runs/123) flaky", 123, "flaky"),
    ("@valkeyrie-ops fix www.github.com/valkey-io/valkey/actions/runs/123", 123, ""),
    ("@valkeyrie-ops fix https://github.com/valkey-io/valkey/actions/runs/123\tonly the NAN test", 123,
     "only the NAN test"),
    ("@valkeyrie-ops fix look at https://github.com/valkey-io/valkey/actions/runs/9/job/10 please", 9,
     "look at please"),
])
def test_a_wrapped_or_placed_run_link_still_targets_that_run(body, run_id, hint):
    cmd = parse_command(body)
    assert cmd is not None
    assert (cmd.run_id, cmd.hint) == (run_id, hint)


@pytest.mark.parametrize("body", [
    "@valkeyrie-ops fix <https://github.com/valkey-io/valkey/pull/12>",
    "@valkeyrie-ops fix see https://example.com/log for details",
])
def test_a_line_with_only_non_run_links_is_not_a_command(body):
    assert parse_command(body) is None



@pytest.mark.parametrize("branch", ["agent/release-cut/9.0.4-ga", "agent/something-else/x"])
def test_other_automations_agent_branches_are_never_pushed_to(branch):
    """Release-cut PRs must not gain CI-fix commits; they get a suggestion instead."""
    result = _request_for(pr_head_ref=branch, run_head_branch=branch)
    assert (result.publication, result.policy) == (Publication.SUGGEST, Policy.FIX)


def test_a_job_link_keeps_the_job():
    command = parse_command(
        "@valkeyrie-ops fix https://github.com/valkey-io/valkey/actions/runs/123/job/456?pr=789 look at tcl"
    )
    assert (command.run_id, command.job_id, command.hint) == (123, 456, "look at tcl")
    assert parse_command("@valkeyrie-ops fix https://github.com/o/r/actions/runs/9").job_id == 0


def test_a_job_link_targets_that_job(monkeypatch):
    monkeypatch.setattr(gate_mod, "failed_jobs_for_run", lambda *a, **k: [
        FailedJob(name="test-ubuntu-latest", conclusion="failure", id=455),
        FailedJob(name="test-sanitizer-address", conclusion="failure", id=456),
    ])
    result = build_fix_request(
        _gh_for_request(), command=ParsedCommand("valkey-io", "valkey", 42, "", job_id=456),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
    )
    assert isinstance(result, FixRequest)
    assert result.target == "the failure in job `test-sanitizer-address`"
    assert result.job == "test-sanitizer-address"
    assert request_from_dict(to_dict(result)).job == "test-sanitizer-address"


def test_a_job_link_to_a_job_that_did_not_fail_is_refused(monkeypatch):
    monkeypatch.setattr(gate_mod, "failed_jobs_for_run", lambda *a, **k: [])
    result = build_fix_request(
        _gh_for_request(), command=ParsedCommand("valkey-io", "valkey", 42, "", job_id=999),
        pr_repo_full_name="valkey-io/valkey", pr_number=3988, commenter="alice",
    )
    assert isinstance(result, GateRejection) and "not a failed job of run 42" in result.reason
