"""Tests for issue-fix branch and draft-PR publication."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.ci_fix import push as push_mod
from scripts.ci_fix.issue_publish import create_issue_fix_pull_request
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    ReviewVerdict,
    RunResult,
)
from scripts.ci_fix.push import PushRefused, commit_and_push_issue_fix


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _proposal() -> FixProposal:
    return FixProposal(
        path=FixPath.AUTHOR,
        failing_check="COPY Preserves TTLs",
        root_cause="the fixture compares allocator-dependent memory sizes",
        reasoning="compare semantic fields instead",
        confidence=0.9,
        failing_job_hint="test-sanitizer",
        build_command="make",
        verify_command="./runtest --single unit/hashexpire",
    )


def _origin(tmp_path: Path) -> tuple[Path, Path]:
    work = tmp_path / "work"
    work.mkdir()
    _git(work, "init", "-q", "-b", "unstable")
    _git(work, "config", "user.name", "Test")
    _git(work, "config", "user.email", "test@example.com")
    (work / "test.tcl").write_text("old\n")
    _git(work, "add", ".")
    _git(work, "commit", "-qm", "base")
    bare = tmp_path / "origin.git"
    _git(work, "clone", "-q", "--bare", str(work), str(bare))
    _git(bare, "symbolic-ref", "HEAD", "refs/heads/unstable")
    return work, bare


def test_issue_fix_applies_reviewed_patch_to_latest_default_tip(tmp_path, monkeypatch):
    verified, bare = _origin(tmp_path)
    run_sha = _git(verified, "rev-parse", "HEAD").strip()
    (verified / "test.tcl").write_text("fixed\n")

    # Advance the default branch after the failing-run checkout. Publication
    # must retain this new commit and apply only the reviewed patch on top.
    latest = tmp_path / "latest"
    _git(tmp_path, "clone", "-q", str(bare), str(latest))
    _git(latest, "config", "user.name", "Test")
    _git(latest, "config", "user.email", "test@example.com")
    (latest / "new.txt").write_text("new default-branch work\n")
    _git(latest, "add", ".")
    _git(latest, "commit", "-qm", "advance default")
    _git(latest, "push", "-q", "origin", "unstable")

    def clone(_repo: str, dest: Path) -> None:
        _git(dest.parent, "clone", "-q", str(bare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", clone)
    monkeypatch.setattr(push_mod, "github_https_url", lambda _repo: str(bare))
    branch = "agent/ci-fix/issue-4149-run-29132625592"
    publication = commit_and_push_issue_fix(
        str(verified),
        repo_full_name="valkey-io/valkey",
        base_branch="unstable",
        branch_name=branch,
        issue_number=4149,
        run_id=29132625592,
        run_sha=run_sha,
        proposal=_proposal(),
        changed_paths=("test.tcl",),
        git_env={},
    )

    assert _git(bare, "show", f"{publication.commit_sha}:test.tcl") == "fixed\n"
    assert _git(bare, "show", f"{publication.commit_sha}:new.txt") == "new default-branch work\n"
    assert "Refs #4149" in _git(
        bare, "log", "-1", "--format=%B", publication.commit_sha
    )
    assert publication.base_sha == _git(latest, "rev-parse", "HEAD").strip()

    # A retry after PR creation failed must recover the exact bot-authored
    # branch instead of getting stuck on a non-fast-forward push.
    recovered = commit_and_push_issue_fix(
        str(verified),
        repo_full_name="valkey-io/valkey",
        base_branch="unstable",
        branch_name=branch,
        issue_number=4149,
        run_id=29132625592,
        run_sha=run_sha,
        proposal=_proposal(),
        changed_paths=("test.tcl",),
        git_env={},
    )
    assert recovered == publication


def test_issue_fix_rejects_non_namespaced_branch(tmp_path):
    with pytest.raises(PushRefused, match="agent/ci-fix/issue-"):
        push_mod.commit_and_push_issue_patch(
            "diff --git a/a b/a\n",
            repo_full_name="valkey-io/valkey",
            base_branch="unstable",
            branch_name="feature/fix",
            issue_number=1,
            run_id=3,
            run_sha="a" * 40,
            proposal=_proposal(),
            git_env={},
        )


def test_issue_fix_rejects_wrong_or_mismatched_issue_branch(tmp_path):
    for branch, message in (
        ("agent/ci-fix/issue-1-extra", "<issue>-run-<run>"),
        ("agent/ci-fix/issue-2-run-3", "does not match"),
        ("agent/ci-fix/issue-1-run-4", "run ID does not match"),
    ):
        with pytest.raises(PushRefused, match=message):
            push_mod.commit_and_push_issue_patch(
                "diff --git a/a b/a\n",
                repo_full_name="valkey-io/valkey",
                base_branch="unstable",
                branch_name=branch,
                issue_number=1,
                run_id=3,
                run_sha="a" * 40,
                proposal=_proposal(),
                git_env={},
            )


def test_issue_fix_rechecks_run_ancestry_before_publication(tmp_path, monkeypatch):
    verified, bare = _origin(tmp_path)
    (verified / "test.tcl").write_text("fixed\n")

    def clone(_repo: str, dest: Path) -> None:
        _git(dest.parent, "clone", "-q", str(bare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", clone)
    with pytest.raises(PushRefused, match="no longer an ancestor"):
        commit_and_push_issue_fix(
            str(verified),
            repo_full_name="valkey-io/valkey",
            base_branch="unstable",
            branch_name="agent/ci-fix/issue-4149-run-29132625592",
            issue_number=4149,
            run_id=29132625592,
            run_sha="f" * 40,
            proposal=_proposal(),
            changed_paths=("test.tcl",),
            git_env={},
        )


def test_issue_handoff_rejects_paths_outside_reviewed_set(tmp_path, monkeypatch):
    verified, bare = _origin(tmp_path)
    run_sha = _git(verified, "rev-parse", "HEAD").strip()
    (verified / "test.tcl").write_text("fixed\n")
    patch = _git(verified, "diff", "--binary", "--", "test.tcl")

    def clone(_repo: str, dest: Path) -> None:
        _git(dest.parent, "clone", "-q", str(bare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", clone)
    with pytest.raises(PushRefused, match="unexpected paths"):
        push_mod.commit_and_push_issue_patch(
            patch,
            repo_full_name="valkey-io/valkey",
            base_branch="unstable",
            branch_name="agent/ci-fix/issue-4149-run-29132625592",
            issue_number=4149,
            run_id=29132625592,
            run_sha=run_sha,
            proposal=_proposal(),
            expected_paths=("other.txt",),
            git_env={},
        )


def test_create_issue_fix_pull_request_is_draft_and_linked():
    pull = SimpleNamespace(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
    )
    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.create_pull.return_value = pull
    gh = MagicMock()
    gh.get_repo.return_value = repo
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="baseline did not reproduce",
        proposal=_proposal(),
        run_result=RunResult(True, True, 0, "./runtest", "ok"),
        review=ReviewVerdict(True, "minimal causal fix"),
        verify_backend="local",
    )

    publication = create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=outcome,
        verified=False,
    )

    assert publication.number == 5000
    kwargs = repo.create_pull.call_args.kwargs
    assert kwargs["draft"] is True
    assert kwargs["base"] == "unstable"
    assert kwargs["head"].startswith("agent/ci-fix/issue-")
    assert "Fixes #4149" in kwargs["body"]
    assert "did not establish a failing baseline" in kwargs["body"]


def test_existing_ready_issue_fix_pull_is_not_moved_back_to_draft():
    pull = MagicMock(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
        draft=False,
    )
    repo = MagicMock()
    repo.get_pulls.return_value = [pull]
    gh = MagicMock()
    gh.get_repo.return_value = repo

    publication = create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=FixOutcome(kind=OutcomeKind.HANDOFF, summary="handoff"),
        verified=False,
    )

    assert publication.number == 5000
    pull.convert_to_draft.assert_not_called()
    repo.create_pull.assert_not_called()


def test_ambiguous_create_failure_recovers_existing_draft():
    pull = MagicMock(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
        draft=False,
    )
    repo = MagicMock()
    repo.get_pulls.side_effect = [[], [pull]]
    repo.create_pull.side_effect = GithubException(
        500,
        {"message": "response lost after create"},
        None,
    )
    gh = MagicMock()
    gh.get_repo.return_value = repo

    publication = create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=FixOutcome(kind=OutcomeKind.HANDOFF, summary="handoff"),
        verified=False,
        retries=1,
    )

    assert publication.number == 5000
    assert repo.get_pulls.call_count == 2
    pull.convert_to_draft.assert_not_called()


def test_unrunnable_handoff_pr_does_not_claim_post_change_passes():
    pull = SimpleNamespace(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
    )
    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.create_pull.return_value = pull
    gh = MagicMock()
    gh.get_repo.return_value = repo
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="verification could not run",
        proposal=_proposal(),
        run_result=SimpleNamespace(ran=False, passed=False),
        review=ReviewVerdict(True, "minimal causal fix"),
    )

    create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=outcome,
        verified=False,
    )

    body = repo.create_pull.call_args.kwargs["body"]
    assert "could not produce a reliable post-change test verdict" in body
    assert "passed repeated post-change runs" not in body


def test_historical_verification_defers_to_pull_request_ci():
    pull = SimpleNamespace(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
    )
    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.create_pull.return_value = pull
    gh = MagicMock()
    gh.get_repo.return_value = repo
    outcome = FixOutcome(
        kind=OutcomeKind.PUSHED,
        summary="verified against the failing run",
        proposal=_proposal(),
        run_result=RunResult(True, True, 0, "./runtest", "ok"),
        review=ReviewVerdict(True, "minimal causal fix"),
        verify_backend="local",
    )

    create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=outcome,
        verified=False,
        verification_sha="a" * 40,
    )

    body = repo.create_pull.call_args.kwargs["body"]
    assert "failing-run commit `aaaaaaaaaaaa`" in body
    assert "default branch had advanced" in body
    assert "pull request CI" in body


def test_explicit_link_pr_references_without_closing_ordinary_issue():
    pull = SimpleNamespace(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
    )
    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.create_pull.return_value = pull
    gh = MagicMock()
    gh.get_repo.return_value = repo

    create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=FixOutcome(
            kind=OutcomeKind.PUSHED,
            summary="pushed",
            proposal=_proposal(),
            verify_backend="local",
        ),
        verified=True,
        close_issue=False,
    )

    body = repo.create_pull.call_args.kwargs["body"]
    assert "Refs #4149" in body
    assert "Fixes #4149" not in body


def test_generated_pr_text_cannot_create_mentions_or_closing_references():
    pull = SimpleNamespace(
        number=5000,
        html_url="https://github.com/valkey-io/valkey/pull/5000",
    )
    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.create_pull.return_value = pull
    gh = MagicMock()
    gh.get_repo.return_value = repo
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="bad [check](https://example.com) @team",
        root_cause="Fixes #999\n@valkey-io/security [details](https://example.com)",
        reasoning="minimal",
        confidence=0.9,
    )

    create_issue_fix_pull_request(
        gh,
        repo_full_name="valkey-io/valkey",
        issue_number=4149,
        issue_url="https://github.com/valkey-io/valkey/issues/4149",
        run_id=29132625592,
        base_branch="unstable",
        branch_name="agent/ci-fix/issue-4149-run-29132625592",
        outcome=FixOutcome(
            kind=OutcomeKind.PUSHED,
            summary="pushed",
            proposal=proposal,
            review=ReviewVerdict(True, "Approved by @reviewers for #1000"),
            verify_backend="local",
        ),
        verified=True,
    )

    kwargs = repo.create_pull.call_args.kwargs
    assert "@team" not in kwargs["title"]
    assert "Fixes #999" not in kwargs["body"]
    assert "@valkey-io/security" not in kwargs["body"]
    assert "@reviewers" not in kwargs["body"]
    assert "Fixes #4149" in kwargs["body"]
