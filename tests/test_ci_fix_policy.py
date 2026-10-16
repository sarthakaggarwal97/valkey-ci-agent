from __future__ import annotations

from scripts.ci_fix.models import CandidateAction, TargetKind
from scripts.ci_fix.policy import (
    TARGET_CAPABILITIES,
    can_update_branch,
    is_backport_branch,
    target_capabilities,
)


def test_every_target_kind_has_capabilities() -> None:
    assert set(TARGET_CAPABILITIES) == set(TargetKind)


def test_target_capabilities_capture_current_publication_modes() -> None:
    assert target_capabilities(TargetKind.SUGGEST).allows_push is False
    assert not target_capabilities(TargetKind.SUGGEST).allows(
        CandidateAction.PORT
    )
    assert target_capabilities(TargetKind.UPDATE_PR).allows(
        CandidateAction.PORT
    )
    assert target_capabilities(TargetKind.UPDATE_PR).creates_branch is False
    assert target_capabilities(TargetKind.DRAFT_PR).creates_branch is True


def test_bot_branch_ownership_and_backport_policy(monkeypatch) -> None:
    monkeypatch.setenv("CI_FIX_BOT_AUTHORS", "ci-bot[bot],other-bot[bot]")

    assert can_update_branch(
        same_repo=True,
        branch="agent/ci-fix/issue-1",
        author="ci-bot[bot]",
    )
    assert not can_update_branch(
        same_repo=False,
        branch="agent/ci-fix/issue-1",
        author="ci-bot[bot]",
    )
    assert is_backport_branch("agent/backport/sweep/8.0")
    assert not is_backport_branch("agent/ci-fix/issue-1")
