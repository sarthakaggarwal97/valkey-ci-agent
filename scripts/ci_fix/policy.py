"""Shared publication capabilities and ownership policy for CI fixes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from scripts.ci_fix.models import CandidateAction, FixPolicy, FixTask, TargetKind
from scripts.common.identity import APP_LOGIN

WRITABLE_BRANCH_PREFIXES = ("backport/", "agent/backport/", "agent/ci-fix/")
BACKPORT_BRANCH_PREFIXES = ("backport/", "agent/backport/")
CiFixAgentProfile = Literal["ci_fix_edit", "ci_fix_suggest"]


@dataclass(frozen=True)
class TargetCapabilities:
    """Behavior that every consumer must agree on for one target kind."""

    agent_profile: CiFixAgentProfile
    publication_guidance: str
    allowed_actions: frozenset[CandidateAction]
    patch_limit: int

    def allows(self, action: CandidateAction) -> bool:
        return action in self.allowed_actions


TARGET_CAPABILITIES: dict[TargetKind, TargetCapabilities] = {
    TargetKind.UPDATE_PR: TargetCapabilities(
        agent_profile="ci_fix_edit",
        publication_guidance=(
            "A model-authored patch will be posted for maintainer review and "
            "will not be pushed. An exact fix already merged on the trusted "
            "default branch may be ported to the existing bot-owned PR branch."
        ),
        allowed_actions=frozenset(
            {
                CandidateAction.PATCH,
                CandidateAction.PORT,
                CandidateAction.REFUSE,
            }
        ),
        patch_limit=1_000_000,
    ),
    TargetKind.SUGGEST: TargetCapabilities(
        agent_profile="ci_fix_suggest",
        publication_guidance=(
            "This is not a bot-owned branch. Prepare a patch for a PR comment "
            "only; nothing will be pushed."
        ),
        allowed_actions=frozenset(
            {
                CandidateAction.PATCH,
                CandidateAction.REFUSE,
            }
        ),
        patch_limit=45_000,
    ),
    TargetKind.DRAFT_PR: TargetCapabilities(
        agent_profile="ci_fix_edit",
        publication_guidance=(
            "A model-authored patch will be posted on the issue for maintainer "
            "review and will not be pushed. An exact fix already merged on the "
            "trusted default branch may be ported to a new bot-owned draft PR."
        ),
        allowed_actions=frozenset(
            {
                CandidateAction.PATCH,
                CandidateAction.PORT,
                CandidateAction.REFUSE,
            }
        ),
        patch_limit=1_000_000,
    ),
}


def target_capabilities(kind: TargetKind) -> TargetCapabilities:
    """Return the exhaustive capability record for ``kind``."""
    return TARGET_CAPABILITIES[kind]


def is_bot_author(author: str) -> bool:
    """Whether ``author`` is the App, whose namespaced PR branches the agent may update."""
    return author == f"{APP_LOGIN}[bot]"


def is_writable_branch(branch: str) -> bool:
    return branch.startswith(WRITABLE_BRANCH_PREFIXES)


def is_backport_branch(branch: str) -> bool:
    return branch.startswith(BACKPORT_BRANCH_PREFIXES)


def allows_ci_changes(task: FixTask) -> bool:
    """Whether ``task`` may port a merged upstream ``.github/`` change.

    This never authorizes a model-authored patch. Only an exact commit already
    merged on the trusted default branch may use the capability, and only on a
    bot-owned backport branch: a release branch often lacks that upstream CI
    change. Everywhere else a person applies workflow changes.
    """
    target = task.target
    return (
        task.policy is FixPolicy.BACKPORT
        and target.kind is TargetKind.UPDATE_PR
        and target.execution_trusted
        and is_backport_branch(target.branch)
    )


def can_update_branch(*, same_repo: bool, branch: str, author: str) -> bool:
    """Whether a validated PR branch is eligible for an automated update."""
    return same_repo and is_writable_branch(branch) and is_bot_author(author)
