"""Live authorization against the policy's GitHub team.

Every privileged entry point (start a release, adopt a moved branch head)
verifies the acting user's team membership against GitHub *at execution
time*. No decision is ever read back from issue text, labels, or comments
authored by others, and failures fail closed: if membership cannot be
established, the actor is not authorized.
"""

from __future__ import annotations

from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.policy import RepoReleasePolicy


class NotAuthorizedError(Exception):
    """The actor is not a member of the policy's authorized team."""


def ensure_authorized(gh: Any, policy: RepoReleasePolicy, actor: str) -> None:
    """Raise :class:`NotAuthorizedError` unless *actor* is authorized.

    ``authorized_team`` is normally ``org/team-slug`` and membership is
    queried live. The ``user:<login>`` form authorizes exactly that user;
    it exists for fork policies, where no organization team exists (a personal fork
    cannot have teams and a fork PAT cannot read the upstream org's).

    Lookup failures also refuse (fail closed), with a message naming the
    failed lookup, since "the token cannot read the org's teams" needs a
    different operator response than "not a member".
    """
    actor = actor.strip()
    if not actor:
        raise NotAuthorizedError("no acting user supplied")

    if policy.authorized_team.startswith("user:"):
        allowed = policy.authorized_team[len("user:"):]
        if actor.lower() != allowed.lower():
            raise NotAuthorizedError(
                f"@{actor} is not the policy's authorized user (@{allowed}); "
                f"only that user may perform release actions on {policy.repo}"
            )
        return

    try:
        team = retry_github_call(
            lambda: gh.get_organization(policy.team_org).get_team_by_slug(policy.team_slug),
            retries=2, description=f"resolve team {policy.authorized_team}",
        )
        user = retry_github_call(
            lambda: gh.get_user(actor),
            retries=2, description=f"resolve user {actor}",
        )
        is_member = retry_github_call(
            lambda: team.has_in_members(user),
            retries=2, description=f"check {actor} membership in {policy.authorized_team}",
        )
    except GithubException as exc:
        raise NotAuthorizedError(
            f"could not verify membership of @{actor} in {policy.authorized_team} "
            f"(HTTP {exc.status}); refusing (fail closed). The token must be able "
            f"to read {policy.team_org}'s teams (GitHub App: members:read; a 404 "
            f"can mean either the team does not exist or the token cannot see it)."
        ) from exc
    if not is_member:
        raise NotAuthorizedError(
            f"@{actor} is not a member of {policy.authorized_team}; "
            f"only that team may perform release actions on {policy.repo}"
        )
