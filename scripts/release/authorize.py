"""Live authorization against the release policy's configured principal.

Production policies use GitHub team membership. A fork-only E2E policy may
instead name one explicit user. Preparation and final approved publication
verify the acting user at execution time, and all failures fail closed.
Publication planning is non-writing and may be started automatically after the
canonical PR merge.
"""

from __future__ import annotations

from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import ReleasePolicy


class NotAuthorizedError(Exception):
    """The actor is not authorized by the configured release principal."""


def ensure_authorized(gh: Any, policy: ReleasePolicy, actor: str) -> None:
    """Raise :class:`NotAuthorizedError` unless *actor* is authorized.

    ``authorized_team`` is normally ``org/team-slug`` and membership is
    queried live. The fork-only ``user:LOGIN`` form performs an exact,
    case-insensitive login comparison without an organization lookup.

    Team lookup failures also refuse (fail closed), with a message naming the
    failed lookup, since "the token cannot read the org's teams" needs a
    different operator response than "not a member".
    """
    actor = actor.strip()
    if not actor:
        raise NotAuthorizedError("no acting user supplied")

    authorized_user = policy.authorized_user
    if authorized_user is not None:
        if actor.casefold() != authorized_user.casefold():
            raise NotAuthorizedError(
                f"@{actor} is not the authorized user @{authorized_user}; "
                f"only that user may perform release actions on {policy.repo}"
            )
        return

    try:
        team = retry_github_call(
            lambda: gh.get_organization(policy.team_org).get_team_by_slug(policy.team_slug),
            retries=2,
            description=f"resolve team {policy.authorized_team}",
        )
        user = retry_github_call(
            lambda: gh.get_user(actor),
            retries=2,
            description=f"resolve user {actor}",
        )
        is_member = retry_github_call(
            lambda: team.has_in_members(user),
            retries=2,
            description=f"check {actor} membership in {policy.authorized_team}",
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
