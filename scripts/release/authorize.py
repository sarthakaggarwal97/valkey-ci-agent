"""Live authorization against the release policy's GitHub teams.

Preparation and the final approved publication verify the acting user's
membership at execution time. Failures fail closed. Publication planning is
non-writing and may be started automatically after the canonical PR merge.
"""

from __future__ import annotations

from typing import Any

from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import ReleasePolicy


class NotAuthorizedError(Exception):
    """The actor is not a member of any of the policy's authorized teams."""


def ensure_authorized(gh: Any, policy: ReleasePolicy, actor: str) -> None:
    """Raise :class:`NotAuthorizedError` unless *actor* is authorized.

    Each ``authorized_teams`` entry is ``org/team-slug`` and membership is
    queried live; membership in ANY listed team authorizes.

    Lookup failures also refuse (fail closed), with a message naming the
    failed lookup, since "the token cannot read the org's teams" needs a
    different operator response than "not a member". A lookup failure on one
    team never overrides a confirmed membership in another: the confirmed
    membership is checked first because it is the grant, but with no grant
    the failed lookup means the answer is unknown, so the check refuses
    rather than concluding "not a member" from a partial view.
    """
    actor = actor.strip()
    if not actor:
        raise NotAuthorizedError("no acting user supplied")

    try:
        user = retry_github_call(
            lambda: gh.get_user(actor),
            retries=2,
            description=f"resolve user {actor}",
        )
    except GithubException as exc:
        raise NotAuthorizedError(
            f"could not resolve user @{actor} (HTTP {exc.status}); "
            f"refusing (fail closed)."
        ) from exc

    lookup_failures: list[str] = []
    for team_full in policy.authorized_teams:
        org_name, team_slug = team_full.split("/", 1)
        try:
            team = retry_github_call(
                lambda: gh.get_organization(org_name).get_team_by_slug(team_slug),
                retries=2,
                description=f"resolve team {team_full}",
            )
            is_member = retry_github_call(
                lambda: team.has_in_members(user),
                retries=2,
                description=f"check {actor} membership in {team_full}",
            )
        except GithubException as exc:
            lookup_failures.append(f"{team_full} (HTTP {exc.status})")
            continue
        if is_member:
            return

    if lookup_failures:
        raise NotAuthorizedError(
            f"could not verify membership of @{actor} in {', '.join(lookup_failures)}; "
            f"refusing (fail closed). The token must be able to read the org's "
            f"teams (GitHub App: members:read; a 404 can mean either the team "
            f"does not exist or the token cannot see it)."
        )
    raise NotAuthorizedError(
        f"@{actor} is not a member of {policy.authorized_teams_display}; "
        f"only those teams may perform release actions on {policy.repo}"
    )
