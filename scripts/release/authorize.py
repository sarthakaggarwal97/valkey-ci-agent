"""Live authorization against the release policy's GitHub teams.

Preparation and the final approved publication verify the acting user's
membership at execution time. Failures fail closed. Publication planning is
non-writing and may be started automatically after the canonical PR merge.
"""

from __future__ import annotations

from typing import Any

import requests
from github.GithubException import GithubException

from scripts.common.github_client import retry_github_call
from scripts.release.models import ReleasePolicy


class NotAuthorizedError(Exception):
    """The actor is not a member of any of the policy's authorized teams."""


def _membership_state(team: Any, actor: str) -> str | None:
    """The actor's membership state in *team*, or ``None`` for not a member.

    Uses the checked team-membership endpoint rather than
    ``Team.has_in_members``: that helper returns ``status == 204``, so a 403,
    a rate limit, or a server error all read as "not a member" without ever
    raising. Here only a 404 means non-membership; every other error
    propagates so the caller can record an unverifiable lookup, and
    retryable statuses actually get retried on the way.
    """
    try:
        return team.get_team_membership(actor).state
    except GithubException as exc:
        if exc.status == 404:
            return None
        raise


def ensure_authorized(gh: Any, policy: ReleasePolicy, actor: str) -> None:
    """Raise :class:`NotAuthorizedError` unless *actor* is authorized.

    Each ``authorized_teams`` entry is ``org/team-slug`` and membership is
    queried live; an ACTIVE membership in ANY listed team authorizes. A
    pending invitation is not a membership.

    Lookup failures also refuse (fail closed), with a message naming the
    failed lookup, since "the token cannot read the org's teams" needs a
    different operator response than "not a member". A lookup failure on one
    team never overrides a confirmed membership in another, whatever the
    order the teams are listed in: the failed team is recorded and the next
    is still consulted. With no confirmed membership, any failed lookup
    means the answer is unknown, so the check refuses as unverifiable rather
    than concluding "not a member" from a partial view.
    """
    actor = actor.strip()
    if not actor:
        raise NotAuthorizedError("no acting user supplied")

    lookup_failures: list[str] = []
    for team_full in policy.authorized_teams:
        org_name, team_slug = team_full.split("/", 1)
        try:
            team = retry_github_call(
                lambda: gh.get_organization(org_name).get_team_by_slug(team_slug),
                retries=2,
                description=f"resolve team {team_full}",
            )
            state = retry_github_call(
                lambda: _membership_state(team, actor),
                retries=2,
                description=f"check {actor} membership in {team_full}",
            )
        except GithubException as exc:
            lookup_failures.append(f"{team_full} (HTTP {exc.status})")
            continue
        except requests.RequestException as exc:
            # PyGithub's transport is `requests`; a connection or timeout
            # failure reaching one team must not decide for the others.
            lookup_failures.append(f"{team_full} ({type(exc).__name__})")
            continue
        if state == "active":
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
        f"only members of those teams may perform release actions on {policy.repo}"
    )
