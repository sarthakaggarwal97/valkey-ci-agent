"""Tests for the live team-membership authorization check."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests
from github.GithubException import GithubException

from scripts.release.authorize import NotAuthorizedError, ensure_authorized
from scripts.release.models import ReleasePolicy

# What the real endpoint does, mirrored by the mocks below:
# GET /orgs/{org}/teams/{team}/memberships/{user} returns a membership with a
# state ("active" or "pending") for members and invitees, and raises
# GithubException(404) for everyone else. 403/429/5xx raise too; they are
# never a membership answer.
_NOT_A_MEMBER = GithubException(404, "not found", {})


def _policy(*teams: str) -> ReleasePolicy:
    return ReleasePolicy(
        repo="valkey-io/valkey",
        authorized_teams=teams or ("valkey-io/core-team",),
        branches=("9.1",),
        checks_workflow="ci.yml",
        required_checks=("test",),
    )


_POLICY = _policy()
_TWO_TEAMS = _policy("valkey-io/valkey-committers", "valkey-io/valkey-release")


def _membership(state: str) -> MagicMock:
    membership = MagicMock()
    membership.state = state
    return membership


def _gh(member: bool = True) -> MagicMock:
    gh = MagicMock()
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    if member:
        team.get_team_membership.return_value = _membership("active")
    else:
        team.get_team_membership.side_effect = _NOT_A_MEMBER
    return gh


def _gh_per_team(memberships: dict[str, object]) -> MagicMock:
    """A mock whose membership answer depends on the team slug looked up.

    Values: a state string, None for the 404 non-member answer, or an
    exception instance to raise from the membership call.
    """
    gh = MagicMock()

    def by_slug(slug: str) -> MagicMock:
        team = MagicMock()
        answer = memberships[slug]
        if isinstance(answer, Exception):
            team.get_team_membership.side_effect = answer
        elif answer is None:
            team.get_team_membership.side_effect = _NOT_A_MEMBER
        else:
            team.get_team_membership.return_value = _membership(answer)
        return team

    gh.get_organization.return_value.get_team_by_slug.side_effect = by_slug
    return gh


def test_team_member_is_authorized() -> None:
    gh = _gh(member=True)
    ensure_authorized(gh, _POLICY, "madolson")
    gh.get_organization.assert_called_once_with("valkey-io")
    gh.get_organization.return_value.get_team_by_slug.assert_called_once_with("core-team")


def test_non_member_is_refused() -> None:
    with pytest.raises(NotAuthorizedError, match="not a member"):
        ensure_authorized(_gh(member=False), _POLICY, "drive-by")


def test_pending_invitation_is_not_a_membership() -> None:
    # An invitee has a membership object with state "pending"; until they
    # accept, they must not be able to publish a release.
    gh = _gh_per_team({"core-team": "pending"})
    with pytest.raises(NotAuthorizedError, match="not a member"):
        ensure_authorized(gh, _POLICY, "invited-but-not-accepted")


def test_member_of_the_second_team_is_authorized() -> None:
    # The valkey-release team grants exactly what valkey-committers does; a
    # release manager who is only in valkey-release must not be refused.
    gh = _gh_per_team({"valkey-committers": None, "valkey-release": "active"})
    ensure_authorized(gh, _TWO_TEAMS, "release-manager")


def test_member_of_no_listed_team_is_refused_naming_every_team() -> None:
    gh = _gh_per_team({"valkey-committers": None, "valkey-release": None})
    with pytest.raises(
        NotAuthorizedError,
        match="valkey-io/valkey-committers or valkey-io/valkey-release",
    ):
        ensure_authorized(gh, _TWO_TEAMS, "drive-by")


def test_confirmed_membership_survives_a_failed_lookup_on_another_team() -> None:
    # The grant is the confirmed membership; a 403 on a sibling team is not
    # evidence against it and must not turn a member away mid-release.
    gh = _gh_per_team({
        "valkey-committers": GithubException(403, "forbidden", {}),
        "valkey-release": "active",
    })
    ensure_authorized(gh, _TWO_TEAMS, "release-manager")


def test_confirmed_membership_survives_a_transport_failure_on_another_team() -> None:
    # PyGithub's transport is `requests`; a connection failure reaching the
    # first team is not a GithubException and must be contained per team,
    # not allowed to abort the loop before the member's own team is asked.
    gh = _gh_per_team({
        "valkey-committers": requests.ConnectionError("connection reset"),
        "valkey-release": "active",
    })
    ensure_authorized(gh, _TWO_TEAMS, "release-manager")


def test_no_grant_plus_a_failed_lookup_fails_closed() -> None:
    # "Not a member of the readable team" is a partial view when the other
    # team could not be read: the unreadable team might hold the membership,
    # so the check must refuse as unverifiable, not conclude non-membership.
    gh = _gh_per_team({
        "valkey-committers": None,
        "valkey-release": GithubException(403, "forbidden", {}),
    })
    with pytest.raises(NotAuthorizedError, match="could not verify membership"):
        ensure_authorized(gh, _TWO_TEAMS, "maybe-member")


def test_no_grant_plus_a_transport_failure_fails_closed() -> None:
    gh = _gh_per_team({
        "valkey-committers": None,
        "valkey-release": requests.Timeout("read timed out"),
    })
    with pytest.raises(NotAuthorizedError, match="could not verify membership"):
        ensure_authorized(gh, _TWO_TEAMS, "maybe-member")


def test_empty_actor_is_refused() -> None:
    with pytest.raises(NotAuthorizedError, match="no acting user"):
        ensure_authorized(_gh(), _POLICY, "  ")


def test_lookup_failure_fails_closed_with_actionable_message() -> None:
    gh = MagicMock()
    gh.get_organization.side_effect = GithubException(404, "gone", {})
    with pytest.raises(NotAuthorizedError, match="could not verify membership"):
        ensure_authorized(gh, _POLICY, "madolson")


def test_membership_call_failure_also_fails_closed() -> None:
    # The failure can happen on the LAST call of the chain, after the team
    # resolved fine; it must refuse, not authorize half-checked.
    gh = _gh_per_team({"core-team": GithubException(403, "forbidden", {})})
    with pytest.raises(NotAuthorizedError, match="could not verify membership"):
        ensure_authorized(gh, _POLICY, "madolson")


def test_membership_probe_is_the_checked_endpoint_never_the_unchecked_one() -> None:
    # Team.has_in_members returns `status == 204`, so a 403, a rate limit,
    # or a server error all read as "not a member" without raising; on this
    # gate that turns an unverifiable lookup into a definitive refusal (or
    # worse, skips retries a 429 deserved). The checked membership endpoint
    # raises for everything except the genuine 404 non-member answer.
    gh = _gh(member=True)
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    ensure_authorized(gh, _POLICY, "member")
    team.has_in_members.assert_not_called()
    team.get_members.assert_not_called()
    team.get_team_membership.assert_called_once_with("member")


def test_actor_whitespace_is_stripped_before_the_lookup() -> None:
    # Workflow inputs arrive as raw strings; '@ madolson ' pasted with
    # padding must query the real login, not a login with spaces.
    gh = _gh(member=True)
    ensure_authorized(gh, _POLICY, "  madolson  ")
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    team.get_team_membership.assert_called_once_with("madolson")
