"""Tests for the live team-membership authorization check."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.release.authorize import NotAuthorizedError, ensure_authorized
from scripts.release.models import ReleasePolicy


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


def _gh(member: bool = True) -> MagicMock:
    gh = MagicMock()
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    team.has_in_members.return_value = member
    return gh


def _gh_per_team(memberships: dict[str, bool | Exception]) -> MagicMock:
    """A mock whose membership answer depends on the team slug looked up."""
    gh = MagicMock()

    def by_slug(slug: str) -> MagicMock:
        team = MagicMock()
        answer = memberships[slug]
        if isinstance(answer, Exception):
            team.has_in_members.side_effect = answer
        else:
            team.has_in_members.return_value = answer
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


def test_member_of_the_second_team_is_authorized() -> None:
    # The valkey-release team grants exactly what valkey-committers does; a
    # release manager who is only in valkey-release must not be refused.
    gh = _gh_per_team({"valkey-committers": False, "valkey-release": True})
    ensure_authorized(gh, _TWO_TEAMS, "release-manager")


def test_member_of_no_listed_team_is_refused_naming_every_team() -> None:
    gh = _gh_per_team({"valkey-committers": False, "valkey-release": False})
    with pytest.raises(
        NotAuthorizedError,
        match="valkey-io/valkey-committers or valkey-io/valkey-release",
    ):
        ensure_authorized(gh, _TWO_TEAMS, "drive-by")


def test_confirmed_membership_survives_a_failed_lookup_on_another_team() -> None:
    # The grant is the confirmed membership; a 404 on a sibling team is not
    # evidence against it and must not turn a member away mid-release.
    gh = _gh_per_team({
        "valkey-committers": GithubException(404, "gone", {}),
        "valkey-release": True,
    })
    ensure_authorized(gh, _TWO_TEAMS, "release-manager")


def test_no_grant_plus_a_failed_lookup_fails_closed() -> None:
    # "Not a member of the readable team" is a partial view when the other
    # team could not be read: the unreadable team might hold the membership,
    # so the check must refuse as unverifiable, not conclude non-membership.
    gh = _gh_per_team({
        "valkey-committers": False,
        "valkey-release": GithubException(403, "forbidden", {}),
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


def test_user_resolution_failure_fails_closed() -> None:
    gh = MagicMock()
    gh.get_user.side_effect = GithubException(404, "gone", {})
    with pytest.raises(NotAuthorizedError, match="could not resolve user"):
        ensure_authorized(gh, _POLICY, "ghost")


def test_membership_call_failure_also_fails_closed() -> None:
    # The failure can happen on the LAST call of the chain, after the team
    # and user resolved fine; it must refuse, not authorize half-checked.
    gh = _gh()
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    team.has_in_members.side_effect = GithubException(403, "forbidden", {})
    with pytest.raises(NotAuthorizedError, match="could not verify membership"):
        ensure_authorized(gh, _POLICY, "madolson")


def test_membership_is_a_server_side_probe_never_a_truncated_listing() -> None:
    # A member on page 2 of the team listing must still authorize: the
    # check has to be the has_in_members membership probe, not iteration
    # over get_members() (which a naive implementation could truncate).
    gh = _gh(member=True)
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    page_one = MagicMock()
    page_one.login = "someone-else"
    team.get_members.return_value = [page_one]  # actor absent from page 1
    ensure_authorized(gh, _POLICY, "member-on-page-2")
    team.get_members.assert_not_called()
    team.has_in_members.assert_called_once_with(gh.get_user.return_value)
    gh.get_user.assert_called_once_with("member-on-page-2")


def test_actor_whitespace_is_stripped_before_the_lookup() -> None:
    # Workflow inputs arrive as raw strings; '@ madolson ' pasted with
    # padding must resolve the real login, not a login with spaces.
    gh = _gh(member=True)
    ensure_authorized(gh, _POLICY, "  madolson  ")
    gh.get_user.assert_called_once_with("madolson")
