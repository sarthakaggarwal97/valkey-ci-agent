"""Tests for the live team-membership authorization check."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.release.authorize import NotAuthorizedError, ensure_authorized
from tests.release_fixtures import make_policy

_POLICY = make_policy()


def _gh(member: bool = True) -> MagicMock:
    gh = MagicMock()
    team = gh.get_organization.return_value.get_team_by_slug.return_value
    team.has_in_members.return_value = member
    return gh


def test_team_member_is_authorized() -> None:
    gh = _gh(member=True)
    ensure_authorized(gh, _POLICY, "madolson")
    gh.get_organization.assert_called_once_with("valkey-io")
    gh.get_organization.return_value.get_team_by_slug.assert_called_once_with("core-team")


def test_non_member_is_refused() -> None:
    with pytest.raises(NotAuthorizedError, match="not a member"):
        ensure_authorized(_gh(member=False), _POLICY, "drive-by")


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


def test_user_form_authorizes_exactly_that_user_without_api_calls() -> None:
    policy = make_policy(authorized_team="user:sarthakaggarwal97")
    gh = MagicMock()
    ensure_authorized(gh, policy, "SarthakAggarwal97")  # case-insensitive
    gh.get_organization.assert_not_called()
    with pytest.raises(NotAuthorizedError, match="authorized user"):
        ensure_authorized(gh, policy, "drive-by")


def test_user_form_rejects_prefix_and_suffix_lookalikes() -> None:
    # GitHub logins are case-insensitive but never substring-equivalent:
    # 'sarthakaggarwal97x' and 'sarthakaggarwal9' must both be refused.
    policy = make_policy(authorized_team="user:sarthakaggarwal97")
    for lookalike in ("sarthakaggarwal97x", "sarthakaggarwal9"):
        with pytest.raises(NotAuthorizedError, match="authorized user"):
            ensure_authorized(MagicMock(), policy, lookalike)
