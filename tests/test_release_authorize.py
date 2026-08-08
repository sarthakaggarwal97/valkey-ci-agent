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


def test_user_form_authorizes_exactly_that_user_without_api_calls() -> None:
    policy = make_policy(authorized_team="user:sarthakaggarwal97")
    gh = MagicMock()
    ensure_authorized(gh, policy, "SarthakAggarwal97")  # case-insensitive
    gh.get_organization.assert_not_called()
    with pytest.raises(NotAuthorizedError, match="authorized user"):
        ensure_authorized(gh, policy, "drive-by")
