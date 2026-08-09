"""Tests for deterministic version derivation from branch + tags."""

from __future__ import annotations

import pytest

from scripts.release.models import ReleaseIntent
from scripts.release.versioning import derive_version, parse_release_branch


class TestParseReleaseBranch:
    def test_accepts_release_lines(self) -> None:
        assert parse_release_branch("9.1") == (9, 1)
        assert parse_release_branch(" 10.0 ") == (10, 0)

    @pytest.mark.parametrize("branch", ["unstable", "main", "9.1.0", "9", "v9.1", ""])
    def test_rejects_non_release_branches(self, branch: str) -> None:
        with pytest.raises(ValueError, match="not a release branch"):
            parse_release_branch(branch)


class TestDeriveRC:
    def test_first_rc_on_fresh_line(self) -> None:
        derived = derive_version("9.1", ReleaseIntent.RC, ["9.0.0", "9.0.1"])
        assert (derived.version, derived.stage) == ("9.1.0", "rc1")
        assert derived.tag == "9.1.0-rc1"

    def test_next_rc_follows_existing_rcs(self) -> None:
        tags = ["9.1.0-rc1", "9.1.0-rc2", "9.0.0"]
        derived = derive_version("9.1", ReleaseIntent.RC, tags)
        assert (derived.version, derived.stage) == ("9.1.0", "rc3")

    def test_deterministic_regardless_of_tag_order(self) -> None:
        tags = ["9.1.0-rc2", "9.0.3", "9.1.0-rc1", "8.1.0"]
        assert derive_version("9.1", ReleaseIntent.RC, tags) == derive_version(
            "9.1", ReleaseIntent.RC, list(reversed(tags))
        )


class TestDeriveGA:
    def test_initial_ga(self) -> None:
        derived = derive_version("9.1", ReleaseIntent.GA, ["9.1.0-rc1", "9.1.0-rc2"])
        assert (derived.version, derived.stage) == ("9.1.0", "ga")
        assert derived.tag == "9.1.0"


@pytest.mark.parametrize("intent", [ReleaseIntent.RC, ReleaseIntent.GA])
@pytest.mark.parametrize("shipped_tag", ["9.1.0", "9.1.1"])
def test_rc_and_ga_refused_after_line_released(
    intent: ReleaseIntent, shipped_tag: str,
) -> None:
    # Any final release on the line closes the rc/ga window. That includes a
    # line without a .0 tag: a deleted .0 tag (or a line seeded at .1) must
    # not reopen the rc window, since deriving 9.1.0-rc1 there would version
    # BELOW the shipped 9.1.1.
    with pytest.raises(ValueError, match="final release"):
        derive_version("9.1", intent, [shipped_tag])


class TestDerivePatch:
    def test_next_patch_after_initial_release(self) -> None:
        derived = derive_version("8.0", ReleaseIntent.PATCH, ["8.0.0", "8.0.1", "8.0.7"])
        assert (derived.version, derived.stage) == ("8.0.8", "ga")

    def test_patch_refused_on_unreleased_line(self) -> None:
        with pytest.raises(ValueError, match="no final release"):
            derive_version("9.1", ReleaseIntent.PATCH, ["9.1.0-rc1", "9.0.0"])

    def test_other_line_and_malformed_tags_ignored(self) -> None:
        tags = ["8.0.0", "8.1.9", "v8.0.5", "8.0.2-rc1", "junk", "8.0.1"]
        derived = derive_version("8.0", ReleaseIntent.PATCH, tags)
        # 8.1.9 is another line; v8.0.5 and junk are not valkey tags;
        # 8.0.2-rc1 is not a final release. Max final patch on 8.0 is 1.
        assert derived.version == "8.0.2"


def test_security_intent_never_derives() -> None:
    with pytest.raises(ValueError, match="security"):
        derive_version("9.1", ReleaseIntent.SECURITY, [])
