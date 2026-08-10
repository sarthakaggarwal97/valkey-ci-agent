"""Tests for release issue rendering, discovery, and adoption-marker trust."""

from __future__ import annotations

from unittest.mock import MagicMock

from scripts.release import issue as issue_mod
from scripts.release.models import (
    Candidate,
    CandidateState,
    CheckState,
    ReleasePhase,
    ReleaseStatus,
    RequiredCheck,
)

_SHA_A = "a" * 40
_SHA_B = "b" * 40


def _status(**overrides: object) -> ReleaseStatus:
    base: "dict[str, object]" = {
        "repo": "valkey-io/valkey",
        "branch": "9.1",
        "version": "9.1.1",
        "stage": "ga",
        "notes_pr_number": 42,
        "notes_pr_url": "https://x/pull/42",
        "notes_pr_merged": True,
        "candidate": Candidate(
            state=CandidateState.CURRENT, sha=_SHA_A, branch_head=_SHA_A,
        ),
        "checks": (RequiredCheck(name="test-ubuntu-latest", state=CheckState.PASSED, url="https://x/run/1"),),
        "ready": True,
        "phase": ReleasePhase.READY,
        "blockers": (),
    }
    base.update(overrides)
    return ReleaseStatus(**base)  # type: ignore[arg-type]


class TestRender:
    def test_body_carries_identity_marker_and_is_deterministic(self) -> None:
        status = _status()
        body = issue_mod.render_body(status)
        assert issue_mod.identity_marker("9.1") in body
        assert body == issue_mod.render_body(status)

    def test_ready_body_shows_checks_and_readiness(self) -> None:
        body = issue_mod.render_body(_status())
        assert "| `test-ubuntu-latest` | ✅ Passed" in body
        assert "[!IMPORTANT]" in body
        assert "**Ready to publish.**" in body
        assert "- [x] Release notes cut and merged" in body  # progress checklist

    def test_blocked_body_lists_every_blocker(self) -> None:
        body = issue_mod.render_body(_status(ready=False, blockers=("one", "two")))
        assert "[!WARNING]" in body
        assert "**Not ready. Blocked on:**" in body
        assert "> - one" in body and "> - two" in body

    def test_invalidated_candidate_calls_for_adoption(self) -> None:
        candidate = Candidate(
            state=CandidateState.INVALIDATED, sha=_SHA_A, branch_head=_SHA_B,
        )
        body = issue_mod.render_body(_status(candidate=candidate, checks=(), ready=False,
                                             blockers=("branch moved",)))
        assert "**Invalidated**" in body

    def test_title_uses_version_or_branch(self) -> None:
        assert issue_mod.render_title("9.1", "9.1.0", "rc2") == "Release 9.1.0-rc2"
        assert issue_mod.render_title("9.1", "9.1.1", "ga") == "Release 9.1.1"
        assert issue_mod.render_title("9.1", "", "") == "Next release on 9.1"

    def test_published_candidate_renders_as_tag_pinned_not_branch_head(self) -> None:
        # After publication the branch may legitimately move on; the tracker
        # must not keep asserting the candidate is the current branch head.
        published = _status(
            candidate=Candidate(state=CandidateState.CURRENT, sha=_SHA_A,
                                branch_head=_SHA_B),
            phase=ReleasePhase.PUBLISHED, published=True,
            release_url="https://x/releases/9.1.1", ready=False,
        )
        body = issue_mod.render_body(published)
        assert "Pinned by the release tag" in body
        assert "Current branch head" not in body


def _comment(author: str, body: str) -> MagicMock:
    comment = MagicMock()
    comment.user.login = author
    comment.body = body
    return comment


class TestAdoptedShas:
    def test_bot_authored_markers_count(self) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]", f"{issue_mod.adopt_marker(_SHA_A)}\nadopted"),
        ]
        assert issue_mod.adopted_shas(tracker) == (_SHA_A,)

    def test_markers_from_other_authors_are_ignored(self) -> None:
        # A marker pasted by anyone else must not acknowledge a branch move:
        # commenting on (or editing) the issue cannot authorize actions.
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("some-drive-by-user", f"{issue_mod.adopt_marker(_SHA_A)}\nlgtm"),
            _comment("madolson", issue_mod.adopt_marker(_SHA_B)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()

    def test_bare_bot_logins_are_not_trusted(self) -> None:
        # App slugs and user accounts are distinct namespaces: an outsider
        # registering the bare username could post markers, so only the
        # "[bot]" forms are trusted.
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops", issue_mod.adopt_marker(_SHA_A)),
            _comment("valkeyrie-bot", issue_mod.adopt_marker(_SHA_B)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()


class TestFindReleaseIssue:
    def test_matches_marker_and_skips_pull_requests(self) -> None:
        marked = MagicMock(number=7)
        marked.body = issue_mod.identity_marker("9.1")
        marked._rawData = {}
        pull = MagicMock(number=8)
        pull.body = issue_mod.identity_marker("9.1")
        pull._rawData = {"pull_request": {}}
        other = MagicMock(number=9)
        other.body = issue_mod.identity_marker("8.0")
        other._rawData = {}
        repo = MagicMock()
        # The label-pair query trusts GitHub's server-side filter; model it:
        # no issue carries release:9.1, so the pair query is empty and the
        # marker fallback identifies the tracker.
        repo.get_issues.side_effect = lambda state, labels: (
            [] if "release:9.1" in labels else [pull, other, marked]
        )

        found = issue_mod.find_release_issue(repo, "9.1", label="release-tracker")

        assert found is marked
        first, second = repo.get_issues.call_args_list
        assert first.kwargs["labels"] == ["release-tracker", "release:9.1"]
        assert second.kwargs["labels"] == ["release-tracker"]

    def test_returns_none_when_no_tracker(self) -> None:
        repo = MagicMock()
        repo.get_issues.return_value = []
        assert issue_mod.find_release_issue(repo, "9.1", label="release-tracker") is None


class TestAesthetics:
    def test_header_carries_badges_and_progress_bar(self) -> None:
        body = issue_mod.render_body(_status())
        assert '<div align="center">' in body
        assert "img.shields.io/badge/version-9.1.1-" in body
        # Phase message slashes must be percent-encoded or the badge 404s.
        assert "img.shields.io/badge/phase-4%2F6" in body
        assert "🟩🟩🟩🟦⬜⬜" in body  # READY: three done, current, two ahead

    def test_progress_bar_turns_red_on_failure(self) -> None:
        failing = _status(
            ready=False,
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED),),
            blockers=("Required check failed",),
        )
        body = issue_mod.render_body(failing)
        assert "🟥" in body

    def test_rendered_body_never_contains_an_em_dash(self) -> None:
        from scripts.release.models import DownstreamOutput, OutputState
        variants = (
            _status(),
            _status(ready=False, blockers=("one", "two")),
            _status(notes_pr_merged=False, ready=False, blockers=("open",)),
            _status(
                candidate=Candidate(state=CandidateState.INVALIDATED,
                                    sha=_SHA_A, branch_head=_SHA_B),
                checks=(), ready=False, blockers=("moved",),
            ),
            _status(
                phase=ReleasePhase.PUBLISHED, published=True, ready=False,
                release_url="https://x/releases/9.1.1",
                outputs=(
                    DownstreamOutput(name="tarballs", state=OutputState.VERIFIED,
                                     detail="public"),
                    DownstreamOutput(name="helm", state=OutputState.FAILED,
                                     detail="closed without merging"),
                ),
            ),
        )
        for status in variants:
            assert "\u2014" not in issue_mod.render_body(status)


class TestAuthenticatedIdentityTrust:
    def test_pat_identity_comments_become_trusted(self) -> None:
        # Fork runs authenticate as the fork owner; markers the controller
        # wrote through that PAT must be readable back (the live dup-notify
        # bug this fixes).
        tracker = MagicMock()
        comment = _comment("sarthakaggarwal97", issue_mod.adopt_marker(_SHA_A))
        tracker.get_comments.return_value = [comment]
        gh = MagicMock()
        gh._release_controller_login = None
        gh.get_user.return_value.login = "sarthakaggarwal97"
        assert issue_mod.adopted_shas(tracker, gh) == (_SHA_A,)
        # Without the client context the same comment stays untrusted.
        assert issue_mod.adopted_shas(tracker) == ()

    def test_app_token_without_user_context_falls_back_to_bot_identities(self) -> None:
        from github.GithubException import GithubException
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]", issue_mod.adopt_marker(_SHA_A)),
        ]
        gh = MagicMock()
        gh._release_controller_login = None
        gh.get_user.side_effect = GithubException(403, "no user context", {})
        assert issue_mod.adopted_shas(tracker, gh) == (_SHA_A,)


class TestCommentMemo:
    def test_comments_fetched_once_per_issue_until_invalidated(self) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]", issue_mod.adopt_marker(_SHA_A)),
        ]
        assert issue_mod.adopted_shas(tracker) == (_SHA_A,)
        assert issue_mod.adopted_shas(tracker) == (_SHA_A,)
        tracker.get_comments.assert_called_once()

        issue_mod.invalidate_comment_memo(tracker)
        tracker.get_comments.return_value = []
        assert issue_mod.adopted_shas(tracker) == ()
        assert tracker.get_comments.call_count == 2

    def test_memo_caches_raw_comments_so_trust_stays_per_caller(self) -> None:
        # The memo stores the fetched list; the trust filter still runs per
        # call, so the same cached comments read differently with a PAT
        # identity in scope.
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("sarthakaggarwal97", issue_mod.adopt_marker(_SHA_A)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()
        gh = MagicMock()
        gh._release_controller_login = "sarthakaggarwal97"
        assert issue_mod.adopted_shas(tracker, gh) == (_SHA_A,)
        tracker.get_comments.assert_called_once()
