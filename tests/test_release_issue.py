"""Tests for release issue rendering, discovery, and adoption-marker trust."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from scripts.release import issue as issue_mod
from scripts.release.models import (
    Candidate,
    CandidateState,
    CheckState,
    DownstreamOutput,
    OutputState,
    ReleasePhase,
    ReleaseStatus,
    RequiredCheck,
)
from tests.release_fixtures import BEFORE_TRACKER

_SHA_A = "a" * 40
_SHA_B = "b" * 40

_NOW = datetime(2026, 8, 10, 17, 30, tzinfo=timezone.utc)


def _render(status: ReleaseStatus) -> str:
    return issue_mod.render_body(status, _NOW)


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
        body = _render(status)
        assert issue_mod.identity_marker("9.1") in body
        assert body == _render(status)

    def test_ready_body_shows_checks_and_readiness(self) -> None:
        body = _render(_status())
        assert "| `test-ubuntu-latest` | ✅ Passed" in body
        assert "[!IMPORTANT]" in body
        assert "**Ready to publish.**" in body
        assert "- [x] Release notes cut and merged" in body  # progress checklist

    def test_blocked_body_lists_every_blocker(self) -> None:
        body = _render(_status(ready=False, blockers=("one", "two")))
        assert "[!WARNING]" in body
        assert "**Not ready. Blocked on:**" in body
        assert "> - one" in body and "> - two" in body

    def test_invalidated_candidate_calls_for_adoption(self) -> None:
        candidate = Candidate(
            state=CandidateState.INVALIDATED, sha=_SHA_A, branch_head=_SHA_B,
        )
        body = _render(_status(candidate=candidate, checks=(), ready=False,
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
        body = _render(published)
        assert "Pinned by the release tag" in body
        assert "Current branch head" not in body

    def test_untrusted_tag_renders_the_candidate_as_untrusted(self) -> None:
        # A tag at a SHA that was never a trusted candidate must not read
        # as a valid pinned publication.
        untrusted = _status(
            candidate=Candidate(state=CandidateState.CURRENT, sha=_SHA_B,
                                branch_head=_SHA_A),
            phase=ReleasePhase.PUBLISHED, published=True, tag_trusted=False,
            release_url="https://x/releases/9.1.1", ready=False,
            alerts=("Release tag 9.1.1 points at bbbbbbbbbbbb, which was "
                    "never a trusted candidate (notes merge or "
                    "owner-adopted). Manual investigation required.",),
        )
        body = _render(untrusted)
        assert "**Untrusted**" in body
        assert "Pinned by the release tag" not in body
        assert issue_mod.has_failures(untrusted)

    def test_ready_callout_does_not_assert_tag_immutability(self) -> None:
        # Whether the tag is immutable depends on the repository's rulesets
        # (verified on the publish plan path); the callout must not claim it.
        body = _render(_status())
        assert "creates the release tag" in body
        assert "permanent tag" not in body

    def test_ready_callout_carries_the_approval_link_when_known(self) -> None:
        body = _render(_status(approval_run_url="https://x/actions/runs/500"))
        assert "> **Approve here:** https://x/actions/runs/500" in body

    def test_ready_callout_omits_the_approval_line_when_unknown(self) -> None:
        body = _render(_status())
        assert "Approve here:" not in body


class TestLiveTitle:
    @pytest.mark.parametrize("phase", list(ReleasePhase), ids=lambda p: p.name.lower())
    def test_title_is_constant_across_phases(self, phase: ReleasePhase) -> None:
        # Owner preference: the title never carries phase or state. The
        # phase lives inside the tracker; the list-level failure signal
        # is the needs-attention label.
        assert issue_mod.render_live_title(_status(phase=phase)) == "Release 9.1.1"

    def test_rc_title_carries_the_full_tag(self) -> None:
        title = issue_mod.render_live_title(
            _status(version="9.1.0", stage="rc2", phase=ReleasePhase.QUALIFICATION))
        assert title == "Release 9.1.0-rc2"

    def test_failures_never_change_the_title(self) -> None:
        failing = _status(
            ready=False, phase=ReleasePhase.QUALIFICATION,
            alerts=("The release metadata is broken; fix it.",),
            blockers=("The release metadata is broken; fix it.",),
        )
        assert issue_mod.render_live_title(failing) == "Release 9.1.1"


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

    # Trust is an exact-string membership check on the author login. Every
    # lookalike of the trusted "[bot]" forms must stay untrusted: bare app
    # slugs and user accounts are distinct namespaces (an outsider could
    # register the bare name), and GitHub returns canonical login casing so
    # a case or whitespace variant can only be a spoof.
    @pytest.mark.parametrize("author", [
        pytest.param("some-drive-by-user", id="drive-by"),
        pytest.param("madolson", id="maintainer"),
        pytest.param("valkeyrie-ops", id="bare-app-slug"),
        pytest.param("valkeyrie-bot", id="bare-bot-login"),
        pytest.param("Valkeyrie-Ops[bot]", id="case-variant"),
        pytest.param("valkeyrie-ops[bot] ", id="trailing-space"),
        pytest.param("valkeyrie-ops[bot]x", id="suffix-lookalike"),
        pytest.param("", id="empty-login"),
    ])
    def test_untrusted_authors_cannot_forge_an_adoption(self, author: str) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment(author, issue_mod.adopt_marker(_SHA_A)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()

    def test_deleted_user_comment_is_untrusted(self) -> None:
        # A comment whose author account was deleted has user None; the
        # login getter must degrade to "" and stay outside the trusted set.
        ghost = MagicMock()
        ghost.user = None
        ghost.body = issue_mod.adopt_marker(_SHA_A)
        tracker = MagicMock()
        tracker.get_comments.return_value = [ghost]
        assert issue_mod.adopted_shas(tracker) == ()

    @pytest.mark.parametrize("env_value", [
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
    ])
    def test_blank_release_bot_login_adds_nothing_to_the_trusted_set(
            self, monkeypatch: pytest.MonkeyPatch, env_value: str) -> None:
        # A workflow exporting RELEASE_BOT_LOGIN="" (unset secret, template
        # miss) must not add "" to the trusted set, or every ghost/empty
        # login would become a trusted author.
        monkeypatch.setenv("RELEASE_BOT_LOGIN", env_value)
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("", issue_mod.adopt_marker(_SHA_A)),
            _comment(env_value, issue_mod.adopt_marker(_SHA_B)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()

    # The marker regex demands exactly 40 lowercase hex characters; any
    # malformed SHA in a marker (even one a trusted author posted) must not
    # record an adoption of anything.
    @pytest.mark.parametrize("sha_text", [
        pytest.param(_SHA_A[:39], id="39-chars"),
        pytest.param(_SHA_A.upper(), id="uppercase"),
        pytest.param(_SHA_A + "a", id="41-chars"),
        pytest.param("g" * 40, id="non-hex"),
    ])
    def test_malformed_sha_markers_record_nothing(self, sha_text: str) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]", issue_mod.adopt_marker(sha_text)),
        ]
        assert issue_mod.adopted_shas(tracker) == ()

    def test_quoted_marker_in_a_bot_comment_is_not_an_adoption(self) -> None:
        body = (
            "Never paste adoption markers by hand. For reference, they "
            "look like:\n```\n" + issue_mod.adopt_marker(_SHA_A) + "\n```"
        )
        tracker = MagicMock()
        tracker.get_comments.return_value = [_comment("valkeyrie-ops[bot]", body)]
        assert issue_mod.adopted_shas(tracker) == ()

    def test_indented_marker_in_a_bot_comment_is_not_an_adoption(self) -> None:
        # The controller always posts markers at the start of their own
        # line; an indented quotation of one must never count.
        body = ("For reference, adoption markers look like:\n    "
                + issue_mod.adopt_marker(_SHA_A))
        tracker = MagicMock()
        tracker.get_comments.return_value = [_comment("valkeyrie-ops[bot]", body)]
        assert issue_mod.adopted_shas(tracker) == ()

    def test_comment_timestamps_are_not_consulted(self) -> None:
        # adopted_shas applies no created_at filter. Held acceptable:
        # comments are scoped to the tracker issue itself, so none can
        # predate the tracker the way a pre-existing PR can. If trackers are
        # ever reused across releases this needs a created_at binding like
        # the notes-PR search has.
        comment = _comment("valkeyrie-ops[bot]", issue_mod.adopt_marker(_SHA_A))
        comment.created_at = BEFORE_TRACKER
        tracker = MagicMock()
        tracker.get_comments.return_value = [comment]
        assert issue_mod.adopted_shas(tracker) == (_SHA_A,)


class TestBinding:
    """The identity-binding receipt: trusted parse, edit-in-place writes."""

    def _binding(self, **overrides: object) -> issue_mod.ReleaseBinding:
        values: "dict[str, object]" = {"version": "9.1.1", "stage": "rc1",
                                       "notes_pr_number": 42,
                                       "merge_sha": _SHA_A}
        values.update(overrides)
        return issue_mod.ReleaseBinding(**values)  # type: ignore[arg-type]

    def test_marker_roundtrips_every_field(self) -> None:
        binding = self._binding()
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]",
                     f"{issue_mod.binding_marker(binding)}\nreceipt"),
        ]
        assert issue_mod.read_binding(tracker) == binding

    def test_version_only_binding_roundtrips(self) -> None:
        binding = self._binding(notes_pr_number=0, merge_sha="")
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]",
                     f"{issue_mod.binding_marker(binding)}\nreceipt"),
        ]
        assert issue_mod.read_binding(tracker) == binding

    def test_untrusted_author_cannot_bind(self) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("drive-by", f"{issue_mod.binding_marker(self._binding())}\nx"),
        ]
        assert issue_mod.read_binding(tracker) is None

    def test_quoted_marker_in_a_fence_cannot_bind(self) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("valkeyrie-ops[bot]",
                     "```\n" + issue_mod.binding_marker(self._binding()) + "\n```"),
        ]
        assert issue_mod.read_binding(tracker) is None

    def test_write_updates_the_existing_receipt_in_place(self) -> None:
        original = self._binding(merge_sha="")
        existing = _comment("valkeyrie-ops[bot]",
                            issue_mod._binding_body(original))
        tracker = MagicMock()
        tracker.get_comments.return_value = [existing]

        issue_mod.write_binding(tracker, self._binding())

        existing.edit.assert_called_once()
        tracker.create_comment.assert_not_called()
        assert issue_mod.binding_marker(self._binding()) in (
            existing.edit.call_args.kwargs["body"]
        )

    def test_write_is_a_noop_when_the_receipt_already_matches(self) -> None:
        binding = self._binding()
        existing = _comment("valkeyrie-ops[bot]",
                            issue_mod._binding_body(binding))
        tracker = MagicMock()
        tracker.get_comments.return_value = [existing]

        issue_mod.write_binding(tracker, binding)

        existing.edit.assert_not_called()
        tracker.create_comment.assert_not_called()

    def test_write_creates_the_receipt_when_absent(self) -> None:
        tracker = MagicMock()
        tracker.get_comments.return_value = []

        issue_mod.write_binding(tracker, self._binding())

        tracker.create_comment.assert_called_once()
        body = tracker.create_comment.call_args.kwargs["body"]
        assert issue_mod.binding_marker(self._binding()) in body


class TestNormalizeBodyTimestamp:
    def test_updated_and_legacy_reconciled_footers_normalize_alike(self) -> None:
        updated = "state\n<sub>Updated 2026-08-10 17:30 UTC · x</sub>\n"
        legacy = "state\n<sub>Reconciled 2026-08-09 03:05 UTC · x</sub>\n"
        assert (issue_mod.normalize_body_timestamp(updated)
                == issue_mod.normalize_body_timestamp(legacy))

    def test_timestamp_only_difference_compares_equal(self) -> None:
        status = _status()
        early = issue_mod.render_body(status, _NOW)
        late = issue_mod.render_body(
            status, _NOW.replace(hour=23, minute=59))
        assert early != late
        assert (issue_mod.normalize_body_timestamp(early)
                == issue_mod.normalize_body_timestamp(late))

    def test_real_differences_survive_normalization(self) -> None:
        early = issue_mod.render_body(_status(), _NOW)
        changed = issue_mod.render_body(_status(ready=False), _NOW)
        assert (issue_mod.normalize_body_timestamp(early)
                != issue_mod.normalize_body_timestamp(changed))

    def test_none_body_normalizes_to_empty(self) -> None:
        assert issue_mod.normalize_body_timestamp(None) == ""  # type: ignore[arg-type]


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
        # The centered badge header is a deliberate product choice
        # (restored by owner request after the native-markdown redesign).
        body = _render(_status())
        assert '<div align="center">' in body
        assert "img.shields.io/badge/version-9.1.1-" in body
        # Stage badge renders uppercase, and the phase message is
        # percent-encoded or the badge 404s.
        assert "img.shields.io/badge/stage-GA-" in body
        assert "img.shields.io/badge/phase-4%2F6" in body
        assert "🟩🟩🟩🟦⬜⬜" in body  # READY: three done, current, two ahead

    def test_progress_bar_label_never_asserts_the_unfinished_outcome(self) -> None:
        # The bar describes the phase in flight; the checklist owns the
        # completed-form titles. A READY tracker must not read "Published".
        ready_body = _render(_status())
        assert "⬜ **Published (human-approved)**" not in ready_body
        assert "**Ready to Publish: Awaiting Approval**" in ready_body
        failing = _status(
            ready=False,
            alerts=("The release metadata is broken; fix it.",),
            blockers=("The release metadata is broken; fix it.",),
        )
        failing_body = _render(failing)
        assert "🟥" in failing_body
        assert "(Failures Need Attention)**" in failing_body

    def test_red_checks_alone_never_turn_the_bar_red(self) -> None:
        # Checks are informational: a red check renders in the table (the
        # human sees it) but is not a failure, so the bar stays blue/green
        # and the tracker never claims the release needs attention for it.
        red_checks = _status(
            checks=(RequiredCheck(name="test-ubuntu-latest",
                                  state=CheckState.FAILED),
                    RequiredCheck(name="build-macos-latest",
                                  state=CheckState.STALLED)),
        )
        assert not issue_mod.has_failures(red_checks)
        body = _render(red_checks)
        assert "🟥" not in body
        assert "(Failures Need Attention)" not in body
        assert "| `test-ubuntu-latest` | ❌ Failed |" in body

    def test_header_links_the_branch_and_the_tracker_search(self) -> None:
        body = _render(_status())
        assert "## Valkey 9.1.1" in body
        assert "[`9.1`](https://github.com/valkey-io/valkey/tree/9.1)" in body
        assert ("[All trackers](https://github.com/valkey-io/valkey/"
                "issues?q=is%3Aissue+is%3Aopen+label%3Arelease-tracker)") in body

    def test_stage_renders_uppercase(self) -> None:
        # The stage lives in the badge row and the Details table; the
        # compact subtitle carries identity links only.
        body = _render(_status())
        assert "img.shields.io/badge/stage-GA-" in body
        assert "| Stage | `GA` |" in body
        rc = _render(_status(version="9.1.0", stage="rc2"))
        assert "img.shields.io/badge/stage-RC2-" in rc
        assert "| Stage | `RC2` |" in rc

    def test_candidate_sha_links_to_the_commit(self) -> None:
        body = _render(_status())
        assert (f"[`{_SHA_A[:12]}`](https://github.com/valkey-io/valkey/"
                f"commit/{_SHA_A})") in body

    def test_qualification_link_carries_no_raw_run_id(self) -> None:
        from scripts.release.models import QualificationStatus
        body = _render(_status(qualification=QualificationStatus(
            run_id=900, url="https://x/qruns/900", passed=True)))
        assert "Passed ([qualification run](https://x/qruns/900))" in body
        assert "Run 900" not in body

    def test_footer_carries_the_freshness_stamp(self) -> None:
        body = _render(_status())
        assert "<sub>Updated 2026-08-10 17:30 UTC" in body
        assert "Updated " in body and "UTC" in body
        assert "manual edits are overwritten.</sub>" in body
        assert body.rstrip().endswith("manual edits are overwritten.</sub>")

    def test_status_vocabulary_is_the_capitalized_icon_set_only(self) -> None:
        stalled = _status(
            ready=False,
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.STALLED),
                    RequiredCheck(name="build-macos-latest", state=CheckState.MISSING)),
            blockers=("stalled",),
        )
        body = _render(stalled)
        assert "❌ Stalled" in body
        assert "⛔ Missing" in body
        assert "⚠️" not in body and "🛑" not in body

    def test_rendered_body_never_contains_an_em_dash(self) -> None:
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
            assert "\u2014" not in _render(status)


def _output(name: str, state: OutputState) -> DownstreamOutput:
    return DownstreamOutput(name=name, state=state, detail=f"{name} detail")


class TestTablesTriageAndCollapse:
    def _published(self, outputs: tuple[DownstreamOutput, ...]) -> ReleaseStatus:
        return _status(phase=ReleasePhase.PUBLISHED, published=True, ready=False,
                       release_url="https://x/releases/9.1.1", outputs=outputs)

    def test_outputs_sort_in_triage_order_stable_within_groups(self) -> None:
        body = _render(self._published((
            _output("v-one", OutputState.VERIFIED),
            _output("p-one", OutputState.PENDING),
            _output("f-one", OutputState.FAILED),
            _output("n-one", OutputState.SKIPPED),
            _output("b-one", OutputState.BLOCKED),
            _output("f-two", OutputState.FAILED),
        )))
        order = [body.index(f"| **{name}** |")
                 for name in ("f-one", "f-two", "b-one", "p-one", "n-one", "v-one")]
        assert order == sorted(order)

    def test_all_passed_checks_collapse_behind_a_summary(self) -> None:
        body = _render(_status(checks=(
            RequiredCheck(name="test-ubuntu-latest", state=CheckState.PASSED),
            RequiredCheck(name="build-macos-latest", state=CheckState.PASSED),
        )))
        assert ("<details><summary>Candidate CI: 2 of 2 checks green "
                "(informational; the release gates on Qualification)"
                "</summary>") in body
        assert "</details>" in body
        assert "| `test-ubuntu-latest` | ✅ Passed |" in body

    def test_red_check_keeps_the_table_open_with_the_informational_line(self) -> None:
        # Pin (c): a failed check still renders with the failure icon (a
        # human SEES red CI before approving) and the summary line says the
        # display is informational; the release itself can be READY.
        body = _render(_status(checks=(
            RequiredCheck(name="test-ubuntu-latest", state=CheckState.FAILED,
                          url="https://x/run/1"),
            RequiredCheck(name="build-macos-latest", state=CheckState.PASSED),
        )))
        assert "<details>" not in body
        assert "| `test-ubuntu-latest` | ❌ Failed ([run](https://x/run/1)) |" in body
        assert ("Candidate CI: 1 of 2 checks green (informational; the "
                "release gates on Qualification)") in body

    def test_any_unfinished_check_keeps_the_table_open(self) -> None:
        body = _render(_status(
            ready=False,
            checks=(RequiredCheck(name="test-ubuntu-latest", state=CheckState.PASSED),
                    RequiredCheck(name="build-macos-latest", state=CheckState.PENDING)),
            blockers=("pending",),
        ))
        assert "<details>" not in body
        assert "| `build-macos-latest` | ⏳ Pending |" in body

    def test_all_settled_outputs_collapse_behind_a_summary(self) -> None:
        body = _render(self._published((
            _output("tarballs", OutputState.VERIFIED),
            _output("packages", OutputState.SKIPPED),
        )))
        assert "<details><summary>All public outputs verified</summary>" in body
        assert "| **tarballs** | ✅ Verified |" in body

    def test_any_failed_output_keeps_the_table_open(self) -> None:
        body = _render(self._published((
            _output("tarballs", OutputState.VERIFIED),
            _output("helm", OutputState.FAILED),
        )))
        assert "All public outputs verified" not in body
        assert "| **helm** | ❌ Failed |" in body


class TestAuthenticatedIdentityTrust:
    def test_env_provided_app_login_becomes_trusted(
            self, monkeypatch) -> None:
        # A fork running its OWN GitHub App posts as "<forkslug>[bot]",
        # which is not in the static set, and App tokens have no user
        # context; the workflows pass the minted App's slug so the
        # controller can read back its own markers.
        tracker = MagicMock()
        tracker.get_comments.return_value = [
            _comment("myapp[bot]", issue_mod.adopt_marker(_SHA_A)),
        ]
        monkeypatch.setenv("RELEASE_BOT_LOGIN", "myapp[bot]")
        assert issue_mod.adopted_shas(tracker) == (_SHA_A,)
        # Unset, the same comment stays untrusted: nothing else changes.
        monkeypatch.delenv("RELEASE_BOT_LOGIN")
        issue_mod.invalidate_comment_memo(tracker)
        assert issue_mod.adopted_shas(tracker) == ()

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



class TestFindReleaseIssues:
    """The plural finder used by the "one active release per branch"
    invariant (multiple-open refusal, ambiguous-create readback). Follows
    the same identity model as :func:`find_release_issue`: label-pair
    query first, body-marker migration path as fallback, PRs dropped."""

    def _issue(self, number: int, branch: str = "9.1") -> MagicMock:
        issue = MagicMock(number=number)
        issue.body = issue_mod.identity_marker(branch)
        issue._rawData = {}
        return issue

    def test_returns_every_matching_open_tracker(self) -> None:
        first = self._issue(7)
        second = self._issue(8)
        repo = MagicMock()
        repo.get_issues.side_effect = lambda state, labels: (
            [first, second] if "release:9.1" in labels else []
        )

        matches = issue_mod.find_release_issues(repo, "9.1", label="release-tracker")

        assert [i.number for i in matches] == [7, 8]

    def test_marker_fallback_covers_pre_label_trackers(self) -> None:
        # A tracker created before the branch label existed still carries
        # the body marker; the finder unions both candidate sets.
        labelled = self._issue(9)
        marker_only = self._issue(10)
        repo = MagicMock()
        # The label-pair query returns only the labelled tracker; the
        # label-only fallback returns both (the pair-matched one is
        # deduped by number).
        repo.get_issues.side_effect = lambda state, labels: (
            [labelled] if "release:9.1" in labels else [labelled, marker_only]
        )

        matches = issue_mod.find_release_issues(repo, "9.1", label="release-tracker")

        assert sorted(i.number for i in matches) == [9, 10]

    def test_pull_requests_are_dropped(self) -> None:
        # The REST list endpoint returns PRs alongside issues; drop them.
        real = self._issue(11)
        pull = MagicMock(number=12)
        pull.body = issue_mod.identity_marker("9.1")
        pull._rawData = {"pull_request": {}}
        repo = MagicMock()
        repo.get_issues.return_value = [pull, real]

        matches = issue_mod.find_release_issues(repo, "9.1", label="release-tracker")

        assert [i.number for i in matches] == [11]

    def test_empty_set_when_no_tracker(self) -> None:
        repo = MagicMock()
        repo.get_issues.return_value = []
        assert issue_mod.find_release_issues(repo, "9.1", label="release-tracker") == []

    def test_find_release_issue_returns_first_match(self) -> None:
        # The singular finder is a thin wrapper: same identity model,
        # returns the first hit (matching the single-finder behavior).
        first = self._issue(13)
        second = self._issue(14)
        repo = MagicMock()
        repo.get_issues.side_effect = lambda state, labels: (
            [first, second] if "release:9.1" in labels else []
        )

        assert issue_mod.find_release_issue(repo, "9.1", label="release-tracker") is first


class TestHasCompletionMarker:
    """The refuse-start-on-abandoned-closed-tracker and heal-closed-tracker (heal
    controller-completed CLOSED tracker) guards depend on this: only a
    trusted-author completion marker counts."""

    def _tracker(self, comments: "list[MagicMock] | None" = None) -> MagicMock:
        issue = MagicMock(number=7)
        issue.get_comments.return_value = comments or []
        return issue

    def test_returns_true_when_trusted_marker_present(self) -> None:
        comment = _comment("valkeyrie-ops[bot]",
                           f"{issue_mod.complete_marker()}\nrelease complete")
        assert issue_mod.has_completion_marker(self._tracker([comment])) is True

    def test_returns_false_when_no_comments(self) -> None:
        assert issue_mod.has_completion_marker(self._tracker([])) is False

    def test_returns_false_for_untrusted_author(self) -> None:
        # A hand-pasted completion marker by an outsider cannot forge
        # controller-closed status; the same trust model as adoptions.
        comment = _comment("drive-by",
                           f"{issue_mod.complete_marker()}\nrelease complete")
        assert issue_mod.has_completion_marker(self._tracker([comment])) is False

    def test_returns_false_for_quoted_marker_inside_a_fence(self) -> None:
        # A trusted comment quoting the marker (e.g., an operator help
        # message) inside a code fence must not read as completion.
        body = f"For reference:\n```\n{issue_mod.complete_marker()}\n```"
        comment = _comment("valkeyrie-ops[bot]", body)
        assert issue_mod.has_completion_marker(self._tracker([comment])) is False


class TestTrustedBotAuthors:
    """The trust set the notes-PR author gate consults. Mirrors
    :func:`trusted_comments`' trust model but exposed as a set for
    caller-side authentication of *other* GitHub resources."""

    def test_covers_the_static_bot_identities(self) -> None:
        trusted = issue_mod.trusted_bot_authors()
        assert issue_mod.TRUSTED_MARKER_AUTHORS.issubset(trusted)

    def test_env_release_bot_login_joins_the_set(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RELEASE_BOT_LOGIN", "myapp[bot]")
        assert "myapp[bot]" in issue_mod.trusted_bot_authors()

    def test_blank_env_release_bot_login_adds_nothing(
            self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An unset secret (empty string) must not silently trust the
        # empty-login author.
        monkeypatch.setenv("RELEASE_BOT_LOGIN", "")
        assert "" not in issue_mod.trusted_bot_authors()
        monkeypatch.setenv("RELEASE_BOT_LOGIN", "   ")
        assert "   " not in issue_mod.trusted_bot_authors()

    def test_authenticated_user_identity_joins_the_set(self) -> None:
        gh = MagicMock()
        gh._release_controller_login = None
        gh.get_user.return_value.login = "sarthakaggarwal97"
        assert "sarthakaggarwal97" in issue_mod.trusted_bot_authors(gh)
