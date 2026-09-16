"""Tests for the AI triage of label-less PRs, with a faked run_fn."""

from __future__ import annotations

import json
from functools import partial

from scripts.release_notes.models import MergedPR
from scripts.release_notes.projects import VALKEY_PROFILE
from scripts.release_notes.triage import build_prompt as _build_prompt
from scripts.release_notes.triage import triage as _triage

build_prompt = partial(
    _build_prompt,
    project_description=VALKEY_PROFILE.triage_prompt_project,
)
triage = partial(
    _triage,
    project_description=VALKEY_PROFILE.triage_prompt_project,
)


def _pr(
    number: int,
    author: str = "alice",
    body: str = "",
    sha: str = "",
    title: str | None = None,
) -> MergedPR:
    return MergedPR(number=number, title=title or f"PR {number}", author=author, url=f"https://x/{number}",
                    body=body, labels=(), merge_commit_sha=sha)


def _stream(obj: dict) -> str:
    """Wrap a JSON object in a stream-json 'result' event, like the claude CLI."""
    return json.dumps({"type": "result", "result": json.dumps(obj)})


def _fake_run(obj, *, exit_code: int = 0):
    """Build a run_fn that returns the given object as stream-json output."""
    def _run(prompt, **kwargs):
        return _stream(obj), "", exit_code
    return _run


def _fake_run_raw(text: str, *, exit_code: int = 0):
    """Build a run_fn that returns arbitrary (possibly unparseable) text."""
    def _run(prompt, **kwargs):
        return text, "", exit_code
    return _run


class TestBuildPrompt:
    def test_default_core_project_wording_is_preserved(self) -> None:
        prompt = build_prompt([], base_ref="9.0.0")
        assert prompt.startswith(
            "You are triaging pull requests for the release notes of Valkey, a "
            "production key-value datastore. These PRs\nmerged after 9.0.0"
        )

    def test_lists_pr_numbers_and_asks_for_verdicts(self) -> None:
        prompt = build_prompt([_pr(40), _pr(41)])
        assert "40" in prompt and "41" in prompt
        assert '"verdicts"' in prompt
        assert '"include"' in prompt

    def test_inlines_diff_when_supplied(self) -> None:
        prompt = build_prompt([_pr(40)], diffs={40: "DIFFSTAT+PATCH"})
        assert '"diff": "DIFFSTAT+PATCH"' in prompt

    def test_omits_diff_field_when_absent_or_empty(self) -> None:
        prompt = build_prompt([_pr(40), _pr(41)], diffs={41: ""})
        payload = prompt.split("## Pull requests", 1)[1]
        assert '"diff"' not in payload

    def test_prompt_treats_pr_text_as_untrusted(self) -> None:
        prompt = build_prompt([_pr(1)])
        assert "untrusted" in prompt.lower()

    def test_prompt_defaults_uncertain_safety_fixes_to_include(self) -> None:
        prompt = build_prompt([_pr(1)])
        assert "omission is more costly" in prompt
        assert 'INCLUDE with `"uncertain": true`' in prompt
        assert "lacks a published advisory" in prompt
        assert "crafted RESTORE payload" in prompt


class TestTriage:
    def test_empty_input_returns_empty_result(self) -> None:
        result = triage([], repo_dir="/tmp", run_fn=_fake_run({"verdicts": []}))
        assert result.included == () and result.excluded == () and result.undecided == ()

    def test_partitions_include_and_exclude(self) -> None:
        prs = [_pr(1), _pr(2)]
        run = _fake_run({"verdicts": [
            {"pr": 1, "include": True, "reason": "adds a command"},
            {"pr": 2, "include": False, "reason": "test-only"},
        ]})
        result = triage(prs, repo_dir="/tmp", run_fn=run)
        assert [(d.pr_number, d.reason) for d in result.included] == [(1, "adds a command")]
        assert [(d.pr_number, d.reason) for d in result.excluded] == [(2, "test-only")]
        assert result.undecided == ()

    def test_uncertain_flag_carried_through(self) -> None:
        run = _fake_run({"verdicts": [{"pr": 1, "include": True, "uncertain": True}]})
        result = triage([_pr(1)], repo_dir="/tmp", run_fn=run)
        assert result.included[0].uncertain is True

    def test_unknown_pr_number_dropped(self) -> None:
        # A verdict for a PR not in the batch is dropped; that PR ends up undecided.
        run = _fake_run({"verdicts": [{"pr": 999, "include": True}]})
        result = triage([_pr(1)], repo_dir="/tmp", run_fn=run)
        assert result.included == () and result.excluded == ()
        assert result.undecided == (1,)

    def test_missing_verdict_leaves_pr_undecided(self) -> None:
        # The model returns a verdict for one PR but not the other; the omitted one
        # is undecided, never silently included or dropped.
        run = _fake_run({"verdicts": [{"pr": 1, "include": True}]})
        result = triage([_pr(1), _pr(2)], repo_dir="/tmp", run_fn=run)
        assert [d.pr_number for d in result.included] == [1]
        assert result.undecided == (2,)

    def test_non_boolean_include_is_undecided(self) -> None:
        # A verdict whose "include" is not a real bool is not guessed either way.
        run = _fake_run({"verdicts": [{"pr": 1, "include": "yes"}]})
        result = triage([_pr(1)], repo_dir="/tmp", run_fn=run)
        assert result.included == () and result.excluded == ()
        assert result.undecided == (1,)

    def test_duplicate_verdict_keeps_first(self) -> None:
        run = _fake_run({"verdicts": [
            {"pr": 1, "include": True, "reason": "first"},
            {"pr": 1, "include": False, "reason": "second"},
        ]})
        result = triage([_pr(1)], repo_dir="/tmp", run_fn=run)
        assert [(d.pr_number, d.reason) for d in result.included] == [(1, "first")]
        assert result.excluded == ()

    def test_unparseable_batch_leaves_all_undecided(self) -> None:
        run = _fake_run_raw("not json at all", exit_code=1)
        result = triage([_pr(1), _pr(2)], repo_dir="/tmp", run_fn=run)
        assert result.included == () and result.excluded == ()
        assert result.undecided == (1, 2)

    def test_safety_signal_overrides_ai_exclusion(self) -> None:
        pr = _pr(
            3921,
            title="Reject NAN scores in sorted sets on RDB load",
            body="A crafted RESTORE payload can remotely crash the server.",
        )
        run = _fake_run({"verdicts": [
            {"pr": 3921, "include": False, "reason": "crafted input is rare"},
        ]})
        result = triage([pr], repo_dir="/tmp", run_fn=run)
        assert result.excluded == ()
        assert result.undecided == ()
        assert result.included[0].guardrail is True
        assert result.included[0].uncertain is True
        assert "server crash" in result.included[0].reason

    def test_safety_signal_overrides_missing_or_unparseable_verdict(self) -> None:
        risky = _pr(
            3848,
            title="Fix cluster AUX-field control-character injection",
        )
        ordinary = _pr(2, title="Refactor an internal helper")
        result = triage(
            [risky, ordinary],
            repo_dir="/tmp",
            run_fn=_fake_run_raw("not json", exit_code=1),
        )
        assert [decision.pr_number for decision in result.included] == [3848]
        assert result.included[0].guardrail is True
        assert result.undecided == (2,)

    def test_test_only_assertion_remains_excluded(self) -> None:
        pr = _pr(
            4115,
            title="Fix racy remaining_repl_size assertion in slot migration test",
            body="The test is flaky; production code is unchanged.",
        )
        run = _fake_run({"verdicts": [
            {"pr": 4115, "include": False, "reason": "test-only"},
        ]})
        result = triage([pr], repo_dir="/tmp", run_fn=run)
        assert result.included == ()
        assert [decision.pr_number for decision in result.excluded] == [4115]

    def test_test_word_does_not_suppress_a_production_crash_signal(self) -> None:
        pr = _pr(
            12,
            title="Fix RESTORE crash and add regression tests",
            body="A crafted RESTORE payload can remotely crash the server.",
        )
        run = _fake_run({"verdicts": [
            {"pr": 12, "include": False, "reason": "title mentions tests"},
        ]})

        result = triage([pr], repo_dir="/tmp", run_fn=run)

        assert [decision.pr_number for decision in result.included] == [12]
        assert result.included[0].guardrail is True

    def test_9_1_1_regression_set_cannot_all_be_excluded(self) -> None:
        prs = [
            _pr(3743, title="Fix buffered_reply assert in HFE commands"),
            _pr(3847, title="Harden SENTINEL config against control-character injection"),
            _pr(3848, title="Fix nodes.conf delimiter injection"),
            _pr(3920, title="Reject integer overflow in RESTORE validation"),
            _pr(3921, title="Reject crafted RESTORE payload that can crash the server"),
            _pr(3939, title="Fix RESP3 protocol type violation"),
            _pr(3941, title="Avoid modulo by zero undefined behaviour"),
            _pr(3964, title="Restore ACL downgrade compatibility"),
            _pr(4073, title="Reject corrupt RDB that causes use-after-free"),
            _pr(3811, title="Fix off_t to int truncation in replication reporting"),
        ]
        run = _fake_run({"verdicts": [
            {"pr": pr.number, "include": False, "reason": "edge case"}
            for pr in prs
        ]})
        result = triage(prs, repo_dir="/tmp", run_fn=run)
        assert {decision.pr_number for decision in result.included} == {
            3743, 3847, 3848, 3920, 3921, 3939, 3941, 3964, 4073, 3811,
        }
        assert all(decision.guardrail for decision in result.included)
        assert result.excluded == ()


class TestNamedCve:
    """named_cve extracts a factual CVE reference from PR text."""

    def test_cve_in_title(self) -> None:
        from scripts.release_notes.triage import named_cve
        pr = _pr(3619, title="Fix invalid memory access in RESTORE (CVE-2026-25243)")
        assert named_cve(pr) == "CVE-2026-25243"

    def test_cve_in_body_and_case_normalized(self) -> None:
        from scripts.release_notes.triage import named_cve
        pr = _pr(1, body="tracked as cve-2025-1234 by the security team")
        assert named_cve(pr) == "CVE-2025-1234"

    def test_no_cve(self) -> None:
        from scripts.release_notes.triage import named_cve
        pr = _pr(2, title="Fix a crash in RESTORE", body="memory-safety issue")
        assert named_cve(pr) == ""

    def test_short_id_not_matched(self) -> None:
        # A CVE id has a 4+ digit sequence number; "CVE-2026-1" is not one.
        from scripts.release_notes.triage import named_cve
        assert named_cve(_pr(3, body="see CVE-2026-1")) == ""


class TestTestOnlyGuardrailExemption:
    """PRs whose changed files are all tests/CI never trip the impact guardrail."""

    def test_tests_only_pr_exempt(self) -> None:
        from scripts.release_notes.triage import release_impact_reason
        # The #3764/#4115 shape: an "assertion" deflake touching only tests.
        pr = MergedPR(
            number=4115, title="Fix racy remaining_repl_size assertion in slot migration test",
            author="a", url="u",
            changed_files=("tests/unit/cluster/slot-migration.tcl",),
        )
        assert release_impact_reason(pr) is None

    def test_ci_only_pr_exempt(self) -> None:
        from scripts.release_notes.triage import release_impact_reason
        pr = MergedPR(
            number=1, title="Fix crash reporting workflow", author="a", url="u",
            changed_files=(".github/workflows/daily.yml",),
        )
        assert release_impact_reason(pr) is None

    def test_mixed_files_not_exempt(self) -> None:
        from scripts.release_notes.triage import release_impact_reason
        pr = MergedPR(
            number=2, title="Fix crash in expire", author="a", url="u",
            changed_files=("src/expire.c", "tests/unit/expire.tcl"),
        )
        assert release_impact_reason(pr) is not None

    def test_unknown_files_not_exempt(self) -> None:
        # A failed files lookup (empty tuple) must not exempt: unknown is unsafe.
        from scripts.release_notes.triage import release_impact_reason
        pr = MergedPR(number=3, title="Fix crash in expire", author="a", url="u")
        assert release_impact_reason(pr) is not None


class TestUnreleasedCodeExclusion:
    """A fix for code introduced in this same release is not a user-facing fix.

    The check precedes the release-safety guardrail on purpose: the guardrail
    exists to stop under-reporting fixes to code users are running, and code
    that has never shipped is by definition not that.
    """

    @staticmethod
    def _oracle(monkeypatch, verdicts: dict[str, bool | None]):
        import scripts.release_notes.triage as triage_mod

        class FakeOracle:
            usable = True

            def __init__(self, *_args) -> None:
                pass

            def modifies_only_unreleased_code(self, sha):
                return verdicts.get(sha)

        monkeypatch.setattr(triage_mod, "ReleasedCodeOracle", FakeOracle)

    def test_included_fix_to_unreleased_code_is_excluded(self, monkeypatch) -> None:
        self._oracle(monkeypatch, {"a" * 40: True})
        run = _fake_run({"verdicts": [
            {"pr": 7, "include": True, "reason": "fixes a crash", "uncertain": False},
        ]})
        result = triage([_pr(7, sha="a" * 40)], repo_dir="/tmp", base_ref="9.1.2", run_fn=run)
        assert [d.pr_number for d in result.included] == []
        assert [d.pr_number for d in result.excluded] == [7]
        assert "never shipped broken" in result.excluded[0].reason

    def test_fix_to_released_code_is_kept(self, monkeypatch) -> None:
        self._oracle(monkeypatch, {"b" * 40: False})
        run = _fake_run({"verdicts": [
            {"pr": 8, "include": True, "reason": "fixes a crash", "uncertain": False},
        ]})
        result = triage([_pr(8, sha="b" * 40)], repo_dir="/tmp", base_ref="9.1.2", run_fn=run)
        assert [d.pr_number for d in result.included] == [8]

    def test_unknown_code_age_keeps_the_note(self, monkeypatch) -> None:
        # A pure addition or an unreadable commit must not silently drop a fix.
        self._oracle(monkeypatch, {"c" * 40: None})
        run = _fake_run({"verdicts": [
            {"pr": 9, "include": True, "reason": "fixes a crash", "uncertain": False},
        ]})
        result = triage([_pr(9, sha="c" * 40)], repo_dir="/tmp", base_ref="9.1.2", run_fn=run)
        assert [d.pr_number for d in result.included] == [9]

    def test_guardrail_does_not_resurrect_an_unreleased_fix(self, monkeypatch) -> None:
        # The PR body carries a memory-safety signal that would normally force
        # inclusion; it must not override "this code never shipped".
        self._oracle(monkeypatch, {"d" * 40: True})
        run = _fake_run({"verdicts": [
            {"pr": 10, "include": True, "reason": "fixes a crash", "uncertain": False},
        ]})
        pr = _pr(10, sha="d" * 40, body="Fixes a use-after-free crash on shutdown")
        result = triage([pr], repo_dir="/tmp", base_ref="9.1.2", run_fn=run)
        assert [d.pr_number for d in result.included] == []
        assert [d.pr_number for d in result.excluded] == [10]
        assert not result.excluded[0].guardrail

    def test_missing_verdict_consults_the_oracle_before_the_guardrail(self, monkeypatch) -> None:
        # A parse failure leaves no verdict; the guardrail would force-include
        # on the crash signal, but the oracle knows the code never shipped.
        self._oracle(monkeypatch, {"e" * 40: True})
        run = _fake_run({"verdicts": []})  # nothing parseable for this PR
        pr = _pr(11, sha="e" * 40, body="Fixes a use-after-free crash on shutdown")
        result = triage([pr], repo_dir="/tmp", base_ref="9.1.2", run_fn=run)
        assert [d.pr_number for d in result.included] == []
        assert [d.pr_number for d in result.excluded] == [11]
        assert result.excluded[0].unreleased_code

    def test_fix_referencing_an_in_range_introducer_is_excluded(self, monkeypatch) -> None:
        # The blame oracle keeps this one (it touches released files), but the
        # PR's own body names an unreleased introducer.
        self._oracle(monkeypatch, {"f" * 40: False})
        run = _fake_run({"verdicts": [
            {"pr": 12, "include": True, "reason": "fixes a bug", "uncertain": False},
        ]})
        pr = _pr(12, sha="f" * 40, body="Fixes a regression from #4460 during iteration")
        result = triage([pr], repo_dir="/tmp", base_ref="9.1.2", run_fn=run,
                        range_pr_numbers=frozenset({12, 4460}))
        assert [d.pr_number for d in result.excluded] == [12]
        assert "#4460" in result.excluded[0].reason
        assert result.excluded[0].unreleased_code

    def test_self_reference_does_not_self_exclude(self, monkeypatch) -> None:
        self._oracle(monkeypatch, {"a1" * 20: False})
        run = _fake_run({"verdicts": [
            {"pr": 13, "include": True, "reason": "fixes a bug", "uncertain": False},
        ]})
        pr = _pr(13, sha="a1" * 20, body="regression from #13 rework")
        result = triage([pr], repo_dir="/tmp", base_ref="9.1.2", run_fn=run,
                        range_pr_numbers=frozenset({13}))
        assert [d.pr_number for d in result.included] == [13]
