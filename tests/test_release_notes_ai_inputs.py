"""Tests for trusted PR payload and diff preparation shared by AI stages."""

from __future__ import annotations

import json

from scripts.release_notes import ai_inputs
from scripts.release_notes.generate import generate
from scripts.release_notes.models import MergedPR
from scripts.release_notes.triage import triage


def _pr(number: int, sha: str) -> MergedPR:
    return MergedPR(
        number=number,
        title=f"PR {number}",
        author="alice",
        url=f"https://example.test/{number}",
        merge_commit_sha=sha,
    )


def _stream(obj: dict) -> str:
    return json.dumps({"type": "result", "result": json.dumps(obj)})


def test_diff_collector_reads_each_unique_sha_once(monkeypatch) -> None:
    calls = []

    def fake_git_output(repo_dir, *args):
        calls.append((repo_dir, args))
        return f"diff for {args[-1]}"

    monkeypatch.setattr(ai_inputs, "git_output", fake_git_output)
    prs = [_pr(1, "one"), _pr(2, "two")]
    collector = ai_inputs.PRDiffCollector(
        "/repo",
        prs,
    )
    diffs = collector.collect(prs)
    assert collector.collect(prs) == diffs

    assert diffs == {
        1: "diff for one",
        2: "diff for two",
    }
    assert [args[-1] for _repo, args in calls] == ["one", "two"]


def test_diff_collector_omits_combined_sweep_diff(monkeypatch) -> None:
    calls = []

    def fake_git_output(repo_dir, *args):
        calls.append(args[-1])
        return "combined patch"

    monkeypatch.setattr(ai_inputs, "git_output", fake_git_output)
    prs = [_pr(1, "shared"), _pr(2, "shared")]
    collector = ai_inputs.PRDiffCollector("/repo", prs)

    assert collector.collect(prs) == {}
    assert calls == []


def test_triage_and_generation_share_diff_cache(monkeypatch) -> None:
    calls = []

    def fake_git_output(repo_dir, *args):
        calls.append(args[-1])
        return "shared patch"

    monkeypatch.setattr(ai_inputs, "git_output", fake_git_output)
    prs = [_pr(1, "one"), _pr(2, "two")]
    collector = ai_inputs.PRDiffCollector("/repo", prs)

    def triage_run(prompt, **kwargs):
        return (
            _stream(
                {
                    "verdicts": [
                        {"pr": 1, "include": True, "reason": "user-facing"},
                        {"pr": 2, "include": False, "reason": "internal"},
                    ],
                }
            ),
            "",
            0,
        )

    def generate_run(prompt, **kwargs):
        return (
            _stream(
                {
                    "bullets": [{"pr": 1, "category": "Bug Fixes", "text": "Fix it"}],
                    "skipped": [],
                }
            ),
            "",
            0,
        )

    result = triage(
        prs,
        repo_dir="/repo",
        run_fn=triage_run,
        diff_collector=collector,
    )
    generate(
        [prs[0]],
        repo_dir="/repo",
        categories=["Bug Fixes", "Other Changes"],
        run_fn=generate_run,
        diff_collector=collector,
    )

    assert [decision.pr_number for decision in result.included] == [1]
    assert calls == ["one", "two"]


def test_exact_pr_number_rejects_coercible_values() -> None:
    assert ai_inputs.exact_pr_number(40) == 40
    for value in (True, 40.0, "40", None):
        assert ai_inputs.exact_pr_number(value) is None


class TestFailedDiffReads:
    """A failed diff read marks the PR for low-confidence review; empty is not failed."""

    def test_failed_read_recorded(self, monkeypatch) -> None:
        def _fake(repo_dir, sha):
            return "diff --git a b" if sha == "sha_ok" else None

        monkeypatch.setattr(ai_inputs, "_collect_commit_diff", _fake)
        prs = [
            MergedPR(number=1, title="a", author="x", url="u", merge_commit_sha="sha_ok"),
            MergedPR(number=2, title="b", author="x", url="u", merge_commit_sha="sha_fail"),
            MergedPR(number=3, title="c", author="x", url="u"),  # no sha
        ]
        collector = ai_inputs.PRDiffCollector("/tmp", prs)
        diffs = collector.collect(prs)
        assert diffs == {1: "diff --git a b"}
        assert collector.failed_reads == {2}

    def test_empty_diff_is_not_a_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(ai_inputs, "_collect_commit_diff", lambda repo_dir, sha: "")
        prs = [MergedPR(number=1, title="a", author="x", url="u", merge_commit_sha="sha_empty")]
        collector = ai_inputs.PRDiffCollector("/tmp", prs)
        assert collector.collect(prs) == {}
        assert collector.failed_reads == set()
