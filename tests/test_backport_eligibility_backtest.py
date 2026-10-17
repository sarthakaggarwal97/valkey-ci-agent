"""Backtest scoring for backport eligibility."""

from __future__ import annotations

from scripts.backport.eligibility_backtest import PrOutcome, report, score


def _outcomes() -> list[PrOutcome]:
    return [
        # Right on 9.1, wrong on 9.0.
        PrOutcome(1, "fix a", {"9.1"}, ["9.1", "9.0"], "would-comment",
                  predicted={"9.1", "9.0"}, offered={"9.1", "9.0"}, why={"9.0": "has f"}),
        # A real backport the model left out.
        PrOutcome(2, "fix b", {"9.0"}, ["9.1", "9.0"], "not-eligible", offered={"9.0"}),
        # Correctly left alone.
        PrOutcome(3, "perf", set(), ["9.1", "9.0"], "not-eligible", offered={"9.1"}),
    ]


def test_score_counts_per_branch_and_overall():
    scores = score(_outcomes())
    assert (scores["9.1"].tp, scores["9.1"].fp, scores["9.1"].fn) == (1, 0, 0)
    assert (scores["9.0"].tp, scores["9.0"].fp, scores["9.0"].fn) == (0, 1, 1)
    assert scores["all"].precision == 0.5 and scores["all"].recall == 0.5


def test_score_of_the_filter_alone():
    scores = score(_outcomes(), use="offered")
    assert (scores["all"].tp, scores["all"].fp, scores["all"].fn) == (2, 2, 0)


def test_report_lists_each_false_positive_with_its_reason():
    text = report(_outcomes(), since="2026-06-01", until="2026-08-31", with_projects=False)
    assert "- #1 `9.0` (fix a): has f" in text
    assert "- #2 9.0 (fix b): not-eligible" in text
    assert "lacks read:project" in text
