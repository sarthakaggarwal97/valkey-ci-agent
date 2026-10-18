"""Backtest scoring for backport eligibility."""

from __future__ import annotations

from datetime import datetime, timezone

from scripts.backport.eligibility import MergedSourcePR
from scripts.backport.eligibility_backtest import PrOutcome, report, score, triaged_at_run
from scripts.release_notes.models import MergedPR


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


def test_triaged_at_run_sees_only_backports_made_by_the_daily_run():
    source = MergedSourcePR(
        pr=MergedPR(number=4880, title="t", author="a", url="u", merge_commit_sha="x"),
        merged_at="2026-10-10T00:00:00Z",
    )
    landings = {
        "9.1": {4880: datetime(2026, 10, 11, tzinfo=timezone.utc)},  # swept the next day
        "8.0": {4880: datetime(2026, 11, 30, tzinfo=timezone.utc)},  # much later
    }
    assert triaged_at_run(source, ["9.1", "8.0", "7.2"], landings, 3) == ["9.1"]
    assert triaged_at_run(source, ["8.0", "7.2"], landings, 3) == []
