"""Tests for release-notes label disposition (pure).

The gate keys on two labels: ``no-release-notes`` hard-excludes (even over
``release-notes``), ``release-notes`` hard-includes, and everything else is a
CANDIDATE that AI triage judges.
"""

from __future__ import annotations

from scripts.release_notes.classify import classify, disposition_for
from scripts.release_notes.models import MergedPR, PRDisposition


def _pr(number: int, labels: tuple[str, ...]) -> MergedPR:
    return MergedPR(number=number, title="t", author="a", url="u", labels=labels)


class TestDispositionFor:
    def test_release_notes_only_includes(self) -> None:
        assert disposition_for(("release-notes",)) is PRDisposition.INCLUDE

    def test_arbitrary_labels_are_candidates(self) -> None:
        assert disposition_for(("bug", "area/cluster")) is PRDisposition.CANDIDATE

    def test_no_release_notes_hard_excludes(self) -> None:
        assert disposition_for(("no-release-notes",)) is PRDisposition.EXCLUDE

    def test_empty_is_a_candidate(self) -> None:
        assert disposition_for(()) is PRDisposition.CANDIDATE

    def test_release_notes_with_other_labels_includes(self) -> None:
        assert disposition_for(("release-notes", "bug")) is PRDisposition.INCLUDE

    def test_no_release_notes_wins_over_release_notes(self) -> None:
        # Contradictory labelling resolves to the explicit opt-out.
        assert disposition_for(("release-notes", "no-release-notes")) is PRDisposition.EXCLUDE


class TestClassify:
    def test_partitions_and_restamps(self) -> None:
        prs = [
            _pr(1, ("release-notes",)),
            _pr(2, ("bug",)),
            _pr(3, ()),
            _pr(4, ("release-notes", "enhancement")),
            _pr(5, ("no-release-notes",)),
            _pr(6, ("release-notes", "no-release-notes")),
        ]
        include, candidates, excluded = classify(prs)
        assert [p.number for p in include] == [1, 4]
        assert sorted(p.number for p in candidates) == [2, 3]
        assert sorted(p.number for p in excluded) == [5, 6]
        # Disposition is stamped onto the returned objects.
        assert all(p.disposition is PRDisposition.INCLUDE for p in include)
        assert all(p.disposition is PRDisposition.CANDIDATE for p in candidates)
        assert all(p.disposition is PRDisposition.EXCLUDE for p in excluded)

    def test_preserves_pr_fields(self) -> None:
        pr = MergedPR(number=9, title="Title", author="bob", url="https://x/9",
                      labels=("release-notes",), merge_commit_sha="abc")
        include, _, _ = classify([pr])
        out = include[0]
        assert (out.number, out.title, out.author, out.url, out.merge_commit_sha) == (
            9, "Title", "bob", "https://x/9", "abc",
        )

    def test_empty_input(self) -> None:
        assert classify([]) == ([], [], [])

    def test_backport_remapped_pr_classified_by_source_labels(self) -> None:
        # After discovery remaps a backport to its original PR, the MergedPR carries
        # the *source* PR's labels. A source labelled release-notes must INCLUDE,
        # even though the backport PR itself would have carried only "backport"
        # (which is a candidate). This pins that identity, not the on-line PR, gates.
        remapped = MergedPR(number=7, title="Fix a leak", author="alice",
                            url="https://x/7", labels=("release-notes",))
        include, candidates, excluded = classify([remapped])
        assert [p.number for p in include] == [7]
        assert candidates == []
        assert excluded == []

    def test_backport_remapped_pr_hard_excluded_by_source_no_release_notes(self) -> None:
        # A backport whose source PR opted out with no-release-notes is dropped.
        remapped = MergedPR(number=8, title="Internal refactor", author="carol",
                            url="https://x/8", labels=("no-release-notes",))
        include, candidates, excluded = classify([remapped])
        assert include == []
        assert candidates == []
        assert [p.number for p in excluded] == [8]
