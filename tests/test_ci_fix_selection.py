from __future__ import annotations

from scripts.ci_fix.port_discovery import PortCandidate
from scripts.ci_fix.selection import canonical_candidate_sha, match_failed_job


def test_match_failed_job_accepts_exact_and_single_matrix_base() -> None:
    assert match_failed_job("build", ("build", "lint")) == "build"
    assert match_failed_job("test", ("test (clang)",)) == "test (clang)"


def test_match_failed_job_refuses_ambiguous_or_missing_hint() -> None:
    assert match_failed_job("test", ("test (a)", "test (b)")) is None
    assert match_failed_job("other", ("build",)) is None


def test_candidate_sha_requires_one_discovered_match() -> None:
    first = PortCandidate("a" * 40, "first")
    second = PortCandidate("ab" + "b" * 38, "second")
    assert canonical_candidate_sha("a" * 12, (first,)) == first.sha
    assert canonical_candidate_sha("a", (first, second)) is None
    assert canonical_candidate_sha("c" * 12, (first, second)) is None
