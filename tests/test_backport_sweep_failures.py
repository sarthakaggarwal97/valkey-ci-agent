from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest
from github.GithubException import GithubException

from scripts.backport.models import BackportCandidate, CandidateResult
from scripts.backport.sweep_failures import (
    campaign_made_no_progress,
    clear_failure_markers,
    failure_marker_exists,
    failure_marker_ref,
    record_failure_marker,
)


def _candidate(number: int, *, sha: str | None = None) -> BackportCandidate:
    return BackportCandidate(
        source_pr_number=number,
        source_pr_title=f"PR {number}",
        source_pr_url=f"https://example/pull/{number}",
        target_branch="release/1.0",
        merge_commit_sha=sha or f"merge-{number}",
        commit_shas=[f"commit-{number}"],
    )


def test_failure_marker_is_stable_and_state_sensitive() -> None:
    candidates = [_candidate(1), _candidate(2)]
    marker = failure_marker_ref("release/1.0", "a" * 40, candidates)

    assert marker == failure_marker_ref(
        "release/1.0",
        "a" * 40,
        candidates,
    )
    assert marker.startswith(
        "heads/agent/backport/failed-campaign/release-1.0-"
    )
    assert marker != failure_marker_ref(
        "release/1.0",
        "b" * 40,
        candidates,
    )
    assert marker != failure_marker_ref(
        "release/1.0",
        "a" * 40,
        list(reversed(candidates)),
    )
    assert marker != failure_marker_ref(
        "release/1.0",
        "a" * 40,
        [_candidate(1, sha="different"), _candidate(2)],
    )


@pytest.mark.parametrize(
    "target_branch",
    [
        "1.0",
        "release/1.0",
        "release-candidate_1",
        "maintenance/v1.2.x",
    ],
)
def test_failure_marker_is_a_valid_git_ref(target_branch: str) -> None:
    marker = failure_marker_ref(
        target_branch,
        "a" * 40,
        [_candidate(1)],
    )

    subprocess.run(
        ["git", "check-ref-format", f"refs/{marker}"],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    [
        (["skipped-conflict"], True),
        (["skipped-validation-failed"], True),
        (["skipped-existing", "skipped-conflict"], True),
        (["skipped-existing"], False),
        (["error"], False),
        (["applied"], False),
    ],
)
def test_campaign_made_no_progress_classifies_terminal_results(
    outcomes: list[str],
    expected: bool,
) -> None:
    candidates = [_candidate(index + 1) for index in range(len(outcomes))]
    results = [
        CandidateResult(
            source_pr_number=candidate.source_pr_number,
            source_pr_title=candidate.source_pr_title,
            outcome=outcome,  # type: ignore[arg-type]
        )
        for candidate, outcome in zip(candidates, outcomes)
    ]

    assert campaign_made_no_progress(candidates, results) is expected


def test_campaign_with_error_pr_or_partial_results_is_not_suppressed() -> None:
    candidates = [_candidate(1), _candidate(2)]
    result = CandidateResult(1, "PR 1", "skipped-conflict")

    assert not campaign_made_no_progress(candidates, [result])
    assert not campaign_made_no_progress(
        candidates[:1],
        [result],
        error="infrastructure failed",
    )
    assert not campaign_made_no_progress(
        candidates[:1],
        [result],
        pr_url="https://example/pull/9",
    )


class _FakeRef:
    def __init__(self, ref: str, sha: str) -> None:
        self.ref = f"refs/{ref}"
        self.object = SimpleNamespace(sha=sha)
        self.deleted = False

    def delete(self) -> None:
        self.deleted = True


class _FakeRepo:
    def __init__(self) -> None:
        self.refs: dict[str, _FakeRef] = {}
        self.created: list[tuple[str, str]] = []

    def get_git_ref(self, ref: str) -> _FakeRef:
        try:
            return self.refs[ref]
        except KeyError as exc:
            raise GithubException(404, "not found") from exc

    def create_git_ref(self, *, ref: str, sha: str) -> _FakeRef:
        short_ref = ref.removeprefix("refs/")
        created = _FakeRef(short_ref, sha)
        self.refs[short_ref] = created
        self.created.append((ref, sha))
        return created

    def get_git_matching_refs(self, prefix: str) -> list[_FakeRef]:
        return [
            ref
            for short_ref, ref in self.refs.items()
            if short_ref.startswith(f"{prefix}/") and not ref.deleted
        ]


def _fake_github(repo: _FakeRepo):
    return SimpleNamespace(get_repo=lambda _name: repo)


def test_record_failure_marker_creates_current_and_prunes_obsolete() -> None:
    repo = _FakeRepo()
    candidates = [_candidate(1)]
    old_marker = failure_marker_ref("release/1.0", "a" * 40, [_candidate(2)])
    current = failure_marker_ref("release/1.0", "a" * 40, candidates)
    other_branch = failure_marker_ref("release/2.0", "a" * 40, candidates)
    repo.refs[old_marker] = _FakeRef(old_marker, "a" * 40)
    repo.refs[other_branch] = _FakeRef(other_branch, "a" * 40)

    record_failure_marker(
        _fake_github(repo),
        "org/repo",
        current,
        target_branch="release/1.0",
        target_sha="a" * 40,
    )

    assert repo.created == [(f"refs/{current}", "a" * 40)]
    assert repo.refs[old_marker].deleted
    assert not repo.refs[current].deleted
    assert not repo.refs[other_branch].deleted


def test_failure_marker_exists_rejects_corrupt_target() -> None:
    repo = _FakeRepo()
    marker = failure_marker_ref("1.0", "a" * 40, [_candidate(1)])
    repo.refs[marker] = _FakeRef(marker, "b" * 40)

    with pytest.raises(RuntimeError, match="points to"):
        failure_marker_exists(
            _fake_github(repo),
            "org/repo",
            marker,
            target_sha="a" * 40,
        )


def test_clear_failure_markers_is_scoped_to_target_branch() -> None:
    repo = _FakeRepo()
    one = failure_marker_ref("release/1.0", "a" * 40, [_candidate(1)])
    two = failure_marker_ref("release/2.0", "a" * 40, [_candidate(1)])
    repo.refs[one] = _FakeRef(one, "a" * 40)
    repo.refs[two] = _FakeRef(two, "a" * 40)

    clear_failure_markers(
        _fake_github(repo),
        "org/repo",
        "release/1.0",
    )

    assert repo.refs[one].deleted
    assert not repo.refs[two].deleted
