"""Tiered diff-comment rendering and durable resolution evidence."""

from __future__ import annotations

from pathlib import Path

from scripts.backport.diff_comments import render_diff_comment
from scripts.backport.models import (
    BackportCandidate,
    CandidateResult,
    ConflictedFile,
    ResolutionResult,
)
from scripts.backport.resolution_evidence import (
    EVIDENCE_PATCH_NAME,
    write_resolution_evidence,
)


def _diff(path: str, hunks: list[str]) -> str:
    return "\n".join([f"--- a/{path}", f"+++ b/{path}", *hunks])


def _hunk(body_lines: list[str]) -> str:
    return "\n".join([f"@@ -1,{len(body_lines)} +1,{len(body_lines)} @@", *body_lines])


def _resolution(path: str, diff: str) -> ResolutionResult:
    return ResolutionResult(
        path=path,
        resolved_content="resolved\n",
        resolution_summary="resolved",
        reviewer_diff=diff,
    )


def test_small_diff_is_embedded_completely() -> None:
    diff = _diff("src/a.c", [_hunk(["-old", "+new"])])
    body = render_diff_comment(42, [_resolution("src/a.c", diff)])

    assert "<summary>Complete resolved diff</summary>" in body
    assert "-old" in body and "+new" in body
    assert "omitted" not in body
    assert "+1 / \u22121 across 1 hunk" in body


def test_medium_diff_shows_every_small_hunk_and_excerpts_the_huge_one() -> None:
    small_hunks = [_hunk([f"+small {i}"]) for i in range(3)]
    huge_hunk = _hunk([f"+line {i}" for i in range(2000)])
    diff = _diff("src/b.c", [*small_hunks, huge_hunk])
    body = render_diff_comment(42, [_resolution("src/b.c", diff)])

    assert "<summary>Representative conflict hunks</summary>" in body
    for i in range(3):
        assert f"+small {i}" in body
    assert "... omitted" in body  # the huge hunk is excerpted, never dropped silently
    assert "Showing 4 of 4 hunks" in body
    assert "1 excerpted" in body


def test_huge_resolution_stays_an_index_with_explicit_accounting() -> None:
    files = [
        _resolution(
            f"src/f{i}.c",
            _diff(f"src/f{i}.c", [_hunk([f"+x{j}" * 20 for j in range(3000)])]),
        )
        for i in range(30)
    ]
    body = render_diff_comment(
        42,
        files,
        resolved_commit_sha="a" * 40,
        repo_html_url="https://github.com/example/repo",
        evidence_patch_name=EVIDENCE_PATCH_NAME,
    )

    # Hard bound: whatever the input, the comment fits GitHub's limit.
    assert len(body) < 65_536
    # Every file keeps its index bullet and link even when nothing inlines.
    for i in range(30):
        assert f"src/f{i}.c" in body
    assert body.count("view diff") == 30
    assert f"workflow artifact `{EVIDENCE_PATCH_NAME}`" in body


def test_total_inline_budget_is_shared_across_files() -> None:
    # Each file's diff fits the per-file cap, but together they exceed the
    # total budget; later files must degrade to index-only, not overflow.
    per_file = _diff("src/x.c", [_hunk(["+y" * 40 for _ in range(140)])])
    files = [_resolution(f"src/x{i}.c", per_file) for i in range(10)]
    body = render_diff_comment(42, files)

    assert len(body) < 65_536
    assert body.count("<details>") < 10


def test_evidence_patch_records_hashes_and_both_diffs(tmp_path: Path) -> None:
    candidate = BackportCandidate(
        source_pr_number=99,
        source_pr_title="Fix",
        source_pr_url="https://github.com/example/repo/pull/99",
        target_branch="8.1",
    )
    result = CandidateResult(
        source_pr_number=99,
        source_pr_title="Fix",
        outcome="applied",
        resolutions=[
            ResolutionResult(
                path="src/a.c",
                resolved_content="resolved\n",
                resolution_summary="resolved",
                reviewer_diff="--- a/src/a.c\n+++ b/src/a.c\n@@ @@\n+resolved",
                resolution_diff="--- conflicted\n+++ resolved\n@@ @@\n-<<<<<<<\n+resolved",
                llm_summary="Adapted to the older allocator API.",
            )
        ],
        resolved_commit_sha="b" * 40,
        conflicting_files=[
            ConflictedFile(
                path="src/a.c",
                target_branch_content="target\n",
                source_branch_content="source\n",
            )
        ],
    )

    path = write_resolution_evidence(candidate, result, evidence_dir=str(tmp_path))

    assert path == tmp_path / EVIDENCE_PATCH_NAME
    text = path.read_text(encoding="utf-8")
    assert "Source-PR: #99" in text
    assert "Target-Branch: 8.1" in text
    assert f"Resolution-Commit: {'b' * 40}" in text
    assert "Target-Content-SHA256: " in text and "(unavailable)" not in text
    assert "reviewer_diff (complete landing delta)" in text
    assert "resolution_diff (literal resolver edit trace)" in text
    assert "Adapted to the older allocator API." in text

    # Appends across candidates instead of overwriting.
    write_resolution_evidence(candidate, result, evidence_dir=str(tmp_path))
    assert path.read_text(encoding="utf-8").count("Source-PR: #99") == 2


def test_evidence_is_skipped_outside_ci(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CI_AGENT_EVIDENCE_DIR", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    candidate = BackportCandidate(
        source_pr_number=1,
        source_pr_title="t",
        source_pr_url="u",
        target_branch="8.1",
    )
    result = CandidateResult(1, "t", "applied")

    assert write_resolution_evidence(candidate, result) is None
