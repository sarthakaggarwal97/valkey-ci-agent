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


def test_total_body_is_bounded_for_hundreds_of_files() -> None:
    """Bullets, paths and links count against the limit too — not only
    inline diff blocks. Overflow collapses into one explicit count line."""
    files = [
        _resolution(
            f"src/{'x' * 90}/f{i}.cc",
            _diff(f"f{i}", [_hunk([f"+l{j}" for j in range(40)])]),
        )
        for i in range(500)
    ]
    body = render_diff_comment(
        42,
        files,
        resolved_commit_sha="a" * 40,
        repo_html_url="https://github.com/example/repo",
        pr_html_url="https://github.com/example/repo/pull/1",
    )

    assert len(body) <= 65_536
    assert "more AI-edited files" in body


def test_file_link_prefers_the_resolution_own_commit() -> None:
    """A candidate can span two commits (cherry-pick + validation repair);
    each file must link into the commit that actually contains it."""
    pick = _resolution("src/a.c", _diff("src/a.c", [_hunk(["+a"])]))
    pick.commit_sha = "b" * 40
    repair = _resolution("src/b.c", _diff("src/b.c", [_hunk(["+b"])]))
    repair.commit_sha = "c" * 40
    body = render_diff_comment(
        42,
        [pick, repair],
        resolved_commit_sha="c" * 40,
        repo_html_url="https://github.com/example/repo",
    )

    assert f"/commit/{'b' * 40}#diff-" in body
    assert f"/commit/{'c' * 40}#diff-" in body


def test_adaptation_edits_become_reviewable_resolutions(tmp_path: Path) -> None:
    """An AI test adaptation must leave the same diff trail as a conflict
    resolution: the PR gets the AI label, so the evidence must exist."""
    import subprocess
    from types import SimpleNamespace

    from scripts.backport.missing_test_adaptation import (
        adapt_target_missing_tests_with_claude,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    for args in (
        ("init", "-q", "-b", "main"),
        ("config", "user.name", "T"),
        ("config", "user.email", "t@example.com"),
        ("config", "commit.gpgsign", "false"),
    ):
        subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)
    test_file = repo / "tests" / "unit.tcl"
    test_file.parent.mkdir()
    test_file.write_text("existing coverage\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "base"], cwd=repo, check=True, capture_output=True
    )

    def fake_agent(_profile, _prompt, *, cwd):
        Path(cwd, "tests", "unit.tcl").write_text(
            "existing coverage\nadapted coverage\n", encoding="utf-8"
        )
        return SimpleNamespace(
            returncode=0,
            stdout='{"type":"result","result":"adapted"}\n',
            stderr="",
        )

    result = adapt_target_missing_tests_with_claude(
        str(repo),
        BackportCandidate(
            source_pr_number=7,
            source_pr_title="t",
            source_pr_url="u",
            target_branch="8.1",
        ),
        {"tests/unit/upstream_only.tcl": "TEST\n"},
        language="c",
        run_agent_func=fake_agent,
    )

    assert result.fatal is False
    assert result.adapted_paths == ["tests/unit.tcl"]
    assert len(result.resolutions) == 1
    resolution = result.resolutions[0]
    assert resolution.path == "tests/unit.tcl"
    assert "+adapted coverage" in resolution.reviewer_diff
    assert resolution.resolved_content.endswith("adapted coverage\n")
