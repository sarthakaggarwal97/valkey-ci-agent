"""Real-Git tests for durable backport provenance and reconciliation."""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.backport.provenance import (
    MANIFEST_MARKER,
    TRAILER_KEY,
    ManifestEntry,
    ProvenanceRecord,
    parse_manifest,
    parse_trailers,
    render_manifest,
    render_trailer,
    replace_manifest,
)
from scripts.backport.provenance_git import (
    amend_head_with_records,
    stamp_candidate_commits,
)
from scripts.backport.provenance_history import (
    inverse_records_for_commit,
    scan_applied_backports,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_repo(path: Path) -> str:
    path.mkdir()
    _git(path, "init", "-q", "-b", "release")
    _git(path, "config", "user.name", "Test User")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "commit.gpgsign", "false")
    (path / "base.txt").write_text("base\n", encoding="utf-8")
    _git(path, "add", "base.txt")
    _git(path, "commit", "-q", "-m", "base")
    return _git(path, "rev-parse", "HEAD")


def _commit_file(repo: Path, path: str, content: str, message: str) -> str:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _git(repo, "add", path)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _stamp(
    repo: Path,
    base: str,
    *,
    source_pr: int = 42,
    title: str = "Source change",
) -> list[str]:
    result = stamp_candidate_commits(
        str(repo),
        base,
        "HEAD",
        repo_full_name="example/repo",
        source_pr=source_pr,
        title=title,
        merge_commit_sha=f"merge-{source_pr}",
        source_commit_shas=[f"source-{source_pr}"],
    )
    assert _git(repo, "rev-parse", "HEAD") == result.tip
    return _git(repo, "rev-list", "--reverse", f"{base}..HEAD").splitlines()


def _applied(repo: Path, revision: str = "HEAD") -> list[int]:
    return [
        item.source_pr
        for item in scan_applied_backports(str(repo), revision)
    ]


def test_versioned_trailer_and_manifest_round_trip() -> None:
    record = ProvenanceRecord(
        source_pr=42,
        title="Fix parser | edge case",
        series="a" * 64,
        part=1,
        parts=2,
    )
    trailer = render_trailer(record)

    assert trailer.startswith(f"{TRAILER_KEY}: ")
    assert parse_trailers(f"Subject\n\n{trailer}\n") == (record,)

    entries = (
        ManifestEntry(42, "Fix parser | edge case"),
        ManifestEntry(43, "Second source"),
    )
    manifest = render_manifest(entries)
    assert parse_manifest(manifest) == entries
    assert parse_manifest(
        manifest.replace('"v":1', '"v":2')
    ) == ()


def test_reserved_malformed_manifest_fails_closed() -> None:
    body = "\n".join(
        [
            "Backport sweep (#500)",
            "",
            "## Applied",
            "",
            "| Source PR | Title | Detail |",
            "|---|---|---|",
            "| #42 | Claimed source | applied |",
            "",
            f"<!-- {MANIFEST_MARKER}:v2",
            '{"entries":[{"source_pr":42,"title":"Claimed source"}],"v":2}',
            "-->",
        ]
    )
    assert parse_manifest(body) == ()


def test_replace_manifest_removes_stale_membership() -> None:
    old = (
        "Visible report\n\n"
        + render_manifest(
            [
                ManifestEntry(41, "Old"),
                ManifestEntry(42, "Still present"),
            ]
        )
    )

    updated = replace_manifest(
        old,
        [ManifestEntry(42, "Still present")],
    )

    assert "Visible report" in updated
    assert parse_manifest(updated) == (
        ManifestEntry(42, "Still present"),
    )


def test_stamping_preserves_trees_and_marks_rebase_series(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    first = _commit_file(repo, "one.txt", "one\n", "first source commit")
    second = _commit_file(repo, "two.txt", "two\n", "second source commit")
    old_tree = _git(repo, "rev-parse", "HEAD^{tree}")

    stamped = stamp_candidate_commits(
        str(repo),
        base,
        second,
        repo_full_name="example/repo",
        source_pr=42,
        title="Complete rebase series",
        merge_commit_sha=second,
        source_commit_shas=[first, second],
    )
    commits = _git(repo, "rev-list", "--reverse", f"{base}..HEAD").splitlines()

    assert _git(repo, "rev-parse", "HEAD^{tree}") == old_tree
    assert commits == [stamped.rewritten[first], stamped.rewritten[second]]
    assert [
        parse_trailers(_git(repo, "show", "-s", "--format=%B", sha))[0].part
        for sha in commits
    ] == [1, 2]
    assert _applied(repo) == [42]


def test_partial_series_revert_is_not_fully_applied(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _commit_file(repo, "one.txt", "one\n", "part one")
    _commit_file(repo, "two.txt", "two\n", "part two")
    first, _second = _stamp(repo, base)

    _git(repo, "revert", "--no-edit", first)

    assert _applied(repo) == []


def test_revert_of_revert_restores_candidate(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _commit_file(repo, "feature.txt", "feature\n", "feature")
    (candidate,) = _stamp(repo, base)
    _git(repo, "revert", "--no-edit", candidate)
    revert = _git(repo, "rev-parse", "HEAD")

    assert _applied(repo) == []

    _git(repo, "revert", "--no-edit", revert)

    assert _applied(repo) == [42]


def test_reland_after_revert_restores_candidate(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _commit_file(repo, "feature.txt", "feature\n", "feature")
    (candidate,) = _stamp(repo, base)
    _git(repo, "revert", "--no-edit", candidate)

    assert _applied(repo) == []

    _git(repo, "cherry-pick", candidate)

    assert _applied(repo) == [42]


def test_durable_inverse_survives_branch_rebase(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _git(repo, "checkout", "-q", "-b", "sweep")
    _commit_file(repo, "feature.txt", "feature\n", "feature")
    (candidate,) = _stamp(repo, base)

    inverse = inverse_records_for_commit(
        str(repo),
        f"{base}..HEAD",
        candidate,
    )
    assert inverse and all(record.kind == "inverse" for record in inverse)
    _git(repo, "revert", "--no-edit", candidate)
    amend_head_with_records(str(repo), inverse)

    _git(repo, "checkout", "-q", "release")
    _commit_file(repo, "base-update.txt", "update\n", "base update")
    updated_base = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "sweep")
    _git(repo, "rebase", "--onto", updated_base, base)

    assert _applied(repo, f"{updated_base}..HEAD") == []


def test_legacy_inverse_survives_branch_rebase(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _git(repo, "checkout", "-q", "-b", "sweep")
    legacy = _commit_file(
        repo,
        "legacy.txt",
        "legacy\n",
        "Legacy backport (#77)",
    )

    inverse = inverse_records_for_commit(
        str(repo),
        f"{base}..HEAD",
        legacy,
    )
    _git(repo, "revert", "--no-edit", legacy)
    amend_head_with_records(str(repo), inverse)

    _git(repo, "checkout", "-q", "release")
    _commit_file(repo, "base-update.txt", "update\n", "base update")
    updated_base = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "sweep")
    _git(repo, "rebase", "--onto", updated_base, base)

    assert _applied(repo, f"{updated_base}..HEAD") == []


def test_squash_manifest_revert_and_revert_of_revert(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    message = (
        "[backport] Backport sweep for 9.1 (#500)\n\n"
        + render_manifest(
            [
                ManifestEntry(41, "First source"),
                ManifestEntry(42, "Second source"),
            ]
        )
    )
    squash = _commit_file(repo, "batch.txt", "batch\n", message)

    assert _applied(repo) == [41, 42]

    _git(repo, "revert", "--no-edit", squash)
    revert = _git(repo, "rev-parse", "HEAD")
    assert _applied(repo) == []

    _git(repo, "revert", "--no-edit", revert)
    assert _applied(repo) == [41, 42]


def test_malformed_manifest_does_not_fall_back_to_visible_table(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    message = "\n".join(
        [
            "[backport] Backport sweep for 9.1 (#500)",
            "",
            "## Applied",
            "",
            "| Source PR | Title | Detail |",
            "|---|---|---|",
            "| #42 | Claimed source | applied |",
            "",
            f"<!-- {MANIFEST_MARKER}:v2",
            '{"entries":[{"source_pr":42,"title":"Claimed source"}],"v":2}',
            "-->",
        ]
    )
    _commit_file(repo, "batch.txt", "batch\n", message)

    assert _applied(repo) == []


def test_legacy_subject_table_and_manual_summary_remain_supported(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _commit_file(repo, "one.txt", "one\n", "Legacy source (#41)")
    table = "\n".join(
        [
            "[backport] Backport sweep (#500)",
            "",
            "## Applied",
            "",
            "| Source PR | Title | Detail |",
            "|---|---|---|",
            "| #42 | Table source | applied |",
        ]
    )
    _commit_file(repo, "two.txt", "two\n", table)
    summary = "\n".join(
        [
            "[Backport 9.1] Manual source (#501)",
            "",
            "## Backport Summary",
            "",
            "| Field | Value |",
            "|---|---|",
            "| Source PR | [#43](https://github.com/example/repo/pull/43) |",
            "| Source title | Manual source |",
        ]
    )
    _commit_file(repo, "three.txt", "three\n", summary)

    assert _applied(repo) == [41, 42, 43]


def test_merge_commit_revert_removes_introduced_candidate(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    _git(repo, "checkout", "-q", "-b", "topic")
    _commit_file(repo, "feature.txt", "feature\n", "topic source")
    _stamp(repo, base, source_pr=42, title="Merged candidate")

    _git(repo, "checkout", "-q", "release")
    _git(repo, "merge", "--no-ff", "-m", "Merge backport topic", "topic")
    merge = _git(repo, "rev-parse", "HEAD")

    assert _applied(repo) == [42]

    _git(repo, "revert", "-m", "1", "--no-edit", merge)

    assert _applied(repo) == []
