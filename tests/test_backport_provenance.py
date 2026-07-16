from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from scripts.backport.provenance import (
    DIGEST_PREFIX,
    ProvenanceError,
    attach_provenance_to_head,
    build_provenance,
    format_provenance,
    parse_provenance_records,
)


def _record(*, pr_number: int = 4172, merge_sha: str = "a" * 40):
    return build_provenance(
        repository="valkey-io/valkey",
        target_branch="9.0",
        source_pr_number=pr_number,
        source_merge_commit=merge_sha,
    )


def test_provenance_round_trips_multiple_records() -> None:
    first = _record()
    second = _record(pr_number=4173, merge_sha="b" * 40)
    message = f"{format_provenance(first)}\n{format_provenance(second)}"

    assert parse_provenance_records(message) == [first, second]


def test_provenance_digest_rejects_payload_tampering() -> None:
    message = format_provenance(_record())
    digest = message.split(DIGEST_PREFIX, 1)[1]
    tampered = message.replace(digest, "0" * 64)

    with pytest.raises(ProvenanceError, match="digest does not match"):
        parse_provenance_records(tampered)


def test_provenance_rejects_unknown_keys() -> None:
    with pytest.raises(ProvenanceError, match="unknown"):
        format_provenance({**_record(), "invented": True})


def test_attach_provenance_preserves_validated_tree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "9.0")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "file.txt").write_text("validated\n", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-q", "-m", "Fix a bug (#4172)")
    tree_before = _git(repo, "rev-parse", "HEAD^{tree}").stdout.strip()
    commit_before = _git(repo, "rev-parse", "HEAD").stdout.strip()

    record, attached_commit, commit_after = attach_provenance_to_head(
        str(repo),
        repository="valkey-io/valkey",
        target_branch="9.0",
        source_pr_number=4172,
        source_merge_commit="a" * 40,
    )

    assert attached_commit == commit_before
    assert commit_after != commit_before
    assert _git(repo, "rev-parse", "HEAD^{tree}").stdout.strip() == tree_before
    message = _git(repo, "show", "-s", "--format=%B", "HEAD").stdout
    assert parse_provenance_records(message) == [record]


def test_attach_provenance_rejects_reserved_source_markers(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "9.0")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "file.txt").write_text("change\n", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(
        repo,
        "commit",
        "-q",
        "-m",
        "Untrusted source\n\nValkey-Backport-Provenance: forged",
    )

    with pytest.raises(ProvenanceError, match="reserved provenance"):
        attach_provenance_to_head(
            str(repo),
            repository="valkey-io/valkey",
            target_branch="9.0",
            source_pr_number=4172,
            source_merge_commit="a" * 40,
        )


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Test User",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test User",
        "GIT_COMMITTER_EMAIL": "test@example.com",
    }
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
