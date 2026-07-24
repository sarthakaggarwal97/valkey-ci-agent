"""Tree-neutral Git writes for the backport provenance contract."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Sequence

from scripts.backport.provenance import (
    ProvenanceRecord,
    candidate_series_id,
    message_with_records,
    normalize_title,
)


@dataclass(frozen=True)
class StampResult:
    tip: str
    rewritten: dict[str, str]


def stamp_candidate_commits(
    repo_dir: str,
    base_ref: str,
    tip_ref: str,
    *,
    repo_full_name: str,
    source_pr: int,
    title: str,
    merge_commit_sha: str | None,
    source_commit_shas: Sequence[str],
) -> StampResult:
    """Rewrite a linear validated candidate range with tree-neutral trailers."""

    commits = _git_lines(
        repo_dir,
        "rev-list",
        "--reverse",
        "--first-parent",
        f"{base_ref}..{tip_ref}",
    )
    if not commits:
        raise RuntimeError("cannot stamp an empty backport candidate range")

    expected_parent = _resolve_commit(repo_dir, base_ref)
    resolved_tip = _resolve_commit(repo_dir, tip_ref)
    series = candidate_series_id(
        repo_full_name,
        source_pr,
        merge_commit_sha,
        source_commit_shas,
    )
    rewritten: dict[str, str] = {}
    new_parent = expected_parent

    for index, old_sha in enumerate(commits, start=1):
        metadata = _read_commit_metadata(repo_dir, old_sha)
        parents = os.fsdecode(metadata["parents"]).split()
        if parents != [expected_parent]:
            raise RuntimeError(
                "candidate provenance requires a linear commit range; "
                f"{old_sha} has parent(s) {parents!r}, expected {expected_parent}"
            )

        record = ProvenanceRecord(
            source_pr=source_pr,
            title=normalize_title(title, source_pr),
            series=series,
            part=index,
            parts=len(commits),
        )
        message = message_with_records(metadata["message"], (record,))
        env = dict(os.environ)
        env.update(
            {
                "GIT_AUTHOR_NAME": os.fsdecode(metadata["author_name"]),
                "GIT_AUTHOR_EMAIL": os.fsdecode(metadata["author_email"]),
                "GIT_AUTHOR_DATE": _git_date(
                    metadata["author_timestamp"],
                    metadata["author_iso"],
                ),
                "GIT_COMMITTER_NAME": os.fsdecode(metadata["committer_name"]),
                "GIT_COMMITTER_EMAIL": os.fsdecode(metadata["committer_email"]),
                "GIT_COMMITTER_DATE": _git_date(
                    metadata["committer_timestamp"],
                    metadata["committer_iso"],
                ),
            }
        )
        created = subprocess.run(
            [
                "git",
                "commit-tree",
                os.fsdecode(metadata["tree"]),
                "-p",
                new_parent,
            ],
            cwd=repo_dir,
            input=message,
            capture_output=True,
            env=env,
        )
        if created.returncode != 0:
            raise RuntimeError(
                f"could not stamp backport commit {old_sha}: "
                + (
                    os.fsdecode(created.stderr).strip()[:300]
                    or "git commit-tree failed"
                )
            )
        new_sha = os.fsdecode(created.stdout).strip()
        rewritten[old_sha] = new_sha
        expected_parent = old_sha
        new_parent = new_sha

    if commits[-1] != resolved_tip:
        raise RuntimeError(
            f"candidate range did not end at {tip_ref}: "
            f"found {commits[-1]}, expected {resolved_tip}"
        )
    old_tree = _git_output(repo_dir, "rev-parse", f"{resolved_tip}^{{tree}}")
    new_tree = _git_output(repo_dir, "rev-parse", f"{new_parent}^{{tree}}")
    if old_tree != new_tree:
        raise RuntimeError("provenance stamping changed the validated candidate tree")

    _run_git(repo_dir, "reset", "--hard", new_parent)
    return StampResult(tip=new_parent, rewritten=rewritten)


def amend_head_with_records(
    repo_dir: str,
    records: Sequence[ProvenanceRecord],
) -> str:
    """Add canonical provenance records to HEAD without changing its tree."""

    if not records:
        return _resolve_commit(repo_dir, "HEAD")
    message = subprocess.run(
        ["git", "show", "-s", "--format=%B", "HEAD"],
        cwd=repo_dir,
        capture_output=True,
        check=True,
    ).stdout
    updated = message_with_records(message, records)
    before_tree = _git_output(repo_dir, "rev-parse", "HEAD^{tree}")
    amended = subprocess.run(
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "--amend",
            "--no-verify",
            "-F",
            "-",
        ],
        cwd=repo_dir,
        input=updated,
        capture_output=True,
    )
    if amended.returncode != 0:
        raise RuntimeError(
            "could not stamp revert provenance: "
            + (
                os.fsdecode(amended.stderr).strip()[:300]
                or "git commit --amend failed"
            )
        )
    after = _resolve_commit(repo_dir, "HEAD")
    if _git_output(repo_dir, "rev-parse", "HEAD^{tree}") != before_tree:
        raise RuntimeError("revert provenance stamping changed the commit tree")
    return after


def _read_commit_metadata(repo_dir: str, sha: str) -> dict[str, bytes]:
    format_value = (
        "%T%x00%P%x00%an%x00%ae%x00%at%x00%ai%x00"
        "%cn%x00%ce%x00%ct%x00%ci%x00%B"
    )
    result = subprocess.run(
        ["git", "show", "-s", f"--format={format_value}", sha],
        cwd=repo_dir,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"could not read commit {sha}: "
            + (
                os.fsdecode(result.stderr).strip()[:300]
                or "git show failed"
            )
        )
    fields = result.stdout.split(b"\x00", 10)
    if len(fields) != 11:
        raise RuntimeError(f"could not parse commit metadata for {sha}")
    names = (
        "tree",
        "parents",
        "author_name",
        "author_email",
        "author_timestamp",
        "author_iso",
        "committer_name",
        "committer_email",
        "committer_timestamp",
        "committer_iso",
        "message",
    )
    return dict(zip(names, fields))


def _git_date(timestamp: bytes, iso_value: bytes) -> str:
    iso_parts = os.fsdecode(iso_value).strip().split()
    if not iso_parts:
        raise RuntimeError("commit date is missing a timezone")
    return f"@{os.fsdecode(timestamp).strip()} {iso_parts[-1]}"


def _resolve_commit(repo_dir: str, ref: str) -> str:
    return _git_output(repo_dir, "rev-parse", f"{ref}^{{commit}}")


def _git_lines(repo_dir: str, *args: str) -> list[str]:
    output = _git_output(repo_dir, *args)
    return [line for line in output.splitlines() if line]


def _git_output(repo_dir: str, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: "
            + ((result.stderr or "").strip()[:300] or "git command failed")
        )
    return result.stdout.strip()


def _run_git(repo_dir: str, *args: str) -> None:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: "
            + ((result.stderr or "").strip()[:300] or "git command failed")
        )
