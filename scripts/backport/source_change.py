"""Resolve the complete Git change represented by a merged pull request."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Literal, Mapping, Sequence

SourceChangeStrategy = Literal["merge", "squash", "single", "series"]


class SourceChangeError(RuntimeError):
    """Raised when a pull request's source history cannot be resolved safely."""


@dataclass(frozen=True)
class SourceChangePlan:
    """A deterministic plan for applying one pull request.

    ``commits`` is either the authoritative merge/squash commit or the ordered
    non-merge commits GitHub would replay for a rebase merge. ``source_commits``
    retains the complete source identity, including source-branch sync merges.
    Callers must not substitute a different SHA.
    """

    strategy: SourceChangeStrategy
    commits: tuple[str, ...]
    merge_commit_sha: str | None
    source_commits: tuple[str, ...]
    aggregate_patch_id: str


def prepare_source_change(
    repo_dir: str,
    source_pr_number: int,
    merge_commit_sha: str | None,
    commit_shas: Sequence[str],
    *,
    source_commits_complete: bool = True,
    remote: str = "origin",
    git_env: Mapping[str, str] | None = None,
) -> SourceChangePlan:
    """Fetch the authoritative PR objects and return their application plan."""

    if not source_commits_complete:
        raise SourceChangeError(
            f"source commit list for PR #{source_pr_number} is incomplete"
        )

    source_commits = tuple(sha for sha in commit_shas if sha)
    missing_source_commits = tuple(
        sha for sha in source_commits if not _commit_exists(repo_dir, sha)
    )
    if missing_source_commits:
        source_ref = (
            f"+refs/pull/{source_pr_number}/head:"
            f"refs/valkey-ci-agent/backport/{source_pr_number}/head"
        )
        _git(
            repo_dir,
            "fetch",
            "--no-tags",
            remote,
            source_ref,
            env=git_env,
        )
        still_missing = tuple(
            sha for sha in missing_source_commits if not _commit_exists(repo_dir, sha)
        )
        if still_missing:
            raise SourceChangeError(
                "PR head does not contain source commit(s): "
                + ", ".join(still_missing)
            )

    if merge_commit_sha and not _commit_exists(repo_dir, merge_commit_sha):
        _git(
            repo_dir,
            "fetch",
            "--no-tags",
            remote,
            merge_commit_sha,
            env=git_env,
        )
        if not _commit_exists(repo_dir, merge_commit_sha):
            raise SourceChangeError(
                f"could not fetch merge commit {merge_commit_sha}"
            )

    return plan_source_change(repo_dir, merge_commit_sha, source_commits)


def plan_source_change(
    repo_dir: str,
    merge_commit_sha: str | None,
    commit_shas: Sequence[str],
) -> SourceChangePlan:
    """Return a complete application plan for a merged pull request.

    GitHub's ``merge_commit_sha`` has different meanings by merge method. It is
    a real merge commit for "merge", an aggregate commit for "squash", and only
    the final rewritten commit for a multi-commit "rebase and merge". The last
    case must use the full source series or earlier commits are silently lost.

    For a one-parent merge SHA and multiple source commits, compare exact patch
    identities. A matching aggregate is a squash commit; a non-match is treated
    as a rebase and uses the complete source series. Disconnected or unreadable
    histories fail closed.
    """

    source_commits = tuple(sha for sha in commit_shas if sha)
    if len(set(source_commits)) != len(source_commits):
        raise SourceChangeError("source commit list contains duplicate SHAs")

    if not merge_commit_sha:
        if not source_commits:
            raise SourceChangeError("pull request has neither a merge SHA nor source commits")
        source_patch_id = _series_patch_id(repo_dir, source_commits)
        strategy: SourceChangeStrategy = "single" if len(source_commits) == 1 else "series"
        return SourceChangePlan(
            strategy=strategy,
            commits=(
                source_commits
                if strategy == "single"
                else _replayable_series_commits(repo_dir, source_commits)
            ),
            merge_commit_sha=None,
            source_commits=source_commits,
            aggregate_patch_id=source_patch_id,
        )

    merge_parents = _commit_parents(repo_dir, merge_commit_sha)
    if not merge_parents:
        raise SourceChangeError(f"merge SHA {merge_commit_sha} has no parent")

    if len(merge_parents) > 1:
        return SourceChangePlan(
            strategy="merge",
            commits=(merge_commit_sha,),
            merge_commit_sha=merge_commit_sha,
            source_commits=source_commits,
            aggregate_patch_id=_exact_patch_id(
                repo_dir,
                merge_parents[0],
                merge_commit_sha,
            ),
        )

    merge_patch_id = _exact_patch_id(
        repo_dir,
        merge_parents[0],
        merge_commit_sha,
    )
    if len(source_commits) <= 1:
        return SourceChangePlan(
            strategy="single",
            commits=(merge_commit_sha,),
            merge_commit_sha=merge_commit_sha,
            source_commits=source_commits,
            aggregate_patch_id=merge_patch_id,
        )

    if merge_commit_sha in source_commits:
        source_patch_id = _series_patch_id(repo_dir, source_commits)
        return SourceChangePlan(
            strategy="series",
            commits=_replayable_series_commits(repo_dir, source_commits),
            merge_commit_sha=merge_commit_sha,
            source_commits=source_commits,
            aggregate_patch_id=source_patch_id,
        )

    source_base = _unique_merge_base(
        repo_dir,
        merge_parents[0],
        source_commits[-1],
    )
    source_patch_id = _exact_patch_id(
        repo_dir,
        source_base,
        source_commits[-1],
    )
    if source_patch_id and source_patch_id == merge_patch_id:
        return SourceChangePlan(
            strategy="squash",
            commits=(merge_commit_sha,),
            merge_commit_sha=merge_commit_sha,
            source_commits=source_commits,
            aggregate_patch_id=merge_patch_id,
        )

    _validate_series(repo_dir, source_commits)
    return SourceChangePlan(
        strategy="series",
        commits=_replayable_series_commits(repo_dir, source_commits),
        merge_commit_sha=merge_commit_sha,
        source_commits=source_commits,
        aggregate_patch_id=source_patch_id,
    )


def _series_patch_id(repo_dir: str, commits: tuple[str, ...]) -> str:
    first_parent = _validate_series(repo_dir, commits)
    return _exact_patch_id(repo_dir, first_parent, commits[-1])


def _validate_series(repo_dir: str, commits: tuple[str, ...]) -> str:
    first_parents = _commit_parents(repo_dir, commits[0])
    if not first_parents:
        raise SourceChangeError(f"source commit {commits[0]} has no parent")
    for previous, current in zip(commits, commits[1:]):
        result = _git(
            repo_dir,
            "merge-base",
            "--is-ancestor",
            previous,
            current,
            check=False,
        )
        if result.returncode == 1:
            raise SourceChangeError(
                f"source commits are disconnected or out of order: "
                f"{previous} is not an ancestor of {current}"
            )
        if result.returncode != 0:
            raise SourceChangeError(
                f"could not verify source commit order for {previous}..{current}: "
                f"{result.stderr.strip()[:300]}"
            )
    return first_parents[0]


def _replayable_series_commits(
    repo_dir: str,
    commits: tuple[str, ...],
) -> tuple[str, ...]:
    """Return the non-merge commits GitHub rebase-and-merge would replay.

    Merge commits in the series are source-branch syncs whose target-side
    content is not part of the pull request's change, so they are skipped —
    but only when they carry no content of their own. A merge whose tree
    differs from the automatic merge of its parents contains manual edits
    (conflict resolutions, or fixes slipped into the merge), and replaying
    only the non-merge commits would silently lose them.
    """

    replayable: list[str] = []
    for sha in commits:
        parents = _commit_parents(repo_dir, sha)
        if len(parents) == 1:
            replayable.append(sha)
            continue
        if not _merge_is_content_neutral(repo_dir, sha, parents):
            raise SourceChangeError(
                f"source merge commit {sha} carries changes of its own "
                "(e.g. a manual conflict resolution); replaying the series "
                "would silently drop them, so this pull request must be "
                "backported manually"
            )
    if not replayable:
        raise SourceChangeError(
            "source series contains only merge commits; cannot reproduce "
            "rebase-and-merge safely"
        )
    return tuple(replayable)


def _merge_is_content_neutral(
    repo_dir: str,
    sha: str,
    parents: tuple[str, ...],
) -> bool:
    """Whether *sha*'s tree is exactly the automatic merge of its parents.

    A conflicting automatic merge means the commit resolved those conflicts
    by hand, so it is never neutral.
    """

    if len(parents) != 2:
        return False
    result = _git(
        repo_dir,
        "merge-tree",
        "--write-tree",
        parents[0],
        parents[1],
        check=False,
    )
    if result.returncode not in (0, 1):
        raise SourceChangeError(
            f"could not compute the automatic merge for {sha}: "
            f"{result.stderr.strip()[:300]}"
        )
    if result.returncode == 1:
        return False
    fields = result.stdout.split()
    auto_tree = fields[0] if fields else ""
    actual_tree = _git(repo_dir, "rev-parse", f"{sha}^{{tree}}").stdout.strip()
    return bool(auto_tree) and auto_tree == actual_tree


def _unique_merge_base(repo_dir: str, left: str, right: str) -> str:
    result = _git(repo_dir, "merge-base", "--all", left, right)
    bases = tuple(line for line in result.stdout.splitlines() if line)
    if not bases:
        raise SourceChangeError(
            f"no merge base exists for {left} and {right}; source history is "
            "disconnected"
        )
    if len(bases) > 1:
        raise SourceChangeError(
            f"ambiguous merge base for {left} and {right}: found {len(bases)}; "
            "cannot safely classify this pull request as squash or rebase"
        )
    return bases[0]


def _commit_parents(repo_dir: str, sha: str) -> tuple[str, ...]:
    result = _git(repo_dir, "rev-list", "--parents", "-n", "1", sha)
    fields = result.stdout.strip().split()
    if not fields or fields[0] != sha:
        raise SourceChangeError(f"could not resolve commit {sha}")
    return tuple(fields[1:])


def _commit_exists(repo_dir: str, sha: str) -> bool:
    result = _git(
        repo_dir,
        "cat-file",
        "-e",
        f"{sha}^{{commit}}",
        check=False,
    )
    return result.returncode == 0


def _exact_patch_id(repo_dir: str, base: str, tip: str) -> str:
    diff = _git_bytes(
        repo_dir,
        "diff",
        "--binary",
        "--full-index",
        "--no-ext-diff",
        base,
        tip,
    )
    result = subprocess.run(
        ["git", "patch-id", "--verbatim"],
        cwd=repo_dir,
        input=diff.stdout,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SourceChangeError(
            f"could not compute patch identity for {base}..{tip}: "
            f"{result.stderr.decode('utf-8', errors='replace').strip()[:300]}"
        )
    fields = result.stdout.decode("ascii", errors="replace").strip().split()
    return fields[0] if fields else ""


def _git(
    repo_dir: str,
    *args: str,
    check: bool = True,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if check and result.returncode != 0:
        raise SourceChangeError(
            f"git {' '.join(args)} failed: {result.stderr.strip()[:300]}"
        )
    return result


def _git_bytes(
    repo_dir: str,
    *args: str,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SourceChangeError(
            f"git {' '.join(args)} failed: "
            f"{result.stderr.decode('utf-8', errors='replace').strip()[:300]}"
        )
    return result
