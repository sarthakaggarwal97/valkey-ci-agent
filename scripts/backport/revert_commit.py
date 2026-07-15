"""Revert one commit on a legacy agent sweep branch."""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile

from github import Auth, Github

from scripts.backport.pr_creator import find_existing_pr
from scripts.common.git_auth import GitAuth, github_https_url
from scripts.common.markdown import bounded_body, escape_text, inline_code
from scripts.common.proc import (
    BOT_EMAIL,
    BOT_NAME,
    GitPathEncodingError,
    decode_git_paths,
    git_output,
    run_git,
    run_git_bytes,
)

logger = logging.getLogger(__name__)

BRANCH_PREFIX = "agent/backport/sweep"


def revert_commit(
    repo: str,
    branch: str,
    commit_sha: str,
    token: str,
    push_repo: str | None = None,
    base_branch: str | None = None,
) -> None:
    target_repo = push_repo or repo
    if not branch.startswith(f"{BRANCH_PREFIX}/"):
        raise ValueError(
            f"Refusing to edit non-namespaced branch: {branch!r}. "
            f"Agent targets must start with {BRANCH_PREFIX}/."
        )
    base_branch = base_branch or branch[len(f"{BRANCH_PREFIX}/"):]

    with (
        tempfile.TemporaryDirectory(prefix="revert-commit-") as repo_dir,
        GitAuth(token, prefix="revert-commit-") as auth,
    ):
        env = auth.env()
        run_git(
            None,
            "clone",
            "--branch",
            branch,
            github_https_url(target_repo),
            repo_dir,
            env=env,
        )
        run_git(repo_dir, "config", "user.name", BOT_NAME)
        run_git(repo_dir, "config", "user.email", BOT_EMAIL)
        run_git(
            repo_dir,
            "fetch",
            "--quiet",
            "origin",
            base_branch,
            env=env,
        )
        if not _in_branch_range(repo_dir, base_branch, commit_sha):
            raise RuntimeError(
                f"Commit {commit_sha} is not unique to {branch} "
                f"(not in origin/{base_branch}..HEAD). Refusing to revert a "
                "base-branch commit."
            )
        if _is_merge(repo_dir, commit_sha):
            raise RuntimeError(
                f"Commit {commit_sha} is a merge commit; refusing to revert."
            )

        subject = _git(repo_dir, "log", "-1", "--format=%s", commit_sha)
        revert = run_git(
            repo_dir,
            "revert",
            "--no-edit",
            commit_sha,
            check=False,
        )
        if revert.returncode != 0:
            conflict_result = run_git_bytes(
                repo_dir,
                "diff",
                "--name-only",
                "-z",
                "--diff-filter=U",
            )
            try:
                conflict_paths = decode_git_paths(
                    conflict_result.stdout,
                    context="revert conflict path list",
                )
                conflicts = ", ".join(repr(path) for path in conflict_paths)
            except GitPathEncodingError as exc:
                conflicts = str(exc)
            run_git(repo_dir, "revert", "--abort")
            raise RuntimeError(
                f"Cannot revert {commit_sha[:12]} ({subject!r}) on {branch}: "
                "a later commit overlaps it. Conflicts: "
                f"{conflicts or 'unknown'}. Branch left untouched."
            )

        run_git(
            repo_dir,
            "push",
            "origin",
            f"HEAD:refs/heads/{branch}",
            env=env,
        )
        logger.info(
            "Reverted %s (%r) on %s:%s",
            commit_sha[:12],
            subject,
            target_repo,
            branch,
        )

    _note_pr(repo, target_repo, branch, commit_sha, subject, token)


def _note_pr(
    base_repo: str,
    push_repo: str,
    branch: str,
    commit_sha: str,
    subject: str,
    token: str,
) -> None:
    """Append a bounded revert note when the legacy sweep PR still exists."""
    try:
        gh = Github(auth=Auth.Token(token))
        pull = find_existing_pr(gh, base_repo, push_repo, branch)
        if pull is None:
            return
        note = (
            f"\n\nReverted {inline_code(commit_sha[:12])} "
            f"({escape_text(subject, max_bytes=1024, multiline=False)})."
        )
        pull.edit(body=bounded_body((pull.body or "") + note))
        logger.info("Noted revert on PR #%d", pull.number)
    except Exception as exc:
        logger.warning("Could not annotate PR for %s: %s", branch, exc)


def _in_branch_range(repo_dir: str, base_branch: str, commit_sha: str) -> bool:
    resolved = run_git(
        repo_dir,
        "rev-parse",
        "--verify",
        "--quiet",
        f"{commit_sha}^{{commit}}",
        check=False,
    )
    if resolved.returncode != 0:
        return False
    revs = _git(
        repo_dir,
        "rev-list",
        f"origin/{base_branch}..HEAD",
    ).splitlines()
    return resolved.stdout.strip() in revs


def _is_merge(repo_dir: str, commit_sha: str) -> bool:
    parents = _git(
        repo_dir,
        "rev-list",
        "--parents",
        "-n",
        "1",
        commit_sha,
    ).split()
    return len(parents) > 2


def _git(repo_dir: str, *args: str) -> str:
    return git_output(repo_dir, *args).strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Revert a commit on a legacy agent sweep branch.",
    )
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--push-repo", default="")
    parser.add_argument("--base-branch", default="")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    try:
        revert_commit(
            args.repo,
            args.branch,
            args.commit_sha,
            args.token,
            push_repo=args.push_repo or None,
            base_branch=args.base_branch or None,
        )
    except (ValueError, RuntimeError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
