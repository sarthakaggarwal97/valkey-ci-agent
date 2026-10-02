"""Revert a single commit on an agent backport branch and push the result."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import tempfile

from github import Auth, Github

from scripts.backport.main import _run_git as run_git_default
from scripts.backport.sweep_git import BRANCH_PREFIX, clone_target_branch
from scripts.backport.sweep_prs import find_existing_pr
from scripts.common.git_auth import GitAuth
from scripts.common.logging_utils import configure_logging, log_outcome

logger = logging.getLogger(__name__)


def revert_commit(
    repo: str, branch: str, commit_sha: str, token: str,
    push_repo: str | None = None, base_branch: str | None = None,
) -> None:
    target_repo = push_repo or repo
    if not branch.startswith(f"{BRANCH_PREFIX}/"):
        raise ValueError(
            f"Refusing to edit non-namespaced branch: {branch!r}. "
            f"Agent targets must start with {BRANCH_PREFIX}/."
        )
    base_branch = base_branch or branch[len(f"{BRANCH_PREFIX}/"):]
    logger.info("Reverting %s on %s:%s (base branch %s, PR repo %s)",
                commit_sha, target_repo, branch, base_branch, repo)

    with tempfile.TemporaryDirectory(prefix="revert-commit-") as repo_dir, GitAuth(token) as auth:
        env = auth.env()
        clone_target_branch(target_repo, branch, repo_dir, env)

        run_git_default(repo_dir, "fetch", "--quiet", "origin", base_branch, env=env)
        if not _resolves_to_commit(repo_dir, commit_sha):
            raise RuntimeError(
                f"Commit {commit_sha} does not exist on {target_repo}:{branch} or "
                f"origin/{base_branch}; check the SHA."
            )
        if not _in_branch_range(repo_dir, base_branch, commit_sha):
            raise RuntimeError(
                f"Commit {commit_sha} is not unique to {branch} "
                f"(not in origin/{base_branch}..HEAD). Refusing to revert a base-branch commit."
            )

        if _is_merge(repo_dir, commit_sha):
            raise RuntimeError(f"Commit {commit_sha} is a merge commit; refusing to revert.")

        subject = _git(repo_dir, "log", "-1", "--format=%s", commit_sha)
        revert = subprocess.run(
            ["git", "revert", "--no-edit", commit_sha],
            cwd=repo_dir, capture_output=True, text=True, env=env,
        )
        if revert.returncode != 0:
            conflicts = _git(repo_dir, "diff", "--name-only", "--diff-filter=U")
            git_error = _tail(revert.stderr or revert.stdout)
            # An empty or refused revert may leave no sequencer state, and a
            # failing --abort must not replace git's actual reason.
            subprocess.run(["git", "revert", "--abort"], cwd=repo_dir,
                           capture_output=True, text=True, env=env)
            if conflicts:
                reason = f"a later commit overlaps it. Conflicts: {' '.join(conflicts.split())}"
            else:
                reason = f"git revert exited {revert.returncode}: {git_error or 'no output'}"
            raise RuntimeError(
                f"Cannot revert {commit_sha[:12]} ({subject!r}) on {branch}: "
                f"{reason}. Branch left untouched."
            )

        revert_sha = _git(repo_dir, "rev-parse", "HEAD")
        run_git_default(repo_dir, "push", "origin", branch, env=env)
        logger.info("Reverted %s (%r) on %s:%s with %s: https://github.com/%s/commit/%s",
                    commit_sha[:12], subject, target_repo, branch, revert_sha[:12],
                    target_repo, revert_sha)

    _note_pr(repo, target_repo, branch, commit_sha, subject, token)


def _note_pr(base_repo: str, push_repo: str, branch: str, commit_sha: str, subject: str, token: str) -> None:
    """Append a revert note to the branch's open PR, if one exists.

    The revert already landed; a missing PR or a GitHub hiccup here must
    not fail the operation, so failures are logged and swallowed.
    """
    try:
        gh = Github(auth=Auth.Token(token))
        pull = find_existing_pr(gh, base_repo, push_repo, branch)
        if pull is None:
            logger.info("No open PR for %s:%s; skipping the revert note", push_repo, branch)
            return
        note = f"\n\nReverted `{commit_sha[:12]}` ({subject})."
        pull.edit(body=(pull.body or "") + note)
        logger.info("Noted revert on PR #%d: %s", pull.number, pull.html_url)
    except Exception as exc:  # noqa: BLE001 - best-effort annotation
        logger.warning("Could not annotate PR for %s: %s", branch, exc)


def _resolves_to_commit(repo_dir: str, commit_sha: str) -> bool:
    return subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{commit_sha}^{{commit}}"],
        cwd=repo_dir, capture_output=True, text=True,
    ).returncode == 0


def _in_branch_range(repo_dir: str, base_branch: str, commit_sha: str) -> bool:
    """True if commit_sha is unique to the agent branch (not on the base)."""
    if not _resolves_to_commit(repo_dir, commit_sha):
        return False
    resolved = _git(repo_dir, "rev-parse", f"{commit_sha}^{{commit}}")
    revs = _git(repo_dir, "rev-list", f"origin/{base_branch}..HEAD").splitlines()
    return resolved in revs


def _tail(text: str, limit: int = 500) -> str:
    return " ".join((text or "").split())[-limit:]


def _is_merge(repo_dir: str, commit_sha: str) -> bool:
    parents = _git(repo_dir, "rev-list", "--parents", "-n", "1", commit_sha).split()
    return len(parents) > 2


def _git(repo_dir: str, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo_dir, capture_output=True, text=True, check=True,
    ).stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Revert a commit on an agent backport branch.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--push-repo", default="")
    parser.add_argument("--base-branch", default="")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    configure_logging(verbose=args.verbose)

    try:
        revert_commit(
            args.repo, args.branch, args.commit_sha, args.token,
            push_repo=args.push_repo or None,
            base_branch=args.base_branch or None,
        )
    except (ValueError, RuntimeError) as exc:
        log_outcome(logger, logging.ERROR, "Revert refused: %s", " ".join(str(exc).split()))
        return 1
    except subprocess.CalledProcessError as exc:
        log_outcome(logger, logging.ERROR, "Revert failed: git %s exited %d: %s",
                    " ".join(map(str, exc.cmd[1:2])), exc.returncode,
                    _tail(exc.stderr or exc.output or "") or "no output")
        return 1
    log_outcome(logger, logging.INFO, "Reverted %s on %s", args.commit_sha[:12], args.branch)
    return 0


if __name__ == "__main__":
    sys.exit(main())
