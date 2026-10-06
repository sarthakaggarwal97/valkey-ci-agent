"""Commit a validated fix and push it to an ``agent/...`` branch.

This is the only place ``ci_fix`` mutates a repository, so it carries the push
discipline:

- The fix is committed authored as the bot, without a DCO sign-off - a human
  must certify the change before it can be merged upstream. Local git
  commands run with a scrubbed environment so a repository git hook can never
  read a credential from the ambient environment.
- The push target must be a branch in a namespace this engine owns
  (``agent/backport/...`` sweep branches and ``agent/ci-fix/...`` fix branches)
  on the target repository. Release branches, the default branch, contributor
  branches, and other bots' branches (e.g. ``agent/release-cut/...``) are
  never written.
- The generated commit must descend from the validated base SHA, and the push
  uses an exact lease: the remote branch must still equal that SHA when
  updating an existing PR branch, or must not exist yet when creating one. Git
  therefore rejects a moved, deleted, or concurrently created branch instead of
  overwriting it.

The branch is never merged. The push re-triggers the repository's normal CI.
"""

from __future__ import annotations

import logging
import re
import subprocess
import tempfile
import textwrap
from pathlib import Path
from typing import Callable

from scripts.ci_fix.models import FixProposal
from scripts.ci_fix.port_discovery import resolve_default_branch
from scripts.common.git_auth import github_https_url
from scripts.common.git_clone import REPO_RE, SHA_RE
from scripts.common.proc import BOT_EMAIL, BOT_NAME, git_output, run_git

logger = logging.getLogger(__name__)

# The branch namespaces CI fix may write. Other ``agent/`` namespaces belong
# to other automation (release cuts) whose PRs must not gain CI-fix commits.
ALLOWED_BRANCH_PREFIXES = ("agent/backport/", "agent/ci-fix/")
PrePushCheck = Callable[[], str]


class PushRefused(Exception):
    """Raised when a push target falls outside the allowed namespace."""


def commit_and_push_fix(
    *,
    patch: str,
    changed_paths: tuple[str, ...],
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    proposal: FixProposal,
    git_env: dict[str, str],
    pre_push_check: PrePushCheck | None = None,
    create: bool = False,
) -> str:
    """Commit the approved ``patch`` on ``head_sha`` and push it to ``head_branch``.

    The checkout that produced the patch may have run untrusted test code, so it
    is never pushed from: the patch is applied in a fresh clone at ``head_sha``
    and must stage exactly ``changed_paths``. The clean clone is the only
    checkout that receives credentials. ``create`` pushes a new branch and
    requires that it does not exist yet. Returns the new commit SHA. Raises
    ``PushRefused`` if any trust-boundary check fails.
    """
    _validate_target(head_repo_full_name, head_branch, head_sha)
    if not changed_paths:
        raise PushRefused("Refusing to push: no approved changed paths to stage.")
    if not patch.strip():
        raise PushRefused("Refusing to push: the approved patch is empty.")

    with tempfile.TemporaryDirectory(prefix="ci-fix-push-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(head_repo_full_name, clean_repo)
        try:
            run_git(str(clean_repo), "checkout", "--detach", head_sha)
            _apply_patch(str(clean_repo), patch)

            staged = _staged_paths(str(clean_repo))
            if staged != tuple(sorted(changed_paths)):
                raise PushRefused(
                    "Refusing to push: approved patch staged unexpected paths "
                    f"{staged!r} (expected {tuple(sorted(changed_paths))!r})."
                )

            run_git(str(clean_repo), "config", "user.name", BOT_NAME)
            run_git(str(clean_repo), "config", "user.email", BOT_EMAIL)
            run_git(str(clean_repo), "commit", "-m", _commit_message(proposal))

            _push_with_expected_head(
                str(clean_repo),
                head_repo_full_name=head_repo_full_name,
                head_branch=head_branch,
                head_sha=head_sha,
                git_env=git_env,
                pre_push_check=pre_push_check,
                create=create,
            )
        except subprocess.CalledProcessError as exc:
            # Keep the pipeline's "every outcome is a report" guarantee: a git
            # failure in the clean clone (unreachable SHA, rejected lease, etc.)
            # becomes a refusal, never an uncaught crash.
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to push: git failed: {detail}") from exc

        return git_output(str(clean_repo), "rev-parse", "HEAD").strip()


def commit_and_push_port(
    *,
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    unstable_fix_commit: str,
    git_env: dict[str, str],
    pre_push_check: PrePushCheck | None = None,
    create: bool = False,
) -> str:
    """Cherry-pick an existing upstream fix onto ``head_sha`` and push it.

    Unlike an authored fix, a PORT carries an already-merged upstream commit, so
    we preserve its original authorship and add the standard ``cherry picked
    from`` trailer rather than re-authoring it as the bot. The same push
    discipline applies: ``agent/`` branch, validated repo/SHA, descendant-only
    commit, and exact lease from a fresh clone. A conflicting or empty
    cherry-pick, or any git failure, becomes ``PushRefused``.
    """
    _validate_target(head_repo_full_name, head_branch, head_sha)
    if not SHA_RE.fullmatch(unstable_fix_commit):
        raise PushRefused(f"Refusing to port malformed commit {unstable_fix_commit!r}.")

    with tempfile.TemporaryDirectory(prefix="ci-fix-port-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(head_repo_full_name, clean_repo)
        try:
            # The fix commit lives on the default branch and may not be in the
            # blobless clone yet; fetch the exact object before picking.
            run_git(str(clean_repo), "fetch", "origin", unstable_fix_commit)
            run_git(str(clean_repo), "checkout", "--detach", head_sha)
            # Code, not the AI, owns "this is a real already-merged upstream
            # fix". Verify the commit is reachable from the default branch and
            # is not already on the base, so a model-chosen SHA cannot skip
            # local verification by pointing at an arbitrary or already-present
            # commit. A SHA that fails this is refused, not ported.
            _verify_portable_commit(str(clean_repo), unstable_fix_commit, head_sha)
            # The cherry-pick keeps the upstream commit's author and sign-off,
            # but it still needs a committer identity to create the commit (a
            # fresh clone has none). The bot is the committer, the human stays
            # the author, which is the normal backport shape.
            run_git(str(clean_repo), "config", "user.name", BOT_NAME)
            run_git(str(clean_repo), "config", "user.email", BOT_EMAIL)
            # -x records "cherry picked from commit <sha>".
            run_git(str(clean_repo), "cherry-pick", "-x", unstable_fix_commit)

            _push_with_expected_head(
                str(clean_repo),
                head_repo_full_name=head_repo_full_name,
                head_branch=head_branch,
                head_sha=head_sha,
                git_env=git_env,
                pre_push_check=pre_push_check,
                create=create,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to push: git failed: {detail}") from exc

        return git_output(str(clean_repo), "rev-parse", "HEAD").strip()


def _validate_target(head_repo_full_name: str, head_branch: str, head_sha: str) -> None:
    if not head_branch.startswith(ALLOWED_BRANCH_PREFIXES):
        # The prefix is the bot's namespace, not proof the branch is bot-owned:
        # the push is contained by the descendant check plus exact lease, the
        # front doors' same-repository requirement, and the App token being
        # scoped to the one target repo.
        raise PushRefused(
            f"Refusing to push to {head_branch!r}: ci_fix only pushes to branches "
            f"under {' or '.join(ALLOWED_BRANCH_PREFIXES)}."
        )
    if not REPO_RE.fullmatch(head_repo_full_name):
        raise PushRefused(f"Refusing to push to malformed repo {head_repo_full_name!r}.")
    if not SHA_RE.fullmatch(head_sha):
        raise PushRefused(f"Refusing to push from malformed head SHA {head_sha!r}.")
    if not _is_valid_branch_name(head_branch):
        raise PushRefused(f"Refusing to push to malformed branch {head_branch!r}.")


def _verify_push_authorized(pre_push_check: PrePushCheck | None) -> None:
    """Fail closed when a trusted caller can no longer authorize this push."""
    if pre_push_check is None:
        return
    try:
        reason = pre_push_check()
    except Exception as exc:  # noqa: BLE001 - authorization errors must fail closed
        raise PushRefused(
            "Refusing to push: the PR could not be revalidated immediately before push."
        ) from exc
    if reason:
        raise PushRefused(f"Refusing to push: {reason}")


def _push_with_expected_head(
    clean_repo: str,
    *,
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    git_env: dict[str, str],
    pre_push_check: PrePushCheck | None,
    create: bool = False,
) -> None:
    """Push only when HEAD descends from ``head_sha`` and the lease holds.

    Updating an existing branch requires the remote to still equal
    ``head_sha``; creating one (``create``) requires that it does not exist.
    """
    if not _is_ancestor(clean_repo, head_sha, "HEAD"):
        raise PushRefused(
            "Refusing to push: the generated commit does not descend from the validated base."
        )
    destination = f"refs/heads/{head_branch}"
    # An empty expected value means "the ref must not exist yet".
    expected = "" if create else head_sha
    run_git(clean_repo, "remote", "set-url", "origin", github_https_url(head_repo_full_name))
    # Keep the mutable PR-state check adjacent to the actual push. A concurrent
    # head update or deletion after this call is still rejected by the lease.
    _verify_push_authorized(pre_push_check)
    run_git(
        clean_repo,
        "push",
        f"--force-with-lease={destination}:{expected}",
        "origin",
        f"HEAD:{destination}",
        env=git_env,
    )


def _verify_portable_commit(clean_repo: str, fix_commit: str, head_sha: str) -> None:
    """Refuse unless ``fix_commit`` is a genuine upstream fix missing from head.

    A PORT skips local verification because the commit is already merged and
    tested on the default branch. That exception is only safe if *code*, not the
    AI, proves the SHA is exactly that. Two deterministic checks:

    - the commit is reachable from the default branch (it really is merged
      upstream, not an arbitrary or fabricated SHA); and
    - the commit is not already an ancestor of the PR head (porting it actually
      adds the missing fix rather than being a no-op).

    A SHA that fails either check raises ``PushRefused`` instead of being
    cherry-picked.
    """
    default_branch = resolve_default_branch(clean_repo)
    ref = f"origin/{default_branch}"
    try:
        git_output(clean_repo, "rev-parse", "--verify", ref)
    except subprocess.CalledProcessError:
        run_git(
            clean_repo, "fetch", "origin",
            f"refs/heads/{default_branch}:refs/remotes/origin/{default_branch}",
        )
    if not _is_ancestor(clean_repo, fix_commit, ref):
        raise PushRefused(
            f"Refusing to port {fix_commit[:12]}: it is not reachable from {ref}, "
            "so it is not a merged upstream fix."
        )
    if _is_ancestor(clean_repo, fix_commit, head_sha):
        raise PushRefused(
            f"Refusing to port {fix_commit[:12]}: it is already present on the PR head."
        )


def _is_ancestor(repo_dir: str, maybe_ancestor: str, descendant: str) -> bool:
    """True if ``maybe_ancestor`` is an ancestor of ``descendant`` (or equal)."""
    try:
        git_output(repo_dir, "merge-base", "--is-ancestor", maybe_ancestor, descendant)
        return True
    except subprocess.CalledProcessError:
        return False


def _clone_clean(head_repo_full_name: str, dest: Path) -> None:
    url = github_https_url(head_repo_full_name)
    try:
        run_git(None, "clone", "--filter=blob:none", url, str(dest))
    except subprocess.CalledProcessError as exc:
        raise PushRefused(f"Refusing to push: clone failed: {(exc.stderr or '')[:300]}") from exc


def _apply_patch(repo_dir: str, patch: str) -> None:
    try:
        run_git(repo_dir, "apply", "--index", "--whitespace=nowarn", "-", input=patch)
    except subprocess.CalledProcessError as exc:
        raise PushRefused(
            f"Refusing to push: approved patch did not apply cleanly: {(exc.stderr or '')[:300]}"
        ) from exc


def _staged_paths(repo_dir: str) -> tuple[str, ...]:
    out = git_output(repo_dir, "diff", "--cached", "--no-renames", "--name-only", "-z", "HEAD")
    return tuple(sorted(path for path in out.split("\0") if path))


def _is_valid_branch_name(branch: str) -> bool:
    try:
        run_git(None, "check-ref-format", "--branch", branch)
    except subprocess.CalledProcessError:
        return False
    return True


def _commit_message(proposal: FixProposal) -> str:
    """A focused commit message with a maintainer-readable subject.

    ``failing_check`` often comes from logs and can be a raw build command
    ("make SERVER_CFLAGS=...") rather than a useful commit subject. Prefer a
    source file named in the compiler diagnostic for build failures, and keep
    the detailed root cause in a wrapped body.
    """
    subject = commit_subject(proposal)
    body = _format_commit_body(proposal.root_cause)
    return f"{subject}\n\n{body}\n"


def _format_commit_body(body: str) -> str:
    return "\n\n".join(
        textwrap.fill(
            paragraph.strip(),
            width=72,
            break_long_words=False,
            break_on_hyphens=False,
        )
        for paragraph in body.strip().split("\n\n")
        if paragraph.strip()
    )


_SOURCE_LOCATION_RE = re.compile(
    r"`?([A-Za-z0-9_./-]+\.(?:c|h|cc|cpp|cxx|m|mm|py|tcl|sh|rs|go|java|js|ts)):\d+"
)


def commit_subject(proposal: FixProposal) -> str:
    source = _source_file_from_root_cause(proposal.root_cause)
    if source and _looks_like_build_failure(proposal):
        return fit_subject(f"Fix {source} build failure")

    check = _clean_failing_check(proposal.failing_check)
    if not check:
        return "Fix CI failure"
    return fit_subject(f"Fix {check}")


def _source_file_from_root_cause(root_cause: str) -> str:
    match = _SOURCE_LOCATION_RE.search(root_cause)
    if not match:
        return ""
    return Path(match.group(1)).name


def _looks_like_build_failure(proposal: FixProposal) -> bool:
    check = proposal.failing_check.strip().lower()
    if check.startswith(("make ", "cmake ", "ninja ", "clang ", "gcc ", "cc ")):
        return True
    text = f"{check} {proposal.root_cause}".lower()
    return any(
        marker in text
        for marker in (
            "compile",
            "compiler",
            "clang",
            "gcc",
            "-werror",
        )
    )


def _clean_failing_check(failing_check: str) -> str:
    check = " ".join(failing_check.strip().split())
    check = check.strip(" .")
    lowered = check.lower()
    if lowered.startswith(("make ", "cmake ", "ninja ", "clang ", "gcc ", "cc ")):
        return "build failure"
    return check


def fit_subject(subject: str) -> str:
    """Trim to Git's conventional 72-char subject length at a word boundary."""
    subject = " ".join(subject.split())
    if len(subject) <= 72:
        return subject
    clipped = subject[:72].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0]
    return clipped.rstrip(" .")
