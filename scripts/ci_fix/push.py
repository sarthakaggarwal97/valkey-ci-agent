"""Commit a validated fix and push it to the backport PR's own branch.

This is the only place ``ci_fix`` mutates a remote, so it carries the push
discipline:

- The fix is committed authored as the bot, without a DCO sign-off - a human
  must certify the change before it can be merged upstream. Local git
  commands run with a scrubbed environment so a repository git hook can never
  read a credential from the ambient environment.
- The push target must live in the allowed agent namespace
  (``agent/backport/...``) on the PR's own head repo. Anything else is refused.
- The push is fast-forward only: the refspec is ``HEAD:<branch>`` with no
  ``+``, so git itself rejects a non-fast-forward rather than overwriting.

The branch is never merged. The push re-triggers the PR's normal CI.
"""

from __future__ import annotations

import logging
import re
import subprocess
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path

from scripts.ci_fix.models import FixProposal
from scripts.ci_fix.port_discovery import resolve_default_branch
from scripts.ci_fix.rendering import normalize_generated_text
from scripts.common.git_auth import github_https_url
from scripts.common.git_clone import REPO_RE, SHA_RE
from scripts.common.proc import BOT_EMAIL, BOT_NAME, EmptyPatch, build_approved_patch, git_output, run_git

logger = logging.getLogger(__name__)

ALLOWED_BRANCH_PREFIX = "agent/backport/"
ISSUE_BRANCH_PREFIX = "agent/ci-fix/issue-"
_ISSUE_BRANCH_RE = re.compile(r"^agent/ci-fix/issue-(\d+)-run-(\d+)$")


class PushRefused(Exception):
    """Raised when a push target falls outside the allowed namespace."""


@dataclass(frozen=True)
class IssuePushResult:
    commit_sha: str
    base_sha: str


def commit_and_push_fix(
    repo_dir: str,
    *,
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    proposal: FixProposal,
    changed_paths: tuple[str, ...],
    git_env: dict[str, str],
) -> str:
    """Commit the working-tree fix and push it to the PR head branch.

    The verified checkout is treated as untrusted: test commands may have
    modified ``.git/config`` or hooks. We only extract a binary patch for the
    approved paths, then apply it in a fresh clone at ``head_sha``. The clean
    clone is the only checkout that receives credentials. Returns the new
    commit SHA. Raises ``PushRefused`` if any trust-boundary check fails.
    """
    if not head_branch.startswith(ALLOWED_BRANCH_PREFIX):
        # The prefix is a convention, not proof the branch is bot-owned: the
        # push is contained by the fast-forward-only refspec (can only append,
        # never rewrite), the gate's same-repo head requirement, and the App
        # token being scoped to the one target repo.
        raise PushRefused(
            f"Refusing to push to {head_branch!r}: ci_fix only pushes to branches "
            f"under {ALLOWED_BRANCH_PREFIX}."
        )
    if not REPO_RE.fullmatch(head_repo_full_name):
        raise PushRefused(f"Refusing to push to malformed repo {head_repo_full_name!r}.")
    if not SHA_RE.fullmatch(head_sha):
        raise PushRefused(f"Refusing to push from malformed head SHA {head_sha!r}.")
    if not changed_paths:
        raise PushRefused("Refusing to push: no approved changed paths to stage.")
    if not _is_valid_branch_name(head_branch):
        raise PushRefused(f"Refusing to push to malformed branch {head_branch!r}.")

    try:
        patch = build_approved_patch(repo_dir, changed_paths)
    except EmptyPatch as exc:
        raise PushRefused(f"Refusing to push: {exc}.") from exc

    with tempfile.TemporaryDirectory(prefix="ci-fix-push-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(head_repo_full_name, clean_repo)
        try:
            run_git(str(clean_repo), "checkout", head_sha)
            run_git(str(clean_repo), "checkout", "-B", head_branch)
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

            run_git(str(clean_repo), "remote", "set-url", "origin", github_https_url(head_repo_full_name))
            run_git(str(clean_repo), "push", "origin", f"HEAD:{head_branch}", env=git_env)
        except subprocess.CalledProcessError as exc:
            # Keep the pipeline's "every outcome is a comment" guarantee: a git
            # failure in the clean clone (unreachable SHA, non-fast-forward
            # push, etc.) becomes a refusal, never an uncaught crash.
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to push: git failed: {detail}") from exc

        return git_output(str(clean_repo), "rev-parse", "HEAD").strip()


def commit_and_push_issue_fix(
    repo_dir: str,
    *,
    repo_full_name: str,
    base_branch: str,
    branch_name: str,
    issue_number: int,
    run_id: int,
    run_sha: str,
    proposal: FixProposal,
    changed_paths: tuple[str, ...],
    git_env: dict[str, str],
) -> IssuePushResult:
    """Publish a verified issue fix on a new agent-owned branch."""
    if not changed_paths:
        raise PushRefused("Refusing to publish issue fix: no approved changed paths.")
    try:
        patch = build_approved_patch(repo_dir, changed_paths)
    except EmptyPatch as exc:
        raise PushRefused(f"Refusing to publish issue fix: {exc}.") from exc
    return commit_and_push_issue_patch(
        patch,
        repo_full_name=repo_full_name,
        base_branch=base_branch,
        branch_name=branch_name,
        issue_number=issue_number,
        run_id=run_id,
        run_sha=run_sha,
        proposal=proposal,
        expected_paths=changed_paths,
        git_env=git_env,
    )


def commit_and_push_issue_patch(
    patch: str,
    *,
    repo_full_name: str,
    base_branch: str,
    branch_name: str,
    issue_number: int,
    run_id: int,
    run_sha: str,
    proposal: FixProposal,
    expected_paths: tuple[str, ...] = (),
    git_env: dict[str, str],
) -> IssuePushResult:
    """Apply a reviewed patch to the latest default branch and push a new ref.

    This path also publishes handoff patches whose flaky baseline could not be
    reproduced locally. The caller opens a draft PR and labels that limitation;
    this function only guarantees exact-patch transfer and namespace-safe,
    non-force publication.
    """
    if not REPO_RE.fullmatch(repo_full_name):
        raise PushRefused(f"Refusing to push to malformed repo {repo_full_name!r}.")
    issue_branch = _ISSUE_BRANCH_RE.fullmatch(branch_name)
    if issue_branch is None:
        raise PushRefused(
            f"Refusing to push to {branch_name!r}: issue fixes require "
            f"{ISSUE_BRANCH_PREFIX}<issue>-run-<run>."
        )
    if int(issue_branch.group(1)) != issue_number:
        raise PushRefused(
            "Refusing to publish issue fix: branch issue number does not match "
            f"issue #{issue_number}."
        )
    if int(issue_branch.group(2)) != run_id:
        raise PushRefused(
            "Refusing to publish issue fix: branch run ID does not match "
            f"run {run_id}."
        )
    if not _is_valid_branch_name(branch_name):
        raise PushRefused(f"Refusing to push to malformed branch {branch_name!r}.")
    if not _is_valid_branch_name(base_branch):
        raise PushRefused(f"Refusing to use malformed base branch {base_branch!r}.")
    if issue_number < 1:
        raise PushRefused("Refusing to publish an issue fix without a valid issue number.")
    if run_id < 1:
        raise PushRefused("Refusing to publish an issue fix without a valid run ID.")
    if not SHA_RE.fullmatch(run_sha):
        raise PushRefused(f"Refusing to publish issue fix from malformed run SHA {run_sha!r}.")
    if not patch.strip():
        raise PushRefused("Refusing to publish an empty issue-fix patch.")

    with tempfile.TemporaryDirectory(prefix="ci-fix-issue-push-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(repo_full_name, clean_repo)
        try:
            base_ref = f"refs/remotes/origin/{base_branch}"
            branch_ref = f"refs/remotes/origin/{branch_name}"
            run_git(
                str(clean_repo),
                "fetch",
                "origin",
                f"refs/heads/{base_branch}:{base_ref}",
            )
            run_git(str(clean_repo), "checkout", "--detach", base_ref)
            publication_base_sha = git_output(
                str(clean_repo), "rev-parse", "HEAD"
            ).strip()
            if not _is_ancestor(str(clean_repo), run_sha, base_ref):
                raise PushRefused(
                    "Refusing to publish issue fix: the failing run commit is "
                    f"no longer an ancestor of {base_branch!r}."
                )
            existing = _existing_ref(str(clean_repo), branch_ref)
            if existing:
                return _recover_issue_branch(
                    str(clean_repo),
                    branch_ref=branch_ref,
                    current_base_ref=base_ref,
                    patch=patch,
                    issue_number=issue_number,
                    run_sha=run_sha,
                    proposal=proposal,
                )
            run_git(str(clean_repo), "checkout", "-b", branch_name)
            _apply_patch(str(clean_repo), patch)

            staged = _staged_paths(str(clean_repo))
            if not staged:
                raise PushRefused("Refusing to publish issue fix: patch staged no files.")
            if expected_paths and staged != tuple(sorted(expected_paths)):
                raise PushRefused(
                    "Refusing to publish issue fix: reviewed patch staged unexpected "
                    f"paths {staged!r} (expected {tuple(sorted(expected_paths))!r})."
                )

            run_git(str(clean_repo), "config", "user.name", BOT_NAME)
            run_git(str(clean_repo), "config", "user.email", BOT_EMAIL)
            message = f"{_commit_message(proposal).rstrip()}\n\nRefs #{issue_number}\n"
            run_git(str(clean_repo), "commit", "-m", message)
            run_git(
                str(clean_repo),
                "remote",
                "set-url",
                "origin",
                github_https_url(repo_full_name),
            )
            run_git(
                str(clean_repo), "push", "origin", f"HEAD:refs/heads/{branch_name}",
                env=git_env,
            )
        except PushRefused:
            raise
        except subprocess.CalledProcessError as exc:
            recovered = _recover_after_push_race(
                str(clean_repo),
                branch_name=branch_name,
                current_base_ref=base_ref,
                patch=patch,
                issue_number=issue_number,
                run_sha=run_sha,
                proposal=proposal,
            )
            if recovered is not None:
                return recovered
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to publish issue fix: git failed: {detail}") from exc

        return IssuePushResult(
            commit_sha=git_output(str(clean_repo), "rev-parse", "HEAD").strip(),
            base_sha=publication_base_sha,
        )


def _recover_after_push_race(
    repo_dir: str,
    *,
    branch_name: str,
    current_base_ref: str,
    patch: str,
    issue_number: int,
    run_sha: str,
    proposal: FixProposal,
) -> IssuePushResult | None:
    """Recover when another invocation published the exact branch first."""
    branch_ref = f"refs/remotes/origin/{branch_name}"
    try:
        run_git(
            repo_dir,
            "fetch",
            "origin",
            f"refs/heads/{branch_name}:{branch_ref}",
        )
    except subprocess.CalledProcessError:
        return None
    return _recover_issue_branch(
        repo_dir,
        branch_ref=branch_ref,
        current_base_ref=current_base_ref,
        patch=patch,
        issue_number=issue_number,
        run_sha=run_sha,
        proposal=proposal,
    )


def _recover_issue_branch(
    repo_dir: str,
    *,
    branch_ref: str,
    current_base_ref: str,
    patch: str,
    issue_number: int,
    run_sha: str,
    proposal: FixProposal,
) -> IssuePushResult:
    """Accept an orphan branch only when it is exactly our reviewed commit."""
    commit_sha = git_output(repo_dir, "rev-parse", "--verify", branch_ref).strip()
    commit_line = git_output(
        repo_dir, "rev-list", "--parents", "-n", "1", commit_sha
    ).strip().split()
    if len(commit_line) != 2:
        raise PushRefused(
            "Refusing to recover issue-fix branch: expected exactly one commit "
            "on top of its publication base."
        )
    base_sha = commit_line[1]
    if not _is_ancestor(repo_dir, run_sha, base_sha):
        raise PushRefused(
            "Refusing to recover issue-fix branch: its base does not contain "
            "the failing run commit."
        )
    if not _is_ancestor(repo_dir, base_sha, current_base_ref):
        raise PushRefused(
            "Refusing to recover issue-fix branch: its base is not in the "
            "current default-branch history."
        )

    expected_message = (
        f"{_commit_message(proposal).rstrip()}\n\nRefs #{issue_number}"
    )
    actual_message = git_output(repo_dir, "show", "-s", "--format=%B", commit_sha).strip()
    author_email = git_output(repo_dir, "show", "-s", "--format=%ae", commit_sha).strip()
    committer_email = git_output(repo_dir, "show", "-s", "--format=%ce", commit_sha).strip()
    if (
        actual_message != expected_message
        or author_email != BOT_EMAIL
        or committer_email != BOT_EMAIL
    ):
        raise PushRefused(
            "Refusing to recover issue-fix branch: commit identity or provenance "
            "does not match this invocation."
        )

    run_git(repo_dir, "checkout", "--detach", base_sha)
    _apply_patch(repo_dir, patch)
    expected_tree = git_output(repo_dir, "write-tree").strip()
    actual_tree = git_output(repo_dir, "show", "-s", "--format=%T", commit_sha).strip()
    if expected_tree != actual_tree:
        raise PushRefused(
            "Refusing to recover issue-fix branch: existing commit does not "
            "contain the exact reviewed patch."
        )
    return IssuePushResult(commit_sha=commit_sha, base_sha=base_sha)


def _existing_ref(repo_dir: str, ref: str) -> str:
    try:
        return git_output(repo_dir, "rev-parse", "--verify", ref).strip()
    except subprocess.CalledProcessError:
        return ""


def commit_and_push_port(
    repo_dir: str,
    *,
    head_repo_full_name: str,
    head_branch: str,
    head_sha: str,
    unstable_fix_commit: str,
    git_env: dict[str, str],
) -> str:
    """Cherry-pick an existing upstream fix onto the PR branch and push it.

    Unlike an authored fix, a PORT carries an already-merged upstream commit, so
    we preserve its original authorship and add the standard ``cherry picked
    from`` trailer rather than re-authoring it as the bot. The same push
    discipline applies: namespaced branch, validated repo/SHA, fast-forward-only
    push from a fresh clone. A conflicting or empty cherry-pick, or any git
    failure, becomes ``PushRefused`` so the outcome is always a comment.
    """
    if not head_branch.startswith(ALLOWED_BRANCH_PREFIX):
        raise PushRefused(
            f"Refusing to push to {head_branch!r}: ci_fix only pushes to branches "
            f"under {ALLOWED_BRANCH_PREFIX}."
        )
    if not REPO_RE.fullmatch(head_repo_full_name):
        raise PushRefused(f"Refusing to push to malformed repo {head_repo_full_name!r}.")
    if not SHA_RE.fullmatch(head_sha):
        raise PushRefused(f"Refusing to push from malformed head SHA {head_sha!r}.")
    if not SHA_RE.fullmatch(unstable_fix_commit):
        raise PushRefused(f"Refusing to port malformed commit {unstable_fix_commit!r}.")
    if not _is_valid_branch_name(head_branch):
        raise PushRefused(f"Refusing to push to malformed branch {head_branch!r}.")

    with tempfile.TemporaryDirectory(prefix="ci-fix-port-") as tmpdir:
        clean_repo = Path(tmpdir) / "repo"
        _clone_clean(head_repo_full_name, clean_repo)
        try:
            # The fix commit lives on the default branch and may not be in the
            # blobless clone yet; fetch the exact object before picking.
            run_git(str(clean_repo), "fetch", "origin", unstable_fix_commit)
            run_git(str(clean_repo), "checkout", head_sha)
            run_git(str(clean_repo), "checkout", "-B", head_branch)
            # Code, not the AI, owns "this is a real already-merged upstream
            # fix". Verify the commit is reachable from the default branch and
            # is not already on the PR head, so a model-chosen SHA cannot skip
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

            run_git(str(clean_repo), "remote", "set-url", "origin", github_https_url(head_repo_full_name))
            run_git(str(clean_repo), "push", "origin", f"HEAD:{head_branch}", env=git_env)
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or str(exc)).strip()[:300]
            raise PushRefused(f"Refusing to push: git failed: {detail}") from exc

        return git_output(str(clean_repo), "rev-parse", "HEAD").strip()


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
    out = git_output(repo_dir, "diff", "--cached", "--name-only", "-z", "HEAD")
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
    subject = _commit_subject(proposal)
    body = _format_commit_body(proposal.root_cause)
    return f"{subject}\n\n{body}\n"


def _format_commit_body(body: str) -> str:
    paragraphs: list[str] = []
    remaining = 2_000
    for raw in re.split(r"\n\s*\n", body):
        normalized = normalize_generated_text(raw, limit=remaining)
        if not normalized:
            continue
        paragraphs.append(normalized)
        remaining -= len(normalized)
        if remaining <= 0:
            break
    if not paragraphs:
        paragraphs = ["Unspecified CI failure."]

    rendered = []
    for index, paragraph in enumerate(paragraphs):
        label = "Root cause: " if index == 0 else "Detail: "
        rendered.append(
            textwrap.fill(
                f"{label}{paragraph}",
                width=72,
                break_long_words=False,
                break_on_hyphens=False,
            )
        )
    return "\n\n".join(rendered)


_SOURCE_LOCATION_RE = re.compile(
    r"`?([A-Za-z0-9_./-]+\.(?:c|h|cc|cpp|cxx|m|mm|py|tcl|sh|rs|go|java|js|ts)):\d+"
)


def _commit_subject(proposal: FixProposal) -> str:
    source = _source_file_from_root_cause(proposal.root_cause)
    if source and _looks_like_build_failure(proposal):
        return _fit_subject(f"Fix {source} build failure")

    check = _clean_failing_check(proposal.failing_check)
    if not check:
        return "Fix CI failure"
    return _fit_subject(f"Fix {check}")


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


def _fit_subject(subject: str) -> str:
    """Trim to Git's conventional 72-char subject length at a word boundary."""
    subject = " ".join(subject.split())
    if len(subject) <= 72:
        return subject
    clipped = subject[:72].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0]
    return clipped.rstrip(" .")
