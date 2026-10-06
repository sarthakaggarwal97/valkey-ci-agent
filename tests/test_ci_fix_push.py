"""Push-path tests, focused on the PORT path preserving upstream authorship.

A port carries an already-merged upstream commit, so the pushed commit must keep
the original author and sign-off rather than being re-authored as the bot.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.ci_fix import push as push_mod
from scripts.ci_fix.models import FixPath, FixProposal
from scripts.ci_fix.push import PushRefused, commit_and_push_port


def _git(repo, *args, **kw):
    return subprocess.run(["git", "-C", str(repo), *args], check=True,
                          capture_output=True, text=True, **kw).stdout


def _make_origin(tmp_path):
    """A repo whose default branch (unstable) carries a fix authored by a
    distinct human, and a release branch that diverged before the fix and is
    missing it. Returns (work, bare, fix_sha, head_sha).

    ``unstable`` is set as the default branch (origin/HEAD) so the port path's
    ancestry check resolves it the way a real clone would.
    """
    work = tmp_path / "origin"
    work.mkdir()
    _git(work, "init", "-q", "-b", "unstable")
    _git(work, "config", "user.email", "base@t")
    _git(work, "config", "user.name", "Base")
    (work / "f.txt").write_text("base\n")
    _git(work, "add", "f.txt")
    _git(work, "commit", "-qm", "base")
    base_sha = _git(work, "rev-parse", "HEAD").strip()

    # The release branch diverges at base, before the fix lands.
    _git(work, "branch", "agent/backport/sweep/9.0", base_sha)

    # The upstream fix on unstable (the default branch), authored by a human.
    (work / "g.txt").write_text("the fix\n")
    _git(work, "add", "g.txt")
    _git(work, "-c", "user.email=author@example.com", "-c", "user.name=Real Author",
         "commit", "-qm", "the upstream fix\n\nSigned-off-by: Real Author <author@example.com>")
    fix_sha = _git(work, "rev-parse", "HEAD").strip()

    head_sha = _git(work, "rev-parse", "agent/backport/sweep/9.0").strip()
    _git(work, "checkout", "-q", "agent/backport/sweep/9.0")

    bare = tmp_path / "remote.git"
    _git(work, "clone", "-q", "--bare", str(work), str(bare))
    # Point the bare remote's HEAD at unstable so a clone resolves it as default.
    _git(bare, "symbolic-ref", "HEAD", "refs/heads/unstable")
    return work, bare, fix_sha, head_sha


def test_port_push_preserves_original_authorship(tmp_path, monkeypatch):
    work, bare, fix_sha, head_sha = _make_origin(tmp_path)

    # Clone from the bare remote (it has both branches, the fix commit, and
    # HEAD->unstable), and push back to it, so no network or real GitHub is used.
    def fake_clone(_full_name, dest: Path):
        _git(dest.parent, "clone", "-q", str(bare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", fake_clone)
    monkeypatch.setattr(push_mod, "github_https_url", lambda _n: str(bare))

    pushed_sha = commit_and_push_port(
        head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/9.0",
        head_sha=head_sha,
        unstable_fix_commit=fix_sha,
        git_env={},
    )

    # The pushed commit on the bare remote keeps the original author, not the bot.
    author = _git(bare, "log", "-1", "--format=%an <%ae>", pushed_sha).strip()
    assert author == "Real Author <author@example.com>"
    body = _git(bare, "log", "-1", "--format=%B", pushed_sha)
    assert "Signed-off-by: Real Author <author@example.com>" in body
    assert "cherry picked from commit" in body  # the -x trailer


def test_port_push_refuses_commit_not_on_default_branch(tmp_path, monkeypatch):
    """A SHA the model points at that is not reachable from the default branch
    is refused, not ported: code owns 'this is a merged upstream fix'."""
    work, bare, _fix_sha, head_sha = _make_origin(tmp_path)

    # A commit that exists but lives only on a side branch, never merged to
    # the default branch.
    _git(work, "checkout", "-qb", "rogue", head_sha)
    (work / "rogue.txt").write_text("not upstream\n")
    _git(work, "add", "rogue.txt")
    _git(work, "commit", "-qm", "rogue commit")
    rogue_sha = _git(work, "rev-parse", "HEAD").strip()
    _git(work, "checkout", "-q", "agent/backport/sweep/9.0")
    # Refresh the bare so the clone can fetch the rogue object by SHA.
    rebare = tmp_path / "remote2.git"
    _git(work, "clone", "-q", "--bare", str(work), str(rebare))
    _git(rebare, "symbolic-ref", "HEAD", "refs/heads/unstable")

    def fake_clone(_full_name, dest: Path):
        _git(dest.parent, "clone", "-q", str(rebare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", fake_clone)
    monkeypatch.setattr(push_mod, "github_https_url", lambda _n: str(rebare))

    with pytest.raises(PushRefused, match="not reachable from"):
        commit_and_push_port(
            head_repo_full_name="valkey-io/valkey",
            head_branch="agent/backport/sweep/9.0",
            head_sha=head_sha,
            unstable_fix_commit=rogue_sha,
            git_env={},
        )


def test_port_push_refuses_commit_already_on_head(tmp_path, monkeypatch):
    """Porting a commit already present on the PR head is a no-op and refused."""
    work, bare, fix_sha, _head_sha = _make_origin(tmp_path)

    # Make the head branch already contain the fix (fast-forward it to unstable).
    head_with_fix = fix_sha

    def fake_clone(_full_name, dest: Path):
        _git(dest.parent, "clone", "-q", str(bare), str(dest))

    monkeypatch.setattr(push_mod, "_clone_clean", fake_clone)
    monkeypatch.setattr(push_mod, "github_https_url", lambda _n: str(bare))

    with pytest.raises(PushRefused, match="already present on the PR head"):
        commit_and_push_port(
            head_repo_full_name="valkey-io/valkey",
            head_branch="agent/backport/sweep/9.0",
            head_sha=head_with_fix,
            unstable_fix_commit=fix_sha,
            git_env={},
        )


@pytest.mark.parametrize("branch", ["main", "unstable", "9.0", "contributor/fix", "agentx/fix",
                                    "agent/release-cut/9.0.4-ga", "agent/other/x"])
def test_port_push_refuses_non_namespaced_branch(branch):
    """Only agent/... branches are ever written: never unstable or a release line."""
    with pytest.raises(PushRefused, match="only pushes to branches under agent/backport/ or agent/ci-fix/"):
        commit_and_push_port(
            head_repo_full_name="valkey-io/valkey",
            head_branch=branch, head_sha="a" * 40, unstable_fix_commit="b" * 40, git_env={},
        )


def test_port_push_refuses_malformed_commit():
    with pytest.raises(PushRefused):
        commit_and_push_port(
            head_repo_full_name="valkey-io/valkey",
            head_branch="agent/backport/sweep/9.0", head_sha="a" * 40,
            unstable_fix_commit="not-a-sha", git_env={},
        )


def test_pre_push_check_refuses_expired_authorization():
    with pytest.raises(PushRefused, match="authorization expired"):
        push_mod._verify_push_authorized(lambda: "automatic follow-up authorization expired")


def test_pre_push_check_fails_closed_on_revalidation_error():
    def explode() -> str:
        raise RuntimeError("GitHub unavailable")

    with pytest.raises(PushRefused, match="could not be revalidated"):
        push_mod._verify_push_authorized(explode)


def test_author_fix_commit_message_uses_source_file_for_build_failure():
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="make SERVER_CFLAGS='-Werror' (compile of timeout.o)",
        root_cause=(
            "timeout.c:169 `if (ftval > LLONG_MAX)` compares a long double "
            "against the LLONG_MAX const int; newer Xcode clang errors under "
            "-Werror."
        ),
        reasoning="minimal cast",
        confidence=0.9,
        build_command="make SERVER_CFLAGS='-Werror'",
        verify_command="make SERVER_CFLAGS='-Werror'",
    )

    message = push_mod._commit_message(proposal)
    subject = message.splitlines()[0]
    assert subject == "Fix timeout.c build failure"
    assert "make SERVER_CFLAGS" not in subject
    assert all(len(line) <= 72 for line in message.splitlines())


def test_author_fix_commit_message_keeps_natural_test_name():
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11",
        reasoning="scaffolding fix",
        confidence=0.9,
        build_command="make",
        verify_command="./runtest --single x",
    )

    assert push_mod._commit_message(proposal).splitlines()[0] == (
        "Fix corrupt payload: zset listpack with NAN score"
    )


def test_author_fix_commit_message_does_not_treat_make_progress_as_build():
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="replication timeout",
        root_cause="timeout.c:169 the test does not make progress before timing out",
        reasoning="timeout fix",
        confidence=0.9,
        build_command="make",
        verify_command="./runtest --single replication",
    )

    assert push_mod._commit_message(proposal).splitlines()[0] == "Fix replication timeout"


def test_author_fix_commit_message_does_not_treat_test_error_as_build():
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="protocol test",
        root_cause="tests/unit/protocol.tcl:42 assertion error: invalid CRLF response",
        reasoning="protocol fix",
        confidence=0.9,
        build_command="make",
        verify_command="./runtest --single unit/protocol",
    )

    assert push_mod._commit_message(proposal).splitlines()[0] == "Fix protocol test"


def test_author_fix_commit_message_preserves_body_paragraphs():
    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="protocol test",
        root_cause=(
            "First sentence describes the failure and has enough words to wrap "
            "onto another line cleanly.\n\n"
            "Second sentence explains why the fix is safe."
        ),
        reasoning="protocol fix",
        confidence=0.9,
        build_command="make",
        verify_command="./runtest --single unit/protocol",
    )

    message = push_mod._commit_message(proposal)
    assert "\n\nSecond sentence explains why the fix is safe.\n" in message
    assert all(len(line) <= 72 for line in message.splitlines())


# --- Authored-fix push: the approved patch text is applied in a clean clone ---


def _fix_proposal() -> FixProposal:
    return FixProposal(
        path=FixPath.AUTHOR, failing_check="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11", reasoning="scaffolding fix",
        confidence=0.9,
    )


def _remote_with_branch(tmp_path, branch: str = "agent/backport/sweep/8.0"):
    """A bare remote with ``branch`` and ``unstable`` at one seed commit.

    Returns (remote, head_sha, patch) where ``patch`` adds test.tcl.
    """
    work = tmp_path / "work"
    work.mkdir()
    _git(work, "init", "-q", "-b", "unstable")
    _git(work, "config", "user.email", "t@t")
    _git(work, "config", "user.name", "t")
    (work / "seed.txt").write_text("seed\n")
    _git(work, "add", "-A")
    _git(work, "commit", "-qm", "seed")
    head_sha = _git(work, "rev-parse", "HEAD").strip()
    _git(work, "branch", branch)
    (work / "test.tcl").write_text("fixed payload\n")
    _git(work, "add", "--intent-to-add", "test.tcl")
    patch = _git(work, "diff", "--binary", "HEAD")
    remote = tmp_path / "remote.git"
    _git(tmp_path, "clone", "-q", "--bare", str(work), str(remote))
    return remote, head_sha, patch


def _fix_push(remote, monkeypatch, **overrides):
    monkeypatch.setattr(push_mod, "github_https_url", lambda _n: str(remote))
    kwargs = dict(
        changed_paths=("test.tcl",), head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/8.0", proposal=_fix_proposal(), git_env={},
    )
    kwargs.update(overrides)
    return push_mod.commit_and_push_fix(**kwargs)


def test_fix_push_commits_the_patch_as_the_bot_without_signoff(tmp_path, monkeypatch):
    remote, head_sha, patch = _remote_with_branch(tmp_path)
    sha = _fix_push(remote, monkeypatch, patch=patch, head_sha=head_sha)

    assert _git(remote, "rev-parse", "agent/backport/sweep/8.0").strip() == sha
    assert _git(remote, "rev-parse", f"{sha}^").strip() == head_sha
    assert _git(remote, "show", "--name-only", "--format=", sha).split() == ["test.tcl"]
    message = _git(remote, "log", "-1", "--format=%B", sha)
    assert "Signed-off-by:" not in message
    assert "NAN score" in message
    assert "valkeyrie-bot" in _git(remote, "log", "-1", "--format=%an", sha)


def test_fix_push_refuses_paths_outside_the_approved_set(tmp_path, monkeypatch):
    remote, head_sha, patch = _remote_with_branch(tmp_path)
    with pytest.raises(PushRefused, match="unexpected paths"):
        _fix_push(remote, monkeypatch, patch=patch, head_sha=head_sha, changed_paths=("other.tcl",))
    assert _git(remote, "rev-parse", "agent/backport/sweep/8.0").strip() == head_sha


def test_fix_push_refuses_empty_patch(monkeypatch):
    with pytest.raises(PushRefused, match="empty"):
        push_mod.commit_and_push_fix(
            patch="  \n", changed_paths=("t",), head_repo_full_name="valkey-io/valkey",
            head_branch="agent/backport/sweep/8.0", head_sha="a" * 40,
            proposal=_fix_proposal(), git_env={},
        )


@pytest.mark.parametrize("branch", ["unstable", "9.0", "feature/x", "agent/release-cut/9.0.4-ga"])
def test_fix_push_refuses_non_agent_branches(branch):
    with pytest.raises(PushRefused, match="only pushes to branches under agent/backport/ or agent/ci-fix/"):
        push_mod.commit_and_push_fix(
            patch="diff", changed_paths=("t",), head_repo_full_name="valkey-io/valkey",
            head_branch=branch, head_sha="a" * 40, proposal=_fix_proposal(), git_env={},
        )


def test_fix_push_exact_lease_refuses_deleted_branch(tmp_path, monkeypatch):
    """A branch deleted while the fix was prepared must not be recreated."""
    remote, head_sha, patch = _remote_with_branch(tmp_path)

    def delete_branch_at_last_check() -> str:
        _git(remote, "update-ref", "-d", "refs/heads/agent/backport/sweep/8.0")
        return ""

    with pytest.raises(PushRefused, match="stale info|failed to push"):
        _fix_push(remote, monkeypatch, patch=patch, head_sha=head_sha,
                  pre_push_check=delete_branch_at_last_check)
    assert "agent/backport/sweep/8.0" not in _git(remote, "branch", "--list")


def test_fix_push_create_opens_a_new_branch_on_the_base(tmp_path, monkeypatch):
    """The issue flow creates agent/ci-fix/...; the base branch is untouched."""
    remote, head_sha, patch = _remote_with_branch(tmp_path)
    sha = _fix_push(remote, monkeypatch, patch=patch, head_sha=head_sha,
                    head_branch="agent/ci-fix/issue-7-99", create=True)

    assert _git(remote, "rev-parse", "agent/ci-fix/issue-7-99").strip() == sha
    assert _git(remote, "rev-parse", "unstable").strip() == head_sha


def test_fix_push_create_refuses_an_existing_branch(tmp_path, monkeypatch):
    """Create mode must never overwrite a branch that already exists."""
    remote, head_sha, patch = _remote_with_branch(tmp_path)
    with pytest.raises(PushRefused, match="stale info|failed to push|rejected"):
        _fix_push(remote, monkeypatch, patch=patch, head_sha=head_sha, create=True)
    assert _git(remote, "rev-parse", "agent/backport/sweep/8.0").strip() == head_sha
