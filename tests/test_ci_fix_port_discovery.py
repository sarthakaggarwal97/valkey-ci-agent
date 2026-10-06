"""Tests for default-branch port candidate discovery."""

from __future__ import annotations

import subprocess

import pytest

from scripts.ci_fix.port_discovery import (
    PortCandidate,
    commits_in_range,
    discover_port_candidates,
    format_port_candidates,
    resolve_commit,
)


def _git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _commit(repo, message):
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", message)
    return _git(repo, "rev-parse", "HEAD")


def test_discovers_default_branch_fix_by_logged_path(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "src").mkdir()
    (repo / "src" / "logreqres.c").write_text("base\n")
    _commit(repo, "release base")

    _git(repo, "checkout", "-qb", "unstable")
    (repo / "src" / "logreqres.c").write_text("skip internal clients\n")
    upstream = _commit(repo, "Skips the internal clients from logreqres checks (#3154)")
    _git(repo, "update-ref", "refs/remotes/origin/unstable", "unstable")
    _git(repo, "checkout", "-q", "release")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "validator.txt").write_text(
        "validator failed while checking src/logreqres.c for missing replies\n"
    )

    candidates = discover_port_candidates(str(repo), str(logs))

    assert candidates
    assert candidates[0].sha == upstream
    assert candidates[0].paths == ("src/logreqres.c",)
    rendered = format_port_candidates(candidates)
    assert upstream[:12] in rendered
    assert "logreqres" in rendered


def test_discovers_default_branch_fix_by_message_term_when_no_path(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "README").write_text("base\n")
    _commit(repo, "release base")

    _git(repo, "checkout", "-qb", "unstable")
    (repo / "README").write_text("fixed\n")
    upstream = _commit(repo, "Fix reply schema validator for internal clients")
    _git(repo, "update-ref", "refs/remotes/origin/unstable", "unstable")
    _git(repo, "checkout", "-q", "release")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "validator.txt").write_text("reply schema validator failed\n")

    candidates = discover_port_candidates(str(repo), str(logs))

    assert [c.sha for c in candidates] == [upstream]


def test_discovery_respects_max_candidates(tmp_path):
    """Many matching commits are capped, so the bot does not expand every commit
    (each PortCandidate runs a diff-tree) on a large history."""
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "src").mkdir()
    (repo / "src" / "logreqres.c").write_text("base\n")
    _commit(repo, "release base")

    _git(repo, "checkout", "-qb", "unstable")
    for i in range(10):
        (repo / "src" / "logreqres.c").write_text(f"change {i}\n")
        _commit(repo, f"touch logreqres {i}")
    _git(repo, "update-ref", "refs/remotes/origin/unstable", "unstable")
    _git(repo, "checkout", "-q", "release")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "validator.txt").write_text("failed in src/logreqres.c\n")

    candidates = discover_port_candidates(str(repo), str(logs), max_candidates=3)

    assert len(candidates) == 3


def test_discovers_fix_when_default_branch_is_main(tmp_path):
    """The default branch is resolved from origin/HEAD, so discovery works for a
    repo whose default is 'main' (e.g. valkey-search), not only 'unstable'."""
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "src").mkdir()
    (repo / "src" / "index.cc").write_text("base\n")
    _commit(repo, "release base")

    _git(repo, "checkout", "-qb", "main")
    (repo / "src" / "index.cc").write_text("fixed\n")
    upstream = _commit(repo, "Fix the index build")
    _git(repo, "update-ref", "refs/remotes/origin/main", "main")
    # Record main as the remote default, the way a real clone would.
    _git(repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    _git(repo, "checkout", "-q", "release")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "build.txt").write_text("compile failed in src/index.cc\n")

    candidates = discover_port_candidates(str(repo), str(logs))

    assert [c.sha for c in candidates] == [upstream]


# --- culprit candidates -----------------------------------------------------------



def _commit_file(repo, name, message):
    (repo / name).parent.mkdir(parents=True, exist_ok=True)
    (repo / name).write_text(message)
    subprocess.run(["git", "-C", str(repo), "add", name], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", message], check=True)
    return subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                          check=True, capture_output=True, text=True).stdout.strip()


def _history(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    base = _commit_file(repo, "a.txt", "base")
    first = _commit_file(repo, "src/tls.c", "Change TLS handshake (#100)")
    second = _commit_file(repo, "tests/unit/x.tcl", "Add a test (#101)")
    return repo, base, first, second


def test_commits_in_range_lists_newest_first_with_paths(tmp_path):
    repo, base, first, second = _history(tmp_path)
    commits = commits_in_range(str(repo), f"{base}..HEAD")
    assert [c.sha for c in commits] == [second, first]
    assert commits[1].subject == "Change TLS handshake (#100)"
    assert commits[1].paths == ("src/tls.c",)


def test_commits_in_range_honors_the_limit(tmp_path):
    repo, base, _first, second = _history(tmp_path)
    assert [c.sha for c in commits_in_range(str(repo), f"{base}..HEAD", limit=1)] == [second]


@pytest.mark.parametrize("rev_range", ["", "HEAD", "--all..HEAD", "a b..HEAD", "nope..HEAD"])
def test_commits_in_range_rejects_malformed_or_unknown_ranges(tmp_path, rev_range):
    repo, *_ = _history(tmp_path)
    assert commits_in_range(str(repo), rev_range) == ()


def test_resolve_commit_matches_a_unique_prefix_only():
    commits = (PortCandidate(sha="abcdef1" + "0" * 33, subject="a"),
               PortCandidate(sha="abcdef2" + "0" * 33, subject="b"))
    assert resolve_commit("abcdef1", commits) is commits[0]
    assert resolve_commit("ABCDEF2" + "0" * 33, commits) is commits[1]
    assert resolve_commit("abcdef", commits) is None      # too short to trust
    assert resolve_commit("abcdef0", commits) is None     # not listed
