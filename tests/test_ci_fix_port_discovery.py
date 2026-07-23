"""Tests for default-first existing-fix discovery."""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.ci_fix.port_discovery import (
    _MAX_FILE_LOG_BYTES,
    PortCandidate,
    _read_logs,
    discover_port_candidates,
    format_port_candidates,
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


def test_discovers_older_release_branch_deflake_with_provenance(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release-9", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "tests" / "unit").mkdir(parents=True)
    test_file = repo / "tests" / "unit" / "cluster.tcl"
    test_file.write_text("base\n")
    base = _commit(repo, "release base")

    # The default branch has no fix for this failure.
    _git(repo, "update-ref", "refs/remotes/origin/unstable", base)

    # An older supported release line fixed the same intermittent test.
    _git(repo, "checkout", "-qb", "7.2")
    test_file.write_text("wait for failover deterministically\n")
    historical = _commit(repo, "Deflake cluster failover timeout by waiting for state")
    _git(repo, "update-ref", "refs/remotes/origin/7.2", "7.2")
    _git(repo, "checkout", "-q", "release-9")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "cluster.txt").write_text(
        "[err] intermittent timeout in tests/unit/cluster.tcl during failover\n"
    )

    candidates = discover_port_candidates(
        str(repo), str(logs), history_branches=("7.2",),
    )

    assert candidates[0].sha == historical
    assert candidates[0].source_ref == "origin/7.2"
    assert candidates[0].source_branch == "7.2"
    rendered = format_port_candidates(candidates)
    assert "[7.2" in rendered
    assert "Deflake cluster failover" in rendered


def test_default_branch_candidates_suppress_release_fallback(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release-9", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "tests").mkdir()
    test_file = repo / "tests" / "cluster.tcl"
    test_file.write_text("base\n")
    _commit(repo, "release base")

    _git(repo, "checkout", "-qb", "unstable")
    test_file.write_text("upstream fix\n")
    upstream = _commit(repo, "Fix cluster test on unstable")
    _git(repo, "update-ref", "refs/remotes/origin/unstable", "unstable")

    _git(repo, "checkout", "-q", "release-9")
    _git(repo, "checkout", "-qb", "7.2")
    test_file.write_text("release-only fix\n")
    _commit(repo, "Deflake cluster test on an old release")
    _git(repo, "update-ref", "refs/remotes/origin/7.2", "7.2")
    _git(repo, "checkout", "-q", "release-9")

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "cluster.txt").write_text("[err] timeout in tests/cluster.tcl\n")

    candidates = discover_port_candidates(
        str(repo), str(logs), history_branches=("7.2",),
    )

    assert [candidate.sha for candidate in candidates] == [upstream]
    assert candidates[0].source_branch == "unstable"


def test_release_branches_are_not_inspected_after_default_match(
    tmp_path, monkeypatch,
):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "failure.txt").write_text("test failed\n")
    calls = []
    primary = PortCandidate(
        sha="a" * 40,
        subject="Fix on unstable",
        source_ref="origin/unstable",
        source_branch="unstable",
    )

    def candidates(_repo, branch, **_kwargs):
        calls.append(branch)
        return (primary,) if branch == "unstable" else ()

    monkeypatch.setattr(
        "scripts.ci_fix.port_discovery._candidates_for_branch",
        candidates,
    )

    discovered = discover_port_candidates(
        str(tmp_path),
        str(logs),
        default_branch="unstable",
        history_branches=("7.2", "8.0"),
    )

    assert discovered == (primary,)
    assert calls == ["unstable"]


def test_historical_discovery_excludes_fix_already_on_pr_head(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "release", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "tests").mkdir()
    (repo / "tests" / "flaky.tcl").write_text("fixed\n")
    present = _commit(repo, "Deflake timeout")
    _git(repo, "update-ref", "refs/remotes/origin/unstable", present)
    _git(repo, "update-ref", "refs/remotes/origin/7.2", present)

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "test.txt").write_text("[err] timeout in tests/flaky.tcl\n")

    assert discover_port_candidates(
        str(repo), str(logs), history_branches=("7.2",),
    ) == ()


def test_log_reader_seeks_to_bounded_tail_without_reading_whole_file(
    tmp_path, monkeypatch,
):
    logs = tmp_path / "logs"
    logs.mkdir()
    payload = logs / "large.txt"
    payload.write_bytes(
        b"old-noise\n" * (_MAX_FILE_LOG_BYTES // 10 + 100)
        + b"[err] bounded tail failure\n"
    )

    def reject_unbounded_read(_path):
        raise AssertionError("Path.read_bytes would load the whole log")

    monkeypatch.setattr(Path, "read_bytes", reject_unbounded_read)

    text = _read_logs(str(logs))

    assert "bounded tail failure" in text
