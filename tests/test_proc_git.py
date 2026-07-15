"""Credential-boundary regression tests for the shared git wrappers.

The verification command runs untrusted PR code in the worktree before the
pipeline calls ``git_output`` (``reset --hard``, ``clean``, ``diff``) to undo
its changes. That code can plant repo-local git config (a diff/filter driver,
``core.sshCommand``) which git would later execute. These tests prove the
parent's credentials are never in scope when that happens.
"""

import os
import subprocess
from pathlib import Path

import pytest

from scripts.common import proc
from scripts.common.git_auth import GitAuth
from scripts.common.proc import git_output, run_git


def _captured_env(monkeypatch):
    seen = {}
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        seen.setdefault("env", kwargs.get("env"))
        # Run for real so return-code/output behavior is unchanged.
        return real_run(cmd, **kwargs)

    monkeypatch.setattr(proc.subprocess, "run", fake_run)
    return seen


def test_git_output_runs_with_scrubbed_env(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    (repo / "f.txt").write_text("hi\n")
    run_git(str(repo), "add", "f.txt")
    run_git(str(repo), "-c", "user.email=t@t", "-c", "user.name=t",
            "commit", "-qm", "init")

    # Untrusted code mutates repo-local config (would run on next git op).
    run_git(str(repo), "config", "core.sshCommand", "leak $TARGET_TOKEN")

    # A secret is present in the parent environment.
    monkeypatch.setenv("TARGET_TOKEN", "super-secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

    seen = _captured_env(monkeypatch)
    git_output(str(repo), "reset", "--hard", "HEAD")

    env = seen["env"]
    assert env is not None, "git_output must pass an explicit scrubbed env"
    assert "TARGET_TOKEN" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env


def test_git_output_diff_still_works_scrubbed(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    (repo / "f.txt").write_text("a\n")
    run_git(str(repo), "add", "f.txt")
    run_git(str(repo), "-c", "user.email=t@t", "-c", "user.name=t",
            "commit", "-qm", "init")
    (repo / "f.txt").write_text("b\n")

    monkeypatch.setenv("TARGET_TOKEN", "super-secret")
    out = git_output(str(repo), "diff", "--name-only")
    assert "f.txt" in out


def test_authenticated_git_ignores_planted_hooks_and_config(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    repo.mkdir()
    run_git(None, "init", "--bare", "-q", str(remote))
    run_git(str(repo), "init", "-q")
    (repo / "f.txt").write_text("initial\n")
    run_git(str(repo), "add", "f.txt")
    run_git(
        str(repo),
        "-c", "user.email=t@t",
        "-c", "user.name=t",
        "commit", "-qm", "initial",
    )
    run_git(str(repo), "remote", "add", "origin", str(remote))

    hook_marker = tmp_path / "hook-marker"
    commit_hook_marker = tmp_path / "commit-hook-marker"
    checkout_hook_marker = tmp_path / "checkout-hook-marker"
    helper_marker = tmp_path / "helper-marker"
    fsmonitor_marker = tmp_path / "fsmonitor-marker"
    uploadpack_marker = tmp_path / "uploadpack-marker"
    receivepack_marker = tmp_path / "receivepack-marker"
    _write_marker_script(repo / ".git" / "hooks" / "pre-push", hook_marker)
    _write_marker_script(
        repo / ".git" / "hooks" / "pre-commit",
        commit_hook_marker,
    )
    _write_marker_script(
        repo / ".git" / "hooks" / "post-checkout",
        checkout_hook_marker,
    )
    helper = tmp_path / "credential-helper"
    _write_marker_script(helper, helper_marker)
    fsmonitor = tmp_path / "fsmonitor"
    _write_marker_script(fsmonitor, fsmonitor_marker)
    uploadpack = tmp_path / "uploadpack"
    _write_marker_script(uploadpack, uploadpack_marker)
    receivepack = tmp_path / "receivepack"
    _write_marker_script(receivepack, receivepack_marker)

    run_git(str(repo), "config", "credential.helper", str(helper))
    run_git(str(repo), "config", "core.fsmonitor", str(fsmonitor))
    monkeypatch.setenv("TARGET_TOKEN", "ambient-target-secret")

    with GitAuth("explicit-target-secret") as auth:
        (repo / "f.txt").write_text("authenticated operation\n")
        run_git(str(repo), "add", "f.txt", env=auth.env())
        run_git(str(repo), "commit", "-m", "authenticated commit", env=auth.env())
        run_git(str(repo), "checkout", "-b", "publish", env=auth.env())
        subprocess.run(
            ["git", "config", "remote.origin.uploadpack", str(uploadpack)],
            cwd=repo,
            check=True,
        )
        with pytest.raises(ValueError, match="transport command"):
            run_git(str(repo), "fetch", "origin", env=auth.env())
        subprocess.run(
            ["git", "config", "--unset", "remote.origin.uploadpack"],
            cwd=repo,
            check=True,
        )
        run_git(str(repo), "fetch", "origin", env=auth.env())
        run_git(str(repo), "diff", "HEAD^", env=auth.env())
        subprocess.run(
            ["git", "config", "remote.origin.receivepack", str(receivepack)],
            cwd=repo,
            check=True,
        )
        with pytest.raises(ValueError, match="transport command"):
            run_git(
                str(repo),
                "push", "origin", "HEAD:refs/heads/publish",
                env=auth.env(),
            )
        subprocess.run(
            ["git", "config", "--unset", "remote.origin.receivepack"],
            cwd=repo,
            check=True,
        )
        run_git(
            str(repo),
            "push", "origin", "HEAD:refs/heads/publish",
            env=auth.env(),
        )
        run_git(str(repo), "status", "--short", env=auth.env())

    assert not hook_marker.exists()
    assert not commit_hook_marker.exists()
    assert not checkout_hook_marker.exists()
    assert not helper_marker.exists()
    assert not fsmonitor_marker.exists()
    assert not uploadpack_marker.exists()
    assert not receivepack_marker.exists()


def test_git_ignores_global_executable_config_for_all_operations(
    tmp_path,
    monkeypatch,
):
    home = tmp_path / "home"
    home.mkdir()
    marker = tmp_path / "global-config-marker"
    executable = tmp_path / "global-executable"
    _write_marker_script(executable, marker)
    hooks = tmp_path / "global-hooks"
    hooks.mkdir()
    _write_marker_script(hooks / "pre-commit", marker)
    global_env = {
        "HOME": str(home),
        "PATH": os.environ["PATH"],
    }
    for key, value in (
        ("core.hooksPath", str(hooks)),
        ("core.fsmonitor", str(executable)),
        ("commit.gpgSign", "true"),
        ("gpg.program", str(executable)),
        ("credential.helper", str(executable)),
        ("diff.external", str(executable)),
        ("remote.origin.uploadpack", str(executable)),
        ("remote.origin.receivepack", str(executable)),
    ):
        subprocess.run(
            ["git", "config", "--global", key, value],
            check=True,
            env=global_env,
        )
    monkeypatch.setenv("HOME", str(home))

    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(None, "init", "--bare", "-q", str(remote))
    run_git(str(repo), "init", "-q")
    run_git(str(repo), "config", "user.email", "test@example.com")
    run_git(str(repo), "config", "user.name", "Test")
    run_git(str(repo), "remote", "add", "origin", str(remote))
    (repo / "f.txt").write_text("one\n", encoding="utf-8")
    run_git(str(repo), "add", "f.txt")
    run_git(str(repo), "commit", "-m", "initial")
    run_git(str(repo), "checkout", "-b", "publish")
    run_git(str(repo), "fetch", "origin")
    (repo / "f.txt").write_text("two\n", encoding="utf-8")
    run_git(str(repo), "diff")
    run_git(str(repo), "commit", "-am", "update")
    run_git(str(repo), "push", "origin", "HEAD:refs/heads/publish")

    assert not marker.exists()


def test_git_neutralizes_named_filter_diff_and_merge_drivers(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    run_git(str(repo), "config", "user.email", "test@example.com")
    run_git(str(repo), "config", "user.name", "Test")
    (repo / ".gitattributes").write_text(
        "tracked.txt filter=attack diff=attack\n"
        "merge.txt merge=attack\n",
        encoding="utf-8",
    )
    (repo / "tracked.txt").write_text("base\n", encoding="utf-8")
    (repo / "merge.txt").write_text("base\n", encoding="utf-8")
    run_git(str(repo), "add", ".gitattributes", "tracked.txt", "merge.txt")
    run_git(str(repo), "commit", "-qm", "base")
    run_git(str(repo), "branch", "-M", "main")

    run_git(str(repo), "checkout", "-qb", "other")
    (repo / "merge.txt").write_text("other\n", encoding="utf-8")
    run_git(str(repo), "commit", "-qam", "other")
    run_git(str(repo), "checkout", "-q", "main")
    (repo / "merge.txt").write_text("main\n", encoding="utf-8")
    run_git(str(repo), "commit", "-qam", "main")

    filter_marker = tmp_path / "filter-marker"
    diff_marker = tmp_path / "diff-marker"
    merge_marker = tmp_path / "merge-marker"
    filter_driver = tmp_path / "filter-driver"
    diff_driver = tmp_path / "diff-driver"
    merge_driver = tmp_path / "merge-driver"
    filter_driver.write_text(
        f"#!/bin/sh\nprintf executed > {filter_marker}\ncat\n",
        encoding="utf-8",
    )
    diff_driver.write_text(
        f"#!/bin/sh\nprintf executed > {diff_marker}\ncat \"$1\"\n",
        encoding="utf-8",
    )
    merge_driver.write_text(
        f"#!/bin/sh\nprintf executed > {merge_marker}\ncp \"$3\" \"$2\"\n",
        encoding="utf-8",
    )
    for path in (filter_driver, diff_driver, merge_driver):
        path.chmod(0o700)
    run_git(str(repo), "config", "filter.attack.clean", str(filter_driver))
    run_git(str(repo), "config", "filter.attack.smudge", str(filter_driver))
    run_git(str(repo), "config", "diff.attack.textconv", str(diff_driver))
    run_git(
        str(repo),
        "config",
        "merge.attack.driver",
        f"{merge_driver} %O %A %B",
    )

    (repo / "tracked.txt").write_text("changed\n", encoding="utf-8")
    run_git(str(repo), "add", "tracked.txt")
    run_git(str(repo), "diff", "HEAD", "--", "tracked.txt", check=False)
    run_git(str(repo), "checkout", "HEAD", "--", "tracked.txt", check=False)
    run_git(str(repo), "merge", "other", check=False)

    assert not filter_marker.exists()
    assert not diff_marker.exists()
    assert not merge_marker.exists()


@pytest.mark.parametrize(
    "config",
    [
        "filter.attack.clean=malicious-command",
        "remote.origin.uploadpack=malicious-command",
        "core.hooksPath=.git/unsafe-hooks",
        "commit.gpgSign=true",
    ],
)
def test_git_rejects_caller_supplied_executable_config(tmp_path, config):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")

    with pytest.raises(ValueError, match="locked Git config"):
        run_git(
            str(repo),
            "-c",
            config,
            "status",
        )


def _write_marker_script(path: Path, marker: Path) -> None:
    path.write_text(f"#!/bin/sh\nprintf executed > {marker}\nexit 0\n")
    path.chmod(0o700)


def test_build_approved_patch_treats_paths_literally(tmp_path):
    """A file named with pathspec magic must not broaden the approved patch.

    Without ``--literal-pathspecs``, ``git diff HEAD -- ':(glob)*'`` would
    expand to all tracked changes, leaking an unapproved edit into the patch
    that gets reviewed and pushed.
    """
    from scripts.common.proc import build_approved_patch

    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    (repo / "approved.txt").write_text("v1\n")
    (repo / "secret.txt").write_text("orig\n")
    run_git(str(repo), "add", "approved.txt", "secret.txt")
    run_git(str(repo), "-c", "user.email=t@t", "-c", "user.name=t",
            "commit", "-qm", "init")

    # Two tracked edits: only approved.txt is in the approved set.
    (repo / "approved.txt").write_text("v2\n")
    (repo / "secret.txt").write_text("leaked\n")
    # An untracked file whose name is pathspec magic.
    (repo / ":(glob)*").write_text("x\n")

    patch = build_approved_patch(str(repo), (":(glob)*", "approved.txt"))
    assert "approved.txt" in patch
    # The unrelated tracked edit must not be pulled in by the magic pathspec.
    assert "secret.txt" not in patch
    assert "leaked" not in patch


def test_reset_worktree_removes_ignored_build_artifacts(tmp_path):
    from scripts.ci_fix.review import reset_worktree

    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(str(repo), "init", "-q")
    (repo / ".gitignore").write_text("*.o\nbuild/\n")
    (repo / "f.c").write_text("int main(){return 0;}\n")
    run_git(str(repo), "add", ".gitignore", "f.c")
    run_git(
        str(repo),
        "-c", "user.email=t@t",
        "-c", "user.name=t",
        "commit", "-qm", "init",
    )

    (repo / "f.o").write_text("object")
    (repo / "build").mkdir()
    (repo / "build" / "server").write_text("binary")
    (repo / "scratch.txt").write_text("untracked")

    reset_worktree(str(repo))

    assert not (repo / "f.o").exists()
    assert not (repo / "build").exists()
    assert not (repo / "scratch.txt").exists()
    assert (repo / "f.c").exists()
