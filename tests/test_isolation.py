"""Untrusted commands run as a separate account that cannot reach the job's secrets.

The tests that need a real account run where CI_FIX_TEST_AGENT_USER names one
this process can switch to, as root or through sudo (CI creates it); elsewhere
they skip.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from scripts.ai import claude_code
from scripts.ci_fix import engine
from scripts.common import isolation

_USER = os.environ.get("CI_FIX_TEST_AGENT_USER", "")
needs_user = pytest.mark.skipif(
    not _USER or (os.geteuid() != 0 and (
        shutil.which("sudo") is None
        or subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0)),
    reason="CI_FIX_TEST_AGENT_USER names no account this test can switch to",
)


def _disable_process_cleanup(monkeypatch) -> None:
    """Keep filesystem-focused handoff tests away from the host account."""
    monkeypatch.setattr(isolation, "_quiesce", lambda _user: None)


def test_inside_actions_an_account_is_required(monkeypatch):
    for variable in (isolation.USER_ENV, isolation.COMMAND_USER_ENV):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(RuntimeError, match="refusing to run untrusted commands as the workflow user"):
        isolation.isolated_user()
    with pytest.raises(RuntimeError, match="refusing to run untrusted commands as the workflow user"):
        isolation.isolated_command_user()
    monkeypatch.delenv("GITHUB_ACTIONS")
    assert isolation.isolated_user() == ""
    assert isolation.isolated_command_user() == ""


def test_the_command_runs_with_its_own_home_and_git_allowed(monkeypatch):
    monkeypatch.setattr(isolation.sys, "platform", "linux")
    monkeypatch.setattr(isolation.os, "geteuid", lambda: 1001)
    monkeypatch.setattr(isolation.shutil, "which", lambda name, **_kwargs: name)
    invocation = isolation.as_user(
        "v",
        {"PATH": "/usr/bin", "HOME": "/home/runner", "AWS_SECRET_ACCESS_KEY": "secret"},
        ["bash", "-c", "make"],
        home="/w/ci-fix-home-1",
    )
    assert invocation.argv[:11] == (
        "sudo", "-n", "-E", "runuser", "--preserve-environment", "-u", "v", "--",
        "setpriv", "--no-new-privs", "--",
    )
    assert invocation.argv[11:] == ("env", "bash", "-c", "make")
    assert all("secret" not in argument for argument in invocation.argv)
    assert invocation.env == {
        "PATH": "/usr/bin",
        "HOME": "/w/ci-fix-home-1",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "TMPDIR": "/w/ci-fix-home-1",
        "TMP": "/w/ci-fix-home-1",
        "USER": "v",
        "LOGNAME": "v",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": "*",
    }


def test_root_switches_account_without_sudo_and_macos_through_sudo(monkeypatch):
    """A job's container runs as root and has no sudo; macOS has no runuser."""
    monkeypatch.setattr(isolation.os, "geteuid", lambda: 0)
    monkeypatch.setattr(isolation.sys, "platform", "linux")
    monkeypatch.setattr(
        isolation.shutil,
        "which",
        lambda name, **_kwargs: f"/usr/sbin/{name}",
    )
    invocation = isolation.as_user("v", {}, ["make"], home="/h")
    assert invocation.argv[:9] == (
        "/usr/sbin/runuser", "--preserve-environment", "-u", "v", "--",
        "/usr/sbin/setpriv", "--no-new-privs", "--", "/usr/sbin/env",
    )
    monkeypatch.setattr(isolation.os, "geteuid", lambda: 501)
    monkeypatch.setattr(isolation.sys, "platform", "darwin")
    invocation = isolation.as_user("v", {}, ["make"], home="/h")
    assert invocation.argv[:7] == (
        "/usr/sbin/sudo", "-n", "-E", "-u", "v", "--", "/usr/sbin/env",
    )


@pytest.mark.parametrize(
    ("platform", "euid", "expected"),
    [
        (
            "linux",
            1001,
            [
                "/usr/bin/sudo", "-n", "/usr/sbin/runuser", "-u", "agent", "--",
                "/usr/bin/env", "-i", "/usr/bin/stat", "/home/runner/.",
            ],
        ),
        (
            "linux",
            0,
            [
                "/usr/sbin/runuser", "-u", "agent", "--",
                "/usr/bin/env", "-i", "/usr/bin/stat", "/home/runner/.",
            ],
        ),
        (
            "darwin",
            501,
            [
                "/usr/bin/sudo", "-n", "-u", "agent", "--",
                "/usr/bin/env", "-i", "/usr/bin/stat", "/home/runner/.",
            ],
        ),
    ],
)
def test_traversal_is_probed_by_resolving_dot_as_the_account(monkeypatch, platform, euid, expected):
    """The kernel evaluates the account's ACL; no mode-bit/access(2) shortcut can override it."""
    paths = {
        "env": "/usr/bin/env",
        "runuser": "/usr/sbin/runuser",
        "stat": "/usr/bin/stat",
        "sudo": "/usr/bin/sudo",
    }
    calls = []
    monkeypatch.setattr(isolation.sys, "platform", platform)
    monkeypatch.setattr(isolation.os, "geteuid", lambda: euid)
    monkeypatch.setattr(isolation.shutil, "which", lambda name, **_kwargs: paths.get(name))

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(isolation.subprocess, "run", run)

    assert isolation._traversable("agent", Path("/home/runner"))
    assert calls == [(expected, {"capture_output": True, "check": False})]


def test_traversal_probe_fails_closed_on_any_unsuccessful_account_lookup(monkeypatch):
    monkeypatch.setattr(
        isolation.subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 1),
    )
    assert not isolation._traversable("agent", Path("/home/runner"))


@pytest.mark.parametrize("argument", ("root", "readable"))
def test_filesystem_root_is_never_a_recursive_handoff_target(
    tmp_path,
    monkeypatch,
    argument,
):
    target = tmp_path / "root-link"
    target.symlink_to("/", target_is_directory=True)
    repo = tmp_path / "repo"
    repo.mkdir()
    root = target if argument == "root" else repo
    readable = (target,) if argument == "readable" else ()
    privileged_calls = []
    monkeypatch.setattr(
        isolation,
        "_traversable",
        lambda *_args: pytest.fail("root must be refused before traversal probes"),
    )
    monkeypatch.setattr(
        isolation,
        "_as_root",
        lambda *argv: privileged_calls.append(argv),
    )

    with pytest.raises(ValueError, match="filesystem root"):
        with isolation.handed_to("agent", root, readable=readable):
            pass

    assert privileged_calls == []


@pytest.fixture
def checkout():
    """A checkout under a traversable directory, as the engine makes one."""
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    repo = work / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / "file").write_text("x\n")
    yield repo
    subprocess.run([*isolation._root(), "rm", "-rf", str(work)], check=False)


@pytest.fixture
def credentialed():
    """A process of the workflow user whose environment holds a secret, as the job's does.

    ``/proc/<pid>/environ`` shows the environment a process started with, so
    a secret set in this process after it started would never appear there,
    and a broken isolation would go unnoticed.
    """
    holder = subprocess.Popen(["sleep", "60"], env={**os.environ, "TARGET_TOKEN": "ghs_secret"})
    environ = f"/proc/{holder.pid}/environ"
    assert b"ghs_secret" in Path(environ).read_bytes()  # the workflow user can read it
    yield environ
    holder.kill()
    holder.wait()


@needs_user
def test_a_test_command_cannot_read_the_parent_environment_or_change_git(checkout, monkeypatch, credentialed):
    monkeypatch.setenv(isolation.COMMAND_USER_ENV, _USER)
    head = (checkout / ".git" / "HEAD").read_text()
    run = engine.run_test_command(
        str(checkout),
        f"n=$(tr '\\0' '\\n' < {credentialed} 2>/dev/null | grep -c ghs_secret); echo \"seen=${{n:-0}}\"; "
        "echo evil > .git/HEAD; echo left > /tmp/ci-fix-isolation-marker; echo ok > made; id -un",
    )
    assert "seen=0" in run.output and _USER in run.output
    assert (checkout / ".git" / "HEAD").read_text() == head
    assert not Path("/tmp/ci-fix-isolation-marker").exists()
    assert (checkout / "made").stat().st_uid == os.getuid()  # handed back


@needs_user
def test_nothing_the_command_started_outlives_it(checkout, monkeypatch):
    monkeypatch.setenv(isolation.COMMAND_USER_ENV, _USER)
    marker = checkout / "late"
    engine.run_test_command(str(checkout), f"(setsid sleep 2; touch {marker}) >/dev/null 2>&1 &")
    time.sleep(3)
    assert not marker.exists()


@needs_user
def test_the_claude_cli_runs_as_the_account(checkout, monkeypatch, tmp_path, credentialed):
    """The real CLI path, with a fake ``claude`` that reports who it is and what it can read."""
    bin_dir = Path(tempfile.mkdtemp(prefix="ci-fix-bin-", dir="/tmp"))
    bin_dir.chmod(0o755)
    fake = bin_dir / "claude"
    fake.write_text(
        "#!/bin/bash\ncat >/dev/null\n"
        f"seen=$(tr '\\0' '\\n' < {credentialed} 2>/dev/null | grep -c ghs_secret)\n"
        'echo "{\\"type\\":\\"result\\",\\"result\\":\\"user=$(id -un) seen=${seen:-0}\\"}"\n'
    )
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    stdout, _stderr, rc = claude_code.run_claude_code("p", cwd=str(checkout), run_as=_USER, timeout=60)
    shutil.rmtree(bin_dir, ignore_errors=True)
    assert rc == 0
    assert f"user={_USER} seen=0" in stdout


def test_a_replaced_git_directory_is_never_handed_back(monkeypatch):
    """The account owns the checkout's directory, so it can swap .git for one of its own."""
    privileged_calls = []
    monkeypatch.setattr(
        isolation,
        "_as_root",
        lambda *argv: privileged_calls.append(argv),
    )
    monkeypatch.setattr(isolation, "_traversable", lambda user, directory: True)
    me = isolation.pwd.getpwuid(os.getuid())
    monkeypatch.setattr(isolation.pwd, "getpwnam", lambda user: me)
    _disable_process_cleanup(monkeypatch)
    monkeypatch.setattr(isolation, "_remove_files_of", lambda user: None)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    repo = work / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setattr(isolation.tempfile, "mkdtemp", lambda **_kw: str(work / "home"))

    with isolation.handed_to("agent", repo):
        pass  # an untouched .git is handed back

    with pytest.raises(OSError, match="replaced"):
        with isolation.handed_to("agent", repo):
            (repo / ".git").rename(repo / ".git.old")
            shutil.copytree(repo / ".git.old", repo / ".git")
            calls_before_reclaim = len(privileged_calls)
    assert len(privileged_calls) == calls_before_reclaim
    shutil.rmtree(work)


def test_reclaim_does_not_touch_a_tree_until_the_account_is_quiescent(
    tmp_path,
    monkeypatch,
):
    repo = tmp_path / "repo"
    home = tmp_path / "home"
    git_dir = repo / ".git"
    git_dir.mkdir(parents=True)
    home.mkdir()
    privileged_calls = []
    monkeypatch.setattr(
        isolation,
        "_quiesce",
        lambda _user: (_ for _ in ()).throw(OSError("still running")),
    )
    monkeypatch.setattr(
        isolation,
        "_as_root",
        lambda *argv: privileged_calls.append(argv),
    )
    monkeypatch.setattr(
        isolation,
        "_detach_nested_mounts",
        lambda _path: pytest.fail("mount cleanup must wait for quiescence"),
    )

    with pytest.raises(OSError, match="still running"):
        isolation._reclaim(
            "agent",
            repo,
            str(home),
            "1:1",
            git_dir,
            isolation._identity(repo),
            isolation._identity(home),
            isolation._identity(git_dir),
        )

    assert privileged_calls == []


def test_mount_scan_includes_the_root_and_parses_adversarial_macos_sources(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "repo"
    root.mkdir()
    nested = root / "name on disk"
    output = (
        f"/source on misleading text on {nested} (fusefs, local)\n"
        f"/another-source on {root} (nullfs, local)\n"
        "/dev/disk1s1 on /Volumes/elsewhere (apfs, local)\n"
    )
    monkeypatch.setattr(isolation.sys, "platform", "darwin")
    monkeypatch.setattr(isolation, "_program", lambda name: f"/sbin/{name}")
    monkeypatch.setattr(
        isolation.subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, output, ""),
    )

    assert isolation._nested_mounts(root) == [nested, root]


def test_checkout_modes_are_safe_and_readable_when_the_account_returns_it(monkeypatch):
    """Directory modes are not in Git, and unreadable files would prevent rollback."""
    def as_root(*argv):
        if argv[0] == "rm":
            subprocess.run(argv, check=True)
        elif argv[0] == "find" and "chmod" in argv:
            subprocess.run(argv, check=True)

    monkeypatch.setattr(isolation, "_as_root", as_root)
    monkeypatch.setattr(isolation, "_traversable", lambda user, directory: True)
    me = isolation.pwd.getpwuid(os.getuid())
    monkeypatch.setattr(isolation.pwd, "getpwnam", lambda user: me)
    _disable_process_cleanup(monkeypatch)
    monkeypatch.setattr(isolation, "_remove_files_of", lambda user: None)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    repo = work / "repo"
    nested = repo / "nested"
    nested.mkdir(parents=True)
    unreadable = nested / "unreadable"
    writable = nested / "world-writable"
    unreadable.write_text("x\n", encoding="utf-8")
    writable.write_text("x\n", encoding="utf-8")

    with isolation.handed_to("agent", repo):
        unreadable.chmod(0o000)
        writable.chmod(0o777)
        nested.chmod(0o000)

    assert nested.stat().st_mode & 0o777 == 0o700
    assert unreadable.stat().st_mode & 0o777 == 0o600
    assert writable.stat().st_mode & 0o777 == 0o755
    shutil.rmtree(work)


def test_nested_git_metadata_is_removed_before_the_checkout_is_reused(monkeypatch):
    """No repository metadata planted by the account reaches trusted git commands."""
    def as_root(*argv):
        if argv[0] in ("find", "rm"):  # the removals for real; the test's own files need no chown
            subprocess.run(argv, check=True)

    monkeypatch.setattr(isolation, "_as_root", as_root)
    monkeypatch.setattr(isolation, "_traversable", lambda user, directory: True)
    me = isolation.pwd.getpwuid(os.getuid())
    monkeypatch.setattr(isolation.pwd, "getpwnam", lambda user: me)
    _disable_process_cleanup(monkeypatch)
    monkeypatch.setattr(isolation, "_remove_files_of", lambda user: None)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    repo = work / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setattr(isolation.tempfile, "mkdtemp", lambda **_kw: str(work / "home"))

    with isolation.handed_to("agent", repo):
        # A dependency the job checked out, and a repository the agent made.
        subprocess.run(["git", "init", "-q", str(repo / "libbacktrace")], check=True)
        (repo / "libbacktrace" / "x.c").write_text("int x;\n")
        (repo / "deep" / "planted").mkdir(parents=True)
        (repo / "deep" / "planted" / ".git").write_text("gitdir: /tmp/elsewhere\n")  # a gitlink
    assert (repo / ".git").is_dir() and (repo / "libbacktrace" / "x.c").exists()
    assert not (repo / "libbacktrace" / ".git").exists() and not (repo / "deep" / "planted" / ".git").exists()
    shutil.rmtree(work)


def test_a_hard_link_in_the_checkout_is_refused_going_in_and_removed_coming_back(monkeypatch):
    """``chown -P`` stops at a symbolic link but reaches through a hard one; macOS lets the account make them."""
    def as_root(*argv):
        if argv[0] in ("find", "rm"):
            subprocess.run(argv, check=True)

    monkeypatch.setattr(isolation, "_as_root", as_root)
    monkeypatch.setattr(isolation, "_traversable", lambda user, directory: True)
    me = isolation.pwd.getpwuid(os.getuid())
    monkeypatch.setattr(isolation.pwd, "getpwnam", lambda user: me)
    _disable_process_cleanup(monkeypatch)
    monkeypatch.setattr(isolation, "_remove_files_of", lambda user: None)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    repo = work / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setattr(isolation.tempfile, "mkdtemp", lambda **_kw: str(work / "home"))
    outside = work / "runner-owned"
    outside.write_text("secret\n")

    with isolation.handed_to("agent", repo):
        os.link(outside, repo / "linked")  # what the account could do on macOS
    assert not (repo / "linked").exists() and outside.read_text() == "secret\n"

    os.link(outside, repo / "linked")
    with pytest.raises(OSError, match="hard links"):
        with isolation.handed_to("agent", repo):
            pass
    shutil.rmtree(work)


def test_every_root_command_ends_its_options_before_its_first_operand(monkeypatch):
    """macOS's chmod and chown take the first operand for the mode or owner,
    so a ``--`` after it is read as a file that does not exist."""
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(isolation, "_as_root", lambda *argv: calls.append(argv))
    monkeypatch.setattr(isolation, "_traversable", lambda user, directory: True)
    me = isolation.pwd.getpwuid(os.getuid())
    monkeypatch.setattr(isolation.pwd, "getpwnam", lambda user: me)
    _disable_process_cleanup(monkeypatch)
    monkeypatch.setattr(isolation, "_remove_files_of", lambda user: None)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    (work / "repo").mkdir()
    (work / "log").mkdir()

    with isolation.handed_to("agent", work / "repo", readable=(work / "log",)):
        pass
    shutil.rmtree(work)

    assert calls
    for argv in calls:
        if "--" in argv:
            options = argv[1:argv.index("--")]
            assert all(option.startswith("-") for option in options), argv


@pytest.mark.parametrize("listing, live", [
    (" 10 1000 1000 Z\n 11 1000 0 Z+\n 1 0 0 Ss\n", []),
    (" 10 1000 1000 S\n 11 1000 0 Z\n", ["S"]),
    (" 10 1000 0 R\n", ["R"]),  # setuid: real identity still belongs to the account
    (" 10 0 1000 S\n", ["S"]),  # effective identity belongs to it
    (" 10 1001 1001 R\n", []),  # another account's
])
def test_a_zombie_the_command_left_does_not_count_as_surviving(monkeypatch, listing, live):
    monkeypatch.setattr(isolation.subprocess, "run",
                        lambda argv, **_kw: subprocess.CompletedProcess(argv, 0, listing, ""))

    assert isolation._live_processes(1000) == live


def test_process_scan_uses_the_portable_effective_uid_field(monkeypatch):
    seen = []

    def run(argv, **_kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(isolation.subprocess, "run", run)

    assert isolation._live_processes(1000) == []
    assert Path(seen[0][0]).name == "ps"
    assert seen[0][1:] == [
        "-A", "-o", "pid=", "-o", "ruid=", "-o", "uid=", "-o", "stat=",
    ]


def test_disposable_account_cron_and_at_jobs_are_removed_and_verified(monkeypatch):
    calls = []
    atq_outputs = iter(("17 Thu Oct 29 12:00:00 2026 a agent\n", ""))
    programs = {
        "crontab": "/usr/bin/crontab",
        "atq": "/usr/bin/atq",
        "atrm": "/usr/bin/atrm",
    }
    monkeypatch.setattr(isolation.sys, "platform", "linux")
    monkeypatch.setattr(isolation.shutil, "which", lambda name, **_kwargs: programs.get(name))
    monkeypatch.setattr(isolation, "_root", lambda: ["/usr/bin/sudo", "-n"])
    monkeypatch.setattr(isolation, "_as_account", lambda user: ("/switch", user))

    def run(argv, **kwargs):
        calls.append((argv, kwargs["env"]))
        if argv[-1] in ("-r", "-l"):
            return subprocess.CompletedProcess(argv, 1, "", "no crontab for agent")
        if argv[-1] == "/usr/bin/atq":
            return subprocess.CompletedProcess(argv, 0, next(atq_outputs), "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(isolation.subprocess, "run", run)

    isolation.clear_delayed_jobs("agent")

    assert [call[0] for call in calls] == [
        ["/usr/bin/sudo", "-n", "/usr/bin/crontab", "-u", "agent", "-r"],
        ["/usr/bin/sudo", "-n", "/usr/bin/crontab", "-u", "agent", "-l"],
        ["/switch", "agent", "/usr/bin/atq"],
        ["/switch", "agent", "/usr/bin/atrm", "17"],
        ["/switch", "agent", "/usr/bin/atq"],
    ]
    assert all(env == isolation._CLEAN_SYSTEM_ENV for _argv, env in calls)


def test_macos_atq_uses_the_trailing_job_number(monkeypatch):
    monkeypatch.setattr(isolation.sys, "platform", "darwin")
    monkeypatch.setattr(isolation, "_as_account", lambda user: ("/switch", user))
    monkeypatch.setattr(
        isolation.subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(
            argv,
            0,
            "Date Owner Queue Job#\nThu Oct 29 12:00:00 2026 agent a 17\n",
            "",
        ),
    )

    assert isolation._at_jobs("agent", "/usr/bin/atq") == ["17"]


@needs_user
def test_the_account_cannot_swap_git_for_one_of_its_own(checkout, monkeypatch):
    monkeypatch.setenv(isolation.COMMAND_USER_ENV, _USER)
    with pytest.raises(OSError, match="replaced"):
        engine.run_test_command(str(checkout), "mv .git .git.old && cp -r .git.old .git")


def test_root_is_reached_by_sudos_full_path(monkeypatch):
    """A command started with an empty environment finds nothing on PATH (macOS's sudo)."""
    from scripts.common import isolation as isolation_mod

    monkeypatch.setattr(isolation_mod.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(
        isolation_mod.shutil,
        "which",
        lambda name, **_kwargs: f"/usr/bin/{name}",
    )
    assert isolation_mod._root() == ["/usr/bin/sudo", "-n"]


def test_a_readable_directory_the_account_cannot_reach_fails_loudly(monkeypatch):
    """macOS's default temp directory is the runner's own: the agent would read nothing."""
    monkeypatch.setattr(isolation, "_as_root", lambda *argv: None)
    _disable_process_cleanup(monkeypatch)
    work = Path(tempfile.mkdtemp(prefix="ci-fix-test-", dir="/tmp"))
    work.chmod(0o711)
    (work / "repo").mkdir()
    private = work / "private"
    (private / "log").mkdir(parents=True)
    private.chmod(0o700)
    blocked = private.resolve()

    checked = []

    def traversable(user, directory):
        checked.append(directory)
        return directory != blocked

    monkeypatch.setattr(isolation, "_traversable", traversable)
    with pytest.raises(OSError) as failure:
        with isolation.handed_to("agent", work / "repo", readable=(private / "log",)):
            pass
    assert f"{blocked} is not traversable" in str(failure.value)
    assert checked == [*reversed((work / "repo").resolve().parents), blocked]
    shutil.rmtree(work)
