"""Replay a failed CI job's non-privileged setup as a disposable account.

The setup script comes from the target repository's workflow. It runs before
AWS credentials exist, as an account that owns only the checkout. The
authoritative plan, trusted agent, real GitHub command files, OIDC request
variables, and runner temp remain unavailable. Every process of the account
is killed before the checkout is reclaimed.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path

from scripts.common.isolation import as_user, handed_to
from scripts.common.logging_utils import capture_untrusted_output

_SAFE_ENV = (
    "CI",
    "GITHUB_ACTIONS",
    "LANG",
    "LC_ALL",
    "RUNNER_ARCH",
    "RUNNER_OS",
)
SETUP_TIMEOUT_S = 15 * 60


def replay_setup(repo_dir: Path, setup_file: Path, *, user: str, runner_temp: Path) -> None:
    """Run ``setup_file`` as ``user`` over ``repo_dir``, then reclaim the tree."""
    repo = repo_dir.resolve(strict=True)
    setup = setup_file.resolve(strict=True)
    temp = runner_temp.resolve(strict=True)
    if not setup.is_file():
        raise ValueError("the setup plan is not a regular file")
    if setup.parent != temp / "ci-fix-plan":
        raise ValueError("the setup plan is outside the authoritative plan directory")

    script = setup.read_text(encoding="utf-8")
    path = _safe_path(repo, temp)
    shell = shutil.which("sh", path=path)
    if not shell or not Path(shell).is_absolute():
        raise RuntimeError("no trusted POSIX shell is available for the job setup")

    timed_out = False
    result = None
    with capture_untrusted_output(label="target setup") as output:
        with handed_to(user, repo) as home:
            private = Path(home)
            env = {
                **{name: value for name in _SAFE_ENV if (value := os.environ.get(name))},
                "PATH": path,
                "GITHUB_WORKSPACE": str(repo),
                # The generated driver gives each replayed step fresh files of its
                # own. These outer values are also private in case a script reads
                # one before the driver replaces it.
                "GITHUB_ENV": str(private / "github-env"),
                "GITHUB_PATH": str(private / "github-path"),
                "GITHUB_OUTPUT": str(private / "github-output"),
                "GITHUB_STATE": str(private / "github-state"),
                "RUNNER_TEMP": str(private),
            }
            try:
                invocation = as_user(user, env, [shell, "-s"], home=home)
                result = subprocess.run(
                    invocation.argv,
                    cwd=repo,
                    env=invocation.env,
                    input=script,
                    text=True,
                    errors="replace",
                    check=False,
                    timeout=SETUP_TIMEOUT_S,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.TimeoutExpired:
                timed_out = True
    if timed_out:
        raise RuntimeError("the isolated job setup exceeded its time limit") from None
    assert result is not None
    if result.returncode != 0:
        raise RuntimeError(f"the isolated job setup failed with exit status {result.returncode}")


def _safe_path(repo: Path, runner_temp: Path) -> str:
    """The pre-setup PATH without any target- or runner-temp-controlled entry."""
    paths: list[str] = []
    for raw in os.environ.get("PATH", os.defpath).split(os.pathsep):
        if not raw or not Path(raw).is_absolute():
            continue
        try:
            resolved = Path(raw).resolve(strict=True)
        except OSError:
            continue
        if not resolved.is_dir():
            continue
        if _within(resolved, repo) or _within(resolved, runner_temp):
            continue
        value = str(resolved)
        if value not in paths:
            paths.append(value)
    for value in ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        if Path(value).is_dir() and value not in paths:
            paths.append(value)
    return os.pathsep.join(paths)


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-dir", type=Path, required=True)
    parser.add_argument("--setup", type=Path, required=True)
    parser.add_argument("--runner-temp", type=Path, required=True)
    parser.add_argument("--user", required=True)
    args = parser.parse_args(argv)
    replay_setup(args.repo_dir, args.setup, user=args.user, runner_temp=args.runner_temp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
