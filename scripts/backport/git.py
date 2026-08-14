"""Shared git command execution for backport workflows."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from typing import Any, Callable

from scripts.common.git_auth import github_https_url
from scripts.common.identity import BOT_EMAIL, BOT_NAME

RunProcess = Callable[..., subprocess.CompletedProcess[str]]
RunGitEffect = Callable[..., object]
BuildCloneUrl = Callable[[str], str]


@dataclass(frozen=True)
class GitRunMode:
    """Subprocess details that intentionally differ between backport callers."""

    include_env_when_none: bool = False
    replace_decode_errors: bool = False
    pass_check_false: bool = False
    raise_with_command: bool = False
    debug_with_cwd: bool = False
    log_failures: bool = False


# main.py and callers that historically used its checked, side-effect runner.
PIPELINE_GIT_MODE = GitRunMode(
    include_env_when_none=True,
    debug_with_cwd=True,
    log_failures=True,
)

# cherry_pick.py historically decoded malformed git output lossily and always
# passed check=False to subprocess.run before enforcing its own check flag.
CHERRY_PICK_GIT_MODE = GitRunMode(
    replace_decode_errors=True,
    pass_check_false=True,
    raise_with_command=True,
)

# sweep_apply.py historically captured strict text without passing check/env.
SWEEP_APPLY_GIT_MODE = GitRunMode()

# The single pipeline runner behind main._run_git and the sweep helpers'
# run_git_default. It emits log records under main's historical logger name so
# existing log filtering/capture keyed on "scripts.backport.main" observes no
# change.
_PIPELINE_LOGGER = logging.getLogger("scripts.backport.main")


def run_git_command(
    repo_dir: str,
    *args: str,
    mode: GitRunMode,
    check: bool = True,
    env: dict[str, str] | None = None,
    run_process: RunProcess = subprocess.run,
    log: logging.Logger | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run git with a caller-selected legacy behavior profile.

    The profiles preserve whether ``env=None`` and ``check=False`` are passed
    through, decoding behavior, and the checked runner's existing diagnostics.
    No profile adds environment scrubbing, hook flags, or a timeout.
    """
    cmd = ["git", *args]
    if log is not None:
        if mode.debug_with_cwd:
            log.debug("Running: %s (cwd=%s)", " ".join(cmd), repo_dir)
        else:
            log.debug("Running: %s", " ".join(cmd))

    kwargs: dict[str, Any] = {
        "cwd": repo_dir,
        "capture_output": True,
        "text": True,
    }
    if mode.replace_decode_errors:
        kwargs["errors"] = "replace"
    if mode.include_env_when_none or env is not None:
        kwargs["env"] = env
    if mode.pass_check_false:
        kwargs["check"] = False

    result = run_process(cmd, **kwargs)
    if check and result.returncode != 0:
        if mode.log_failures and log is not None:
            log.error(
                "git %s failed (rc=%d)\nstdout: %s\nstderr: %s",
                args[0],
                result.returncode,
                result.stdout.strip()[-500:] if result.stdout else "",
                result.stderr.strip()[-500:] if result.stderr else "",
            )
        if mode.raise_with_command:
            raise subprocess.CalledProcessError(
                result.returncode,
                cmd,
                output=result.stdout,
                stderr=result.stderr,
            )
        result.check_returncode()
    return result


def run_pipeline_git(
    repo_dir: str,
    *args: str,
    env: dict[str, str] | None = None,
    log: logging.Logger = _PIPELINE_LOGGER,
    run_process: RunProcess | None = None,
) -> None:
    """Checked runner shared by the per-PR pipeline and the sweep helpers."""
    run_git_command(
        repo_dir,
        *args,
        mode=PIPELINE_GIT_MODE,
        env=env,
        run_process=subprocess.run if run_process is None else run_process,
        log=log,
    )


def clone_repository(
    repo_full_name: str,
    dest_dir: str,
    git_env: dict[str, str],
    *,
    clone_args: tuple[str, ...],
    clone_into_existing_directory: bool,
    configure_identity: bool,
    identity: tuple[str, str] = (BOT_NAME, BOT_EMAIL),
    identity_runner: RunGitEffect | None = None,
    fetch_all: bool = False,
    run_process: RunProcess = subprocess.run,
    url_builder: BuildCloneUrl = github_https_url,
) -> None:
    """Clone a branch with path-specific options and post-clone actions."""
    destination = "." if clone_into_existing_directory else dest_dir
    kwargs: dict[str, Any] = {
        "check": True,
        "capture_output": True,
        "text": True,
        "env": git_env,
    }
    if clone_into_existing_directory:
        kwargs["cwd"] = dest_dir
    run_process(
        [
            "git",
            "clone",
            *clone_args,
            url_builder(repo_full_name),
            destination,
        ],
        **kwargs,
    )
    if configure_identity:
        name, email = identity
        for key, value in (("user.name", name), ("user.email", email)):
            if identity_runner is not None:
                identity_runner(dest_dir, "config", key, value)
            else:
                run_process(
                    ["git", "config", key, value],
                    cwd=dest_dir,
                    check=True,
                    capture_output=True,
                    text=True,
                )
    if fetch_all:
        run_process(
            ["git", "fetch", "--all"],
            cwd=dest_dir,
            check=True,
            capture_output=True,
            text=True,
            env=git_env,
        )
