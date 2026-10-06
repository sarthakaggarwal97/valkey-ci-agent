"""Sanitized execution of AI-proposed verification commands.

The AI proposes how to build and run a single failing test; this module
decides whether it passed. That split is the trust anchor of the whole
workflow: a passing verdict can only come from a real subprocess exit code,
never from anything the model says.

Every command is hardened: it runs with a no-secret environment (process
basics and CA certs only, never a GitHub token or AWS credentials, since it is
untrusted PR code), in a working directory locked inside the cloned repo, under
a timeout, with its output capped to the tail (where the failure summary lives).
Commands run through Bash when available because GitHub-hosted Linux/macOS
jobs default to Bash and real build+test recipes may use Bash conditionals;
otherwise they fall back to ``/bin/sh -c``. Docker verification still uses the
container's ``/bin/sh`` because minimal images may not include Bash.

A scrubbed environment is not isolation on its own: a process running as the
workflow user can read the credentialed parent's ``/proc/<pid>/environ``,
rewrite the state file the publish step trusts, or plant an executable on its
``PATH``, and a background process outlives the command. So when
``CI_FIX_VERIFY_USER`` names an unprivileged account (the workflows create
one), a host command runs as that user over a checkout handed to it for the
run, and every process the user owns is killed before the checkout is handed
back. A container command runs in its own PID and mount namespace and is
removed when the run ends.
"""

from __future__ import annotations

import logging
import os
import pwd
import select
import shlex
import shutil
import signal
import stat
import subprocess
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from scripts.ci_fix.models import RunResult
from scripts.common.proc import NETWORK_ENV, PROCESS_BASICS, filter_env
from scripts.common.text_utils import strip_ansi

logger = logging.getLogger(__name__)

# The verification command is untrusted PR code. It gets only process basics
# (plus CA certs for HTTPS) - no GitHub token and no AWS credentials, so it
# cannot read the Bedrock role the AI layer uses.
_VERIFY_ENV_ALLOWLIST = PROCESS_BASICS + NETWORK_ENV

_DEFAULT_TIMEOUT_S = 30 * 60
_OUTPUT_TAIL_CHARS = 32 * 1024
# Hard cap on captured output. The timeout bounds runtime; this bounds memory
# so a command that floods stdout cannot exhaust the runner. We keep only the
# tail, which is where build/test failure summaries live.
_MAX_CAPTURED_BYTES = 8 * 1024 * 1024

VERIFY_USER_ENV = "CI_FIX_VERIFY_USER"


def run_verification_command(
    repo_dir: str,
    command: str,
    *,
    workdir: str = "",
    timeout: int = _DEFAULT_TIMEOUT_S,
    env_allowlist: tuple[str, ...] = _VERIFY_ENV_ALLOWLIST,
    container_image: str = "",
) -> RunResult:
    """Run ``command`` in ``repo_dir`` and report its real verdict.

    ``command`` is the AI-proposed targeted build+verify recipe (commonly
    chained with ``&&``), run via the local verification shell with a scrubbed environment,
    locked to a working directory inside the clone. ``passed`` is
    ``exit_code == 0`` - the subprocess decides, not the caller and not the AI.

    When ``container_image`` is set, the command runs inside that image via
    ``docker run`` (the working directory is bind-mounted), so a container-job
    failure is verified in its own distro. The image is chosen by code from the
    failed job's metadata, never from the AI.

    A command that cannot be executed at all (missing cwd, OS error) returns
    ``ran=False`` so the pipeline treats it as a refusal, not a pass.
    """
    command = command.strip()
    if not command:
        return RunResult(
            ran=False, passed=False, exit_code=-1, command=command,
            output_tail="empty command",
        )

    cwd = _resolve_workdir(repo_dir, workdir)
    if cwd is None:
        return RunResult(
            ran=False, passed=False, exit_code=-1, command=command,
            output_tail=f"workdir {workdir!r} escapes or does not exist under repo",
        )

    repo_root = Path(repo_dir).resolve()
    env = filter_env(env_allowlist)
    user = "" if container_image else os.environ.get(VERIFY_USER_ENV, "")
    where = f"docker[{container_image}]" if container_image else str(cwd)
    logger.info("Running verification command in %s (timeout=%ds): %s", where, timeout, command)
    try:
        if container_image:
            name = f"ci-fix-verify-{uuid.uuid4().hex[:12]}"
            argv = [*_verification_shell(), _dockerize(command, repo_root, workdir, container_image, name=name)]
            try:
                ran, exit_code, output, timed_out = _run_capped(argv, cwd, env, timeout)
            finally:
                # Killing the docker client on a timeout leaves the container running.
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        elif user:
            with _handed_to(user, repo_root):
                argv = _as_user(user, env, [*_verification_shell(), command])
                ran, exit_code, output, timed_out = _run_capped(argv, cwd, env, timeout)
        else:
            ran, exit_code, output, timed_out = _run_capped([*_verification_shell(), command], cwd, env, timeout)
    except OSError as exc:
        logger.warning("Verification command failed to start: %s", exc)
        return RunResult(
            ran=False, passed=False, exit_code=-1, command=command,
            output_tail=f"failed to start: {exc}",
        )

    tail = _tail(output)
    if timed_out:
        logger.warning("Verification command timed out after %ds", timeout)
        return RunResult(
            ran=True, passed=False, exit_code=-1, command=command,
            output_tail=tail or f"timed out after {timeout}s", timed_out=True,
        )
    passed = exit_code == 0
    logger.info("Verification command exited %d (passed=%s)", exit_code, passed)
    return RunResult(
        ran=True, passed=passed, exit_code=exit_code,
        command=command, output_tail=tail,
    )


def _dockerize(command: str, repo_root: Path, workdir: str, image: str, *, name: str) -> str:
    """Wrap ``command`` to run inside ``image`` via docker, mounting the repo.

    The whole repository (including ``.git`` and any sibling paths the command
    needs) is bind-mounted at /src, and the command runs in /src/<workdir> so a
    nested ``workdir`` does not lose the rest of the checkout. The container is
    sandboxed: ``--cap-drop ALL`` and ``--security-opt no-new-privileges`` strip
    privileges, ``--user`` matches the host uid/gid so files stay owned by the
    runner user, and ``--network none`` denies network (the verify env is
    credential-free and the repo is already checked out). ``--rm`` cleans up.
    The image is code-chosen and already validated; the command is passed
    through ``sh -c`` inside the container exactly as the AI proposed it.
    """
    container_dir = "/src" if not workdir else f"/src/{workdir}"
    inner = f"cd {shlex.quote(container_dir)} && ({command})"
    uid = os.getuid()
    gid = os.getgid()
    return (
        f"docker run --rm --name {shlex.quote(name)} --network none --cap-drop ALL "
        "--security-opt no-new-privileges "
        f"--user {uid}:{gid} "
        f"-v {shlex.quote(str(repo_root))}:/src -w /src "
        f"{shlex.quote(image)} /bin/sh -c {shlex.quote(inner)}"
    )


def _as_user(user: str, env: dict[str, str], argv: list[str]) -> list[str]:
    """``argv`` run as ``user`` with ``env``, its home and temp directories its own."""
    home = pwd.getpwnam(user).pw_dir
    scrubbed = {**{k: v for k, v in env.items() if k not in ("TMPDIR", "TMP")},
                "HOME": home, "USER": user, "LOGNAME": user}
    # runuser, not ``sudo -u``: it needs only root, which a runner's sudo grants.
    return ["sudo", "-n", "runuser", "-u", user, "--", "env", "-i",
            *(f"{key}={value}" for key, value in scrubbed.items()), *argv]


@contextmanager
def _handed_to(user: str, repo_root: Path) -> Iterator[None]:
    """Give ``repo_root`` to ``user`` for one command, then take it back.

    Every process ``user`` owns is killed before ownership returns, so nothing
    started by the command can touch the checkout once it is reviewed.
    ``chown -h -P`` changes symbolic links themselves and never follows them,
    so a link in the checkout cannot redirect the privileged chown.
    """
    for parent in repo_root.parents:
        if not parent.stat().st_mode & stat.S_IXOTH:
            raise OSError(f"{user} cannot reach the checkout: {parent} is not traversable")
    _sudo("chown", "-R", "-h", "-P", "--", f"{user}:", str(repo_root))
    try:
        yield
    finally:
        _kill_all(user)
        _sudo("chown", "-R", "-h", "-P", "--", f"{os.getuid()}:{os.getgid()}", str(repo_root))


def _kill_all(user: str) -> None:
    deadline = time.monotonic() + 30
    while True:
        subprocess.run(["sudo", "-n", "pkill", "-KILL", "-u", user], capture_output=True, check=False)
        if subprocess.run(["pgrep", "-u", user], capture_output=True, check=False).returncode == 1:
            return
        if time.monotonic() > deadline:
            raise OSError(f"processes of {user} survived the verification run")
        time.sleep(0.2)


def _sudo(*argv: str) -> None:
    result = subprocess.run(["sudo", "-n", *argv], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise OSError(f"sudo {' '.join(argv)} failed: {result.stderr.strip()}")


def _resolve_workdir(repo_dir: str, workdir: str) -> Path | None:
    """Resolve ``workdir`` under ``repo_dir``, rejecting any escape."""
    repo_root = Path(repo_dir).resolve()
    if not repo_root.is_dir():
        return None
    candidate = (repo_root / workdir).resolve() if workdir else repo_root
    if repo_root != candidate and repo_root not in candidate.parents:
        return None
    if not candidate.is_dir():
        return None
    return candidate


def _tail(text: str) -> str:
    text = strip_ansi(text)
    if len(text) <= _OUTPUT_TAIL_CHARS:
        return text
    return "…[truncated]…\n" + text[-_OUTPUT_TAIL_CHARS:]


def _run_capped(
    argv: list[str], cwd: Path, env: dict[str, str], timeout: int
) -> tuple[bool, int, str, bool]:
    """Run ``argv`` capturing at most ``_MAX_CAPTURED_BYTES`` of tail output.

    Merges stdout and stderr and keeps only the trailing bytes, so a command
    that floods output cannot exhaust memory. The deadline is enforced with
    ``select`` so a silent long-running command is still killed on time.
    Returns ``(ran, exit_code, output, timed_out)``. Raises ``OSError`` if the
    process cannot start.
    """
    proc = subprocess.Popen(
        argv,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        # Its own process group, so what it started dies with it.
        start_new_session=True,
    )
    assert proc.stdout is not None
    fd = proc.stdout
    buf = bytearray()
    deadline = time.monotonic() + timeout
    timed_out = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            ready, _, _ = select.select([fd], [], [], min(remaining, 1.0))
            if not ready:
                continue
            chunk = fd.read1(65536) if hasattr(fd, "read1") else fd.read(65536)
            if not chunk:
                break
            buf.extend(chunk)
            if len(buf) > _MAX_CAPTURED_BYTES:
                del buf[:-_MAX_CAPTURED_BYTES]
    finally:
        fd.close()
        if timed_out:
            _kill_group(proc.pid)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _kill_group(proc.pid)
            proc.wait()
        _kill_group(proc.pid)  # anything it left running in the background
    return True, proc.returncode if proc.returncode is not None else -1, \
        buf.decode("utf-8", errors="replace"), timed_out


def _kill_group(pgid: int) -> None:
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _verification_shell() -> tuple[str, ...]:
    """Return the local shell used for non-container verification commands."""
    bash = shutil.which("bash")
    if bash:
        return (bash, "-c")
    return ("/bin/sh", "-c")
