"""Sanitized execution of AI-proposed verification commands.

The AI proposes *how* to build and run a single failing test; this module
decides *whether it passed*. That split is the trust anchor of the whole
workflow: a passing verdict can only come from a real subprocess exit code,
never from anything the model says.

Hardening applied to every command:

- **No-secret environment.** The command gets only process basics (PATH/HOME/
  locale/CA). No GitHub token and — critically — no AWS credentials: the
  command is untrusted PR code and must never be able to read the Bedrock
  role used by the AI layer.
- **Working directory locked** to the cloned repo; the resolved path must stay
  inside it.
- **Timeout** so a hung test can't wedge the job.
- **Output cap** so a runaway log can't exhaust the runner; only the tail is
  retained (the failure summary is almost always at the end of a test run).
- **Run via ``/bin/sh -c``** because real build+test recipes chain with
  ``&&``/``;``/pipes.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from scripts.ci_fix.models import RunResult
from scripts.common.proc import filter_env

logger = logging.getLogger(__name__)

# The verification command is untrusted PR code. It gets ONLY enough to run a
# build/test — never any credential. Notably this excludes the AWS variables
# that the Claude subprocess needs (those stay in the AI layer), so a test
# cannot read the Bedrock role credentials.
_VERIFY_ENV_ALLOWLIST = (
    "PATH", "HOME", "TMPDIR", "TMP", "USER", "LOGNAME", "LANG", "LC_ALL",
    "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE",
)

_DEFAULT_TIMEOUT_S = 30 * 60
_OUTPUT_TAIL_CHARS = 32 * 1024


def run_verification_command(
    repo_dir: str,
    command: str,
    *,
    workdir: str = "",
    timeout: int = _DEFAULT_TIMEOUT_S,
    env_allowlist: tuple[str, ...] = _VERIFY_ENV_ALLOWLIST,
) -> RunResult:
    """Run ``command`` in ``repo_dir`` and report its real verdict.

    ``command`` is the AI-proposed build+test recipe (commonly chained with
    ``&&``), run via ``/bin/sh -c`` with a scrubbed environment, locked to a
    working directory inside the clone. ``passed`` is ``exit_code == 0`` — the
    subprocess decides, not the caller and not the AI.

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

    env = filter_env(env_allowlist)
    logger.info("Running verification command in %s (timeout=%ds): %s", cwd, timeout, command)
    try:
        result = subprocess.run(
            ["/bin/sh", "-c", command],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        tail = _tail(_decode(exc.stdout) + _decode(exc.stderr))
        logger.warning("Verification command timed out after %ds", timeout)
        return RunResult(
            ran=True, passed=False, exit_code=-1, command=command,
            output_tail=tail or f"timed out after {timeout}s", timed_out=True,
        )
    except OSError as exc:
        logger.warning("Verification command failed to start: %s", exc)
        return RunResult(
            ran=False, passed=False, exit_code=-1, command=command,
            output_tail=f"failed to start: {exc}",
        )

    tail = _tail(result.stdout + result.stderr)
    passed = result.returncode == 0
    logger.info("Verification command exited %d (passed=%s)", result.returncode, passed)
    return RunResult(
        ran=True, passed=passed, exit_code=result.returncode,
        command=command, output_tail=tail,
    )


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
    if len(text) <= _OUTPUT_TAIL_CHARS:
        return text
    return "…[truncated]…\n" + text[-_OUTPUT_TAIL_CHARS:]


def _decode(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)
