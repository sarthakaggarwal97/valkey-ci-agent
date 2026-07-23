"""Find an existing upstream fix that may be missing from a release PR.

The repository's default branch is authoritative and is searched first
(``unstable`` for Valkey core). Configured release branches are a rare fallback:
they are searched only when the default branch yields no candidate.

The model decides whether a surfaced commit addresses the observed root cause.
Code owns provenance: only commits emitted here may use the PORT path, and the
publisher proves reachability from the recorded source ref before cherry-pick.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from scripts.common.proc import git_output, run_git

logger = logging.getLogger(__name__)

DEFAULT_BRANCH_FALLBACK = "unstable"
_MAX_LOG_BYTES = 2 * 1024 * 1024
_MAX_FILE_LOG_BYTES = 256 * 1024
_MAX_TERMS = 16
_MAX_CANDIDATES = 12
_MAX_SCANNED_PER_REF = 40

_PATH_RE = re.compile(r"(?:[A-Za-z0-9_.@+-]+/)+[A-Za-z0-9_.@+-]+")
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{4,}")
_FAILURE_LINE_RE = re.compile(
    r"\[err\]|\[exception\]|\bfailed\b|\bfailure\b|\bfatal\b|"
    r"\berror:|\bassert(?:ion)?\b|\btimeout\b|\btimed out\b|"
    r"\bsanitizer\b|\bsegmentation fault\b|\bpanic\b",
    re.IGNORECASE,
)
_NOISY_TOKENS = frozenset({
    "action", "actions", "branch", "build", "command", "error", "failed",
    "failure", "fatal", "github", "install", "output", "python", "runner",
    "tests", "verbose", "warning",
})


@dataclass(frozen=True)
class PortCandidate:
    """A commit from trusted remote history that may fix this failure."""

    sha: str
    subject: str
    paths: tuple[str, ...] = ()
    source_ref: str = ""
    source_branch: str = ""


def discover_port_candidates(
    repo_dir: str,
    logs_dir: str,
    *,
    default_branch: str = "",
    history_branches: tuple[str, ...] = (),
    max_candidates: int = _MAX_CANDIDATES,
) -> tuple[PortCandidate, ...]:
    """Return default-branch candidates, with release history as a fallback.

    Commits already reachable from the failed PR head are excluded. Within one
    branch, commits touching paths from the failure log come first; commit
    message matches fill the remaining slots. A configured history branch is
    not fetched unless the default branch produced no candidate.
    """
    if max_candidates <= 0:
        return ()
    try:
        resolved_default = default_branch or resolve_default_branch(repo_dir)
    except (subprocess.CalledProcessError, OSError) as exc:
        logger.info("Could not resolve default branch for candidate search: %s", exc)
        resolved_default = DEFAULT_BRANCH_FALLBACK

    log_text = _read_logs(logs_dir)
    paths = _extract_repo_paths(repo_dir, log_text)
    terms = _distinctive_terms(log_text)

    primary = _candidates_for_branch(
        repo_dir,
        resolved_default,
        paths=paths,
        terms=terms,
        limit=max_candidates,
    )
    if primary:
        return primary

    fallback: dict[str, PortCandidate] = {}
    for branch in _dedupe(history_branches):
        if branch == resolved_default:
            continue
        remaining = max_candidates - len(fallback)
        if remaining <= 0:
            break
        for candidate in _candidates_for_branch(
            repo_dir,
            branch,
            paths=paths,
            terms=terms,
            limit=remaining,
        ):
            fallback.setdefault(candidate.sha, candidate)
    return tuple(fallback.values())


def format_port_candidates(candidates: tuple[PortCandidate, ...]) -> str:
    """Render code-discovered candidates for the diagnosis prompt."""
    if not candidates:
        return ""
    lines = [
        "## Existing fix candidates (code-discovered)",
        "The repository default branch is searched first. Configured release "
        "branches are used only when the default branch yields no candidate. "
        "Prefer `path: \"port\"` only when a commit clearly fixes the same root "
        "cause without missing prerequisites.",
    ]
    for candidate in candidates:
        branch = candidate.source_branch or candidate.source_ref or "default branch"
        paths = f"; paths: {', '.join(candidate.paths[:4])}" if candidate.paths else ""
        lines.append(
            f"- {candidate.sha[:12]} [{branch}{paths}] "
            f"{candidate.subject}"
        )
    return "\n".join(lines) + "\n"


def resolve_default_branch(repo_dir: str) -> str:
    """Resolve the remote's default branch, e.g. ``main`` or ``unstable``."""
    try:
        ref = git_output(repo_dir, "rev-parse", "--abbrev-ref", "origin/HEAD").strip()
        if ref.startswith("origin/"):
            return ref[len("origin/"):]
    except subprocess.CalledProcessError:
        pass
    try:
        out = git_output(repo_dir, "remote", "show", "origin")
        for line in out.splitlines():
            if "HEAD branch:" in line:
                return line.split("HEAD branch:", 1)[1].strip()
    except subprocess.CalledProcessError:
        pass
    return DEFAULT_BRANCH_FALLBACK


def _ensure_branch_ref(repo_dir: str, branch: str) -> str:
    if not _valid_branch_name(branch):
        raise ValueError(f"malformed history branch {branch!r}")
    ref = f"origin/{branch}"
    try:
        git_output(repo_dir, "rev-parse", "--verify", ref)
        return ref
    except subprocess.CalledProcessError:
        run_git(
            repo_dir, "fetch", "--no-tags", "origin",
            f"refs/heads/{branch}:refs/remotes/origin/{branch}",
        )
        return ref


def _valid_branch_name(branch: str) -> bool:
    return (
        bool(re.fullmatch(r"[A-Za-z0-9._/-]+", branch))
        and ".." not in branch
        and not branch.startswith("/")
        and not branch.endswith(("/", "."))
        and "//" not in branch
    )


def _candidates_for_branch(
    repo_dir: str,
    branch: str,
    *,
    paths: tuple[str, ...],
    terms: tuple[str, ...],
    limit: int,
) -> tuple[PortCandidate, ...]:
    """Return path-first candidates from one trusted remote branch."""
    if limit <= 0:
        return ()
    try:
        ref = _ensure_branch_ref(repo_dir, branch)
    except (subprocess.CalledProcessError, OSError, ValueError) as exc:
        logger.info("Skipping fix search on branch %s: %s", branch, exc)
        return ()

    records: dict[str, str] = {}
    for sha, subject in _path_history(repo_dir, ref, paths):
        records.setdefault(sha, subject)
        if len(records) >= limit:
            break
    if len(records) < limit:
        for sha, subject in _message_history(repo_dir, ref, terms):
            records.setdefault(sha, subject)
            if len(records) >= limit:
                break

    return tuple(
        PortCandidate(
            sha=sha,
            subject=subject,
            paths=_changed_paths(repo_dir, sha),
            source_ref=ref,
            source_branch=branch,
        )
        for sha, subject in records.items()
    )


def _read_logs(logs_dir: str) -> str:
    """Read bounded failure-focused slices from all downloaded step logs."""
    root = Path(logs_dir)
    if not root.is_dir():
        return ""
    chunks: list[str] = []
    remaining = _MAX_LOG_BYTES
    for path in sorted(
        p for p in root.iterdir() if not p.is_symlink() and p.is_file()
    ):
        if remaining <= 0:
            break
        try:
            limit = min(_MAX_FILE_LOG_BYTES, remaining)
            with path.open("rb") as handle:
                size = os.fstat(handle.fileno()).st_size
                if size > limit:
                    handle.seek(-limit, os.SEEK_END)
                payload = handle.read(limit)
        except OSError:
            continue
        text = payload.decode("utf-8", errors="replace")
        chunks.append(_failure_context(text))
        remaining -= len(payload)
    return "\n".join(chunks)


def _failure_context(text: str) -> str:
    lines = text.splitlines()
    selected: set[int] = set()
    for index, line in enumerate(lines):
        if not _FAILURE_LINE_RE.search(line):
            continue
        selected.update(range(max(0, index - 2), min(len(lines), index + 3)))
    if not selected:
        return "\n".join(lines[-200:])
    return "\n".join(lines[index] for index in sorted(selected))


def _extract_repo_paths(repo_dir: str, text: str) -> tuple[str, ...]:
    root = Path(repo_dir)
    found: set[str] = set()
    for match in _PATH_RE.findall(text):
        path = match.strip("`'\".,:;()[]{}")
        # Strip a trailing line/column suffix without damaging filenames that
        # legitimately contain digits.
        path = re.sub(r":\d+(?::\d+)?$", "", path)
        if path and (root / path).exists():
            found.add(path)
    return tuple(sorted(found))


def _distinctive_terms(text: str) -> tuple[str, ...]:
    frequencies: dict[str, tuple[str, int]] = {}
    for raw in _TOKEN_RE.findall(text):
        normalized = raw.lower().replace("-", "").replace("_", "")
        if normalized in _NOISY_TOKENS:
            continue
        original, count = frequencies.get(normalized, (raw, 0))
        frequencies[normalized] = (original, count + 1)
    ranked = sorted(
        frequencies.values(),
        key=lambda item: (-item[1], -len(item[0]), item[0].lower()),
    )
    return tuple(term for term, _count in ranked[:_MAX_TERMS])


def _path_history(
    repo_dir: str, ref: str, paths: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    if not paths:
        return ()
    try:
        out = git_output(
            repo_dir,
            "log",
            "--no-merges",
            f"--max-count={_MAX_SCANNED_PER_REF}",
            "--format=%H%x00%s",
            ref,
            "--not",
            "HEAD",
            "--",
            *paths,
        )
    except subprocess.CalledProcessError:
        return ()
    return _parse_log_lines(out)


def _message_history(
    repo_dir: str, ref: str, terms: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    if not terms:
        return ()
    args = [
        "log",
        "--no-merges",
        f"--max-count={_MAX_SCANNED_PER_REF}",
        "--format=%H%x00%s",
        "--regexp-ignore-case",
    ]
    args.extend(f"--grep={term}" for term in terms)
    args.extend((ref, "--not", "HEAD"))
    try:
        return _parse_log_lines(git_output(repo_dir, *args))
    except subprocess.CalledProcessError:
        return ()


def _parse_log_lines(output: str) -> tuple[tuple[str, str], ...]:
    result = []
    for line in output.splitlines():
        sha, separator, subject = line.partition("\0")
        if separator and re.fullmatch(r"[0-9a-fA-F]{40}", sha):
            result.append((sha, subject))
    return tuple(result)


def _changed_paths(repo_dir: str, sha: str) -> tuple[str, ...]:
    try:
        out = git_output(
            repo_dir, "diff-tree", "--no-commit-id", "--name-only", "-r", sha,
        )
    except subprocess.CalledProcessError:
        return ()
    return tuple(path for path in out.splitlines() if path)


def _dedupe(items: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item for item in items if item))
