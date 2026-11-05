"""Walk every merge of Valkey and its modules through the fuzzer, newest first, then every new one.

A run walks one repository: valkey ``unstable`` or a module in
:data:`~scripts.fuzzer.pr_batch.MODULES`, each with a state file of its own.
``plan`` reads it and lists the merges the run processes: the merges after
the forward head, oldest first, then the merges to retry, then backlog
merges from the cursor back toward the end (valkey: where ``unstable`` left
7.2, a module: its first commit). A merge that changes only docs, CI or
tests is done without a job (``no-code-change``).

``build`` builds a head for the whole run: ``unstable`` with ASan and its
tests/modules, or a module's default branch against it.

``run`` processes one merge, in its own job: it builds the merge and its
parent (for a module, both loaded into the run's ``unstable`` build), asks
Claude for as many tests as the change's size needs
(:func:`~scripts.fuzzer.propose.propose`), turns every check on, and runs
each test on three builds: the merge against its parent, the parent against
the merge, and ``unstable`` against the parent, on several seeds. A crash, a
sanitizer report or a check failure is filed only when it shows on
``unstable`` and again on one rerun, and so is a test that reached the
change at the merge but no longer does on ``unstable`` (a fix that
regressed). The rest is recorded. Issues go to the fuzzer repository, one
per signature: a repeat adds an occurrence. For a Valkey merge, the tests
then run once more on a gcov build of the merge, and while they leave too
many of its changed lines unrun, more rounds ask Claude for tests that reach
them (:func:`cover`).

``update`` folds a run's results into its repository's state on the state
branch, copies each merge's proven tests beside it under ``tests/``,
writes ``PROGRESS.md`` from every repository's state, and pushes, again
from a fresh fetch when another repository's run pushed first.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ai.runtime import AgentRunResult, run_agent
from scripts.common.incidents import compute_fingerprint
from scripts.common.issue_dedup import IssueContent, IssueDedupPublisher
from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.common.proc import PROCESS_BASICS, filter_env
from scripts.common.text_utils import strip_ansi
from scripts.fuzzer.analyzer import (
    _ANOMALY_PATTERNS,
    _CRASH_TITLES,
    SEQUENCE_EVIDENCE,
    TOOL_RUNS_EVIDENCE,
    UNEXPLAINED,
    UNREACHED,
    _change_outcome,
)
from scripts.fuzzer.batch import report_problem
from scripts.fuzzer.pr_batch import MODULES, module_repository
from scripts.fuzzer.propose import (
    VALKEY_REPO,
    ChangeRequest,
    ProposalRun,
    fetch_change,
    fuzzer_lister,
    fuzzer_validator,
    load_catalog,
    propose,
    validated_scenarios,
    write_results,
)
from scripts.fuzzer.replay import _PR_SUBJECT_RE

logger = logging.getLogger(__name__)

VALKEY_BRANCH = "unstable"
# git merge-base unstable 7.2.4: where unstable left 7.2, the end of Valkey's backlog.
VALKEY_BASE = "f4549d1cf4830c0584f8e41369a8628842a67aec"
# Valkey's first commit on unstable ("A single commit to get stuff building", 2024-03-21), right after Redis's
# #13154: the merges before it are Redis's, and their (#N) are redis/redis pull requests.
VALKEY_FORK = "38632278fd06fe186f7707e4fa099f666d805547"
REDIS_REPO = "redis/redis"
# The repositories walked, by name: valkey-io/<name>.
NAMES = ("valkey", *MODULES)
STATE_BRANCH = "walk-state"
# Each repository's state on the state branch: state/<name>.json.
STATE_DIR = "state"
# A push the state branch rejects (another repository's run pushed first) is redone this often.
MAX_PUSHES = 5
PROGRESS_FILE = "PROGRESS.md"
RESULT_FILE = "result.json"
TESTS_DIR = "tests"
# GitHub runs at most this many jobs from one matrix.
MAX_JOBS = 256
# A merge whose run fails this often is given up and recorded with its last error; one whose job leaves no
# result at all (it never ran, or the runner was lost) is given up after MAX_MISSES runs.
MAX_ATTEMPTS = 3
MAX_MISSES = 10
# Seeds each test runs with on unstable: its own and the next ones.
HUNT_SEEDS = 3
RUN_TIMEOUT_S = 900
# What Claude is told when its timeout cuts it off, in the same session, and how long it has to answer.
TIME_UP_PROMPT = ("Your time is up. Stop reading and answer now with what you have: the one JSON object the first "
                  "message asks for, or its \"reason\" alone if you found no test worth writing.")
TIME_UP_TIMEOUT_S = 600
# The tests asked for: MIN_TESTS for a change of a few lines, one more per 50 changed lines of code and per file
# of code past the first, at most MAX_TESTS.
MIN_TESTS, MAX_TESTS = 3, 15
# Changed-line coverage, of Valkey merges for now: while the tests run fewer than COVERAGE_TARGET percent of the
# merge's changed lines, up to COVERAGE_ROUNDS more rounds ask for tests that reach the rest. A round starts only
# within ROUNDS_WITHIN_S of the walk's start, so the job's time holds it. The first UNCOVERED_KEPT lines no test
# ran are kept, each shown to Claude with CONTEXT_LINES of code around it.
COVERAGE_TARGET = 80
COVERAGE_ROUNDS = 3
ROUNDS_WITHIN_S = 3 * 3600
UNCOVERED_KEPT = 50
CONTEXT_LINES = 3
MERGE_KEYS = ("repo", "sha", "parent", "pr", "pr_repo", "subject", "date")
# The checks the walk turns on in every compared command sequence.
CHECKS = {"variants": True, "replica": True, "acl": True, "concurrency": 4, "compat": True}
# The diagnostics.reason a failed check gives.
CHECK_REASONS = frozenset({
    "variant-mismatch", "replica-diverged", "acl-bypass", "acl-overreach", "compat-load", "compat-sync", "hang",
    # The client could not parse a reply; the diagnostics hold its raw bytes.
    "protocol-error",
})
MARKER_NAMESPACE = "valkey-ci-agent:fuzzer-walk"
VALKEY_TOOLS = ("valkey-server", "valkey-cli", "valkey-benchmark", "valkey-check-rdb", "valkey-check-aof")
_CRASH_PATTERNS = [(title, pattern) for title, _, pattern, _ in _ANOMALY_PATTERNS if title in _CRASH_TITLES]
_SANITIZER_RE = re.compile(r"SUMMARY: \w*Sanitizer: [^\n]*")
_ASSERTION_RE = re.compile(r"ASSERTION FAILED[^\n]*\n([^\n]*)")
_SIGNAL_RE = re.compile(r"crashed by signal[^\n]*")
_FRAME_RE = re.compile(r"#\d+ 0x[0-9a-f]+ in (\S+)")
_LIBRARY_FRAME_RE = re.compile(r"alloc|Alloc|operator new|^rapidjson::|^std::|^__|^_Z")
_EIP_RE = re.compile(r"EIP:\s*\n[^\n(]*\((\w+)")
_FIX_RE = re.compile(r"\bfix", re.IGNORECASE)
_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)")


def _git(repo_dir: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo_dir), *args], check=True, capture_output=True, text=True).stdout


def _name(repo: str) -> str:
    return repo.split("/")[-1]


def _branch(repo: str) -> str:
    return VALKEY_BRANCH if repo == VALKEY_REPO else MODULES[_name(repo)].branch


def _pr_repo(merge: dict[str, Any]) -> str:
    """The repository the merge's pull request is in: its own, or Redis's before Valkey forked."""
    return str(merge.get("pr_repo") or merge["repo"])


def _short(merge: dict[str, Any]) -> str:
    """How a verdict names the merge: #PR (redis/redis#PR before the fork), or its commit."""
    if not merge.get("pr"):
        return str(merge["sha"])[:12]
    return f"#{merge['pr']}" if _pr_repo(merge) == merge["repo"] else f"{_pr_repo(merge)}#{merge['pr']}"


def _where(merge: dict[str, Any]) -> str:
    """How an issue or PROGRESS.md names the merge: OWNER/NAME#PR, or the repository and its commit."""
    return f"{_pr_repo(merge)}#{merge['pr']}" if merge.get("pr") else f"{merge['repo']}@{merge['sha'][:12]}"


def _pr_url(merge: dict[str, Any]) -> str:
    """The merge's pull request, or its commit."""
    if merge.get("pr"):
        return f"https://github.com/{_pr_repo(merge)}/pull/{merge['pr']}"
    return f"https://github.com/{merge['repo']}/commit/{merge['sha']}"


def merge_folder(merge: dict[str, Any]) -> str:
    """Where a merge's proven tests are kept: tests/<repository name>/<pr>-<short sha>."""
    folder = f"{merge['pr']}-{merge['sha'][:9]}" if merge.get("pr") else merge["sha"][:9]
    return f"{TESTS_DIR}/{_name(merge['repo'])}/{folder}"


# --- Plan ----------------------------------------------------------------------


def first_parent(repo_dir: Path, repo: str) -> list[dict[str, Any]]:
    """The commits on the checked-out branch's first-parent history, newest first."""
    history = []
    redis = False
    for line in _git(repo_dir, "log", "--first-parent", "--format=%H%x09%P%x09%cs%x09%s", "HEAD").splitlines():
        sha, parents, date, subject = line.split("\t", 3)
        match = _PR_SUBJECT_RE.search(subject)
        history.append({"repo": repo, "sha": sha, "parent": parents.split(" ")[0], "pr": int(match.group(1))
                        if match else None, "pr_repo": REDIS_REPO if redis else repo, "subject": subject[:200],
                        "date": date})
        # Older than Valkey's first commit: Redis's merges.
        redis = redis or (repo == VALKEY_REPO and sha == VALKEY_FORK)
    return history


def _code(path: str, repo: str) -> bool:
    """Whether a changed path is code: not docs (``*.md``, ``docs/``), CI (``.github/``) or tests (``tests/``,
    but Valkey's tests/modules, and ``integration/``)."""
    if path.endswith(".md") or path.startswith(("docs/", ".github/", "integration/")):
        return False
    return not path.startswith("tests/") or (repo == VALKEY_REPO and path.startswith("tests/modules/"))


def no_code_change(paths: list[str], repo: str) -> bool:
    """Whether a merge changes only docs, CI or tests."""
    return not any(_code(path, repo) for path in paths)


def plan(repo_dir: Path, repo: str, state: dict[str, Any], max_jobs: int, valkey_head: str) -> dict[str, Any]:
    """The run's merges, the merges done without a job, the repository's new pointer, and the heads the run
    builds: the repository's, and for a module the Valkey ``unstable`` commit ``valkey_head`` it loads into.

    ``repo_dir`` is a full clone of ``repo`` with its default branch checked out. A repository the state does
    not know yet starts at its head: the forward head and the backlog cursor both name the newest merge, so the
    walk begins with it.
    """
    history = first_parent(repo_dir, repo)
    index = {merge["sha"]: i for i, merge in enumerate(history)}
    pointer = dict(state["pointer"] or {"head": history[0]["sha"], "cursor": history[0]["sha"],
                                        "end": VALKEY_BASE if repo == VALKEY_REPO else history[-1]["sha"]})
    for key in ("head", "cursor", "end"):
        if pointer[key] not in index:
            raise ValueError(f"the walk's {key} of {repo}, {pointer[key]}, is not on its first-parent history")
    new = [(i, "new") for i in reversed(range(index[pointer["head"]]))]
    retries = [(entry, "retry") for entry in state["failed"].values()]
    backlog = [(i, "backlog") for i in range(index[pointer["cursor"]], index[pointer["end"]])]
    jobs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for item, kind in [*new, *retries, *backlog]:
        if len(jobs) >= max_jobs:
            break
        if kind == "retry":
            jobs.append({key: item.get(key) for key in MERGE_KEYS})
            continue
        merge = history[item]
        if kind == "new":
            pointer["head"] = merge["sha"]
        else:
            pointer["cursor"] = history[item + 1]["sha"]
        if merge["sha"] in state["done"] or merge["sha"] in state["failed"]:
            continue
        if no_code_change(_git(repo_dir, "diff", "--name-only", merge["parent"], merge["sha"]).split(), repo):
            skipped.append(merge)
        else:
            jobs.append(merge)
    cursor = history[index[pointer["cursor"]]]
    pointer.update(remaining=index[pointer["end"]] - index[pointer["cursor"]], cursor_pr=cursor["pr"],
                   cursor_date=cursor["date"])
    # The workflow's names for the job's builds and cache.
    extra = {"name": _name(repo), "deps": MODULES[_name(repo)].deps if module_repository(repo) else ""}
    heads = {repo: history[0]["sha"]} if repo == VALKEY_REPO else {VALKEY_REPO: valkey_head, repo: history[0]["sha"]}
    return {"repo": repo, "merges": [{**job, **extra} for job in jobs], "skipped": skipped, "pointer": pointer,
            "heads": heads}


def clone(repo: str, dest: Path) -> Path:
    """A clone of ``repo``'s default branch with its whole history, without the files' contents."""
    subprocess.run(["git", "clone", "-q", "--filter=blob:none", "--single-branch", "--branch", _branch(repo),
                    f"https://github.com/{repo}.git", str(dest)], check=True, capture_output=True, text=True)
    return dest


def remote_head(repo: str) -> str:
    """The commit ``repo``'s default branch points at."""
    out = subprocess.run(["git", "ls-remote", "--exit-code", f"https://github.com/{repo}.git",
                          f"refs/heads/{_branch(repo)}"], check=True, capture_output=True, text=True).stdout
    return out.split()[0]


# --- Builds --------------------------------------------------------------------


def _sh(command: str, cwd: Path, log: Path, env: dict[str, str] | None = None) -> None:
    """Run bash ``command`` in ``cwd``, its output in ``log``; on failure, raise with the output's tail."""
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as out:
        # The repositories' own build scripts run without the job's tokens and credentials.
        done = subprocess.run(["bash", "-euo", "pipefail", "-c", command], cwd=cwd, stdout=out,
                              stderr=subprocess.STDOUT,
                              env={**filter_env((*PROCESS_BASICS, "CARGO_HOME", "RUSTUP_HOME")), **(env or {})})
    if done.returncode != 0:
        tail = "\n".join(log.read_text(errors="replace").splitlines()[-30:])
        raise RuntimeError(f"`{command.splitlines()[0]}` failed in {cwd.name} (exit {done.returncode}):\n{tail}")


def fetch(repo: str, sha: str, dest: Path) -> Path:
    """``repo`` at commit ``sha`` in ``dest``, without history."""
    dest.mkdir(parents=True, exist_ok=True)
    for args in (["init", "-q"], ["fetch", "-q", "--depth", "1", f"https://github.com/{repo}.git", sha],
                 ["checkout", "-q", "FETCH_HEAD"]):
        _git(dest, *args)
    if _git(dest, "rev-parse", "HEAD").strip() != sha:
        raise RuntimeError(f"{repo} at {sha} checked out another commit")
    return dest


def build_valkey(src: Path, *, asan: bool = False, coverage: bool = False) -> Path:
    """Build Valkey in ``src`` as the fuzzer's PR batch does, with ASan, or for gcov (Valkey's ``make gcov``, at -O0
    so each line keeps its code); the server it built."""
    nproc = os.cpu_count() or 2
    flags = " CC=clang OPT=-O3 SANITIZER=address" if asan else " gcov OPTIMIZATION=-O0" if coverage else ""
    _sh(f"make -j{nproc} BUILD_ZSTD=yes{flags}", src, src.parent / f"{src.name}-build.log")
    # A commit from before the rename builds redis-server.
    server = src / "src" / "valkey-server"
    return server if server.exists() else src / "src" / "redis-server"


def build_test_modules(src: Path) -> None:
    _sh(f"make -C tests/modules -j{os.cpu_count() or 2}", src, src.parent / f"{src.name}-modules.log")


def setup_module(name: str, work: Path) -> None:
    """What the module's builds need on the runner, once per job."""
    module = MODULES[name]
    if module.setup:
        _sh(module.setup, work, work / f"{name}-setup.log",
            {"DEPS_DIR": os.environ.get("DEPS_DIR", str(work / "deps")), "DEPS_VERSION": module.deps})


def build_module(name: str, src: Path, valkey_src: Path) -> Path:
    """Build the module checked out in ``src`` against the Valkey checkout ``valkey_src``; the module it built."""
    module = MODULES[name]
    _sh(module.build, src, src.parent / f"{src.name}-build.log", {"VALKEY_SRC": str(valkey_src.resolve())})
    built = src / module.file
    if not built.is_file():
        raise RuntimeError(f"building {name} left no {module.file}")
    return built


def build_head(repo: str, sha: str, valkey_sha: str, work: Path, out: Path) -> None:
    """Build ``repo``'s head for a whole run, into the tarball ``out``: unstable with ASan, its tools, module
    header and tests/modules; or a module's default branch, against unstable."""
    with tarfile.open(out, "w:gz") as tar:
        if repo == VALKEY_REPO:
            src = fetch(repo, sha, work / "valkey")
            build_valkey(src, asan=True)
            build_test_modules(src)
            paths = [src / "src" / tool for tool in VALKEY_TOOLS] + [src / "src" / "valkeymodule.h"]
            paths += sorted((src / "tests" / "modules").glob("*.so"))
            for path in paths:
                if path.exists() or path.is_symlink():
                    tar.add(path, arcname=str(path.relative_to(src)))
        else:
            if not valkey_sha:
                raise ValueError(f"building {repo} needs the Valkey commit it builds against")
            name = _name(repo)
            setup_module(name, work)
            built = build_module(name, fetch(repo, sha, work / name), fetch(VALKEY_REPO, valkey_sha, work / "valkey"))
            tar.add(built, arcname=MODULES[name].file)


# --- Coverage ------------------------------------------------------------------


def diff_lines(diff: str) -> tuple[dict[str, list[int]], dict[str, int]]:
    """Each file a diff changes: the numbers of its added lines in the new file, and how many lines it removes."""
    added: dict[str, list[int]] = {}
    removed: dict[str, int] = {}
    path, line, hunk = "", 0, False
    for text in diff.splitlines():
        if text.startswith("diff --git "):
            path, hunk = text.rsplit(" b/", 1)[-1], False
            added[path], removed[path] = [], 0
        elif text.startswith("@@") and path:
            match = _HUNK_RE.match(text)
            line, hunk = (int(match.group(1)), True) if match else (0, False)
        elif not hunk:
            continue
        elif text.startswith("+"):
            added[path].append(line)
            line += 1
        elif text.startswith("-"):
            removed[path] += 1
        elif not text.startswith("\\"):
            line += 1
    return added, removed


def budget(diff: str, repo: str) -> int:
    """How many tests a merge is asked for, from its diff's files of code (see MIN_TESTS)."""
    added, removed = diff_lines(diff)
    code = [path for path in added if _code(path, repo)]
    lines = sum(len(added[path]) + removed[path] for path in code)
    return min(MAX_TESTS, MIN_TESTS + lines // 50 + max(len(code) - 1, 0))


def coverage_lines(diff: str) -> dict[str, list[int]]:
    """The changed lines coverage measures: the added lines of Valkey's C files under src/, but its unit tests'."""
    added, _ = diff_lines(diff)
    return {path: lines for path, lines in added.items()
            if lines and path.startswith("src/") and path.endswith(".c") and not path.startswith("src/unit/")}


def gcov_counts(text: str) -> dict[int, int]:
    """How often each executable line ran, from a ``.gcov`` file: ``-`` is a line of no code, ``#####`` and
    ``=====`` never ran, ``N`` (or ``N*``, some of its blocks never ran) ran N times."""
    counts = {}
    for row in text.splitlines():
        parts = row.split(":", 2)
        if len(parts) < 3 or not parts[1].strip().isdigit() or parts[1].strip() == "0":
            continue
        count, number = parts[0].strip().rstrip("*"), int(parts[1])
        if count in ("#####", "====="):
            counts[number] = 0
        elif count.isdigit():
            counts[number] = int(count)
    return counts


def gcov(tree: Path, path: str) -> dict[int, int] | None:
    """How often each executable line of ``path`` ran on the coverage build in ``tree``, from every run so far;
    None when the build compiles no object of it."""
    source = tree / path
    if not source.with_suffix(".gcno").is_file():
        return None
    subprocess.run(["gcov", "-o", ".", source.name], cwd=source.parent, check=True, capture_output=True, text=True,
                   timeout=300)
    return gcov_counts((source.parent / f"{source.name}.gcov").read_text(errors="replace"))


def coverage(changed: dict[str, list[int]], counts: dict[str, dict[int, int] | None]) -> dict[str, Any]:
    """The changed lines gcov counts as code, how many of them ran, the percent (rounded down) and the first
    UNCOVERED_KEPT that never ran, as ``file:line``."""
    executable = [(path, line, (counts.get(path) or {})[line]) for path, lines in changed.items()
                  for line in lines if line in (counts.get(path) or {})]
    uncovered = [f"{path}:{line}" for path, line, ran in executable if not ran]
    executed = len(executable) - len(uncovered)
    return {"lines": len(executable), "executed": executed,
            "percent": 100 * executed // len(executable) if executable else None, "uncovered": uncovered[:UNCOVERED_KEPT]}


def uncovered_section(found: dict[str, Any], tree: Path, experiment_ids: list[str]) -> str:
    """The prompt's section for one more round: the changed lines no test ran, each with code around it."""
    shown: dict[str, set[int]] = {}
    for item in found["uncovered"]:
        path, line = item.rsplit(":", 1)
        shown.setdefault(path, set()).update(range(int(line) - CONTEXT_LINES, int(line) + CONTEXT_LINES + 1))
    blocks = []
    for path, numbers in shown.items():
        source = (tree / path).read_text(errors="replace").splitlines()
        rows: list[str] = []
        for number in sorted(n for n in numbers if 1 <= n <= len(source)):
            if rows and number - 1 not in numbers:
                rows.append("...")
            mark = ">" if f"{path}:{number}" in found["uncovered"] else " "
            rows.append(f"{mark} {number:5} | {source[number - 1]}")
        blocks.append(f"{path}:\n```c\n" + "\n".join(rows) + "\n```")
    ids = f" Experiment ids already used: {', '.join(experiment_ids)}; give new experiments other ids." \
        if experiment_ids else ""
    return ("## Changed lines no test reached\n"
            f"The tests proposed so far ran {found['executed']} of the change's {found['lines']} changed lines of code "
            f"({found['percent']}%); the lines marked > below never ran. Write new scenarios, unlike the earlier ones, "
            "that reach them: find in the code the commands, arguments, config or state that lead there." + ids
            + "\n\n" + "\n\n".join(blocks))


def cover(measure: Callable[[list[Any]], dict[str, Any]], more: Callable[[dict[str, Any]], list[Any]],
          tests: list[Any], started: float) -> dict[str, Any]:
    """The changed lines' coverage by ``tests``, and by the tests of more rounds while it is under COVERAGE_TARGET:
    at most COVERAGE_ROUNDS, each started within ROUNDS_WITHIN_S of ``started``, until one covers no new line.
    ``measure`` runs tests on the coverage build and returns the coverage of every test run on it so far; ``more``
    walks one more round and returns its tests."""
    found = {**measure(tests), "rounds": 0}
    while (found["lines"] and found["executed"] * 100 < COVERAGE_TARGET * found["lines"]
           and found["rounds"] < COVERAGE_ROUNDS and time.monotonic() - started < ROUNDS_WITHIN_S):
        tests = more(found)
        again = {**(measure(tests) if tests else found), "rounds": found["rounds"] + 1}
        if again["executed"] <= found["executed"]:
            return again
        found = again
    return found


# --- Running a test ------------------------------------------------------------


def with_checks(text: str) -> str:
    """An experiment's YAML with CHECKS on every valkey.action.commands that compares two builds: of Valkey
    (``compare_with``), or of a module (``modules_parent``), which leaves out compat, a check of two Valkey
    builds.

    It edits the YAML's nodes, not the values they load as, so every scalar keeps its text: PyYAML reads ``no``
    as false, and a command's ``no`` would come back a boolean the fuzzer refuses."""

    def child(node: yaml.Node | None, key: str) -> yaml.Node | None:
        if isinstance(node, yaml.MappingNode):
            return next((v for k, v in node.value if isinstance(k, yaml.ScalarNode) and k.value == key), None)
        return None

    root = yaml.compose(text, Loader=yaml.SafeLoader)
    components = child(root, "components")
    for component in components.value if isinstance(components, yaml.SequenceNode) else []:
        use, config = child(component, "use"), child(component, "config")
        if not (isinstance(use, yaml.ScalarNode) and use.value == "valkey.action.commands"):
            continue
        if not isinstance(config, yaml.MappingNode) or child(config, "checks") is not None:
            continue
        if child(config, "compare_with") is not None:
            checks = dict(CHECKS)
        elif child(config, "modules_parent") is not None:
            checks = {key: value for key, value in CHECKS.items() if key != "compat"}
        else:
            continue
        key = yaml.ScalarNode("tag:yaml.org,2002:str", "checks")
        config.value.append((key, yaml.compose(yaml.safe_dump(checks), Loader=yaml.SafeLoader)))
    return yaml.serialize(root, Dumper=yaml.SafeDumper)


def log_cause(text: str) -> str | None:
    """What a crash or sanitizer report in a log names: the sanitizer's summary (with the first frame outside
    the allocator for a leak), the failed assertion, or the signal and the function it hit."""
    summary = _SANITIZER_RE.search(text)
    if summary:
        cause = summary.group(0)
        if "leaked" in cause:
            # The first frame of the project's own code: a library's (rapidjson's, the allocator's) is shared by
            # every leak through it, and the byte counts change with the data, not with the bug.
            frame = next((m.group(1) for m in _FRAME_RE.finditer(text) if not _LIBRARY_FRAME_RE.search(m.group(1))),
                         None)
            cause = re.sub(r"\d+ byte\(s\) leaked in \d+ allocation\(s\)\.", "leak", cause)
            cause += f" from {frame.split('(')[0]}" if frame else ""
        return cause
    assertion = _ASSERTION_RE.search(text)
    if assertion:
        return f"assertion failed: {assertion.group(1).strip()}"
    signal = _SIGNAL_RE.search(text)
    if signal:
        eip = _EIP_RE.search(text, signal.end())
        return signal.group(0).split(",")[0] + (f" in {eip.group(1)}" if eip else "")
    return None


def findings(report: dict[str, Any], run_dir: Path) -> dict[str, dict[str, Any]]:
    """The candidate's crashes, sanitizer reports and check failures in one run, by signature.

    A crash the components name (``diagnostics.crashes``) is its cause; otherwise the candidate's logs are
    scanned, never the compared build's (``parent.*``).
    """
    found: dict[str, dict[str, Any]] = {}

    def add(rule: str, detail: str, lines: list[str], *key: str) -> None:
        fingerprint = compute_fingerprint(namespace=("fuzzer-walk", rule, *key), shapes=[detail])
        found.setdefault(fingerprint, {"rule": rule, "detail": detail[:300], "lines": lines[:20]})

    causes = []
    for result in report["outcome"]["component_results"]:
        if not isinstance(result, dict):
            continue
        diagnostics: dict[str, Any] = result["diagnostics"] if isinstance(result.get("diagnostics"), dict) else {}
        crashes = diagnostics.get("crashes")
        if isinstance(crashes, dict):
            causes += [str(cause) for side, cause in crashes.items() if side != "parent"]
        reason = diagnostics.get("reason")
        if result.get("status") in ("FAIL", "ERROR") and reason in CHECK_REASONS:
            summary = " ".join(str(result.get("summary") or "").split())
            lines = [summary]
            if reason == "protocol-error":
                # The reply's raw bytes, and what else the diagnostics say of it, for the issue.
                lines.append(json.dumps({k: v for k, v in diagnostics.items() if k != "reason"}, default=str))
            add("check", f"{reason}: {summary}", lines, str(reason))
    for cause in causes:
        add("crash", f"crash: {cause}", [cause])
    if causes:
        return found
    for path in sorted(run_dir.rglob("*.log")):
        if path.name.startswith("parent.") or path.name == "console.log":
            continue
        text = strip_ansi(path.read_text(errors="replace"))
        for title, pattern in _CRASH_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                start = text.rfind("\n", 0, match.start()) + 1
                lines = text[start:].splitlines()
                add("crash", f"{title}: {log_cause(text) or lines[0].strip()}", lines)
                break
    return found


def reached_commands(report: dict[str, Any]) -> set[tuple[str, int]]:
    """The commands (or tool runs) that reached the change in one run, as (component id, index): the
    ``reached`` lists of its sequence and tool runs evidence."""
    reached = set()
    for result in report["outcome"]["component_results"]:
        if not isinstance(result, dict):
            continue
        for evidence in result.get("evidence") or []:
            if isinstance(evidence, dict) and evidence.get("evidence_id") in (SEQUENCE_EVIDENCE, TOOL_RUNS_EVIDENCE):
                value = evidence.get("value")
                indexes = value.get("reached") if isinstance(value, dict) else None
                reached |= {(str(result.get("component_id")), int(index)) for index in indexes or []}
    return reached


def fuzz(fuzzer_bin: str, doc: dict[str, Any], seed: int, *, candidate: dict[str, Any], compare: dict[str, Any],
         core: bool, experiment_dir: Path, run_dir: Path) -> dict[str, Any]:
    """Run one test with ``candidate`` as the build under test and ``compare`` as what it compares with: each a
    ``{"server", "module"}``. The run's status, how it met the change, and its findings."""
    artifacts = {}
    if core:
        artifacts["parent"] = {"use": "valkey.artifact.local-binary", "with": {"path": str(compare["server"])}}
    if candidate.get("module"):
        artifacts["module"] = {"use": "valkey.artifact.local-module", "with": {"path": str(candidate["module"])}}
        artifacts["module_parent"] = {"use": "valkey.artifact.local-module", "with": {"path": str(compare["module"])}}
    run_dir.mkdir(parents=True, exist_ok=True)
    scenario = run_dir / "scenario.yaml"
    # The deployment's data and logs in the run's folder, where findings() reads the logs.
    deployment = {**doc["deployment"], "base_data_dir": str(run_dir / "artifacts" / "data")}
    scenario.write_text(yaml.safe_dump({**doc, "seed": seed, "deployment": deployment, "artifacts": artifacts},
                                       sort_keys=False))
    report_path = run_dir / "report.json"
    command = [fuzzer_bin, "run", str(scenario), "--valkey-binary", str(candidate["server"]), "--artifact-dir",
               str(run_dir / "artifacts"), "--timeout", str(RUN_TIMEOUT_S), "--output", str(report_path)]
    if any(experiment_dir.glob("*.yaml")):
        command += ["--experiment-dir", str(experiment_dir)]
    try:
        with (run_dir / "console.out").open("w") as out:
            subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, timeout=RUN_TIMEOUT_S + 300,
                           env=filter_env((*PROCESS_BASICS, "ASAN_OPTIONS")))
    except subprocess.TimeoutExpired:
        return {"status": None, "error": "the run timed out", "found": {}}
    try:
        report = json.loads(report_path.read_text())
    except (OSError, ValueError) as exc:
        return {"status": None, "error": f"no run report: {exc}", "found": {}}
    problem = report_problem(report)
    if problem:
        return {"status": None, "error": problem, "found": {}}
    if report["status"] == "ERROR" and not report["outcome"]["component_results"]:
        # Nothing ran: the deployment or an artifact failed before the test, so the run says nothing of it.
        return {"status": None, "error": f"the run could not start: {report['summary']}"[:1000], "found": {},
                "unstarted": True}
    outcome = _change_outcome(report)
    sequence = (outcome.sequence if outcome else None) or {}
    return {"status": report["status"], "reason": outcome.reason if outcome else None,
            "reached": bool(outcome and outcome.reached), "reached_commands": reached_commands(report),
            "parent_exited": isinstance(sequence.get("parent"), dict) and "exited" in sequence["parent"],
            "found": findings(report, run_dir / "artifacts"), "summary": " ".join(str(report["summary"]).split())[:300]}


def verdict(builds: dict[str, bool], pr: str) -> str:
    """What a finding's outcome on the parent, the merge and unstable says of the merge."""
    if builds["unstable"]:
        if builds["merge"]:
            return f"predates {pr}" if builds["parent"] else f"introduced by {pr}"
        return f"predates {pr}, which hid it" if builds["parent"] else f"appeared after {pr}"
    if builds["merge"]:
        return f"predates {pr}, since fixed" if builds["parent"] else "introduced and since fixed"
    return f"fixed by {pr}" if builds["parent"] else "not reproduced"


def files(finding: dict[str, Any]) -> bool:
    """The one filing rule: a finding is filed when unstable shows it, after a rerun, unless the change meant it."""
    return bool(finding["builds"]["unstable"]) and not finding.get("verdict", "").startswith("intended by")


_STRING_RE = re.compile(r'"((?:[^"\\]|\\.){12,})"')
_FORMAT_RE = re.compile(r"%[-+ #0-9.*]*(?:hh|h|ll|l|z|j|t|L)?[a-zA-Z]")


def intended_crash(diff: str, cause: str) -> bool:
    """Whether a crash's message is a string the change adds: a panic it raises on purpose (serverPanic's format,
    its arguments in place of %zu and the like). Assertions are not: a new assert that fires is the bug it guards."""
    for line in diff.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        for literal in _STRING_RE.findall(line):
            parts = [re.escape(part) for part in _FORMAT_RE.split(literal)]
            if sum(len(part) for part in _FORMAT_RE.split(literal)) >= 12 and re.search(".+?".join(parts), cause):
                return True
    return False


def cli_steps(doc: dict[str, Any], experiments: dict[str, str], module: str) -> list[str] | None:
    """The valkey-cli steps that replay a test, when it is one plain command sequence on one server: commands of
    text, no other connection, node, raw bytes, restart or file. None otherwise."""
    experiment_id = str(doc["experiment"]["id"])
    text = experiments.get(f"{experiment_id}.yaml")
    if text is None:
        return None
    components = [c for c in yaml.safe_load(text).get("components") or [] if isinstance(c, dict)]
    sequences = [c for c in components if c.get("use") == "valkey.action.commands"]
    if len(sequences) != 1 or not isinstance(sequences[0].get("config"), dict):
        return None
    config = sequences[0]["config"]
    if set(config) - {"compare_with", "modules", "modules_parent", "commands", "timeout_s", "server_config",
                      "checks", "resp"} or config.get("resp", 2) not in (2, 3):
        return None
    server_config = config.get("server_config") or {}
    if not isinstance(server_config, dict) or any(isinstance(v, (dict, list, bytes)) for v in server_config.values()):
        return None
    lines = ["valkey-server --port 6379" + (f" --loadmodule {module}" if module else "")
             + "".join(f" --{key} {shlex.quote(str(value))}" for key, value in server_config.items()) + " &"]
    cli = "valkey-cli -p 6379" + (" -3" if config.get("resp") == 3 else "")
    for step in config.get("commands") or []:
        if not isinstance(step, dict) or set(step) - {"command", "expect", "expect_difference", "compare"}:
            return None
        args = step.get("command")
        if not isinstance(args, list) or not all(isinstance(a, (str, int)) and not isinstance(a, bool) for a in args):
            return None
        lines.append(f"{cli} {shlex.join(str(arg) for arg in args)}")
    return lines


def render_issue(finding: dict[str, Any], merge: dict[str, Any], context: dict[str, Any]) -> Callable[[str, int], IssueContent]:
    """The issue for a finding, and the comment a repeat adds: what failed, on which builds, the scenario, how to
    reproduce it, the key log lines, and a draft of the report to file upstream."""
    repo = merge["repo"]
    where, commit = _where(merge), f"https://github.com/{repo}/commit/{merge['sha']}"
    builds = finding["builds"]
    branch = VALKEY_BRANCH if repo == VALKEY_REPO else MODULES[_name(repo)].branch
    head = context["module_head"] or context["unstable"]
    rows = [("parent", merge["parent"], builds["parent"]), (f"merge, {where}", merge["sha"], builds["merge"]),
            (f"latest {branch} (on Valkey unstable with ASan)", head, True)]
    affected = ", ".join(f"{name} `{sha[:12]}`" for name, sha, failed in rows if failed)
    reproduce = ("valkey-fuzzer run scenario.yaml --valkey-binary /path/to/valkey-server"
                 + (" --experiment-dir experiments" if context["experiment"] else "")
                 + "  # the scenario's artifacts name the compared builds")
    steps = cli_steps(context["doc"], context["experiments"], "/path/to/module.so" if context["module"] else "") \
        if finding["rule"] in ("crash", "regressed-fix") else None
    title = f"[fuzzer-walk] {finding['detail'][:100]}"
    lines = "\n".join(finding.get("lines") or [])[:4000]

    def render(marker: str, occurrences: int) -> IssueContent:
        body = [
            marker, f"<!-- {MARKER_NAMESPACE}:occurrences:{occurrences} -->", "",
            f"**{finding['rule']}**: {finding['detail']}", "",
            f"{finding['verdict'].capitalize()}: [{where}]({_pr_url(merge)}) {merge['subject']} "
            f"(commit [`{merge['sha'][:12]}`]({commit}))", "",
            "| Build | Commit | Failed |", "|---|---|---|",
            *(f"| {name} | `{sha[:12]}` | {'yes' if failed else 'no'} |" for name, sha, failed in rows), "",
            f"Reproduce: `{reproduce}`. The test's files: {context['tests_url']}", "",
            "<details><summary>Scenario</summary>", "", "```yaml", context["scenario"][:20000], "```",
            *(["", "```yaml", context["experiment"][:20000], "```"] if context["experiment"] else []),
            "</details>", "", "Key log lines:", "", "```", lines, "```", "",
            "### Upstream report (draft)", "",
            f"**Title:** {finding['detail'][:120]}", "",
            f"**Affected builds:** {affected}", "",
            f"**Since:** {finding['verdict']}", "", "**Reproduce:**", "", "```",
            *(steps or [reproduce]), "```", "", "---", "*Filed by valkey-ci-agent's fuzzer walk*",
        ]
        comment = (f"Seen again testing [{where}]({_pr_url(merge)}) on "
                   f"`{head[:12]}`: {finding['verdict']}. The test's files: {context['tests_url']}")
        return IssueContent(title=title, body="\n".join(body)[:65000], comment=comment)
    return render


def session_id(stdout: str) -> str | None:
    """The session id Claude's stream names, so a run cut off by its timeout can be resumed."""
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get("session_id"):
            return str(event["session_id"])
    return None


def claude_cost(stdout: str) -> float:
    """The cost Claude's stream reports for one run (its ``result`` events' ``total_cost_usd``)."""
    total = 0.0
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get("type") == "result":
            total += float(event.get("total_cost_usd") or 0)
    return total


def run(merge: dict[str, Any], *, gh: Any, token: str, publisher: Any, fuzzer_repo: str, fuzzer_dir: Path,
        fuzzer_bin: str, heads: dict[str, str], unstable: Path, module_head: Path | None, out: Path, work: Path,
        tests_url: str, runner: Callable[[str, Path], AgentRunResult] | None = None) -> dict[str, Any]:
    """Process one merge end to end; its result.json, beside its proven tests, in ``out``/:func:`merge_folder`."""
    repo = merge["repo"]
    core = repo == VALKEY_REPO
    folder = out / merge_folder(merge)
    folder.mkdir(parents=True, exist_ok=True)
    module_head_sha = "" if core else heads[repo]
    result: dict[str, Any] = {
        **{key: merge.get(key) for key in MERGE_KEYS}, "unstable": heads[VALKEY_REPO],
        "module_head": module_head_sha or None, "outcome": "tested", "proposed": 0, "proven": 0, "hunted": 0,
        "dropped": 0, "recall": False, "coverage": None, "cost_usd": 0.0, "findings": [], "recorded": [],
        "error": "the run did not finish",
    }
    cost = [0.0]

    def save() -> None:
        result["cost_usd"] = round(cost[0], 4)
        (folder / RESULT_FILE).write_text(json.dumps(result, indent=2), encoding="utf-8")

    def agent(prompt: str, workdir: Path, resume: str | None = None) -> AgentRunResult:
        if runner:
            return runner(prompt, workdir)
        return run_agent("fuzzer_proposal_readonly", prompt, cwd=str(workdir), resume=resume,
                         timeout=TIME_UP_TIMEOUT_S if resume else None)

    def ask(prompt: str, workdir: Path) -> AgentRunResult:
        answer = agent(prompt, workdir)
        cost[0] += claude_cost(answer.stdout)
        # A long reading of a complex change can run out the clock before it answers: what it read is kept.
        session = session_id(answer.stdout) if answer.stderr.startswith("timeout") else None
        if session:
            logger.warning("Claude ran out of time; asking it to answer with what it has")
            answer = agent(TIME_UP_PROMPT, workdir, resume=session)
            cost[0] += claude_cost(answer.stdout)
        return answer

    save()
    try:
        _walk(merge, result, ask, gh=gh, token=token, publisher=publisher, fuzzer_repo=fuzzer_repo,
              fuzzer_dir=fuzzer_dir, fuzzer_bin=fuzzer_bin, heads=heads, unstable=unstable,
              module_head=module_head, folder=folder, work=work, tests_url=tests_url)
        del result["error"]
    except Exception as exc:
        logger.warning("The walk of %s failed: %s", merge["sha"], exc, exc_info=True)
        result["error"] = " ".join(str(exc).split())[:1000]
    finally:
        save()
    return result


def _walk(merge: dict[str, Any], result: dict[str, Any], ask: Callable[[str, Path], AgentRunResult], *, gh: Any,
          token: str, publisher: Any, fuzzer_repo: str, fuzzer_dir: Path, fuzzer_bin: str, heads: dict[str, str],
          unstable: Path, module_head: Path | None, folder: Path, work: Path, tests_url: str) -> None:
    started = time.monotonic()
    repo, core = merge["repo"], merge["repo"] == VALKEY_REPO
    server = unstable / "src" / "valkey-server"
    # The three builds: each a Valkey server and, for a module merge or a test module, the module it loads.
    sides: dict[str, dict[str, Any]]
    if core:
        sides = {build: {"server": build_valkey(fetch(repo, merge[key], work / build)), "module": None}
                 for build, key in (("merge", "sha"), ("parent", "parent"))}
        sides["unstable"] = {"server": server, "module": None}
    else:
        name = _name(repo)
        if module_head is None:
            raise ValueError(f"a merge of {repo} needs the run's build of its head")
        setup_module(name, work)
        sides = {build: {"server": server, "module": build_module(name, fetch(repo, merge[key], work / build), unstable)}
                 for build, key in (("merge", "sha"), ("parent", "parent"))}
        sides["unstable"] = {"server": server, "module": module_head / MODULES[name].file}

    change = fetch_change(gh, token, ChangeRequest(repo=repo, base=merge["parent"], head=merge["sha"]))
    change.title = merge["subject"]
    count = budget(change.diff, repo)
    logger.info("Asking for %d test(s) of %s", count, _short(merge))
    catalog = load_catalog(fuzzer_bin)
    # The proposed experiments as written, and with every check on; the runs read them from these folders.
    plain: dict[str, str] = {}
    experiments: dict[str, str] = {}
    plain_dir, experiment_dir = work / "experiments-plain", work / "experiments"
    walked: set[str] = set()
    rounds: list[ProposalRun] = []

    def ask_tests(extra: str = "") -> tuple[ProposalRun, dict[str, str]]:
        """One round of proposals: Claude's run, and the validated scenarios not walked yet."""
        proposals = work / ("proposals" + (f"-{len(rounds)}" if rounds else ""))
        with tempfile.TemporaryDirectory(prefix="fuzzer-walk-") as td:
            proposal_run = propose(change, count=count, workdir=Path(td), out_dir=proposals, catalog=catalog,
                                   fuzzer_dir=fuzzer_dir, validator=fuzzer_validator(fuzzer_bin),
                                   lister=fuzzer_lister(fuzzer_bin), runner=ask,
                                   valkey_sha="" if core else heads[VALKEY_REPO], extra=extra)
        rounds.append(proposal_run)
        write_results(proposal_run, proposals)
        _, scenarios, proposed = validated_scenarios(proposals)
        result["dropped"] += len(proposal_run.dropped)
        if core and proposal_run.module is not None and sides["merge"]["module"] is None:
            # A test module of Valkey's own, built in each tree; unstable's came with its build.
            module_file = proposal_run.module.file
            for build in ("merge", "parent"):
                tree = Path(sides[build]["server"]).parents[1]
                build_test_modules(tree)
                sides[build]["module"] = tree / module_file
            sides["unstable"]["module"] = unstable / module_file
        # A later round's experiment named like an earlier one's but unlike it is not taken, nor its scenarios.
        clash = {file for file, text in proposed.items() if plain.get(file, text) != text}
        plain_dir.mkdir(parents=True, exist_ok=True)
        experiment_dir.mkdir(parents=True, exist_ok=True)
        for file, text in proposed.items():
            if file not in clash:
                plain[file], experiments[file] = text, with_checks(text)
                (plain_dir / file).write_text(text, encoding="utf-8")
                (experiment_dir / file).write_text(experiments[file], encoding="utf-8")
        fresh = {}
        for test, text in scenarios.items():
            if test in walked:
                continue
            result["proposed"] += 1
            if f"{yaml.safe_load(text)['experiment']['id']}.yaml" in clash:
                result["dropped"] += 1
                result["recorded"].append({"scenario": test, "reason": "its experiment's id is an earlier one's"})
                continue
            fresh[test] = text
        return proposal_run, fresh

    first_round, scenarios = ask_tests()
    if not scenarios:
        result["outcome"] = "declined"
        result["reason"] = first_round.reason or "no proposed test validated"
        return
    against = {"merge": "parent", "parent": "merge", "unstable": "parent"}
    runs: dict[tuple[str, str, int], dict[str, Any]] = {}

    def view(test: str, doc: dict[str, Any], build: str, seed: int, again: bool = False) -> dict[str, Any]:
        """The run of ``test`` on ``build`` at ``seed``, once; ``again`` runs it a second time. The checks run at
        the test's own seed: they do not change with the seed, and on ASan builds they take most of a run, so
        the further seeds the hunt adds for crashes that show only sometimes run without them."""
        key = (test, build, seed)
        if again or key not in runs:
            label = f"{test}-{build}-{seed}" + ("-again" if again else "")
            logger.info("Running %s", label)
            directory = experiment_dir if seed == int(doc["seed"]) else plain_dir
            done = fuzz(fuzzer_bin, doc, seed, candidate=sides[build], compare=sides[against[build]], core=core,
                        experiment_dir=directory, run_dir=work / "runs" / label)
            if again:
                return done
            runs[key] = done
        return runs[key]

    def all_started() -> None:
        # A run that could not start tested nothing: the merge is an error, to be walked again, not a pass.
        unstarted = [run for run in runs.values() if run.get("unstarted")]
        if unstarted:
            raise RuntimeError(f"{len(unstarted)} of {len(runs)} run(s) could not start: {unstarted[0]['error']}")

    pr = _short(merge)

    def walk_tests(scenarios: dict[str, str]) -> list[tuple[str, dict[str, Any]]]:
        """Prove, hunt and file each test; the tests that joined the hunt."""
        hunting = []
        for test, text in scenarios.items():
            walked.add(test)
            doc = yaml.safe_load(text)
            seed = int(doc["seed"])
            first = view(test, doc, "merge", seed)
            ran = first.get("reached") or first["status"] in ("PASS", "FAIL") or first.get("reason") == UNREACHED
            if not ran:
                # It did not run to its end on the merge (it errored, or was skipped): dropped, only counted.
                result["dropped"] += 1
                result["recorded"].append({"scenario": test, "build": "merge", "status": first["status"],
                                           "reason": first.get("reason") or first.get("error") or first.get("summary")})
                continue
            # A test that ran to its end hunts, though its marked commands replied alike: it still runs the code
            # the change touched, and its lines count for coverage. It is proven only if it reached the change.
            hunting.append((test, doc))
            result["proven"] += int(bool(first.get("reached")))
            result["recall"] = result["recall"] or bool(first.get("reached")) or bool(first.get("parent_exited"))
            (folder / "scenarios").mkdir(exist_ok=True)
            (folder / "scenarios" / f"{test}.yaml").write_text(text, encoding="utf-8")
            experiment_file = f"{doc['experiment']['id']}.yaml"
            if experiment_file in experiments:
                (folder / "experiments").mkdir(exist_ok=True)
                (folder / "experiments" / experiment_file).write_text(experiments[experiment_file], encoding="utf-8")
            seeds = [seed + offset for offset in range(HUNT_SEEDS)]
            view(test, doc, "parent", seed)
            result["hunted"] += 1
            found: list[dict[str, Any]] = []
            judged: set[str] = set()
            for hunt_seed in seeds:
                for fingerprint, info in view(test, doc, "unstable", hunt_seed)["found"].items():
                    if fingerprint in judged:
                        continue
                    judged.add(fingerprint)
                    again = fingerprint in view(test, doc, "unstable", hunt_seed, again=True)["found"]
                    found.append({**info, "fingerprint": fingerprint, "seed": hunt_seed, "builds": {
                        "parent": fingerprint in view(test, doc, "parent", hunt_seed)["found"],
                        "merge": fingerprint in view(test, doc, "merge", hunt_seed)["found"], "unstable": again}})
            for build in ("merge", "parent"):
                for fingerprint, info in view(test, doc, build, seed)["found"].items():
                    if fingerprint not in judged:
                        judged.add(fingerprint)
                        found.append({**info, "fingerprint": fingerprint, "seed": seed, "builds": {
                            "parent": fingerprint in view(test, doc, "parent", seed)["found"],
                            "merge": fingerprint in view(test, doc, "merge", seed)["found"], "unstable": False}})
            hunted = view(test, doc, "unstable", seed)
            # A fix regressed when commands that reached the change at the merge no longer do on unstable: not when
            # some marked command is unreached on both, which unreached-change alone says.
            lost = first["reached_commands"] - hunted.get("reached_commands", set())
            if first.get("reached") and hunted.get("reason") == UNREACHED and lost:
                rerun = view(test, doc, "unstable", seed, again=True)
                named = ", ".join(f"{component} {index}" for component, index in sorted(lost))
                found.append({"rule": "regressed-fix", "detail": f"regressed fix: the change of {_where(merge)} no longer "
                              f"shows on {heads[repo][:12]} (commands {named})", "lines": [], "seed": seed,
                              "fingerprint": compute_fingerprint(namespace=("fuzzer-walk", "regressed-fix", repo,
                                                                            merge["sha"]), shapes=[]),
                              "builds": {"parent": True, "merge": False, "unstable": rerun.get("reason") == UNREACHED
                                         and lost.isdisjoint(rerun.get("reached_commands", set()))}})
            for hunt_seed in seeds:
                if view(test, doc, "unstable", hunt_seed).get("reason") == UNEXPLAINED:
                    result["recorded"].append({"scenario": test, "build": "unstable", "seed": hunt_seed,
                                               "reason": UNEXPLAINED})
            for finding in found:
                finding.update(scenario=test, verdict=verdict(finding["builds"], pr))
                if finding["rule"] == "regressed-fix":
                    finding["verdict"] = f"the fix of {pr} regressed" if finding["builds"]["unstable"] else "not reproduced"
                if (finding["rule"] == "crash" and finding["builds"]["merge"] and not finding["builds"]["parent"]
                        and intended_crash(change.diff, finding["detail"])):
                    # The change raises this panic on purpose, as valkey#5155's ValkeyModule_Calloc does on overflow.
                    finding["verdict"] = f"intended by {pr}: a panic the change adds"
                builds = finding["builds"]
                result["recall"] = result["recall"] or (builds["parent"] and not builds["merge"]
                                                        and finding["rule"] != "regressed-fix")
                if files(finding):
                    context = {"doc": doc, "scenario": text, "experiment": experiments.get(experiment_file, ""),
                               "experiments": experiments, "module": sides["unstable"]["module"],
                               "unstable": heads[VALKEY_REPO], "module_head": result["module_head"] or "",
                               "tests_url": f"{tests_url}/{merge_folder(merge)}"}
                    action, url = publisher.upsert(fuzzer_repo, fingerprint=finding["fingerprint"],
                                                   render=render_issue(finding, merge, context),
                                                   idempotency_key=merge["sha"], keep_title=True)
                    finding.update(issue=url, issue_action=action)
                    logger.info("%s %s: %s", action, url, finding["detail"])
                result["findings"].append({key: finding.get(key) for key in (
                    "rule", "detail", "scenario", "seed", "builds", "verdict", "fingerprint", "issue")})
        return hunting

    tests = walk_tests(scenarios)
    all_started()
    changed = coverage_lines(change.diff) if core else {}
    if not core:
        result["coverage_reason"] = "not measured for modules yet"
    elif not changed:
        result["coverage_reason"] = "the merge adds no line to a C file under src/"
    else:
        tree = work / "coverage"

        def measure(tests: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
            """Run ``tests`` once on the coverage build (built first), checks off, against the parent; the changed
            lines' coverage by every run on it so far."""
            try:
                if not (tree / "src").is_dir():
                    build_valkey(fetch(repo, merge["sha"], tree), coverage=True)
                server = tree / "src" / "valkey-server"
                server = server if server.exists() else tree / "src" / "redis-server"
                done = [fuzz(fuzzer_bin, doc, int(doc["seed"]), candidate={**sides["merge"], "server": server},
                             compare=sides["parent"], core=core, experiment_dir=plain_dir,
                             run_dir=work / "runs" / f"{test}-coverage") for test, doc in tests]
                if done and all(run["status"] is None for run in done):
                    raise RuntimeError(f"no run on the coverage build finished: {done[0]['error']}")
                return coverage(changed, {path: gcov(tree, path) for path in changed})
            except Exception as exc:
                raise _Unmeasured(" ".join(str(exc).split())[:500]) from exc

        def more(found: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
            """One more round: tests aimed at the lines no test ran, walked like the first."""
            logger.info("Coverage %s%% of %d changed line(s): asking for tests that reach the rest",
                        found["percent"], found["lines"])
            try:
                _, scenarios = ask_tests(uncovered_section(found, tree, sorted(file[:-5] for file in plain)))
            except Exception as exc:
                logger.warning("Round %d proposed nothing: %s", len(rounds), exc)
                return []
            return walk_tests(scenarios)

        try:
            result["coverage"] = cover(measure, more, tests, started)
        except _Unmeasured as exc:
            result["coverage_reason"] = str(exc)
    all_started()


class _Unmeasured(Exception):
    """The coverage build, its runs or gcov failed: the merge's coverage is not known, and the merge goes on."""


# --- State ---------------------------------------------------------------------


def initial_state(repo: str) -> dict[str, Any]:
    return {"repo": repo, "pointer": {}, "done": {}, "failed": {}, "last_run": {}}


def _summary(merge: dict[str, Any], result: dict[str, Any] | None, now: str) -> dict[str, Any]:
    """The state's line for a merge done."""
    entry = {key: merge.get(key) for key in ("repo", "pr", "pr_repo", "subject")}
    entry["at"] = now
    if result is None:
        return {**entry, "outcome": "no-code-change"}
    entry.update({key: result.get(key) for key in ("outcome", "proposed", "proven", "hunted", "recall", "cost_usd")})
    entry["coverage"] = (result.get("coverage") or {}).get("percent")
    # Tests skipped as unsupported, most often a setting the build does not know: one reason, so a skip that
    # every merge hits (a renamed config) shows in PROGRESS.md, not only in a dropped count.
    skipped = [r for r in result.get("recorded") or [] if r.get("status") == "SKIP_UNSUPPORTED"]
    if skipped:
        entry["skipped"] = {"count": len(skipped), "reason": str(skipped[0].get("reason") or "")[:200]}
    if not result.get("error"):
        # record() keeps the tests of a merge whose run finished.
        entry["folder"] = merge_folder(merge)
    # One line per finding, though several of the merge's tests found it.
    unique: dict[Any, dict[str, Any]] = {}
    for f in result.get("findings") or []:
        unique.setdefault(f.get("fingerprint") or id(f), f)
    entry["findings"] = [{"rule": f["rule"], "verdict": f["verdict"], "issue": f.get("issue")}
                         for f in unique.values()]
    if result.get("reason") or result.get("error"):
        entry["reason"] = result.get("reason") or result.get("error")
    if result.get("error"):
        entry["outcome"] = "error"
    return entry


def update(state: dict[str, Any], planned: dict[str, Any], results: list[dict[str, Any]], *, now: str,
           run_url: str = "") -> dict[str, Any]:
    """The state after a run: its pointer, the merges done (and those with no code change), and the merges
    to retry. A merge whose run failed counts an attempt, and after MAX_ATTEMPTS it is done with its error; one
    whose job left no result (it never ran, or died) counts a miss, and is given up after MAX_MISSES."""
    state = copy.deepcopy(state)
    state["pointer"] = planned["pointer"]
    by_sha = {result["sha"]: result for result in results}
    cost = 0.0
    for merge in planned["skipped"]:
        state["done"][merge["sha"]] = _summary(merge, None, now)
    for merge in planned["merges"]:
        sha = merge["sha"]
        result = by_sha.get(sha)
        cost += float((result or {}).get("cost_usd") or 0)
        if result is not None and not result.get("error"):
            state["done"][sha] = _summary(merge, result, now)
            state["failed"].pop(sha, None)
            continue
        entry = state["failed"].get(sha) or {**{key: merge.get(key) for key in MERGE_KEYS}, "attempts": 0}
        entry["attempts"] += int(result is not None)
        entry["misses"] = entry.get("misses", 0) + int(result is None)
        entry["error"] = result["error"] if result is not None else "the job left no result"
        entry["cost_usd"] = round(float(entry.get("cost_usd") or 0) + float((result or {}).get("cost_usd") or 0), 4)
        if entry["attempts"] >= MAX_ATTEMPTS or entry["misses"] >= MAX_MISSES:
            state["done"][sha] = {**_summary(merge, result or {"error": entry["error"]}, now),
                                  "cost_usd": entry["cost_usd"]}
            state["failed"].pop(sha, None)
        else:
            state["failed"][sha] = entry
    state["last_run"] = {"at": now, "url": run_url, "merges": [m["sha"] for m in planned["merges"]],
                         "skipped": len(planned["skipped"]), "cost_usd": round(cost, 4)}
    return state


def dump_state(state: dict[str, Any]) -> str:
    """The state as JSON, one line per merge, so its history diffs by merge."""
    parts = []
    for key, value in state.items():
        if key in ("done", "failed"):
            lines = [f"  {json.dumps(sha)}: {json.dumps(entry, sort_keys=True)}" for sha, entry in value.items()]
            parts.append(f"{json.dumps(key)}: {{\n" + ",\n".join(lines) + ("\n" if lines else "") + "}")
        else:
            parts.append(f"{json.dumps(key)}: {json.dumps(value, sort_keys=True)}")
    return "{\n" + ",\n".join(parts) + "\n}\n"


def progress(states: list[dict[str, Any]], now: datetime) -> str:
    """PROGRESS.md, from every repository's state alone."""
    done = {sha: entry for state in states for sha, entry in state["done"].items()}
    failed = {sha: entry for state in states for sha, entry in state["failed"].items()}

    def since(entry: dict[str, Any], days: float) -> bool:
        return datetime.fromisoformat(entry["at"]) >= now - timedelta(days=days)

    def name(sha: str, entry: dict[str, Any]) -> str:
        merge = {**entry, "sha": sha}
        tests = f" ([tests]({entry['folder']}/))" if entry.get("folder") else ""
        return f"[{_where(merge)}]({_pr_url(merge)}){tests}"

    def spend(days: float | None) -> str:
        return f"${sum(float(e.get('cost_usd') or 0) for e in done.values() if days is None or since(e, days)):.2f}"

    states = sorted(states, key=lambda state: state["repo"])
    lines = ["# Fuzzer walk", "", f"Updated {now.isoformat(timespec='minutes')}.", "",
             "| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |",
             "|---|---|---|---|---|---|"]
    for state in states:
        mine, pointer = list(state["done"].values()), state["pointer"]
        cursor = f"#{pointer['cursor_pr']}" if pointer.get("cursor_pr") else pointer.get("cursor", "?")[:12]
        lines.append(f"| {state['repo']} | {len(mine)} | {pointer.get('remaining', 0) + len(state['failed'])} | "
                     f"{cursor} ({pointer.get('cursor_date', '?')}) | {sum(since(e, 1) for e in mine)} | "
                     f"{sum(since(e, 7) for e in mine) / 7:.1f} |")
    lines += ["", "## Last runs"]
    for state in states:
        last = state.get("last_run") or {}
        if not last:
            continue
        lines += ["", f"### {state['repo']}", "", f"[{last['at']}]({last['url']}): {len(last['merges'])} merge(s) "
                  f"tested, {last.get('skipped', 0)} without a code change.", ""]
        for sha in last["merges"]:
            if sha in failed:
                lines.append(f"- {name(sha, failed[sha])}: to retry, {failed[sha]['error'][:200]}")
                continue
            entry = done.get(sha)
            if entry is None:
                continue
            what = f"{entry.get('subject', '')}: "
            if entry["outcome"] == "tested":
                what += f"{entry['proven']} of {entry['proposed']} test(s) reached the change"
                what += f", coverage {entry['coverage']}%" if entry.get("coverage") is not None else ""
                skipped = entry.get("skipped")
                what += f", {skipped['count']} skipped ({skipped['reason']})" if skipped else ""
                what += "".join(f"; [{f['rule']}, {f['verdict']}]({f['issue']})" if f.get("issue")
                                else f"; {f['rule']}, {f['verdict']} (recorded)" for f in entry.get("findings", []))
            else:
                what += f"{entry['outcome']}: {entry.get('reason', '')}"[:300]
            lines.append(f"- {name(sha, entry)}: {what}")
    tested = [e for e in done.values() if e["outcome"] == "tested"]
    fixes = [e for e in tested if _FIX_RE.search(e.get("subject") or "")]
    issues = sorted({f["issue"] for e in done.values() for f in e.get("findings", []) if f.get("issue")})
    reached = sum(1 for e in tested if e.get("proven"))
    covered = [e["coverage"] for e in tested if e.get("coverage") is not None]
    lines += ["", "## Totals", "",
              f"- Merges whose tests reached the change: {reached} of {len(tested)} tested"
              + (f" ({100 * reached / len(tested):.0f}%)" if tested else ""),
              f"- Changed-line coverage: median {statistics.median(covered):.0f}% of {len(covered)} merge(s) measured, "
              f"{sum(1 for c in covered if c < 50)} under 50%" if covered else "- Changed-line coverage: none measured",
              f"- Recall: {sum(1 for e in fixes if e.get('recall'))} parents flagged of {len(fixes)} fix merges",
              f"- Claude: {spend(1)} in the last 24 hours, {spend(7)} in the last 7 days, {spend(None)} in all",
              f"- Issues filed: {len(issues)}", *(f"  - {url}" for url in issues)]
    lines += ["", "## Retries", ""]
    lines += [f"- {name(sha, e)}: attempt {e['attempts']} of {MAX_ATTEMPTS}, {e['error'][:200]}"
              for sha, e in failed.items()] or ["None."]
    return "\n".join(lines) + "\n"


def record(branch: Path, planned: dict[str, Any], results_dir: Path, *, now: datetime, run_url: str) -> None:
    """Fold a run into the state branch's checkout: its repository's state and the done merges' tests, then
    PROGRESS.md from every repository's state."""
    path = branch / STATE_DIR / f"{_name(planned['repo'])}.json"
    state = json.loads(path.read_text()) if path.is_file() else initial_state(planned["repo"])
    results = []
    for result_file in sorted(results_dir.glob(f"{TESTS_DIR}/*/*/{RESULT_FILE}")):
        results.append(json.loads(result_file.read_text()))
    state = update(state, planned, results, now=now.isoformat(timespec="seconds"), run_url=run_url)
    for result in results:
        if result["sha"] in state["done"] and not result.get("error"):
            target = branch / merge_folder(result)
            shutil.rmtree(target, ignore_errors=True)
            shutil.copytree(results_dir / merge_folder(result), target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_state(state), encoding="utf-8")
    states = [json.loads(p.read_text()) for p in sorted((branch / STATE_DIR).glob("*.json"))]
    (branch / PROGRESS_FILE).write_text(progress(states, now), encoding="utf-8")


def publish(remote: str, work: Path, apply: Callable[[Path], None], message: str) -> None:
    """Commit what ``apply`` does to a fresh checkout of the state branch at ``remote`` and push it. A push
    rejected because the branch moved (another repository's run pushed first) starts again from a new fetch,
    up to MAX_PUSHES times. Without the branch, the first push creates it."""
    for attempt in range(1, MAX_PUSHES + 1):
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        _git(work, "init", "-q")
        listed = subprocess.run(["git", "-C", str(work), "ls-remote", "--exit-code", "--heads", remote, STATE_BRANCH],
                                capture_output=True, text=True)
        if listed.returncode == 0:
            _git(work, "fetch", "-q", "--depth", "1", remote, STATE_BRANCH)
            _git(work, "checkout", "-q", "-B", STATE_BRANCH, "FETCH_HEAD")
        elif listed.returncode == 2:
            _git(work, "checkout", "-q", "--orphan", STATE_BRANCH)
        else:
            raise RuntimeError(f"cannot read {remote}: {listed.stderr.strip()[:300]}")
        apply(work)
        _git(work, "add", "-A")
        _git(work, "-c", "user.name=github-actions[bot]",
             "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com", "commit", "-q", "-m", message)
        pushed = subprocess.run(["git", "-C", str(work), "push", "-q", remote, f"HEAD:{STATE_BRANCH}"],
                                capture_output=True, text=True)
        if pushed.returncode == 0:
            return
        logger.warning("Push %d of %d to %s was rejected: %s", attempt, MAX_PUSHES, STATE_BRANCH,
                       pushed.stderr.strip()[:300])
    raise RuntimeError(f"{STATE_BRANCH} rejected {MAX_PUSHES} pushes")


# --- Command line --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("plan", help="list the run's merges of one repository")
    p.add_argument("--repo", required=True, choices=NAMES, help="the repository walked, by name")
    p.add_argument("--state", type=Path, required=True, help="its state file; absent means a walk that starts now")
    p.add_argument("--work", type=Path, required=True, help="where the repository is cloned")
    p.add_argument("--max", type=int, default=20, help="jobs in the run")
    p.add_argument("--out", type=Path, required=True)
    b = commands.add_parser("build", help="build a head for the run")
    b.add_argument("--repo", required=True, choices=NAMES)
    b.add_argument("--sha", required=True)
    b.add_argument("--valkey-sha", default="", help="for a module: the Valkey commit it builds against")
    b.add_argument("--work", type=Path, required=True)
    b.add_argument("--out", type=Path, required=True, help="the tarball of the build")
    r = commands.add_parser("run", help="walk one merge")
    r.add_argument("--merge", required=True, help="one of plan's merges, as JSON")
    r.add_argument("--heads", required=True, help="plan's heads, as JSON")
    r.add_argument("--unstable", type=Path, required=True, help="the run's unstable build, extracted")
    r.add_argument("--module-head", type=Path, help="for a module merge: the run's build of its head, extracted")
    r.add_argument("--fuzzer-repo", required=True, help="where issues are filed")
    r.add_argument("--fuzzer-dir", type=Path, required=True)
    r.add_argument("--fuzzer-bin", required=True)
    r.add_argument("--tests-url", required=True, help="where the state branch keeps the tests, for the issues")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--work", type=Path, required=True)
    u = commands.add_parser("update", help="fold a run into the state branch and publish it")
    u.add_argument("--remote", required=True, help="the repository whose state branch holds the walk's state")
    u.add_argument("--work", type=Path, required=True, help="where the state branch is checked out")
    u.add_argument("--plan", type=Path, required=True)
    u.add_argument("--results", type=Path, required=True, help="the jobs' tests folders, merged")
    u.add_argument("--run-url", default="")
    args = parser.parse_args(argv)
    configure_logging()

    if args.command == "plan":
        if not 1 <= args.max <= MAX_JOBS:
            parser.error(f"--max must be 1 to {MAX_JOBS}")
        repo = f"{VALKEY_REPO.split('/')[0]}/{args.repo}"
        state = json.loads(args.state.read_text()) if args.state.is_file() else initial_state(repo)
        planned = plan(clone(repo, args.work / args.repo), repo, state, args.max,
                       "" if repo == VALKEY_REPO else remote_head(VALKEY_REPO))
        args.out.write_text(json.dumps(planned, indent=2), encoding="utf-8")
        log_outcome(logger, logging.INFO, f"{len(planned['merges'])} merge(s) of {repo} to walk, "
                                          f"{len(planned['skipped'])} without a code change")
        return 0
    if args.command == "build":
        repo = f"{VALKEY_REPO.split('/')[0]}/{args.repo}"
        build_head(repo, args.sha, args.valkey_sha, args.work, args.out)
        log_outcome(logger, logging.INFO, f"Built {repo} at {args.sha} into {args.out}")
        return 0
    if args.command == "update":
        planned = json.loads(args.plan.read_text())
        now = datetime.now(timezone.utc)
        publish(args.remote, args.work,
                lambda branch: record(branch, planned, args.results, now=now, run_url=args.run_url),
                f"Walk {planned['repo']}: {len(planned['merges'])} merge(s), {len(planned['skipped'])} without code")
        log_outcome(logger, logging.INFO, f"Published the walk's state of {planned['repo']} on {STATE_BRANCH}")
        return 0

    token = os.environ.get("TARGET_TOKEN", "")
    if not token:
        parser.error("the TARGET_TOKEN env var is required")
    from github import Auth, Github

    gh = Github(auth=Auth.Token(token))
    merge = json.loads(args.merge)
    result = run(merge, gh=gh, token=token, publisher=IssueDedupPublisher(gh, marker_namespace=MARKER_NAMESPACE),
                 fuzzer_repo=args.fuzzer_repo, fuzzer_dir=args.fuzzer_dir, fuzzer_bin=args.fuzzer_bin,
                 heads=json.loads(args.heads), unstable=args.unstable, module_head=args.module_head, out=args.out,
                 work=args.work, tests_url=args.tests_url)
    filed = [f["issue"] for f in result["findings"] if f.get("issue")]
    log_outcome(logger, logging.ERROR if result.get("error") else logging.INFO,
                f"{merge['repo']} {_short(merge)}: {result['outcome']}, {result['proven']} of {result['proposed']} "
                f"test(s) reached the change, {len(filed)} issue(s)"
                + (f"; error: {result['error']}" if result.get("error") else ""))
    return 1 if result.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
