"""Walk every merge of Valkey and its modules through the fuzzer, newest first, then every new one.

``plan`` reads the walk's state and lists the merges a run processes: for
each repository (valkey ``unstable`` and each module in
:data:`~scripts.fuzzer.pr_batch.MODULES`), the merges after the forward
head, oldest first, then the merges to retry, then backlog merges from the
cursor back toward the end (valkey: where ``unstable`` left 7.2, a module:
its first commit). The repositories' queues share the run's matrix, taken in
turn. A merge that changes only docs, CI or tests is done without a job
(``no-code-change``).

``build`` builds one repository's head for the whole run: ``unstable`` with
ASan and its tests/modules, or a module's default branch against it.

``run`` processes one merge, in its own job: it builds the merge and its
parent (for a module, both loaded into the run's ``unstable`` build), asks
Claude for tests (:func:`~scripts.fuzzer.propose.propose`), turns every
check on, and runs each test on three builds: the merge against its parent,
the parent against the merge, and ``unstable`` against the parent, on
several seeds. A crash, a sanitizer report or a check failure is filed only
when it shows on ``unstable`` and again on one rerun, and so is a test that
reached the change at the merge but no longer does on ``unstable`` (a fix
that regressed). The rest is recorded. Issues go to the fuzzer repository,
one per signature: a repeat adds an occurrence.

``update`` folds a run's results into the state, writes ``PROGRESS.md`` and
copies each merge's proven tests beside them, under ``tests/``.
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
import subprocess
import sys
import tarfile
import tempfile
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
from scripts.fuzzer.analyzer import _ANOMALY_PATTERNS, _CRASH_TITLES, UNEXPLAINED, UNREACHED, _change_outcome
from scripts.fuzzer.batch import report_problem
from scripts.fuzzer.pr_batch import MODULES, module_repository
from scripts.fuzzer.propose import (
    VALKEY_REPO,
    ChangeRequest,
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
REPOSITORIES = (VALKEY_REPO, *(f"valkey-io/{name}" for name in MODULES))
STATE_FILE = "walk-state.json"
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
MERGE_KEYS = ("repo", "sha", "parent", "pr", "subject", "date")
# The checks the walk turns on in every compared command sequence.
CHECKS = {"variants": True, "replica": True, "acl": True, "concurrency": 4, "compat": True}
# Whether the walk sets CHECKS: off until valkey-fuzzer takes `checks`, which its validation refuses until then.
WITH_CHECKS = False
# The diagnostics.reason a failed check gives.
CHECK_REASONS = frozenset({
    "variant-mismatch", "replica-diverged", "acl-bypass", "acl-overreach", "compat-load", "compat-sync", "hang",
})
MARKER_NAMESPACE = "valkey-ci-agent:fuzzer-walk"
VALKEY_TOOLS = ("valkey-server", "valkey-cli", "valkey-benchmark", "valkey-check-rdb", "valkey-check-aof")
_CRASH_PATTERNS = [(title, pattern) for title, _, pattern, _ in _ANOMALY_PATTERNS if title in _CRASH_TITLES]
_SANITIZER_RE = re.compile(r"SUMMARY: \w*Sanitizer: [^\n]*")
_ASSERTION_RE = re.compile(r"ASSERTION FAILED[^\n]*\n([^\n]*)")
_SIGNAL_RE = re.compile(r"crashed by signal[^\n]*")
_FRAME_RE = re.compile(r"#\d+ 0x[0-9a-f]+ in (\S+)")
_EIP_RE = re.compile(r"EIP:\s*\n[^\n(]*\((\w+)")
_FIX_RE = re.compile(r"\bfix", re.IGNORECASE)


def _git(repo_dir: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo_dir), *args], check=True, capture_output=True, text=True).stdout


def _name(repo: str) -> str:
    return repo.split("/")[-1]


def _short(merge: dict[str, Any]) -> str:
    """How an issue names the merge: #PR, or its commit."""
    return f"#{merge['pr']}" if merge.get("pr") else str(merge["sha"])[:12]


def merge_folder(merge: dict[str, Any]) -> str:
    """Where a merge's proven tests are kept: tests/<repository name>/<pr>-<short sha>."""
    folder = f"{merge['pr']}-{merge['sha'][:9]}" if merge.get("pr") else merge["sha"][:9]
    return f"{TESTS_DIR}/{_name(merge['repo'])}/{folder}"


# --- Plan ----------------------------------------------------------------------


def first_parent(repo_dir: Path, repo: str) -> list[dict[str, Any]]:
    """The commits on the checked-out branch's first-parent history, newest first."""
    history = []
    for line in _git(repo_dir, "log", "--first-parent", "--format=%H%x09%P%x09%cs%x09%s", "HEAD").splitlines():
        sha, parents, date, subject = line.split("\t", 3)
        match = _PR_SUBJECT_RE.search(subject)
        history.append({"repo": repo, "sha": sha, "parent": parents.split(" ")[0], "pr": int(match.group(1))
                        if match else None, "subject": subject[:200], "date": date})
    return history


def no_code_change(paths: list[str], repo: str) -> bool:
    """Whether a merge changes only docs (``*.md``, ``docs/``), CI (``.github/``) or tests (``tests/``, but
    Valkey's tests/modules, and ``integration/``)."""
    def other(path: str) -> bool:
        if path.endswith(".md") or path.startswith(("docs/", ".github/", "integration/")):
            return False
        return not path.startswith("tests/") or (repo == VALKEY_REPO and path.startswith("tests/modules/"))
    return not any(other(path) for path in paths)


def plan(clones: dict[str, Path], state: dict[str, Any], max_jobs: int, walk: list[str] | None = None,
         ) -> dict[str, Any]:
    """The run's merges, the merges done without a job, and each repository's new pointers.

    ``clones`` maps each repository to a full clone with its default branch checked out; ``walk`` names the
    ones walked (all by default), and the others give only their head. A repository the state does not know
    yet starts at its head: the forward head and the backlog cursor both name the newest merge, so the walk
    begins with it.
    """
    walk = list(clones) if walk is None else walk
    histories = {repo: first_parent(repo_dir, repo) for repo, repo_dir in clones.items()}
    queues: dict[str, Any] = {}
    pointers: dict[str, dict[str, Any]] = {}
    for repo in walk:
        history = histories[repo]
        index = {merge["sha"]: i for i, merge in enumerate(history)}
        end = VALKEY_BASE if repo == VALKEY_REPO else history[-1]["sha"]
        pointer = pointers[repo] = dict(state["repos"].get(repo) or {
            "head": history[0]["sha"], "cursor": history[0]["sha"], "end": end})
        for key in ("head", "cursor", "end"):
            if pointer[key] not in index:
                raise ValueError(f"the walk's {key} of {repo}, {pointer[key]}, is not on its first-parent history")
        new = [(i, "new") for i in reversed(range(index[pointer["head"]]))]
        backlog = [(i, "backlog") for i in range(index[pointer["cursor"]], index[pointer["end"]])]
        retries = [(entry, "retry") for entry in state["failed"].values() if entry["repo"] == repo]
        queues[repo] = iter([*new, *retries, *backlog])

    jobs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    def next_job(repo: str) -> dict[str, Any] | None:
        history, pointer = histories[repo], pointers[repo]
        for item, kind in queues[repo]:
            if kind == "retry":
                return {key: item[key] for key in MERGE_KEYS}
            merge = history[item]
            if kind == "new":
                pointer["head"] = merge["sha"]
            else:
                pointer["cursor"] = history[item + 1]["sha"]
            if merge["sha"] in state["done"] or merge["sha"] in state["failed"]:
                continue
            if no_code_change(_git(clones[repo], "diff", "--name-only", merge["parent"], merge["sha"]).split(), repo):
                skipped.append(merge)
                continue
            return merge
        return None

    live = list(walk)
    while live and len(jobs) < max_jobs:
        for repo in list(live):
            if len(jobs) >= max_jobs:
                break
            merge = next_job(repo)
            if merge is None:
                live.remove(repo)
            else:
                # The workflow's names for the job's builds and cache.
                jobs.append({**merge, "name": _name(repo),
                             "deps": MODULES[_name(repo)].deps if module_repository(repo) else ""})
    for repo, pointer in pointers.items():
        index = {merge["sha"]: i for i, merge in enumerate(histories[repo])}
        cursor = histories[repo][index[pointer["cursor"]]]
        pointer.update(remaining=index[pointer["end"]] - index[pointer["cursor"]], cursor_pr=cursor["pr"],
                       cursor_date=cursor["date"])
    heads = {repo: history[0]["sha"] for repo, history in histories.items()}
    return {"merges": jobs, "skipped": skipped, "repos": pointers, "heads": heads}


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


def build_valkey(src: Path, *, asan: bool = False) -> Path:
    """Build Valkey in ``src`` as the fuzzer's PR batch does; the server it built."""
    nproc = os.cpu_count() or 2
    flags = " CC=clang OPT=-O3 SANITIZER=address" if asan else ""
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


# --- Running a test ------------------------------------------------------------


def with_checks(text: str) -> str:
    """An experiment's YAML with CHECKS on every valkey.action.commands that compares with another build."""
    doc = yaml.safe_load(text)
    for component in doc.get("components") or []:
        config = component.get("config") if isinstance(component, dict) else None
        if component.get("use") == "valkey.action.commands" and isinstance(config, dict) \
                and config.get("compare_with") is not None:
            config["checks"] = dict(CHECKS)
    return yaml.safe_dump(doc, sort_keys=False)


def log_cause(text: str) -> str | None:
    """What a crash or sanitizer report in a log names: the sanitizer's summary (with the first frame outside
    the allocator for a leak), the failed assertion, or the signal and the function it hit."""
    summary = _SANITIZER_RE.search(text)
    if summary:
        cause = summary.group(0)
        if "leaked" in cause:
            frame = next((m.group(1) for m in _FRAME_RE.finditer(text) if "alloc" not in m.group(1)), None)
            cause += f" from {frame}" if frame else ""
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
            add("check", f"{reason}: {summary}", [summary], str(reason))
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
    outcome = _change_outcome(report)
    sequence = (outcome.sequence if outcome else None) or {}
    return {"status": report["status"], "reason": outcome.reason if outcome else None,
            "reached": bool(outcome and outcome.reached),
            "parent_exited": isinstance(sequence.get("parent"), dict) and "exited" in sequence["parent"],
            "found": findings(report, run_dir / "artifacts")}


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
    """The one filing rule: a finding is filed when unstable shows it, after a rerun."""
    return bool(finding["builds"]["unstable"])


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
    where = f"{repo}#{merge['pr']}" if merge.get("pr") else f"{repo}@{merge['sha'][:12]}"
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
            f"{finding['verdict'].capitalize()}: [{where}](https://github.com/{repo}/commit/{merge['sha']}) "
            f"{merge['subject']}", "",
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
        comment = (f"Seen again testing [{where}](https://github.com/{repo}/commit/{merge['sha']}) on "
                   f"`{head[:12]}`: {finding['verdict']}. The test's files: {context['tests_url']}")
        return IssueContent(title=title, body="\n".join(body)[:65000], comment=comment)
    return render


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
        count: int, tests_url: str, runner: Callable[[str, Path], AgentRunResult] | None = None) -> dict[str, Any]:
    """Process one merge end to end; its result.json, beside its proven tests, in ``out``/:func:`merge_folder`."""
    repo = merge["repo"]
    core = repo == VALKEY_REPO
    folder = out / merge_folder(merge)
    folder.mkdir(parents=True, exist_ok=True)
    module_head_sha = "" if core else heads[repo]
    result: dict[str, Any] = {
        **{key: merge.get(key) for key in MERGE_KEYS}, "unstable": heads[VALKEY_REPO],
        "module_head": module_head_sha or None, "outcome": "tested", "proposed": 0, "proven": 0, "hunted": 0,
        "dropped": 0, "recall": False, "cost_usd": 0.0, "findings": [], "recorded": [],
        "error": "the run did not finish",
    }
    cost = [0.0]

    def save() -> None:
        result["cost_usd"] = round(cost[0], 4)
        (folder / RESULT_FILE).write_text(json.dumps(result, indent=2), encoding="utf-8")

    def ask(prompt: str, workdir: Path) -> AgentRunResult:
        answer = runner(prompt, workdir) if runner else run_agent("fuzzer_proposal_readonly", prompt, cwd=str(workdir))
        cost[0] += claude_cost(answer.stdout)
        return answer

    save()
    try:
        _walk(merge, result, ask, gh=gh, token=token, publisher=publisher, fuzzer_repo=fuzzer_repo,
              fuzzer_dir=fuzzer_dir, fuzzer_bin=fuzzer_bin, heads=heads, unstable=unstable,
              module_head=module_head, folder=folder, work=work, count=count, tests_url=tests_url)
        del result["error"]
    except Exception as exc:
        logger.warning("The walk of %s failed: %s", merge["sha"], exc, exc_info=True)
        result["error"] = " ".join(str(exc).split())[:1000]
    finally:
        save()
    return result


def _walk(merge: dict[str, Any], result: dict[str, Any], ask: Callable[[str, Path], AgentRunResult], *, gh: Any,
          token: str, publisher: Any, fuzzer_repo: str, fuzzer_dir: Path, fuzzer_bin: str, heads: dict[str, str],
          unstable: Path, module_head: Path | None, folder: Path, work: Path, count: int, tests_url: str) -> None:
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
    proposals = work / "proposals"
    with tempfile.TemporaryDirectory(prefix="fuzzer-walk-") as td:
        proposal_run = propose(change, count=count, workdir=Path(td), out_dir=proposals,
                               catalog=load_catalog(fuzzer_bin), fuzzer_dir=fuzzer_dir,
                               validator=fuzzer_validator(fuzzer_bin), lister=fuzzer_lister(fuzzer_bin),
                               runner=ask, valkey_sha="" if core else heads[VALKEY_REPO])
    write_results(proposal_run, proposals)
    _, scenarios, experiments = validated_scenarios(proposals)
    result["proposed"], result["dropped"] = len(scenarios), len(proposal_run.dropped)
    if not scenarios:
        result["outcome"] = "declined"
        result["reason"] = proposal_run.reason or "no proposed test validated"
        return
    if core and proposal_run.module is not None:
        # A test module of Valkey's own, built in each tree; unstable's came with its build.
        module_file = proposal_run.module.file
        for build in ("merge", "parent"):
            tree = Path(sides[build]["server"]).parents[1]
            build_test_modules(tree)
            sides[build]["module"] = tree / module_file
        sides["unstable"]["module"] = unstable / module_file
    if WITH_CHECKS:
        experiments = {file: with_checks(text) for file, text in experiments.items()}
    experiment_dir = work / "experiments"
    experiment_dir.mkdir(parents=True, exist_ok=True)
    for file, text in experiments.items():
        (experiment_dir / file).write_text(text, encoding="utf-8")
    against = {"merge": "parent", "parent": "merge", "unstable": "parent"}
    runs: dict[tuple[str, str, int], dict[str, Any]] = {}

    def view(test: str, doc: dict[str, Any], build: str, seed: int, again: bool = False) -> dict[str, Any]:
        """The run of ``test`` on ``build`` at ``seed``, once; ``again`` runs it a second time."""
        key = (test, build, seed)
        if again or key not in runs:
            label = f"{test}-{build}-{seed}" + ("-again" if again else "")
            logger.info("Running %s", label)
            done = fuzz(fuzzer_bin, doc, seed, candidate=sides[build], compare=sides[against[build]], core=core,
                        experiment_dir=experiment_dir, run_dir=work / "runs" / label)
            if again:
                return done
            runs[key] = done
        return runs[key]

    pr = _short(merge)
    for test, text in scenarios.items():
        doc = yaml.safe_load(text)
        seed = int(doc["seed"])
        first = view(test, doc, "merge", seed)
        if not first["reached"] and first["status"] not in ("PASS", "FAIL"):
            # It neither reached the change nor ran to a verdict on the merge: dropped, only counted.
            result["dropped"] += 1
            result["recorded"].append({"scenario": test, "build": "merge", "status": first["status"],
                                       "reason": first.get("reason") or first.get("error")})
            continue
        result["proven"] += int(first["reached"])
        result["recall"] = result["recall"] or first["reached"] or first["parent_exited"]
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
        if first["reached"] and hunted.get("reason") == UNREACHED:
            rerun = view(test, doc, "unstable", seed, again=True)
            found.append({"rule": "regressed-fix", "detail": f"regressed fix: the change of {repo} {pr} no longer "
                          f"shows on {heads[repo][:12]}", "lines": [], "seed": seed,
                          "fingerprint": compute_fingerprint(namespace=("fuzzer-walk", "regressed-fix", repo,
                                                                        merge["sha"]), shapes=[]),
                          "builds": {"parent": True, "merge": False, "unstable": rerun.get("reason") == UNREACHED}})
        for hunt_seed in seeds:
            if view(test, doc, "unstable", hunt_seed).get("reason") == UNEXPLAINED:
                result["recorded"].append({"scenario": test, "build": "unstable", "seed": hunt_seed,
                                           "reason": UNEXPLAINED})
        for finding in found:
            finding.update(scenario=test, verdict=verdict(finding["builds"], pr))
            if finding["rule"] == "regressed-fix":
                finding["verdict"] = f"the fix of {pr} regressed" if finding["builds"]["unstable"] else "not reproduced"
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


# --- State ---------------------------------------------------------------------


def initial_state() -> dict[str, Any]:
    return {"repos": {}, "done": {}, "failed": {}, "last_run": {}}


def _summary(merge: dict[str, Any], result: dict[str, Any] | None, now: str) -> dict[str, Any]:
    """The state's line for a merge done."""
    entry = {key: merge.get(key) for key in ("repo", "pr", "subject")}
    entry["at"] = now
    if result is None:
        return {**entry, "outcome": "no-code-change"}
    entry.update({key: result.get(key) for key in ("outcome", "proposed", "proven", "hunted", "recall", "cost_usd")})
    if not result.get("error"):
        # record() keeps the tests of a merge whose run finished.
        entry["folder"] = merge_folder(merge)
    entry["findings"] = [{"rule": f["rule"], "verdict": f["verdict"], "issue": f.get("issue")}
                         for f in result.get("findings") or []]
    if result.get("reason") or result.get("error"):
        entry["reason"] = result.get("reason") or result.get("error")
    if result.get("error"):
        entry["outcome"] = "error"
    return entry


def update(state: dict[str, Any], planned: dict[str, Any], results: list[dict[str, Any]], *, now: str,
           run_url: str = "") -> dict[str, Any]:
    """The state after a run: its pointers, the merges done (and those with no code change), and the merges
    to retry. A merge whose run failed counts an attempt, and after MAX_ATTEMPTS it is done with its error; one
    whose job left no result (it never ran, or died) counts a miss, and is given up after MAX_MISSES."""
    state = copy.deepcopy(state)
    state["repos"].update(planned["repos"])
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


def progress(state: dict[str, Any], now: datetime) -> str:
    """PROGRESS.md, from the state alone."""
    done = state["done"]
    failed = state["failed"]

    def since(entry: dict[str, Any], days: float) -> bool:
        return datetime.fromisoformat(entry["at"]) >= now - timedelta(days=days)

    def name(sha: str, entry: dict[str, Any]) -> str:
        label = f"{entry['repo']}{'#' + str(entry['pr']) if entry.get('pr') else ' ' + sha[:12]}"
        return f"[{label}]({entry['folder']}/)" if entry.get("folder") else label

    def spend(days: float | None) -> str:
        return f"${sum(float(e.get('cost_usd') or 0) for e in done.values() if days is None or since(e, days)):.2f}"

    lines = ["# Fuzzer walk", "", f"Updated {now.isoformat(timespec='minutes')}.", "",
             "| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |",
             "|---|---|---|---|---|---|"]
    for repo, pointer in sorted(state["repos"].items()):
        mine = [e for e in done.values() if e["repo"] == repo]
        retries = sum(1 for e in failed.values() if e["repo"] == repo)
        cursor = f"#{pointer['cursor_pr']}" if pointer.get("cursor_pr") else pointer["cursor"][:12]
        lines.append(f"| {repo} | {len(mine)} | {pointer.get('remaining', 0) + retries} | {cursor} "
                     f"({pointer.get('cursor_date', '?')}) | {sum(since(e, 1) for e in mine)} | "
                     f"{sum(since(e, 7) for e in mine) / 7:.1f} |")
    last = state.get("last_run") or {}
    lines += ["", "## Last run", ""]
    if last:
        lines += [f"[{last['at']}]({last['url']}): {len(last['merges'])} merge(s) tested, "
                  f"{last.get('skipped', 0)} without a code change.", ""]
    for sha in last.get("merges", []):
        if sha in failed:
            lines.append(f"- {name(sha, failed[sha])}: to retry, {failed[sha]['error'][:200]}")
            continue
        entry = done.get(sha)
        if entry is None:
            continue
        what = f"{entry.get('subject', '')}: "
        if entry["outcome"] == "tested":
            what += f"{entry['proven']} of {entry['proposed']} test(s) reached the change"
            what += "".join(f"; [{f['rule']}, {f['verdict']}]({f['issue']})" if f.get("issue")
                            else f"; {f['rule']}, {f['verdict']} (recorded)" for f in entry.get("findings", []))
        else:
            what += f"{entry['outcome']}: {entry.get('reason', '')}"[:300]
        lines.append(f"- {name(sha, entry)}: {what}")
    tested = [e for e in done.values() if e["outcome"] == "tested"]
    fixes = [e for e in tested if _FIX_RE.search(e.get("subject") or "")]
    issues = sorted({f["issue"] for e in done.values() for f in e.get("findings", []) if f.get("issue")})
    reached = sum(1 for e in tested if e.get("proven"))
    lines += ["", "## Totals", "",
              f"- Merges whose tests reached the change: {reached} of {len(tested)} tested"
              + (f" ({100 * reached / len(tested):.0f}%)" if tested else ""),
              f"- Recall: {sum(1 for e in fixes if e.get('recall'))} parents flagged of {len(fixes)} fix merges",
              f"- Claude: {spend(1)} in the last 24 hours, {spend(7)} in the last 7 days, {spend(None)} in all",
              f"- Issues filed: {len(issues)}", *(f"  - {url}" for url in issues)]
    lines += ["", "## Retries", ""]
    lines += [f"- {name(sha, e)}: attempt {e['attempts']} of {MAX_ATTEMPTS}, {e['error'][:200]}"
              for sha, e in failed.items()] or ["None."]
    return "\n".join(lines) + "\n"


def record(state_dir: Path, planned: dict[str, Any], results_dir: Path, *, now: datetime, run_url: str) -> None:
    """Fold a run into the state branch's checkout: the state, PROGRESS.md and the done merges' tests."""
    path = state_dir / STATE_FILE
    state = json.loads(path.read_text()) if path.is_file() else initial_state()
    results = []
    for result_file in sorted(results_dir.glob(f"{TESTS_DIR}/*/*/{RESULT_FILE}")):
        results.append(json.loads(result_file.read_text()))
    state = update(state, planned, results, now=now.isoformat(timespec="seconds"), run_url=run_url)
    for result in results:
        if result["sha"] in state["done"] and not result.get("error"):
            target = state_dir / merge_folder(result)
            shutil.rmtree(target, ignore_errors=True)
            shutil.copytree(results_dir / merge_folder(result), target)
    path.write_text(dump_state(state), encoding="utf-8")
    (state_dir / PROGRESS_FILE).write_text(progress(state, now), encoding="utf-8")


# --- Command line --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("plan", help="list the run's merges")
    p.add_argument("--state", type=Path, help="walk-state.json; absent means a walk that starts now")
    p.add_argument("--clone", action="append", default=[], metavar="OWNER/NAME=DIR",
                   help="a full clone of a repository on its default branch; one per repository walked")
    p.add_argument("--walk", default="all", help="the repository to walk, or all; the others give only their head")
    p.add_argument("--max", type=int, default=20, help="jobs in the run, across the repositories")
    p.add_argument("--out", type=Path, required=True)
    b = commands.add_parser("build", help="build a repository's head for the run")
    b.add_argument("--repo", required=True, choices=REPOSITORIES)
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
    r.add_argument("--count", type=int, default=5)
    r.add_argument("--tests-url", required=True, help="where the state branch keeps the tests, for the issues")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--work", type=Path, required=True)
    u = commands.add_parser("update", help="fold a run into the state branch's checkout")
    u.add_argument("--state-dir", type=Path, required=True)
    u.add_argument("--plan", type=Path, required=True)
    u.add_argument("--results", type=Path, required=True, help="the jobs' tests folders, merged")
    u.add_argument("--run-url", default="")
    args = parser.parse_args(argv)
    configure_logging()

    if args.command == "plan":
        clones = {}
        for item in args.clone:
            repo, _, directory = item.partition("=")
            if repo not in REPOSITORIES or not directory:
                parser.error(f"--clone takes OWNER/NAME=DIR for one of {', '.join(REPOSITORIES)}, got {item!r}")
            clones[repo] = Path(directory)
        walk = list(clones) if args.walk == "all" else [args.walk]
        if VALKEY_REPO not in clones or not set(walk) <= set(clones) or not 1 <= args.max <= MAX_JOBS:
            parser.error(f"give a --clone of {VALKEY_REPO} and of each repository walked, and --max from 1 to "
                         f"{MAX_JOBS}")
        state = json.loads(args.state.read_text()) if args.state and args.state.is_file() else initial_state()
        planned = plan(clones, state, args.max, walk)
        args.out.write_text(json.dumps(planned, indent=2), encoding="utf-8")
        log_outcome(logger, logging.INFO, f"{len(planned['merges'])} merge(s) to walk, "
                                          f"{len(planned['skipped'])} without a code change")
        return 0
    if args.command == "build":
        build_head(args.repo, args.sha, args.valkey_sha, args.work, args.out)
        log_outcome(logger, logging.INFO, f"Built {args.repo} at {args.sha} into {args.out}")
        return 0
    if args.command == "update":
        record(args.state_dir, json.loads(args.plan.read_text()), args.results, now=datetime.now(timezone.utc),
               run_url=args.run_url)
        log_outcome(logger, logging.INFO, f"Updated the walk's state in {args.state_dir}")
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
                 work=args.work, count=args.count, tests_url=args.tests_url)
    filed = [f["issue"] for f in result["findings"] if f.get("issue")]
    log_outcome(logger, logging.ERROR if result.get("error") else logging.INFO,
                f"{merge['repo']} {_short(merge)}: {result['outcome']}, {result['proven']} of {result['proposed']} "
                f"test(s) reached the change, {len(filed)} issue(s)"
                + (f"; error: {result['error']}" if result.get("error") else ""))
    return 1 if result.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
