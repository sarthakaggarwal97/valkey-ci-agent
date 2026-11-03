"""Replay merged Valkey pull requests through the fuzzer, as if each had just merged.

``plan`` lists the merges on a branch's first-parent history in a range,
oldest first, each with its parent and its pull request (from the
``(#1234)`` its squash-merge subject ends with). ``run`` replays one of them
as it would run live when it merged: Claude proposes scenarios for its change
(the diff of the merge against its parent), the fuzzer's PR batch runs them on
the merge commit beside a random baseline, and the run waits for the batch and
writes ``replay.json``, which names the batch's run and the change for the
fuzzer monitor (``--pr-batch-run``, ``--change``). Only the replay is made up:
the proposals, the batch and the monitor are the live ones.

valkey-io/valkey#4749 changed how a full sync picks its codec and transport,
and what the primary logs of it. The fuzzer's ``valkey.oracle.repl-compression``
knows only the new rules, and nothing a build reports tells the two apart, so
a merge before #4749 leaves the experiments it judges out of its batch, both
the random draws and the proposals. A live batch never tests a build that old.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.fuzzer.pr_batch import PR_BATCH_WORKFLOW, batch_dispatch_inputs, dispatch_batch, wait_for_run
from scripts.fuzzer.propose import (
    MAX_COUNT,
    REPO_RE,
    ChangeRequest,
    fetch_change,
    fuzzer_validator,
    load_catalog,
    propose,
    validated_scenarios,
    write_results,
)

logger = logging.getLogger(__name__)

REPLAY_FILE = "replay.json"
# A squash merge's subject ends with its pull request: "Fix the thing (#1234)".
_PR_SUBJECT_RE = re.compile(r"\(#([1-9][0-9]*)\)\s*$")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
# valkey-io/valkey#4749, and the experiments whose oracle models its rules.
REPL_COMPRESSION_RULES = "7fe7ca45ffc7bb0ba009a952a2f64395e45e432a"
REPL_COMPRESSION_EXPERIMENTS = (
    "valkey.repl-compression-full-sync", "valkey.repl-compression-partial-resync",
    "valkey.repl-compression-runtime-change", "valkey.replication-chaos",
)


def plan(valkey_dir: Path, commit_range: str, *, first: int = 0) -> list[dict[str, Any]]:
    """The merges in ``commit_range`` (BASE..TIP) on TIP's first-parent history, oldest first, each with
    the experiments its batch leaves out."""
    base, sep, tip = commit_range.partition("..")
    if not sep or not base or not tip:
        raise ValueError(f"a range is BASE..TIP, got {commit_range!r}")
    out = subprocess.run(
        ["git", "-C", str(valkey_dir), "log", "--first-parent", "--reverse", "--format=%H %P%x09%s",
         f"{base}..{tip}"],
        check=True, capture_output=True, text=True,
    ).stdout
    merges = []
    for index, line in enumerate(out.splitlines()):
        shas, _, subject = line.partition("\t")
        sha, parent = shas.split()[:2]
        match = _PR_SUBJECT_RE.search(subject)
        merges.append({"index": index, "sha": sha, "parent": parent,
                       "pr": int(match.group(1)) if match else None, "subject": subject[:200],
                       "exclude_experiments": excluded_experiments(valkey_dir, sha)})
    return merges[:first] if first else merges


def excluded_experiments(valkey_dir: Path, sha: str) -> list[str]:
    """The experiments a batch of ``sha`` leaves out: those whose rules ``sha`` predates."""
    check = subprocess.run(
        ["git", "-C", str(valkey_dir), "merge-base", "--is-ancestor", REPL_COMPRESSION_RULES, sha],
        capture_output=True, text=True,
    )
    if check.returncode not in (0, 1):
        raise RuntimeError(f"cannot tell whether {sha} has {REPL_COMPRESSION_RULES}: {check.stderr.strip()}")
    return [] if check.returncode == 0 else list(REPL_COMPRESSION_EXPERIMENTS)


def _experiment(text: str) -> str | None:
    doc = yaml.safe_load(text)
    experiment = doc.get("experiment") if isinstance(doc, dict) else None
    return str(experiment.get("id")) if isinstance(experiment, dict) else None


def describe(merge: dict[str, Any], repo: str) -> str:
    """How an issue names the merge: its pull request and commit."""
    where = f"{repo}#{merge['pr']}" if merge.get("pr") else repo
    return f"{where}, merged as {merge['sha'][:12]}: {merge['subject']}"


def request_id(merge: dict[str, Any]) -> str:
    return f"replay-{merge['index']:03d}-{merge['sha'][:9]}"


def run(
    merge: dict[str, Any], *, gh: Any, token: str, repo: str, fuzzer_repo: str, fuzzer_ref: str,
    fuzzer_dir: Path, fuzzer_bin: str, count: int, random_seeds: int, out_dir: Path, timeout_s: float,
) -> dict[str, Any]:
    """Replay one merge: propose scenarios for it, run them with a random baseline, wait for the batch."""
    for key in ("sha", "parent"):
        if not _SHA_RE.fullmatch(str(merge[key])):
            raise ValueError(f"merge {key} must be a full commit SHA, got {merge[key]!r}")
    result: dict[str, Any] = {**merge, "change": describe(merge, repo), "request_id": request_id(merge)}
    change = fetch_change(gh, token, ChangeRequest(repo=repo, base=merge["parent"], head=merge["sha"]))
    change.title = merge["subject"]
    proposals_dir = out_dir / "proposals"
    try:
        with tempfile.TemporaryDirectory(prefix="fuzzer-replay-") as td:
            proposal_run = propose(
                change, count=count, workdir=Path(td), out_dir=proposals_dir, catalog=load_catalog(fuzzer_bin),
                fuzzer_dir=fuzzer_dir, validator=fuzzer_validator(fuzzer_bin),
            )
        write_results(proposal_run, proposals_dir)
        summary, scenarios = validated_scenarios(proposals_dir)
        result["proposed"] = len(scenarios)
        result["dropped"] = len(proposal_run.dropped)
    except Exception as exc:
        # The random baseline still runs: a failed proposal is recorded, not fatal.
        logger.warning("Proposing scenarios for %s failed: %s", result["change"], exc, exc_info=True)
        summary, scenarios = change.summary(), {}
        result["proposal_error"] = " ".join(str(exc).split())[:500]
    excluded = list(merge.get("exclude_experiments") or [])
    if excluded:
        kept = {name: text for name, text in scenarios.items() if _experiment(text) not in excluded}
        result["excluded_proposals"] = len(scenarios) - len(kept)
        scenarios = kept
    inputs = batch_dispatch_inputs(summary, scenarios, random_seeds=random_seeds, request_id=result["request_id"],
                                   exclude_experiments=excluded)
    # GitHub's clock and ours may differ a little.
    since = datetime.now(timezone.utc) - timedelta(minutes=2)
    dispatch_batch(gh, inputs, fuzzer_repo=fuzzer_repo, workflow=PR_BATCH_WORKFLOW, ref=fuzzer_ref)
    logger.info("Dispatched %s with %d scenario(s) and %d random seed(s) per experiment",
                result["request_id"], len(scenarios), random_seeds)
    batch = wait_for_run(gh, fuzzer_repo=fuzzer_repo, workflow=PR_BATCH_WORKFLOW, request_id=result["request_id"],
                         since=since, timeout_s=timeout_s)
    result.update(run_id=batch.id, run_url=batch.html_url, conclusion=batch.conclusion)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / REPLAY_FILE).write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("plan", help="list the merges to replay as a JSON array")
    p.add_argument("--valkey-dir", required=True, help="a Valkey clone holding the whole range")
    p.add_argument("--range", required=True, help="BASE..TIP")
    p.add_argument("--first", type=int, default=0, help="replay only the first N merges (0: all)")
    p.add_argument("--out", required=True)
    r = commands.add_parser("run", help="replay one merge")
    r.add_argument("--merge", required=True, help="one entry of plan's array, as JSON")
    r.add_argument("--repo", default="valkey-io/valkey")
    r.add_argument("--fuzzer-repo", required=True)
    r.add_argument("--fuzzer-ref", required=True, help="the fuzzer branch whose PR batch workflow runs")
    r.add_argument("--fuzzer-dir", required=True)
    r.add_argument("--fuzzer-bin", required=True)
    r.add_argument("--count", type=int, default=5)
    r.add_argument("--random-seeds", type=int, default=2)
    r.add_argument("--timeout", type=float, default=5 * 3600, help="seconds to wait for the PR batch")
    r.add_argument("--out", required=True)
    r.add_argument("--target-token", default=None, help="GitHub token (falls back to TARGET_TOKEN)")
    args = parser.parse_args(argv)
    configure_logging()

    if args.command == "plan":
        merges = plan(Path(args.valkey_dir), args.range, first=args.first)
        Path(args.out).write_text(json.dumps(merges), encoding="utf-8")
        log_outcome(logger, logging.INFO, f"{len(merges)} merge(s) to replay in {args.range}")
        return 0

    for name in ("repo", "fuzzer_repo"):
        if not REPO_RE.fullmatch(getattr(args, name)):
            parser.error(f"not an owner/name repository: {getattr(args, name)!r}")
    if not 1 <= args.count <= MAX_COUNT:
        parser.error(f"--count must be 1 to {MAX_COUNT}")
    token = args.target_token or os.environ.get("TARGET_TOKEN", "")
    if not token:
        parser.error("--target-token or TARGET_TOKEN env var is required")
    from github import Auth, Github

    merge = json.loads(args.merge)
    result = run(
        merge, gh=Github(auth=Auth.Token(token)), token=token, repo=args.repo, fuzzer_repo=args.fuzzer_repo,
        fuzzer_ref=args.fuzzer_ref, fuzzer_dir=Path(args.fuzzer_dir), fuzzer_bin=args.fuzzer_bin,
        count=args.count, random_seeds=args.random_seeds, out_dir=Path(args.out), timeout_s=args.timeout,
    )
    log_outcome(logger, logging.INFO,
                f"{result['change']}: {result.get('proposed', 0)} scenario(s); PR batch "
                f"{result['conclusion']}: {result['run_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
