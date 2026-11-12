"""Replay merged Valkey pull requests through the fuzzer, as if each had just merged.

``plan`` lists the merges on a branch's first-parent history in a range,
oldest first, or the merges given by SHA in the order given, each with its
parent and its pull request (from the ``(#1234)`` its squash-merge subject
ends with). ``run`` replays one of them
as it would run live when it merged: Claude proposes scenarios for its change
(the diff of the merge against its parent), and experiments of its own when
no installed one reaches it; the fuzzer's PR batch runs them on the merge
commit, with the parent built beside it for the experiments that compare the
two, beside a random baseline; and the run waits for the batch and writes
``replay.json``, which names the batch's run and the change for the fuzzer
monitor (``--pr-batch-run``, ``--change``), beside the merge's diff
(``change.diff``, for ``--change-diff``). The monitor's result for the
batch counts the runs that reached the change, did not, or showed an
unexplained difference (``change_runs``). Only the replay is made up: the
proposals, the batch and the monitor are the live ones.

The merges of a module repository the PR batch builds (valkey-json) replay
the same way, on one pinned Valkey build: ``plan`` resolves a valkey-io/valkey
ref once and gives every merge that commit (``valkey_sha``), and each merge's
batch builds the module at the merge and its parent and loads both builds
into it, without a Valkey parent. The #4749 exclusion is for Valkey merges.
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
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.fuzzer.pr_batch import (
    BUILDS,
    MODULE_FILES,
    PR_BATCH_WORKFLOW,
    ModuleBuild,
    batch_dispatch_inputs,
    dispatch_batch,
    module_repository,
    wait_for_run,
)
from scripts.fuzzer.propose import (
    MAX_COUNT,
    REPO_RE,
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

logger = logging.getLogger(__name__)

REPLAY_FILE = "replay.json"
CHANGE_DIFF_FILE = "change.diff"
# GitHub runs at most this many jobs from one matrix, and the workflow replays one merge per job.
MAX_MERGES = 256
# A squash merge's subject ends with its pull request: "Fix the thing (#1234)".
_PR_SUBJECT_RE = re.compile(r"\(#([1-9][0-9]*)\)\s*$")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
# A merge given to plan by hand may be abbreviated; the clone resolves it.
_MERGE_SHA_RE = re.compile(r"[0-9a-f]{7,40}")
# The Valkey a module's merges load into: a branch, a tag or a full commit, fetched from here.
_REF_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,99}")
VALKEY_URL = f"https://github.com/{VALKEY_REPO}.git"


def plan(
    repo_dir: Path, commit_range: str = "", *, first: int = 0, merges: Sequence[str] = (),
    repository: str = VALKEY_REPO, valkey_sha: str = "",
) -> list[dict[str, Any]]:
    """The merges to replay, each with the experiments its batch leaves out: the commits in ``commit_range``
    (BASE..TIP) on TIP's first-parent history, oldest first, or the given ``merges`` (commit SHAs, resolved in
    the clone) in the order given. ``repo_dir`` is a clone of ``repository``: Valkey, or a module repository,
    whose merges each load into ``valkey_sha`` and leave nothing out."""
    name = repository.split("/")[-1]
    if name not in MODULE_FILES:
        raise ValueError(f"{repository} is neither Valkey nor a module the PR batch builds ({', '.join(MODULE_FILES)})")
    is_module = module_repository(repository)
    if is_module and not _SHA_RE.fullmatch(valkey_sha):
        raise ValueError(f"a replay of {repository} needs the full Valkey commit its merges load into")
    git = ["git", "-C", str(repo_dir)]
    if merges:
        lines = []
        for sha in merges:
            if not _MERGE_SHA_RE.fullmatch(sha):
                raise ValueError(f"a merge is a commit SHA of 7 to 40 lowercase hex characters, got {sha!r}")
            resolved = subprocess.run([*git, "rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}"],
                                      capture_output=True, text=True)
            if resolved.returncode != 0:
                raise ValueError(f"{sha} is not a commit in {repo_dir}")
            lines += subprocess.run([*git, "log", "-1", "--format=%H %P%x09%s", resolved.stdout.strip()],
                                    check=True, capture_output=True, text=True).stdout.splitlines()
        what = f"the {len(merges)} merge(s) given"
    else:
        base, sep, tip = commit_range.partition("..")
        if not sep or not base or not tip:
            raise ValueError(f"a range is BASE..TIP, got {commit_range!r}")
        lines = subprocess.run(
            [*git, "log", "--first-parent", "--reverse", "--format=%H %P%x09%s", f"{base}..{tip}"],
            check=True, capture_output=True, text=True,
        ).stdout.splitlines()
        what = commit_range
    planned = []
    for index, line in enumerate(lines):
        shas, _, subject = line.partition("\t")
        sha, parent = shas.split()[:2]
        match = _PR_SUBJECT_RE.search(subject)
        entry = {"index": index, "sha": sha, "parent": parent, "pr": int(match.group(1)) if match else None,
                 "subject": subject[:200]}
        planned.append({**entry, "valkey_sha": valkey_sha} if is_module else entry)
    planned = planned[:first] if first else planned
    if len(planned) > MAX_MERGES:
        raise ValueError(
            f"{what} holds {len(planned)} merges; a replay runs at most {MAX_MERGES} (GitHub's matrix "
            "limit). Replay a shorter range, or set first."
        )
    return planned


def resolve_commit(ref: str, url: str) -> str:
    """The commit ``ref`` (a branch, a tag or a full commit SHA) names in the repository at ``url``."""
    if not _REF_RE.fullmatch(ref) or ".." in ref:
        raise ValueError(f"not a branch, tag or commit: {ref!r}")
    with tempfile.TemporaryDirectory(prefix="fuzzer-replay-valkey-") as td:
        subprocess.run(["git", "init", "-q", td], check=True, capture_output=True)
        fetched = subprocess.run(["git", "-C", td, "fetch", "-q", "--depth", "1", url, ref],
                                 capture_output=True, text=True)
        if fetched.returncode != 0:
            raise ValueError(f"cannot fetch {ref} from {url}: {fetched.stderr.strip()[:300]}")
        return subprocess.run(["git", "-C", td, "rev-parse", "FETCH_HEAD^{commit}"], check=True,
                              capture_output=True, text=True).stdout.strip()


def describe(merge: dict[str, Any], repo: str) -> str:
    """How an issue names the merge: its pull request and commit."""
    where = f"{repo}#{merge['pr']}" if merge.get("pr") else repo
    return f"{where}, merged as {merge['sha'][:12]}: {merge['subject']}"


def request_id(merge: dict[str, Any], replay_run: str = "") -> str:
    """The name the merge's PR batch is dispatched with; ``replay_run`` (the replay workflow's run ID) keeps
    two replays of one range, started close together, from adopting each other's batches."""
    run = f"{replay_run}-" if replay_run else ""
    return f"replay-{run}{merge['index']:03d}-{merge['sha'][:9]}"


def run(
    merge: dict[str, Any], *, gh: Any, token: str, repo: str, fuzzer_repo: str, fuzzer_ref: str,
    fuzzer_dir: Path, fuzzer_bin: str, count: int, random_seeds: int, out_dir: Path, timeout_s: float,
    build: str = "default",
) -> dict[str, Any]:
    """Replay one merge: propose scenarios for it, run them with a random baseline on ``build`` of the merge
    commit and its parent, and wait for the batch. A merge of a module repository runs on its pinned Valkey
    build, which loads the module built at the merge and at its parent."""
    is_module = module_repository(repo)
    for key in ("sha", "parent", "valkey_sha") if is_module else ("sha", "parent"):
        if not _SHA_RE.fullmatch(str(merge.get(key))):
            raise ValueError(f"merge {key} must be a full commit SHA, got {merge.get(key)!r}")
    result: dict[str, Any] = {**merge, "change": describe(merge, repo), "request_id": request_id(merge, os.environ.get("GITHUB_RUN_ID", "")),
                              "build": build}
    change = fetch_change(gh, token, ChangeRequest(repo=repo, base=merge["parent"], head=merge["sha"]))
    change.title = merge["subject"]
    # The monitor's triage judges an unexplained difference against it (--change-diff).
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / CHANGE_DIFF_FILE).write_text(change.diff, encoding="utf-8")
    proposals_dir = out_dir / "proposals"
    experiments: dict[str, str] = {}
    module: ModuleBuild | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="fuzzer-replay-") as td:
            proposal_run = propose(
                change, count=count, workdir=Path(td), out_dir=proposals_dir, catalog=load_catalog(fuzzer_bin),
                fuzzer_dir=fuzzer_dir, validator=fuzzer_validator(fuzzer_bin), lister=fuzzer_lister(fuzzer_bin),
                valkey_sha=str(merge.get("valkey_sha") or ""),
            )
        write_results(proposal_run, proposals_dir)
        # The tests/modules build a Valkey merge's proposals load, if any.
        module = proposal_run.module
        summary, scenarios, experiments = validated_scenarios(proposals_dir)
        result["proposed"] = len(scenarios)
        result["dropped"] = len(proposal_run.dropped)
        result["experiments"] = len(experiments)
        result["dropped_experiments"] = len(proposal_run.dropped_experiments)
        if proposal_run.reason:
            result["reason"] = proposal_run.reason
    except Exception as exc:
        # The random baseline still runs: a failed proposal is recorded, not fatal.
        logger.warning("Proposing scenarios for %s failed: %s", result["change"], exc, exc_info=True)
        summary, scenarios = change.summary(), {}
        result["proposal_error"] = " ".join(str(exc).split())[:500]
    parent_sha: str | None = merge["parent"]
    if is_module:
        # One pinned Valkey build, and the module built at the merge and at its parent, whatever was proposed.
        module = ModuleBuild(repo, merge["sha"], merge["parent"])
        summary, parent_sha = {"repo": VALKEY_REPO, "pr_number": None, "head_sha": merge["valkey_sha"]}, None
    if not scenarios and not random_seeds:
        # Nothing to run: no scenario survived and there is no random baseline. Recorded, not dispatched.
        result["skipped"] = "no scenario to run and random_seeds is 0"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / REPLAY_FILE).write_text(json.dumps(result, indent=2), encoding="utf-8")
        logger.info("%s: %s", result["change"], result["skipped"])
        return result
    inputs = batch_dispatch_inputs(summary, scenarios, random_seeds=random_seeds, request_id=result["request_id"],
                                   parent_sha=parent_sha, build=build, experiments=experiments or None,
                                   module=module)
    # GitHub's clock and ours may differ a little.
    since = datetime.now(timezone.utc) - timedelta(minutes=2)
    dispatch_batch(gh, inputs, fuzzer_repo=fuzzer_repo, workflow=PR_BATCH_WORKFLOW, ref=fuzzer_ref)
    logger.info("Dispatched %s with %d scenario(s), %d experiment(s) and %d random seed(s) per experiment, "
                "build %s", result["request_id"], len(scenarios), len(experiments), random_seeds, build)

    def record(**fields: Any) -> None:
        """Write replay.json now: a wait that fails later still leaves the batch's run for the triage."""
        result.update(fields)
        (out_dir / REPLAY_FILE).write_text(json.dumps(result, indent=2), encoding="utf-8")

    record(dispatched=True)
    try:
        batch = wait_for_run(
            gh, fuzzer_repo=fuzzer_repo, workflow=PR_BATCH_WORKFLOW, request_id=result["request_id"],
            since=since, timeout_s=timeout_s,
            on_found=lambda run: record(run_id=run.id, run_url=run.html_url, conclusion=None),
        )
    except Exception as exc:
        record(wait_error=" ".join(str(exc).split())[:500])
        raise
    record(run_id=batch.id, run_url=batch.html_url, conclusion=batch.conclusion)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("plan", help="list the merges to replay as a JSON array")
    p.add_argument("--repo-dir", required=True, help="a clone of --repository holding the whole range")
    p.add_argument("--repository", default=VALKEY_REPO, help="Valkey, or a module repository the PR batch builds")
    p.add_argument("--valkey-ref", default="unstable",
                   help="for a module repository: the valkey-io/valkey branch, tag or commit its merges load into")
    p.add_argument("--range", default="", help="BASE..TIP")
    p.add_argument("--merges", default="", help="instead of a range: commit SHAs, separated by commas or spaces")
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
    r.add_argument("--build", choices=BUILDS, default="default", help="the build of the merge commit and its parent")
    r.add_argument("--timeout", type=float, default=5 * 3600, help="seconds to wait for the PR batch")
    r.add_argument("--out", required=True)
    r.add_argument("--target-token", default=None, help="GitHub token (falls back to TARGET_TOKEN)")
    args = parser.parse_args(argv)
    configure_logging()

    if args.command == "plan":
        given = args.merges.replace(",", " ").split()
        if bool(given) == bool(args.range):
            parser.error("give exactly one of --range and --merges")
        if not REPO_RE.fullmatch(args.repository):
            parser.error(f"not an owner/name repository: {args.repository!r}")
        # Resolved once, so every merge of the replay loads into the same Valkey build.
        valkey_sha = resolve_commit(args.valkey_ref, VALKEY_URL) if module_repository(args.repository) else ""
        merges = plan(Path(args.repo_dir), args.range, first=args.first, merges=given, repository=args.repository,
                      valkey_sha=valkey_sha)
        Path(args.out).write_text(json.dumps(merges), encoding="utf-8")
        log_outcome(logger, logging.INFO, f"{len(merges)} merge(s) of {args.repository} to replay in "
                                          f"{args.range or 'the merges given'}"
                                          + (f", loaded into Valkey {valkey_sha}" if valkey_sha else ""))
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
        build=args.build,
    )
    log_outcome(logger, logging.INFO,
                f"{result['change']}: {result.get('proposed', 0)} scenario(s), {result.get('experiments', 0)} experiment(s)"
                + (f"; skipped: {result['skipped']}" if result.get("skipped")
                   else f"; PR batch {result['conclusion']}: {result['run_url']}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
