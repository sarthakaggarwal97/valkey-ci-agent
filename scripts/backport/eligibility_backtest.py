"""Measure backport eligibility against what maintainers actually backported.

Replays eligibility, in dry run and blind to existing backports, over the PRs
merged to the development branch in a past window, then compares the
high-confidence suggestions with ground truth per release branch:

* the PR's commits landed on the branch or its open backport sweep (a
  cherry-pick naming the PR), or
* the PR sits in the branch's backport project (needs a token that can read
  org projects; without one, ground truth is landings only and the report
  says so).

A branch cut after the PR merged carries it by ancestry, and a branch not
yet GA (no ``<branch>.N`` release tag) takes nearly every change; both are left out
of both sides for that PR.

Per-branch evidence and failure sites are read from each branch as it was
when the PR merged, as the daily run would have seen it; today's tip may
already carry the fix.

Production never comments on a PR maintainers already took to a GA branch.
The backtest mirrors that from history: a PR whose commits reached a GA
branch or its sweep within ``--triage-days`` of merging (the daily run looks
1-3 days after a merge) is "triaged" and gets no suggestion. Reports precision and recall per branch and lists
every false positive with the model's reason. Nothing is written to GitHub.

Example:

    python -m scripts.backport.eligibility_backtest --since 2026-05-01 \\
        --until 2026-08-31 --repo-dir /path/to/valkey-clone --out backtest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ai.claude_code import run_claude_code
from scripts.backport import eligibility
from scripts.backport.eligibility import _REMOTE_PREFIX, _SWEEP_PREFIX, BranchHistory, Candidate, MergedSourcePR
from scripts.backport.mark_done import branch_pr_index
from scripts.backport.registry import RepoEntry, load_registry
from scripts.backport.sweep_git import clone_target_branch
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.common.git_auth import GitAuth
from scripts.common.logging_utils import configure_logging
from scripts.common.proc import git_output

logger = logging.getLogger(__name__)

# The daily query without the fields only a commenting run needs; project
# items are added back when the token can read org projects.
_PROJECT_ITEMS = """
        projectItems(first: 50) {
          pageInfo { hasNextPage }
          nodes { project { number owner { ... on Organization { login } ... on User { login } } } }
        }
"""
_QUERY = """
query($q: String!, $cursor: String) {
  search(query: $q, type: ISSUE, first: 50, after: $cursor) {
    nodes {
      ... on PullRequest {
        number title body url mergedAt
        author { login }
        mergeCommit { oid }
        commits(first: 100) { pageInfo { hasNextPage } nodes { commit { oid } } }
        closingIssuesReferences(first: 5) { nodes { number title body } }
        %s
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


@dataclass
class BranchScore:
    tp: int = 0
    fp: int = 0
    fn: int = 0

    @property
    def precision(self) -> float | None:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else None

    @property
    def recall(self) -> float | None:
        return self.tp / (self.tp + self.fn) if self.tp + self.fn else None


@dataclass
class PrOutcome:
    number: int
    title: str
    truth: set[str]
    evaluated: list[str]
    action: str
    predicted: set[str] = field(default_factory=set)
    offered: set[str] = field(default_factory=set)
    confidence: str = ""
    reason: str = ""
    why: dict[str, str] = field(default_factory=dict)


def discover(
    gql: Any, *, repo: str, base_branch: str, since: str, until: str, project_owner: str,
    with_projects: bool,
) -> list[MergedSourcePR]:
    """PRs merged in [since, until], one month-sized search at a time (search caps at 1000)."""
    query = _QUERY % (_PROJECT_ITEMS if with_projects else "")
    prs: dict[int, MergedSourcePR] = {}
    start = date.fromisoformat(since)
    end = date.fromisoformat(until)
    while start <= end:
        stop = min(end, start + timedelta(days=30))
        q = (f"repo:{repo} is:pr is:merged base:{base_branch} "
             f"merged:{start.isoformat()}T00:00:00Z..{stop.isoformat()}T23:59:59Z")
        cursor = None
        while True:
            page = gql.execute(query, {"q": q, "cursor": cursor}).get("search") or {}
            for node in page.get("nodes") or []:
                pr = eligibility._pr_from_node(node, project_owner)
                if pr is not None:
                    prs[pr.number] = pr
            info = page.get("pageInfo") or {}
            if not info.get("hasNextPage"):
                break
            cursor = info.get("endCursor")
        start = stop + timedelta(days=1)
    return [prs[n] for n in sorted(prs)]


def ground_truth(
    source: MergedSourcePR,
    branches: Sequence[str],
    history: BranchHistory,
    projects: dict[str, int],
    with_projects: bool,
    ga: dict[str, datetime] | None = None,
) -> tuple[list[str], set[str]]:
    """Return (branches evaluated for this PR, branches that got the PR).

    A branch is evaluated when it was GA before the PR merged (with *ga*) and
    did not inherit the merge commit by being cut after it.
    """
    merged = datetime.fromisoformat(source.merged_at.replace("Z", "+00:00"))
    evaluated = [
        b for b in branches
        if (ga is None or (b in ga and ga[b] <= merged))
        and b not in history.containing(source.pr.merge_commit_sha)
    ]
    truth = {b for b in evaluated if source.number in history.present(b, {source.number: source.merged_at})}
    if with_projects and source.project_numbers:
        truth |= {b for b in evaluated if projects.get(b) in source.project_numbers}
    return evaluated, truth


def branch_refs_at(repo_dir: str, branches: Sequence[str], when: str) -> dict[str, str]:
    """Each branch as it was at *when*: what the daily run read after the merge.

    Today's tip may already carry the fix, which would hide the very code a
    suggestion has to point at.
    """
    refs = {}
    for branch in branches:
        sha = git_output(repo_dir, "rev-list", "-1", "--first-parent", f"--before={when}",
                         f"origin/{branch}").strip()
        if sha:
            refs[branch] = sha
    return refs


def first_landings(repo_dir: str, branches: Sequence[str]) -> dict[str, dict[int, datetime]]:
    """Per branch, when each PR first reached it or its backport sweep."""
    out: dict[str, dict[int, datetime]] = {}
    for branch in branches:
        index = branch_pr_index(repo_dir, f"origin/{branch}", earliest=True)
        sweep = f"{_REMOTE_PREFIX}{_SWEEP_PREFIX}/{branch}"
        if git_output(repo_dir, "for-each-ref", "--format=%(refname)", sweep).strip():
            for number, when in branch_pr_index(repo_dir, sweep, earliest=True).items():
                index[number] = min(when, index.get(number, when))
        out[branch] = index
    return out


def triaged_at_run(
    source: MergedSourcePR, evaluated: Sequence[str], landings: dict[str, dict[int, datetime]], days: int,
) -> list[str]:
    """GA branches the PR reached within *days* of merging: what the daily run would see."""
    merged = datetime.fromisoformat(source.merged_at.replace("Z", "+00:00"))
    by = merged + timedelta(days=days)
    return [b for b in evaluated if (when := landings.get(b, {}).get(source.number)) and when <= by]


def backtest(
    *,
    repo_entry: RepoEntry,
    repo_dir: str,
    prs: Sequence[MergedSourcePR],
    with_projects: bool,
    use_model: bool = True,
    run_fn: Callable[..., tuple[str, str, int]] = run_claude_code,
    verdict_cache: str = "",
    post_ga_only: bool = True,
    triage_days: int = 3,
) -> list[PrOutcome]:
    branches = eligibility._existing_branches(
        repo_dir,
        sorted((b.branch for b in repo_entry.branches), key=eligibility._branch_sort_key, reverse=True),
    )
    projects = {b.branch: b.project_number for b in repo_entry.branches}
    history = BranchHistory(repo_dir)
    ga = eligibility.ga_dates(repo_dir, branches) if post_ga_only else None
    landings = first_landings(repo_dir, branches) if triage_days >= 0 else {}
    triaged: dict[int, list[str]] = {}
    refs: dict[int, dict[str, str]] = {}
    outcomes: dict[int, PrOutcome] = {}
    candidates: list[Candidate] = []
    for source in prs:
        try:
            evaluated, truth = ground_truth(source, branches, history, projects, with_projects, ga)
        except subprocess.SubprocessError as exc:
            # A merge commit the development branch no longer carries.
            outcomes[source.number] = PrOutcome(source.number, source.pr.title, set(), [],
                                                action="merge-commit-unreadable", reason=str(exc)[:200])
            continue
        outcome = PrOutcome(source.number, source.pr.title, truth, evaluated, action="")
        outcomes[source.number] = outcome
        if not evaluated:
            outcome.action = "no-branch-predates-merge"
            continue
        if triage_days >= 0 and (taken := triaged_at_run(source, evaluated, landings, triage_days)):
            triaged[source.number] = taken
        try:
            refs[source.number] = branch_refs_at(repo_dir, evaluated, source.merged_at)
            assessed = eligibility.assess(repo_dir, source, evaluated, history=history, skip_landed=False,
                                          branch_refs=refs[source.number])
        except Exception as exc:  # report, never abort a long backtest
            outcome.action, outcome.reason = "tracing-failed", str(exc)[:200]
            continue
        if isinstance(assessed, dict):
            outcome.action = "triaged" if source.number in triaged else assessed["action"]
            outcome.reason = str(assessed.get("detail", ""))
            continue
        outcome.offered = set(assessed.branches)
        outcome.action = "candidate"
        candidates.append(assessed)

    logger.info("%d PR(s), %d candidate(s) for the model", len(prs), len(candidates))
    if not use_model:
        return list(outcomes.values())

    cache: dict[str, Any] = {}
    if verdict_cache and os.path.exists(verdict_cache):
        cache = json.loads(Path(verdict_cache).read_text())
    # A missing verdict (None) is retried, as the daily run's overlap would.
    todo = [c for c in candidates if cache.get(str(c.source.number)) is None]
    for start in range(0, len(todo), eligibility._BATCH_SIZE):
        batch = todo[start:start + eligibility._BATCH_SIZE]
        verdicts = eligibility.judge(batch, repo=repo_entry.repo, repo_dir=repo_dir, run_fn=run_fn,
                                     branch_refs=refs)
        for c in batch:
            v = verdicts.get(c.source.number)
            cache[str(c.source.number)] = None if v is None else {
                "backport": v.backport, "branches": list(v.branches), "reason": v.reason,
                "confidence": v.confidence, "why": dict(v.why), "dropped": list(v.dropped),
                "sites": [list(site) for site in v.sites],
            }
        if verdict_cache:
            Path(verdict_cache).write_text(json.dumps(cache, indent=2))
        logger.info("Judged %d of %d candidate(s)", min(start + len(batch), len(todo)), len(todo))

    for c in candidates:
        outcome = outcomes[c.source.number]
        v = cache.get(str(c.source.number))
        if v is None:
            outcome.action = "no-verdict"
            continue
        outcome.reason, outcome.confidence, outcome.why = v["reason"], v["confidence"], v["why"]
        if c.source.number in triaged:
            outcome.action = "triaged"  # what the model adds is information only
            continue
        if not v["backport"]:
            outcome.action = "not-eligible"
        elif v["confidence"] != "high" or not v["branches"]:
            outcome.action = "not-commented"
        else:
            outcome.action = "would-comment"
            outcome.predicted = set(v["branches"])
    return list(outcomes.values())


def score(outcomes: Sequence[PrOutcome], *, use: str = "predicted") -> dict[str, BranchScore]:
    """Per-branch and overall ("all") counts for the predicted (or offered) branches."""
    scores: dict[str, BranchScore] = {}
    for outcome in outcomes:
        guessed = getattr(outcome, use)
        for branch in outcome.evaluated:
            s = scores.setdefault(branch, BranchScore())
            total = scores.setdefault("all", BranchScore())
            hit, real = branch in guessed, branch in outcome.truth
            for target in (s, total):
                target.tp += hit and real
                target.fp += hit and not real
                target.fn += real and not hit
    return scores


def report(outcomes: Sequence[PrOutcome], *, since: str, until: str, with_projects: bool) -> str:
    def fmt(value: float | None) -> str:
        return "n/a" if value is None else f"{value:.2f}"

    lines = [f"# Backport eligibility backtest, {since} to {until}", ""]
    lines.append(
        "Ground truth: PR landed on the branch"
        + (" or is in its backport project." if with_projects else
           " (project membership not read: the token lacks read:project).")
    )
    lines += ["", f"PRs: {len(outcomes)}", ""]
    for title, use in (("Comments (high confidence)", "predicted"), ("History + evidence filter", "offered")):
        scores = score(outcomes, use=use)
        lines += [f"## {title}", "", "| Branch | TP | FP | FN | Precision | Recall |", "| --- | --- | --- | --- | --- | --- |"]
        for branch in sorted(scores, key=lambda b: (b == "all", eligibility._branch_sort_key(b)), reverse=False):
            s = scores[branch]
            lines.append(f"| {branch} | {s.tp} | {s.fp} | {s.fn} | {fmt(s.precision)} | {fmt(s.recall)} |")
        lines.append("")
    lines += ["## False positives", ""]
    for o in outcomes:
        for branch in sorted(o.predicted - o.truth):
            lines.append(f"- #{o.number} `{branch}` ({o.title}): {o.why.get(branch) or o.reason}")
    lines += ["", "## Missed backports", ""]
    for o in outcomes:
        missed = sorted(o.truth - o.predicted)
        if missed:
            lines.append(f"- #{o.number} {', '.join(missed)} ({o.title}): {o.action}"
                         + (f", {o.reason}" if o.reason else ""))
    actions: dict[str, int] = {}
    for o in outcomes:
        actions[o.action] = actions.get(o.action, 0) + 1
    lines += ["", "## Outcomes", ""] + [f"- {a}: {n}" for a, n in sorted(actions.items())]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default="repos.yml")
    parser.add_argument("--repo", default="valkey-io/valkey")
    parser.add_argument("--since", required=True, help="First UTC merge date YYYY-MM-DD")
    parser.add_argument("--until", required=True, help="Last UTC merge date YYYY-MM-DD")
    parser.add_argument("--token", default=os.environ.get("TARGET_TOKEN") or os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--repo-dir", default="", help="Existing clone with origin/<release> refs")
    parser.add_argument("--with-projects", action="store_true", help="Read backport projects (read:project)")
    parser.add_argument("--no-model", action="store_true", help="Score the history and evidence filter only")
    parser.add_argument("--verdict-cache", default="", help="JSON file reused across runs")
    parser.add_argument("--out", default="", help="Write the outcomes as JSON here")
    parser.add_argument("--triage-days", type=int, default=3,
                        help="Days after merge the daily run looks; -1 disables the triage rule")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if not args.token:
        parser.error("--token/TARGET_TOKEN/GITHUB_TOKEN is required")
    configure_logging(verbose=args.verbose)

    repo_entry = load_registry(args.registry).get_repo(args.repo)
    gql = GitHubGraphQLClient(args.token)
    with tempfile.TemporaryDirectory(prefix="eligibility-backtest-") as tmp, ExitStack() as stack:
        repo_dir = args.repo_dir
        if not repo_dir:
            repo_dir = os.path.join(tmp, "repo")
            auth = stack.enter_context(GitAuth(args.token, prefix="eligibility-backtest-askpass-"))
            clone_target_branch(repo_entry.repo, "unstable", repo_dir, auth.env())
        prs = discover(gql, repo=repo_entry.repo, base_branch="unstable", since=args.since,
                       until=args.until, project_owner=repo_entry.project_owner,
                       with_projects=args.with_projects)
        outcomes = backtest(repo_entry=repo_entry, repo_dir=repo_dir, prs=prs,
                            with_projects=args.with_projects, use_model=not args.no_model,
                            verdict_cache=args.verdict_cache, triage_days=args.triage_days)
    print(report(outcomes, since=args.since, until=args.until, with_projects=args.with_projects))
    if args.out:
        Path(args.out).write_text(json.dumps([
            {**o.__dict__, "truth": sorted(o.truth), "predicted": sorted(o.predicted),
             "offered": sorted(o.offered)}
            for o in outcomes
        ], indent=2))


if __name__ == "__main__":
    main()
