"""Replay issues whose fix is known, and score what the factory makes of them.

The cases are in ``replay_cases.yml``: one closed issue each, and the merged
pull request that fixed it. A replay runs the same preparation a labelled
issue gets (:mod:`scripts.factory.prepare`), and publishes nothing:

1. ``plan``: the issue as it was when the fix's pull request was opened, with
   the committers' comments up to then, on the commit that pull request
   merged onto, so the fix that landed is not there to find.
2. the preparation itself, unchanged, in ``factory-replay.yml``'s fix job.
3. ``score``: the bot's test is applied to the known fix. A test that fails
   without the fix (the fix job showed that) and passes with it caught the
   bug the person fixed. The bot's own change is compared with the known
   fix's lines, so a person reads the ones that differ.

Step 1 of the rollout runs this over the recent issues before anything is
written to Valkey.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml
from github import Auth, Github

from scripts.ci_fix.engine import added_tests, changed_lines, changed_text, is_test_path
from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FixCandidate,
    FixOutcome,
    FixPlan,
    FixPolicy,
    FixTarget,
    FixTask,
    FrontDoor,
    TargetKind,
)
from scripts.ci_fix.state import load, save
from scripts.ci_fix.verification import plan_for
from scripts.common.git_clone import is_full_sha, shallow_clone_at_sha
from scripts.common.github_client import retry_github_call
from scripts.common.proc import PROCESS_BASICS, filter_env, run_git
from scripts.factory.evidence import JOB
from scripts.factory.main import BRANCH_PREFIX, read_brief

logger = logging.getLogger(__name__)

CASES_FILE = Path(__file__).resolve().parent / "replay_cases.yml"
PLAN_FILE = "plan.json"
OUTCOME_FILE = "outcome.json"
SCORE_FILE = "score.json"
# Who counts as a committer in a replay, which reads with a token that cannot
# ask the repository: GitHub's own association of the comment's author.
_COMMITTER_ASSOCIATIONS = frozenset({"OWNER", "MEMBER", "COLLABORATOR"})
# A fix is the known fix when at least this share of the lines it adds are the known fix's.
_MATCH_SHARE = 50
_RUN_TIMEOUT_S = 30 * 60


@dataclass(frozen=True)
class Case:
    id: str
    about: str
    repo: str
    issue: int
    fix_pull: int


@dataclass(frozen=True)
class Score:
    """How one replay went."""

    case: str
    verdict: str  # caught, missed, differs, refused, failed
    detail: str
    # The bot's test, when it wrote one, and whether it passes with the known fix.
    test_written: bool = False
    test_passes_with_known_fix: bool | None = None
    # Of the bot's fix, the share of its added lines that the known fix adds too, and the files both change.
    shared_percent: int = 0
    shared_files: tuple[str, ...] = ()
    known_fix_files: tuple[str, ...] = ()


def load_cases(path: Path = CASES_FILE) -> dict[str, Case]:
    listed = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("cases") or []
    cases: dict[str, Case] = {}
    for item in listed:
        case = Case(
            id=str(item.get("id") or ""), about=str(item.get("about") or ""), repo=str(item.get("repo") or ""),
            issue=int(item.get("issue") or 0), fix_pull=int(item.get("fix_pull") or 0),
        )
        if not (case.id and case.repo and case.issue > 0 and case.fix_pull > 0) or case.id in cases:
            raise ValueError(f"replay case {case.id!r}: it needs a unique id, repo, issue and fix_pull")
        cases[case.id] = case
    return cases


def plan_case(gh: Any, case: Case) -> tuple[FixPlan, str]:
    """The plan a labelled issue would have got as of the fix's pull request, and that fix's merge commit."""
    repo = retry_github_call(lambda: gh.get_repo(case.repo), retries=2, description=f"get {case.repo}")
    issue = retry_github_call(lambda: repo.get_issue(case.issue), retries=2, description=f"get issue #{case.issue}")
    pull = retry_github_call(lambda: repo.get_pull(case.fix_pull), retries=2, description=f"get PR #{case.fix_pull}")
    opened = pull.created_at
    merge_sha = str(getattr(pull, "merge_commit_sha", "") or "")
    if not getattr(pull, "merged_at", None) or not is_full_sha(merge_sha):
        raise ValueError(f"PR #{case.fix_pull} is not merged, so its fix is not known")
    merged = retry_github_call(lambda: repo.get_commit(merge_sha), retries=2, description=f"get {merge_sha[:12]}")
    parents = list(getattr(merged, "parents", None) or [])
    if not parents:
        raise ValueError(f"{merge_sha[:12]} has no parent")
    base_sha = str(parents[0].sha)
    brief = read_brief(
        issue, event_id=case.fix_pull, labeller="replay", labelled_at=opened,
        committer=lambda comment: str(getattr(comment, "author_association", "") or "") in _COMMITTER_ASSOCIATIONS,
    )
    task = FixTask(
        failure=Failure(
            repo_full_name=case.repo, run_id=0, head_sha=base_sha, job_id=0, job_name="",
            base_branch=str(pull.base.ref), description=f"Issue #{brief.number}: {brief.title[:300]}",
            issue_number=brief.number,
        ),
        target=FixTarget(
            kind=TargetKind.DRAFT_PR, repo_full_name=case.repo, base_repo_full_name=case.repo,
            branch=f"{BRANCH_PREFIX}replay-{case.id}", expected_sha=base_sha,
        ),
        policy=FixPolicy.GENERAL,
    )
    return FixPlan(front_door=FrontDoor.FACTORY, task=task, brief=brief), merge_sha


def score(case: Case, plan: FixPlan, merge_sha: str, outcome: FixOutcome, *, run_known_fix: bool = True) -> Score:
    """Compare what the fix job prepared with the known fix."""
    candidate = outcome.candidate
    if candidate is None:
        return Score(case.id, "failed", outcome.error or "the fix job wrote no result")
    if candidate.action is CandidateAction.REFUSE:
        return Score(
            case.id, "refused", candidate.summary, test_written=bool(candidate.evidence_patch),
        )
    known = _known_fix(plan, merge_sha)
    bot_fix = _fix_only(candidate)
    shared = _shared_percent(bot_fix, known)
    files = tuple(sorted(set(changed_lines(bot_fix)) & set(changed_lines(known))))
    passes = None
    if run_known_fix and candidate.evidence_patch:
        passes = _test_passes_with(plan, merge_sha, candidate)
    verdict = "differs"
    if passes is False:
        verdict = "missed"
    elif shared >= _MATCH_SHARE:
        verdict = "caught" if passes else "match"
    elif passes:
        verdict = "caught"
    detail = (
        f"The bot's fix shares {shared}% of its added lines with #{case.fix_pull}"
        + (f", in {', '.join(files)}" if files else "") + "."
        + (" Its test passes with the known fix." if passes else
           " Its test does not pass with the known fix: it tests something else." if passes is False else "")
    )
    return Score(
        case.id, verdict, detail, test_written=bool(candidate.evidence_patch), test_passes_with_known_fix=passes,
        shared_percent=shared, shared_files=files,
        known_fix_files=tuple(sorted(path for path in changed_lines(known) if not is_test_path(path))),
    )


def _fix_only(candidate: FixCandidate) -> str:
    """The candidate's patch without the test: what is compared with the known fix's code change."""
    kept: list[str] = []
    keep = True
    for line in candidate.patch.splitlines():
        if line.startswith("diff --git "):
            path = line.split(" b/", 1)[-1]
            keep = not is_test_path(path)
        if keep:
            kept.append(line)
    return "\n".join(kept)


def _shared_percent(ours: str, known: str) -> int:
    added = changed_text(ours, "+")
    if not added:
        return 0
    return round(100 * len(added & changed_text(known, "+")) / len(added))


def _known_fix(plan: FixPlan, merge_sha: str) -> str:
    """The known fix's diff, test files included, from a clone of the repository."""
    with _clone(plan) as repo_dir:
        return run_git(repo_dir, "diff", "--no-ext-diff", "--no-textconv", f"{merge_sha}^", merge_sha).stdout


def _test_passes_with(plan: FixPlan, merge_sha: str, candidate: FixCandidate) -> bool | None:
    """Whether the bot's test passes with the known fix applied: it tests the bug the person fixed.

    None when the known fix and the test cannot be put together (they change
    the same lines of a test file) or nothing here runs the test.
    """
    tests = plan.task.failure.failed_tests or added_tests(candidate.evidence_patch)
    with _clone(plan) as repo_dir:
        try:
            run_git(repo_dir, "-c", "user.name=replay", "-c", "user.email=replay@localhost",
                    "cherry-pick", "--no-commit", merge_sha)
            run_git(repo_dir, "apply", "--3way", "--whitespace=nowarn", "-", input=candidate.evidence_patch)
        except subprocess.CalledProcessError as exc:
            logger.warning("The known fix and the bot's test do not apply together: %s", exc)
            return None
        verify = plan_for(repo_dir, tests, scheduled=False, job=JOB) if tests else None
        if verify is None:
            return None
        env = filter_env(PROCESS_BASICS)
        result = subprocess.run(
            ["bash", "-c", verify.command(1)], cwd=repo_dir, env=env, capture_output=True, text=True,
            timeout=_RUN_TIMEOUT_S, check=False,
        )
        return result.returncode == 0


class _clone:
    """A blobless clone of the plan's repository at its working commit, removed afterwards."""

    def __init__(self, plan: FixPlan) -> None:
        self._plan = plan
        self._dir: tempfile.TemporaryDirectory[str] | None = None

    def __enter__(self) -> str:
        self._dir = tempfile.TemporaryDirectory(prefix="factory-replay-")
        clone = Path(self._dir.name) / "repo"
        failure, sha = self._plan.task.failure, self._plan.task.target.expected_sha
        if not shallow_clone_at_sha(failure.repo_full_name, clone, sha):
            raise RuntimeError(f"could not clone {failure.repo_full_name} at {sha[:12]}")
        return str(clone)

    def __exit__(self, *exc: object) -> None:
        if self._dir is not None:
            self._dir.cleanup()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan_ = commands.add_parser("plan", help="plan one case as a labelled issue would have been planned")
    plan_.add_argument("--case", required=True)
    plan_.add_argument("--out", required=True, type=Path)
    score_ = commands.add_parser("score", help="compare the fix job's result with the known fix")
    score_.add_argument("--case", required=True)
    score_.add_argument("--plan", required=True, type=Path)
    score_.add_argument("--outcome", required=True, type=Path)
    score_.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    case = load_cases()[args.case]
    if args.command == "plan":
        gh = Github(auth=Auth.Token(os.environ["TARGET_TOKEN"]))
        plan, merge_sha = plan_case(gh, case)
        save(args.out / PLAN_FILE, plan)
        (args.out / "known-fix.txt").write_text(merge_sha + "\n", encoding="utf-8")
        print(json.dumps({"case": case.id, "issue": case.issue, "working_commit": plan.task.target.expected_sha,
                          "known_fix": merge_sha}, indent=2))
        return 0
    plan = load(args.plan / PLAN_FILE, FixPlan)
    merge_sha = (args.plan / "known-fix.txt").read_text(encoding="utf-8").strip()
    try:
        outcome = load(args.outcome / OUTCOME_FILE, FixOutcome)
    except (FileNotFoundError, ValueError, TypeError, KeyError) as exc:
        outcome = FixOutcome(error=f"the fix job's result could not be read ({exc})")
    result = score(case, plan, merge_sha, outcome)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / SCORE_FILE).write_text(json.dumps(asdict(result), indent=2), encoding="utf-8")
    print(json.dumps(asdict(result), indent=2))
    summary = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(f"### Replay {case.id}: **{result.verdict}**\n\n{result.detail}\n\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
