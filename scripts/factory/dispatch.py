"""Start one factory run per issue labelled ``needs-agent-fix`` that is worth an attempt, up to a cap.

Each tick reads the target repository (never writes it) and dispatches
``factory.yml`` in this repository for the newest labelled issues. An issue is
skipped while:

- a factory run for it is queued or in progress (each run is named after its
  issue and the labelling it answers, see ``RUN_NAME``),
- it is assigned or an open pull request says it fixes it,
- the bot already reported on its current labelling, or
  ``MAX_FAILED_ATTEMPTS`` of its runs for that labelling failed (a failed
  attempt is never commented on the issue): a label removed and applied again
  is a new request.

Whether the label was applied by a committer is checked by the run itself,
which can take the label off; the dispatcher only reads.

Nothing is dispatched while the bot's pull requests say it should stop
(:func:`paused`): at step ``draft-pr``, when ``max_open_prs`` of them are
open, when the last three were closed without merging, or when one was
reverted. At most ``max_runs`` factory runs are active at once.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from dataclasses import asdict
from itertools import islice
from pathlib import Path
from typing import Any, Callable

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.ci_fix.issue_dispatch import Decision, active_runs, failed_attempts
from scripts.ci_fix.issue_state import has_open_linked_pr
from scripts.ci_fix.models import MAX_FAILED_ATTEMPTS, IssuePublication
from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN
from scripts.common.logging_utils import configure_logging
from scripts.factory.main import BRANCH_PREFIX, LABEL, fix_reported, labelling

logger = logging.getLogger(__name__)

WORKFLOW = "factory.yml"
# factory.yml's run-name; the runs API does not return dispatch inputs. The
# ``run`` group is the labelling event, the occurrence a run answers.
RUN_NAME = re.compile(r"^Factory issue #(?P<issue>\d+)(?: \(label (?P<run>\d+)\))?$")
# Newest labelled issues considered per tick, and bot pull requests read for the pause rules.
_MAX_ISSUES = 100
_RECENT_PULLS = 100
# Commits of the default branch read for a revert of a merged bot pull request.
_RECENT_COMMITS = 300
_UNMERGED_IN_A_ROW = 3
_REVERT_RE = re.compile(r'^Revert "(?P<subject>.*)"')
_PR_NUMBER_RE = re.compile(r"\(#(?P<number>\d+)\)\s*$")


def open_labelled_issues(repo: Any, *, limit: int = _MAX_ISSUES) -> list[Any]:
    """The newest ``limit`` open issues with the label, pull requests left out."""
    issues = retry_github_call(
        # islice, not a slice: PyGithub's slice raises IndexError when a page comes back short.
        lambda: list(islice(repo.get_issues(state="open", labels=[LABEL]), limit)),
        retries=2,
        description=f"list open {LABEL} issues",
    )
    return [issue for issue in issues if getattr(issue, "pull_request", None) is None]


def _skip_reason(
    issue: Any, event_id: int, *, bot_login: str, running: set[int], failed: Counter[tuple[int, int]],
) -> str:
    """Why ``issue`` gets no run now ("" when it should get one). Cheapest checks first."""
    if issue.number in running:
        return f"a {WORKFLOW} run for it is in progress"
    if getattr(issue, "assignees", None):
        return f"issue #{issue.number} is assigned"
    if has_open_linked_pr(issue):
        return f"issue #{issue.number} already has an open linked pull request"
    if fix_reported(issue, event_id, bot_login=bot_login):
        return f"the bot is done with labelling {event_id}"
    if failed[(issue.number, event_id)] >= MAX_FAILED_ATTEMPTS:
        return f"{MAX_FAILED_ATTEMPTS} attempts at labelling {event_id} failed"
    return ""


def dispatch_labelled(
    target: Any,
    workflow: Any,
    *,
    bot_login: str,
    max_runs: int,
    max_open_prs: int,
    publication: IssuePublication,
    dispatch: Callable[[int, int], None],
) -> list[Decision]:
    """Dispatch runs for eligible labelled issues, newest first, until ``max_runs`` are active."""
    running = active_runs(workflow, run_name=RUN_NAME)
    slots = max_runs - len(running)
    if slots <= 0:
        logger.info("%d %s runs are active (cap %d); nothing to dispatch", len(running), WORKFLOW, max_runs)
        return []
    reason = paused(target, bot_login=bot_login, max_open_prs=max_open_prs, publication=publication)
    if reason:
        logger.info("Dispatching nothing: %s", reason)
        return [Decision(0, "paused", reason)]
    failed = failed_attempts(workflow, run_name=RUN_NAME)
    decisions = []
    for issue in open_labelled_issues(target):
        if slots <= 0:
            break
        labelled = labelling(issue)
        if labelled is None:
            decisions.append(Decision(issue.number, "skipped", "the label's application is not in the timeline"))
            continue
        event_id = labelled[0]
        reason = _skip_reason(issue, event_id, bot_login=bot_login, running=set(running), failed=failed)
        if reason:
            decisions.append(Decision(issue.number, "skipped", reason))
            continue
        dispatch(issue.number, event_id)
        decisions.append(Decision(issue.number, "dispatched"))
        slots -= 1
    return decisions


def paused(target: Any, *, bot_login: str, max_open_prs: int, publication: IssuePublication) -> str:
    """Why nothing should be dispatched ("" when the bot may go on), from the state of its pull requests.

    Only pull requests count, so only at the step that opens them.
    """
    if publication is not IssuePublication.DRAFT_PR:
        return ""
    pulls = _factory_pulls(target, bot_login)
    open_pulls = [pull for pull in pulls if str(getattr(pull, "state", "") or "") == "open"]
    if len(open_pulls) >= max_open_prs:
        return f"{len(open_pulls)} factory pull requests are open (cap {max_open_prs})"
    closed = sorted(
        (pull for pull in pulls if str(getattr(pull, "state", "") or "") == "closed"),
        key=lambda pull: getattr(pull, "closed_at", None) or 0, reverse=True,
    )
    newest = closed[:_UNMERGED_IN_A_ROW]
    if len(newest) == _UNMERGED_IN_A_ROW and all(getattr(pull, "merged_at", None) is None for pull in newest):
        numbers = ", ".join(f"#{pull.number}" for pull in newest)
        return f"the last {_UNMERGED_IN_A_ROW} factory pull requests ({numbers}) were closed without merging"
    merged = {int(pull.number) for pull in closed if getattr(pull, "merged_at", None) is not None}
    reverted = _reverted(target, merged) if merged else 0
    if reverted:
        return f"factory pull request #{reverted} was reverted"
    return ""


def _factory_pulls(target: Any, bot_login: str) -> list[Any]:
    """The bot's factory pull requests, newest first, from the repository's recent ones."""
    pulls = retry_github_call(
        lambda: list(islice(target.get_pulls(state="all", sort="created", direction="desc"), _RECENT_PULLS)),
        retries=2,
        description="list recent pull requests",
    )
    return [
        pull for pull in pulls
        if str(getattr(getattr(pull, "head", None), "ref", "") or "").startswith(BRANCH_PREFIX)
        and str(getattr(getattr(pull, "user", None), "login", "") or "") == bot_login
    ]


def _reverted(target: Any, merged: set[int]) -> int:
    """The merged factory pull request a recent default-branch commit reverts, or 0."""
    commits = retry_github_call(
        lambda: list(islice(target.get_commits(), _RECENT_COMMITS)),
        retries=2,
        description="list recent commits",
    )
    for commit in commits:
        subject = str(getattr(getattr(commit, "commit", None), "message", "") or "").splitlines()[:1]
        revert = _REVERT_RE.match(subject[0]) if subject else None
        if revert is None:
            continue
        number = _PR_NUMBER_RE.search(revert.group("subject"))
        if number and int(number.group("number")) in merged:
            return int(number.group("number"))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="the repository whose labelled issues are fixed")
    parser.add_argument("--max-runs", type=int, required=True)
    parser.add_argument("--max-open-prs", type=int, required=True)
    parser.add_argument("--step", required=True, choices=[mode.value for mode in IssuePublication],
                        help="the rollout step: comment, or draft-pr, whose pull requests the pause rules read")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.max_runs < 1 or args.max_open_prs < 1:
        parser.error("--max-runs and --max-open-prs must be at least 1")
    configure_logging()

    # The target token reads the target repository; this repository's own
    # token lists and dispatches its runs.
    target = Github(auth=Auth.Token(os.environ["TARGET_TOKEN"])).get_repo(args.repo)
    agent_repo, ref = os.environ["GITHUB_REPOSITORY"], os.environ.get("GITHUB_REF_NAME", "main")
    workflow = Github(auth=Auth.Token(os.environ["GITHUB_TOKEN"])).get_repo(agent_repo).get_workflow(WORKFLOW)
    bot_login = f"{APP_LOGIN}[bot]"

    def dispatch(issue: int, event_id: int) -> None:
        if args.dry_run:
            return
        dispatched = retry_github_call(
            lambda: workflow.create_dispatch(ref, {"issue": str(issue), "label_event": str(event_id)}),
            retries=2,
            description=f"dispatch {WORKFLOW} for issue #{issue}",
        )
        if not dispatched:
            raise RuntimeError(f"{agent_repo}/{WORKFLOW}@{ref} rejected the dispatch for #{issue}")

    decisions = dispatch_labelled(
        target, workflow, bot_login=bot_login, max_runs=args.max_runs, max_open_prs=args.max_open_prs,
        publication=IssuePublication(args.step), dispatch=dispatch,
    )
    if args.dry_run:
        decisions = [Decision(d.issue, "would dispatch") if d.action == "dispatched" else d for d in decisions]
    print(json.dumps([asdict(decision) for decision in decisions], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
