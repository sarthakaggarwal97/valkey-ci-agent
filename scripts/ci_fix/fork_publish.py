"""Fork test only, do not merge: publish a replayed fix to the fork's test sweep PR, as the follow-up would.

The eval (``scripts.ci_fix.eval``) replays a real failure of valkey-io's 9.2
sweep PR and never publishes it. This takes its plan and outcome, moves the
task onto the fork's copy of that sweep (the same tree), and publishes it
through the follow-up's own code: the CI-sweep PR, the sweep PR's one line.

Every write goes to FORK, as the user who owns the token; nothing else is
written to, and the task is checked for it before anything is sent.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from github import Auth, Github

from scripts.backport import ci_followup, ci_sweep
from scripts.ci_fix import policy, publish
from scripts.ci_fix.github_runs import FailedJob
from scripts.ci_fix.models import Failure, FixOutcome, FixPlan, FixPolicy, FixTarget, FrontDoor, TargetKind
from scripts.ci_fix.run import OUTCOME_FILE, PLAN_FILE
from scripts.ci_fix.state import load
from scripts.common.git_auth import GitAuth

FORK = "sarthakaggarwal97/valkey"
RELEASE = "9.2-replay"
SWEEP_PR = 533
SWEEP = f"agent/backport/sweep/{RELEASE}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--outcome", type=Path, required=True)
    args = parser.parse_args()
    token = os.environ["FORK_TOKEN"]
    gh = Github(auth=Auth.Token(token))
    login = gh.get_user().login
    # The token's user stands in for the bot on the fork: the test sweep PR is theirs.
    policy.is_bot_author = lambda author: author == login  # type: ignore[assignment]
    publish.is_bot_author = policy.is_bot_author  # type: ignore[assignment]
    ci_followup.is_bot_author = policy.is_bot_author  # type: ignore[assignment]
    # The failed run is valkey-io's, not the fork's: its links go there.
    origin = "https://github.com/valkey-io/valkey/actions/runs"
    Failure.run_url = property(  # type: ignore[method-assign,assignment]
        lambda self: f"{origin}/{self.run_id}" + (f"/attempts/{self.run_attempt}" if self.run_attempt > 1 else "")
    )

    replayed = load(args.plan / PLAN_FILE, FixPlan)
    outcome = load(args.outcome / OUTCOME_FILE, FixOutcome)
    sweep = gh.get_repo(FORK).get_pull(SWEEP_PR)
    failure = replace(
        replayed.task.failure, repo_full_name=FORK, pr_number=SWEEP_PR, head_sha=sweep.head.sha, base_branch=RELEASE,
    )
    target = FixTarget(
        kind=TargetKind.UPDATE_PR, repo_full_name=FORK, base_repo_full_name=FORK, branch=SWEEP,
        expected_sha=sweep.head.sha, fix_branch=ci_sweep.ci_sweep_branch(RELEASE),
    )
    task = replace(replayed.task, failure=failure, target=target, policy=FixPolicy.BACKPORT)
    if {task.failure.repo_full_name, task.target.repo_full_name, task.target.base_repo_full_name} != {FORK}:
        raise SystemExit("refusing: the task is not the fork's")
    run = SimpleNamespace(id=failure.run_id, path=f".github/workflows/{failure.workflow or 'daily'}.yml")
    plan = FixPlan(
        front_door=FrontDoor.FOLLOW_UP, task=task,
        markers=tuple(ci_followup._markers(failure, run, (FailedJob(failure.job_name, id=failure.job_id),))),
    )
    if outcome.candidate is None:
        print(json.dumps({"published": False, "error": outcome.error}))
        return 0
    with GitAuth(token, prefix="fork-publish-askpass-") as auth:
        result = publish.publish_candidate(gh, task, outcome.candidate, git_env=auth.env())
    print(json.dumps(ci_followup.report_result(gh, plan, result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
