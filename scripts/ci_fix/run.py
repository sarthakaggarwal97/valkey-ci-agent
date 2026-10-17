"""Run one CI fix as three jobs: plan, fix, publish.

Every front door runs its fix the same way, through ``ci-fix-run.yml``:
a Test Failure Detector issue (:mod:`scripts.ci_fix.issues`), a maintainer's
command on a pull request (:mod:`scripts.ci_fix.main`), and a bot-owned pull
request's red head (:mod:`scripts.backport.ci_followup`).

1. ``plan``: the front door validates its request and binds one exact failed
   job (a :class:`FixPlan`); this module finds the copied Valkey job that can
   host the fix (:mod:`scripts.ci_fix.copies`), or none.
2. ``fix``: :func:`scripts.ci_fix.engine.prepare_fix`, inside that copied job
   after its own setup and build (``scripts/ci_fix/job.sh``), or on the bot's
   own runner. It holds no GitHub token.
3. ``publish``: the candidate is published as its target says, and the
   front door reports it (a draft PR and an issue comment, or a push or a suggestion and
   a PR comment), also when the fix job failed or never finished.

The plan and the fix's outcome pass between the jobs as JSON
(:mod:`scripts.ci_fix.state`).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from github import Auth, Github

from scripts.backport import ci_followup
from scripts.ci_fix import issues
from scripts.ci_fix import main as pr_command
from scripts.ci_fix.copies import copy_for, fix_inputs, load_manifest
from scripts.ci_fix.engine import failed_tests, prepare_fix
from scripts.ci_fix.models import FixOutcome, FixPlan, FixResult, FrontDoor, ResultKind
from scripts.ci_fix.publish import publish_candidate
from scripts.ci_fix.reproduce import JobCommands
from scripts.ci_fix.state import load, save
from scripts.common.git_auth import GitAuth
from scripts.common.identity import APP_LOGIN
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

PLAN_FILE = "plan.json"
OUTCOME_FILE = "outcome.json"

Planner = Callable[..., "FixPlan | dict[str, Any]"]
Reporter = Callable[..., "dict[str, Any]"]

# Each front door: how it plans a request, and how the result is reported.
# The candidate is published as its target says (scripts/ci_fix/publish.py).
_FRONT_DOORS: dict[FrontDoor, tuple[Planner, Reporter]] = {
    FrontDoor.ISSUE: (issues.plan_request, issues.report_result),
    FrontDoor.PR: (pr_command.plan_request, pr_command.report_result),
    FrontDoor.FOLLOW_UP: (ci_followup.plan_request, ci_followup.report_result),
}


def plan(
    gh: Any,
    artifact_client: Any,
    front_door: FrontDoor,
    request: dict[str, Any],
    *,
    bot_login: str,
    out: Path,
) -> dict[str, str]:
    """Plan one request; the workflow's outputs.

    ``mode`` is ``copy`` (with the copied job's inputs), ``local`` (no copied
    job can host the fix), or ``done`` (the front door's result was final: a
    skip, or a refusal it already reported, with its ``action``).
    """
    planner = _FRONT_DOORS[front_door][0]
    planned = planner(gh, artifact_client, request, bot_login=bot_login)
    if not isinstance(planned, FixPlan):
        print(json.dumps(planned, indent=2))
        return {"mode": "done", "action": str(planned.get("action") or "")}
    failure = planned.task.failure
    job_log = str(artifact_client.download_job_log(failure.repo_full_name, failure.job_id) or "")
    fix_plan = replace(planned, job_log=job_log)
    save(out / PLAN_FILE, fix_plan)
    copy = copy_for(load_manifest(), fix_plan.task)
    inputs = fix_inputs(copy, fix_plan.task, failed_tests(fix_plan.task, job_log)) if copy else None
    if inputs is None:
        logger.info("No copied job can host %s; the fix is prepared on the bot's runner", failure.job_name)
        return {"mode": "local"}
    return {"mode": "copy", **inputs}


def fix(plan_dir: Path, out: Path, *, repo_dir: str = "") -> FixOutcome:
    """Prepare the planned fix, in the copied job's checkout ``repo_dir`` or a clone."""
    fix_plan = load(plan_dir / PLAN_FILE, FixPlan)
    try:
        candidate = prepare_fix(
            fix_plan.task, fix_plan.job_log,
            repo_dir=repo_dir, job=JobCommands.from_env() if repo_dir else None,
        )
        outcome = FixOutcome(candidate=candidate)
    except Exception as exc:  # noqa: BLE001 - the publish job reports every outcome
        logger.exception("CI-fix failed")
        outcome = FixOutcome(error=str(exc) or type(exc).__name__)
    save(out / OUTCOME_FILE, outcome)
    return outcome


def publish_and_report(
    gh: Any,
    plan_dir: Path,
    outcome_dir: Path,
    *,
    git_env: dict[str, str],
    run_url: str,
) -> dict[str, Any]:
    """Publish the fix job's candidate, or the reason there is none, and report it."""
    fix_plan = load(plan_dir / PLAN_FILE, FixPlan)
    try:
        outcome = load(outcome_dir / OUTCOME_FILE, FixOutcome)
    except FileNotFoundError:
        outcome = FixOutcome(error=f"the fix job ended without a result (see {run_url})")
    reporter = _FRONT_DOORS[fix_plan.front_door][1]
    if outcome.candidate is None:
        result = FixResult(
            kind=ResultKind.FAILED, summary=f"The CI-fix attempt failed: {outcome.error}", task=fix_plan.task,
        )
    else:
        try:
            result = publish_candidate(gh, fix_plan.task, outcome.candidate, git_env=git_env)
        except Exception as exc:  # noqa: BLE001 - every request gets a reported result
            logger.exception("CI-fix publication failed")
            result = FixResult(kind=ResultKind.FAILED, summary=f"The CI-fix attempt failed: {exc}",
                               task=fix_plan.task, candidate=outcome.candidate)
    return reporter(gh, fix_plan, result)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan_ = commands.add_parser("plan", help="validate a request and plan its fix")
    plan_.add_argument("--front-door", required=True, choices=[door.value for door in FrontDoor])
    plan_.add_argument("--request", required=True, help="the front door's request, as JSON")
    plan_.add_argument("--out", required=True, type=Path)
    fix_ = commands.add_parser("fix", help="prepare the planned fix; needs no GitHub token")
    fix_.add_argument("--plan", required=True, type=Path)
    fix_.add_argument("--out", required=True, type=Path)
    fix_.add_argument("--repo-dir", default="", help="the copied job's own checkout, if it hosts the fix")
    publish_ = commands.add_parser("publish", help="publish the fix and report it")
    publish_.add_argument("--plan", required=True, type=Path)
    publish_.add_argument("--outcome", required=True, type=Path)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "fix":
        outcome = fix(args.plan, args.out, repo_dir=args.repo_dir)
        return 0 if outcome.candidate is not None else 1

    token = os.environ.get("TARGET_TOKEN", "")
    if not token:
        parser.error("TARGET_TOKEN is required")
    gh = Github(auth=Auth.Token(token))
    bot_login = f"{APP_LOGIN}[bot]"
    if args.command == "plan":
        try:
            request = json.loads(args.request)
        except ValueError:
            parser.error("--request must be JSON")
        if not isinstance(request, dict):
            parser.error("--request must be a JSON object")
        outputs = plan(
            gh, ArtifactClient(gh, token=token), FrontDoor(args.front_door), request,
            bot_login=bot_login, out=args.out,
        )
        _write_outputs(outputs)
        return 1 if outputs.get("action") == ResultKind.FAILED.value else 0

    run_url = "{}/{}/actions/runs/{}".format(
        os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
        os.environ.get("GITHUB_REPOSITORY", ""), os.environ.get("GITHUB_RUN_ID", ""),
    )
    with GitAuth(token, prefix="ci-fix-git-askpass-") as auth:
        result = publish_and_report(
            gh, args.plan, args.outcome, git_env=auth.env(), run_url=run_url,
        )
    print(json.dumps(result, indent=2))
    return 1 if result.get("action") == ResultKind.FAILED.value else 0


def _write_outputs(outputs: dict[str, str]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        print(json.dumps(outputs, indent=2))
        return
    with open(path, "a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


if __name__ == "__main__":
    raise SystemExit(main())
