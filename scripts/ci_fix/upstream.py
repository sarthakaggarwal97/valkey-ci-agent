"""Fix a release branch's failure on the default branch first.

A backport sweep's failure that no upstream change fixes, in a test the
default branch shares, is fixed there: after the release branch's fix, a
second fix job, the default branch's own CI job, makes the same fix for it
(``engine.prepare_upstream``), and :func:`publish` opens it as a pull request
into the default branch. The sweep PR is told to backport it
once it merges. There is one such pull request a failing test, whichever
release branches fail it, and at most ``MAX_OPEN`` of the bot's pull requests
into the default branch are open at once. A fix for a test the default
branch does not have is the release branch's own, and goes to its CI-sweep
PR as before.

Once a day :func:`upkeep` keeps those pull requests going: it rebases one
that fell behind, has the fix agent address a maintainer's review of one
(the ``review`` front door: :func:`plan_request`, :func:`report_result`), and
when one is merged or closed, says so on the sweep PR of each release branch
it fixes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github
from github.GithubException import GithubException

from scripts.backport.pr_creator import create_pull_from_push_repo
from scripts.backport.sweep import _BRANCH_PREFIX as SWEEP_PREFIX
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_prs import find_existing_pr
from scripts.ci_fix.comment import candidate_context, check_items, prose, sentence
from scripts.ci_fix.gate import is_authorized
from scripts.ci_fix.issue_state import open_linked_prs
from scripts.ci_fix.models import (
    FailedTest,
    Failure,
    FixCandidate,
    FixPlan,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    OpenFix,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.push import PushFailed, PushRefused, commit_message, push_candidate
from scripts.common.github_client import retry_github_call
from scripts.common.identity import APP_LOGIN
from scripts.common.logging_utils import configure_logging
from scripts.common.valkey_ci import Source

logger = logging.getLogger(__name__)

BRANCH_PREFIX = "agent/ci-fix/upstream/"
MAX_OPEN = 3
# Whose pull requests into the default branch count against ``MAX_OPEN``.
BOT_LOGIN = f"{APP_LOGIN}[bot]"

# In a pull request's body, one a release branch failure it fixes: how the
# review front door replays the failure, and whom upkeep tells of a merge.
_FIX_MARKER = "<!-- valkey-ci-agent:upstream-fix {} -->"
_FIX_MARKER_RE = re.compile(r"<!-- valkey-ci-agent:upstream-fix (\{[^\n]*?\}) -->")
_ANNOUNCED = "<!-- valkey-ci-agent:upstream-{outcome} pr={number} -->"
_CONFLICT = "<!-- valkey-ci-agent:upstream-conflict head={head} -->"
_ANSWERED = "<!-- valkey-ci-agent:review-answered ids={ids} -->"
_ANSWERED_RE = re.compile(r"<!-- valkey-ci-agent:review-answered ids=([a-z0-9,]*) -->")

# Open pull requests the fix agent is shown, and how much of each diff.
_MAX_OPEN_FIXES = 5
_MAX_DIFF_CHARS = 6_000
# How far back upkeep looks for a merged or closed pull request.
_CLOSED_LOOKBACK = timedelta(days=14)
_MAX_REVIEW_CHARS = 6_000


def branch_for(tests: tuple[FailedTest, ...]) -> str:
    """The one branch for a fix of ``tests``' first test, whichever release branch failed it."""
    test = tests[0]
    return BRANCH_PREFIX + hashlib.sha256(f"{test.file}\0{test.name}".encode()).hexdigest()[:16]


def _author_query(bot_login: str) -> str:
    """The search qualifier for ``bot_login``'s pull requests: an App's is ``app/<slug>``."""
    return f"app/{bot_login.removesuffix('[bot]')}" if bot_login.endswith("[bot]") else bot_login


def open_count(gh: Any, repo_full_name: str, branch: str) -> int:
    """How many of the bot's pull requests into ``branch`` are open."""
    query = f"repo:{repo_full_name} is:pr is:open base:{branch} author:{_author_query(BOT_LOGIN)}"
    return int(retry_github_call(
        lambda: gh.search_issues(query).totalCount, retries=2, description=f"count the bot's PRs into {branch}",
    ))


def cap_reached(gh: Any, repo_full_name: str, branch: str) -> bool:
    return open_count(gh, repo_full_name, branch) >= MAX_OPEN


def _login(item: Any) -> str:
    return str(getattr(getattr(item, "user", None), "login", "") or "")


# --- the open pull requests a fix is shown ----------------------------------


def find_open_fixes(gh: Any, task: FixTask, tests: tuple[FailedTest, ...]) -> tuple[OpenFix, ...]:
    """Open pull requests into the default branch that name a failing test or close its detector issue.

    One of them may already fix the failure, and then the sweep waits for
    it. When they cannot be listed, the agent is shown none.
    """
    repo_full_name, branch = task.failure.repo_full_name, task.target.upstream_branch
    try:
        numbers: dict[int, None] = {}
        for test in tests:
            phrase = test.name or test.unit
            query = f'repo:{repo_full_name} is:pr is:open base:{branch} "{phrase}"'
            # A search is at most 256 characters, and a quote would end the phrase.
            if not phrase or '"' in phrase or len(query) > 256:
                continue
            numbers.update(dict.fromkeys(int(item.number) for item in retry_github_call(
                lambda: list(islice(gh.search_issues(query), _MAX_OPEN_FIXES)),
                retries=2, description=f"search open PRs naming {phrase}",
            )))
        repo = gh.get_repo(repo_full_name)
        for issue, parsed in _open_detector_issues(repo):
            if any(test in parsed.failed_tests for test in tests):
                numbers.update(dict.fromkeys(open_linked_prs(issue)))
        named = {test.file for test in tests}
        fixes = []
        for number in islice(numbers, _MAX_OPEN_FIXES):
            pull = retry_github_call(lambda: repo.get_pull(number), retries=2, description=f"get #{number}")
            if str(getattr(pull, "state", "") or "") != "open" or str(pull.base.ref) != branch:
                continue
            files = retry_github_call(lambda: list(pull.get_files()), retries=2, description=f"list #{number}'s files")
            chosen = [file for file in files if file.filename in named] or files
            diff = "\n".join(f"--- a/{file.filename}\n+++ b/{file.filename}\n{file.patch or ''}" for file in chosen)
            fixes.append(OpenFix(number=number, title=str(pull.title), diff=diff[:_MAX_DIFF_CHARS]))
        return tuple(fixes)
    except Exception as exc:  # noqa: BLE001 - without them the agent simply is not shown any
        logger.warning("Could not list the open pull requests into %s: %s", branch, exc)
        return ()


# --- publication --------------------------------------------------------------


def open_fix_result(task: FixTask, candidate: FixCandidate) -> FixResult:
    """A refusal because an open pull request into the default branch fixes it: that pull request, to backport."""
    return FixResult(
        kind=ResultKind.SUGGESTED,
        summary=f"#{candidate.open_fix}, open on {task.target.upstream_branch}, fixes it.",
        task=task, candidate=candidate, pr_number=candidate.open_fix, open_upstream=True,
    )


def publish(gh: Any, task: FixTask, candidate: FixCandidate, *, git_env: dict[str, str]) -> FixResult:
    """A sweep's own fix that was run on the default branch: a pull request into it.

    The bot's pull request for the same test is that fix, whether it is open
    (this failure is added to those it fixes) or merged (it is to be
    backported); one that was closed unmerged turned the fix down.
    """
    failure, target, branch = task.failure, task.target, task.target.upstream_branch
    if candidate.upstream_refusal:
        return FixResult(kind=ResultKind.REFUSED, summary=candidate.upstream_refusal, task=task, candidate=candidate)
    run = candidate.upstream
    if run is None:
        raise ValueError("only a fix run on the default branch is opened into it")
    head = branch_for(run.tests)
    marker = _FIX_MARKER.format(json.dumps(_marked(failure), sort_keys=True))
    try:
        repo = gh.get_repo(failure.repo_full_name)
        owner = target.repo_full_name.split("/", 1)[0]
        earlier = retry_github_call(
            lambda: list(islice(repo.get_pulls(state="all", head=f"{owner}:{head}"), 1)),
            retries=2, description=f"find the PR from {head}",
        )
        if earlier:
            return _earlier(task, candidate, earlier[0], marker)
        if open_count(gh, failure.repo_full_name, branch) >= MAX_OPEN:
            return FixResult(kind=ResultKind.FAILED, task=task, candidate=candidate, summary=(
                f"{MAX_OPEN} of the bot's pull requests into {branch} are open: the fix waits for one of them."
            ))
        _delete_branch(repo, head)
        issue = _detector_issue(repo, run.tests)
    except Exception as exc:  # noqa: BLE001 - retry can recover GitHub API failures
        return FixResult(kind=ResultKind.FAILED, task=task, candidate=candidate,
                         summary=f"The pull request into {branch} could not be prepared: {exc}")
    pushed = FixTarget(
        kind=TargetKind.DRAFT_PR, repo_full_name=target.repo_full_name,
        base_repo_full_name=failure.repo_full_name, branch=head, expected_sha=run.sha,
    )
    try:
        commit_sha = push_candidate(on_upstream(candidate), pushed, git_env=git_env)
    except PushRefused as exc:
        return FixResult(kind=ResultKind.REFUSED, summary=str(exc), task=task, candidate=candidate)
    except PushFailed as exc:
        return FixResult(kind=ResultKind.FAILED, summary=str(exc), task=task, candidate=candidate)
    try:
        pull = retry_github_call(
            lambda: create_pull_from_push_repo(
                repo, base_repo=failure.repo_full_name, push_repo=target.repo_full_name,
                title=commit_message(on_upstream(candidate)).splitlines()[0], body=_body(task, candidate, issue, marker),
                head_branch=head, base_branch=branch, draft=False,
            ),
            retries=2, description=f"open the PR from {head}",
        )
    except Exception as exc:  # noqa: BLE001 - the next attempt pushes the branch again
        return FixResult(kind=ResultKind.FAILED, task=task, candidate=candidate, commit_sha=commit_sha,
                         summary=f"The fix is on {head} ({commit_sha[:12]}), but its PR could not be opened: {exc}")
    return FixResult(
        kind=ResultKind.OPENED, summary=f"Opened #{int(pull.number)} into {branch} with the fix.",
        task=task, candidate=candidate, commit_sha=commit_sha, pr_number=int(pull.number),
        pr_url=str(getattr(pull, "html_url", "") or ""), open_upstream=True,
    )


def _marked(failure: Failure) -> dict[str, Any]:
    """What a fix marker records of ``failure``."""
    return {
        "branch": failure.base_branch, "run": failure.run_id, "attempt": failure.run_attempt,
        "job": failure.job_id, "name": failure.job_name, "workflow": failure.workflow,
        "scheduled": failure.scheduled,
    }


def _earlier(task: FixTask, candidate: FixCandidate, pull: Any, marker: str) -> FixResult:
    """The result when the bot already has a pull request for this test."""
    number, branch = int(pull.number), task.target.upstream_branch
    if getattr(pull, "merged_at", None):
        return FixResult(kind=ResultKind.SUGGESTED, task=task, candidate=candidate, pr_number=number,
                         summary=f"#{number} fixes it on {branch}; it is to be backported to {task.failure.base_branch}.")
    if str(getattr(pull, "state", "") or "") != "open":
        return FixResult(kind=ResultKind.REFUSED, task=task, candidate=candidate, summary=(
            f"#{number}, the bot's fix for this test on {branch}, was closed without merging."
        ))
    body = str(getattr(pull, "body", "") or "")
    if marker not in body:
        try:
            retry_github_call(lambda: pull.edit(body=f"{body}\n{marker}"), retries=2, description=f"describe #{number}")
        except Exception as exc:  # noqa: BLE001 - the sweep PR is told all the same
            logger.warning("Could not add %s to #%s: %s", task.failure.base_branch, number, exc)
    return FixResult(kind=ResultKind.SUGGESTED, task=task, candidate=candidate, pr_number=number,
                     summary=f"#{number}, open on {branch}, fixes it.", open_upstream=True)


def _delete_branch(repo: Any, branch: str) -> None:
    """Delete ``branch``, an earlier attempt's push whose pull request was never opened."""
    try:
        ref = repo.get_git_ref(f"heads/{branch}")
    except GithubException as exc:
        if exc.status == 404:
            return
        raise
    logger.info("Deleting %s, pushed by an earlier attempt that opened no PR", branch)
    ref.delete()


def _open_detector_issues(repo: Any) -> list[tuple[Any, Any]]:
    # Imported here: the issues front door imports publish, which imports this module.
    from scripts.ci_fix.issues import open_detector_issues

    return open_detector_issues(repo, BOT_LOGIN, limit=100)


def _detector_issue(repo: Any, tests: tuple[FailedTest, ...]) -> int:
    """The open detector issue of ``tests``' first test, or 0."""
    for _issue, parsed in _open_detector_issues(repo):
        if tests[0] in parsed.failed_tests:
            return parsed.number
    return 0


def on_upstream(candidate: FixCandidate) -> FixCandidate:
    """``candidate`` as it is on the default branch: its patch there, and what was checked there."""
    run = candidate.upstream
    if run is None:
        raise ValueError("the fix was not made for the default branch")
    return replace(
        candidate, patch=run.patch, changed_paths=run.changed_paths, title=run.title or candidate.title,
        reasoning=run.reasoning or candidate.reasoning, verification=run.verification,
        # The agent's notes on an adaptation are its session's, which are not kept.
        validation="" if run.adapted else candidate.validation,
    )


def _body(task: FixTask, candidate: FixCandidate, issue: int, marker: str) -> str:
    failure, run, branch = task.failure, candidate.upstream, task.target.upstream_branch
    assert run is not None
    release = failure.base_branch
    tests = " and ".join(f"`{test.name}`" if test.name else f"`{test.file}`" for test in run.tests)
    how = (
        f"The fix made on `{release}` does not apply here as it is, so this is the same fix written for this "
        "branch's code" if run.adapted else "The fix made there applies here as it is"
    )
    lines = [f"Fixes #{issue}", ""] if issue else []
    lines += [
        f"{tests} failed in `{prose(failure.job_name, 300)}` on `{release}` ([run]({failure.run_url})). "
        f"No change on `{branch}` fixes it. {how}: it is made here first, and backported to `{release}` once "
        "it merges.",
        "",
        *candidate_context(on_upstream(candidate), include_reasoning=True),
    ]
    if run.adapted:
        lines += [f"<details><summary>The fix made on <code>{release}</code></summary>", "",
                  "```diff", candidate.patch.rstrip("\n"), "```", "", "</details>", ""]
    lines += [
        f"<details><summary>What the bot checked on <code>{release}</code></summary>", "",
        *check_items(candidate.verification), "", "</details>", "",
        "---",
        "",
        "<sub>Prepared by the CI-fix bot, which never merges. This PR's CI and a maintainer's review decide. "
        "A review comment is answered by the bot within a day. Its commit has no DCO sign-off: whoever takes "
        "the change over signs it off.</sub>",
        "",
        marker,
    ]
    return "\n".join(lines)


# --- upkeep -------------------------------------------------------------------


def _bot_pulls(gh: Any, repo_full_name: str, branch: str, state: str, since: datetime | None = None) -> list[Any]:
    """The bot's pull requests into ``branch`` from ``BRANCH_PREFIX``, open or closed since ``since``."""
    query = f"repo:{repo_full_name} is:pr is:{state} base:{branch} author:{_author_query(BOT_LOGIN)}"
    if since is not None:
        query += f" closed:>={since.date().isoformat()}"
    found = retry_github_call(
        lambda: list(islice(gh.search_issues(query), 50)), retries=2, description=f"list the bot's {state} PRs",
    )
    repo = gh.get_repo(repo_full_name)
    pulls = [retry_github_call(lambda: repo.get_pull(int(item.number)), retries=2, description="get a PR")
             for item in found]
    return [pull for pull in pulls if str(pull.head.ref).startswith(BRANCH_PREFIX)]


def _comments(pull: Any) -> list[Any]:
    return retry_github_call(lambda: list(pull.get_issue_comments()), retries=2,
                             description=f"list comments on #{pull.number}")


def _rebase(pull: Any, repo: Any, graphql: GitHubGraphQLClient | None, *, dry_run: bool) -> str:
    """Rebase ``pull`` onto its base when it is behind; what happened."""
    head = str(pull.head.sha)
    compared = retry_github_call(lambda: repo.compare(str(pull.base.ref), head), retries=2,
                                 description=f"compare #{pull.number} with its base")
    if not int(getattr(compared, "behind_by", 0) or 0):
        return "up to date"
    if dry_run or graphql is None:
        return "would rebase"
    try:
        graphql.execute(
            "mutation($id: ID!, $head: GitObjectID!) { updatePullRequestBranch(input: "
            "{pullRequestId: $id, expectedHeadOid: $head, updateMethod: REBASE}) { pullRequest { number } } }",
            {"id": str(pull.node_id), "head": head},
        )
        return "rebased"
    except Exception as exc:  # noqa: BLE001 - a conflict is said once, on the pull request
        tag = _CONFLICT.format(head=head)
        if not any(tag in str(getattr(comment, "body", "") or "") for comment in _comments(pull)):
            retry_github_call(lambda: pull.create_issue_comment(
                f"This no longer rebases onto `{pull.base.ref}` cleanly, so the bot left it as it is.\n\n{tag}"
            ), retries=2, description=f"report the conflict on #{pull.number}")
        return f"not rebased: {exc}"


def _announce(gh: Any, repo_full_name: str, pull: Any, *, dry_run: bool) -> list[str]:
    """Tell the sweep PR of each release branch ``pull`` fixes that it was merged, or closed unmerged."""
    number, merged = int(pull.number), bool(getattr(pull, "merged_at", None))
    jobs: dict[str, list[str]] = defaultdict(list)
    for found in _FIX_MARKER_RE.finditer(str(getattr(pull, "body", "") or "")):
        fixed = json.loads(found.group(1))
        jobs[str(fixed["branch"])].append(str(fixed["name"]))
    told = []
    for release, names in jobs.items():
        sweep = find_existing_pr(gh, repo_full_name, repo_full_name, f"{SWEEP_PREFIX}/{release}")
        if sweep is None:
            continue
        tag = _ANNOUNCED.format(outcome="merged" if merged else "closed", number=number)
        if any(tag in str(getattr(comment, "body", "") or "") for comment in _comments(sweep)):
            continue
        listed = " and ".join(f"`{name}`" for name in dict.fromkeys(names))
        text = (
            f"#{number}, merged into `{pull.base.ref}`, fixes {listed}. Backport it to `{release}`."
            if merged else f"#{number} was closed without merging, so {listed} still fails here."
        )
        if not dry_run:
            retry_github_call(lambda: sweep.create_issue_comment(f"{text}\n\n{tag}"), retries=2,
                              description=f"tell #{sweep.number} of #{number}")
        told.append(f"#{sweep.number}")
    return told


def upkeep(
    gh: Any, repo_full_name: str, *, graphql: GitHubGraphQLClient | None, org: str, team: str, dry_run: bool,
) -> dict[str, Any]:
    """Rebase the bot's open pull requests into the default branch, list those with a review to address,
    and tell the sweep PRs of those merged or closed."""
    repo = gh.get_repo(repo_full_name)
    branch = str(repo.default_branch)
    report: dict[str, Any] = {"open": {}, "closed": {}, "review": []}
    for pull in _bot_pulls(gh, repo_full_name, branch, "open"):
        report["open"][pull.number] = _rebase(pull, repo, graphql, dry_run=dry_run)
        if unanswered_review(gh, pull, org=org, team=team):
            report["review"].append(int(pull.number))
    since = datetime.now(timezone.utc) - _CLOSED_LOOKBACK
    for pull in _bot_pulls(gh, repo_full_name, branch, "closed", since):
        report["closed"][pull.number] = _announce(gh, repo_full_name, pull, dry_run=dry_run)
    return report


# --- the review front door -----------------------------------------------------


def unanswered_review(gh: Any, pull: Any, *, org: str, team: str) -> list[tuple[str, str]]:
    """The review comments on ``pull`` the bot has not answered, as (id, text), oldest first.

    Reviews with a body, inline comments and comments on the pull request,
    by members of ``org/team`` only: anyone else's is not a review the bot
    acts on.
    """
    comments = _comments(pull)
    answered = {
        item for comment in comments if _login(comment) == BOT_LOGIN
        for found in _ANSWERED_RE.finditer(str(getattr(comment, "body", "") or ""))
        for item in found.group(1).split(",")
    }
    found: list[tuple[str, Any, str]] = []
    for review in retry_github_call(lambda: list(pull.get_reviews()), retries=2, description="list reviews"):
        if str(getattr(review, "body", "") or "").strip():
            found.append((f"r{review.id}", review, ""))
    for comment in retry_github_call(lambda: list(pull.get_review_comments()), retries=2,
                                     description="list review comments"):
        found.append((f"c{comment.id}", comment, f" on `{comment.path}`, line {comment.line or comment.original_line}"))
    found += [(f"i{comment.id}", comment, "") for comment in comments]
    allowed: dict[str, bool] = {}
    texts = []
    for key, item, where in found:
        author = _login(item)
        if key in answered or not author or author == BOT_LOGIN or author.endswith("[bot]"):
            continue
        if author not in allowed:
            allowed[author] = is_authorized(gh, org, team, author)
        if allowed[author]:
            texts.append((key, f"{author}{where}: {str(item.body).strip()}"))
    return texts


def plan_request(gh: Any, artifact_client: Any, request: dict[str, Any], *, bot_login: str) -> FixPlan | dict[str, Any]:
    """The fix plan that addresses the unanswered review of the bot's pull request ``request["pr"]``."""
    del artifact_client, bot_login  # the review is read from the pull request
    repo_full_name, number = str(request["repo"]), int(request["pr"])
    skipped = {"repo": repo_full_name, "pr": number, "action": "skipped"}
    pull = retry_github_call(lambda: gh.get_repo(repo_full_name).get_pull(number), retries=2,
                             description=f"get #{number}")
    head_repo = str(getattr(getattr(pull.head, "repo", None), "full_name", "") or "")
    if (
        str(pull.state) != "open" or _login(pull) != BOT_LOGIN or head_repo != repo_full_name
        or not str(pull.head.ref).startswith(BRANCH_PREFIX)
    ):
        return {**skipped, "reason": "not an open pull request of the bot's into the default branch"}
    marked = _FIX_MARKER_RE.search(str(pull.body or ""))
    if marked is None:
        return {**skipped, "reason": "the pull request names no failure it fixes"}
    review = unanswered_review(gh, pull, org=os.environ.get("CI_FIX_AUTH_ORG", "valkey-io"),
                               team=os.environ.get("CI_FIX_AUTH_TEAM", "contributors"))
    if not review:
        return {**skipped, "reason": "no unanswered review"}
    fixed = json.loads(marked.group(1))
    hint = prose("\n\n".join(text for _key, text in review), _MAX_REVIEW_CHARS)
    failure = Failure(
        repo_full_name=repo_full_name, run_id=int(fixed["run"]), run_attempt=int(fixed["attempt"]),
        head_sha=str(pull.head.sha), job_id=int(fixed["job"]), job_name=str(fixed["name"]),
        base_branch=str(pull.base.ref), pr_number=number, hint=hint,
        source=Source.DAILY if fixed["scheduled"] else None, workflow=str(fixed["workflow"]),
        description=(
            f"This failure, on release branch `{fixed['branch']}`, is fixed by the working commit, which is "
            f"the bot's pull request #{number} into `{pull.base.ref}`. Maintainers reviewed it: change the "
            "fix as the maintainer hint asks, keeping it a fix for this failure. Refuse, and say why, when "
            "the review asks for nothing to change or for what would not fix the failure."
        ),
    )
    task = FixTask(
        failure=failure,
        target=FixTarget(kind=TargetKind.UPDATE_PR, repo_full_name=repo_full_name, base_repo_full_name=repo_full_name,
                         branch=str(pull.head.ref), expected_sha=str(pull.head.sha)),
        policy=FixPolicy.GENERAL,
    )
    return FixPlan(front_door=FrontDoor.REVIEW, task=task,
                   markers=(_ANSWERED.format(ids=",".join(key for key, _text in review)),))


def report_result(gh: Any, plan: FixPlan, result: FixResult) -> dict[str, Any]:
    """Answer the review on the pull request, and mark it answered whatever the result."""
    failure = plan.task.failure
    if result.kind is ResultKind.PUSHED:
        text = f"Changed the fix as the review asked: {result.commit_sha}."
        if result.candidate is not None:
            text += "\n\n" + "\n".join(candidate_context(result.candidate, include_reasoning=True)).strip()
    elif result.kind is ResultKind.REFUSED:
        text = f"The bot left the fix as it is. {sentence(result.summary)}"
    else:
        text = f"The bot could not act on the review: {sentence(result.summary)} Comment again to have it try once more."
    pull = retry_github_call(lambda: gh.get_repo(failure.repo_full_name).get_pull(failure.pr_number), retries=2,
                             description=f"get #{failure.pr_number}")
    retry_github_call(lambda: pull.create_issue_comment(f"{text}\n\n" + "\n".join(plan.markers)), retries=3,
                      description=f"answer the review on #{failure.pr_number}")
    return {"repo": failure.repo_full_name, "pr": failure.pr_number, "action": result.kind.value,
            "summary": result.summary, "commit": result.commit_sha}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("upkeep", help="rebase, list reviews to address, and announce merges")
    run.add_argument("--repo", required=True)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--output-file", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args(argv)
    configure_logging()
    token = os.environ["TARGET_TOKEN"]
    gh = Github(auth=Auth.Token(token))
    report = upkeep(
        gh, args.repo, graphql=GitHubGraphQLClient(token), dry_run=args.dry_run,
        org=os.environ.get("CI_FIX_AUTH_ORG", "valkey-io"), team=os.environ.get("CI_FIX_AUTH_TEAM", "contributors"),
    )
    print(json.dumps(report, indent=2, default=str))
    if args.output_file:
        with open(args.output_file, "a", encoding="utf-8") as output:
            output.write(f"review={json.dumps([] if args.dry_run else report['review'])}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
