"""Entry point for the release controller.

Subcommands:

    start      Authorize, derive the version from live tags, and create (or
               reuse) the release tracking issue. Emits machine-readable
               outputs (version, stage, tag, issue, created) for workflow
               chaining into the release-notes cut.
    reconcile  Recompute release truth (notes PR, candidate SHA, required
               checks by exact SHA) and re-render the tracking issue. Short
               and idempotent; safe to run on a schedule.
    adopt      Record an authorized owner's acknowledgement of branch
               movement by adopting the exact current head as the candidate.

Returns 0 on success, 1 on refusal/failure, 2 on usage error.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.common.job_summary import emit_job_summary
from scripts.release.authorize import NotAuthorizedError
from scripts.release.models import ReleaseIntent
from scripts.release.policy import RepoReleasePolicy, load_policy
from scripts.release.publish import (
    ensure_environment_protected,
    plan_publication,
    publish_release,
    render_plan_summary,
)
from scripts.release.reconcile import (
    ReleaseControlError,
    adopt_candidate,
    reconcile_branch,
    start_release,
)

logger = logging.getLogger(__name__)

_DEFAULT_POLICY = str(Path(__file__).resolve().parents[2] / "release_policy.yml")
_DEFAULT_REPO = "valkey-io/valkey"


def _token() -> str:
    """Resolve the GitHub token from the environment variable chain."""
    return (
        os.environ.get("RELEASE_GITHUB_TOKEN", "")
        or os.environ.get("TARGET_TOKEN", "")
        or os.environ.get("GITHUB_TOKEN", "")
    )


def _emit_outputs(values: dict[str, str]) -> None:
    """Append ``key=value`` lines to ``$GITHUB_OUTPUT`` when running in Actions.

    Values are single-line by construction (regex-derived versions, GitHub
    URLs, integers); refuse anything else rather than corrupt the output file.
    """
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as fh:
        for key, value in values.items():
            if "\n" in value:
                raise ValueError(f"refusing multi-line GITHUB_OUTPUT value for {key!r}")
            fh.write(f"{key}={value}\n")


def _resolve_policy(parser: argparse.ArgumentParser, args: argparse.Namespace) -> RepoReleasePolicy:
    try:
        policies = load_policy(args.policy)
    except (OSError, ValueError) as exc:
        parser.error(f"cannot load release policy: {exc}")
    policy = policies.get(args.repo)
    if policy is None:
        parser.error(
            f"{args.repo} has no release policy in {args.policy} "
            f"(configured: {', '.join(sorted(policies))})"
        )
    return policy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=_token(),
                        help="GitHub token (App installation or PAT)")
    parser.add_argument("--policy", default=_DEFAULT_POLICY,
                        help="Path to release_policy.yml")
    parser.add_argument("--repo", default=_DEFAULT_REPO,
                        help="Target repo, owner/name")
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Start a release on a branch")
    start_p.add_argument("--branch", required=True, help="Release branch, e.g. 9.1")
    start_p.add_argument("--intent", required=True,
                         choices=[intent.value for intent in ReleaseIntent],
                         help="rc, ga, patch (security is refused: break-glass only)")
    start_p.add_argument("--actor", required=True,
                         help="GitHub login requesting the release (checked live "
                              "against the policy's authorized team)")
    start_p.add_argument("--dry-run", action="store_true",
                         help="Authorize and derive without creating the issue")

    reconcile_p = sub.add_parser("reconcile", help="Reconcile active release issues")
    reconcile_p.add_argument("--branch", default="",
                             help="Single branch to reconcile (default: every policy branch)")
    reconcile_p.add_argument("--no-actions", action="store_true",
                             help="Observe and render only; perform no progress actions")

    adopt_p = sub.add_parser("adopt", help="Adopt the moved branch head as candidate")
    adopt_p.add_argument("--branch", required=True, help="Release branch, e.g. 9.1")
    adopt_p.add_argument("--sha", required=True,
                         help="Exact current head of the branch (full 40-char SHA)")
    adopt_p.add_argument("--actor", required=True,
                         help="GitHub login acknowledging the branch movement")

    publish_p = sub.add_parser(
        "publish",
        help="Publish the ready release at its candidate SHA (protected path)",
    )
    publish_p.add_argument("--branch", required=True, help="Release branch, e.g. 9.1")
    publish_p.add_argument("--actor", required=True,
                           help="GitHub login approving the publication")
    publish_p.add_argument("--plan-only", action="store_true",
                           help="Run every validation and emit the publication plan "
                                "as approver evidence, without publishing")
    publish_p.add_argument("--expected-tag", default="",
                           help="Tag the approver saw; revalidation must reproduce it")
    publish_p.add_argument("--expected-sha", default="",
                           help="Candidate SHA the approver saw; revalidation must reproduce it")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.token:
        parser.error("a GitHub token is required (--token or RELEASE_GITHUB_TOKEN/GITHUB_TOKEN)")
    policy = _resolve_policy(parser, args)
    gh = Github(auth=Auth.Token(args.token))
    # Cross-repo work (qualification dispatch, downstream verification,
    # bundle/helm writes) uses a separately scoped token when provided, so
    # the client that touches valkey can be read-only there.
    downstream_token = os.environ.get("RELEASE_DOWNSTREAM_TOKEN", "")
    gh_downstream = Github(auth=Auth.Token(downstream_token)) if downstream_token else gh

    try:
        if args.command == "start":
            result = start_release(
                gh, policy,
                branch=args.branch,
                intent=ReleaseIntent(args.intent),
                actor=args.actor,
                dry_run=args.dry_run,
            )
            _emit_outputs({
                "version": result.version,
                "stage": result.stage,
                "tag": result.tag,
                "issue_number": str(result.issue_number),
                "issue_url": result.issue_url,
                "created": "true" if result.created else "false",
                "cut_needed": "true" if result.cut_needed else "false",
            })
            if result.created:
                logger.info("Started release %s (%s) on %s", result.version, result.stage, args.branch)
            elif result.cut_needed:
                logger.info(
                    "Resuming release on %s: tracker %s has no notes PR; requesting a (re)cut of %s",
                    args.branch, result.issue_url, result.tag,
                )
            else:
                logger.info(
                    "Duplicate start: release %s already in flight on %s, reusing issue %s",
                    result.tag, args.branch, result.issue_url,
                )
            return 0

        if args.command == "reconcile":
            branches = [args.branch] if args.branch else list(policy.branches)
            failed: list[str] = []
            for branch in branches:
                if branch not in policy.branches:
                    parser.error(
                        f"branch {branch!r} is not a configured release branch of {policy.repo}"
                    )
                # One branch's failure (deleted branch, transient API error)
                # must not skip the remaining branches until the next cron.
                try:
                    reconcile_branch(gh, policy, branch, act=not args.no_actions,
                                     gh_downstream=gh_downstream)
                except Exception:
                    logger.exception("Reconcile failed for %s %s", policy.repo, branch)
                    failed.append(branch)
            if failed:
                logger.error("Reconcile failed for branch(es): %s", ", ".join(failed))
                return 1
            return 0

        if args.command == "publish":
            # The gate must be real before either path runs: plan-only fails
            # early so the problem surfaces before anyone approves anything.
            agent_repo = os.environ.get("GITHUB_REPOSITORY", "valkey-io/valkey-ci-agent")
            # The downstream client carries the agent-repo read scope.
            ensure_environment_protected(gh_downstream, policy, agent_repo)
            if args.plan_only:
                plan = plan_publication(gh, policy, branch=args.branch,
                                        actor=args.actor, gh_downstream=gh_downstream)
                summary = render_plan_summary(plan)
                emit_job_summary(summary)
                print(summary)
                _emit_outputs({"tag": plan.tag, "sha": plan.sha,
                               "make_latest": plan.make_latest})
                return 0
            # The execute path only runs behind an approval whose evidence is
            # a specific tag and SHA; an empty binding means that evidence
            # never reached this job, and proceeding unbound defeats the gate.
            if not args.expected_tag or not args.expected_sha:
                parser.error("publish (execute) requires --expected-tag and "
                             "--expected-sha from the approved validation")
            url = publish_release(
                gh, policy, branch=args.branch, actor=args.actor,
                expected_tag=args.expected_tag, expected_sha=args.expected_sha,
                gh_downstream=gh_downstream,
            )
            logger.info("Published: %s", url)
            _emit_outputs({"release_url": url})
            return 0

        # adopt
        status = adopt_candidate(
            gh, policy, branch=args.branch, sha=args.sha, actor=args.actor,
        )
        logger.info(
            "Adopted %s as candidate on %s (ready=%s)",
            status.candidate.sha, args.branch, status.ready,
        )
        return 0
    except (ReleaseControlError, NotAuthorizedError, ValueError) as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
