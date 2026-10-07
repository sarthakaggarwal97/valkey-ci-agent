"""CLI used by release preparation and protected publication workflows."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from github import Auth, Github
from github.GithubException import GithubException

from scripts.common.github_actions import write_outputs
from scripts.common.job_summary import emit_job_summary
from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.release.authorize import NotAuthorizedError
from scripts.release.models import ReleaseIntent
from scripts.release.policy import load_policy
from scripts.release.publish import (
    ReleaseError,
    plan_digest,
    plan_publication,
    prepare_release,
    publish_release,
    render_plan,
)

_ROOT = Path(__file__).resolve().parents[2]

logger = logging.getLogger(__name__)


def _token() -> str:
    return os.environ.get("RELEASE_GITHUB_TOKEN", "") or os.environ.get("GITHUB_TOKEN", "")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=_token())
    parser.add_argument("--policy", default=str(_ROOT / "release_policy.yml"))
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare", help="derive the next release identity")
    prepare.add_argument("--branch", required=True)
    prepare.add_argument("--intent", required=True, choices=[item.value for item in ReleaseIntent])
    prepare.add_argument("--actor", required=True)

    plan = sub.add_parser("plan", help="validate and render a publication plan")
    plan.add_argument("--branch", required=True)
    plan.add_argument("--candidate-sha", required=True)

    publish = sub.add_parser("publish", help="revalidate and publish an approved plan")
    publish.add_argument("--branch", required=True)
    publish.add_argument("--candidate-sha", required=True)
    publish.add_argument("--actor", required=True)
    publish.add_argument("--expected-digest", required=True)

    args = parser.parse_args(argv)
    if not args.token:
        parser.error("a GitHub token is required")
    try:
        policy = load_policy(args.policy)
    except (OSError, ValueError) as exc:
        parser.error(f"cannot load release policy: {exc}")

    configure_logging()
    gh = Github(auth=Auth.Token(args.token))
    try:
        if args.command == "prepare":
            release = prepare_release(
                gh,
                policy,
                branch=args.branch,
                intent=ReleaseIntent(args.intent),
                actor=args.actor,
            )
            write_outputs({"version": release.version, "stage": release.stage, "tag": release.tag})
            log_outcome(logger, logging.INFO, "Prepared %s on %s", release.tag, args.branch)
            return 0
        if args.command == "plan":
            publication = plan_publication(
                gh,
                policy,
                branch=args.branch,
                candidate_sha=args.candidate_sha,
            )
            summary = render_plan(publication)
            emit_job_summary(summary)
            print(summary)
            write_outputs(
                {
                    "version": publication.tag,
                    "tag": publication.tag,
                    "sha": publication.sha,
                    "plan_digest": plan_digest(publication),
                }
            )
            return 0
        if args.command == "publish":
            url = publish_release(
                gh,
                policy,
                branch=args.branch,
                candidate_sha=args.candidate_sha,
                actor=args.actor,
                expected_digest=args.expected_digest,
                )
            write_outputs({"release_url": url})
            log_outcome(logger, logging.INFO, "Published the %s release at %s (approved by @%s): %s",
                        args.branch, args.candidate_sha[:12], args.actor, url)
            return 0
        raise AssertionError(args.command)
    except (ReleaseError, NotAuthorizedError, ValueError, GithubException, ConnectionError) as exc:
        log_outcome(logger, logging.ERROR, "Release %s refused: %s", args.command,
                    " ".join(str(exc).split()))
        return 1


if __name__ == "__main__":
    sys.exit(main())
