"""PR review workflow entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github  # noqa: E402

from scripts.common.github_client import retry_github_call  # noqa: E402
from scripts.review.specialist_reviewer import ReviewResult, SpecialistReviewer  # noqa: E402

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Run specialist review on a pull request."""
    parser = argparse.ArgumentParser(description="9-specialist parallel PR review")
    parser.add_argument("--repo", required=True, help="Repository full name (owner/repo)")
    parser.add_argument("--pr-number", type=int, required=True, help="PR number to review")
    parser.add_argument("--token", required=True, help="GitHub token")
    parser.add_argument("--checkout-dir", default=".", help="Local checkout directory")
    args = parser.parse_args(argv)

    gh = Github(auth=Auth.Token(args.token))
    repo = gh.get_repo(args.repo)
    pr = retry_github_call(lambda: repo.get_pull(args.pr_number), retries=3, description="get PR")

    # Fetch diff and changed files
    pr_files = retry_github_call(lambda: list(pr.get_files()), retries=3, description="get PR files")
    diff_parts = []
    for f in pr_files:
        if f.patch:
            diff_parts.append(f"diff --git a/{f.filename} b/{f.filename}\n--- a/{f.filename}\n+++ b/{f.filename}\n{f.patch}")
    diff = "\n".join(diff_parts)
    changed_files = [f.filename for f in pr_files]

    # Run review
    reviewer = SpecialistReviewer()
    result: ReviewResult = reviewer.review(diff, changed_files, args.checkout_dir)

    # Post summary comment
    pr.create_issue_comment(result.markdown_summary)

    # Post inline comments via review API
    if result.findings:
        comments = []
        for finding in result.findings:
            if finding.line and finding.path:
                comments.append({
                    "path": finding.path,
                    "position": finding.line,
                    "body": f"**[{finding.severity.upper()}] {finding.title}**\n\n{finding.description}",
                })
        if comments:
            pr.create_review(body="Specialist review findings", comments=comments, event="COMMENT")  # type: ignore[arg-type]

    logger.info("Review complete: %s (%d findings)", result.verdict, len(result.findings))
    return 0 if result.verdict == "Ready to Merge" else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    sys.exit(main())
