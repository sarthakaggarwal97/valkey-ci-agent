"""Monitor the latest scheduled Valkey fuzzer workflow run, or one PR batch run.

With ``--pr-batch-run``, it analyzes that run of the fuzzer's PR batch
instead, ``--change`` names what the batch tested in the issues it files, and
``--change-diff`` gives Claude the change's diff, which its triage of a run
that replied differently on the merge commit and its parent needs. With
``--pr``, the pull request the batch tested also gets what ``spec conform``
found, per spec bundle, as one comment that later runs edit in place.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.common.github_client import retry_github_call
from scripts.common.issue_dedup import IssueDedupPublisher
from scripts.common.logging_utils import configure_logging, log_group, log_outcome
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.fuzzer import issue_renderer
from scripts.fuzzer.analyzer import DISMISSING_VERDICTS, FuzzerRunAnalyzer, same_root_cause
from scripts.fuzzer.pr_batch import BATCH_ARTIFACT_PREFIX, PR_BATCH_WORKFLOW

TARGET_REPO = "valkey-io/valkey-fuzzer"
WORKFLOW_FILE = "fuzzer-run.yml"
# --pr: OWNER/NAME#NUMBER.
_PR_RE = re.compile(r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#([1-9][0-9]*)")

logger = logging.getLogger(__name__)


def _post_spec_comment(gh: Any, repo_name: str, number: int, body: str) -> tuple[str, str]:
    """Create the spec conform comment on pull request ``number`` of ``repo_name``, or edit the one an earlier
    run made."""
    pr = f"{repo_name}#{number}"
    pull = retry_github_call(lambda: gh.get_repo(repo_name).get_pull(number), retries=3, description=f"read {pr}")
    comments = retry_github_call(lambda: list(pull.get_issue_comments()), retries=3,
                                 description=f"list the comments on {pr}")
    existing = [c for c in comments if (c.body or "").startswith(issue_renderer.SPEC_COMMENT_MARKER)]
    if existing:
        retry_github_call(lambda: existing[0].edit(body), retries=3, description=f"edit the spec comment on {pr}")
        return "updated", existing[0].html_url
    comment = retry_github_call(lambda: pull.create_issue_comment(body), retries=3,
                                description=f"post the spec comment on {pr}")
    return "created", comment.html_url


def _should_publish(analysis: Any) -> bool:
    """Publish on anomalous status OR any bug-candidate triage verdict."""
    if analysis.overall_status == "anomalous":
        return True
    return analysis.triage_verdict not in DISMISSING_VERDICTS


def _publish(publisher: IssueDedupPublisher, repo: str, run_id: int, finding: Any) -> dict[str, Any]:
    """File or update the issue for one finding, when it needs one."""
    logger.info("Finding %s %s: status %s, verdict %s", finding.kind,
                finding.signature_key or ", ".join(finding.batch_runs) or "-",
                finding.overall_status, finding.triage_verdict or "none")
    item: dict[str, Any] = {
        "kind": finding.kind,
        "status": finding.overall_status,
        "verdict": finding.triage_verdict,
        "summary": finding.summary,
        "runs": list(finding.batch_runs),
        "signature_key": finding.signature_key,
        "rerun_verdict": finding.rerun_verdict,
    }
    if not _should_publish(finding):
        return item
    if not finding.incident_fingerprint:
        # Refuse to publish without a fingerprint — otherwise unrelated
        # findings would collide on a single issue.
        logger.error("A %s finding of run %s passed the publish gate but has no fingerprint; skipping",
                     finding.kind, run_id)
        item["issue_action"] = "skipped-no-fingerprint"
        return item
    render = issue_renderer.render_for(finding)
    action, url = publisher.upsert(
        repo,
        fingerprint=finding.incident_fingerprint,
        render=render,
        idempotency_key=str(run_id),
        # An issue's title names its root cause once; later occurrences add comments.
        keep_title=True,
        match_existing=lambda issues: same_root_cause(
            finding, render("", 1).title, issues, marker_namespace=issue_renderer.MARKER_NAMESPACE,
        ),
    )
    item["issue_action"] = action
    item["issue_url"] = url
    return item


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-token", default=None,
                        help="GitHub token (falls back to TARGET_TOKEN env var)")
    parser.add_argument("--output",
                        help="Write JSON result to this path instead of stdout")
    parser.add_argument("--dry-run", action="store_true",
                        help="List the latest run without analyzing or filing an issue")
    parser.add_argument("--fuzzer-repo", default=TARGET_REPO,
                        help="The valkey-fuzzer repository whose runs are read and where issues are filed")
    parser.add_argument("--pr-batch-run", type=int,
                        help="Analyze this run of the PR batch workflow instead of the latest scheduled run")
    parser.add_argument("--change", help="What the PR batch tested, such as a merged pull request, for the issues")
    parser.add_argument("--change-diff", type=Path,
                        help="A file holding the diff of what the PR batch tested, for Claude's triage of a run "
                             "that replied differently on the merge commit and its parent")
    parser.add_argument("--pr", help="The pull request the PR batch tested, OWNER/NAME#NUMBER: post what spec "
                                     "conform found there, as one comment later runs edit")
    parser.add_argument("--pr-token", default=None,
                        help="GitHub token that can comment on --pr (falls back to PR_TOKEN, then the target token)")
    args = parser.parse_args(argv)

    configure_logging()

    token = args.target_token or os.environ.get("TARGET_TOKEN", "")
    if not token:
        parser.error("--target-token or TARGET_TOKEN env var is required")
    change_diff = None
    if args.change_diff is not None:
        if not args.pr_batch_run:
            parser.error("--change-diff needs --pr-batch-run")
        change_diff = args.change_diff.read_text(encoding="utf-8", errors="replace")
    pr_target = None
    if args.pr is not None:
        if not args.pr_batch_run:
            parser.error("--pr needs --pr-batch-run")
        pr_target = _PR_RE.fullmatch(args.pr)
        if pr_target is None:
            parser.error(f"--pr must be OWNER/NAME#NUMBER, got {args.pr!r}")

    gh = Github(auth=Auth.Token(token))
    pr_token = args.pr_token or os.environ.get("PR_TOKEN", "")
    pr_gh = Github(auth=Auth.Token(pr_token)) if pr_target and pr_token else gh
    client = ArtifactClient(gh, token=token)
    publisher = IssueDedupPublisher(gh, marker_namespace=issue_renderer.MARKER_NAMESPACE)
    repo = args.fuzzer_repo
    analyzer = FuzzerRunAnalyzer(
        gh, github_token=token, artifact_client=client, change_diff=change_diff,
        known=lambda fingerprint: publisher.has_open_issue(repo, fingerprint),
    )

    if args.pr_batch_run:
        workflow = PR_BATCH_WORKFLOW
        runs = [gh.get_repo(repo).get_workflow_run(args.pr_batch_run)]
    else:
        workflow = WORKFLOW_FILE
        runs = client.list_recent_runs(repo, workflow, event="schedule", max_runs=1)
    logger.info("Fuzzer monitor for %s (%s, %s)%s", repo, workflow,
                f"run {args.pr_batch_run}" if args.pr_batch_run else "scheduled runs",
                "; dry run" if args.dry_run else "")
    if not runs:
        logger.warning("No completed %s run found on %s", workflow, repo)
    results: list[dict[str, Any]] = []
    for run in runs:
        logger.info("Latest fuzzer run %s concluded %s: %s", run.id,
                    run.conclusion or "unknown", run.html_url)
        entry: dict[str, Any] = {
            "run_id": run.id,
            "conclusion": run.conclusion or "",
            "html_url": run.html_url,
        }

        if args.dry_run:
            entry["action"] = "would-analyze"
            results.append(entry)
            continue

        try:
            with log_group(f"Analyze fuzzer run {run.id}"):
                analysis = analyzer.analyze(
                    repo, run.id, workflow_file=workflow, change=args.change,
                    artifact_name=f"{BATCH_ARTIFACT_PREFIX}{run.id}" if args.pr_batch_run else None,
                )
            logger.info("Run %s: batch %s %s; %d finding(s)", run.id, analysis.batch_status or "unreadable",
                        analysis.counts or "", len(analysis.findings))
            entry["action"] = "analyzed"
            entry["status"] = analysis.overall_status
            entry["batch_status"] = analysis.batch_status
            entry["counts"] = analysis.counts
            # The runs that tested a change across builds, by how they met it,
            # and the runs judged but not filed; the replay reports both.
            entry["change_runs"] = dict(analysis.change_runs)
            entry["recorded"] = list(analysis.recorded)
            entry["spec"] = list(analysis.spec)
            entry["findings"] = [_publish(publisher, repo, run.id, finding) for finding in analysis.findings]
            if pr_target is not None and analysis.spec:
                body = issue_renderer.render_spec_comment(
                    analysis.spec, f"fuzzer PR batch [run {run.id}]({run.html_url})",
                )
                entry["spec_comment"], entry["spec_comment_url"] = _post_spec_comment(
                    pr_gh, pr_target.group(1), int(pr_target.group(2)), body,
                )
                logger.info("Spec conform results %s on %s: %s", entry["spec_comment"], args.pr,
                            entry["spec_comment_url"])
        except Exception as exc:
            entry["action"] = "error"
            entry["error"] = str(exc)
            logger.warning("Failed to analyze run %s: %s", run.id, exc, exc_info=True)

        results.append(entry)

    output = {"target_repo": repo, "dry_run": args.dry_run, "runs": results}
    rendered = json.dumps(output, indent=2)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    log_outcome(logger, *_outcome(results, dry_run=args.dry_run))
    # Surface monitor errors via the workflow's exit code so a failed run
    # shows ❌ in the Actions tab instead of being hidden in the JSON artifact.
    return 1 if any(r.get("action") == "error" for r in results) else 0


def _outcome(results: list[dict[str, Any]], *, dry_run: bool) -> tuple[int, str]:
    if not results:
        return logging.WARNING, "Fuzzer monitor: no run to analyze"
    entry = results[-1]
    run = f"Fuzzer run {entry['run_id']}"
    if dry_run:
        return logging.INFO, f"Dry run: would analyze {run} ({entry['html_url']})"
    if entry["action"] == "error":
        return logging.ERROR, f"{run}: analysis failed: {' '.join(entry['error'].split())}"
    batch = f"batch {entry.get('batch_status') or 'unreadable'}"
    findings = entry.get("findings") or []
    change_runs = entry.get("change_runs") or {}
    if any(change_runs.values()):
        batch += ", change runs " + ", ".join(f"{k} {v}" for k, v in change_runs.items())
    issues = [f"{f['issue_action']}: {f.get('issue_url') or 'no URL'}" for f in findings if f.get("issue_action")]
    if not issues:
        return logging.INFO, f"{run}: {batch}, {len(findings)} finding(s), status {entry['status']}; no issue needed"
    return logging.WARNING, (f"{run}: {batch}, {len(findings)} finding(s), status {entry['status']}; "
                             f"issues {'; '.join(issues)}")


if __name__ == "__main__":
    raise SystemExit(main())
