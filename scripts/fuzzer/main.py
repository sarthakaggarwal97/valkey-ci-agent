"""Monitor the latest scheduled Valkey fuzzer workflow run, or one PR batch run.

With ``--pr-batch-run``, it analyzes that run of the fuzzer's PR batch
instead, ``--change`` names what the batch tested in the issues it files, and
``--change-diff`` gives Claude the change's diff, which its triage of a run
that replied differently on the merge commit and its parent needs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.common.issue_dedup import IssueDedupPublisher
from scripts.common.logging_utils import configure_logging, log_group, log_outcome
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.fuzzer import issue_renderer
from scripts.fuzzer.analyzer import DISMISSING_VERDICTS, FuzzerRunAnalyzer, same_root_cause
from scripts.fuzzer.pr_batch import BATCH_ARTIFACT_PREFIX, PR_BATCH_WORKFLOW

TARGET_REPO = "valkey-io/valkey-fuzzer"
WORKFLOW_FILE = "fuzzer-run.yml"

logger = logging.getLogger(__name__)


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

    gh = Github(auth=Auth.Token(token))
    client = ArtifactClient(gh, token=token)
    analyzer = FuzzerRunAnalyzer(gh, github_token=token, artifact_client=client, change_diff=change_diff)
    publisher = IssueDedupPublisher(gh, marker_namespace=issue_renderer.MARKER_NAMESPACE)

    repo = args.fuzzer_repo
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
            entry["findings"] = [_publish(publisher, repo, run.id, finding) for finding in analysis.findings]
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
