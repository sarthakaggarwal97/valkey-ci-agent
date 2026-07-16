"""Process the pending scheduled Valkey fuzzer workflow-run backlog."""

from __future__ import annotations

import argparse
import hashlib
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
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.fuzzer import issue_renderer
from scripts.fuzzer.analyzer import FuzzerRunAnalyzer
from scripts.fuzzer.state import FuzzerStateStore

TARGET_REPO = "valkey-io/valkey-fuzzer"
WORKFLOW_FILE = "fuzzer-run.yml"

# Verdicts that should NOT produce an issue. Everything else does — including
# `needs-human-triage` (Claude failed and there are unresolved signals).
_NO_PUBLISH_VERDICTS = frozenset({"expected-chaos-noise", "environmental-or-infra"})
_MAX_BATCH_RUNS = 20
_MAX_SCANNED_RUNS = 1_000

logger = logging.getLogger(__name__)


def _should_publish(analysis: Any) -> bool:
    """Publish on anomalous status OR any bug-candidate triage verdict."""
    if analysis.overall_status == "anomalous":
        return True
    return analysis.triage_verdict not in _NO_PUBLISH_VERDICTS


def _select_unprocessed_runs(
    workflow: Any,
    *,
    cursor: int,
    max_runs: int,
) -> list[Any]:
    """Return the oldest completed prefix above a durable run-ID cursor."""
    if cursor < 0:
        raise ValueError("cursor must be non-negative")
    if not 1 <= max_runs <= _MAX_BATCH_RUNS:
        raise ValueError(f"max_runs must be between 1 and {_MAX_BATCH_RUNS}")

    newer: list[Any] = []
    for scanned, run in enumerate(workflow.get_runs(event="schedule"), start=1):
        if scanned > _MAX_SCANNED_RUNS:
            raise RuntimeError("fuzzer run backlog exceeds the discovery window")
        run_id = getattr(run, "id", None)
        if (
            not isinstance(run_id, int)
            or isinstance(run_id, bool)
            or run_id <= 0
        ):
            raise RuntimeError("GitHub returned a fuzzer run with an invalid ID")
        if cursor == 0:
            if str(getattr(run, "status", "")) == "completed":
                return [run]
            continue
        if run_id <= cursor:
            break
        newer.append(run)

    newer.sort(key=lambda item: item.id)
    completed_prefix: list[Any] = []
    for run in newer:
        if str(getattr(run, "status", "")) != "completed":
            break
        completed_prefix.append(run)
        if len(completed_prefix) == max_runs:
            break
    return completed_prefix


def _result_digest(entry: dict[str, Any]) -> str:
    payload = json.dumps(
        entry,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-token", default=None,
                        help="GitHub token (falls back to TARGET_TOKEN env var)")
    parser.add_argument("--output",
                        help="Write JSON result to this path instead of stdout")
    parser.add_argument("--dry-run", action="store_true",
                        help="List pending runs without analyzing or filing issues")
    parser.add_argument(
        "--max-runs",
        type=int,
        default=2,
        help="Maximum oldest-first backlog runs to process (default: 2)",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.max_runs <= _MAX_BATCH_RUNS:
        parser.error(f"--max-runs must be between 1 and {_MAX_BATCH_RUNS}")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    token = args.target_token or os.environ.get("TARGET_TOKEN", "")
    if not token:
        parser.error("--target-token or TARGET_TOKEN env var is required")

    gh = Github(auth=Auth.Token(token))
    client = ArtifactClient(gh, token=token)
    analyzer = FuzzerRunAnalyzer(gh, github_token=token, artifact_client=client)
    publisher = IssueDedupPublisher(gh, marker_namespace=issue_renderer.MARKER_NAMESPACE)
    state_store = FuzzerStateStore(gh)
    state = state_store.read(TARGET_REPO)

    repo = gh.get_repo(TARGET_REPO)
    workflow = repo.get_workflow(WORKFLOW_FILE)
    runs = _select_unprocessed_runs(
        workflow,
        cursor=state.cursor,
        max_runs=args.max_runs,
    )
    results: list[dict[str, Any]] = []
    cursor = state.cursor
    bootstrap_anchor: int | None = None
    if runs and state.cursor == 0 and not args.dry_run:
        initialized = state_store.initialize(
            TARGET_REPO,
            first_run_id=runs[0].id,
            first_run_url=runs[0].html_url,
        )
        cursor = initialized.cursor
        bootstrap_anchor = cursor
    for run in runs:
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
            analysis = analyzer.analyze(TARGET_REPO, run.id, workflow_file=WORKFLOW_FILE)
            entry["action"] = "analyzed"
            entry["status"] = analysis.overall_status
            entry["verdict"] = analysis.triage_verdict
            entry["summary"] = analysis.summary
            if _should_publish(analysis):
                if not analysis.incident_fingerprint:
                    raise RuntimeError(
                        f"run {run.id} passed the publish gate without a fingerprint"
                    )
                action, url = publisher.upsert(
                    TARGET_REPO,
                    fingerprint=analysis.incident_fingerprint,
                    render=issue_renderer.render_for(analysis),
                    idempotency_key=str(run.id),
                )
                entry["issue_action"] = action
                entry["issue_url"] = url
            else:
                entry["issue_action"] = "not-required"

            result_sha256 = _result_digest(entry)
            state_store.advance(
                TARGET_REPO,
                expected_cursor=cursor,
                run_id=run.id,
                run_url=run.html_url,
                result_sha256=result_sha256,
            )
            cursor = run.id
            entry["result_sha256"] = result_sha256
            entry["cursor_action"] = "advanced"
        except Exception as exc:
            entry["action"] = "error"
            entry["error"] = str(exc)
            logger.warning("Failed to analyze run %s: %s", run.id, exc, exc_info=True)
            results.append(entry)
            break

        results.append(entry)

    output = {
        "target_repo": TARGET_REPO,
        "dry_run": args.dry_run,
        "initial_cursor": state.cursor,
        "final_cursor": cursor,
        "bootstrap_anchor": bootstrap_anchor,
        "runs": results,
    }
    rendered = json.dumps(output, indent=2)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    # Surface monitor errors via the workflow's exit code so a failed run
    # shows ❌ in the Actions tab instead of being hidden in the JSON artifact.
    return 1 if any(r.get("action") == "error" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
