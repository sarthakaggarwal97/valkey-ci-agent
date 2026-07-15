"""Credential-separated fuzzer discovery, analysis, and publication."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path, PurePosixPath
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from github import Auth, Github

from scripts.common.ai_evidence import (
    ai_evidence_directory,
    finalize_ai_evidence,
)
from scripts.common.issue_dedup import IssueDedupPublisher
from scripts.common.phase_artifact import SCHEMA_VERSION, write_json
from scripts.common.publication_manifest import (
    publisher_context,
    write_publication_manifest,
)
from scripts.common.workflow_artifacts import ArtifactClient, ArtifactState
from scripts.fuzzer import issue_renderer
from scripts.fuzzer.analyzer import (
    _build_error_analysis,
    _load_artifacts,
    analyze_context,
)
from scripts.fuzzer.models import FuzzerRunContext
from scripts.fuzzer.phase_artifact import (
    MAX_RUNS,
    load_analyzed,
    load_discovery,
)
from scripts.fuzzer.policy import should_publish
from scripts.fuzzer.schema import analysis_to_dict
from scripts.fuzzer.state import FuzzerStateStore

TARGET_REPO = "valkey-io/valkey-fuzzer"
WORKFLOW_FILE = "fuzzer-run.yml"
_MAX_SCANNED_RUNS = 1_000

logger = logging.getLogger(__name__)
_PUBLICATION_STATE_KEYS = {
    "repository",
    "workflow_file",
    "initial_cursor",
    "final_cursor",
    "runs",
}


def _select_unprocessed_runs(
    workflow: Any,
    *,
    cursor: int,
    max_runs: int = MAX_RUNS,
) -> list[Any]:
    """Return the oldest completed prefix above a durable run-ID cursor."""
    if cursor < 0:
        raise ValueError("cursor must be non-negative")
    if not 1 <= max_runs <= MAX_RUNS:
        raise ValueError(f"max_runs must be between 1 and {MAX_RUNS}")

    newer: list[Any] = []
    scanned = 0
    for run in workflow.get_runs(event="schedule"):
        scanned += 1
        if scanned > _MAX_SCANNED_RUNS:
            raise RuntimeError(
                "fuzzer run backlog exceeds the bounded discovery window",
            )
        run_id = getattr(run, "id", None)
        if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
            raise RuntimeError("GitHub returned a fuzzer run with an invalid ID")
        if cursor == 0:
            # Establish a new ledger at the newest completed event. Runs after
            # this point are then processed consecutively without gaps.
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


def discover(
    *,
    token: str,
    output_directory: Path,
    max_runs: int = MAX_RUNS,
) -> dict[str, Any]:
    gh = Github(auth=Auth.Token(token))
    state = FuzzerStateStore(gh).read(TARGET_REPO)
    repo = gh.get_repo(TARGET_REPO)
    workflow = repo.get_workflow(WORKFLOW_FILE)
    runs = _select_unprocessed_runs(workflow, cursor=state.cursor, max_runs=max_runs)
    client = ArtifactClient(gh, token=token)

    output_directory.mkdir(parents=True, exist_ok=True)
    manifest_runs = [
        _discover_run(client, run, output_directory)
        for run in runs
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "fuzzer-discovery",
        "repository": TARGET_REPO,
        "workflow_file": WORKFLOW_FILE,
        "expected_cursor": state.cursor,
        "bootstrap": state.cursor == 0,
        "runs": manifest_runs,
    }
    write_json(output_directory / "discovery.json", manifest)
    load_discovery(output_directory)
    return manifest


def _discover_run(
    client: ArtifactClient,
    run: Any,
    output_directory: Path,
) -> dict[str, Any]:
    run_id = int(run.id)
    base = {
        "run_id": run_id,
        "run_url": str(run.html_url),
        "conclusion": str(run.conclusion or ""),
        "head_sha": str(run.head_sha or ""),
    }
    artifacts = client.list_run_artifacts(TARGET_REPO, run_id)
    bundles = [
        artifact
        for artifact in artifacts
        if artifact.name.startswith("fuzzer-run-artifacts") and not artifact.expired
    ]
    if not bundles:
        return {
            **base,
            "evidence_status": "missing",
            "evidence_error": "no fuzzer artifact bundle found",
            "files": [],
        }
    if len(bundles) != 1:
        return {
            **base,
            "evidence_status": "empty",
            "evidence_error": "multiple fuzzer artifact bundles found",
            "files": [],
        }
    destination = output_directory / "runs" / str(run_id) / "artifacts"
    download = client.download_artifact(
        TARGET_REPO,
        bundles[0].artifact_id,
        destination=destination,
    )
    if download.state is not ArtifactState.AVAILABLE:
        status = (
            "oversized"
            if download.state is ArtifactState.OVERSIZED
            else "missing"
            if download.state in {ArtifactState.NOT_FOUND, ArtifactState.EXPIRED}
            else "empty"
        )
        return {
            **base,
            "evidence_status": status,
            "evidence_error": (
                f"fuzzer artifact {download.state.value}: {download.detail}"
            )[:2_000],
            "files": [],
        }
    if not download.members:
        return {
            **base,
            "evidence_status": "empty",
            "evidence_error": "fuzzer artifact contains no files",
            "files": [],
        }

    refs: list[dict[str, Any]] = []
    for member in download.members:
        relative = member.path.relative_to(output_directory).as_posix()
        refs.append({
            "path": relative,
            "sha256": member.sha256,
            "bytes": member.size,
        })
    return {
        **base,
        "evidence_status": "ready",
        "evidence_error": "",
        "files": refs,
    }


def analyze(artifact_directory: Path) -> dict[str, Any]:
    discovery = load_discovery(artifact_directory)
    refs: list[dict[str, Any]] = []
    with ai_evidence_directory(artifact_directory):
        for run in discovery.runs:
            context = FuzzerRunContext(
                repo=discovery.repository,
                workflow_file=discovery.workflow_file,
                run_id=run.run_id,
                run_url=run.run_url,
                conclusion=run.conclusion,
                head_sha=run.head_sha,
            )
            if run.evidence_status == "ready":
                files = {
                    item.relative_path: item.path.read_bytes()
                    for item in run.files
                }
                _load_artifacts(context, files)
                result = analyze_context(context)
            else:
                result = _build_error_analysis(context, run.evidence_error)

            relative = PurePosixPath("analyses", f"{run.run_id}.json")
            digest = write_json(
                artifact_directory / Path(*relative.parts),
                analysis_to_dict(result),
            )
            refs.append({
                "run_id": run.run_id,
                "file": relative.as_posix(),
                "sha256": digest,
            })
    ai_evidence_file, ai_evidence_sha = finalize_ai_evidence(
        artifact_directory,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "fuzzer-analyzed",
        "discovery_sha256": discovery.manifest_sha256,
        "analyses": refs,
        "ai_evidence_file": ai_evidence_file,
        "ai_evidence_sha256": ai_evidence_sha,
    }
    write_json(artifact_directory / "analyzed.json", manifest)
    load_analyzed(artifact_directory)
    return manifest


def publish(*, token: str, artifact_directory: Path) -> dict[str, Any]:
    artifact = load_analyzed(artifact_directory)
    publisher_context_value = publisher_context()
    gh = Github(auth=Auth.Token(token))
    state_store = FuzzerStateStore(gh)
    state = state_store.read(artifact.discovery.repository)
    valid_resume_cursors = {
        artifact.discovery.expected_cursor,
        *(item.analysis.run_id for item in artifact.analyses),
    }
    if state.cursor not in valid_resume_cursors:
        raise RuntimeError(
            "durable fuzzer cursor does not match this analyzed batch",
        )

    publisher = IssueDedupPublisher(
        gh,
        marker_namespace=issue_renderer.MARKER_NAMESPACE,
    )
    results: list[dict[str, Any]] = []
    cursor = state.cursor
    for item in artifact.analyses:
        analysis = item.analysis
        if analysis.run_id <= cursor:
            results.append({
                "run_id": analysis.run_id,
                "run_url": analysis.run_url,
                "head_sha": analysis.head_sha,
                "analysis_sha256": item.sha256,
                "action": "already-processed",
                "issue_action": "unchanged",
                "issue_url": "",
            })
            continue
        entry: dict[str, Any] = {
            "run_id": analysis.run_id,
            "run_url": analysis.run_url,
            "head_sha": analysis.head_sha,
            "analysis_sha256": item.sha256,
            "action": "processed",
            "issue_url": "",
        }
        if should_publish(analysis):
            if not analysis.incident_fingerprint:
                raise RuntimeError(
                    f"run {analysis.run_id} is publishable but has no fingerprint",
                )
            action, url = publisher.upsert(
                artifact.discovery.repository,
                fingerprint=analysis.incident_fingerprint,
                render=issue_renderer.render_for(analysis),
                idempotency_key=(
                    f"{artifact.discovery.workflow_file}:{analysis.run_id}"
                ),
            )
            entry["issue_action"] = action
            entry["issue_url"] = url
        else:
            entry["issue_action"] = "not-required"

        state_store.advance(
            artifact.discovery.repository,
            expected_cursor=cursor,
            run_id=analysis.run_id,
            run_url=analysis.run_url,
            analysis_sha256=item.sha256,
        )
        cursor = analysis.run_id
        results.append(entry)
    final_state = {
        "workflow_file": artifact.discovery.workflow_file,
        "repository": artifact.discovery.repository,
        "initial_cursor": state.cursor,
        "final_cursor": cursor,
        "runs": results,
    }
    write_publication_manifest(
        artifact_directory,
        kind="fuzzer-publication",
        source_manifest_file=artifact.manifest_path.name,
        source_manifest_sha256=artifact.manifest_sha256,
        publisher=publisher_context_value,
        final_state=final_state,
        final_state_keys=_PUBLICATION_STATE_KEYS,
    )
    return final_state


def _write_github_output(path: str | None, *, run_count: int) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"run_count={run_count}\n")
        handle.write(f"has_runs={'true' if run_count else 'false'}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover_parser = subparsers.add_parser("discover")
    discover_parser.add_argument("--output-directory", type=Path, required=True)
    discover_parser.add_argument("--max-runs", type=int, default=MAX_RUNS)
    discover_parser.add_argument("--github-output")

    analyze_parser = subparsers.add_parser("analyze")
    analyze_parser.add_argument("--artifact-directory", type=Path, required=True)

    preflight_parser = subparsers.add_parser("preflight-publish")
    preflight_parser.add_argument("--artifact-directory", type=Path, required=True)

    publish_parser = subparsers.add_parser("publish")
    publish_parser.add_argument("--artifact-directory", type=Path, required=True)
    publish_parser.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "discover":
        token = os.environ.get("READ_GITHUB_TOKEN", "")
        if not token:
            parser.error("READ_GITHUB_TOKEN is required")
        result = discover(
            token=token,
            output_directory=args.output_directory,
            max_runs=args.max_runs,
        )
        _write_github_output(args.github_output, run_count=len(result["runs"]))
        print(json.dumps({
            "expected_cursor": result["expected_cursor"],
            "run_ids": [item["run_id"] for item in result["runs"]],
        }))
        return 0
    if args.command == "analyze":
        result = analyze(args.artifact_directory)
        print(json.dumps({"analyses": len(result["analyses"])}))
        return 0
    if args.command == "preflight-publish":
        artifact = load_analyzed(args.artifact_directory)
        print(json.dumps({
            "manifest_sha256": artifact.manifest_sha256,
            "runs": len(artifact.analyses),
        }))
        return 0

    token = os.environ.get("PUBLISH_GITHUB_TOKEN", "")
    if not token:
        parser.error("PUBLISH_GITHUB_TOKEN is required")
    result = publish(token=token, artifact_directory=args.artifact_directory)
    rendered = json.dumps(result, indent=2)
    if args.output:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
