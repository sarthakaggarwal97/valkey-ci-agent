"""Credential-separated CI-fix discovery, AI, validation, and publication."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml
from github import Auth, Github

from scripts.ci_fix.apply import apply_fix
from scripts.ci_fix.comment import render_comment
from scripts.ci_fix.diagnose import diagnose_failure
from scripts.ci_fix.gate import GateRejection, build_fix_request, parse_command
from scripts.ci_fix.models import (
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    ReviewVerdict,
    RunResult,
)
from scripts.ci_fix.phase_artifact import (
    PreparedFixArtifact,
    load_discovery,
    load_prepared,
    load_validated,
)
from scripts.ci_fix.port_discovery import discover_port_candidates
from scripts.ci_fix.port_policy import (
    ALLOWED_BRANCH_PREFIX,
    verify_portable_commit,
)
from scripts.ci_fix.review import (
    DEFAULT_VERIFY_RUNS,
    build_and_review_patch,
    precheck_command,
    reproduce_failure,
    reset_worktree,
    verify_repeatedly,
)
from scripts.ci_fix.runner import (
    prepare_verification_runtime,
    verification_runtime_contract,
)
from scripts.ci_fix.selection import canonical_candidate_sha, match_failed_job
from scripts.ci_fix.verify.base import VerifyEnv, backend_label
from scripts.ci_fix.verify.job_metadata import resolve_workflow_job
from scripts.common.ai_evidence import (
    ai_evidence_directory,
    finalize_ai_evidence,
)
from scripts.common.desired_comments import (
    DesiredComment,
    reconcile_desired_comment,
    record_desired_comment,
)
from scripts.common.execution_identity import agent_execution_identity
from scripts.common.git_auth import GitAuth, github_https_url
from scripts.common.phase_artifact import (
    MAX_PATCH_BYTES,
    SCHEMA_VERSION,
    ArtifactError,
    canonical_json_bytes,
    sha256_bytes,
    write_json,
)
from scripts.common.proc import (
    BOT_EMAIL,
    BOT_NAME,
    GitPathEncodingError,
    decode_git_paths,
    git_output,
    run_git,
    run_git_bytes,
)
from scripts.common.publication_manifest import (
    publisher_context,
    write_publication_manifest,
)
from scripts.common.workflow_artifacts import ArtifactClient, ArtifactState

logger = logging.getLogger(__name__)

_FAILED_CONCLUSIONS = {"failure", "timed_out"}
_PROPOSAL_KEYS = {
    "path",
    "failing_check",
    "root_cause",
    "reasoning",
    "confidence",
    "failing_job_hint",
    "build_command",
    "verify_command",
    "workdir",
    "unstable_fix_commit",
    "other_failing_checks",
}
_REVIEW_KEYS = {"approved", "reasoning"}
_RESULT_KEYS = {
    "ran",
    "passed",
    "exit_code",
    "command",
    "output_tail",
    "timed_out",
}
_PERMIT_KEYS = {
    "schema_version",
    "kind",
    "validated_sha256",
    "prepared_sha256",
    "patch_sha256",
    "head_sha",
    "result_tree",
    "repository",
    "head_branch",
}
_REFUSAL_PERMIT_KEYS = {
    "schema_version",
    "kind",
    "prepared_sha256",
    "discovery_sha256",
    "repository",
    "pr_number",
    "reason_sha256",
}
_PUBLICATION_STATE_KEYS = {
    "repository",
    "pull_request_number",
    "pull_request_url",
    "pull_request_state",
    "head_branch",
    "previous_head_sha",
    "remote_ref",
    "remote_ref_sha",
    "published_commit",
    "published_tree",
    "comment_status",
    "reaction_status",
    "authoritative_check",
}
_REFUSAL_PUBLICATION_STATE_KEYS = {
    "repository",
    "pull_request_number",
    "pull_request_url",
    "pull_request_state",
    "reason_sha256",
    "comment_status",
    "reaction_status",
}
_VALIDATION_PLAN_KEYS = {
    "schema_version",
    "kind",
    "commands",
    "workdir",
    "verification_runs",
    "platform",
    "job",
    "fidelity",
    "runtime",
    "agent",
}


def discover(
    *,
    repository: str,
    pr_number: int,
    run_url: str,
    commenter: str,
    hint: str,
    comment_id: int,
    token: str,
    output_directory: str | Path,
    auth_org: str = "valkey-io",
    auth_team: str = "contributors",
) -> None:
    """Authorize and collect immutable evidence without checking out target code."""
    command = parse_command(f"@valkeyrie-bot fix {run_url} {hint}".strip())
    if command is None:
        raise ArtifactError("run URL is not a valid GitHub Actions run URL")
    gh = Github(auth=Auth.Token(token))
    request = build_fix_request(
        gh,
        command=command,
        pr_repo_full_name=repository,
        pr_number=pr_number,
        commenter=commenter,
        org=auth_org,
        auth_team=auth_team,
    )
    if isinstance(request, GateRejection):
        raise ArtifactError(request.reason)

    repo = gh.get_repo(repository)
    run = repo.get_workflow_run(request.run_id)
    workflow_path = _workflow_path(run)
    workflow_content = _workflow_at_sha(repo, workflow_path, request.head_sha)
    workflow_id = _positive_int(getattr(run, "workflow_id", 0), "workflow_id")
    run_attempt = _positive_int(getattr(run, "run_attempt", 1), "run_attempt")

    jobs: list[dict[str, Any]] = []
    for job in run.jobs():
        conclusion = str(getattr(job, "conclusion", "") or "")
        if conclusion not in _FAILED_CONCLUSIONS:
            continue
        display_name = str(getattr(job, "name", "") or "")
        resolved = resolve_workflow_job(workflow_content, display_name)
        jobs.append(
            {
                "database_id": _positive_int(getattr(job, "id", 0), "job id"),
                "display_name": display_name,
                "conclusion": conclusion,
                "labels": [
                    str(label)
                    for label in (getattr(job, "labels", None) or [])
                ][:100],
                "runner_name": str(getattr(job, "runner_name", "") or ""),
                "runner_group_id": max(
                    0,
                    int(getattr(job, "runner_group_id", 0) or 0),
                ),
                "job_id": resolved.job_id,
                "matrix": dict(resolved.matrix),
                "environment": resolved.environment.env.value,
                "image": resolved.environment.image,
                "fidelity": resolved.fidelity,
                "reason": resolved.reason,
            }
        )
    if not jobs:
        raise ArtifactError("linked workflow run contains no failed or timed-out jobs")

    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    raw_logs = output / "raw-logs"
    log_download = ArtifactClient(gh, token=token).download_run_logs(
        repository,
        request.run_id,
        destination=raw_logs,
    )
    if log_download.state is not ArtifactState.AVAILABLE:
        raise ArtifactError(
            f"linked workflow logs are {log_download.state.value}: "
            f"{log_download.detail}",
        )
    if not log_download.members:
        raise ArtifactError("linked workflow logs contain no files")
    workflow_file = output / "workflow.yml"
    workflow_bytes = workflow_content.encode("utf-8")
    if len(workflow_bytes) > 1024 * 1024:
        raise ArtifactError("workflow file exceeds 1 MiB")
    workflow_file.write_bytes(workflow_bytes)
    workflow_sha = sha256_bytes(workflow_bytes)

    log_entries: list[dict[str, Any]] = []
    total_log_bytes = 0
    for index, member in enumerate(
        sorted(log_download.members, key=lambda item: item.name),
    ):
        if member.size > 16 * 1024 * 1024:
            raise ArtifactError(f"log member {member.name!r} exceeds 16 MiB")
        total_log_bytes += member.size
        if total_log_bytes > 128 * 1024 * 1024:
            raise ArtifactError("workflow logs exceed the 128 MiB phase limit")
        name = f"log-{index:04d}.txt"
        shutil.copyfile(member.path, output / name)
        log_entries.append(
            {
                "source_name": member.name[:4096],
                "file": name,
                "sha256": member.sha256,
                "bytes": member.size,
            }
        )
    shutil.rmtree(raw_logs)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-discovery",
        "request": {
            "repository": request.repo_full_name,
            "pr_number": request.pr_number,
            "head_repository": request.head_repo_full_name,
            "head_branch": request.head_branch,
            "head_sha": request.head_sha.lower(),
            "run_id": request.run_id,
            "requested_by": request.requested_by,
            "hint": request.hint[:500],
            "comment_id": comment_id,
        },
        "workflow": {
            "workflow_id": workflow_id,
            "workflow_path": workflow_path,
            "run_attempt": run_attempt,
            "file": workflow_file.name,
            "sha256": workflow_sha,
        },
        "failed_jobs": jobs,
        "logs": log_entries,
    }
    write_json(output / "discovery.json", manifest)
    load_discovery(output)


def prepare(
    *,
    artifact_directory: str | Path,
) -> tuple[str, str]:
    """Diagnose, edit, and review without running target repository commands."""
    discovery = load_discovery(artifact_directory)
    root = discovery.root
    request = discovery.request
    status = "refused"
    reason = ""
    selected_job = ""
    environment = ""
    patch = b""
    changed_paths: tuple[str, ...] = ()
    result_tree = _empty_tree_fallback()
    port_commit = ""
    proposal = FixProposal(
        path=FixPath.REFUSE,
        failing_check="",
        root_cause="",
        reasoning="preparation did not run",
        confidence=0.0,
    )
    review = ReviewVerdict(approved=False, reasoning="no reviewed patch")

    with (
        ai_evidence_directory(root),
        tempfile.TemporaryDirectory(prefix="ci-fix-prepare-") as temporary,
    ):
        workspace = Path(temporary)
        repo_dir = workspace / "repo"
        logs_dir = workspace / "logs"
        logs_dir.mkdir()
        for entry in discovery.logs:
            shutil.copyfile(root / entry["file"], logs_dir / entry["file"])
        _clone_head(request["repository"], request["head_branch"], request["head_sha"], repo_dir)
        candidates = discover_port_candidates(str(repo_dir), str(logs_dir))
        proposal = diagnose_failure(
            str(logs_dir),
            str(repo_dir),
            hint=request["hint"],
            port_candidates=candidates,
        )
        if proposal.path is FixPath.REFUSE:
            reason = proposal.reasoning or "diagnosis refused to propose a fix"
        else:
            selected_job = match_failed_job(
                proposal.failing_job_hint,
                tuple(job["display_name"] for job in discovery.failed_jobs),
            ) or ""
            job = next(
                (
                    item
                    for item in discovery.failed_jobs
                    if item["display_name"] == selected_job
                ),
                None,
            )
            if job is None:
                reason = "proposal did not select one exact failed workflow job"
                selected_job = ""
            elif job["environment"] == VerifyEnv.UNSUPPORTED.value:
                reason = job["reason"] or "selected workflow job is unsupported"
                selected_job = ""
            elif (precheck := precheck_command(proposal)):
                reason = precheck
                selected_job = ""
            else:
                environment = job["environment"]
                if proposal.path is FixPath.PORT:
                    port_commit = canonical_candidate_sha(
                        proposal.unstable_fix_commit,
                        candidates,
                    ) or ""
                    if not port_commit:
                        reason = "selected port commit is not a discovered upstream candidate"
                    else:
                        run_git(str(repo_dir), "fetch", "origin", port_commit)
                        verify_portable_commit(
                            str(repo_dir),
                            port_commit,
                            request["head_sha"],
                        )
                        run_git(str(repo_dir), "config", "user.name", BOT_NAME)
                        run_git(str(repo_dir), "config", "user.email", BOT_EMAIL)
                        try:
                            run_git(str(repo_dir), "cherry-pick", "-x", port_commit)
                        except subprocess.CalledProcessError:
                            reason = "selected upstream fix does not cherry-pick cleanly"
                        else:
                            patch = _committed_patch(
                                str(repo_dir),
                                request["head_sha"],
                                "HEAD",
                            )
                            changed_paths = _changed_paths(
                                str(repo_dir),
                                request["head_sha"],
                                "HEAD",
                            )
                            result_tree = git_output(
                                str(repo_dir),
                                "rev-parse",
                                "HEAD^{tree}",
                            ).strip()
                            review = ReviewVerdict(
                                approved=True,
                                reasoning=(
                                    f"Upstream commit {port_commit[:12]} was code-discovered, "
                                    "ancestry-checked, and cherry-picked cleanly."
                                ),
                            )
                            status = "ready"
                else:
                    applied, changed_paths = apply_fix(str(repo_dir), proposal)
                    if not applied:
                        reason = "fix agent made no scoped edit"
                        changed_paths = ()
                    else:
                        reviewed = build_and_review_patch(
                            str(repo_dir),
                            changed_paths,
                            proposal,
                        )
                        review = reviewed.review or review
                        if not reviewed.ok:
                            reason = reviewed.detail
                            changed_paths = ()
                        else:
                            patch = reviewed.patch.encode("utf-8")
                            result_tree = _tree_for_patch(
                                request["repository"],
                                request["head_branch"],
                                request["head_sha"],
                                patch,
                            )
                            status = "ready"

    if status != "ready":
        selected_job = ""
        environment = ""
        patch = b""
        changed_paths = ()
        port_commit = ""
        result_tree = _empty_tree_fallback()
    if len(patch) > MAX_PATCH_BYTES:
        raise ArtifactError("CI-fix patch exceeds phase artifact limit")

    ai_evidence_file, ai_evidence_sha = finalize_ai_evidence(root)
    proposal_path = root / "proposal.json"
    proposal_sha = write_json(proposal_path, _proposal_dict(proposal))
    review_path = root / "review.json"
    review_sha = write_json(review_path, _review_dict(review))
    patch_path = root / "change.patch"
    patch_path.write_bytes(patch)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-prepared",
        "status": status,
        "discovery_sha256": discovery.manifest_sha256,
        "repository": request["repository"],
        "head_branch": request["head_branch"],
        "head_sha": request["head_sha"],
        "selected_job": selected_job,
        "proposal_file": proposal_path.name,
        "proposal_sha256": proposal_sha,
        "review_file": review_path.name,
        "review_sha256": review_sha,
        "patch_file": patch_path.name,
        "patch_sha256": sha256_bytes(patch),
        "patch_bytes": len(patch),
        "changed_paths": list(changed_paths),
        "result_tree": result_tree,
        "port_commit": port_commit,
        "reason": reason[:16_384],
        "ai_evidence_file": ai_evidence_file,
        "ai_evidence_sha256": ai_evidence_sha,
    }
    write_json(root / "prepared.json", manifest)
    load_prepared(root)
    return status, environment


def validate(
    *,
    artifact_directory: str | Path,
    platform: str,
    verify_runs: int = DEFAULT_VERIFY_RUNS,
) -> None:
    """Prove a clean failing baseline and repeated passing patched runs."""
    prepared = load_prepared(artifact_directory)
    if prepared.status != "ready":
        raise ArtifactError("only a ready patch can be validated")
    proposal = _load_proposal(prepared.proposal_path)
    job = _selected_job(prepared)
    environment = VerifyEnv(job["environment"])
    expected_platform = "macos" if environment is VerifyEnv.MACOS else "linux"
    if platform != expected_platform:
        raise ArtifactError(
            f"selected job requires {expected_platform}, not {platform}"
        )
    image = job["image"] if environment is VerifyEnv.DOCKER else ""
    bounded_runs = max(1, min(verify_runs, 10))
    try:
        runtime = prepare_verification_runtime(
            platform=platform,
            container_image=image,
        )
    except RuntimeError as exc:
        raise ArtifactError(f"could not establish verification isolation: {exc}") from exc

    with tempfile.TemporaryDirectory(prefix="ci-fix-validate-") as temporary:
        repo_dir = Path(temporary, "repo")
        _clone_head(
            prepared.repository,
            prepared.head_branch,
            prepared.head_sha,
            repo_dir,
        )
        try:
            with runtime.sandbox(str(repo_dir)) as baseline_sandbox:
                baseline = reproduce_failure(
                    str(repo_dir),
                    proposal,
                    container_image=image,
                    run_command=baseline_sandbox.run,
                )
        except RuntimeError as exc:
            raise ArtifactError(
                f"could not establish baseline verification sandbox: {exc}",
            ) from exc
        if not baseline.ran:
            raise ArtifactError("failing baseline could not be executed")
        if baseline.passed:
            raise ArtifactError("failing baseline passed; refusing a false fix")
        reset_worktree(str(repo_dir))

        _apply_patch(str(repo_dir), prepared.patch_path.read_bytes())
        _verify_tree(repo_dir, prepared)
        try:
            with runtime.sandbox(str(repo_dir)) as patched_sandbox:
                result = verify_repeatedly(
                    str(repo_dir),
                    proposal,
                    runs=bounded_runs,
                    container_image=image,
                    run_command=patched_sandbox.run,
                )
        except RuntimeError as exc:
            raise ArtifactError(
                f"could not establish patched verification sandbox: {exc}",
            ) from exc
        if not result.ran or not result.passed:
            raise ArtifactError(
                "patched verification failed: " + result.output_tail[-2000:]
            )
        _verify_tree(repo_dir, prepared)
        if run_git(
            str(repo_dir),
            "diff",
            "--quiet",
            "--",
            check=False,
        ).returncode != 0:
            raise ArtifactError("verification modified tracked files outside the patch")

    image_identity = runtime.image_identity
    plan = _validation_plan(
        proposal,
        job,
        platform=platform,
        verify_runs=bounded_runs,
        image_identity=image_identity,
        sandbox_uid=runtime.sandbox_uid,
    )
    if plan["runtime"] != runtime.contract():
        raise ArtifactError("recorded runtime differs from enforced isolation")
    command_sha = sha256_bytes(canonical_json_bytes(plan))
    root = prepared.root
    plan_path = root / "validation-plan.json"
    plan_sha = write_json(plan_path, plan)
    if plan_sha != command_sha:
        raise ArtifactError("validation plan digest is not canonical")
    baseline_path = root / "baseline.json"
    baseline_sha = write_json(baseline_path, _run_result_dict(baseline))
    result_path = root / "validation-result.json"
    result_sha = write_json(result_path, _run_result_dict(result))
    validated = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-validated",
        "status": "passed",
        "prepared_sha256": prepared.manifest_sha256,
        "discovery_sha256": prepared.discovery.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "head_sha": prepared.head_sha,
        "result_tree": prepared.result_tree,
        "selected_job": prepared.selected_job,
        "command_sha256": command_sha,
        "plan_file": plan_path.name,
        "plan_sha256": plan_sha,
        "baseline_file": baseline_path.name,
        "baseline_sha256": baseline_sha,
        "result_file": result_path.name,
        "result_sha256": result_sha,
    }
    write_json(root / "validated.json", validated)
    load_validated(root)


def preflight_publish(*, artifact_directory: str | Path) -> None:
    """Reproduce the validated tree and current branch head before token minting."""
    validated = load_validated(artifact_directory)
    prepared = validated.prepared
    proposal = _load_proposal(prepared.proposal_path)
    _verify_command_digest(validated, proposal)
    with tempfile.TemporaryDirectory(prefix="ci-fix-preflight-") as temporary:
        repo_dir = Path(temporary, "repo")
        _clone_head(
            prepared.repository,
            prepared.head_branch,
            prepared.head_sha,
            repo_dir,
        )
        _apply_patch(str(repo_dir), prepared.patch_path.read_bytes())
        _verify_tree(repo_dir, prepared)
    permit = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-publisher-permit",
        "validated_sha256": validated.manifest_sha256,
        "prepared_sha256": prepared.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "head_sha": prepared.head_sha,
        "result_tree": prepared.result_tree,
        "repository": prepared.repository,
        "head_branch": prepared.head_branch,
    }
    write_json(prepared.root / "publisher-permit.json", permit)


def preflight_refusal(*, artifact_directory: str | Path) -> None:
    """Verify a non-actionable result before minting a comment-only token."""
    prepared = load_prepared(artifact_directory)
    if prepared.status != "refused":
        raise ArtifactError("refusal reporter accepts only refused artifacts")
    _load_proposal(prepared.proposal_path)
    reason_sha = sha256_bytes(prepared.reason.encode("utf-8"))
    permit = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-refusal-permit",
        "prepared_sha256": prepared.manifest_sha256,
        "discovery_sha256": prepared.discovery.manifest_sha256,
        "repository": prepared.repository,
        "pr_number": prepared.discovery.request["pr_number"],
        "reason_sha256": reason_sha,
    }
    write_json(prepared.root / "refusal-permit.json", permit)


def report_refusal(
    *,
    artifact_directory: str | Path,
    token: str,
) -> None:
    """Post a verified refusal without ever receiving a candidate patch."""
    if not token:
        raise ArtifactError("reporter token is required")
    prepared = load_prepared(artifact_directory)
    publisher = publisher_context()
    if prepared.status != "refused":
        raise ArtifactError("reporter refuses an actionable patch artifact")
    permit = _load_refusal_permit(prepared.root / "refusal-permit.json")
    expected = {
        "prepared_sha256": prepared.manifest_sha256,
        "discovery_sha256": prepared.discovery.manifest_sha256,
        "repository": prepared.repository,
        "pr_number": prepared.discovery.request["pr_number"],
        "reason_sha256": sha256_bytes(prepared.reason.encode("utf-8")),
    }
    for key, value in expected.items():
        if permit[key] != value:
            raise ArtifactError(f"refusal permit {key} differs from artifact")
    proposal = _load_proposal(prepared.proposal_path)
    outcome = FixOutcome(
        kind=OutcomeKind.REFUSED,
        summary=prepared.reason or "No safe automated fix was identified.",
        proposal=proposal,
        failing_run_url=(
            f"https://github.com/{prepared.repository}/actions/runs/"
            f"{prepared.discovery.request['run_id']}"
        ),
        other_failing_checks=proposal.other_failing_checks,
    )
    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(prepared.repository)
    pr = repo.get_pull(prepared.discovery.request["pr_number"])
    issue = repo.get_issue(prepared.discovery.request["pr_number"])
    desired = DesiredComment(
        key=(
            f"ci-fix:{prepared.discovery.request['run_id']}:"
            f"{prepared.discovery.request['pr_number']}:refused:"
            f"{prepared.manifest_sha256[:16]}"
        ),
        expected_head_sha=prepared.head_sha,
        body=render_comment(outcome),
        reaction_comment_id=prepared.discovery.request["comment_id"],
        reaction=(
            "-1" if prepared.discovery.request["comment_id"] else ""
        ),
    )
    comment = record_desired_comment(
        issue,
        desired,
        writer_login=publisher["identity"],
    )
    current_sha = str(getattr(getattr(pr, "head", None), "sha", "") or "").lower()
    if not reconcile_desired_comment(
        repo,
        comment,
        desired,
        current_head_sha=current_sha,
    ):
        raise ArtifactError("refusal PR head differs from desired comment state")
    reaction_status = (
        "attempted"
        if prepared.discovery.request["comment_id"]
        else "not-requested"
    )
    write_publication_manifest(
        prepared.root,
        kind="ci-fix-refusal-publication",
        source_manifest_file=prepared.manifest_path.name,
        source_manifest_sha256=prepared.manifest_sha256,
        publisher=publisher,
        final_state={
            "repository": prepared.repository,
            "pull_request_number": prepared.discovery.request["pr_number"],
            "pull_request_url": str(
                getattr(pr, "html_url", "")
                or (
                    f"https://github.com/{prepared.repository}/pull/"
                    f"{prepared.discovery.request['pr_number']}"
                )
            ),
            "pull_request_state": str(getattr(pr, "state", "") or "open"),
            "reason_sha256": sha256_bytes(prepared.reason.encode("utf-8")),
            "comment_status": "reconciled",
            "reaction_status": (
                "reconciled" if reaction_status == "attempted" else reaction_status
            ),
        },
        final_state_keys=_REFUSAL_PUBLICATION_STATE_KEYS,
    )


def publish(
    *,
    artifact_directory: str | Path,
    token: str,
) -> str:
    """Publish the exact validated tree and post its evidence."""
    if not token:
        raise ArtifactError("publisher token is required")
    validated = load_validated(artifact_directory)
    prepared = validated.prepared
    publisher = publisher_context()
    permit = _load_permit(prepared.root / "publisher-permit.json")
    expected = {
        "validated_sha256": validated.manifest_sha256,
        "prepared_sha256": prepared.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "head_sha": prepared.head_sha,
        "result_tree": prepared.result_tree,
        "repository": prepared.repository,
        "head_branch": prepared.head_branch,
    }
    for key, value in expected.items():
        if permit[key] != value:
            raise ArtifactError(f"publisher permit {key} differs from validation")
    if not prepared.head_branch.startswith(ALLOWED_BRANCH_PREFIX):
        raise ArtifactError(
            f"CI-fix can publish only under {ALLOWED_BRANCH_PREFIX}"
        )
    proposal = _load_proposal(prepared.proposal_path)
    review = _load_review(prepared.review_path)
    result = _load_run_result(validated.result_path)
    _verify_command_digest(validated, proposal)

    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(prepared.repository)
    pr = repo.get_pull(prepared.discovery.request["pr_number"])
    issue = repo.get_issue(prepared.discovery.request["pr_number"])
    current_sha = str(getattr(pr.head, "sha", "") or "").lower()
    current_branch = str(getattr(pr.head, "ref", "") or "")
    if current_branch != prepared.head_branch:
        raise ArtifactError("PR head branch changed after validation")
    recovering_publication = current_sha != prepared.head_sha

    with tempfile.TemporaryDirectory(prefix="ci-fix-publish-") as temporary:
        repo_dir = Path(temporary, "repo")
        _clone_head(
            prepared.repository,
            prepared.head_branch,
            current_sha,
            repo_dir,
        )
        if recovering_publication:
            commit_sha = _verify_existing_ci_fix_publication(
                str(repo_dir),
                prepared,
                proposal,
            )
        else:
            run_git(str(repo_dir), "checkout", "-B", prepared.head_branch)
            if prepared.port_commit:
                run_git(str(repo_dir), "fetch", "origin", prepared.port_commit)
                verify_portable_commit(
                    str(repo_dir),
                    prepared.port_commit,
                    prepared.head_sha,
                )
                run_git(str(repo_dir), "config", "user.name", BOT_NAME)
                run_git(str(repo_dir), "config", "user.email", BOT_EMAIL)
                run_git(str(repo_dir), "cherry-pick", "-x", prepared.port_commit)
            else:
                _apply_patch(str(repo_dir), prepared.patch_path.read_bytes())
                _verify_tree(repo_dir, prepared)
                run_git(str(repo_dir), "config", "user.name", BOT_NAME)
                run_git(str(repo_dir), "config", "user.email", BOT_EMAIL)
                run_git(
                    str(repo_dir),
                    "commit",
                    "-m",
                    _ci_fix_commit_message(prepared, proposal),
                )
            tree = git_output(str(repo_dir), "rev-parse", "HEAD^{tree}").strip()
            if tree != prepared.result_tree:
                raise ArtifactError("publisher commit tree differs from validated tree")
            commit_sha = git_output(str(repo_dir), "rev-parse", "HEAD").strip()
        tree = git_output(str(repo_dir), "rev-parse", "HEAD^{tree}").strip()
        if tree != prepared.result_tree:
            raise ArtifactError("publisher commit tree differs from validated tree")
        job = _selected_job(prepared)
        outcome = FixOutcome(
            kind=OutcomeKind.PUSHED,
            summary=f"Pushed fix for {proposal.failing_check}",
            proposal=proposal,
            run_result=result,
            review=review,
            commit_sha=commit_sha,
            failing_run_url=(
                f"https://github.com/{prepared.repository}/actions/runs/"
                f"{prepared.discovery.request['run_id']}"
            ),
            verify_backend=backend_label(
                VerifyEnv(job["environment"]),
                job["image"],
            ),
            other_failing_checks=proposal.other_failing_checks,
        )
        desired = DesiredComment(
            key=(
                f"ci-fix:{prepared.discovery.request['run_id']}:"
                f"{prepared.discovery.request['pr_number']}:pushed:"
                f"{prepared.patch_sha256[:16]}"
            ),
            expected_head_sha=commit_sha,
            body=render_comment(outcome),
            reaction_comment_id=prepared.discovery.request["comment_id"],
            reaction=(
                "+1" if prepared.discovery.request["comment_id"] else ""
            ),
        )
        comment = record_desired_comment(
            issue,
            desired,
            writer_login=publisher["identity"],
        )
        with GitAuth(token, prefix="ci-fix-phased-publish-") as auth:
            if not recovering_publication:
                run_git(
                    str(repo_dir),
                    "push",
                    (
                        "--force-with-lease="
                        f"refs/heads/{prepared.head_branch}:{prepared.head_sha}"
                    ),
                    "origin",
                    f"HEAD:refs/heads/{prepared.head_branch}",
                    env=auth.env(),
                )
            remote_ref_sha = _remote_branch_sha(
                str(repo_dir),
                prepared.head_branch,
                auth.env(),
            )
            if remote_ref_sha != commit_sha:
                raise ArtifactError("published CI-fix branch does not point to commit")

    if not reconcile_desired_comment(
        repo,
        comment,
        desired,
        current_head_sha=commit_sha,
    ):
        raise ArtifactError("published head differs from desired comment state")
    reaction_status = (
        "attempted"
        if prepared.discovery.request["comment_id"]
        else "not-requested"
    )
    write_publication_manifest(
        prepared.root,
        kind="ci-fix-publication",
        source_manifest_file=validated.manifest_path.name,
        source_manifest_sha256=validated.manifest_sha256,
        publisher=publisher,
        final_state={
            "repository": prepared.repository,
            "pull_request_number": prepared.discovery.request["pr_number"],
            "pull_request_url": str(
                getattr(pr, "html_url", "")
                or (
                    f"https://github.com/{prepared.repository}/pull/"
                    f"{prepared.discovery.request['pr_number']}"
                )
            ),
            "pull_request_state": str(getattr(pr, "state", "") or "open"),
            "head_branch": prepared.head_branch,
            "previous_head_sha": prepared.head_sha,
            "remote_ref": f"refs/heads/{prepared.head_branch}",
            "remote_ref_sha": remote_ref_sha,
            "published_commit": commit_sha,
            "published_tree": prepared.result_tree,
            "comment_status": "reconciled",
            "reaction_status": (
                "reconciled" if reaction_status == "attempted" else reaction_status
            ),
            "authoritative_check": job["fidelity"]["authoritative_check"],
        },
        final_state_keys=_PUBLICATION_STATE_KEYS,
    )
    return commit_sha


def _workflow_path(run: Any) -> str:
    raw = str(
        getattr(run, "path", "")
        or getattr(run, "raw_data", {}).get("path", "")
        or ""
    )
    marker = ".github/workflows/"
    index = raw.find(marker)
    if index < 0:
        raise ArtifactError("run metadata does not identify its workflow path")
    path = raw[index:].split("@", 1)[0]
    if not path.endswith((".yml", ".yaml")):
        raise ArtifactError("run workflow path is not a YAML workflow")
    return path


def _workflow_at_sha(repo: Any, path: str, sha: str) -> str:
    content = repo.get_contents(path, ref=sha)
    if isinstance(content, list):
        raise ArtifactError("workflow path resolved to a directory")
    payload = getattr(content, "decoded_content", None)
    if not isinstance(payload, bytes) or len(payload) > 1024 * 1024:
        raise ArtifactError("workflow content is absent or oversized")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArtifactError("workflow content is not UTF-8") from exc
    try:
        if not isinstance(yaml.safe_load(text), dict):
            raise ArtifactError("workflow content is not a mapping")
    except yaml.YAMLError as exc:
        raise ArtifactError("workflow content does not parse") from exc
    return text


def _clone_head(repository: str, branch: str, sha: str, destination: Path) -> None:
    run_git(
        None,
        "clone",
        "--filter=blob:none",
        "--no-checkout",
        github_https_url(repository),
        str(destination),
    )
    remote_sha = git_output(
        str(destination),
        "rev-parse",
        f"refs/remotes/origin/{branch}",
    ).strip()
    if remote_sha != sha:
        raise ArtifactError(f"PR branch moved ({remote_sha} != {sha})")
    run_git(str(destination), "checkout", "--detach", sha)


def _tree_for_patch(repository: str, branch: str, sha: str, patch: bytes) -> str:
    with tempfile.TemporaryDirectory(prefix="ci-fix-patch-tree-") as temporary:
        repo = Path(temporary, "repo")
        _clone_head(repository, branch, sha, repo)
        _apply_patch(str(repo), patch)
        return git_output(str(repo), "write-tree").strip()


def _apply_patch(repo_dir: str, patch: bytes) -> None:
    if not patch or len(patch) > MAX_PATCH_BYTES:
        raise ArtifactError("patch is empty or oversized")
    try:
        run_git_bytes(
            repo_dir,
            "apply",
            "--index",
            "--whitespace=nowarn",
            "-",
            input=patch,
        )
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode("utf-8", errors="replace")[:500]
        raise ArtifactError(f"patch did not apply: {detail}") from exc


def _verify_tree(repo_dir: Path, prepared: PreparedFixArtifact) -> None:
    paths = _changed_paths(str(repo_dir), "HEAD", "--cached")
    if paths != prepared.changed_paths:
        raise ArtifactError("applied paths differ from reviewed paths")
    tree = git_output(str(repo_dir), "write-tree").strip()
    if tree != prepared.result_tree:
        raise ArtifactError("applied patch tree differs from reviewed tree")


def _ci_fix_commit_message(
    prepared: PreparedFixArtifact,
    proposal: FixProposal,
) -> str:
    return (
        f"Fix {proposal.failing_check or 'CI failure'}\n\n"
        f"Source-Run: {prepared.discovery.request['run_id']}\n"
        f"Validated-Tree: {prepared.result_tree}\n"
        f"Patch-SHA256: {prepared.patch_sha256}"
    )


def _verify_existing_ci_fix_publication(
    repo_dir: str,
    prepared: PreparedFixArtifact,
    proposal: FixProposal,
) -> str:
    """Accept a retry only when the current head is this exact validated fix."""
    commit = git_output(repo_dir, "rev-parse", "HEAD").strip()
    parent = git_output(repo_dir, "rev-parse", "HEAD^").strip()
    if parent != prepared.head_sha:
        raise ArtifactError("PR head moved to a commit outside this publication")
    tree = git_output(repo_dir, "rev-parse", "HEAD^{tree}").strip()
    if tree != prepared.result_tree:
        raise ArtifactError("existing CI-fix publication has a different tree")
    message = git_output(repo_dir, "show", "-s", "--format=%B", "HEAD").strip()
    if prepared.port_commit:
        trailer = f"(cherry picked from commit {prepared.port_commit})"
        if trailer not in message:
            raise ArtifactError("existing port publication lacks source provenance")
    elif message != _ci_fix_commit_message(prepared, proposal):
        raise ArtifactError("existing CI-fix publication metadata differs")
    return commit


def _changed_paths(repo_dir: str, first: str, second: str) -> tuple[str, ...]:
    if second == "--cached":
        args = ("diff", "--cached", "--name-only", "-z", first)
    else:
        args = ("diff", "--name-only", "-z", first, second)
    result = run_git_bytes(repo_dir, *args)
    try:
        paths = tuple(sorted(decode_git_paths(
            result.stdout,
            context="CI-fix changed path list",
        )))
    except GitPathEncodingError as exc:
        raise ArtifactError(f"{exc}; human handling is required") from exc
    if any(path == ".git" or path.startswith(".git/") for path in paths):
        raise ArtifactError("patch modifies Git metadata")
    return paths


def _committed_patch(repo_dir: str, first: str, second: str) -> bytes:
    patch = run_git_bytes(
        repo_dir,
        "diff",
        "--binary",
        "--no-ext-diff",
        first,
        second,
    ).stdout
    if not patch or len(patch) > MAX_PATCH_BYTES:
        raise ArtifactError("committed patch is empty or oversized")
    return patch


def _selected_job(prepared: PreparedFixArtifact) -> dict[str, Any]:
    for job in prepared.discovery.failed_jobs:
        if job["display_name"] == prepared.selected_job:
            return job
    raise ArtifactError("prepared selected job is absent from discovery")


def _verify_command_digest(validated: Any, proposal: FixProposal) -> None:
    job = _selected_job(validated.prepared)
    plan = validated.plan
    if set(plan) != _VALIDATION_PLAN_KEYS:
        raise ArtifactError("validation plan keys are invalid")
    runs = plan["verification_runs"]
    if not isinstance(runs, int) or isinstance(runs, bool) or not 1 <= runs <= 10:
        raise ArtifactError("validation plan run count is invalid")
    environment = VerifyEnv(job["environment"])
    expected_platform = "macos" if environment is VerifyEnv.MACOS else "linux"
    runtime = plan.get("runtime")
    if not isinstance(runtime, dict):
        raise ArtifactError("validation runtime contract is invalid")
    image_identity = runtime.get("container_image_identity")
    if not isinstance(image_identity, str):
        raise ArtifactError("validation image identity is invalid")
    sandbox_uid = runtime.get("sandbox_uid")
    if expected_platform == "linux":
        if (
            not image_identity.startswith("sha256:")
            or len(image_identity) != 71
            or any(
                character not in "0123456789abcdef"
                for character in image_identity[7:]
            )
        ):
            raise ArtifactError("validation image identity is invalid")
        if sandbox_uid is not None:
            raise ArtifactError("Linux validation cannot carry a macOS sandbox UID")
    else:
        if image_identity:
            raise ArtifactError("macOS validation cannot carry a container identity")
        if (
            not isinstance(sandbox_uid, int)
            or isinstance(sandbox_uid, bool)
            or sandbox_uid <= 0
        ):
            raise ArtifactError("macOS validation sandbox UID is invalid")
    expected = _validation_plan(
        proposal,
        job,
        platform=expected_platform,
        verify_runs=runs,
        image_identity=image_identity,
        sandbox_uid=sandbox_uid,
    )
    if plan != expected:
        raise ArtifactError("validation plan differs from proposal/job metadata")
    if validated.command_sha256 != sha256_bytes(canonical_json_bytes(expected)):
        raise ArtifactError("validation command digest differs from proposal/job")


def _validation_plan(
    proposal: FixProposal,
    job: dict[str, Any],
    *,
    platform: str,
    verify_runs: int,
    image_identity: str,
    sandbox_uid: int | None,
) -> dict[str, Any]:
    runtime = verification_runtime_contract(
        platform=platform,
        container_image=job["image"],
        image_identity=image_identity,
        sandbox_uid=sandbox_uid,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "ci-fix-validation-plan",
        "commands": {
            "build": proposal.build_command,
            "verify": proposal.verify_command,
        },
        "workdir": proposal.workdir,
        "verification_runs": verify_runs,
        "platform": platform,
        "job": {
            "database_id": job["database_id"],
            "display_name": job["display_name"],
            "job_id": job["job_id"],
            "matrix": job["matrix"],
            "environment": job["environment"],
            "image": job["image"],
            "labels": job["labels"],
            "runner_name": job["runner_name"],
            "runner_group_id": job["runner_group_id"],
        },
        "fidelity": job["fidelity"],
        "runtime": runtime,
        "agent": agent_execution_identity(),
    }


def _remote_branch_sha(
    repo_dir: str,
    branch: str,
    env: dict[str, str],
) -> str:
    result = run_git(
        repo_dir,
        "ls-remote",
        "--heads",
        "origin",
        f"refs/heads/{branch}",
        env=env,
    )
    fields = result.stdout.strip().split()
    if len(fields) != 2 or fields[1] != f"refs/heads/{branch}":
        raise ArtifactError("published branch lookup returned malformed output")
    sha = fields[0].lower()
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise ArtifactError("published branch lookup returned an invalid SHA")
    return sha


def _proposal_dict(proposal: FixProposal) -> dict[str, Any]:
    return {
        "path": proposal.path.value,
        "failing_check": proposal.failing_check,
        "root_cause": proposal.root_cause,
        "reasoning": proposal.reasoning,
        "confidence": proposal.confidence,
        "failing_job_hint": proposal.failing_job_hint,
        "build_command": proposal.build_command,
        "verify_command": proposal.verify_command,
        "workdir": proposal.workdir,
        "unstable_fix_commit": proposal.unstable_fix_commit,
        "other_failing_checks": list(proposal.other_failing_checks),
    }


def _load_proposal(path: Path) -> FixProposal:
    raw = _exact_json(path, _PROPOSAL_KEYS, "proposal")
    try:
        fix_path = FixPath(raw["path"])
    except (TypeError, ValueError) as exc:
        raise ArtifactError("proposal path is invalid") from exc
    strings = (
        "failing_check",
        "root_cause",
        "reasoning",
        "failing_job_hint",
        "build_command",
        "verify_command",
        "workdir",
        "unstable_fix_commit",
    )
    if not all(isinstance(raw[key], str) for key in strings):
        raise ArtifactError("proposal string field has invalid type")
    confidence = raw["confidence"]
    if (
        not isinstance(confidence, (int, float))
        or isinstance(confidence, bool)
        or not 0 <= float(confidence) <= 1
    ):
        raise ArtifactError("proposal confidence is invalid")
    other = raw["other_failing_checks"]
    if (
        not isinstance(other, list)
        or len(other) > 50
        or not all(isinstance(item, str) and len(item) <= 1024 for item in other)
    ):
        raise ArtifactError("proposal other_failing_checks is invalid")
    return FixProposal(
        path=fix_path,
        failing_check=raw["failing_check"],
        root_cause=raw["root_cause"],
        reasoning=raw["reasoning"],
        confidence=float(confidence),
        failing_job_hint=raw["failing_job_hint"],
        build_command=raw["build_command"],
        verify_command=raw["verify_command"],
        workdir=raw["workdir"],
        unstable_fix_commit=raw["unstable_fix_commit"],
        other_failing_checks=tuple(other),
    )


def _review_dict(review: ReviewVerdict) -> dict[str, Any]:
    return {"approved": review.approved, "reasoning": review.reasoning}


def _load_review(path: Path) -> ReviewVerdict:
    raw = _exact_json(path, _REVIEW_KEYS, "review")
    if not isinstance(raw["approved"], bool) or not isinstance(raw["reasoning"], str):
        raise ArtifactError("review fields have invalid types")
    if len(raw["reasoning"]) > 16_384:
        raise ArtifactError("review reasoning is oversized")
    if not raw["approved"]:
        raise ArtifactError("publisher refuses an unapproved review")
    return ReviewVerdict(raw["approved"], raw["reasoning"])


def _run_result_dict(result: RunResult) -> dict[str, Any]:
    return {
        "ran": result.ran,
        "passed": result.passed,
        "exit_code": result.exit_code,
        "command": result.command,
        "output_tail": result.output_tail,
        "timed_out": result.timed_out,
    }


def _load_run_result(path: Path) -> RunResult:
    raw = _exact_json(path, _RESULT_KEYS, "run result")
    if not all(isinstance(raw[key], bool) for key in ("ran", "passed", "timed_out")):
        raise ArtifactError("run result booleans have invalid types")
    if not isinstance(raw["exit_code"], int) or isinstance(raw["exit_code"], bool):
        raise ArtifactError("run result exit_code is invalid")
    if not isinstance(raw["command"], str) or not isinstance(raw["output_tail"], str):
        raise ArtifactError("run result text has invalid types")
    if not raw["ran"] or not raw["passed"] or raw["exit_code"] != 0:
        raise ArtifactError("validated run result is not a passing execution")
    return RunResult(
        ran=raw["ran"],
        passed=raw["passed"],
        exit_code=raw["exit_code"],
        command=raw["command"],
        output_tail=raw["output_tail"],
        timed_out=raw["timed_out"],
    )


def _exact_json(path: Path, keys: set[str], label: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
            raise ArtifactError(f"{label} is not a bounded regular file")
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"cannot parse {label}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ArtifactError(f"{label} must be an object")
    unknown = sorted(set(raw) - keys)
    missing = sorted(keys - set(raw))
    if unknown or missing:
        raise ArtifactError(f"{label} keys invalid: unknown={unknown}, missing={missing}")
    return raw


def _load_permit(path: Path) -> dict[str, Any]:
    raw = _exact_json(path, _PERMIT_KEYS, "publisher permit")
    if raw["schema_version"] != SCHEMA_VERSION or raw["kind"] != "ci-fix-publisher-permit":
        raise ArtifactError("unsupported publisher permit")
    return raw


def _load_refusal_permit(path: Path) -> dict[str, Any]:
    raw = _exact_json(path, _REFUSAL_PERMIT_KEYS, "refusal permit")
    if raw["schema_version"] != SCHEMA_VERSION or raw["kind"] != "ci-fix-refusal-permit":
        raise ArtifactError("unsupported refusal permit")
    return raw


def _positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ArtifactError(f"{label} must be a positive integer")
    return value


def _empty_tree_fallback() -> str:
    # Used only by refused artifacts, which are never validated or published.
    return "0" * 40


def _write_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT", "")
    if not output:
        return
    if "\n" in value or "\r" in value:
        raise ArtifactError("workflow output must be one line")
    with Path(output).open("a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    phases = parser.add_subparsers(dest="phase", required=True)

    discovery = phases.add_parser("discover")
    discovery.add_argument("--repo", required=True)
    discovery.add_argument("--pr", required=True, type=int)
    discovery.add_argument("--run-url", required=True)
    discovery.add_argument("--commenter", required=True)
    discovery.add_argument("--hint", default="")
    discovery.add_argument("--comment-id", type=int, default=0)
    discovery.add_argument("--token", default=os.environ.get("DISCOVERY_GITHUB_TOKEN", ""))
    discovery.add_argument("--output-directory", required=True)

    preparation = phases.add_parser("prepare")
    preparation.add_argument("--artifact-directory", required=True)

    validation = phases.add_parser("validate")
    validation.add_argument("--artifact-directory", required=True)
    validation.add_argument("--platform", choices=("linux", "macos"), required=True)
    validation.add_argument("--verify-runs", type=int, default=DEFAULT_VERIFY_RUNS)

    preflight = phases.add_parser("preflight-publish")
    preflight.add_argument("--artifact-directory", required=True)

    refusal_preflight = phases.add_parser("preflight-refusal")
    refusal_preflight.add_argument("--artifact-directory", required=True)

    publication = phases.add_parser("publish")
    publication.add_argument("--artifact-directory", required=True)
    publication.add_argument("--token", default=os.environ.get("PUBLISH_GITHUB_TOKEN", ""))

    refusal = phases.add_parser("report-refusal")
    refusal.add_argument("--artifact-directory", required=True)
    refusal.add_argument("--token", default=os.environ.get("REPORT_GITHUB_TOKEN", ""))

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.phase == "discover":
        if not args.token:
            parser.error("discover requires DISCOVERY_GITHUB_TOKEN")
        discover(
            repository=args.repo,
            pr_number=args.pr,
            run_url=args.run_url,
            commenter=args.commenter,
            hint=args.hint,
            comment_id=args.comment_id,
            token=args.token,
            output_directory=args.output_directory,
        )
        return 0
    if args.phase == "prepare":
        status, environment = prepare(artifact_directory=args.artifact_directory)
        _write_output("status", status)
        _write_output("environment", environment)
        return 0
    if args.phase == "validate":
        validate(
            artifact_directory=args.artifact_directory,
            platform=args.platform,
            verify_runs=args.verify_runs,
        )
        return 0
    if args.phase == "preflight-publish":
        preflight_publish(artifact_directory=args.artifact_directory)
        return 0
    if args.phase == "preflight-refusal":
        preflight_refusal(artifact_directory=args.artifact_directory)
        return 0
    if args.phase == "report-refusal":
        if not args.token:
            parser.error("report-refusal requires REPORT_GITHUB_TOKEN")
        report_refusal(
            artifact_directory=args.artifact_directory,
            token=args.token,
        )
        return 0
    if not args.token:
        parser.error("publish requires PUBLISH_GITHUB_TOKEN")
    commit = publish(artifact_directory=args.artifact_directory, token=args.token)
    _write_output("commit_sha", commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
