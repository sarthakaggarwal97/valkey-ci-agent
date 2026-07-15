"""Build, validate, and publish rolling sweep PRs from validated candidates."""

from __future__ import annotations

import argparse
import logging
import os
import re
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from github import Auth, Github

from scripts.backport import phased
from scripts.backport.diff_comments import (
    marked_source_pr_urls,
    reconcile_diff_comments,
)
from scripts.backport.pr_creator import (
    create_pull_from_push_repo,
    find_existing_pr,
)
from scripts.backport.registry import load_registry
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_models import (
    DETAIL_ALREADY_ON_SWEEP_BRANCH,
    DETAIL_RESOLVED_BY_AI,
    BranchSweepResult,
    CandidateResult,
)
from scripts.backport.sweep_reporting import (
    build_pr_body,
    parse_previous_applied,
    validation_failure_detail,
)
from scripts.common.ai_evidence import finalize_ai_evidence
from scripts.common.git_auth import GitAuth, github_https_url
from scripts.common.markdown import bounded_title
from scripts.common.metadata_reconciler import with_desired_labels
from scripts.common.phase_artifact import (
    MAX_PATCH_BYTES,
    SCHEMA_VERSION,
    ArtifactError,
    load_json,
    load_prepared,
    load_validated,
    policy_digest,
    sha256_bytes,
    sha256_file,
    write_json,
)
from scripts.common.proc import (
    BOT_EMAIL,
    BOT_NAME,
    git_output,
    run_git,
    run_git_bytes,
)
from scripts.common.publication_manifest import (
    publisher_context,
    write_publication_manifest,
)

logger = logging.getLogger(__name__)

_MAX_GROUPS = 100
_MAX_SERIES_BYTES = 64 * 1024 * 1024
_SWEEP_PREFIX = "agent/backport/sweep"
_SOURCE_RE = re.compile(r"^Backport #(?P<number>[1-9][0-9]*)\b")
_INDEX_KEYS = {"schema_version", "kind", "groups"}
_INDEX_GROUP_KEYS = {
    "directory",
    "status",
    "repository",
    "target_branch",
}
_REPORT_KEYS = {
    "schema_version",
    "kind",
    "repository",
    "push_repository",
    "target_branch",
    "base_commit",
    "branch_name",
    "existing_head",
    "series_file",
    "series_sha256",
    "series_bytes",
    "prior_source_prs",
    "candidates",
}
_CANDIDATE_KEYS = {
    "source_pr_number",
    "source_pr_title",
    "source_pr_url",
    "outcome",
    "detail",
    "resolved_by_ai",
    "resolutions",
    "resolved_commit_sha",
    "prepared_manifest_sha256",
    "validated_manifest_sha256",
    "patch_sha256",
}
_PUBLICATION_STATE_KEYS = {
    "repository",
    "push_repository",
    "target_branch",
    "base_commit",
    "branch_name",
    "remote_ref",
    "remote_ref_sha",
    "published_tree",
    "pull_request_number",
    "pull_request_url",
    "pull_request_state",
    "candidate_count",
}


def prepare(
    *,
    registry_path: str,
    candidates_directory: str | Path,
    output_directory: str | Path,
    max_candidates: int = 0,
) -> dict[str, Any]:
    """Combine validated candidates by repository and target branch."""
    if (
        not isinstance(max_candidates, int)
        or isinstance(max_candidates, bool)
        or not 0 <= max_candidates <= _MAX_GROUPS
    ):
        raise ArtifactError(
            f"max_candidates must be between 0 and {_MAX_GROUPS}",
        )
    candidates, failures = _load_handoffs(candidates_directory)
    if not candidates and not failures:
        raise ArtifactError("no aggregate candidate results were found")
    grouped: dict[tuple[str, str], list[Any]] = defaultdict(list)
    failed_grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[
            (candidate.prepared.repository, candidate.prepared.target_branch)
        ].append(candidate)
    for failure in failures:
        prepared = failure["prepared"]
        failed_grouped[(prepared.repository, prepared.target_branch)].append(
            failure,
        )
    group_keys = sorted(set(grouped) | set(failed_grouped))
    if len(group_keys) > _MAX_GROUPS:
        raise ArtifactError(f"aggregate candidate groups exceed {_MAX_GROUPS}")

    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    groups: list[dict[str, Any]] = []
    for index, (repository, target_branch) in enumerate(
        group_keys,
        start=1,
    ):
        directory_name = f"group-{index:03d}"
        status = _prepare_group(
            registry_path=registry_path,
            artifacts=grouped[(repository, target_branch)],
            failures=failed_grouped[(repository, target_branch)],
            output=output / directory_name,
            max_candidates=max_candidates,
        )
        groups.append({
            "directory": directory_name,
            "status": status,
            "repository": repository,
            "target_branch": target_branch,
        })
    result = {
        "schema_version": SCHEMA_VERSION,
        "kind": "backport-aggregate-index",
        "groups": groups,
    }
    write_json(output / "aggregate-index.json", result)
    return result


def validate(
    *,
    registry_path: str,
    artifact_directory: str | Path,
) -> dict[str, Any]:
    """Run the normal hardened validator for every ready aggregate group."""
    root = Path(artifact_directory)
    index = _load_index(root / "aggregate-index.json")
    groups: list[dict[str, Any]] = []
    for group in index["groups"]:
        status = group["status"]
        if status == "ready":
            status = phased.validate(
                registry_path=registry_path,
                artifact_directory=root / group["directory"],
            )
        groups.append({**group, "status": status})
    result = {
        "schema_version": SCHEMA_VERSION,
        "kind": "backport-aggregate-validation-index",
        "groups": groups,
    }
    write_json(root / "aggregate-validation-index.json", result)
    return result


def preflight_publish(
    *,
    registry_path: str,
    artifact_directory: str | Path,
    repositories_output: str | Path | None = None,
) -> tuple[str, ...]:
    """Reproduce every passed aggregate before a write token is minted."""
    root = Path(artifact_directory)
    index = _load_index(
        root / "aggregate-validation-index.json",
        kind="backport-aggregate-validation-index",
    )
    passed = 0
    repositories: set[str] = set()
    for group in index["groups"]:
        if group["status"] != "passed":
            continue
        group_root = root / group["directory"]
        validated = load_validated(group_root)
        prepared = validated.prepared
        if (
            group["repository"] != prepared.repository
            or group["target_branch"] != prepared.target_branch
        ):
            raise ArtifactError(
                "aggregate index group differs from validated artifact",
            )
        _load_report(validated)
        phased.preflight_publish(
            registry_path=registry_path,
            artifact_directory=group_root,
        )
        repositories.add(prepared.repository)
        passed += 1
    if not passed:
        raise ArtifactError("no aggregate group passed validation")
    verified = tuple(sorted(repositories))
    if repositories_output is not None:
        output = Path(repositories_output)
        try:
            with output.open("x", encoding="utf-8") as handle:
                handle.write("".join(f"{repository}\n" for repository in verified))
        except OSError as exc:
            raise ArtifactError(
                f"cannot write verified aggregate repositories: {exc}",
            ) from exc
    return verified


def publish(
    *,
    registry_path: str,
    artifact_directory: str | Path,
    token: str,
) -> list[str]:
    """Publish every passed aggregate as one rolling PR per target branch."""
    if not token:
        raise ArtifactError("aggregate publisher token is required")
    root = Path(artifact_directory)
    index = _load_index(
        root / "aggregate-validation-index.json",
        kind="backport-aggregate-validation-index",
    )
    gh = Github(auth=Auth.Token(token))
    urls: list[str] = []
    for group in index["groups"]:
        if group["status"] != "passed":
            continue
        urls.append(
            _publish_group(
                registry_path=registry_path,
                root=root / group["directory"],
                gh=gh,
                token=token,
            ),
        )
    if not urls:
        raise ArtifactError("no aggregate group is publishable")
    return urls


def _prepare_group(
    *,
    registry_path: str,
    artifacts: list[Any],
    failures: list[dict[str, Any]],
    output: Path,
    max_candidates: int,
) -> str:
    artifacts.sort(key=lambda item: item.prepared.source_pr_number)
    failures.sort(key=lambda item: item["prepared"].source_pr_number)
    first = (
        artifacts[0].prepared
        if artifacts
        else failures[0]["prepared"]
    )
    repository = first.repository
    target_branch = first.target_branch
    repo_entry, _ = load_registry(registry_path).get_branch(
        repository,
        target_branch,
    )
    current_policy = phased._policy(repo_entry)
    expected_policy = policy_digest(
        phased._validation_policy_payload(current_policy),
    )
    for artifact in artifacts:
        prepared = artifact.prepared
        if (
            prepared.repository != repository
            or prepared.target_branch != target_branch
            or prepared.push_repository != first.push_repository
        ):
            raise ArtifactError("aggregate group contains mixed repository policy")
        if prepared.policy_sha256 != expected_policy:
            raise ArtifactError("candidate validation policy changed before aggregation")
    for failure in failures:
        prepared = failure["prepared"]
        if (
            prepared.repository != repository
            or prepared.target_branch != target_branch
            or prepared.push_repository != first.push_repository
        ):
            raise ArtifactError("aggregate group contains mixed failure policy")
        if prepared.policy_sha256 != expected_policy:
            raise ArtifactError("failure validation policy changed before aggregation")

    output.mkdir(parents=True, exist_ok=True)
    branch_name = f"{_SWEEP_PREFIX}/{target_branch}"
    candidate_rows: list[dict[str, Any]] = []
    prior_source_prs: list[int] = []
    existing_head: str | None = None
    series = b""
    patch = b""
    changed_paths: tuple[str, ...] = ()
    status = "no-change"

    with tempfile.TemporaryDirectory(prefix="backport-aggregate-prepare-") as temporary:
        repo_dir = Path(temporary, "repo")
        run_git(
            None,
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            github_https_url(repository),
            str(repo_dir),
        )
        base_commit = git_output(
            str(repo_dir),
            "rev-parse",
            f"refs/remotes/origin/{target_branch}",
        ).strip()
        run_git(str(repo_dir), "checkout", "-B", branch_name, base_commit)
        run_git(str(repo_dir), "config", "user.name", BOT_NAME)
        run_git(str(repo_dir), "config", "user.email", BOT_EMAIL)

        existing_head = _existing_branch_head(repo_dir, branch_name)
        if existing_head:
            run_git(
                str(repo_dir),
                "fetch",
                "origin",
                f"refs/heads/{branch_name}",
            )
            run_git(str(repo_dir), "checkout", "-B", branch_name, "FETCH_HEAD")
            rebased = run_git(
                str(repo_dir),
                "rebase",
                base_commit,
                check=False,
            )
            if rebased.returncode != 0:
                run_git(str(repo_dir), "rebase", "--abort", check=False)
                raise ArtifactError(
                    f"existing aggregate branch {branch_name} cannot be rebased "
                    "onto the current target",
                )
            prior_source_prs = _source_prs_on_branch(
                str(repo_dir),
                base_commit,
            )

        existing_sources = set(prior_source_prs)
        applied_count = 0
        for artifact in artifacts:
            prepared = artifact.prepared
            descriptor = phased._parse_discovery(
                load_json(prepared.discovery_path, max_bytes=4 * 1024 * 1024),
            )
            source = descriptor["source_pr"]
            metadata = phased._parse_metadata(
                phased._load_json(prepared.metadata_path),
            )
            resolutions = list(metadata["resolutions"])
            row = {
                "source_pr_number": prepared.source_pr_number,
                "source_pr_title": source["title"],
                "source_pr_url": source["url"],
                "outcome": "",
                "detail": "",
                "resolved_by_ai": bool(
                    prepared.attempt == 1
                    or any(item["source"] == "llm" for item in resolutions)
                ),
                "resolutions": resolutions,
                "resolved_commit_sha": None,
                "prepared_manifest_sha256": prepared.manifest_sha256,
                "validated_manifest_sha256": artifact.manifest_sha256,
                "patch_sha256": prepared.patch_sha256,
            }
            if prepared.source_pr_number in existing_sources:
                row["outcome"] = "skipped-existing"
                row["detail"] = DETAIL_ALREADY_ON_SWEEP_BRANCH
                candidate_rows.append(row)
                continue
            if max_candidates > 0 and applied_count >= max_candidates:
                break

            applied = run_git_bytes(
                str(repo_dir),
                "apply",
                "--3way",
                "--index",
                "--whitespace=nowarn",
                "-",
                input=prepared.patch_path.read_bytes(),
                check=False,
            )
            if applied.returncode != 0:
                run_git(str(repo_dir), "reset", "--hard", "HEAD")
                row["outcome"] = "skipped-conflict"
                row["detail"] = (
                    "validated candidate conflicts with another change already "
                    "on the rolling aggregate branch"
                )
                candidate_rows.append(row)
                continue
            if (
                git_output(str(repo_dir), "write-tree").strip()
                == git_output(str(repo_dir), "rev-parse", "HEAD^{tree}").strip()
            ):
                run_git(str(repo_dir), "reset", "--hard", "HEAD")
                row["outcome"] = "skipped-existing"
                row["detail"] = "candidate is already present on aggregate branch"
                candidate_rows.append(row)
                continue

            title = " ".join(str(source["title"]).split())[:200]
            author = _source_author(
                str(repo_dir),
                prepared.source_merge_commit or prepared.source_commits[-1],
            )
            commit_args = ["commit"]
            if author:
                commit_args.append(f"--author={author}")
            commit_args.extend([
                "-m",
                f"Backport #{prepared.source_pr_number}: {title}",
            ])
            run_git(str(repo_dir), *commit_args)
            if resolutions:
                row["resolved_commit_sha"] = git_output(
                    str(repo_dir),
                    "rev-parse",
                    "HEAD",
                ).strip()
            row["outcome"] = "applied"
            row["detail"] = (
                DETAIL_RESOLVED_BY_AI
                if row["resolved_by_ai"]
                else "validated candidate added to rolling aggregate"
            )
            candidate_rows.append(row)
            existing_sources.add(prepared.source_pr_number)
            applied_count += 1

        candidate_rows.extend(_failure_row(failure) for failure in failures)
        candidate_rows.sort(key=lambda item: item["source_pr_number"])
        changed_paths = phased._changed_paths_between(
            str(repo_dir),
            base_commit,
            "HEAD",
        )
        if changed_paths:
            patch = run_git_bytes(
                str(repo_dir),
                "diff",
                "--binary",
                "--no-ext-diff",
                base_commit,
                "HEAD",
            ).stdout
            series = run_git_bytes(
                str(repo_dir),
                "format-patch",
                "--stdout",
                "--binary",
                f"{base_commit}..HEAD",
            ).stdout
            if (
                not patch
                or len(patch) > MAX_PATCH_BYTES
                or not series
                or len(series) > _MAX_SERIES_BYTES
            ):
                raise ArtifactError("aggregate patch or commit series exceeds bounds")
            status = "ready"
        result_tree = git_output(
            str(repo_dir),
            "rev-parse",
            "HEAD^{tree}" if changed_paths else f"{base_commit}^{{tree}}",
        ).strip()

    series_path = output / "series.mbox"
    series_path.write_bytes(series)
    report = {
        "schema_version": SCHEMA_VERSION,
        "kind": "backport-aggregate-report",
        "repository": repository,
        "push_repository": first.push_repository,
        "target_branch": target_branch,
        "base_commit": base_commit,
        "branch_name": branch_name,
        "existing_head": existing_head,
        "series_file": series_path.name,
        "series_sha256": sha256_bytes(series),
        "series_bytes": len(series),
        "prior_source_prs": prior_source_prs,
        "candidates": candidate_rows,
    }
    report_path = output / "aggregate-report.json"
    report_sha = write_json(report_path, report)

    original_descriptor = phased._parse_discovery(
        load_json(first.discovery_path, max_bytes=4 * 1024 * 1024),
    )
    descriptor = {
        **original_descriptor,
        "base_commit": base_commit,
        "branch_name": branch_name,
        "policy": current_policy,
    }
    discovery_sha = write_json(output / "discovery.json", descriptor)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "kind": "manual-backport-metadata",
        "source_pr": descriptor["source_pr"],
        "had_conflicts": any(row["resolved_by_ai"] for row in candidate_rows),
        "applied_commits": [],
        "resolutions": [],
        "reason": "rolling aggregate; see aggregate-report.json",
    }
    metadata_sha = write_json(output / "metadata.json", metadata)
    patch_path = output / "change.patch"
    patch_path.write_bytes(patch)
    evidence_file, evidence_sha = finalize_ai_evidence(output)
    prepared_manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "backport-prepared",
        "status": status,
        "discovery_file": "discovery.json",
        "discovery_sha256": discovery_sha,
        "repository": repository,
        "push_repository": first.push_repository,
        "target_branch": target_branch,
        "base_commit": base_commit,
        "source_pr_number": first.source_pr_number,
        "source_merge_commit": first.source_merge_commit,
        "source_commits": list(first.source_commits),
        "branch_name": branch_name,
        "patch_file": patch_path.name,
        "patch_sha256": sha256_bytes(patch),
        "patch_bytes": len(patch),
        "changed_paths": list(changed_paths),
        "result_tree": result_tree,
        "policy_sha256": expected_policy,
        "metadata_file": "metadata.json",
        "metadata_sha256": metadata_sha,
        "ai_evidence_file": evidence_file,
        "ai_evidence_sha256": evidence_sha,
        "attempt": 0,
        "parent_prepared_manifest_sha256": None,
        "failed_validation_manifest_sha256": None,
        "aggregate_file": report_path.name,
        "aggregate_sha256": report_sha,
    }
    write_json(output / "prepared.json", prepared_manifest)
    load_prepared(output)
    return status


def _publish_group(
    *,
    registry_path: str,
    root: Path,
    gh: Any,
    token: str,
) -> str:
    validated = load_validated(root)
    prepared = validated.prepared
    report = _load_report(validated)
    permit = phased._load_permit(root / "publisher-permit.json")
    expected_permit = {
        "validated_manifest_sha256": validated.manifest_sha256,
        "prepared_manifest_sha256": prepared.manifest_sha256,
        "patch_sha256": prepared.patch_sha256,
        "base_commit": prepared.base_commit,
        "result_tree": prepared.result_tree,
        "repository": prepared.repository,
        "target_branch": prepared.target_branch,
    }
    for key, value in expected_permit.items():
        if permit[key] != value:
            raise ArtifactError(f"aggregate publisher permit {key} differs")
    repo_entry, _ = load_registry(registry_path).get_branch(
        prepared.repository,
        prepared.target_branch,
    )
    publisher = publisher_context()
    series_path = root / report["series_file"]
    published_source_commits: dict[int, str] = {}
    with tempfile.TemporaryDirectory(prefix="backport-aggregate-publish-") as temporary:
        repo_dir = Path(temporary, "repo")
        phased._clone_exact_base(
            {
                "repository": prepared.repository,
                "target_branch": prepared.target_branch,
                "base_commit": prepared.base_commit,
            },
            repo_dir,
        )
        run_git(str(repo_dir), "checkout", "-B", prepared.branch_name)
        run_git(str(repo_dir), "config", "user.name", BOT_NAME)
        run_git(str(repo_dir), "config", "user.email", BOT_EMAIL)
        run_git_bytes(
            str(repo_dir),
            "am",
            "--3way",
            "--empty=drop",
            "-",
            input=series_path.read_bytes(),
        )
        if (
            git_output(str(repo_dir), "rev-parse", "HEAD^{tree}").strip()
            != prepared.result_tree
        ):
            raise ArtifactError("aggregate commit series differs from validated tree")
        published_source_commits = _source_commit_map(
            str(repo_dir),
            prepared.base_commit,
        )
        run_git(
            str(repo_dir),
            "commit",
            "--allow-empty",
            "-m",
            (
                "Attest rolling backport aggregate\n\n"
                f"Aggregate-Prepared-SHA256: {prepared.manifest_sha256}\n"
                f"Aggregate-Validated-SHA256: {validated.manifest_sha256}\n"
                f"Validated-Tree: {prepared.result_tree}"
            ),
        )
        published_commit = git_output(str(repo_dir), "rev-parse", "HEAD").strip()
        if (
            git_output(str(repo_dir), "rev-parse", "HEAD^{tree}").strip()
            != prepared.result_tree
        ):
            raise ArtifactError("aggregate attestation changed the validated tree")
        with GitAuth(token, prefix="backport-aggregate-publish-") as auth:
            remote = "origin"
            if prepared.push_repository != prepared.repository:
                remote = "push_target"
                run_git(
                    str(repo_dir),
                    "remote",
                    "add",
                    remote,
                    github_https_url(prepared.push_repository),
                )
            old_sha = phased._remote_branch_sha(
                str(repo_dir),
                remote,
                prepared.branch_name,
                auth.env(),
            )
            push_args = ["push"]
            if old_sha:
                push_args.append(
                    f"--force-with-lease=refs/heads/{prepared.branch_name}:{old_sha}",
                )
            push_args.extend([
                remote,
                f"HEAD:refs/heads/{prepared.branch_name}",
            ])
            run_git(str(repo_dir), *push_args, env=auth.env())
            remote_sha = phased._remote_branch_sha(
                str(repo_dir),
                remote,
                prepared.branch_name,
                auth.env(),
            )
            if remote_sha != published_commit:
                raise ArtifactError("aggregate remote does not match published commit")

    result, branch_applied = _report_results(report)
    for candidate in result.results:
        if candidate.resolutions:
            candidate.resolved_commit_sha = published_source_commits.get(
                candidate.source_pr_number,
            )
    push_repo = (
        prepared.push_repository
        if prepared.push_repository != prepared.repository
        else None
    )
    existing = find_existing_pr(
        gh,
        prepared.repository,
        prepared.push_repository,
        prepared.branch_name,
    )
    previous_body = (
        str(getattr(existing, "body", "") or "")
        if existing is not None
        else None
    )
    labels = [repo_entry.backport_label]
    if any(
        candidate.resolved_by_ai
        for candidate in [
            *result.results,
            *branch_applied,
            *parse_previous_applied(previous_body or ""),
        ]
    ):
        labels.append(repo_entry.llm_conflict_label)
    body = with_desired_labels(
        build_pr_body(
            result,
            branch_applied=branch_applied,
            previous_body=previous_body,
        ),
        labels,
    )
    title = bounded_title(
        f"[backport] Backport sweep for {prepared.target_branch}",
    )
    repo = gh.get_repo(prepared.repository)
    if existing is None:
        pull = create_pull_from_push_repo(
            repo,
            base_repo=prepared.repository,
            push_repo=push_repo,
            title=title,
            body=body,
            head_branch=prepared.branch_name,
            base_branch=prepared.target_branch,
            draft=False,
        )
    else:
        existing.edit(title=title, body=body)
        pull = existing
        if bool(getattr(existing, "draft", False)):
            node_id = str(getattr(existing, "node_id", "") or "")
            if node_id:
                _mark_ready_for_review(token, node_id)
    try:
        pull.add_to_labels(*labels)
    except Exception as exc:  # noqa: BLE001 - body marker is self-healing
        logger.warning("Could not apply aggregate PR label: %s", exc)
    url = str(getattr(pull, "html_url", "") or "")
    number = int(getattr(pull, "number", 0) or 0)
    if not url or number <= 0:
        raise ArtifactError("aggregate pull request identity is invalid")
    comment_urls = _reconcile_aggregate_diff_comments(
        pull,
        result,
        branch_applied=branch_applied,
        bot_login=publisher["identity"],
    )
    if comment_urls:
        linked_body = with_desired_labels(
            build_pr_body(
                result,
                branch_applied=branch_applied,
                previous_body=body,
                comment_urls=comment_urls,
            ),
            labels,
        )
        try:
            pull.edit(body=linked_body)
        except Exception as exc:  # noqa: BLE001 - comments remain authoritative
            logger.warning("Could not link aggregate PR body to AI comments: %s", exc)

    write_publication_manifest(
        root,
        kind="backport-aggregate-publication",
        source_manifest_file=validated.manifest_path.name,
        source_manifest_sha256=validated.manifest_sha256,
        publisher=publisher,
        final_state={
            "repository": prepared.repository,
            "push_repository": prepared.push_repository,
            "target_branch": prepared.target_branch,
            "base_commit": prepared.base_commit,
            "branch_name": prepared.branch_name,
            "remote_ref": f"refs/heads/{prepared.branch_name}",
            "remote_ref_sha": published_commit,
            "published_tree": prepared.result_tree,
            "pull_request_number": number,
            "pull_request_url": url,
            "pull_request_state": str(getattr(pull, "state", "") or "open"),
            "candidate_count": len(report["candidates"]),
        },
        final_state_keys=_PUBLICATION_STATE_KEYS,
    )
    return url


def _load_handoffs(
    directory: str | Path,
) -> tuple[list[Any], list[dict[str, Any]]]:
    roots = {
        manifest.parent.resolve()
        for manifest in Path(directory).rglob("prepared.json")
    }
    if len(roots) > 100:
        raise ArtifactError("aggregate candidate count exceeds 100")
    candidates: list[Any] = []
    failures: list[dict[str, Any]] = []
    identities: set[tuple[str, str, int]] = set()
    for root in sorted(roots):
        validation_path = root / "validated.json"
        if validation_path.exists():
            validation = phased.load_validation(root)
            if validation.status == "passed":
                candidate = load_validated(root)
                prepared = candidate.prepared
                candidates.append(candidate)
            else:
                failure = phased.load_failure_handoff(root)
                prepared = failure["prepared"]
                failures.append(failure)
        else:
            failure = phased.load_failure_handoff(root)
            prepared = failure["prepared"]
            failures.append(failure)
        identity = (
            prepared.repository,
            prepared.target_branch,
            prepared.source_pr_number,
        )
        if identity in identities:
            raise ArtifactError("aggregate candidate results are duplicated")
        identities.add(identity)
    return candidates, failures


def _failure_row(failure: dict[str, Any]) -> dict[str, Any]:
    prepared = failure["prepared"]
    descriptor = phased._parse_discovery(
        load_json(prepared.discovery_path, max_bytes=4 * 1024 * 1024),
    )
    metadata = phased._parse_metadata(
        phased._load_json(prepared.metadata_path),
    )
    kind = str(failure["failure_kind"])
    detail = str(failure["detail"])
    if kind.startswith("validation-"):
        outcome = "skipped-validation-failed"
        detail = validation_failure_detail(detail)
    else:
        outcome = "skipped-conflict"
    resolutions = list(metadata["resolutions"])
    return {
        "source_pr_number": prepared.source_pr_number,
        "source_pr_title": descriptor["source_pr"]["title"],
        "source_pr_url": descriptor["source_pr"]["url"],
        "outcome": outcome,
        "detail": f"{kind}: {detail}",
        "resolved_by_ai": bool(
            prepared.attempt == 1
            or any(item["source"] == "llm" for item in resolutions)
        ),
        "resolutions": resolutions,
        "resolved_commit_sha": None,
        "prepared_manifest_sha256": prepared.manifest_sha256,
        "validated_manifest_sha256": failure["source_manifest_sha256"],
        "patch_sha256": prepared.patch_sha256,
    }


def _existing_branch_head(repo_dir: Path, branch: str) -> str | None:
    result = run_git(
        str(repo_dir),
        "ls-remote",
        "--heads",
        "origin",
        f"refs/heads/{branch}",
        check=False,
    )
    if result.returncode != 0:
        raise ArtifactError("could not inspect existing aggregate branch")
    line = result.stdout.strip()
    if not line:
        return None
    parts = line.split()
    if len(parts) != 2 or not re.fullmatch(r"[0-9a-f]{40}", parts[0]):
        raise ArtifactError("existing aggregate branch response is invalid")
    return parts[0]


def _source_prs_on_branch(repo_dir: str, base_commit: str) -> list[int]:
    subjects = git_output(
        repo_dir,
        "log",
        "--format=%s",
        f"{base_commit}..HEAD",
    )
    values: list[int] = []
    for subject in subjects.splitlines():
        match = _SOURCE_RE.match(subject)
        if match:
            values.append(int(match.group("number")))
    return list(dict.fromkeys(reversed(values)))


def _source_commit_map(repo_dir: str, base_commit: str) -> dict[int, str]:
    records = git_output(
        repo_dir,
        "log",
        "--reverse",
        "--format=%H%x00%s",
        f"{base_commit}..HEAD",
    )
    commits: dict[int, str] = {}
    for record in records.splitlines():
        commit, separator, subject = record.partition("\0")
        match = _SOURCE_RE.match(subject) if separator else None
        if (
            match is not None
            and re.fullmatch(r"[0-9a-f]{40}", commit)
        ):
            commits[int(match.group("number"))] = commit
    return commits


def _source_author(repo_dir: str, source_sha: str) -> str:
    fetched = run_git(
        repo_dir,
        "fetch",
        "origin",
        source_sha,
        check=False,
    )
    if fetched.returncode != 0:
        return ""
    raw = run_git_bytes(
        repo_dir,
        "show",
        "-s",
        "--format=%an%x00%ae",
        source_sha,
        check=False,
    )
    if raw.returncode != 0:
        return ""
    parts = raw.stdout.rstrip(b"\n").split(b"\0")
    if len(parts) != 2:
        return ""
    try:
        name, email = (part.decode("utf-8") for part in parts)
    except UnicodeDecodeError:
        return ""
    if (
        not name
        or not email
        or any(character in name + email for character in "\r\n<>")
    ):
        return ""
    return f"{name} <{email}>"


def _load_index(
    path: Path,
    *,
    kind: str = "backport-aggregate-index",
) -> dict[str, Any]:
    raw = load_json(path, max_bytes=1024 * 1024)
    data = _exact(raw, _INDEX_KEYS, "aggregate index")
    if data["schema_version"] != SCHEMA_VERSION or data["kind"] != kind:
        raise ArtifactError("unsupported aggregate index")
    groups = data["groups"]
    if not isinstance(groups, list) or len(groups) > _MAX_GROUPS:
        raise ArtifactError("aggregate index groups are invalid")
    seen_directories: set[str] = set()
    seen_targets: set[tuple[str, str]] = set()
    for raw_group in groups:
        group = _exact(raw_group, _INDEX_GROUP_KEYS, "aggregate index group")
        directory = group["directory"]
        if (
            not isinstance(directory, str)
            or not re.fullmatch(r"group-[0-9]{3}", directory)
        ):
            raise ArtifactError("aggregate group directory is invalid")
        if group["status"] not in {
            "ready",
            "no-change",
            "refused",
            "passed",
            "failed",
        }:
            raise ArtifactError("aggregate group status is invalid")
        if not isinstance(group["repository"], str):
            raise ArtifactError("aggregate group repository is invalid")
        if not isinstance(group["target_branch"], str):
            raise ArtifactError("aggregate group target branch is invalid")
        target = (group["repository"], group["target_branch"])
        if directory in seen_directories:
            raise ArtifactError("aggregate group directories are duplicated")
        if target in seen_targets:
            raise ArtifactError("aggregate repository and branch groups are duplicated")
        seen_directories.add(directory)
        seen_targets.add(target)
    return data


def _load_report(validated: Any) -> dict[str, Any]:
    prepared = validated.prepared
    if prepared.aggregate_path is None or prepared.aggregate_sha256 is None:
        raise ArtifactError("validated artifact is not a rolling aggregate")
    raw = load_json(prepared.aggregate_path, max_bytes=4 * 1024 * 1024)
    report = _exact(raw, _REPORT_KEYS, "aggregate report")
    if (
        report["schema_version"] != SCHEMA_VERSION
        or report["kind"] != "backport-aggregate-report"
    ):
        raise ArtifactError("unsupported aggregate report")
    expected = {
        "repository": prepared.repository,
        "push_repository": prepared.push_repository,
        "target_branch": prepared.target_branch,
        "base_commit": prepared.base_commit,
        "branch_name": prepared.branch_name,
    }
    for key, value in expected.items():
        if report[key] != value:
            raise ArtifactError(f"aggregate report {key} differs from prepared artifact")
    series_file = report["series_file"]
    if (
        not isinstance(series_file, str)
        or "/" in series_file
        or "\\" in series_file
    ):
        raise ArtifactError("aggregate series file name is invalid")
    series_path = prepared.aggregate_path.parent / series_file
    try:
        if (
            series_path.is_symlink()
            or not series_path.is_file()
            or series_path.resolve().parent != prepared.aggregate_path.parent
        ):
            raise ArtifactError("aggregate series is not a contained regular file")
    except OSError as exc:
        raise ArtifactError(f"cannot inspect aggregate series: {exc}") from exc
    expected_sha = report["series_sha256"]
    expected_bytes = report["series_bytes"]
    if (
        not isinstance(expected_sha, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_sha)
        or not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or not 0 < expected_bytes <= _MAX_SERIES_BYTES
    ):
        raise ArtifactError("aggregate series identity is invalid")
    actual_sha, actual_bytes = sha256_file(
        series_path,
        max_bytes=_MAX_SERIES_BYTES,
    )
    if (actual_sha, actual_bytes) != (expected_sha, expected_bytes):
        raise ArtifactError("aggregate commit series digest or size differs")
    prior = report["prior_source_prs"]
    if (
        not isinstance(prior, list)
        or len(prior) > 1000
        or not all(
            isinstance(value, int)
            and not isinstance(value, bool)
            and value > 0
            for value in prior
        )
    ):
        raise ArtifactError("aggregate prior source PRs are invalid")
    existing_head = report["existing_head"]
    if existing_head is not None and (
        not isinstance(existing_head, str)
        or not re.fullmatch(r"[0-9a-f]{40}", existing_head)
    ):
        raise ArtifactError("aggregate existing head is invalid")
    candidates = report["candidates"]
    if not isinstance(candidates, list) or not 1 <= len(candidates) <= 100:
        raise ArtifactError("aggregate candidate report is invalid")
    seen_candidates: set[int] = set()
    for candidate in candidates:
        item = _exact(candidate, _CANDIDATE_KEYS, "aggregate candidate")
        if (
            not isinstance(item["source_pr_number"], int)
            or isinstance(item["source_pr_number"], bool)
            or item["source_pr_number"] <= 0
        ):
            raise ArtifactError("aggregate candidate source PR is invalid")
        if item["source_pr_number"] in seen_candidates:
            raise ArtifactError("aggregate candidate source PRs are duplicated")
        seen_candidates.add(item["source_pr_number"])
        for key in ("source_pr_title", "source_pr_url", "outcome", "detail"):
            if (
                not isinstance(item[key], str)
                or len(item[key].encode("utf-8")) > 16 * 1024
            ):
                raise ArtifactError(f"aggregate candidate {key} is invalid")
        if item["outcome"] not in {
            "applied",
            "skipped-existing",
            "skipped-conflict",
            "skipped-validation-failed",
        }:
            raise ArtifactError("aggregate candidate outcome is invalid")
        if not isinstance(item["resolved_by_ai"], bool):
            raise ArtifactError("aggregate candidate AI flag is invalid")
        resolutions = item["resolutions"]
        if not isinstance(resolutions, list) or len(resolutions) > 2048:
            raise ArtifactError("aggregate candidate resolutions are invalid")
        for resolution in resolutions:
            _validate_resolution(resolution)
        resolved_commit_sha = item["resolved_commit_sha"]
        if resolved_commit_sha is not None and (
            not isinstance(resolved_commit_sha, str)
            or not re.fullmatch(r"[0-9a-f]{40}", resolved_commit_sha)
        ):
            raise ArtifactError("aggregate candidate resolution commit is invalid")
        if resolutions and item["outcome"] == "applied" and resolved_commit_sha is None:
            raise ArtifactError("aggregate candidate resolution commit is missing")
        for key in (
            "prepared_manifest_sha256",
            "validated_manifest_sha256",
            "patch_sha256",
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", str(item[key])):
                raise ArtifactError(f"aggregate candidate {key} is invalid")
    return report


def _report_results(
    report: dict[str, Any],
) -> tuple[BranchSweepResult, list[CandidateResult]]:
    current = [
        CandidateResult(
            source_pr_number=item["source_pr_number"],
            source_pr_title=item["source_pr_title"],
            outcome=item["outcome"],
            detail=item["detail"],
            resolutions=[
                phased._resolution_from_metadata(resolution)
                for resolution in item["resolutions"]
            ],
            resolved_by_ai=item["resolved_by_ai"],
            resolved_commit_sha=item["resolved_commit_sha"],
        )
        for item in report["candidates"]
    ]
    applied = [
        CandidateResult(
            source_pr_number=number,
            source_pr_title=f"Previously aggregated source PR #{number}",
            outcome="skipped-existing",
            detail=DETAIL_ALREADY_ON_SWEEP_BRANCH,
        )
        for number in report["prior_source_prs"]
    ]
    applied.extend(
        item
        for item in current
        if item.outcome in {"applied", "skipped-existing"}
    )
    return (
        BranchSweepResult(
            target_branch=report["target_branch"],
            candidates_found=len(current),
            results=current,
        ),
        applied,
    )


def _validate_resolution(raw: Any) -> None:
    item = _exact(
        raw,
        {
            "path",
            "resolved",
            "summary",
            "source",
            "resolution_diff",
            "reviewer_diff",
        },
        "aggregate candidate resolution",
    )
    if (
        not isinstance(item["path"], str)
        or not item["path"]
        or len(item["path"].encode("utf-8")) > 4096
        or not isinstance(item["resolved"], bool)
        or not isinstance(item["summary"], str)
        or len(item["summary"].encode("utf-8")) > 8192
        or item["source"] not in {"llm", "automatic"}
    ):
        raise ArtifactError("aggregate candidate resolution is invalid")
    for key in ("resolution_diff", "reviewer_diff"):
        value = item[key]
        if value is not None and (
            not isinstance(value, str)
            or len(value.encode("utf-8")) > 4 * 1024 * 1024
        ):
            raise ArtifactError(f"aggregate candidate {key} is invalid")


def _reconcile_aggregate_diff_comments(
    pull: Any,
    result: BranchSweepResult,
    *,
    branch_applied: list[CandidateResult],
    bot_login: str,
) -> dict[int, str]:
    desired = {
        candidate.source_pr_number: candidate
        for candidate in result.results
        if candidate.outcome == "applied" and candidate.resolutions
    }
    on_branch = {
        candidate.source_pr_number
        for candidate in branch_applied
    }
    try:
        prior_urls = marked_source_pr_urls(pull, bot_login=bot_login)
    except Exception as exc:  # noqa: BLE001 - publication already succeeded
        logger.warning("Could not inspect prior aggregate AI comments: %s", exc)
        prior_urls = {}
    comment_urls = {
        source_pr: url
        for source_pr, url in prior_urls.items()
        if source_pr in on_branch and source_pr not in desired
    }
    stale = set(prior_urls) - on_branch - set(desired)
    for source_pr in sorted(set(desired) | stale):
        candidate = desired.get(source_pr)
        try:
            links = reconcile_diff_comments(
                pull,
                source_pr,
                candidate.resolutions if candidate is not None else [],
                source_title=(
                    candidate.source_pr_title
                    if candidate is not None
                    else None
                ),
                resolved_commit_sha=(
                    candidate.resolved_commit_sha
                    if candidate is not None
                    else None
                ),
                bot_login=bot_login,
            )
            if links:
                comment_urls[source_pr] = next(iter(links.values()))
        except Exception as exc:  # noqa: BLE001 - publication already succeeded
            logger.warning(
                "Could not reconcile aggregate AI comment for source PR #%d: %s",
                source_pr,
                exc,
            )
    return comment_urls


def _mark_ready_for_review(token: str, node_id: str) -> None:
    GitHubGraphQLClient(token).execute(
        """
        mutation($id: ID!) {
          markPullRequestReadyForReview(input: {pullRequestId: $id}) {
            pullRequest { isDraft }
          }
        }
        """,
        {"id": node_id},
    )


def _exact(raw: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ArtifactError(f"{label} must be an object")
    unknown = sorted(set(raw) - keys)
    missing = sorted(keys - set(raw))
    if unknown or missing:
        raise ArtifactError(
            f"{label} keys invalid: unknown={unknown}, missing={missing}",
        )
    return raw


def _write_outputs(result: dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT", "")
    if not path:
        return
    groups = result["groups"]
    passed = sum(group["status"] == "passed" for group in groups)
    failed = sum(group["status"] == "failed" for group in groups)
    ready = sum(group["status"] == "ready" for group in groups)
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"passed_count={passed}\n")
        handle.write(f"failed_count={failed}\n")
        handle.write(f"ready_count={ready}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    phases = parser.add_subparsers(dest="phase", required=True)

    preparation = phases.add_parser("prepare")
    preparation.add_argument("--candidates-directory", required=True)
    preparation.add_argument("--output-directory", required=True)
    preparation.add_argument("--max-candidates", type=int, default=0)

    validation = phases.add_parser("validate")
    validation.add_argument("--artifact-directory", required=True)

    preflight = phases.add_parser("preflight-publish")
    preflight.add_argument("--artifact-directory", required=True)
    preflight.add_argument("--repositories-output", required=True)

    publication = phases.add_parser("publish")
    publication.add_argument("--artifact-directory", required=True)
    publication.add_argument(
        "--token",
        default=os.environ.get("PUBLISH_GITHUB_TOKEN", ""),
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.phase == "prepare":
        result = prepare(
            registry_path=args.registry,
            candidates_directory=args.candidates_directory,
            output_directory=args.output_directory,
            max_candidates=args.max_candidates,
        )
        _write_outputs(result)
        return 0
    if args.phase == "validate":
        result = validate(
            registry_path=args.registry,
            artifact_directory=args.artifact_directory,
        )
        _write_outputs(result)
        return 0
    if args.phase == "preflight-publish":
        preflight_publish(
            registry_path=args.registry,
            artifact_directory=args.artifact_directory,
            repositories_output=args.repositories_output,
        )
        return 0
    if not args.token:
        parser.error("publish requires --token or PUBLISH_GITHUB_TOKEN")
    urls = publish(
        registry_path=args.registry,
        artifact_directory=args.artifact_directory,
        token=args.token,
    )
    for url in urls:
        print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
