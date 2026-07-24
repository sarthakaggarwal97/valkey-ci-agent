"""Daily backport sweep across registered release branches."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from github import Auth, Github

from scripts.backport import sweep_discovery
from scripts.backport.application import apply_candidate
from scripts.backport.git import run_git as _run_git
from scripts.backport.publication import (
    TargetHeadChanged,
    assert_target_head_unchanged,
    get_target_head,
)
from scripts.backport.sweep_failures import (
    campaign_made_no_progress,
    clear_failure_markers,
    failure_marker_exists,
    failure_marker_ref,
    record_failure_marker,
)
from scripts.backport.sweep_git import (
    branch_has_changes,
    clone_target_branch,
    list_already_applied,
    list_applied_prs_on_branch,
    push_backport_branch,
    safe_tmp_component,
    sync_target_branch_to_source,
)
from scripts.backport.sweep_graphql import GitHubGraphQLClient
from scripts.backport.sweep_models import (
    DETAIL_ALREADY_ON_SWEEP_BRANCH,
    BranchSweepResult,
    CandidateResult,
    ProjectBackportCandidate,
)
from scripts.backport.sweep_prs import (
    delete_stale_backport_branch,
    find_existing_pr,
    upsert_pr,
)
from scripts.backport.sweep_reporting import (
    build_summary,
    result_is_on_backport_branch,
    validation_failure_detail,
)
from scripts.backport.sweep_validation import (
    run_test_commands,
    validate_backport_branch,
    validate_branch_with_optional_repair,
)
from scripts.backport.transaction import (
    apply_candidate_transaction,
    validate_baseline,
)
from scripts.common.git_auth import GitAuth, github_https_url
from scripts.common.job_summary import emit_job_summary

if TYPE_CHECKING:
    from scripts.backport.registry import BranchEntry, RepoEntry  # noqa: F401

logger = logging.getLogger(__name__)

_DEFAULT_BRANCH_FIELDS = sweep_discovery.DEFAULT_BRANCH_FIELDS
_DEFAULT_STATUS_FIELD = sweep_discovery.DEFAULT_STATUS_FIELD
_DEFAULT_STATUS_VALUE = sweep_discovery.DEFAULT_STATUS_VALUE
ProjectBackportDiscovery = sweep_discovery.ProjectBackportDiscovery
_BRANCH_PREFIX = "agent/backport/sweep"


def run_backport_sweep(
    *,
    repo_entry: "RepoEntry",
    branch_entry: "BranchEntry",
    github_token: str,
    status_field: str = _DEFAULT_STATUS_FIELD,
    status_value: str = _DEFAULT_STATUS_VALUE,
    branch_fields: list[str] | None = None,
    test_commands_override: list[str] | None = None,
    discover_only: bool = False,
    max_candidates: int = 5,
    skip_if_open_pr: bool = False,
    suppress_unchanged_failures: bool = False,
) -> BranchSweepResult:
    repo_full_name = repo_entry.repo
    push_repo = repo_entry.effective_push_repo
    target_branch = branch_entry.branch
    test_commands = (
        test_commands_override
        if test_commands_override is not None
        else list(repo_entry.build_commands)
    )
    validation_setup_commands = (
        [] if test_commands_override is not None
        else list(repo_entry.validation_setup_commands)
    )
    validation_rules = (
        [] if test_commands_override is not None
        else list(repo_entry.validation_rules)
    )

    gh = Github(auth=Auth.Token(github_token))

    if skip_if_open_pr:
        backport_branch = f"{_BRANCH_PREFIX}/{target_branch}"
        existing_pr = find_existing_pr(
            gh,
            repo_full_name,
            push_repo,
            backport_branch,
        )
        if existing_pr is not None:
            logger.info(
                "Branch %s: open sweep PR #%d exists, preserving it unchanged",
                target_branch,
                existing_pr.number,
            )
            result = BranchSweepResult(
                target_branch=target_branch,
                pr_url=str(existing_pr.html_url),
                skipped_open_pr=True,
            )
            emit_job_summary(build_summary([result]))
            return result

    discovery = ProjectBackportDiscovery(
        GitHubGraphQLClient(github_token),
        project_owner=repo_entry.project_owner,
        project_number=branch_entry.project_number,
        source_repo=repo_full_name,
        project_owner_type=repo_entry.project_owner_type,
        status_field=status_field,
        status_value=status_value,
        branch_fields=branch_fields,
        implicit_target_branch=target_branch,
    )
    candidates = discovery.discover([target_branch]).get(target_branch, [])
    candidates.sort(key=lambda candidate: candidate.merged_at or "")

    if max_candidates > 0:
        logger.info(
            "Branch %s: %d candidate(s) found, will apply up to %d successful cherry-pick(s)",
            target_branch,
            len(candidates),
            max_candidates,
        )
    else:
        logger.info("Branch %s: %d candidate(s)", target_branch, len(candidates))

    if discover_only:
        for candidate in candidates:
            logger.info(
                "  PR #%d: %s (%s)",
                candidate.source_pr_number,
                candidate.source_pr_title,
                candidate.merge_commit_sha or "no merge sha",
            )
        result = BranchSweepResult(
            target_branch=target_branch,
            candidates_found=len(candidates),
        )
        emit_job_summary(build_summary([result]))
        return result

    if not candidates:
        result = BranchSweepResult(target_branch=target_branch)
        _clear_failure_markers_best_effort(gh, push_repo, target_branch)
        emit_job_summary(build_summary([result]))
        return result

    target_head_sha = get_target_head(gh, repo_full_name, target_branch)
    marker_ref = failure_marker_ref(
        target_branch,
        target_head_sha,
        candidates,
    )
    if suppress_unchanged_failures and failure_marker_exists(
        gh, push_repo, marker_ref, target_sha=target_head_sha,
    ):
        logger.info(
            "Branch %s: unchanged target and candidate set previously made no "
            "progress; skipping retry (%s)",
            target_branch,
            marker_ref,
        )
        result = BranchSweepResult(
            target_branch=target_branch,
            candidates_found=len(candidates),
            retry_suppressed=True,
            failure_marker_ref=marker_ref,
        )
        emit_job_summary(build_summary([result]))
        return result

    result = _process_branch(
        gh=gh,
        repo_full_name=repo_full_name,
        github_token=github_token,
        target_branch=target_branch,
        candidates=candidates,
        push_repo=push_repo,
        test_commands=test_commands,
        validation_setup_commands=validation_setup_commands,
        max_applied=max_candidates,
        language=repo_entry.language,
        build_commands=list(repo_entry.build_commands) or None,
        validation_rules=validation_rules,
        repair_validation_failures=repo_entry.repair_validation_failures,
        max_conflicting_files=repo_entry.max_conflicting_files,
        backport_label=repo_entry.backport_label,
        llm_conflict_label=repo_entry.llm_conflict_label,
        expected_target_sha=target_head_sha,
    )
    if campaign_made_no_progress(
        candidates,
        result.results,
        error=result.error,
        pr_url=result.pr_url,
    ):
        try:
            record_failure_marker(
                gh,
                push_repo,
                marker_ref,
                target_branch=target_branch,
                target_sha=target_head_sha,
            )
            result.failure_marker_ref = marker_ref
            logger.info(
                "Branch %s: recorded failed-campaign marker %s",
                target_branch,
                marker_ref,
            )
        except Exception as exc:
            logger.exception(
                "Branch %s: could not record failed-campaign marker",
                target_branch,
            )
            result.error = f"could not record failed-campaign marker: {exc}"
            result.results.append(
                CandidateResult(
                    source_pr_number=0,
                    source_pr_title=f"Branch {target_branch}",
                    outcome="error",
                    detail=result.error,
                )
            )
    elif not result.error:
        _clear_failure_markers_best_effort(gh, push_repo, target_branch)

    emit_job_summary(build_summary([result]))
    return result


def _process_branch(
    *,
    gh: Any,
    repo_full_name: str,
    github_token: str,
    target_branch: str,
    candidates: list[ProjectBackportCandidate],
    push_repo: str,
    test_commands: list[str],
    validation_setup_commands: list[str] | None = None,
    max_applied: int = 0,
    language: str = "c",
    build_commands: list[str] | None = None,
    validation_rules: list[Any] | None = None,
    repair_validation_failures: bool = False,
    max_conflicting_files: int = 100,
    backport_label: str = "backport",
    llm_conflict_label: str = "ai-resolved-conflicts",
    expected_target_sha: str | None = None,
) -> BranchSweepResult:
    result = BranchSweepResult(
        target_branch=target_branch,
        candidates_found=len(candidates),
    )
    tmpdir = tempfile.mkdtemp(prefix=f"backport-{safe_tmp_component(target_branch)}-")

    try:
        with GitAuth(github_token, prefix="backport-sweep-git-askpass-") as git_auth:
            git_env = git_auth.env()
            cloned_target_sha = clone_target_branch(
                repo_full_name,
                target_branch,
                tmpdir,
                git_env,
            )
            if not cloned_target_sha:
                raise RuntimeError(
                    f"could not capture cloned target head for {target_branch}"
                )
            if (
                expected_target_sha is not None
                and cloned_target_sha != expected_target_sha
            ):
                raise TargetHeadChanged(
                    f"{repo_full_name}:{target_branch} moved before the target "
                    f"clone completed (expected {expected_target_sha}, cloned "
                    f"{cloned_target_sha}); retrying from a fresh snapshot is required"
                )

            if push_repo != repo_full_name:
                sync_target_branch_to_source(
                    gh,
                    push_repo,
                    repo_full_name,
                    target_branch,
                )

            backport_branch = f"{_BRANCH_PREFIX}/{target_branch}"
            existing_pr = find_existing_pr(
                gh,
                repo_full_name,
                push_repo,
                backport_branch,
            )

            if existing_pr:
                logger.info(
                    "Found existing PR #%d for %s, fetching branch...",
                    existing_pr.number,
                    target_branch,
                )
                push_url = github_https_url(push_repo)
                _run_git(tmpdir, "remote", "add", "push_target", push_url, env=git_env)
                _run_git(tmpdir, "fetch", "push_target", backport_branch, env=git_env)
                _run_git(tmpdir, "checkout", f"push_target/{backport_branch}")
                _run_git(tmpdir, "checkout", "-B", backport_branch)
                rebase_result = subprocess.run(
                    ["git", "rebase", f"origin/{target_branch}"],
                    cwd=tmpdir,
                    capture_output=True,
                    text=True,
                )
                if rebase_result.returncode != 0:
                    _run_git(tmpdir, "rebase", "--abort")
                    raise RuntimeError(
                        f"Could not rebase existing backport branch "
                        f"{backport_branch} onto origin/{target_branch}. "
                        f"The existing backport PR #{existing_pr.number} "
                        f"likely has conflicts with the refreshed release "
                        f"branch. Rebase manually or close the PR before "
                        f"the next sweep. Git stderr: "
                        f"{rebase_result.stderr.strip()[:300]}"
                    )
            else:
                delete_stale_backport_branch(gh, push_repo, backport_branch)
                _run_git(tmpdir, "checkout", "-b", backport_branch)
                push_url = github_https_url(push_repo)
                _run_git(tmpdir, "remote", "add", "push_target", push_url, env=git_env)

            baseline = validate_baseline(
                tmpdir,
                target_branch,
                validation_setup_commands or [],
                test_commands,
                validation_rules or [],
                run_commands=run_test_commands,
                validate_func=validate_backport_branch,
            )
            if not baseline.ok:
                logger.warning(
                    "Validation baseline failed for %s during %s.\n"
                    "Output (last 4000 chars):\n%s",
                    target_branch,
                    baseline.phase,
                    baseline.output[-4000:],
                )
                raise RuntimeError(
                    f"validation baseline {baseline.phase} failed: "
                    + (baseline.output[:500] or "validation command failed")
                )

            already_applied = list_already_applied(
                tmpdir,
                target_branch,
                backport_branch,
            )
            logger.info("Already applied on %s: %s", backport_branch, already_applied)

            applied_count = 0

            for index, candidate in enumerate(candidates):
                if max_applied > 0 and applied_count >= max_applied:
                    logger.info(
                        "Branch %s: reached cap of %d applied backport(s); deferring remaining %d candidate(s) to next sweep",
                        target_branch,
                        max_applied,
                        len(candidates) - index,
                    )
                    break

                if str(candidate.source_pr_number) in already_applied:
                    result.results.append(
                        CandidateResult(
                            source_pr_number=candidate.source_pr_number,
                            source_pr_title=candidate.source_pr_title,
                            outcome="skipped-existing",
                            detail=DETAIL_ALREADY_ON_SWEEP_BRANCH,
                        )
                    )
                    continue

                candidate_result = apply_candidate_transaction(
                    tmpdir,
                    candidate,
                    repo_full_name,
                    git_env,
                    target_branch=target_branch,
                    setup_commands=validation_setup_commands or [],
                    test_commands=test_commands,
                    validation_rules=validation_rules or [],
                    repair_validation_failures=repair_validation_failures,
                    language=language,
                    build_commands=build_commands,
                    max_conflicting_files=max_conflicting_files,
                    run_commands=run_test_commands,
                    apply_func=apply_candidate,
                    validate_func=validate_branch_with_optional_repair,
                )
                result.results.append(candidate_result)

                if candidate_result.outcome == "skipped-validation-failed":
                    candidate_result.detail = validation_failure_detail(
                        candidate_result.detail,
                    )
                    logger.warning(
                        "Validation failed for candidate #%d on %s; "
                        "discarded isolated candidate and continuing.",
                        candidate.source_pr_number,
                        target_branch,
                    )
                    continue
                if candidate_result.outcome != "applied":
                    continue

                applied_count += 1

            committed = [
                item for item in result.results
                if result_is_on_backport_branch(item)
            ]
            if committed and branch_has_changes(tmpdir, target_branch):
                try:
                    assert_target_head_unchanged(
                        gh,
                        repo_full_name,
                        target_branch,
                        cloned_target_sha,
                    )
                except Exception as exc:
                    for item in result.results:
                        if item.outcome == "applied":
                            item.outcome = "error"
                            item.detail = f"publication blocked: {exc}"
                    raise
                try:
                    push_backport_branch(
                        tmpdir,
                        backport_branch,
                        git_env,
                        force_with_lease=existing_pr is not None,
                    )
                except Exception as exc:
                    for item in result.results:
                        if item.outcome == "applied":
                            item.outcome = "error"
                            item.detail = f"push failed: {exc}"
                    raise
                logger.info(
                    "Pushed %d commit(s) to %s/%s",
                    len(committed),
                    push_repo,
                    backport_branch,
                )

                result.pr_url = upsert_pr(
                    gh,
                    repo_full_name,
                    push_repo,
                    target_branch,
                    backport_branch,
                    result,
                    existing_pr,
                    gql=GitHubGraphQLClient(github_token),
                    branch_applied=list_applied_prs_on_branch(
                        tmpdir,
                        target_branch,
                        backport_branch,
                    ),
                    backport_label=backport_label,
                    llm_conflict_label=llm_conflict_label,
                )

    except Exception as exc:
        logger.exception("Error processing branch %s", target_branch)
        result.error = str(exc)
        result.results.append(
            CandidateResult(
                source_pr_number=0,
                source_pr_title=f"Branch {target_branch}",
                outcome="error",
                detail=str(exc),
            )
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return result


def _clear_failure_markers_best_effort(
    gh: Any,
    push_repo: str,
    target_branch: str,
) -> None:
    try:
        clear_failure_markers(gh, push_repo, target_branch)
    except Exception as exc:
        logger.warning(
            "Could not clear obsolete failed-campaign markers for %s:%s: %s",
            push_repo,
            target_branch,
            exc,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        default="repos.yml",
        help="Path to registry YAML (default: repos.yml)",
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="Repository full name (must exist in registry)",
    )
    parser.add_argument(
        "--branch",
        required=True,
        help="Target branch (must exist in registry for this repo)",
    )
    parser.add_argument("--target-token", required=True)
    parser.add_argument("--status-field", default=_DEFAULT_STATUS_FIELD)
    parser.add_argument("--status-value", default=_DEFAULT_STATUS_VALUE)
    parser.add_argument("--branch-fields", default=",".join(_DEFAULT_BRANCH_FIELDS))
    parser.add_argument(
        "--test-commands",
        default="",
        help="Override test commands (newline-separated). Empty = use registry.",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=5,
        help="Cap the number of applied cherry-picks per branch (0 = unlimited)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--discover-only", action="store_true")
    parser.add_argument(
        "--skip-if-open-pr",
        action="store_true",
        help="Preserve an existing rolling PR instead of updating it",
    )
    parser.add_argument(
        "--suppress-unchanged-failures",
        action="store_true",
        help="Skip an unchanged campaign already recorded as making no progress",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    from scripts.backport.registry import load_registry

    registry = load_registry(args.registry)
    repo_entry, branch_entry = registry.get_branch(args.repo, args.branch)

    test_commands_override = None
    if args.test_commands:
        test_commands_override = [
            command.strip()
            for command in args.test_commands.split("\n")
            if command.strip()
        ]

    result = run_backport_sweep(
        repo_entry=repo_entry,
        branch_entry=branch_entry,
        github_token=args.target_token,
        status_field=args.status_field,
        status_value=args.status_value,
        branch_fields=[
            field.strip()
            for field in args.branch_fields.split(",")
            if field.strip()
        ] or None,
        test_commands_override=test_commands_override,
        discover_only=args.discover_only or args.dry_run,
        max_candidates=args.max_candidates,
        skip_if_open_pr=args.skip_if_open_pr,
        suppress_unchanged_failures=args.suppress_unchanged_failures,
    )

    print(json.dumps({
        "branch": result.target_branch,
        "action": (
            "skipped-open-pr"
            if result.skipped_open_pr
            else "skipped-unchanged-failures"
            if result.retry_suppressed
            else "swept"
        ),
        "found": result.candidates_found,
        "applied": result.applied_count,
        "pr": result.pr_url,
        "failure_marker": result.failure_marker_ref,
    }, indent=2))

    if args.discover_only or args.dry_run:
        return

    if result.error:
        logger.error(
            "Backport sweep failure: %s: %s",
            result.target_branch,
            result.error,
        )
        sys.exit(1)

    if result.candidates_found > 0 and result.results:
        errored = [item for item in result.results if item.outcome == "error"]
        if len(errored) == len(result.results):
            logger.error(
                "Backport sweep failure: %s: all %d candidates errored",
                result.target_branch,
                len(errored),
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
