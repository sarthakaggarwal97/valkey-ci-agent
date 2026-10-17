"""Backport PR creation and duplicate detection."""

from __future__ import annotations

import logging
from typing import Any

from github import Github

from scripts.backport.models import (
    BackportPRContext,
    CherryPickResult,
    ResolutionResult,
)
from scripts.backport.utils import build_branch_name, build_pr_title
from scripts.common.github_client import ensure_label, retry_github_call
from scripts.common.text_utils import escape_cell

logger = logging.getLogger(__name__)

# Default label appearance used when the agent has to create a missing
# label on the target repo. Repos can rename the labels via repos.yml,
# but the colors/descriptions only apply at creation time.
_LABEL_DEFAULTS: dict[str, tuple[str, str]] = {
    "backport": ("0e8a16", "Backport PR opened by valkey-ci-agent"),
    "ai-resolved-conflicts": (
        "fbca04",
        "Backport contains AI-authored changes; needs human review",
    ),
}


def build_pull_create_head_ref(
    base_repo: str,
    push_repo: str | None,
    branch_name: str,
) -> str:
    """Return the head ref used when creating a pull request."""
    if not push_repo or push_repo == base_repo:
        return branch_name
    owner = push_repo.split("/")[0]
    return f"{owner}:{branch_name}"


def build_pull_search_head_ref(
    base_repo: str,
    push_repo: str | None,
    branch_name: str,
) -> str:
    """Return the head ref used when searching pull requests."""
    source_repo = push_repo or base_repo
    owner = source_repo.split("/")[0]
    return f"{owner}:{branch_name}"


def create_pull_from_push_repo(
    repo: Any,
    *,
    base_repo: str,
    push_repo: str | None,
    title: str,
    body: str,
    head_branch: str,
    base_branch: str,
    draft: bool | None = None,
) -> Any:
    """Create a PR from either the upstream branch or a different-owner fork."""
    head_ref = build_pull_create_head_ref(base_repo, push_repo, head_branch)
    kwargs: dict[str, Any] = {
        "title": title,
        "body": body,
        "head": head_ref,
        "base": base_branch,
    }
    if draft is not None:
        kwargs["draft"] = draft
    return repo.create_pull(**kwargs)


def pull_matches_push_repo(pr: Any, push_repo: str) -> bool:
    """Return whether a PR head belongs to the expected push repo."""
    head = getattr(pr, "head", None)
    repo = getattr(head, "repo", None)
    full_name = getattr(repo, "full_name", None)
    return isinstance(full_name, str) and full_name == push_repo


def find_pulls_by_head(
    repo: Any,
    *,
    base_repo: str,
    push_repo: str | None,
    branch: str,
    state: str = "open",
    base: str | None = None,
) -> list[Any]:
    """Return the *state* PRs on *repo* whose head is *branch* of the push repo.

    GitHub's head filter names only the owner, so each PR's head repository is
    checked as well. A missing *push_repo* means *base_repo* itself.
    """
    head_ref = build_pull_search_head_ref(base_repo, push_repo, branch)
    filters = {"state": state, "head": head_ref}
    if base:
        filters["base"] = base
    pulls = retry_github_call(
        lambda: list(repo.get_pulls(**filters)),
        retries=3,
        description=f"list {state} PRs for {head_ref}",
    )
    return [pull for pull in pulls if pull_matches_push_repo(pull, push_repo or base_repo)]


def apply_labels(repo: Any, pr: Any, labels: list[str]) -> None:
    """Apply *labels* to *pr*, creating any missing on *repo* first.

    Best-effort and idempotent: a failure is logged and swallowed, so labeling
    never fails a run whose PR is already open, and ``add_to_labels`` re-adds
    existing labels without error.
    """
    for label in labels:
        color, description = _LABEL_DEFAULTS.get(
            label, ("ededed", f"Created by valkey-ci-agent for label {label!r}"),
        )
        try:
            ensure_label(repo, label, color=color, description=description)
        except Exception as exc:  # noqa: BLE001 - add_to_labels below surfaces a lasting problem
            logger.warning("Could not ensure label %r: %s", label, exc)
    try:
        logger.info("Applying labels %s to PR #%d", labels, pr.number)
        retry_github_call(
            lambda: pr.add_to_labels(*labels),
            retries=3,
            description=f"apply labels to PR #{pr.number}",
        )
    except Exception as exc:  # noqa: BLE001 - labeling must not fail the run
        logger.warning("Failed to apply labels to PR #%d: %s", pr.number, exc)


def _was_llm_resolved(result: ResolutionResult) -> bool:
    return result.resolved_content is not None and result.source == "llm"


class BackportPRCreator:
    """Create backport branches and pull requests via the GitHub API."""

    def __init__(
        self,
        github_client: Github,
        base_repo: str,
        *,
        push_repo: str | None = None,
        backport_label: str = "backport",
        llm_conflict_label: str = "ai-resolved-conflicts",
    ) -> None:
        self._github = github_client
        self._base_repo = base_repo
        self._push_repo = push_repo
        self._backport_label = backport_label or "backport"
        self._llm_conflict_label = llm_conflict_label or "ai-resolved-conflicts"

    def create_backport_pr(
        self,
        context: BackportPRContext,
        cherry_pick_result: CherryPickResult,
        resolution_results: list[ResolutionResult] | None,
        branch_name: str | None = None,
        *,
        ai_involved: bool = False,
        ai_summary: str = "",
    ) -> str:
        """Create backport PR from an already-pushed branch.

        If *branch_name* is provided, the branch is assumed to already
        exist on the remote (pushed from the local cherry-pick clone).
        Otherwise, falls back to creating the branch via the API from
        target branch HEAD (useful for testing).

        Returns the PR URL.

        """
        repo = retry_github_call(
            lambda: self._github.get_repo(self._base_repo),
            retries=3,
            description=f"get repo {self._base_repo}",
        )

        if branch_name is None:
            branch_name = build_branch_name(
                context.source_pr_number, context.target_branch,
            )
        assert branch_name is not None  # for mypy
        title = build_pr_title(context.source_pr_title, context.target_branch)

        had_conflicts = not cherry_pick_result.success
        # AI involvement is broader than per-file conflict resolutions: test
        # adaptation also writes AI-generated content with no resolution row.
        any_ai_changes = ai_involved or bool(
            resolution_results and any(_was_llm_resolved(r) for r in resolution_results)
        )

        body = self.build_pr_body(
            context,
            had_conflicts,
            resolution_results,
            applied_commits=cherry_pick_result.applied_commits,
            ai_involved=any_ai_changes,
            ai_summary=ai_summary,
        )

        # Open the pull request (branch already exists on remote).
        logger.info(
            "Opening backport PR: %s -> %s", branch_name, context.target_branch,
        )
        pr = retry_github_call(
            lambda: create_pull_from_push_repo(
                repo,
                base_repo=self._base_repo,
                push_repo=self._push_repo,
                title=title,
                body=body,
                head_branch=branch_name,
                base_branch=context.target_branch,
            ),
            retries=3,
            description="create backport PR",
        )

        labels = [self._backport_label]
        if any_ai_changes:
            labels.append(self._llm_conflict_label)
        apply_labels(repo, pr, labels)

        logger.info("Backport PR created: %s", pr.html_url)
        return pr.html_url

    @staticmethod
    def build_pr_body(
        context: BackportPRContext,
        had_conflicts: bool,
        resolution_results: list[ResolutionResult] | None,
        *,
        applied_commits: list[str] | None = None,
        comment_links: dict[str, str] | None = None,
        ai_involved: bool = False,
        ai_summary: str = "",
    ) -> str:
        """Build the PR body with links, commit list, conflict info.

        *comment_links* maps a resolved file path to the URL of its AI-diff
        comment. When provided, each resolved file's status line links to its
        comment. The body is built once before comments exist (no links), then
        rebuilt after reconcile with the links filled in.
        """
        links = comment_links or {}
        sections: list[str] = []
        results = resolution_results or []
        resolved_count = sum(result.resolved_content is not None for result in results)
        unresolved_count = len(results) - resolved_count
        any_ai_changes = ai_involved or bool(
            results and any(_was_llm_resolved(result) for result in results)
        )

        if had_conflicts:
            if unresolved_count > 0:
                verdict = (
                    "Cherry-pick encountered conflicts and some files still need "
                    "manual follow-up."
                )
            elif resolved_count > 0:
                verdict = (
                    "Cherry-pick encountered conflicts and the conflicted files were "
                    "resolved automatically."
                )
            else:
                verdict = "Cherry-pick encountered conflicts."
        else:
            verdict = "Cherry-pick applied cleanly with no conflicts."

        sections.append("## Backport Summary\n\n" + verdict)
        sections.append(
            "\n".join([
                "| Field | Value |",
                "|---|---|",
                f"| Source PR | [#{context.source_pr_number}]({context.source_pr_url}) |",
                f"| Source title | {escape_cell(context.source_pr_title)} |",
                f"| Target branch | `{context.target_branch}` |",
                f"| Cherry-picked commits | {len(applied_commits or context.commits)} |",
                f"| Conflicts detected | {'yes' if had_conflicts else 'no'} |",
                f"| Auto-resolved files | {resolved_count} |",
                f"| Unresolved files | {unresolved_count} |",
            ])
        )
        checklist = [
            "- Compare this backport against the source PR before merge.",
        ]
        if any_ai_changes:
            checklist.append(
                "- Review the AI-authored changes carefully for semantic drift."
            )
        elif resolved_count > 0:
            checklist.append(
                "- Review the automatically resolved files carefully for semantic drift."
            )
        if unresolved_count > 0:
            checklist.append(
                "- Resolve the remaining conflicted files or close the PR if the backport is not viable."
            )
        sections.append("### Reviewer Checklist\n\n" + "\n".join(checklist))

        commits_list = "\n".join(f"- `{sha}`" for sha in (applied_commits or context.commits))
        sections.append(f"### Cherry-Picked Commits\n\n{commits_list}")

        # Per-file resolution summaries.
        if results:
            file_lines: list[str] = []
            for result in results:
                status = (
                    "Resolved automatically" if result.resolved_content is not None
                    else "Needs manual resolution"
                )
                link = links.get(result.path)
                suffix = f" ([view diff]({link}))" if link else ""
                file_lines.append(
                    f"- `{result.path}`: {status}. {result.resolution_summary}{suffix}"
                )
            conflict_details = "\n".join(file_lines)
            if any(_was_llm_resolved(r) for r in results):
                conflict_details += (
                    "\n\nAI resolution details are posted as a comment on this "
                    "PR when available."
                )
            sections.append(
                "### Conflict Details\n\n" + conflict_details
            )

        if ai_summary and any_ai_changes:
            sections.append("### AI Adaptation\n\n" + ai_summary)

        if any_ai_changes:
            sections.append(
                "### Human Review Required\n\n"
                "AI was used to resolve conflicts or adapt this backport. "
                "These changes require careful human review. Verify that they "
                "match the intent of the original pull request."
            )

        return "\n\n".join(sections)

    def check_duplicate(
        self,
        source_pr_number: int,
        target_branch: str,
    ) -> str | None:
        """Return existing backport PR URL if one exists, else ``None``.

        Checks for open PRs whose head branch matches the naming
        convention ``backport/<pr>-to-<branch>``.  Also checks recently
        closed PRs to handle label removal and re-addition.

        """
        branch_name = build_branch_name(source_pr_number, target_branch)

        repo = retry_github_call(
            lambda: self._github.get_repo(self._base_repo),
            retries=3,
            description=f"get repo {self._base_repo}",
        )

        logger.info("Checking for duplicate backport PR on branch %s", branch_name)
        for pr in find_pulls_by_head(
            repo, base_repo=self._base_repo, push_repo=self._push_repo, branch=branch_name,
        ):
            logger.info("Found existing open backport PR: %s", pr.html_url)
            return pr.html_url

        # Check closed PRs with matching head branch. Only treat a closed
        # PR as a duplicate if it was merged — a closed-but-not-merged PR
        # means the work was abandoned, and we should be free to reopen a
        # fresh backport. GitHub returns merged PRs as state=closed with
        # merged_at set.
        for pr in find_pulls_by_head(
            repo, base_repo=self._base_repo, push_repo=self._push_repo, branch=branch_name,
            state="closed",
        ):
            if pr.merged_at is not None:
                logger.info(
                    "Found existing merged backport PR: %s", pr.html_url,
                )
                return pr.html_url

        logger.info("No duplicate backport PR found for %s", branch_name)
        return None
