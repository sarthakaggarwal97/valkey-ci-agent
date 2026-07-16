"""Authorization and evidence gate for issue-to-draft-PR CI fixes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from scripts.ci_fix.gate import ParsedCommand, is_authorized, parse_run_url
from scripts.ci_fix.models import FixRequest
from scripts.common.github_client import retry_github_call

_ISSUE_COMMAND_RE = re.compile(
    r"^\s*@valkeyrie-(?:bot|ops)\s+fix(?=$|[^\S\n])"
    r"(?:[^\S\n]+(?P<rest>[^\n]*))?",
    re.IGNORECASE,
)
_RUN_URL_RE = re.compile(
    r"https://github\.com/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+"
    r"/actions/runs/\d+(?:(?:/attempts/\d+)|(?:/job/\d+))?"
    r"/?(?:[?#][^\s)]*)?"
)
_TEST_FAILURE_MARKER = "<!-- valkey-ci-agent:test-failure:"
_TEST_FAILURE_LABEL = "test-failure"
_MAX_EVIDENCE_COMMENTS = 20
_FAILED_RUN_CONCLUSIONS = {"failure", "timed_out"}
DEFAULT_DETECTOR_LOGIN = "valkeyrie-ops[bot]"


@dataclass(frozen=True)
class IssueFixInvocation:
    run_url: str = ""
    hint: str = ""


@dataclass(frozen=True)
class IssueFixRequest:
    fix_request: FixRequest
    issue_number: int
    detector_issue: bool
    issue_title: str
    issue_url: str
    issue_evidence: str
    base_branch: str
    base_sha: str
    branch_name: str


@dataclass(frozen=True)
class IssueGateRejection:
    reason: str


def parse_issue_fix_invocation(body: str) -> IssueFixInvocation | None:
    """Parse ``@valkeyrie-ops fix [run-url] [hint]`` on a source issue."""
    match = _ISSUE_COMMAND_RE.search(body or "")
    if not match:
        return None
    rest = (match.group("rest") or "").strip()
    if not rest:
        return IssueFixInvocation()

    parts = rest.split(maxsplit=1)
    first = parts[0]
    remainder = parts[1] if len(parts) == 2 else ""
    command = parse_run_url(first)
    if command is not None:
        return IssueFixInvocation(run_url=first, hint=remainder.strip())
    if first.startswith(("http://", "https://")):
        return None
    return IssueFixInvocation(hint=rest)


def issue_fix_branch(issue_number: int, run_id: int) -> str:
    return f"agent/ci-fix/issue-{issue_number}-run-{run_id}"


def is_detector_issue(
    issue: Any,
    *,
    detector_login: str = DEFAULT_DETECTOR_LOGIN,
) -> bool:
    """Return whether an issue has trusted detector provenance and metadata."""
    return not _validate_detector_issue(issue, detector_login=detector_login)


def is_issue_fix_target(
    issue: Any,
    invocation: IssueFixInvocation,
    *,
    detector_login: str = DEFAULT_DETECTOR_LOGIN,
) -> bool:
    """Return whether an issue can anchor this explicit or detector fix request."""
    return not _validate_issue_fix_target(
        issue,
        invocation,
        detector_login=detector_login,
    )


def build_issue_fix_request(
    gh: Any,
    *,
    repo_full_name: str,
    issue_number: int,
    commenter: str,
    invocation: IssueFixInvocation,
    org: str = "valkey-io",
    auth_team: str = "contributors",
    detector_login: str = DEFAULT_DETECTOR_LOGIN,
    retries: int = 2,
) -> IssueFixRequest | IssueGateRejection:
    """Validate an issue, linked run, default-branch ancestry, and requester."""
    if not is_authorized(gh, org, auth_team, commenter, retries=retries):
        return IssueGateRejection(
            f"@{commenter} is not an active member of {org}/{auth_team}; refusing."
        )

    try:
        repo = retry_github_call(
            lambda: gh.get_repo(repo_full_name),
            retries=retries,
            description=f"get repository {repo_full_name}",
        )
        issue = retry_github_call(
            lambda: repo.get_issue(issue_number),
            retries=retries,
            description=f"get issue #{issue_number}",
        )
        comments = list(
            retry_github_call(
                lambda: issue.get_comments(),
                retries=retries,
                description=f"list comments on issue #{issue_number}",
            )
        )
    except Exception as exc:  # noqa: BLE001 - gate fails closed
        return IssueGateRejection(f"Could not load source issue: {exc}")

    issue_rejection = _validate_issue_fix_target(
        issue,
        invocation,
        detector_login=detector_login,
    )
    if issue_rejection:
        return IssueGateRejection(issue_rejection)

    command = _command_for_issue(
        invocation,
        issue,
        comments,
        detector_login=detector_login,
    )
    if command is None:
        return IssueGateRejection(
            "No valid GitHub Actions run URL was supplied or found on the source issue."
        )
    run_repo_full_name = f"{command.run_owner}/{command.run_repo}"
    if run_repo_full_name != repo_full_name:
        return IssueGateRejection(
            f"The linked run belongs to {run_repo_full_name}, not {repo_full_name}; refusing."
        )

    try:
        run = retry_github_call(
            lambda: repo.get_workflow_run(command.run_id),
            retries=retries,
            description=f"get run {command.run_id}",
        )
        default_branch = str(getattr(repo, "default_branch", "") or "")
        base = retry_github_call(
            lambda: repo.get_branch(default_branch),
            retries=retries,
            description=f"get default branch {default_branch}",
        )
    except Exception as exc:  # noqa: BLE001 - gate fails closed
        return IssueGateRejection(f"Could not load linked run or default branch: {exc}")

    base_sha = str(getattr(getattr(base, "commit", None), "sha", "") or "")
    run_rejection = _validate_default_branch_run(
        repo,
        run,
        default_branch=default_branch,
        base_sha=base_sha,
        repo_full_name=repo_full_name,
        retries=retries,
    )
    if run_rejection:
        return IssueGateRejection(run_rejection)

    branch_name = issue_fix_branch(issue_number, command.run_id)
    existing = _existing_pull_request(
        repo,
        repo_full_name=repo_full_name,
        base_branch=default_branch,
        branch_name=branch_name,
        retries=retries,
    )
    if isinstance(existing, str):
        return IssueGateRejection(existing)

    run_sha = str(getattr(run, "head_sha", "") or "")
    fix_request = FixRequest(
        repo_full_name=repo_full_name,
        pr_number=issue_number,
        head_repo_full_name=repo_full_name,
        head_branch=default_branch,
        head_sha=run_sha,
        run_id=command.run_id,
        requested_by=commenter,
        hint=invocation.hint,
    )
    return IssueFixRequest(
        fix_request=fix_request,
        issue_number=issue_number,
        detector_issue=is_detector_issue(
            issue,
            detector_login=detector_login,
        ),
        issue_title=str(getattr(issue, "title", "") or ""),
        issue_url=str(getattr(issue, "html_url", "") or ""),
        issue_evidence=_issue_evidence(issue, comments),
        base_branch=default_branch,
        base_sha=base_sha,
        branch_name=branch_name,
    )


def _validate_detector_issue(issue: Any, *, detector_login: str) -> str:
    issue_rejection = _validate_open_issue(issue)
    if issue_rejection:
        return issue_rejection
    if not _is_expected_bot(getattr(issue, "user", None), detector_login):
        return (
            "The issue was not created by the configured detector App "
            f"{detector_login!r}."
        )
    labels = {
        str(getattr(label, "name", "") or "")
        for label in (getattr(issue, "labels", None) or ())
    }
    if _TEST_FAILURE_LABEL not in labels:
        return f"The issue does not have the {_TEST_FAILURE_LABEL!r} label."
    body = str(getattr(issue, "body", "") or "")
    if _TEST_FAILURE_MARKER not in body:
        return "The issue does not carry a Valkey CI agent test-failure marker."
    return ""


def _validate_issue_fix_target(
    issue: Any,
    invocation: IssueFixInvocation,
    *,
    detector_login: str,
) -> str:
    issue_rejection = _validate_open_issue(issue)
    if issue_rejection:
        return issue_rejection
    if invocation.run_url:
        return ""
    return _validate_detector_issue(issue, detector_login=detector_login)


def _validate_open_issue(issue: Any) -> str:
    if getattr(issue, "pull_request", None) is not None:
        return "The requested number is a pull request, not an issue."
    if str(getattr(issue, "state", "") or "") != "open":
        return "The source issue is not open."
    return ""


def _command_for_issue(
    invocation: IssueFixInvocation,
    issue: Any,
    comments: list[Any],
    *,
    detector_login: str,
) -> ParsedCommand | None:
    if invocation.run_url:
        return parse_run_url(invocation.run_url, hint=invocation.hint)

    latest_url = ""
    sources = [str(getattr(issue, "body", "") or "")]
    sources.extend(
        str(getattr(comment, "body", "") or "")
        for comment in comments
        if _is_detector_comment(
            issue,
            comment,
            detector_login=detector_login,
        )
    )
    for source in sources:
        matches = _RUN_URL_RE.findall(source)
        if matches:
            latest_url = matches[-1]
    return parse_run_url(latest_url, hint=invocation.hint) if latest_url else None


def _is_detector_comment(
    issue: Any,
    comment: Any,
    *,
    detector_login: str,
) -> bool:
    """Return whether a recurrence comment came from the configured detector."""
    return _is_expected_bot(
        getattr(issue, "user", None),
        detector_login,
    ) and _is_expected_bot(
        getattr(comment, "user", None),
        detector_login,
    )


def _is_expected_bot(user: Any, expected_login: str) -> bool:
    login = str(getattr(user, "login", "") or "")
    return (
        bool(expected_login)
        and login == expected_login
        and str(getattr(user, "type", "") or "") == "Bot"
    )


def _validate_default_branch_run(
    repo: Any,
    run: Any,
    *,
    default_branch: str,
    base_sha: str,
    repo_full_name: str,
    retries: int,
) -> str:
    if str(getattr(run, "status", "") or "") != "completed":
        return "The linked run is not completed."
    conclusion = str(getattr(run, "conclusion", "") or "")
    if conclusion not in _FAILED_RUN_CONCLUSIONS:
        return (
            "The linked run did not end in a failure conclusion "
            f"(conclusion: {conclusion or 'unknown'})."
        )
    run_branch = str(getattr(run, "head_branch", "") or "")
    if not default_branch or run_branch != default_branch:
        return (
            f"The linked run is from {run_branch or 'an unknown branch'}, not "
            f"the current default branch {default_branch or '(unknown)'}."
        )
    run_sha = str(getattr(run, "head_sha", "") or "")
    if not run_sha or not base_sha:
        return "Could not determine the run or current default-branch commit."
    head_repository = getattr(run, "head_repository", None)
    run_repo = str(getattr(head_repository, "full_name", "") or "")
    if run_repo and run_repo != repo_full_name:
        return f"The run head repository is {run_repo}, not {repo_full_name}."

    try:
        comparison = retry_github_call(
            lambda: repo.compare(run_sha, base_sha),
            retries=retries,
            description="verify run ancestry on the default branch",
        )
    except Exception as exc:  # noqa: BLE001 - gate fails closed
        return f"Could not verify run ancestry: {exc}"
    status = str(getattr(comparison, "status", "") or "")
    if status not in {"ahead", "identical"}:
        return (
            "The failing run commit is no longer an ancestor of the current "
            f"default branch (comparison status: {status or 'unknown'})."
        )
    return ""


def _existing_pull_request(
    repo: Any,
    *,
    repo_full_name: str,
    base_branch: str,
    branch_name: str,
    retries: int,
) -> str | None:
    owner = repo_full_name.split("/", 1)[0]
    try:
        pulls = list(
            retry_github_call(
                lambda: repo.get_pulls(
                    state="all",
                    base=base_branch,
                    head=f"{owner}:{branch_name}",
                ),
                retries=retries,
                description=f"find existing fix PR for {branch_name}",
            )
        )
    except Exception as exc:  # noqa: BLE001 - gate fails closed
        return f"Could not check for an existing fix pull request: {exc}"
    if not pulls:
        return None
    url = str(getattr(pulls[0], "html_url", "") or "")
    return (
        "A fix pull request already exists for this issue and run, including "
        f"closed pull requests: {url or branch_name}"
    )


def _issue_evidence(issue: Any, comments: list[Any]) -> str:
    lines = [str(getattr(issue, "body", "") or "")]
    for comment in comments[-_MAX_EVIDENCE_COMMENTS:]:
        login = str(getattr(getattr(comment, "user", None), "login", "") or "unknown")
        body = str(getattr(comment, "body", "") or "")
        lines.extend(("", f"Comment by @{login}:", body))
    return "\n".join(lines)
