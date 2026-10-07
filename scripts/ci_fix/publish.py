"""Publish an engine decision: the only step that writes to GitHub.

The engine runs with read-only credentials and writes its decision to a state
file; this step runs afterwards with a freshly minted write token (an App
token lives one hour, and a diagnosis plus verification can outlast it). It
re-validates the target before acting:

- ``PUSH``: commit the approved patch (or cherry-pick the approved port) onto
  the bot-owned ``agent/...`` PR branch with an exact-head lease.
- ``SUGGEST``: never push; the patch is rendered into a PR comment for the
  contributor.

The Daily issue flow (``NEW_PR``) has its own publication in ``issues.py``,
which reuses the same push functions.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from scripts.ci_fix.models import FixOutcome, FixRequest, OutcomeKind, Publication
from scripts.ci_fix.push import PrePushCheck, PushRefused, commit_and_push_fix, commit_and_push_port
from scripts.common.atomic_json import write_json_atomic
from scripts.common.github_client import retry_github_call

STATE_VERSION = 1
# Backends whose verdict a push may rely on. A READY outcome without one is a
# reviewed but unexecuted fix, which is only ever suggested, never pushed.
_PUSHABLE_BACKENDS = ("local", "docker", "macos", "upstream-port")

CommitFix = Callable[..., str]
CommitPort = Callable[..., str]


def write_state(path: str, payload: dict[str, Any]) -> None:
    """Atomically persist a token-free handoff between the engine and publication."""
    write_json_atomic(path, {"version": STATE_VERSION, **payload})


def read_state(path: str) -> dict[str, Any] | None:
    """Return the handoff payload, or None when the engine wrote nothing."""
    state_path = Path(path)
    if not state_path.is_file():
        return None
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != STATE_VERSION:
        raise ValueError(f"invalid CI-fix state in {path}")
    return payload


def publish_to_pr(
    outcome: FixOutcome,
    request: FixRequest,
    *,
    git_env: dict[str, str],
    pre_push_check: PrePushCheck | None = None,
    commit_fix: CommitFix = commit_and_push_fix,
    commit_port: CommitPort = commit_and_push_port,
) -> FixOutcome:
    """Turn a READY outcome into what actually happened on the PR.

    Any other outcome is already final and is returned unchanged.
    """
    if outcome.kind is not OutcomeKind.READY:
        return outcome
    if request.publication is Publication.SUGGEST:
        return _suggestion(outcome)
    if request.publication is not Publication.PUSH:
        return replace(outcome, kind=OutcomeKind.FAILED, summary="unsupported publication for a PR")
    if not outcome.verify_backend.startswith(_PUSHABLE_BACKENDS):
        return replace(
            outcome, kind=OutcomeKind.HANDOFF, handoff_patch=outcome.patch,
            summary="the fix was reviewed but never verified, so it is not pushed",
        )
    try:
        if outcome.port_commit:
            sha = commit_port(
                head_repo_full_name=request.head_repo_full_name,
                head_branch=request.head_branch,
                head_sha=request.head_sha,
                unstable_fix_commit=outcome.port_commit,
                git_env=git_env,
                pre_push_check=pre_push_check,
            )
        else:
            if outcome.proposal is None:
                raise PushRefused("Refusing to push: the decision carries no diagnosis.")
            sha = commit_fix(
                patch=outcome.patch,
                changed_paths=outcome.changed_paths,
                head_repo_full_name=request.head_repo_full_name,
                head_branch=request.head_branch,
                head_sha=request.head_sha,
                proposal=outcome.proposal,
                git_env=git_env,
                pre_push_check=pre_push_check,
            )
    except PushRefused as exc:
        return replace(outcome, kind=OutcomeKind.REFUSED, summary=str(exc))
    check = outcome.proposal.failing_check if outcome.proposal else "the failing check"
    verb = "Ported upstream fix" if outcome.port_commit else "Pushed fix"
    return replace(outcome, kind=OutcomeKind.PUSHED, commit_sha=sha, summary=f"{verb} for {check}")


def _suggestion(outcome: FixOutcome) -> FixOutcome:
    if outcome.port_commit:
        return replace(outcome, kind=OutcomeKind.SUGGESTED, summary="An upstream commit already fixes this.")
    if outcome.verify_backend:
        return replace(
            outcome, kind=OutcomeKind.SUGGESTED,
            summary="Targeted verification passed.",
        )
    return replace(
        outcome, kind=OutcomeKind.HANDOFF, handoff_patch=outcome.patch,
        summary=(
            "the PR branch is in a fork, so its code was not run here; the fix "
            "was reviewed but not executed"
        ),
    )


def pr_head_check(gh: Any, request: FixRequest) -> PrePushCheck:
    """A pre-push check that the PR is open and its head has not moved."""

    def _check() -> str:
        pr = retry_github_call(
            lambda: gh.get_repo(request.repo_full_name).get_pull(request.pr_number),
            retries=2, description=f"revalidate PR #{request.pr_number} before push",
        )
        if str(getattr(pr, "state", "") or "") != "open":
            return f"PR #{request.pr_number} is no longer open"
        head = getattr(pr, "head", None)
        if str(getattr(head, "ref", "") or "") != request.head_branch:
            return f"PR #{request.pr_number} no longer has head branch {request.head_branch}"
        current = str(getattr(head, "sha", "") or "")
        if current != request.head_sha:
            return (
                f"the PR head moved from {request.head_sha[:12]} to "
                f"{current[:12] or '(missing)'}"
            )
        return ""

    return _check
