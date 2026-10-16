"""Render a CI-fix result for a pull request or detector issue."""

from __future__ import annotations

import re

from scripts.ci_fix.models import (
    CandidateAction,
    FixCandidate,
    FixResult,
    ResultKind,
    TargetKind,
)
from scripts.ci_fix.policy import target_capabilities
from scripts.common.text_utils import compact_text

_MAX_OTHER_JOBS = 10


def render_result(result: FixResult) -> str:
    """Return the maintainer-facing report for one fix attempt."""
    if result.kind is ResultKind.PUSHED:
        body = [
            f"Pushed `{result.commit_sha[:12]}` to this bot-owned PR branch.",
            "",
        ]
        body += _context(result)
        body += [
            "_Normal pull-request CI is now the verification authority; the bot "
            "does not merge the PR._",
        ]
        return "\n".join(body)

    if result.kind is ResultKind.SUGGESTED:
        body = ["Prepared a fix for the PR author; nothing was pushed.", ""]
        body += _context(result)
        body += _candidate_patch(result.candidate)
        body += [
            "_Apply the change on the contributor branch and let normal PR CI verify it._",
        ]
        return "\n".join(body)

    if result.kind is ResultKind.OPENED:
        link = f"[#{result.pr_number}]({result.pr_url})" if result.pr_url else f"#{result.pr_number}"
        body = [f"Opened draft PR {link}.", ""]
        body += _context(result)
        body += [
            "_The draft stays unmerged until normal PR CI passes and a human reviews it._",
        ]
        return "\n".join(body)

    if result.kind is ResultKind.REFUSED:
        body = [
            f"No fix prepared: {compact_text(result.summary, 2_000)}",
            "",
        ]
        body += _context(result, include_reasoning=False)
        return "\n".join(body).rstrip()

    return (
        "The CI-fix attempt stopped with an error: "
        f"{compact_text(result.summary, 2_000)}"
    )


def _context(result: FixResult, *, include_reasoning: bool = True) -> list[str]:
    failure = result.task.failure
    candidate = result.candidate
    lines = [
        f"Failure: [`{compact_text(failure.job_name, 300)}` in this run]"
        f"({failure.run_url}).",
        f"Prepared for commit `{result.task.target.expected_sha[:12]}`.",
        "",
    ]
    if failure.description:
        lines += [compact_text(failure.description, 2_000), ""]
    if candidate is not None:
        lines += _candidate_context(candidate, include_reasoning=include_reasoning)
    if failure.other_failed_jobs:
        visible = failure.other_failed_jobs[:_MAX_OTHER_JOBS]
        lines += [
            "Other jobs also failed in that run:",
            *[f"- `{compact_text(name, 300)}`" for name in visible],
            "",
        ]
        hidden = len(failure.other_failed_jobs) - len(visible)
        if hidden:
            lines[-1:-1] = [f"- …and {hidden} more.", ""]
    return lines


def _candidate_context(candidate: FixCandidate, *, include_reasoning: bool) -> list[str]:
    lines: list[str] = []
    if candidate.failing_check:
        lines += [
            f"**Failing check:** {compact_text(candidate.failing_check, 300)}",
            "",
        ]
    if candidate.root_cause:
        lines += [
            f"**Root cause:** {compact_text(candidate.root_cause, 2_000)}",
            "",
        ]
    if candidate.failure_type.value != "unknown":
        lines += [f"**Failure type:** {candidate.failure_type.value}", ""]
    if include_reasoning and candidate.reasoning:
        lines += [
            f"**Change:** {compact_text(candidate.reasoning, 3_000)}",
            "",
        ]
    if candidate.verification:
        lines += ["**Checked by the bot:**", ""]
        lines += [f"- {line}" for line in candidate.verification.splitlines() if line.strip()]
        lines += [""]
    if candidate.validation:
        lines += [
            f"**Agent validation notes:** {compact_text(candidate.validation, 2_000)}",
            "",
        ]
    return lines


def _candidate_patch(candidate: FixCandidate | None) -> list[str]:
    if candidate is None:
        return []
    if candidate.action is CandidateAction.PORT:
        return [
            "The fix is an upstream commit:",
            "",
            _fenced(f"git cherry-pick -x {candidate.port_commit}", lang="sh"),
            "",
        ]
    if not candidate.patch:
        return []
    patch_limit = target_capabilities(TargetKind.SUGGEST).patch_limit
    if len(candidate.patch) > patch_limit:
        return [
            "The proposed patch exceeded the comment-size limit and was not posted.",
            "",
        ]
    return [
        "Proposed patch:",
        "",
        _fenced(candidate.patch, lang="diff"),
        "",
    ]


def _fenced(body: str, *, lang: str = "") -> str:
    """Fence untrusted text with a delimiter longer than any inside it."""
    longest = max((len(match) for match in re.findall(r"`+", body)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}{lang}\n{body}\n{fence}"
