"""Render a ``FixOutcome`` into a PR comment.

The comment is the agent's accountability surface: for a push it shows exactly
what was changed, the command that was run, its captured output, and the
review rationale — the evidence a maintainer needs to trust (or reject) the
fix. For a refusal it explains why, so the maintainer can take over.
"""

from __future__ import annotations

import re

from scripts.ci_fix.models import FixOutcome, OutcomeKind

_OUTPUT_TAIL_IN_COMMENT = 3000


def _fenced(body: str) -> str:
    """Wrap untrusted text in a code fence it cannot break out of.

    Command output may itself contain ``` runs; per CommonMark, the fence must
    be longer than the longest backtick run inside, so we size it accordingly.
    """
    longest = max((len(m) for m in re.findall(r"`+", body)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}\n{body}\n{fence}"


def render_comment(outcome: FixOutcome) -> str:
    if outcome.kind is OutcomeKind.PUSHED:
        return _render_pushed(outcome)
    if outcome.kind is OutcomeKind.REFUSED:
        return _render_refused(outcome)
    return _render_failed(outcome)


def _render_pushed(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    run = outcome.run_result
    review = outcome.review
    lines = [
        f"Fixed **{proposal.failing_check if proposal else 'the failing check'}** "
        f"and pushed `{outcome.commit_sha[:12]}` to this PR's branch.",
        "",
        f"**Root cause:** {proposal.root_cause if proposal else ''}",
        "",
    ]
    if run is not None:
        block = f"$ {run.command}\nexit {run.exit_code}\n{run.output_tail[-_OUTPUT_TAIL_IN_COMMENT:]}"
        lines += [
            "<details><summary>Targeted verification (command + output)</summary>",
            "",
            _fenced(block),
            "</details>",
            "",
        ]
    if outcome.verify_backend:
        where = _backend_label(outcome)
        lines += [f"**Verified by:** {where}", ""]
    if review is not None and review.reasoning:
        lines += [f"**Review:** {review.reasoning}", ""]
    lines += _remaining_checks(outcome)
    lines.append(
        "_The fix passed targeted verification of the failing check; this PR's "
        "full CI will confirm. I do not merge._"
    )
    return "\n".join(lines)


def _backend_label(outcome: FixOutcome) -> str:
    backend = outcome.verify_backend
    if backend == "local":
        return "targeted verification on a Linux runner"
    if backend.startswith("docker:"):
        return f"targeted verification in the `{backend[len('docker:'):]}` container"
    if backend == "macos":
        run = f" ([run]({outcome.macos_run_url}))" if outcome.macos_run_url else ""
        return f"targeted verification on a macOS runner{run}"
    return backend


def _render_refused(outcome: FixOutcome) -> str:
    lines = [f"I did not push a fix: {outcome.summary}", ""]
    if outcome.run_result is not None and outcome.run_result.output_tail:
        lines += [
            "<details><summary>Evidence</summary>",
            "",
            _fenced(outcome.run_result.output_tail[-_OUTPUT_TAIL_IN_COMMENT:]),
            "</details>",
            "",
        ]
    lines += _remaining_checks(outcome)
    return "\n".join(lines)


def _render_failed(outcome: FixOutcome) -> str:
    return f"I hit an error and could not complete the fix: {outcome.summary}"


def _remaining_checks(outcome: FixOutcome) -> list[str]:
    if not outcome.other_failing_checks:
        return []
    listed = "\n".join(f"- `{name}`" for name in outcome.other_failing_checks)
    return [
        "Other checks also failed in that run; re-invoke with the same command to "
        "address the next one:",
        listed,
        "",
    ]
