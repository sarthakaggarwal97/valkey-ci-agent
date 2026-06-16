"""Render a ``FixOutcome`` into a PR comment.

The comment is the agent's accountability surface: for a push it shows exactly
what was changed, the command that was run, its captured output, and the
review rationale — the evidence a maintainer needs to trust (or reject) the
fix. For a refusal it explains why, so the maintainer can take over.
"""

from __future__ import annotations

from scripts.ci_fix.models import FixOutcome, OutcomeKind

_OUTPUT_TAIL_IN_COMMENT = 3000


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
        f"Fixed **{proposal.failing_test if proposal else 'the failing test'}** "
        f"and pushed `{outcome.commit_sha[:12]}` to this PR's branch.",
        "",
        f"**Root cause:** {proposal.root_cause if proposal else ''}",
        "",
    ]
    if run is not None:
        lines += [
            "<details><summary>Verification (command + output)</summary>",
            "",
            f"```\n$ {run.command}\nexit {run.exit_code}\n",
            run.output_tail[-_OUTPUT_TAIL_IN_COMMENT:],
            "```",
            "</details>",
            "",
        ]
    if review is not None and review.reasoning:
        lines += [f"**Review:** {review.reasoning}", ""]
    lines += _remaining_tests(outcome)
    lines.append(
        "_The fix was verified by running the test; this PR's CI will confirm. "
        "I do not merge._"
    )
    return "\n".join(lines)


def _render_refused(outcome: FixOutcome) -> str:
    lines = [f"I did not push a fix: {outcome.summary}", ""]
    if outcome.run_result is not None and outcome.run_result.output_tail:
        lines += [
            "<details><summary>Evidence</summary>",
            "",
            f"```\n{outcome.run_result.output_tail[-_OUTPUT_TAIL_IN_COMMENT:]}\n```",
            "</details>",
            "",
        ]
    lines += _remaining_tests(outcome)
    return "\n".join(lines)


def _render_failed(outcome: FixOutcome) -> str:
    return f"I hit an error and could not complete the fix: {outcome.summary}"


def _remaining_tests(outcome: FixOutcome) -> list[str]:
    if not outcome.other_failing_tests:
        return []
    listed = "\n".join(f"- `{name}`" for name in outcome.other_failing_tests)
    return [
        "Other tests also failed in that run; re-invoke with the same command to "
        "address the next one:",
        listed,
        "",
    ]
