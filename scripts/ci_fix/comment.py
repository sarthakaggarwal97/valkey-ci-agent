"""Render a ``FixOutcome`` into a PR comment.

The comment is the agent's accountability surface: for a push it shows exactly
what was changed, the command that was run, its captured output, and the
review rationale - the evidence a maintainer needs to trust (or reject) the
fix. For a refusal it explains why, so the maintainer can take over.
"""

from __future__ import annotations

import re

from scripts.ci_fix.models import FixOutcome, OutcomeKind
from scripts.common.markdown import (
    bounded_comment,
    escape_text,
    fenced_code,
    inline_code,
    markdown_link,
)

_OUTPUT_TAIL_IN_COMMENT = 3000


def _fenced(body: str, *, lang: str = "") -> str:
    return fenced_code(body, language=lang)


def render_comment(outcome: FixOutcome) -> str:
    if outcome.kind is OutcomeKind.PUSHED:
        body = _render_pushed(outcome)
    elif outcome.kind is OutcomeKind.REFUSED:
        body = _render_refused(outcome)
    elif outcome.kind is OutcomeKind.HANDOFF:
        body = _render_handoff(outcome)
    else:
        body = _render_failed(outcome)
    return bounded_comment(body)


def _render_pushed(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    run = outcome.run_result
    review = outcome.review
    check = proposal.failing_check if proposal else "the failing check"
    lines = [
        f"Fixed **{escape_text(check, max_bytes=1024, multiline=False)}** "
        f"and pushed {inline_code(outcome.commit_sha[:12])} to this PR's branch.",
        "",
    ]
    if outcome.failing_run_url:
        lines += [
            f"Fixing the failure from "
            f"{markdown_link('this run', outcome.failing_run_url)}.",
            "",
        ]
    lines += [
        f"**Root cause:** "
        f"{escape_text(proposal.root_cause if proposal else '', max_bytes=4096)}",
        "",
    ]
    if run is not None:
        check_name = proposal.failing_check if proposal else ""
        highlight = _result_lines_for(run.output_tail, check_name)
        if highlight:
            lines += [
                "The previously-failing check now passes:",
                "",
                _fenced(highlight),
                "",
            ]
        block = f"$ {run.command}\nexit {run.exit_code}\n{run.output_tail[-_OUTPUT_TAIL_IN_COMMENT:]}"
        lines += [
            "<details><summary>Full verification output</summary>",
            "",
            _fenced(block),
            "</details>",
            "",
        ]
    if outcome.verify_backend:
        where = _backend_label(outcome)
        lines += [f"**Verified by:** {where}", ""]
    if review is not None and review.reasoning:
        lines += [
            f"**Review:** {escape_text(review.reasoning, max_bytes=4096)}",
            "",
        ]
    lines += _remaining_checks(outcome)
    if outcome.verify_backend == "upstream-port":
        lines.append(
            "_This is a port of an upstream fix; this PR's normal CI is the "
            "verification authority. I do not merge._"
        )
    else:
        lines.append(
            "_This was a targeted approximation, not a replay of the complete "
            "Actions job. It reproduced the failing baseline and patched command; "
            "this PR's full CI is authoritative. I do not merge._"
        )
    return "\n".join(lines)


def _result_lines_for(output: str, check_name: str) -> str:
    """Pull the lines that show the target check's result out of the output.

    A verification run can emit hundreds of lines for other passing tests; a
    maintainer wants the one line proving the previously-failing check now
    passes. Prefer lines mentioning the check name; otherwise fall back to the
    last few result-marker lines. Returns an empty string if nothing matches,
    in which case the caller just shows the full output.
    """
    lines = output.splitlines()
    if check_name:
        # Match on a distinctive slice of the check name (the AI's name and the
        # log's wording can differ slightly), longest word first.
        words = sorted((w for w in re.split(r"\W+", check_name) if len(w) > 3), key=len, reverse=True)
        for w in words:
            hits = [ln for ln in lines if w in ln and _RESULT_MARKER.search(ln)]
            if hits:
                return "\n".join(hits[-5:])
    markers = [ln for ln in lines if _RESULT_MARKER.search(ln)]
    return "\n".join(markers[-3:]) if markers else ""


_RESULT_MARKER = re.compile(r"\[ok\]|\[err\]|\[exception\]|\bPASS\b|\bFAIL\b", re.IGNORECASE)


def _backend_label(outcome: FixOutcome) -> str:
    backend = outcome.verify_backend
    if backend == "local":
        return "targeted verification on a Linux runner"
    if backend.startswith("docker:"):
        return (
            "targeted verification in the "
            f"{inline_code(backend[len('docker:'):])} container"
        )
    if backend == "macos":
        run = (
            f" ({markdown_link('run', outcome.macos_run_url)})"
            if outcome.macos_run_url
            else ""
        )
        return f"targeted verification on a macOS runner{run}"
    if backend == "upstream-port":
        return "ported upstream fix; awaiting this PR's normal CI"
    return escape_text(backend, max_bytes=1024, multiline=False)


def _render_refused(outcome: FixOutcome) -> str:
    lines = [
        f"I did not push a fix: {escape_text(outcome.summary, max_bytes=4096)}",
        "",
    ]
    if outcome.failing_run_url:
        lines += [
            f"Looked at the failure from "
            f"{markdown_link('this run', outcome.failing_run_url)}.",
            "",
        ]
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
    return (
        "I hit an error and could not complete the fix: "
        f"{escape_text(outcome.summary, max_bytes=4096)}"
    )


def _render_handoff(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    lines = [
        "I diagnosed this and prepared a fix, but I could not verify it in my "
        "environment, so I am handing it off rather than pushing it unverified.",
        "",
    ]
    if outcome.failing_run_url:
        lines += [
            f"From the failure in "
            f"{markdown_link('this run', outcome.failing_run_url)}.",
            "",
        ]
    if proposal is not None and proposal.root_cause:
        lines += [
            f"**Root cause:** {escape_text(proposal.root_cause, max_bytes=4096)}",
            "",
        ]
    lines += [
        f"**Why not verified:** {escape_text(outcome.summary, max_bytes=4096)}",
        "",
    ]
    if outcome.review is not None and outcome.review.reasoning:
        lines += [
            f"**Review:** "
            f"{escape_text(outcome.review.reasoning, max_bytes=4096)}",
            "",
        ]
    if outcome.handoff_patch:
        lines += [
            "Proposed patch (apply and let this PR's CI judge it):",
            "",
            _fenced(outcome.handoff_patch, lang="diff"),
            "",
        ]
    lines += _remaining_checks(outcome)
    lines.append("_I did not push this; a human should apply it. I do not merge._")
    return "\n".join(lines)


def _remaining_checks(outcome: FixOutcome) -> list[str]:
    if not outcome.other_failing_checks:
        return []
    listed = "\n".join(
        f"- {inline_code(name)}"
        for name in outcome.other_failing_checks
    )
    return [
        "Other checks also failed in that run; re-invoke with the same command to "
        "address the next one:",
        listed,
        "",
    ]
