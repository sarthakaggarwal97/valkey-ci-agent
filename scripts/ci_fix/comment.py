"""Render a ``FixOutcome`` into a PR comment.

The comment is the agent's accountability surface: for a push it shows exactly
what was changed, the command that was run, its captured output, and the
review rationale - the evidence a maintainer needs to trust (or reject) the
fix. Every outcome also carries the triage: the root cause, how the failure
behaves, and the commit that introduced it when code could confirm one. For a
refusal it explains why, so a maintainer can take over.
"""

from __future__ import annotations

import re

from scripts.ci_fix.models import FailureType, FixOutcome, OutcomeKind

_OUTPUT_TAIL_IN_COMMENT = 3000


def _fenced(body: str, *, lang: str = "") -> str:
    """Wrap untrusted text in a code fence it cannot break out of.

    Command output may itself contain ``` runs; per CommonMark, the fence must
    be longer than the longest backtick run inside, so we size it accordingly.
    """
    longest = max((len(m) for m in re.findall(r"`+", body)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}{lang}\n{body}\n{fence}"


def render_comment(outcome: FixOutcome) -> str:
    if outcome.kind is OutcomeKind.PUSHED:
        return _render_pushed(outcome)
    if outcome.kind is OutcomeKind.SUGGESTED:
        return _render_suggested(outcome)
    if outcome.kind is OutcomeKind.REFUSED:
        return _render_refused(outcome)
    if outcome.kind is OutcomeKind.HANDOFF:
        return _render_handoff(outcome)
    return _render_failed(outcome)


def triage_lines(outcome: FixOutcome) -> list[str]:
    """Root cause, failure type, and confirmed culprit, as Markdown paragraphs."""
    proposal = outcome.proposal
    lines: list[str] = []
    if proposal is not None and proposal.root_cause:
        lines += [f"**Root cause:** {proposal.root_cause}", ""]
    if proposal is not None and proposal.failure_type is not FailureType.UNKNOWN:
        lines += [f"**Failure type:** {proposal.failure_type.value}", ""]
    if outcome.culprit_sha:
        # A bare full SHA autolinks to the commit; a "(#N)" subject links its PR.
        lines += [f"**Introduced by:** {outcome.culprit_sha} {outcome.culprit_subject}".rstrip(), ""]
    return lines


def _render_pushed(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    review = outcome.review
    lines = [
        f"Fixed **{proposal.failing_check if proposal else 'the failing check'}** "
        f"and pushed `{outcome.commit_sha[:12]}` to this PR's branch.",
        "",
    ]
    if outcome.failing_run_url:
        lines += [f"Fixing the failure from [this run]({outcome.failing_run_url}).", ""]
    lines += triage_lines(outcome)
    lines += _evidence_lines(outcome)
    if review is not None and review.reasoning:
        lines += [f"**Review:** {review.reasoning}", ""]
    lines += _remaining_checks(outcome)
    if outcome.verify_backend == "upstream-port":
        lines.append(
            "_Ported from upstream; this PR's CI verifies it._"
        )
    else:
        lines.append(
            "_Targeted verification passed; this PR's full CI still runs._"
        )
    return "\n".join(lines)


def _render_suggested(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    check = proposal.failing_check if proposal else "the failing check"
    lines = [f"Proposed fix for **{check}**. {outcome.summary}", ""]
    if outcome.failing_run_url:
        lines += [f"From the failure in [this run]({outcome.failing_run_url}).", ""]
    lines += triage_lines(outcome)
    lines += _evidence_lines(outcome)
    # A port's "review" is the code's own note that CI verifies it, which is
    # not true of a suggestion nobody pushed.
    if outcome.review is not None and outcome.review.reasoning and not outcome.port_commit:
        lines += [f"**Review:** {outcome.review.reasoning}", ""]
    lines += _patch_lines(outcome)
    lines += _remaining_checks(outcome)
    lines.append("_Not pushed: the bot does not push to contributor branches._")
    return "\n".join(lines)


def _evidence_lines(outcome: FixOutcome) -> list[str]:
    proposal = outcome.proposal
    run = outcome.run_result
    lines: list[str] = []
    if run is not None:
        check_name = proposal.failing_check if proposal else ""
        highlight = _result_lines_for(run.output_tail, check_name)
        if highlight:
            lines += [
                "The failing check with the fix:",
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
    # A suggested port was not pushed, so no CI will verify it; say nothing.
    if outcome.verify_backend and not (outcome.kind is OutcomeKind.SUGGESTED and outcome.port_commit):
        lines += [f"**Verified by:** {_backend_label(outcome)}", ""]
    return lines


def _patch_lines(outcome: FixOutcome) -> list[str]:
    if outcome.port_commit:
        return [
            f"The fix is upstream commit `{outcome.port_commit[:12]}`. If this PR's base "
            "branch already has it, update the branch; otherwise apply it with:",
            "",
            _fenced(f"git cherry-pick -x {outcome.port_commit}", lang="sh"),
            "",
        ]
    patch = outcome.patch or outcome.handoff_patch
    if not patch:
        return []
    return ["Proposed patch (`git apply` it):", "", _fenced(patch, lang="diff"), ""]


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
        return f"targeted verification in the `{backend[len('docker:'):]}` container"
    if backend == "macos":
        run = f" ([run]({outcome.macos_run_url}))" if outcome.macos_run_url else ""
        return f"targeted verification on a macOS runner{run}"
    if backend == "upstream-port":
        return "ported upstream fix; awaiting this PR's normal CI"
    return backend


def _render_refused(outcome: FixOutcome) -> str:
    lines = [f"No fix prepared: {outcome.summary}", ""]
    if outcome.failing_run_url:
        lines += [f"Failure: [this run]({outcome.failing_run_url}).", ""]
    lines += triage_lines(outcome)
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
    return f"The fix attempt stopped with an error: {outcome.summary}"


def _render_handoff(outcome: FixOutcome) -> str:
    lines = [
        f"Unverified fix, not pushed: {outcome.summary}.",
        "",
    ]
    if outcome.failing_run_url:
        lines += [f"From the failure in [this run]({outcome.failing_run_url}).", ""]
    lines += triage_lines(outcome)
    if outcome.review is not None and outcome.review.reasoning:
        lines += [f"**Review:** {outcome.review.reasoning}", ""]
    lines += _patch_lines(outcome)
    lines += _remaining_checks(outcome)
    lines.append("_Review it and apply it by hand; this PR's CI is its only verification._")
    return "\n".join(lines)


def _remaining_checks(outcome: FixOutcome) -> list[str]:
    if not outcome.other_failing_checks:
        return []
    listed = "\n".join(f"- `{name}`" for name in outcome.other_failing_checks)
    return [
        "Other checks also failed in that run; to address one, comment the fix "
        "command with that job's link:",
        listed,
        "",
    ]
