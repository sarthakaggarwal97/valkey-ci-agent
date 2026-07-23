"""Render a ``FixOutcome`` into a PR comment.

The comment is the agent's accountability surface: for a push it identifies
the independent verification run and records the review rationale. For a
refusal it explains why, so the maintainer can take over.
"""

from __future__ import annotations

import re

from scripts.ci_fix.models import BaselineKind, FixOutcome, FixPath, OutcomeKind


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
    if outcome.kind is OutcomeKind.REFUSED:
        return _render_refused(outcome)
    if outcome.kind is OutcomeKind.HANDOFF:
        return _render_handoff(outcome)
    return _render_failed(outcome)


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
    lines += [
        f"**Root cause:** {proposal.root_cause if proposal else ''}",
        "",
    ]
    lines += _baseline_lines(outcome)
    if outcome.verify_backend:
        where = _backend_label(outcome)
        lines += [f"**Verified by:** {where}", ""]
    if review is not None and review.reasoning:
        label = "Port provenance" if proposal and proposal.path is FixPath.PORT else "Review"
        lines += [f"**{label}:** {review.reasoning}", ""]
    lines += _remaining_checks(outcome)
    if proposal and proposal.path is FixPath.PORT:
        lines.append(
            "_The upstream fix passed targeted verification of the failing "
            "check; this PR's full CI will confirm. I do not merge._"
        )
    else:
        lines.append(
            "_The fix passed targeted verification of the failing check; this PR's "
            "full CI will confirm. I do not merge._"
        )
    return "\n".join(lines)


def _backend_label(outcome: FixOutcome) -> str:
    backend = outcome.verify_backend
    run = (
        f" ([run]({outcome.verification_run_url}))"
        if outcome.verification_run_url
        else ""
    )
    if backend == "local":
        return f"targeted verification on an isolated Linux Actions runner{run}"
    if backend.startswith("docker:"):
        return (
            f"targeted verification in the `{backend[len('docker:'):]}` "
            f"container on an isolated Actions runner{run}"
        )
    if backend == "macos":
        return f"targeted verification on a macOS runner{run}"
    if backend == "target-workflow":
        return f"the target repository's exact-environment workflow{run}"
    return backend


def _render_refused(outcome: FixOutcome) -> str:
    lines = [f"I did not push a fix: {outcome.summary}", ""]
    if outcome.failing_run_url:
        lines += [f"Looked at the failure from [this run]({outcome.failing_run_url}).", ""]
    if outcome.verification_run_url:
        lines += [
            f"Latest candidate evidence: [verification run]({outcome.verification_run_url}).",
            "",
        ]
    lines += _baseline_lines(outcome)
    lines += _remaining_checks(outcome)
    return "\n".join(lines)


def _render_failed(outcome: FixOutcome) -> str:
    return f"I hit an error and could not complete the fix: {outcome.summary}"


def _render_handoff(outcome: FixOutcome) -> str:
    proposal = outcome.proposal
    if proposal is not None and proposal.path is FixPath.PORT:
        lines = [
            "I found a fix already merged on a trusted branch, but the available "
            "evidence or repository policy requires a human to port it.",
            "",
        ]
    elif outcome.verify_backend:
        lines = [
            "I diagnosed, prepared, and remotely checked a fix, but the "
            "available evidence or repository policy does not permit an "
            "automatic push.",
            "",
        ]
    else:
        lines = [
            "I diagnosed this and prepared a fix, but I could not verify it in my "
            "environment, so I am handing it off rather than pushing it unverified.",
            "",
        ]
    if outcome.failing_run_url:
        lines += [f"From the failure in [this run]({outcome.failing_run_url}).", ""]
    if (
        outcome.verification_run_url
        and outcome.verify_backend not in {"macos", "target-workflow"}
    ):
        lines += [
            f"Latest candidate evidence: [verification run]({outcome.verification_run_url}).",
            "",
        ]
    if proposal is not None and proposal.root_cause:
        lines += [f"**Root cause:** {proposal.root_cause}", ""]
    lines += _baseline_lines(outcome)
    lines += [f"**Why not pushed:** {outcome.summary}", ""]
    if outcome.verify_backend:
        lines += [f"**Verified by:** {_backend_label(outcome)}", ""]
    if outcome.review is not None and outcome.review.reasoning:
        label = (
            "Port provenance"
            if proposal is not None and proposal.path is FixPath.PORT
            else "Review"
        )
        lines += [f"**{label}:** {outcome.review.reasoning}", ""]
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
    listed = "\n".join(f"- `{name}`" for name in outcome.other_failing_checks)
    return [
        "Other checks also failed in that run; re-invoke with the same command to "
        "address the next one:",
        listed,
        "",
    ]


def _baseline_lines(outcome: FixOutcome) -> list[str]:
    evidence = outcome.baseline
    if evidence is None:
        return []
    if evidence.kind is BaselineKind.FLAKY:
        summary = (
            f"confirmed flaky: {evidence.failed} failure(s), "
            f"{evidence.passed} pass(es) across {evidence.attempts} clean run(s)"
        )
    elif evidence.kind is BaselineKind.DETERMINISTIC:
        summary = (
            f"reproduced deterministically: {evidence.failed}/"
            f"{evidence.attempts} clean run(s) failed"
        )
    elif evidence.kind is BaselineKind.NOT_REPRODUCED:
        summary = (
            f"not reproduced: {evidence.passed}/"
            f"{evidence.attempts} clean run(s) passed"
        )
    else:
        summary = "baseline unavailable"
    return [f"**Clean baseline:** {summary}.", ""]
