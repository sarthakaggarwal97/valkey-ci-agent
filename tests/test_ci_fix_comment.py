"""Tests for rendering a ``FixOutcome`` into a PR comment."""

from __future__ import annotations

from scripts.ci_fix.comment import render_comment, triage_lines
from scripts.ci_fix.models import (
    FailureType,
    FixOutcome,
    FixPath,
    FixProposal,
    OutcomeKind,
    ReviewVerdict,
    RunResult,
)

_RUN = "https://github.com/o/r/actions/runs/9"


def _proposal(path: FixPath = FixPath.AUTHOR, **overrides) -> FixProposal:
    values = dict(
        path=path, failing_check="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11", reasoning="scaffolding fix",
        confidence=0.9,
    )
    values.update(overrides)
    return FixProposal(**values)


def _run(output: str = "All tests passed") -> RunResult:
    return RunResult(ran=True, passed=True, exit_code=0,
                     command="make && ./runtest --single x", output_tail=output)


def test_pushed_comment_includes_evidence_and_review():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed", proposal=_proposal(), run_result=_run(),
        review=ReviewVerdict(True, "minimal and correct"), commit_sha="abcdef1234567890",
        verify_backend="local", failing_run_url=_RUN,
    ))
    assert "pushed `abcdef123456`" in body
    assert "make && ./runtest" in body
    assert "minimal and correct" in body
    assert "targeted verification on a Linux runner" in body
    assert "actions/runs/9" in body
    assert "do not merge" in body.lower()


def test_pushed_comment_leads_with_the_target_checks_result():
    noisy = "\n".join(["[ok]: other test (1 ms)"] * 30
                      + ["[ok]: corrupt payload: zset listpack with NAN score (12 ms)"])
    body = render_comment(FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed", proposal=_proposal(), run_result=_run(noisy),
        commit_sha="abcdef1234567890",
    ))
    assert "previously-failing check now passes" in body
    assert body.index("NAN score (12 ms)") < body.index("Full verification output")


def test_port_comment_names_pr_ci_as_the_authority():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed", proposal=_proposal(FixPath.PORT),
        commit_sha="abcdef1234567890", verify_backend="upstream-port",
    ))
    assert "ported upstream fix" in body
    assert "normal CI is the verification authority" in body


def test_untrusted_output_cannot_break_out_of_its_fence():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.PUSHED, summary="pushed", proposal=_proposal(),
        run_result=_run("ok\n```\n## injected heading\n"), commit_sha="abcdef1234567890",
    ))
    assert "````" in body


def test_triage_lines_show_cause_type_and_confirmed_culprit():
    outcome = FixOutcome(
        kind=OutcomeKind.REFUSED, summary="product bug",
        proposal=_proposal(FixPath.REFUSE, failure_type=FailureType.DETERMINISTIC),
        culprit_sha="c" * 40, culprit_subject="Backport tls change (#4100)",
    )
    lines = "\n".join(triage_lines(outcome))
    assert "**Root cause:** payload embeds RDB v80" in lines
    assert "**Failure type:** deterministic" in lines
    # A bare full SHA and a (#N) subject both autolink on GitHub.
    assert f"**Introduced by:** {'c' * 40} Backport tls change (#4100)" in lines


def test_triage_lines_omit_unknown_type_and_missing_culprit():
    lines = "\n".join(triage_lines(FixOutcome(kind=OutcomeKind.REFUSED, summary="x", proposal=_proposal())))
    assert "Failure type" not in lines
    assert "Introduced by" not in lines


def test_refused_comment_explains_and_lists_other_failures():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.REFUSED, summary="genuinely flaky timing failure; no safe fix",
        other_failing_checks=("other test",), failing_run_url=_RUN, proposal=_proposal(FixPath.REFUSE),
    ))
    assert body.startswith("I did not prepare a fix: genuinely flaky")
    assert "**Root cause:**" in body
    assert "other test" in body


def test_suggested_comment_carries_the_patch_and_never_claims_a_push():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.SUGGESTED, summary="I do not push to contributor branches, so here is the verified fix.",
        proposal=_proposal(), run_result=_run(), verify_backend="local",
        patch="--- a/f\n+++ b/f\n+fix\n", review=ReviewVerdict(True, "sound"),
    ))
    assert "Here is a fix for **corrupt payload" in body
    assert "```diff\n--- a/f\n+++ b/f\n+fix\n" in body
    assert "I did not push this" in body
    assert "pushed `" not in body


def test_suggested_port_gives_the_cherry_pick_command():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.SUGGESTED, summary="s", proposal=_proposal(FixPath.PORT),
        port_commit="9" * 40, verify_backend="upstream-port",
    ))
    assert f"git cherry-pick -x {'9' * 40}" in body


def test_handoff_comment_includes_patch_and_reason():
    body = render_comment(FixOutcome(
        kind=OutcomeKind.HANDOFF, summary="could not verify the fix here (no jsonschema)",
        proposal=_proposal(), handoff_patch="--- a/f\n+++ b/f\n+fix\n", failing_run_url=_RUN,
    ))
    assert "did not push it: could not verify the fix here" in body
    assert "+fix" in body
    assert "A human should apply it" in body


def test_failed_comment():
    body = render_comment(FixOutcome(kind=OutcomeKind.FAILED, summary="could not clone repo"))
    assert "error" in body.lower()
    assert "could not clone repo" in body
