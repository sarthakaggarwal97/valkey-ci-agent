

def test_render_handoff_includes_patch_and_language():
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import FixOutcome, FixPath, FixProposal, OutcomeKind

    proposal = FixProposal(path=FixPath.AUTHOR, failing_check="reply-schemas-validator",
                           root_cause="missing dep", reasoning="r", confidence=0.9,
                           build_command="m", verify_command="v")
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF, summary="could not verify the fix here (no jsonschema)",
        proposal=proposal, handoff_patch="--- a/f\n+++ b/f\n+fix\n",
        failing_run_url="https://github.com/o/r/actions/runs/1",
    )
    body = render_comment(outcome)
    assert "handing it off" in body
    assert "could not verify" in body
    assert "+fix" in body            # the patch is included
    assert "I did not push this" in body


def test_render_handoff_patch_cannot_escape_code_fence():
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import FixOutcome, OutcomeKind

    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="review required",
        handoff_patch="+ok\n```\n## injected heading\n",
    )

    body = render_comment(outcome)

    assert "````diff" in body
    assert "\n## injected heading" in body


def test_render_handoff_explains_flaky_baseline():
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import (
        BaselineEvidence,
        BaselineKind,
        FixOutcome,
        OutcomeKind,
    )

    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="candidate prepared",
        handoff_patch="diff",
        baseline=BaselineEvidence(
            kind=BaselineKind.FLAKY,
            attempts=5,
            passed=3,
            failed=2,
            detail="mixed",
        ),
    )

    body = render_comment(outcome)
    assert "confirmed flaky" in body
    assert "2 failure(s), 3 pass(es)" in body


def test_target_workflow_verification_link_is_rendered():
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import (
        FixOutcome,
        FixPath,
        FixProposal,
        OutcomeKind,
        ReviewVerdict,
    )

    proposal = FixProposal(
        path=FixPath.AUTHOR,
        failing_check="unit/keyspace",
        root_cause="event ordering",
        reasoning="r",
        confidence=0.9,
    )
    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="clean baseline did not reproduce",
        proposal=proposal,
        review=ReviewVerdict(True, "causal fix"),
        verify_backend="target-workflow",
        verification_run_url="https://github.com/o/r/actions/runs/77",
        handoff_patch="diff --git a/tests/x b/tests/x\n",
    )

    body = render_comment(outcome)

    assert "exact-environment workflow" in body
    assert "actions/runs/77" in body


def test_unavailable_handoff_still_links_remote_evidence():
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import FixOutcome, OutcomeKind

    outcome = FixOutcome(
        kind=OutcomeKind.HANDOFF,
        summary="target runner was cancelled",
        verification_run_url="https://github.com/o/r/actions/runs/88",
    )

    assert "actions/runs/88" in render_comment(outcome)
