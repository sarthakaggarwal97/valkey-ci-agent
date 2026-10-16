from scripts.ci_fix.models import (
    CandidateAction,
    Failure,
    FailureType,
    FixCandidate,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    ResultKind,
    TargetKind,
)


def test_failure_run_url_binds_rerun_attempt() -> None:
    first = Failure("owner/repo", 12, "a" * 40, 34, "test")
    rerun = Failure("owner/repo", 12, "a" * 40, 34, "test", run_attempt=3)

    assert first.run_url == "https://github.com/owner/repo/actions/runs/12"
    assert rerun.run_url == "https://github.com/owner/repo/actions/runs/12/attempts/3"


def test_task_candidate_and_result_have_small_explicit_defaults() -> None:
    failure = Failure("owner/repo", 12, "a" * 40, 34, "test")
    target = FixTarget(TargetKind.DRAFT_PR, "owner/repo")
    task = FixTask(failure, target, FixPolicy.GENERAL)
    candidate = FixCandidate(CandidateAction.PATCH)
    result = FixResult(ResultKind.OPENED, "opened", task, candidate=candidate)

    assert target.draft is True
    assert candidate.failure_type is FailureType.UNKNOWN
    assert result.candidate is candidate


def test_enum_values_are_stable_for_workflow_and_json_boundaries() -> None:
    assert FixPolicy.BACKPORT.value == "backport"
    assert TargetKind.UPDATE_PR.value == "update-pr"
    assert CandidateAction.REFUSE.value == "refuse"
    assert ResultKind.SUGGESTED.value == "suggested"
