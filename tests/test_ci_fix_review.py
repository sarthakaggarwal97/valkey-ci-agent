"""Tests for CI-fix patch review and shared sampling policy."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix import review as review_mod
from scripts.ci_fix.models import (
    BaselineEvidence,
    BaselineKind,
    FailureMode,
    FixPath,
    FixProposal,
    ReviewVerdict,
)
from scripts.ci_fix.review import (
    MAX_REVIEWABLE_PATCH_CHARS,
    _is_noop_command,
    build_and_review_patch,
    combined_command,
    precheck_command,
    review_fix,
    sampling_policy,
)
from scripts.common.proc import EmptyPatch


def _proposal() -> FixProposal:
    return FixProposal(
        path=FixPath.AUTHOR,
        failing_check="targeted check",
        root_cause="stale generated fixture",
        reasoning="regenerate the fixture",
        confidence=0.95,
        build_command="make",
        verify_command="./runtest --single x",
    )


def _stream_result(payload: dict) -> str:
    return json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "result": json.dumps(payload),
        }
    )


def test_review_fix_parses_only_strict_boolean_approval(monkeypatch):
    run = MagicMock(
        return_value=MagicMock(
            stdout=_stream_result(
                {"approved": True, "reasoning": " minimal and correct "}
            ),
            returncode=0,
        )
    )
    monkeypatch.setattr(review_mod, "run_agent", run)

    verdict = review_fix("/repo", _proposal(), "diff")

    assert verdict == ReviewVerdict(True, "minimal and correct")
    assert "diff" in run.call_args.args[1]


@pytest.mark.parametrize(
    "result",
    [
        MagicMock(stdout="", returncode=1),
        MagicMock(stdout="no json", returncode=0),
        MagicMock(
            stdout=_stream_result({"approved": "yes", "reasoning": "ambiguous"}),
            returncode=0,
        ),
    ],
)
def test_review_fix_fails_closed(monkeypatch, result):
    monkeypatch.setattr(review_mod, "run_agent", MagicMock(return_value=result))

    assert review_fix("/repo", _proposal(), "diff").approved is False


def test_precheck_rejects_empty_and_vacuous_commands():
    empty = _proposal().__class__(
        **{**_proposal().__dict__, "build_command": "", "verify_command": ""}
    )
    noop = _proposal().__class__(
        **{
            **_proposal().__dict__,
            "build_command": "",
            "verify_command": "make || true",
        }
    )

    assert "no command" in precheck_command(empty)
    assert "no build or test signal" in precheck_command(noop)
    assert precheck_command(_proposal()) == ""


@pytest.mark.parametrize(
    "command",
    [
        "make || true",
        "make; true",
        "./runtest && true",
        "make test || :",
        "make test | tee test.log",
        "true",
    ],
)
def test_noop_detection_catches_masked_exit_status(command):
    assert _is_noop_command(command) is True


@pytest.mark.parametrize(
    "command",
    [
        "set -o pipefail; make test | tee test.log",
        "make && ./runtest --single x",
        "./runtest --single x",
        "cc -o x x.c && ./x",
    ],
)
def test_noop_detection_accepts_real_verification(command):
    assert _is_noop_command(command) is False


def test_combined_command_runs_phases_in_strict_subshells():
    proposal = _proposal().__class__(
        **{
            **_proposal().__dict__,
            "build_command": "export AR=/tmp/missing-ar; false; true",
            "verify_command": "echo should-not-run",
        }
    )

    command = combined_command(proposal)
    result = subprocess.run(
        ["/bin/sh", "-c", command],
        check=False,
        capture_output=True,
        text=True,
    )

    assert command == (
        "set -e\n"
        "(\nexport AR=/tmp/missing-ar; false; true\n)\n"
        "(\necho should-not-run\n)"
    )
    assert result.returncode != 0
    assert "should-not-run" not in result.stdout


def test_combined_command_isolates_plain_build_and_verify_phases():
    assert combined_command(_proposal()) == (
        "set -e\n"
        "(\nmake\n)\n"
        "(\n./runtest --single x\n)"
    )


def test_build_and_review_patch_fails_closed(monkeypatch):
    reviewer = MagicMock(return_value=ReviewVerdict(True, "ok"))

    monkeypatch.setattr(
        review_mod,
        "build_approved_patch",
        MagicMock(side_effect=EmptyPatch("empty")),
    )
    assert build_and_review_patch(
        "/repo", ("f",), _proposal(), review_func=reviewer
    ).ok is False
    reviewer.assert_not_called()

    monkeypatch.setattr(
        review_mod,
        "build_approved_patch",
        MagicMock(return_value="+" * (MAX_REVIEWABLE_PATCH_CHARS + 1)),
    )
    result = build_and_review_patch(
        "/repo", ("f",), _proposal(), review_func=reviewer
    )
    assert result.ok is False
    assert "too large" in result.detail
    reviewer.assert_not_called()


def test_build_and_review_patch_returns_exact_reviewed_patch(monkeypatch):
    monkeypatch.setattr(
        review_mod,
        "build_approved_patch",
        MagicMock(return_value="small diff"),
    )

    rejected = build_and_review_patch(
        "/repo",
        ("f",),
        _proposal(),
        review_func=lambda *_: ReviewVerdict(False, "weak"),
    )
    approved = build_and_review_patch(
        "/repo",
        ("f",),
        _proposal(),
        review_func=lambda *_: ReviewVerdict(True, "causal"),
    )

    assert rejected.ok is False
    assert rejected.patch == "small diff"
    assert approved.ok is True
    assert approved.patch == "small diff"
    assert approved.review == ReviewVerdict(True, "causal")


@pytest.mark.parametrize(
    ("kind", "diagnosed", "candidate_runs", "handoff_only", "observed"),
    [
        (BaselineKind.DETERMINISTIC, FailureMode.UNKNOWN, 2, False, FailureMode.DETERMINISTIC),
        (BaselineKind.FLAKY, FailureMode.UNKNOWN, 10, False, FailureMode.FLAKY),
        (BaselineKind.NOT_REPRODUCED, FailureMode.UNKNOWN, 10, True, FailureMode.UNKNOWN),
        (BaselineKind.UNAVAILABLE, FailureMode.FLAKY, 10, True, FailureMode.FLAKY),
    ],
)
def test_sampling_policy_is_shared_across_backends(
    kind,
    diagnosed,
    candidate_runs,
    handoff_only,
    observed,
):
    baseline = BaselineEvidence(
        kind=kind,
        attempts=3,
        passed=0,
        failed=3,
    )

    policy = sampling_policy(
        baseline,
        diagnosed,
        verify_runs=2,
        flaky_verify_runs=10,
    )

    assert policy.candidate_runs == candidate_runs
    assert policy.handoff_only is handoff_only
    assert policy.observed_mode is observed
