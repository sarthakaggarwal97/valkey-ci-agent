"""Tests for the publication step and the engine-to-publication state handoff."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ci_fix.models import (
    FailureType,
    FixOutcome,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
    Policy,
    Publication,
    ReviewVerdict,
    RunResult,
    outcome_from_dict,
    request_from_dict,
    to_dict,
)
from scripts.ci_fix.publish import pr_head_check, publish_to_pr, read_state, write_state
from scripts.ci_fix.push import PushRefused

_HEAD = "a" * 40


def _proposal() -> FixProposal:
    return FixProposal(path=FixPath.AUTHOR, failing_check="t", root_cause="rc", reasoning="r",
                       confidence=0.9, failure_type=FailureType.FLAKY, culprit_commit="c" * 12)


def _request(**overrides) -> FixRequest:
    values = dict(
        repo_full_name="valkey-io/valkey", pr_number=7, head_repo_full_name="valkey-io/valkey",
        head_branch="agent/backport/sweep/9.0", head_sha=_HEAD, run_id=5, requested_by="alice",
        base_branch="9.0",
    )
    values.update(overrides)
    return FixRequest(**values)


def _ready(**overrides) -> FixOutcome:
    values = dict(kind=OutcomeKind.READY, summary="Fix for t", proposal=_proposal(),
                  patch="diff", changed_paths=("tests/t.tcl",), verify_backend="local")
    values.update(overrides)
    return FixOutcome(**values)


# --- state handoff -----------------------------------------------------------------

def test_state_round_trips_request_and_outcome(tmp_path):
    request = _request(policy=Policy.FIX, publication=Publication.SUGGEST, execute=False,
                       culprit_range="a..b", target="the failure in job `x`")
    outcome = _ready(
        run_result=RunResult(True, True, 0, "cmd", "tail"), review=ReviewVerdict(True, "ok"),
        culprit_sha="c" * 40, culprit_subject="subject", other_failing_checks=("other",),
    )
    path = tmp_path / "state" / "state.json"
    write_state(str(path), {"context": {"pr": 7}, "request": to_dict(request), "outcome": to_dict(outcome)})

    state = read_state(str(path))
    assert state is not None
    assert request_from_dict(state["request"]) == request
    assert outcome_from_dict(state["outcome"]) == outcome
    # The handoff holds diagnosis text from untrusted logs; keep it private.
    assert os.stat(path).st_mode & 0o777 == 0o600


def test_missing_state_means_nothing_to_publish(tmp_path):
    assert read_state(str(tmp_path / "absent.json")) is None


def test_state_from_another_version_is_rejected(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"version": 99}))
    with pytest.raises(ValueError):
        read_state(str(path))


# --- publish_to_pr -----------------------------------------------------------------

def test_final_outcomes_pass_through_unchanged():
    refused = FixOutcome(kind=OutcomeKind.REFUSED, summary="no")
    assert publish_to_pr(refused, _request(), git_env={}) is refused


def test_push_commits_the_approved_patch_on_the_validated_head():
    commit_fix = MagicMock(return_value="b" * 40)
    check = MagicMock(return_value="")
    outcome = publish_to_pr(_ready(), _request(), git_env={"K": "v"}, pre_push_check=check,
                            commit_fix=commit_fix, commit_port=MagicMock())
    assert outcome.kind is OutcomeKind.PUSHED
    assert outcome.commit_sha == "b" * 40
    kwargs = commit_fix.call_args.kwargs
    assert kwargs["patch"] == "diff"
    assert kwargs["changed_paths"] == ("tests/t.tcl",)
    assert (kwargs["head_branch"], kwargs["head_sha"]) == ("agent/backport/sweep/9.0", _HEAD)
    assert kwargs["pre_push_check"] is check
    assert kwargs["git_env"] == {"K": "v"}


def test_push_cherry_picks_an_approved_port():
    commit_port = MagicMock(return_value="b" * 40)
    outcome = publish_to_pr(
        _ready(patch="", changed_paths=(), port_commit="9" * 40, verify_backend="upstream-port"),
        _request(), git_env={}, commit_fix=MagicMock(), commit_port=commit_port,
    )
    assert outcome.kind is OutcomeKind.PUSHED
    assert commit_port.call_args.kwargs["unstable_fix_commit"] == "9" * 40
    assert outcome.summary.startswith("Ported upstream fix")


def test_an_unexecuted_fix_is_never_pushed():
    commit_fix = MagicMock()
    outcome = publish_to_pr(_ready(verify_backend=""), _request(), git_env={},
                            commit_fix=commit_fix, commit_port=MagicMock())
    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.handoff_patch == "diff"
    commit_fix.assert_not_called()


def test_a_refused_push_becomes_a_refusal():
    def refuse(**_kwargs):
        raise PushRefused("Refusing to push: the PR head moved")

    outcome = publish_to_pr(_ready(), _request(), git_env={}, commit_fix=refuse, commit_port=MagicMock())
    assert outcome.kind is OutcomeKind.REFUSED
    assert "head moved" in outcome.summary


def test_contributor_prs_get_a_suggestion_and_no_push():
    commit_fix = MagicMock()
    outcome = publish_to_pr(_ready(), _request(publication=Publication.SUGGEST), git_env={},
                            commit_fix=commit_fix, commit_port=MagicMock())
    assert outcome.kind is OutcomeKind.SUGGESTED
    commit_fix.assert_not_called()


def test_fork_prs_get_an_unexecuted_patch_as_a_handoff():
    outcome = publish_to_pr(_ready(verify_backend=""), _request(publication=Publication.SUGGEST),
                            git_env={}, commit_fix=MagicMock(), commit_port=MagicMock())
    assert outcome.kind is OutcomeKind.HANDOFF
    assert "fork" in outcome.summary


# --- pr_head_check -----------------------------------------------------------------

def _gh_with_pr(*, state="open", ref="agent/backport/sweep/9.0", sha=_HEAD):
    pr = SimpleNamespace(state=state, head=SimpleNamespace(ref=ref, sha=sha))
    gh = MagicMock()
    gh.get_repo.return_value.get_pull.return_value = pr
    return gh


def test_head_check_passes_for_the_unchanged_open_pr():
    assert pr_head_check(_gh_with_pr(), _request())() == ""


@pytest.mark.parametrize("pr, expected", [
    ({"state": "closed"}, "no longer open"),
    ({"ref": "other"}, "no longer has head branch"),
    ({"sha": "b" * 40}, "moved from aaaaaaaaaaaa to bbbbbbbbbbbb"),
])
def test_head_check_refuses_a_changed_pr(pr, expected):
    assert expected in pr_head_check(_gh_with_pr(**pr), _request())()


def test_a_suggested_port_is_not_called_verified():
    outcome = publish_to_pr(
        _ready(patch="", changed_paths=(), port_commit="9" * 40, verify_backend="upstream-port"),
        _request(publication=Publication.SUGGEST), git_env={}, commit_fix=MagicMock(), commit_port=MagicMock(),
    )
    assert outcome.kind is OutcomeKind.SUGGESTED
    assert "verified" not in outcome.summary
    from scripts.ci_fix.comment import render_comment
    from scripts.ci_fix.models import ReviewVerdict

    body = render_comment(replace(outcome, review=ReviewVerdict(
        approved=True, reasoning="Porting upstream commit; CI is the verification authority for a port.")))
    # Nothing was pushed, so no CI will run: the comment must not say otherwise.
    for claim in ("Verified by", "awaiting", "verification authority"):
        assert claim not in body
    assert f"git cherry-pick -x {'9' * 40}" in body and "update the branch" in body
