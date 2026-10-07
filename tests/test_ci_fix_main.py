"""Tests for the ci_fix workflow entry point."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from scripts.ci_fix import main as main_mod
from scripts.ci_fix.main import _parse_event, main
from scripts.ci_fix.models import FixOutcome, OutcomeKind

_RUN_URL = "https://github.com/valkey-io/valkey/actions/runs/27559908167"


def _event(*, action="created", is_pr=True, body=f"@valkeyrie-bot fix {_RUN_URL}",
           login="alice", number=3988, repo="valkey-io/valkey", comment_id=7):
    issue: dict = {"number": number}
    if is_pr:
        issue["pull_request"] = {"url": "..."}
    return {
        "action": action,
        "issue": issue,
        "comment": {"id": comment_id, "body": body, "user": {"login": login}},
        "repository": {"full_name": repo},
    }


def test_parse_event_happy():
    parsed = _parse_event(_event())
    assert parsed == ("valkey-io/valkey", 3988, "alice", f"@valkeyrie-bot fix {_RUN_URL}", 7)


def test_parse_event_ignores_non_pr():
    assert _parse_event(_event(is_pr=False)) is None


def test_parse_event_ignores_non_created():
    assert _parse_event(_event(action="edited")) is None


def test_parse_event_ignores_missing_fields():
    assert _parse_event(_event(login="")) is None


def _write_event(tmp_path, event) -> str:
    path = tmp_path / "event.json"
    path.write_text(json.dumps(event))
    return str(path)


def _gate(tmp_path, monkeypatch, *extra):
    state = tmp_path / "state.json"
    outputs = tmp_path / "outputs.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(outputs))
    rc = main(["gate", "--state", str(state), "--target-token", "t", *extra])
    lines = outputs.read_text().splitlines() if outputs.exists() else []
    return rc, state, dict(line.split("=", 1) for line in lines)


def _request(**overrides):
    from scripts.ci_fix.models import FixRequest

    values = dict(repo_full_name="valkey-io/valkey", pr_number=3988, head_repo_full_name="valkey-io/valkey",
                  head_branch="agent/backport/sweep/8.0", head_sha="a" * 40, run_id=27559908167,
                  requested_by="alice")
    values.update(overrides)
    return FixRequest(**values)


def _stub_gate(monkeypatch, result):
    monkeypatch.setattr(main_mod, "Github", MagicMock())
    gate = result if isinstance(result, MagicMock) else MagicMock(return_value=result)
    monkeypatch.setattr(main_mod, "build_fix_request", gate)
    return gate


def test_gate_ignores_non_command_comments(tmp_path, monkeypatch):
    for event in (_event(body="thanks, lgtm"), _event(is_pr=False)):
        rc, state, outputs = _gate(tmp_path, monkeypatch, "--event-path", _write_event(tmp_path, event))
        assert rc == 0
        assert not state.exists()
        assert outputs == {"publication": "none", "execute": "false"}


def test_gate_records_the_request_and_reports_its_publication(tmp_path, monkeypatch):
    from scripts.ci_fix.models import Publication

    gate = _stub_gate(monkeypatch, _request(publication=Publication.SUGGEST, execute=False))
    rc, state, outputs = _gate(tmp_path, monkeypatch, "--repo", "valkey-io/valkey", "--pr", "3988",
                               "--commenter", "alice", "--hint", "look at payload", "--comment-id", "7")
    assert rc == 0
    assert outputs == {"publication": "suggest", "execute": "false"}
    payload = json.loads(state.read_text())
    assert payload["context"] == {"repo": "valkey-io/valkey", "pr": 3988, "comment_id": 7}
    assert payload["request"]["publication"] == "suggest"
    # Until prepare finishes, the record says the run stopped early.
    assert payload["outcome"]["kind"] == "failed"
    assert gate.call_args.kwargs["command"].hint == "look at payload"
    assert gate.call_args.kwargs["command"].run_id == 0


def test_gate_passes_a_dispatched_run_url(tmp_path, monkeypatch):
    gate = _stub_gate(monkeypatch, _request())
    _gate(tmp_path, monkeypatch, "--repo", "valkey-io/valkey", "--pr", "3988", "--commenter", "alice",
          "--run-url", _RUN_URL)
    assert gate.call_args.kwargs["command"].run_id == 27559908167


def test_gate_rejection_is_recorded_for_publication_without_a_request(tmp_path, monkeypatch):
    from scripts.ci_fix.gate import GateRejection

    _stub_gate(monkeypatch, GateRejection("not a member"))
    rc, state, outputs = _gate(tmp_path, monkeypatch, "--event-path", _write_event(tmp_path, _event()))
    assert rc == 0
    assert outputs["publication"] == "none"
    payload = json.loads(state.read_text())
    assert payload["request"] is None
    assert payload["outcome"] == {**payload["outcome"], "kind": "refused", "summary": "not a member"}


def test_gate_error_is_reported_as_a_failure_without_leaking_it(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "valkey-io/valkey-ci-agent")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    _stub_gate(monkeypatch, MagicMock(side_effect=RuntimeError("boom")))
    _rc, state, outputs = _gate(tmp_path, monkeypatch, "--event-path", _write_event(tmp_path, _event()))
    payload = json.loads(state.read_text())
    assert outputs["publication"] == "none"
    assert payload["request"] is None
    assert payload["outcome"]["kind"] == "failed"
    assert "boom" not in payload["outcome"]["summary"]
    assert "(https://github.com/valkey-io/valkey-ci-agent/actions/runs/42)" in payload["outcome"]["summary"]


def _gated_state(tmp_path, request=None):
    from scripts.ci_fix.main import _INTERRUPTED
    from scripts.ci_fix.models import to_dict
    from scripts.ci_fix.publish import write_state

    state = tmp_path / "state.json"
    write_state(str(state), {"context": {"repo": "valkey-io/valkey", "pr": 3988, "comment_id": 7},
                             "request": to_dict(request or _request()), "outcome": to_dict(_INTERRUPTED)})
    return state


def test_prepare_runs_the_engine_on_the_gated_request(tmp_path, monkeypatch):
    ready = FixOutcome(kind=OutcomeKind.READY, summary="Fix for t", patch="diff", verify_backend="local")
    engine = MagicMock(return_value=ready)
    monkeypatch.setattr(main_mod, "Github", MagicMock())
    monkeypatch.setattr(main_mod, "ArtifactClient", MagicMock())
    monkeypatch.setattr(main_mod, "run_ci_fix_request", engine)
    state = _gated_state(tmp_path)
    assert main(["prepare", "--state", str(state), "--target-token", "t"]) == 0
    assert engine.call_args.kwargs["request"] == _request()
    payload = json.loads(state.read_text())
    assert payload["outcome"]["kind"] == "ready"
    assert payload["request"]["head_sha"] == "a" * 40


def test_prepare_records_an_internal_error_without_leaking_it(tmp_path, monkeypatch):
    monkeypatch.setattr(main_mod, "Github", MagicMock())
    monkeypatch.setattr(main_mod, "ArtifactClient", MagicMock())
    monkeypatch.setattr(main_mod, "run_ci_fix_request", MagicMock(side_effect=RuntimeError("boom")))
    state = _gated_state(tmp_path)
    main(["prepare", "--state", str(state), "--target-token", "t"])
    outcome = json.loads(state.read_text())["outcome"]
    assert outcome["kind"] == "failed"
    assert "boom" not in outcome["summary"]


def test_a_killed_prepare_leaves_the_gate_record(tmp_path, monkeypatch):
    def killed(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(main_mod, "Github", MagicMock())
    monkeypatch.setattr(main_mod, "ArtifactClient", MagicMock())
    monkeypatch.setattr(main_mod, "run_ci_fix_request", killed)
    state = _gated_state(tmp_path)
    try:
        main(["prepare", "--state", str(state), "--target-token", "t"])
    except KeyboardInterrupt:
        pass
    outcome = json.loads(state.read_text())["outcome"]
    assert (outcome["kind"], "stopped before it finished" in outcome["summary"]) == ("failed", True)


def test_prepare_without_gate_state_fails_the_step(tmp_path):
    assert main(["prepare", "--state", str(tmp_path / "none.json"), "--target-token", "t"]) == 1


def _publish(tmp_path, monkeypatch, outcome, *, request=True, publication="push", pr=3988):
    from scripts.ci_fix.models import to_dict
    from scripts.ci_fix.publish import write_state

    state = tmp_path / "state.json"
    write_state(str(state), {
        "context": {"repo": "valkey-io/valkey", "pr": pr, "comment_id": 7},
        "request": to_dict(_request()) if request else None,
        "outcome": to_dict(outcome),
    })
    posted, reacted = {}, {}
    monkeypatch.setattr(main_mod, "Github", MagicMock())
    monkeypatch.setattr(main_mod, "_post_comment",
                        lambda gh, repo, num, body: posted.update(repo=repo, num=num, body=body))
    monkeypatch.setattr(main_mod, "_react_outcome",
                        lambda gh, repo, cid, kind: reacted.update(cid=cid, kind=kind))
    rc = main(["publish", "--state", str(state), "--target-token", "write-token", "--publication", publication])
    return rc, posted, reacted


_READY = FixOutcome(kind=OutcomeKind.READY, summary="Fix for t", patch="diff", verify_backend="local")


def test_publish_pushes_a_ready_fix_then_comments_and_reacts(tmp_path, monkeypatch):
    pushed = FixOutcome(kind=OutcomeKind.PUSHED, summary="Pushed fix for t", commit_sha="b" * 40)
    publish = MagicMock(return_value=pushed)
    monkeypatch.setattr(main_mod, "publish_to_pr", publish)
    check = MagicMock(return_value="")
    monkeypatch.setattr(main_mod, "pr_head_check", lambda _gh, request: check)
    rc, posted, reacted = _publish(tmp_path, monkeypatch, _READY)
    assert rc == 0
    assert publish.call_args.args[1].head_sha == "a" * 40
    # The PR is revalidated immediately before any push.
    assert publish.call_args.kwargs["pre_push_check"] is check
    assert "bbbbbbbbbbbb" in posted["body"]
    assert reacted == {"cid": 7, "kind": OutcomeKind.PUSHED}


def test_publish_refuses_a_decision_the_gate_did_not_allow(tmp_path, monkeypatch):
    """A request that changed after the gate must not get the token the gate scoped."""
    publish = MagicMock()
    monkeypatch.setattr(main_mod, "publish_to_pr", publish)
    rc, posted, _ = _publish(tmp_path, monkeypatch, _READY, publication="suggest")
    assert rc == 1
    publish.assert_not_called()
    assert "internal error stopped publication" in posted["body"]


def test_publish_refuses_a_decision_prepared_for_another_pr(tmp_path, monkeypatch):
    publish = MagicMock()
    monkeypatch.setattr(main_mod, "publish_to_pr", publish)
    rc, posted, _ = _publish(tmp_path, monkeypatch, _READY, pr=7)
    assert rc == 1
    publish.assert_not_called()
    assert posted["num"] == 7 and "internal error stopped publication" in posted["body"]


def test_publish_reports_a_final_outcome_without_publishing(tmp_path, monkeypatch):
    publish = MagicMock()
    monkeypatch.setattr(main_mod, "publish_to_pr", publish)
    refused = FixOutcome(kind=OutcomeKind.REFUSED, summary="not a member")
    rc, posted, reacted = _publish(tmp_path, monkeypatch, refused, request=False, publication="none")
    assert rc == 0
    publish.assert_not_called()
    assert "not a member" in posted["body"]
    assert reacted["kind"] is OutcomeKind.REFUSED


def test_publish_returns_nonzero_for_a_failed_run(tmp_path, monkeypatch):
    failed = FixOutcome(kind=OutcomeKind.FAILED, summary="clone failed")
    rc, posted, _ = _publish(tmp_path, monkeypatch, failed)
    assert rc == 1
    assert "clone failed" in posted["body"]


def test_publish_without_state_does_nothing(tmp_path):
    assert main(["publish", "--state", str(tmp_path / "none.json"), "--target-token", "t",
                 "--publication", "none"]) == 0


def test_verify_runs_env_parsing(monkeypatch):
    from scripts.ci_fix.main import _MAX_VERIFY_RUNS, _verify_runs
    from scripts.ci_fix.review import DEFAULT_VERIFY_RUNS

    monkeypatch.delenv("CI_FIX_VERIFY_RUNS", raising=False)
    assert _verify_runs() == DEFAULT_VERIFY_RUNS

    monkeypatch.setenv("CI_FIX_VERIFY_RUNS", "5")
    assert _verify_runs() == 5

    monkeypatch.setenv("CI_FIX_VERIFY_RUNS", "0")
    assert _verify_runs() == 1

    monkeypatch.setenv("CI_FIX_VERIFY_RUNS", "999")
    assert _verify_runs() == _MAX_VERIFY_RUNS

    monkeypatch.setenv("CI_FIX_VERIFY_RUNS", "not-a-number")
    assert _verify_runs() == DEFAULT_VERIFY_RUNS


def _reaction_gh():
    requester = MagicMock()
    gh = MagicMock()
    gh.get_repo.return_value._requester = requester
    return gh, requester


def test_react_outcome_pushed_adds_plus_one():
    from scripts.ci_fix.main import _react_outcome

    gh, requester = _reaction_gh()
    _react_outcome(gh, "valkey-io/valkey", 55, OutcomeKind.PUSHED)
    requester.requestJsonAndCheck.assert_called_once_with(
        "POST", "/repos/valkey-io/valkey/issues/comments/55/reactions",
        input={"content": "+1"},
    )


def test_react_outcome_refused_adds_minus_one():
    from scripts.ci_fix.main import _react_outcome

    gh, requester = _reaction_gh()
    _react_outcome(gh, "valkey-io/valkey", 55, OutcomeKind.REFUSED)
    requester.requestJsonAndCheck.assert_called_once_with(
        "POST", "/repos/valkey-io/valkey/issues/comments/55/reactions",
        input={"content": "-1"},
    )


def test_react_outcome_no_comment_id_is_noop():
    from scripts.ci_fix.main import _react_outcome

    gh, requester = _reaction_gh()
    _react_outcome(gh, "valkey-io/valkey", 0, OutcomeKind.PUSHED)
    requester.requestJsonAndCheck.assert_not_called()


def test_react_outcome_swallows_failure():
    """A failed reaction must never raise: the comment is the real report."""
    from scripts.ci_fix.main import _react_outcome

    gh, requester = _reaction_gh()
    requester.requestJsonAndCheck.side_effect = RuntimeError("boom")
    _react_outcome(gh, "valkey-io/valkey", 55, OutcomeKind.PUSHED)


def test_react_outcome_swallows_repo_lookup_failure():
    """A transient failure in the repo lookup must not escape either."""
    from scripts.ci_fix.main import _react_outcome

    gh = MagicMock()
    gh.get_repo.side_effect = RuntimeError("transient API error")
    _react_outcome(gh, "valkey-io/valkey", 55, OutcomeKind.PUSHED)
