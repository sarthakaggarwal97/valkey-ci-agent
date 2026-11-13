"""Replaying issues whose fix is known, and scoring the result."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.ci_fix.models import (
    CandidateAction,
    FailedTest,
    Failure,
    FixCandidate,
    FixOutcome,
    FixPlan,
    FixPolicy,
    FixTarget,
    FixTask,
    FrontDoor,
    IssueBrief,
    TargetKind,
)
from scripts.factory import replay as replay_mod

_OPENED = datetime(2026, 10, 22, 6, 38, tzinfo=timezone.utc)
_BASE = "b" * 40
_MERGE = "d" * 40


def test_the_shipped_cases_load():
    cases = replay_mod.load_cases()
    assert "xinfo-groups-lag" in cases and all(case.fix_pull > 0 or case.base_ref for case in cases.values())
    refused = {case.id for case in cases.values() if case.expect == "refused"}
    assert refused == {
        "vague-memory-fragmentation", "acl-digest-new-command", "deps-module-static-linking",
        "zset-bare-paren-already-fixed",
    }
    assert cases["zset-bare-paren-already-fixed"].base_ref and cases["zset-bare-paren"].base_ref == ""


def test_a_case_without_a_fix_pull_is_refused(tmp_path):
    path = tmp_path / "cases.yml"
    path.write_text("cases:\n  - id: x\n    repo: o/r\n    issue: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="needs a unique id, repo, issue and fix_pull"):
        replay_mod.load_cases(path)


def test_a_refused_case_needs_a_base_ref_instead_of_a_fix_pull(tmp_path):
    path = tmp_path / "cases.yml"
    path.write_text("cases:\n  - id: x\n    repo: o/r\n    issue: 1\n    expect: refused\n", encoding="utf-8")
    with pytest.raises(ValueError, match="base_ref instead of fix_pull"):
        replay_mod.load_cases(path)
    path.write_text(
        f"cases:\n  - id: x\n    repo: o/r\n    issue: 1\n    expect: refused\n    base_ref: {_BASE}\n", encoding="utf-8",
    )
    case = replay_mod.load_cases(path)["x"]
    assert (case.fix_pull, case.expect, case.base_ref) == (0, "refused", _BASE)


def test_an_unknown_expectation_and_a_short_base_ref_are_refused(tmp_path):
    path = tmp_path / "cases.yml"
    path.write_text("cases:\n  - id: x\n    repo: o/r\n    issue: 1\n    fix_pull: 2\n    expect: maybe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expect is one of fix, refused"):
        replay_mod.load_cases(path)
    path.write_text("cases:\n  - id: x\n    repo: o/r\n    issue: 1\n    fix_pull: 2\n    base_ref: abc123\n", encoding="utf-8")
    with pytest.raises(ValueError, match="base_ref is a full commit sha"):
        replay_mod.load_cases(path)


def _comment(author, association, body, at):
    return SimpleNamespace(user=SimpleNamespace(login=author), author_association=association, body=body, created_at=at)


def test_the_plan_pins_the_issue_as_of_the_fix_and_works_on_the_commit_it_merged_onto():
    issue = SimpleNamespace(
        number=4979, title="lag too high", body="XTRIM then XINFO", labels=[SimpleNamespace(name="bug")],
        get_comments=lambda: [
            _comment("maintainer", "MEMBER", "confirmed", _OPENED - timedelta(days=1)),
            _comment("passerby", "NONE", "me too", _OPENED - timedelta(days=1)),
            _comment("maintainer", "MEMBER", "fixed in #4983", _OPENED + timedelta(hours=1)),
        ],
    )
    pull = SimpleNamespace(created_at=_OPENED, merged_at=_OPENED + timedelta(days=14), merge_commit_sha=_MERGE,
                           base=SimpleNamespace(ref="unstable"))
    repo = SimpleNamespace(
        get_issue=lambda n: issue, get_pull=lambda n: pull,
        get_commit=lambda sha: SimpleNamespace(parents=[SimpleNamespace(sha=_BASE), SimpleNamespace(sha="e" * 40)]),
    )
    case = replay_mod.Case("xinfo-groups-lag", "about", "valkey-io/valkey", 4979, 4983)

    plan, merge_sha = replay_mod.plan_case(SimpleNamespace(get_repo=lambda name: repo), case)

    assert merge_sha == _MERGE and plan.front_door is FrontDoor.FACTORY
    assert plan.task.target.expected_sha == _BASE and plan.task.target.branch == "agent/factory/replay-xinfo-groups-lag"
    # The fix that landed is the history after the working commit: the clone must not show it.
    assert plan.task.history_ends_at == _BASE
    assert plan.brief is not None and [c.body for c in plan.brief.comments] == ["confirmed"]
    assert plan.brief.labelled_by == "replay" and plan.brief.label_event_id == 4983


def test_a_base_ref_overrides_the_commit_the_fix_merged_onto():
    issue = SimpleNamespace(number=3483, title="bare paren", body="ZRANGEBYSCORE", labels=[], get_comments=lambda: [])
    pull = SimpleNamespace(created_at=_OPENED, merged_at=_OPENED + timedelta(days=1), merge_commit_sha=_MERGE,
                           base=SimpleNamespace(ref="unstable"))
    repo = SimpleNamespace(
        get_issue=lambda n: issue, get_pull=lambda n: pull,
        get_commit=lambda sha: SimpleNamespace(parents=[SimpleNamespace(sha=_BASE)]),
    )
    case = replay_mod.Case("already-fixed", "about", "valkey-io/valkey", 3483, 4054, expect="refused", base_ref=_MERGE)

    plan, merge_sha = replay_mod.plan_case(SimpleNamespace(get_repo=lambda name: repo), case)

    assert merge_sha == _MERGE and plan.task.target.expected_sha == _MERGE and plan.task.failure.head_sha == _MERGE


def test_a_case_without_a_fix_is_pinned_at_the_issues_open_time_on_its_base_ref():
    opened = _OPENED - timedelta(days=30)
    issue = SimpleNamespace(
        number=3517, title="high rss", body="no way to reproduce", labels=[SimpleNamespace(name="bug")],
        created_at=opened,
        get_comments=lambda: [
            _comment("maintainer", "MEMBER", "which allocator?", opened + timedelta(hours=1)),
        ],
    )
    repo = SimpleNamespace(get_issue=lambda n: issue, default_branch="unstable",
                           get_pull=lambda n: pytest.fail("a case without a fix reads no pull request"))
    case = replay_mod.Case("vague", "about", "valkey-io/valkey", 3517, expect="refused", base_ref=_BASE)

    plan, merge_sha = replay_mod.plan_case(SimpleNamespace(get_repo=lambda name: repo), case)

    assert merge_sha == "" and plan.task.target.expected_sha == _BASE and plan.task.failure.base_branch == "unstable"
    assert plan.brief is not None and plan.brief.labelled_at == opened.isoformat() and plan.brief.comments == ()
    assert plan.brief.label_event_id == 3517 and plan.task.target.branch == "agent/factory/replay-vague"


def _plan() -> FixPlan:
    task = FixTask(
        Failure("o/r", 0, _BASE, 0, "", base_branch="unstable", issue_number=9,
                failed_tests=(FailedTest("lag", "tests/unit/type/stream.tcl"),)),
        FixTarget(TargetKind.DRAFT_PR, "o/r", "o/r", "agent/factory/replay-x", _BASE), FixPolicy.GENERAL,
    )
    return FixPlan(front_door=FrontDoor.FACTORY, task=task, brief=IssueBrief(9, "t", "b"))


_CASE = replay_mod.Case("x", "about", "o/r", 9, 42)
_KNOWN = (
    "diff --git a/src/t_stream.c b/src/t_stream.c\n--- a/src/t_stream.c\n+++ b/src/t_stream.c\n@@ -1 +1,2 @@\n"
    "+    lag = streamEstimateDistanceFromFirstEverEntry(s, &cg->last_id);\n+    if (lag < 0) lag = 0;\n"
    "diff --git a/tests/unit/type/stream.tcl b/tests/unit/type/stream.tcl\n--- a/tests/unit/type/stream.tcl\n"
    "+++ b/tests/unit/type/stream.tcl\n@@ -1 +1,2 @@\n+    test {the known test} {}\n"
)


def _candidate(fix_lines: str) -> FixCandidate:
    patch = (
        "diff --git a/tests/unit/type/stream.tcl b/tests/unit/type/stream.tcl\n--- a/tests/unit/type/stream.tcl\n"
        "+++ b/tests/unit/type/stream.tcl\n@@ -1 +1,2 @@\n+    test {lag} {}\n"
        "diff --git a/src/t_stream.c b/src/t_stream.c\n--- a/src/t_stream.c\n+++ b/src/t_stream.c\n@@ -1 +1,2 @@\n"
        + fix_lines
    )
    return FixCandidate(CandidateAction.PATCH, patch=patch, evidence_patch="+    test {lag} {}")


@pytest.fixture
def known(monkeypatch):
    monkeypatch.setattr(replay_mod, "_known_fix", lambda plan, merge_sha: _KNOWN)


def test_a_fix_with_the_known_fixs_lines_whose_test_passes_with_it_caught_the_bug(known, monkeypatch):
    monkeypatch.setattr(replay_mod, "_test_passes_with", lambda plan, merge_sha, candidate: True)
    candidate = _candidate("+    lag = streamEstimateDistanceFromFirstEverEntry(s, &cg->last_id);\n+    if (lag < 0) lag = 0;\n")

    result = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(candidate=candidate, started=True))

    assert result.verdict == "caught" and result.shared_percent == 100
    assert result.shared_files == ("src/t_stream.c",) and result.known_fix_files == ("src/t_stream.c",)
    assert result.test_passes_with_known_fix is True and "passes with the known fix" in result.detail
    assert (result.expected, result.matches_expectation) == ("fix", True)


def test_another_fix_whose_test_still_passes_with_the_known_one_differs_but_caught_the_bug(known, monkeypatch):
    monkeypatch.setattr(replay_mod, "_test_passes_with", lambda plan, merge_sha, candidate: True)
    result = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(candidate=_candidate("+    return 0;\n"), started=True))
    assert result.verdict == "caught" and result.shared_percent == 0


def test_a_port_is_scored_by_the_commit_it_ports(known, monkeypatch):
    """A port has no patch of its own; the first replays scored one as 0% shared and 'caught'."""
    monkeypatch.setattr(replay_mod, "_test_passes_with", lambda plan, merge_sha, candidate: True)
    monkeypatch.setattr(replay_mod, "_commit_diff", lambda plan, sha: _KNOWN if sha == "f" * 40 else "")
    ported = FixCandidate(CandidateAction.PORT, port_commit="f" * 40, evidence_patch="+    test {lag} {}")

    result = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(candidate=ported, started=True))

    assert result.verdict == "caught" and result.shared_percent == 100
    assert result.detail.startswith("The bot ported ffffffffffff.")


def test_a_test_that_does_not_pass_with_the_known_fix_tests_something_else(known, monkeypatch):
    monkeypatch.setattr(replay_mod, "_test_passes_with", lambda plan, merge_sha, candidate: False)
    result = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(candidate=_candidate("+    return 0;\n"), started=True))
    assert result.verdict == "missed" and "tests something else" in result.detail


def test_a_refusal_and_a_failed_job_are_results_too(known):
    refused = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(
        candidate=FixCandidate(CandidateAction.REFUSE, summary="no fix", evidence_patch="+test"), started=True,
    ))
    assert (refused.verdict, refused.test_written) == ("refused", True)
    assert (refused.expected, refused.matches_expectation) == ("fix", False)
    failed = replay_mod.score(_CASE, _plan(), _MERGE, FixOutcome(error="the runner died"))
    assert (failed.verdict, failed.detail, failed.matches_expectation) == ("failed", "the runner died", False)


_REFUSED_CASE = replay_mod.Case("vague", "about", "o/r", 9, expect="refused", base_ref=_BASE)


def test_a_refused_case_the_factory_refuses_matches_its_expectation():
    result = replay_mod.score(_REFUSED_CASE, _plan(), "", FixOutcome(
        candidate=FixCandidate(CandidateAction.REFUSE, summary="nothing to test"), started=True,
    ))
    assert (result.verdict, result.expected, result.matches_expectation) == ("refused", "refused", True)
    assert result.detail == "nothing to test" and result.test_written is False


def test_a_fix_for_a_refused_case_without_a_known_fix_differs_and_misses_the_expectation(known):
    result = replay_mod.score(_REFUSED_CASE, _plan(), "", FixOutcome(candidate=_candidate("+    return 0;\n"), started=True))
    assert (result.verdict, result.expected, result.matches_expectation) == ("differs", "refused", False)
    assert "no known fix" in result.detail and result.test_written is True and result.shared_percent == 0


def test_a_fix_for_an_already_fixed_case_is_still_compared_with_the_known_fix(known, monkeypatch):
    monkeypatch.setattr(replay_mod, "_test_passes_with", lambda plan, merge_sha, candidate: None)
    case = replay_mod.Case("already-fixed", "about", "o/r", 9, 42, expect="refused", base_ref=_MERGE)
    candidate = _candidate("+    lag = streamEstimateDistanceFromFirstEverEntry(s, &cg->last_id);\n+    if (lag < 0) lag = 0;\n")

    result = replay_mod.score(case, _plan(), _MERGE, FixOutcome(candidate=candidate, started=True))

    assert (result.verdict, result.shared_percent, result.matches_expectation) == ("match", 100, False)
