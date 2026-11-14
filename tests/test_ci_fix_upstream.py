from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.ci_fix import engine as engine_mod
from scripts.ci_fix import publish as publish_mod
from scripts.ci_fix import upstream
from scripts.ci_fix.agent import _candidate_from_payload
from scripts.ci_fix.comment import render_result
from scripts.ci_fix.models import (
    CandidateAction,
    FailedTest,
    Failure,
    FixCandidate,
    FixContext,
    FixPolicy,
    FixResult,
    FixTarget,
    FixTask,
    FrontDoor,
    OpenFix,
    ResultKind,
    TargetKind,
    UpstreamRun,
)
from scripts.ci_fix.verification import TestRun, VerifyPlan
from scripts.common.identity import APP_LOGIN
from scripts.common.valkey_ci import Source
from tests.git_helpers import git

_BOT = f"{APP_LOGIN}[bot]"
_HEAD = "a" * 40
_UNSTABLE = "e" * 40
_TEST = FailedTest("Replica keeps its shard_id", "tests/unit/cluster/reset.tcl")


def _task(*, upstream_branch: str = "unstable", open_fixes: tuple[OpenFix, ...] = ()) -> FixTask:
    return FixTask(
        failure=Failure(
            repo_full_name="valkey-io/valkey", run_id=100, head_sha=_HEAD, job_id=55,
            job_name="test-ubuntu-jemalloc", base_branch="8.0", pr_number=5077,
            source=Source.DAILY, workflow="daily",
        ),
        target=FixTarget(
            kind=TargetKind.UPDATE_PR, repo_full_name="valkey-io/valkey", base_repo_full_name="valkey-io/valkey",
            branch="agent/backport/sweep/8.0", expected_sha=_HEAD, fix_branch="agent/backport/ci-sweep/8.0",
            upstream_branch=upstream_branch,
        ),
        policy=FixPolicy.BACKPORT,
        open_fixes=open_fixes,
    )


def _candidate(**changes) -> FixCandidate:
    candidate = FixCandidate(
        action=CandidateAction.PATCH, failing_check="t", title="Wait for the shard_id to settle",
        root_cause="The test reads the shard_id before gossip settles.", reasoning="Wait for it.",
        patch="diff --git a/x b/x\n", changed_paths=("tests/unit/cluster/reset.tcl",),
        verification="With the fix, `make && ./runtest` passed 40 loop(s).",
        upstream=UpstreamRun(
            sha=_UNSTABLE, tests=(_TEST,), verification="It passed 40 loop(s) there.",
            patch="diff --git a/x b/x\n", changed_paths=("tests/unit/cluster/reset.tcl",),
        ),
    )
    return replace(candidate, **changes)


# --- the engine ---------------------------------------------------------------


@pytest.fixture
def _no_isolation(monkeypatch):
    monkeypatch.setattr(engine_mod, "isolated_user", lambda: "")
    monkeypatch.setattr(
        engine_mod, "plan_for", lambda repo_dir, tests, **_kwargs: VerifyPlan(build="true", test="true", tests=tests),
    )


def _repo(tmp_path: Path, unstable_value: str) -> tuple[str, str]:
    """Unstable's own job's checkout, ``value.txt`` as ``unstable_value``, and the release branch's patch."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.com")
    (repo / "value.txt").write_text(unstable_value, encoding="utf-8")
    Path(repo, _TEST.file).parent.mkdir(parents=True)
    Path(repo, _TEST.file).write_text("test {Replica keeps its shard_id} {}\n", encoding="utf-8")
    git(repo, "add", "value.txt", _TEST.file)
    git(repo, "commit", "-m", "unstable")
    patch = "\n".join([
        "diff --git a/value.txt b/value.txt", "--- a/value.txt", "+++ b/value.txt",
        "@@ -1 +1 @@", "-before", "+after", "",
    ])
    return str(repo), patch


_OK = "[ok]: Replica keeps its shard_id (1 ms)"
_FAILED = "[err]: Replica keeps its shard_id in tests/unit/cluster/reset.tcl"


def _prepare_upstream(repo: str, patch: str, run_test, **changes):
    return engine_mod.prepare_upstream(
        _task(), _FAILED, _candidate(upstream=None, patch=patch, **changes), MagicMock(), MagicMock(), run_test,
        repo_dir=repo,
    )


def _passing(tree, command, timeout):
    return TestRun(True, _OK, report=_OK, seconds=1.0)


def _failing(tree, command, timeout):
    if command == "true":
        return TestRun(True, "built")
    return TestRun(False, _FAILED, report=_FAILED, seconds=1.0)


@pytest.fixture
def _adapted(monkeypatch):
    """The agent's session on unstable: what it was given, and the fix it makes."""
    seen: dict = {}

    def fix_loop(task, repo_dir, original_head, job_log, context, plan, **kwargs):
        seen.update(task=task, head=original_head, context=context, kwargs=kwargs,
                    value=Path(repo_dir, "value.txt").read_text(encoding="utf-8"))
        return seen.get("made", FixCandidate(
            action=CandidateAction.PATCH, title="Wait for the shard_id on unstable", reasoning="Same wait.",
            patch="diff --git a/value.txt b/value.txt\n", changed_paths=("value.txt",),
            verification="It passed 40 loop(s) on unstable.",
        ))

    monkeypatch.setattr(engine_mod, "_fix_loop", fix_loop)
    return seen


def test_a_fix_that_applies_upstream_is_run_there(tmp_path, _no_isolation, _adapted) -> None:
    repo, patch = _repo(tmp_path, "before\n")
    seen = []

    def run_test(tree, command, timeout):
        seen.append((Path(tree, "value.txt").read_text(encoding="utf-8"), command))
        return _passing(tree, command, timeout)

    candidate = _prepare_upstream(repo, patch, run_test)

    run = candidate.upstream
    assert run is not None and run.tests == (_TEST,) and run.patch == patch and not run.adapted
    assert run.sha == git(Path(repo), "rev-parse", "HEAD")
    assert seen and all(value == "after\n" for value, _command in seen)
    assert "passed 40 loop(s)" in run.verification
    assert not _adapted  # no session on unstable


def test_a_fix_that_does_not_apply_upstream_is_made_for_its_code(tmp_path, _no_isolation, _adapted) -> None:
    repo, patch = _repo(tmp_path, "other\n")

    candidate = _prepare_upstream(repo, patch, _passing)

    run = candidate.upstream
    assert run is not None and run.adapted and run.patch == "diff --git a/value.txt b/value.txt\n"
    assert run.title == "Wait for the shard_id on unstable" and run.verification == "It passed 40 loop(s) on unstable."
    # The session works on unstable's tree as it is, shown the release branch's fix.
    task = _adapted["task"]
    assert _adapted["head"] == run.sha and _adapted["value"] == "other\n"
    assert task == engine_mod.upstream_task(_task(), run.sha)
    assert task.failure.base_branch == "unstable" and task.policy is FixPolicy.GENERAL
    assert not task.target.fix_branch and not task.target.upstream_branch
    assert "The fix made on `8.0`" in _adapted["context"].release_fix
    assert patch in _adapted["context"].release_fix and "does not apply to `unstable` as it is" in _adapted["context"].release_fix
    assert _adapted["kwargs"]["min_loops"] == engine_mod.UPSTREAM_MIN_LOOPS


def test_a_fix_that_fails_upstream_is_made_again_for_it(tmp_path, _no_isolation, _adapted) -> None:
    repo, patch = _repo(tmp_path, "before\n")

    candidate = _prepare_upstream(repo, patch, _failing)

    assert candidate.upstream is not None and candidate.upstream.adapted
    assert "it did not pass" in _adapted["context"].release_fix and _FAILED in _adapted["context"].release_fix
    # The session starts from unstable as it is, without the release branch's patch.
    assert _adapted["value"] == "before\n"


def test_no_fix_for_unstable_is_a_refusal(tmp_path, _no_isolation, _adapted) -> None:
    repo, patch = _repo(tmp_path, "other\n")
    _adapted["made"] = FixCandidate(action=CandidateAction.REFUSE, summary="Unstable has no such race.")

    candidate = _prepare_upstream(repo, patch, _passing)

    assert candidate.upstream is None
    assert candidate.upstream_refusal == (
        "`unstable` has the failing test, but no fix for it could be made there. Unstable has no such race."
    )


def test_unstables_job_is_read_from_its_own_workflow_at_its_tip() -> None:
    task = engine_mod.upstream_task(replace(_task(), failure=replace(_task().failure, workflow_sha="f" * 40)), _UNSTABLE)

    assert task.failure.base_branch == "unstable" and task.failure.head_sha == _UNSTABLE
    assert not task.failure.workflow_sha and task.failure.job_name == "test-ubuntu-jemalloc"
    assert task.target.kind is TargetKind.DRAFT_PR and task.target.expected_sha == _UNSTABLE


def test_on_unstable_the_tests_the_agent_names_for_the_failing_one_are_run(tmp_path, _no_isolation) -> None:
    repo, _patch = _repo(tmp_path, "before\n")
    variant = FailedTest("Replica keeps its shard_id - drop_nack 1", _TEST.file)
    candidate = _candidate(upstream=None, failing_tests=(variant,))

    # A name the file builds from variables is run on unstable, where the release branch's name may not be.
    plan, tests = engine_mod._named_plan(_task(), repo, candidate, FixContext(release_fix="the fix"), None)
    assert tests == (variant,) and plan.tests == (variant,)
    # Elsewhere, a test code could name from the log is the one run.
    assert engine_mod._named_plan(_task(), repo, candidate, FixContext(), None) is None


def test_too_few_loops_upstream_is_a_failed_attempt(tmp_path, _no_isolation, monkeypatch) -> None:
    repo, patch = _repo(tmp_path, "before\n")
    monkeypatch.setattr(engine_mod, "_loops_that_fit", lambda *_a: 3)

    with pytest.raises(RuntimeError, match="Only 3 loop"):
        _prepare_upstream(repo, patch, _passing)


def test_only_an_open_pull_request_the_agent_was_shown_is_kept(tmp_path) -> None:
    repo, _patch = _repo(tmp_path, "before\n")
    head = git(Path(repo), "rev-parse", "HEAD")
    task = _task(open_fixes=(OpenFix(number=2283, title="Fix it"),))
    refusal = FixCandidate(action=CandidateAction.REFUSE, open_fix=2283)

    assert engine_mod._checked_candidate(task, repo, head, refusal).open_fix == 2283
    assert engine_mod._checked_candidate(task, repo, head, replace(refusal, open_fix=9)).open_fix == 0


@pytest.mark.parametrize(("value", "number"), [(2283, 2283), ("#2283", 2283), ("", 0), (None, 0), (True, 0), (-1, 0)])
def test_the_agents_open_fix_is_a_number(value, number) -> None:
    assert _candidate_from_payload({"action": "refuse", "open_fix": value}).open_fix == number


# --- publication --------------------------------------------------------------


def _gh(*, earlier=(), open_count: int = 0):
    gh, repo = MagicMock(), MagicMock()
    gh.get_repo.return_value = repo
    gh.search_issues.return_value.totalCount = open_count
    repo.get_pulls.return_value = list(earlier)
    repo.get_git_ref.side_effect = GithubException(404, {"message": "Not Found"})
    return gh, repo


@pytest.fixture
def _no_detector_issues(monkeypatch):
    monkeypatch.setattr(upstream, "_open_detector_issues", lambda _repo: [])


def test_a_fix_that_passed_upstream_opens_a_pull_request_into_it(monkeypatch, _no_detector_issues) -> None:
    gh, _repo = _gh()
    pushed = {}
    monkeypatch.setattr(upstream, "push_candidate", lambda candidate, target, *, git_env: pushed.update(
        target=target) or "d" * 40)
    create = MagicMock(return_value=SimpleNamespace(number=9, html_url="https://example/pull/9"))
    monkeypatch.setattr(upstream, "create_pull_from_push_repo", create)

    result = publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert result.kind is ResultKind.OPENED and result.open_upstream and result.pr_number == 9
    target = pushed["target"]
    assert target.kind is TargetKind.DRAFT_PR and target.expected_sha == _UNSTABLE
    assert target.branch == upstream.branch_for((_TEST,)) and target.branch.startswith("agent/ci-fix/upstream/")
    kwargs = create.call_args.kwargs
    assert kwargs["base_branch"] == "unstable" and kwargs["draft"] is False
    assert kwargs["title"] == "Wait for the shard_id to settle"
    body = kwargs["body"]
    assert "`Replica keeps its shard_id` failed in `test-ubuntu-jemalloc` on `8.0`" in body
    assert "It passed 40 loops there." in body
    marked = json.loads(upstream._FIX_MARKER_RE.search(body).group(1))
    assert marked == {"branch": "8.0", "run": 100, "attempt": 1, "job": 55, "name": "test-ubuntu-jemalloc",
                      "workflow": "daily", "scheduled": True}


def test_a_fix_made_for_unstables_code_is_the_one_pushed_there(monkeypatch, _no_detector_issues) -> None:
    gh, _repo = _gh()
    pushed = {}
    monkeypatch.setattr(upstream, "push_candidate", lambda candidate, target, *, git_env: pushed.update(
        candidate=candidate) or "d" * 40)
    create = MagicMock(return_value=SimpleNamespace(number=9, html_url=""))
    monkeypatch.setattr(upstream, "create_pull_from_push_repo", create)
    adapted = UpstreamRun(
        sha=_UNSTABLE, tests=(_TEST,), verification="It passed 40 loop(s) on unstable.",
        patch="diff --git a/y b/y\n", changed_paths=("tests/unit/cluster/y.tcl",),
        title="Wait for the shard_id in unstable's test", reasoning="Unstable's test waits the same way.",
    )

    publish_mod.publish_candidate(gh, _task(), _candidate(upstream=adapted), git_env={})

    candidate = pushed["candidate"]
    assert candidate.patch == "diff --git a/y b/y\n" and candidate.changed_paths == ("tests/unit/cluster/y.tcl",)
    kwargs = create.call_args.kwargs
    assert kwargs["title"] == "Wait for the shard_id in unstable's test"
    body = kwargs["body"]
    assert "The fix made on `8.0` does not apply here as it is, so this is the same fix written for" in body
    assert "**The fix:** Unstable's test waits the same way." in body
    # The release branch's fix is shown too, for the backport.
    assert "<details><summary>The fix made on <code>8.0</code></summary>" in body and "diff --git a/x b/x" in body


def test_a_detector_issue_for_the_test_is_closed_by_the_pull_request(monkeypatch) -> None:
    gh, _repo = _gh()
    issue = SimpleNamespace(number=2600, failed_tests=(_TEST,))
    monkeypatch.setattr(upstream, "_open_detector_issues", lambda _repo: [(None, issue)])
    monkeypatch.setattr(upstream, "push_candidate", lambda *_a, **_k: "d" * 40)
    create = MagicMock(return_value=SimpleNamespace(number=9, html_url=""))
    monkeypatch.setattr(upstream, "create_pull_from_push_repo", create)

    publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert create.call_args.kwargs["body"].startswith("Fixes #2600\n")


def test_at_the_cap_the_fix_waits(monkeypatch, _no_detector_issues) -> None:
    gh, _repo = _gh(open_count=upstream.MAX_OPEN)
    push = MagicMock()
    monkeypatch.setattr(upstream, "push_candidate", push)

    result = publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert result.kind is ResultKind.FAILED and "waits" in result.summary
    push.assert_not_called()


def test_the_bots_open_pull_request_for_the_test_is_the_fix(monkeypatch) -> None:
    pull = SimpleNamespace(number=9, state="open", merged_at=None, body="Fixes it\n", edit=MagicMock())
    gh, _repo = _gh(earlier=(pull,))
    push = MagicMock()
    monkeypatch.setattr(upstream, "push_candidate", push)

    result = publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert result.kind is ResultKind.SUGGESTED and result.open_upstream and result.pr_number == 9
    push.assert_not_called()
    # 8.0 is added to the failures it fixes, for the merge to be told there too.
    assert '"branch": "8.0"' in pull.edit.call_args.kwargs["body"]


def test_a_merged_pull_request_for_the_test_is_to_be_backported(monkeypatch) -> None:
    pull = SimpleNamespace(number=9, state="closed", merged_at="2026-11-01", body="")
    gh, _repo = _gh(earlier=(pull,))

    result = publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert result.kind is ResultKind.SUGGESTED and not result.open_upstream and result.pr_number == 9


def test_a_closed_pull_request_for_the_test_turned_the_fix_down(monkeypatch) -> None:
    pull = SimpleNamespace(number=9, state="closed", merged_at=None, body="")
    gh, _repo = _gh(earlier=(pull,))

    result = publish_mod.publish_candidate(gh, _task(), _candidate(), git_env={})

    assert result.kind is ResultKind.REFUSED and "closed without merging" in result.summary


def test_a_fix_that_failed_upstream_is_refused() -> None:
    result = publish_mod.publish_candidate(
        MagicMock(), _task(), _candidate(upstream=None, upstream_refusal="It fails there."), git_env={},
    )
    assert result.kind is ResultKind.REFUSED and result.summary == "It fails there."


def test_an_open_pull_request_that_fixes_it_is_the_fix_to_backport() -> None:
    refusal = FixCandidate(action=CandidateAction.REFUSE, open_fix=2283, summary="#2283 fixes it.")

    result = publish_mod.publish_candidate(MagicMock(), _task(), refusal, git_env={})

    assert result.kind is ResultKind.SUGGESTED and result.open_upstream and result.pr_number == 2283
    assert render_result(result).startswith("#2283, open on `unstable`, fixes `test-ubuntu-jemalloc` ([run](")
    assert render_result(result).endswith("Backport it to `8.0` once it merges.")


def test_an_opened_pull_request_is_a_line_on_the_sweep_pr() -> None:
    result = FixResult(kind=ResultKind.OPENED, summary="s", task=_task(), pr_number=9, open_upstream=True)
    line = render_result(result)
    assert line.startswith("Opened #9 into `unstable` with a fix for `test-ubuntu-jemalloc` ([run](")
    assert line.endswith("Backport it to `8.0` once it merges.")


# --- the open pull requests a fix is shown ------------------------------------


def test_open_pull_requests_naming_the_test_or_closing_its_issue_are_shown(monkeypatch) -> None:
    gh, repo = MagicMock(), MagicMock()
    gh.get_repo.return_value = repo
    gh.search_issues.return_value = [SimpleNamespace(number=11)]
    issue = SimpleNamespace(number=2600, failed_tests=(_TEST,))
    monkeypatch.setattr(upstream, "_open_detector_issues", lambda _repo: [(issue, issue)])
    monkeypatch.setattr(upstream, "open_linked_prs", lambda _issue: [12])
    files = [SimpleNamespace(filename=_TEST.file, patch="@@ -1 +1 @@"), SimpleNamespace(filename="src/x.c", patch="")]
    pulls = {
        11: SimpleNamespace(state="open", base=SimpleNamespace(ref="unstable"), title="Fix reset", get_files=lambda: files),
        12: SimpleNamespace(state="open", base=SimpleNamespace(ref="9.0"), title="Other", get_files=lambda: files),
    }
    repo.get_pull.side_effect = pulls.__getitem__

    (shown,) = upstream.find_open_fixes(gh, _task(), (_TEST,))

    assert shown.number == 11 and shown.title == "Fix reset"
    assert shown.diff == f"--- a/{_TEST.file}\n+++ b/{_TEST.file}\n@@ -1 +1 @@"
    (query,) = [call.args[0] for call in gh.search_issues.call_args_list]
    assert query == 'repo:valkey-io/valkey is:pr is:open base:unstable "Replica keeps its shard_id"'


def test_open_pull_requests_that_cannot_be_listed_are_not_shown() -> None:
    gh = MagicMock()
    gh.search_issues.side_effect = RuntimeError("down")
    assert upstream.find_open_fixes(gh, _task(), (_TEST,)) == ()


# --- the review front door -----------------------------------------------------


def _review_pull(*, comments=(), reviews=(), review_comments=()):
    body = "Fixes it\n\n" + upstream._FIX_MARKER.format(json.dumps({
        "branch": "8.0", "run": 100, "attempt": 1, "job": 55, "name": "test-ubuntu-jemalloc",
        "workflow": "daily", "scheduled": True,
    }))
    return SimpleNamespace(
        number=9, state="open", user=SimpleNamespace(login=_BOT), body=body,
        head=SimpleNamespace(ref=upstream.branch_for((_TEST,)), sha="f" * 40,
                             repo=SimpleNamespace(full_name="valkey-io/valkey")),
        base=SimpleNamespace(ref="unstable"),
        get_issue_comments=lambda: list(comments), get_reviews=lambda: list(reviews),
        get_review_comments=lambda: list(review_comments), create_issue_comment=MagicMock(),
    )


def _comment(id_: int, login: str, body: str, **extra):
    return SimpleNamespace(id=id_, user=SimpleNamespace(login=login), body=body, **extra)


def test_a_maintainers_review_is_addressed_once(monkeypatch) -> None:
    monkeypatch.setattr(upstream, "is_authorized", lambda _gh, _org, _team, login: login == "maintainer")
    pull = _review_pull(
        reviews=[_comment(1, "maintainer", "Use wait_for_condition."), _comment(2, "maintainer", "")],
        review_comments=[_comment(3, "maintainer", "Too long a wait.", path=_TEST.file, line=12, original_line=12)],
        comments=[
            _comment(4, "someone", "Looks odd."),
            _comment(5, "coderabbitai[bot]", "Summary"),
            _comment(6, _BOT, "Done.\n\n" + upstream._ANSWERED.format(ids="i7")),
            _comment(7, "maintainer", "Answered already."),
        ],
    )
    gh = MagicMock()
    gh.get_repo.return_value.get_pull.return_value = pull

    plan = upstream.plan_request(gh, None, {"repo": "valkey-io/valkey", "pr": 9}, bot_login=_BOT)

    assert plan.front_door is FrontDoor.REVIEW
    task = plan.task
    assert task.target.kind is TargetKind.UPDATE_PR and task.target.branch == pull.head.ref
    assert task.target.expected_sha == "f" * 40 and not task.target.fix_branch
    assert task.failure.job_id == 55 and task.failure.run_id == 100 and task.failure.source is Source.DAILY
    assert task.failure.base_branch == "unstable" and task.failure.pr_number == 9
    assert "maintainer: Use wait_for_condition." in task.failure.hint
    assert f"maintainer on `{_TEST.file}`, line 12: Too long a wait." in task.failure.hint
    assert "Looks odd" not in task.failure.hint and "Answered already" not in task.failure.hint
    assert plan.markers == (upstream._ANSWERED.format(ids="r1,c3"),)

    upstream.report_result(gh, plan, FixResult(kind=ResultKind.PUSHED, summary="s", task=task, commit_sha="d" * 40))
    (answer,) = [call.args[0] for call in pull.create_issue_comment.call_args_list]
    assert answer.startswith(f"Changed the fix as the review asked: {'d' * 40}.")
    assert answer.endswith(upstream._ANSWERED.format(ids="r1,c3"))


def test_a_pull_request_without_a_review_to_address_is_skipped(monkeypatch) -> None:
    monkeypatch.setattr(upstream, "is_authorized", lambda *_a: True)
    gh = MagicMock()
    gh.get_repo.return_value.get_pull.return_value = _review_pull()

    result = upstream.plan_request(gh, None, {"repo": "valkey-io/valkey", "pr": 9}, bot_login=_BOT)

    assert result == {"repo": "valkey-io/valkey", "pr": 9, "action": "skipped", "reason": "no unanswered review"}


def test_a_pull_request_not_the_bots_is_never_planned() -> None:
    pull = _review_pull()
    pull.user = SimpleNamespace(login="someone")
    gh = MagicMock()
    gh.get_repo.return_value.get_pull.return_value = pull

    result = upstream.plan_request(gh, None, {"repo": "valkey-io/valkey", "pr": 9}, bot_login=_BOT)

    assert result["action"] == "skipped"


# --- upkeep -------------------------------------------------------------------


def _upkeep_gh(open_pulls=(), closed_pulls=(), *, behind: int = 0):
    gh, repo = MagicMock(), MagicMock()
    gh.get_repo.return_value = repo
    repo.default_branch = "unstable"
    pulls = {pull.number: pull for pull in (*open_pulls, *closed_pulls)}
    repo.get_pull.side_effect = pulls.__getitem__

    def search(query):
        listed = open_pulls if "is:open" in query else closed_pulls
        return [SimpleNamespace(number=pull.number) for pull in listed]

    gh.search_issues.side_effect = search
    repo.compare.return_value = SimpleNamespace(behind_by=behind)
    return gh


def test_upkeep_rebases_a_pull_request_that_fell_behind(monkeypatch) -> None:
    monkeypatch.setattr(upstream, "is_authorized", lambda *_a: True)
    pull = _review_pull()
    pull.node_id = "PR_1"
    graphql = MagicMock()

    report = upstream.upkeep(_upkeep_gh((pull,), behind=3), "valkey-io/valkey", graphql=graphql,
                             org="valkey-io", team="contributors", dry_run=False)

    assert report["open"] == {9: "rebased"} and report["review"] == []
    assert graphql.execute.call_args.args[1] == {"id": "PR_1", "head": "f" * 40}


def test_upkeep_lists_a_pull_request_with_a_review_to_address(monkeypatch) -> None:
    monkeypatch.setattr(upstream, "is_authorized", lambda *_a: True)
    pull = _review_pull(comments=[_comment(4, "maintainer", "Please shorten the wait.")])

    report = upstream.upkeep(_upkeep_gh((pull,)), "valkey-io/valkey", graphql=MagicMock(),
                             org="valkey-io", team="contributors", dry_run=False)

    assert report["open"] == {9: "up to date"} and report["review"] == [9]


def test_upkeep_tells_the_sweep_pr_once_when_its_fix_merged(monkeypatch) -> None:
    pull = _review_pull()
    pull.state, pull.merged_at = "closed", "2026-11-10"
    comments: list = []
    sweep = SimpleNamespace(number=5077, get_issue_comments=lambda: list(comments),
                            create_issue_comment=lambda body: comments.append(SimpleNamespace(body=body)))
    monkeypatch.setattr(upstream, "find_existing_pr", lambda _gh, _repo, _push, branch: (
        sweep if branch == "agent/backport/sweep/8.0" else None))
    gh = _upkeep_gh(closed_pulls=(pull,))

    for _ in range(2):
        report = upstream.upkeep(gh, "valkey-io/valkey", graphql=None, org="o", team="t", dry_run=False)

    assert report["closed"] == {9: []}
    (told,) = comments
    assert told.body.startswith("#9, merged into `unstable`, fixes `test-ubuntu-jemalloc`. Backport it to `8.0`.")
