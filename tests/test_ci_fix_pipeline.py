"""Tests for the CI-fix engine: diagnosis routing and the READY decision.

The engine never writes to GitHub. Every path ends in a ``FixOutcome``: READY
(an approved patch or port commit for publication) or a refusal/handoff.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from scripts.ci_fix.apply import ApplyResult
from scripts.ci_fix.models import (
    FailureType,
    FixPath,
    FixProposal,
    FixRequest,
    OutcomeKind,
    Policy,
    Publication,
    ReviewVerdict,
    RunResult,
)
from scripts.ci_fix.pipeline import _recent_changes, run_ci_fix_request
from scripts.ci_fix.port_discovery import PortCandidate
from scripts.ci_fix.review import LoopResult, PatchReview
from scripts.ci_fix.verify.base import FailedJob, VerificationResult, VerifyEnv
from scripts.ci_fix.verify.workflow_env import JobEnvironment

_FULL_PORT_SHA = "9f374e15848d7b070cdd58a071a741c0a59a6c75"


def _proposal(path: FixPath = FixPath.AUTHOR, **overrides) -> FixProposal:
    values = dict(
        path=path, failing_check="corrupt payload: zset listpack with NAN score",
        root_cause="payload embeds RDB v80; branch is v11", reasoning="scaffolding fix",
        confidence=0.9, failing_job_hint="test-ubuntu-latest",
        build_command="make", verify_command="./runtest --single x",
    )
    values.update(overrides)
    return FixProposal(**values)


def _request(**overrides) -> FixRequest:
    values = dict(
        repo_full_name="valkey-io/valkey", pr_number=3988,
        head_repo_full_name="valkey-io/valkey", head_branch="agent/backport/sweep/8.0",
        head_sha="a" * 40, run_id=123, requested_by="bot", base_branch="8.0",
    )
    values.update(overrides)
    return FixRequest(**values)


def _passed_run() -> RunResult:
    return RunResult(ran=True, passed=True, exit_code=0,
                     command="make && ./runtest --single x", output_tail="All tests passed")


def _loop_success(**overrides) -> LoopResult:
    values = dict(success=True, run_result=_passed_run(), review=ReviewVerdict(True, "ok"),
                  changed_paths=("test.tcl",), attempts=1, detail="ok", patch="the diff")
    values.update(overrides)
    return LoopResult(**values)


def _artifact_client(logs):
    client = MagicMock()
    client.download_run_logs.return_value = logs
    return client


def _engine(monkeypatch, *, request=None, logs=None, diagnose=None, loop=None, clone=None,
            classify=None, discover=None, commits=None, **kwargs):
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", clone or (lambda *a, **k: True))
    monkeypatch.setattr(
        "scripts.ci_fix.pipeline._classify_failing_job",
        classify or (lambda *a, **k: JobEnvironment(VerifyEnv.LOCAL)),
    )
    monkeypatch.setattr("scripts.ci_fix.pipeline.discover_port_candidates", discover or (lambda *a, **k: ()))
    monkeypatch.setattr("scripts.ci_fix.pipeline.commits_in_range", commits or (lambda *a, **k: ()))
    return run_ci_fix_request(
        MagicMock(),
        request=request or _request(),
        artifact_client=_artifact_client({"1.txt": b"err"} if logs is None else logs),
        failed_jobs=("test-ubuntu-latest",),
        diagnose_func=diagnose or (lambda *a, **k: _proposal()),
        run_loop_func=loop or (lambda *a, **k: _loop_success()),
        **kwargs,
    )


# --- basic outcomes -----------------------------------------------------------

def test_engine_returns_the_approved_patch_without_pushing(monkeypatch):
    outcome = _engine(monkeypatch)
    assert outcome.kind is OutcomeKind.READY
    assert outcome.patch == "the diff"
    assert outcome.changed_paths == ("test.tcl",)
    assert outcome.verify_backend == "local"
    assert outcome.commit_sha == ""
    assert outcome.failing_run_url.endswith("/actions/runs/123")


def test_engine_refuses_when_logs_expired(monkeypatch):
    outcome = _engine(monkeypatch, logs={})
    assert outcome.kind is OutcomeKind.REFUSED
    assert "expired" in outcome.summary


def test_engine_fails_when_clone_fails(monkeypatch):
    outcome = _engine(monkeypatch, clone=lambda *a, **k: False)
    assert outcome.kind is OutcomeKind.FAILED
    assert "clone" in outcome.summary.lower()


def test_engine_clones_a_pr_head_by_pull_number(monkeypatch):
    """A fork PR's head commit is only reachable through refs/pull/<n>/head."""
    seen = {}

    def clone(repo, dest, sha, **kwargs):
        seen.update(repo=repo, sha=sha, **kwargs)
        return True

    _engine(monkeypatch, clone=clone)
    assert seen == {"repo": "valkey-io/valkey", "sha": "a" * 40, "pull_number": 3988}


def test_engine_reports_a_refusal_with_its_triage(monkeypatch):
    refusal = _proposal(FixPath.REFUSE, reasoning="genuinely flaky; no safe fix",
                        failure_type=FailureType.FLAKY)
    outcome = _engine(monkeypatch, diagnose=lambda *a, **k: refusal)
    assert outcome.kind is OutcomeKind.REFUSED
    assert outcome.summary == "genuinely flaky; no safe fix"
    assert outcome.proposal is refusal


def test_engine_refuses_when_the_loop_fails(monkeypatch):
    failed = LoopResult(success=False, run_result=None, review=None,
                        changed_paths=(), attempts=3, detail="test still failing")
    outcome = _engine(monkeypatch, loop=lambda *a, **k: failed)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "still failing" in outcome.summary


def test_engine_hands_off_an_unverifiable_fix(monkeypatch):
    handoff = LoopResult(
        success=False, run_result=None, review=ReviewVerdict(True, "looks sane"),
        changed_paths=("src/x.c",), attempts=1,
        detail="could not verify the fix here (missing dep); handing off",
        handoff=True, handoff_patch="--- a/src/x.c\n+++ b/src/x.c\n",
    )
    outcome = _engine(monkeypatch, loop=lambda *a, **k: handoff)
    assert outcome.kind is OutcomeKind.HANDOFF
    assert outcome.handoff_patch == "--- a/src/x.c\n+++ b/src/x.c\n"
    assert outcome.patch == ""


# --- what reaches the loop ------------------------------------------------------

def test_engine_passes_image_runs_and_policy_to_the_loop(monkeypatch):
    seen = {}

    def loop(_repo_dir, _proposal, **kwargs):
        seen.update(kwargs)
        return _loop_success()

    outcome = _engine(
        monkeypatch, loop=loop, verify_runs=5,
        request=_request(policy=Policy.FIX),
        classify=lambda *a, **k: JobEnvironment(VerifyEnv.DOCKER, image="almalinux:8"),
    )
    assert outcome.kind is OutcomeKind.READY
    assert outcome.verify_backend == "docker:almalinux:8"
    assert seen == {"container_image": "almalinux:8", "verify_runs": 5, "policy": Policy.FIX}


def test_engine_refuses_an_unsupported_environment(monkeypatch):
    outcome = _engine(
        monkeypatch,
        classify=lambda *a, **k: JobEnvironment(VerifyEnv.UNSUPPORTED, reason="self-hosted arm"),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "self-hosted arm" in outcome.summary


def test_engine_refuses_a_job_that_did_not_fail(monkeypatch):
    outcome = _engine(monkeypatch, diagnose=lambda *a, **k: _proposal(failing_job_hint="other"))
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not among the failed jobs" in outcome.summary


def _two_failed_jobs(monkeypatch, request, hint):
    monkeypatch.setattr("scripts.ci_fix.pipeline.shallow_clone_at_sha", lambda *a, **k: True)
    classified = []
    monkeypatch.setattr("scripts.ci_fix.pipeline._classify_failing_job",
                        lambda _repo, job: classified.append(job) or JobEnvironment(VerifyEnv.LOCAL))
    monkeypatch.setattr("scripts.ci_fix.pipeline.commits_in_range", lambda *a, **k: ())
    monkeypatch.setattr("scripts.ci_fix.pipeline.failed_jobs_for_run", lambda *a, **k: [
        FailedJob("job-A", "failure", 1), FailedJob("job-B", "failure", 2)])
    loop = MagicMock(return_value=_loop_success())
    outcome = run_ci_fix_request(
        MagicMock(), request=request, artifact_client=_artifact_client({"1.txt": b"err"}),
        diagnose_func=lambda *a, **k: _proposal(failing_job_hint=hint), run_loop_func=loop,
    )
    return outcome, loop, classified


def test_engine_verifies_only_the_job_the_front_door_selected(monkeypatch):
    outcome, loop, classified = _two_failed_jobs(monkeypatch, _request(job="job-A"), hint="job-B")
    assert outcome.kind is OutcomeKind.REFUSED
    assert "'job-B' is not among the failed jobs of the linked run (job-A)" in outcome.summary
    assert classified == []
    loop.assert_not_called()

    outcome, loop, classified = _two_failed_jobs(monkeypatch, _request(job="job-A"), hint="job-A")
    assert outcome.kind is OutcomeKind.READY
    assert classified == ["job-A"]


def test_without_a_selected_job_any_failed_job_may_be_verified(monkeypatch):
    outcome, _loop, classified = _two_failed_jobs(monkeypatch, _request(), hint="job-B")
    assert outcome.kind is OutcomeKind.READY
    assert classified == ["job-B"]


def test_engine_refuses_a_selected_job_that_is_no_longer_failed(monkeypatch):
    outcome, loop, _classified = _two_failed_jobs(monkeypatch, _request(job="job-C"), hint="job-C")
    assert outcome.kind is OutcomeKind.REFUSED
    assert "'job-C' is not a failed job" in outcome.summary
    loop.assert_not_called()


def test_engine_passes_request_and_recent_changes_to_diagnosis(monkeypatch):
    seen = {}

    def diagnose(_logs, _repo, **kwargs):
        seen.update(kwargs)
        return _proposal()

    commit = PortCandidate(sha="c" * 40, subject="Backport tls change (#4100)", paths=("src/tls.c",))
    request = _request()
    _engine(monkeypatch, request=request, diagnose=diagnose, commits=lambda *a, **k: (commit,))
    assert seen["request"] is request
    assert "cccccccccccc Backport tls change (#4100) [src/tls.c]" in seen["recent_changes"]


# --- PORT -------------------------------------------------------------------------

def test_port_is_ready_with_the_full_discovered_sha(monkeypatch):
    loop = MagicMock(side_effect=AssertionError("PORT must not run the local verifier"))
    classify = MagicMock(side_effect=AssertionError("PORT does not need env classification"))
    outcome = _engine(
        monkeypatch, loop=loop, classify=classify,
        diagnose=lambda *a, **k: _proposal(FixPath.PORT, unstable_fix_commit="9f374e15848d"),
        discover=lambda *a, **k: (PortCandidate(sha=_FULL_PORT_SHA, subject="the upstream fix",
                                                paths=(".github/workflows/daily.yml", "src/x.c")),),
    )
    assert outcome.kind is OutcomeKind.READY
    assert outcome.port_commit == _FULL_PORT_SHA
    assert outcome.changed_paths == (".github/workflows/daily.yml", "src/x.c")
    assert outcome.verify_backend == "upstream-port"
    loop.assert_not_called()
    classify.assert_not_called()


def test_port_refuses_a_sha_that_was_not_discovered(monkeypatch):
    outcome = _engine(
        monkeypatch,
        diagnose=lambda *a, **k: _proposal(FixPath.PORT, unstable_fix_commit="9f374e15848d"),
        discover=lambda *a, **k: (PortCandidate(sha="1" * 40, subject="unrelated"),),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not among the fixes discovered" in outcome.summary


def test_port_refuses_an_ambiguous_short_sha(monkeypatch):
    outcome = _engine(
        monkeypatch,
        diagnose=lambda *a, **k: _proposal(FixPath.PORT, unstable_fix_commit="9f374e1"),
        discover=lambda *a, **k: (
            PortCandidate(sha=_FULL_PORT_SHA, subject="fix a"),
            PortCandidate(sha="9f374e1a" + "0" * 32, subject="fix b"),
        ),
    )
    assert outcome.kind is OutcomeKind.REFUSED


def test_port_without_execution_skips_the_job_check(monkeypatch):
    """The issue flow names the job itself and verifies in CI, so any hint is fine."""
    outcome = _engine(
        monkeypatch, request=_request(execute=False),
        diagnose=lambda *a, **k: _proposal(
            FixPath.PORT, unstable_fix_commit=_FULL_PORT_SHA, failing_job_hint=""),
        discover=lambda *a, **k: (PortCandidate(sha=_FULL_PORT_SHA, subject="fix"),),
    )
    assert outcome.kind is OutcomeKind.READY


# --- culprit ------------------------------------------------------------------------

def test_culprit_is_reported_only_when_it_is_in_the_listed_commits(monkeypatch):
    listed = PortCandidate(sha="c" * 40, subject="Backport tls change (#4100)")
    outcome = _engine(
        monkeypatch, commits=lambda *a, **k: (listed,),
        diagnose=lambda *a, **k: _proposal(culprit_commit="cccccccccccc"),
    )
    assert outcome.culprit_sha == "c" * 40
    assert outcome.culprit_subject == "Backport tls change (#4100)"


def test_culprit_outside_the_listed_commits_is_dropped(monkeypatch):
    outcome = _engine(
        monkeypatch, commits=lambda *a, **k: (PortCandidate(sha="c" * 40, subject="x"),),
        diagnose=lambda *a, **k: _proposal(culprit_commit="deadbeefdead"),
    )
    assert outcome.culprit_sha == ""


def test_culprit_is_reported_on_refusals_too(monkeypatch):
    listed = PortCandidate(sha="c" * 40, subject="Add the flaky helper (#4200)")
    outcome = _engine(
        monkeypatch, commits=lambda *a, **k: (listed,),
        diagnose=lambda *a, **k: _proposal(FixPath.REFUSE, reasoning="product bug",
                                           culprit_commit="c" * 40),
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert outcome.culprit_sha == "c" * 40


def test_recent_changes_default_to_the_prs_own_commits(monkeypatch):
    seen = []
    monkeypatch.setattr("scripts.ci_fix.pipeline.commits_in_range",
                        lambda _repo, rev_range, **k: seen.append(rev_range) or ())
    _recent_changes("/repo", _request(base_branch="8.0"))
    assert seen == ["origin/8.0..HEAD"]


def test_recent_changes_for_a_daily_issue_split_before_and_after_the_failure(monkeypatch):
    seen = []
    before = PortCandidate(sha="d" * 40, subject="introduced it")
    after = PortCandidate(sha="e" * 40, subject="landed later")
    ranges = {"p" * 40 + ".." + "f" * 40: (before,), "f" * 40 + "..HEAD": (after,)}
    monkeypatch.setattr("scripts.ci_fix.pipeline.commits_in_range",
                        lambda _repo, rev_range, **k: seen.append(rev_range) or ranges[rev_range])
    request = _request(publication=Publication.NEW_PR, culprit_range="p" * 40 + ".." + "f" * 40,
                       failing_sha="f" * 40, head_sha="t" * 40)
    suspects, text = _recent_changes("/repo", request)
    assert seen == ["p" * 40 + ".." + "f" * 40, "f" * 40 + "..HEAD"]
    assert suspects == (before,)  # only what landed before the failure can have caused it
    culprits, later = text.split("cannot have caused it")
    assert "introduced it" in culprits and "landed later" not in culprits
    assert "landed later" in later
    assert "between the previous Daily run and the failing run" in text


# --- no execution (fork PRs, Daily issues) ------------------------------------------

def _unexecuted(monkeypatch, *, applies, reviews):
    monkeypatch.setattr("scripts.ci_fix.pipeline.reset_worktree", lambda *a, **k: None)
    apply_calls = []

    def apply(_repo, _proposal, **kwargs):
        apply_calls.append(kwargs)
        return applies.pop(0)

    monkeypatch.setattr("scripts.ci_fix.pipeline.apply_fix", apply)
    def review(*_a, **kwargs):
        assert kwargs["policy"] is Policy.FIX
        return reviews.pop(0)

    monkeypatch.setattr("scripts.ci_fix.pipeline.build_and_review_patch", review)
    loop = MagicMock(side_effect=AssertionError("nothing may run without execution"))
    classify = MagicMock(side_effect=AssertionError("no environment is needed"))
    outcome = _engine(monkeypatch, request=_request(execute=False, policy=Policy.FIX),
                      loop=loop, classify=classify)
    return outcome, apply_calls


def test_unexecuted_fix_is_ready_after_review_without_running_anything(monkeypatch):
    outcome, calls = _unexecuted(
        monkeypatch, applies=[ApplyResult(True, ("tests/x.tcl",))],
        reviews=[PatchReview(ok=True, patch="diff", review=ReviewVerdict(True, "sound"))],
    )
    assert outcome.kind is OutcomeKind.READY
    assert outcome.patch == "diff"
    assert outcome.verify_backend == ""
    assert calls[0]["policy"] is Policy.FIX


def test_unexecuted_fix_retries_on_review_feedback(monkeypatch):
    outcome, calls = _unexecuted(
        monkeypatch,
        applies=[ApplyResult(True, ("tests/x.tcl",)), ApplyResult(True, ("tests/x.tcl",))],
        reviews=[
            PatchReview(ok=False, patch="bad", review=ReviewVerdict(False, "weakens the check"),
                        detail="review rejected the fix: weakens the check"),
            PatchReview(ok=True, patch="good", review=ReviewVerdict(True, "sound")),
        ],
    )
    assert outcome.kind is OutcomeKind.READY
    assert outcome.patch == "good"
    assert "weakens the check" in calls[1]["feedback"]


def test_unexecuted_fix_reports_why_the_agent_declined(monkeypatch):
    outcome, _calls = _unexecuted(
        monkeypatch, applies=[ApplyResult(False, (), "only fix removes the assertion")], reviews=[],
    )
    assert outcome.kind is OutcomeKind.REFUSED
    assert outcome.summary == "fix not applied: only fix removes the assertion"


# --- macOS --------------------------------------------------------------------------

def _macos(monkeypatch, verifier, *, reset=None, apply=None, request=None):
    monkeypatch.setattr("scripts.ci_fix.pipeline.reset_worktree", reset or (lambda *a, **k: None))
    monkeypatch.setattr("scripts.ci_fix.pipeline.apply_fix",
                        apply or (lambda *a, **k: ApplyResult(True, ("test.tcl",))))
    monkeypatch.setattr("scripts.ci_fix.pipeline.build_and_review_patch",
                        lambda *a, **k: PatchReview(ok=True, patch="diff\n", review=ReviewVerdict(True, "ok")))
    return _engine(monkeypatch, macos_verifier=verifier, request=request,
                   classify=lambda *a, **k: JobEnvironment(VerifyEnv.MACOS))


def test_macos_fixes_use_the_requests_policy(monkeypatch):
    policies = []

    def apply(_repo, _proposal, **kwargs):
        policies.append(kwargs["policy"])
        return ApplyResult(True, ("test.tcl",))

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/9")
    _macos(monkeypatch, verifier, apply=apply, request=_request(head_branch="agent/ci-fix/x", policy=Policy.FIX))
    assert policies == [Policy.FIX]


def test_macos_green_is_ready_with_its_run(monkeypatch):
    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/9")
    outcome = _macos(monkeypatch, verifier)
    assert outcome.kind is OutcomeKind.READY
    assert outcome.verify_backend == "macos"
    assert outcome.macos_run_url == "https://run/9"
    assert outcome.patch == "diff\n"


def test_macos_cleanup_failure_preserves_the_decision(monkeypatch):
    calls = {"n": 0}

    def reset(*_a, **_k):
        calls["n"] += 1
        if calls["n"] > 1:
            raise RuntimeError("git reset failed")

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(verified=True, ran=True, detail="ok", run_url="https://run/9")
    assert _macos(monkeypatch, verifier, reset=reset).kind is OutcomeKind.READY


def test_macos_red_retries_with_the_failed_log_then_gives_up(monkeypatch):
    from scripts.ci_fix.pipeline import _MACOS_FIX_MAX_ATTEMPTS

    feedback = []

    def apply(_repo, _proposal, **kwargs):
        feedback.append(kwargs["feedback"])
        return ApplyResult(True, ("test.tcl",))

    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(
        verified=False, ran=True, detail="did not pass", run_url="https://run/1",
        output_tail="unit/test_vset.c:137:25: error: variable length array folded",
    )
    outcome = _macos(monkeypatch, verifier, apply=apply)
    assert outcome.kind is OutcomeKind.REFUSED
    assert verifier.verify.call_count == _MACOS_FIX_MAX_ATTEMPTS
    assert feedback[0] == ""
    assert "unit/test_vset.c:137" in feedback[1] and "https://run/1" in feedback[1]


def test_macos_without_a_verifier_refuses(monkeypatch):
    outcome = _macos(monkeypatch, None)
    assert outcome.kind is OutcomeKind.REFUSED
    assert "not configured" in outcome.summary


# --- job classification over real workflow files --------------------------------------

def _write_workflows(tmp_path, files):
    wf = tmp_path / ".github" / "workflows"
    wf.mkdir(parents=True)
    for name, body in files.items():
        (wf / name).write_text(body)
    return tmp_path


def test_classify_finds_a_matrix_named_container_job(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job

    repo = _write_workflows(tmp_path, {"daily.yml": (
        "jobs:\n  test-rpm:\n    strategy:\n      matrix:\n        include:\n"
        "          - name: test-almalinux8\n            container: almalinux:8\n"
        "    name: ${{ matrix.name }}\n    runs-on: ubuntu-latest\n"
        "    container: ${{ matrix.container }}\n    steps:\n      - run: make\n"
    )})
    env = _classify_failing_job(repo, "test-almalinux8")
    assert (env.env, env.image) == (VerifyEnv.DOCKER, "almalinux:8")


def test_classify_ambiguous_cross_workflow_refuses(tmp_path):
    from scripts.ci_fix.pipeline import _classify_failing_job

    repo = _write_workflows(tmp_path, {
        "a.yml": "jobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n",
        "b.yml": "jobs:\n  test:\n    runs-on: macos-latest\n    steps:\n      - run: make\n",
    })
    env = _classify_failing_job(repo, "test")
    assert env.env is VerifyEnv.UNSUPPORTED
    assert "multiple workflows" in env.reason


def test_match_failed_job_rules():
    from scripts.ci_fix.pipeline import _match_failed_job

    assert _match_failed_job("build", ("build", "lint")) == "build"
    assert _match_failed_job("test", ("test (clang)",)) == "test (clang)"
    assert _match_failed_job("test", ("test (a)", "test (b)")) is None
    assert _match_failed_job("other", ("build",)) is None


def test_read_workflow_safely_skips_symlink_and_oversized(tmp_path):
    from scripts.ci_fix.pipeline import _MAX_WORKFLOW_BYTES, read_workflow_safely

    good = tmp_path / "ok.yml"
    good.write_text("jobs: {}\n")
    assert read_workflow_safely(good) == "jobs: {}\n"
    big = tmp_path / "big.yml"
    big.write_text("x" * (_MAX_WORKFLOW_BYTES + 1))
    assert read_workflow_safely(big) is None
    link = tmp_path / "link.yml"
    link.symlink_to(good)
    assert read_workflow_safely(link) is None
    assert read_workflow_safely(tmp_path / "missing.yml") is None




def test_a_daily_request_without_a_range_lists_no_suspects(monkeypatch):
    """The fallback to the PR's own commits must not apply to a Daily fix branch."""
    from scripts.ci_fix import pipeline as pipeline_mod

    seen = []
    monkeypatch.setattr(pipeline_mod, "commits_in_range", lambda _dir, rng: seen.append(rng) or ())
    request = _request(publication=Publication.NEW_PR, culprit_range="", base_branch="unstable")
    assert pipeline_mod._recent_changes("/repo", request) == ((), "")
    assert all("origin/unstable..HEAD" != rng for rng in seen)
