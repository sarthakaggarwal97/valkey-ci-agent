"""Verify a fix by rerunning the failing job in the project's own Daily workflow.

A flaky failure cannot be proven fixed by one green run, and many Daily jobs
(valgrind, sanitizers, macOS, FreeBSD, arm, distro containers) cannot be
reproduced on the agent's runner at all. So a Daily issue fix is verified where
the failure happened: the repository's ``daily.yml`` is dispatched twice, on
the unfixed base and on the candidate, running only the failing job and only
the failing test file, ``--loops N`` times.

Everything is derived from the workflow file at dispatch time rather than
hardcoded: which ``skipjobs`` / ``skiptests`` tokens keep the target job and
test suite, and whether valgrind jobs take a ``valgrind_test`` input. A job or
test the workflow cannot isolate is refused rather than approximated.

Evaluation is factual. The target job must succeed, and for a test failure its
log must show the test actually ran (``[ok]: <name>``) - a green job that never
executed the test proves nothing. A fix that makes the test skip in that job is
reported as such.
"""

from __future__ import annotations

import itertools
import json
import logging
import re
import shlex
from dataclasses import asdict, dataclass, replace
from pathlib import PurePosixPath
from typing import Any

from scripts.ci_fix.verify.gha_if import UnsupportedExpression, evaluate
from scripts.ci_fix.verify.workflow_env import load_workflow, resolve_job
from scripts.common.github_client import retry_github_call
from scripts.common.polling import env_int
from scripts.common.text_utils import strip_ansi
from scripts.common.workflow_artifacts import ArtifactClient

logger = logging.getLogger(__name__)

DAILY_WORKFLOW = "daily.yml"
DEFAULT_LOOPS = 20
MAX_LOOPS = 100
# A valgrind run of one test file is roughly ten times slower; fewer loops keep
# it inside the job timeout while still exercising the slow environment.
VALGRIND_LOOPS = 5
# The API version whose dispatch response carries the new run's id.
_DISPATCH_API_VERSION = "2026-03-10"

_REQUIRED_INPUTS = ("skipjobs", "skiptests", "test_args", "use_repo", "use_git_ref")
_SKIPJOBS_RE = re.compile(r"contains\(\s*github\.event\.inputs\.skipjobs\s*,\s*'([^']+)'\s*\)")
_SKIPTESTS_RE = re.compile(r"contains\(\s*github\.event\.inputs\.skiptests\s*,\s*'([^']+)'\s*\)")
# A step that hands skiptests to a script and branches on it in shell, e.g.
#   case "${SKIPTESTS:-}" in *valkey*) ;; *) ./runtest ... ;; esac
_SHELL_SKIP_RE = re.compile(r"^\s*\*([a-z][a-z-]*)\*\)\s*;;", re.MULTILINE)
# The dispatch inputs are pasted into the workflow's shell steps, so a test
# path (which reaches here from an issue body) must be a plain repository path.
_TEST_FILE_RE = re.compile(r"tests/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_.-]+\.tcl")
# Test files the Daily ``runtest`` steps can select with ``--single``, and the
# ``skiptests`` token that gates each suite.
_SUITES = (
    ("tests/unit/moduleapi/", "modules"),
    ("tests/unit/", "valkey"),
    ("tests/integration/", "valkey"),
    ("tests/sentinel/", "sentinel"),
)


@dataclass(frozen=True)
class DailyPlan:
    """How to rerun one failing job (and test) through the Daily workflow."""

    job: str               # display name of the job to evaluate in the run
    job_id: str            # workflow job key, for matrix legs the run renames
    inputs: dict[str, str]  # dispatch inputs, without use_repo/use_git_ref
    test_name: str = ""    # "" when only the job's result decides

    def describe(self) -> str:
        target = self.inputs.get("valgrind_test", "")
        parts = (f"--single {target}" if target else "", self.inputs.get("test_args", ""),
                 self.inputs.get("cluster_test_args", ""))
        selection = " ".join(part for part in parts if part)
        job = self.job or f"{self.job_id} (targeted)"
        return f"`{job}`" + (f" with `{selection}`" if selection else " (whole job)")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DailyPlan:
        return cls(
            job=str(data["job"]), job_id=str(data["job_id"]),
            inputs={str(k): str(v) for k, v in dict(data["inputs"]).items()},
            test_name=str(data.get("test_name") or ""),
        )


@dataclass(frozen=True)
class DailyResult:
    """The factual result of one dispatched run for the target job.

    ``state`` follows the job: "failed" whenever it did not succeed, even if the
    target test itself never failed (another step can fail the job), so a
    candidate is never credited for a broken run. ``test_failures`` counts the
    target test's own failure lines, which tell a reproduced failure apart from
    a job that failed for another reason.
    """

    state: str            # "passed" | "skipped" | "failed" | "cancelled" | "not-run"
    run_url: str
    job_url: str = ""
    detail: str = ""
    test_failures: int = 0

    @property
    def verified(self) -> bool:
        return self.state == "passed"

    def reproduces(self, plan: DailyPlan) -> bool:
        """Whether this run shows the failure the plan targets.

        A test plan needs the test's own failure lines; a whole-job plan has
        only the job's result to go on.
        """
        if plan.test_name:
            return self.test_failures > 0
        return self.state == "failed"


def stress_loops() -> int:
    """``CI_FIX_STRESS_LOOPS``: how many times the failing test file is repeated."""
    return env_int("CI_FIX_STRESS_LOOPS", DEFAULT_LOOPS, minimum=1, maximum=MAX_LOOPS)


def plan_daily_run(
    workflow_yaml: str,
    *,
    job_name: str,
    test_file: str = "",
    test_name: str = "",
    loops: int = DEFAULT_LOOPS,
) -> DailyPlan | str:
    """Plan a Daily dispatch that reruns ``job_name`` (and ``test_file``), or explain why not."""
    doc = load_workflow(workflow_yaml)
    plan = _plan(doc, job_name=job_name, test_file=test_file, test_name=test_name, loops=loops)
    if isinstance(plan, str) or doc is None:
        return plan
    return _narrowest(plan, doc)


def _plan(
    doc: dict[str, Any] | None, *, job_name: str, test_file: str, test_name: str, loops: int,
) -> DailyPlan | str:
    if doc is None:
        return f"{DAILY_WORKFLOW} did not parse"
    # YAML 1.1 reads the bare key ``on`` as boolean True.
    raw: dict[Any, Any] = doc
    triggers = raw.get("on", raw.get(True))
    dispatch = triggers.get("workflow_dispatch") if isinstance(triggers, dict) else None
    inputs = dispatch.get("inputs") if isinstance(dispatch, dict) else None
    if not isinstance(inputs, dict) or any(name not in inputs for name in _REQUIRED_INPUTS):
        return f"{DAILY_WORKFLOW} has no workflow_dispatch inputs to select one job"
    resolved = resolve_job(doc, job_name)
    if resolved is None:
        return f"job {job_name!r} is not in {DAILY_WORKFLOW}"

    condition = str(resolved.job.get("if", ""))
    if "github.event_name" in condition and "workflow_dispatch" not in condition:
        return f"job {job_name!r} does not run when {DAILY_WORKFLOW} is dispatched"
    if resolved.job.get("needs"):
        return f"job {job_name!r} only collects other jobs' results; there is nothing of its own to rerun"
    jobs_value = doc.get("jobs")
    jobs: dict[str, Any] = jobs_value if isinstance(jobs_value, dict) else {}
    keep_jobs = set(_SKIPJOBS_RE.findall(condition))
    all_jobs = _tokens(inputs["skipjobs"]) | {
        token for job in jobs.values() if isinstance(job, dict)
        for token in _SKIPJOBS_RE.findall(str(job.get("if", "")))
    }
    skipjobs = ",".join(sorted(all_jobs - keep_jobs))

    plan_inputs = {"skipjobs": skipjobs, "skiptests": "", "test_args": ""}
    # The workflow tests ``contains(skipjobs, 'x')``, a substring match, so a
    # kept token inside a skipped one would skip the target job too.
    if any(token in skipjobs for token in keep_jobs):
        return f"job {job_name!r} cannot be selected on its own with skipjobs"
    # Suite tokens that gate one of this job's steps, and those that only gate
    # the job as a whole (e.g. large-memory): skipping the latter would skip
    # the target job itself, so they are never skipped.
    step_tokens = _step_suite_tokens(resolved.job)
    job_only = set(_SKIPTESTS_RE.findall(condition)) - step_tokens
    other_suites = (_tokens(inputs["skiptests"]) | step_tokens | _all_suite_tokens(jobs)) - job_only

    # Rerunning the whole job is judged by its result alone: not every test
    # harness (sentinel, legacy cluster) prints the "[ok]: <name>" lines. It
    # still skips every suite the target job does not run, so sibling jobs
    # sharing its skipjobs token (valgrind misc, large-memory) stay off.
    whole_job = DailyPlan(job=job_name, job_id=resolved.job_id, inputs={
        **plan_inputs, "skiptests": _skip_list(other_suites - step_tokens, job_only | step_tokens) or "",
    })
    if not test_file:
        # A job-level failure (a build break, a crash with no test record).
        # A valgrind shard reruns as the one "targeted" leg with the shard's
        # own selection, instead of every shard of the job.
        shard = _valgrind_shard_selection(resolved.job, job_name, inputs)
        if shard is not None:
            first, rest = shard
            return DailyPlan(job="", job_id=resolved.job_id, inputs={
                **whole_job.inputs, "valgrind_test": first, "test_args": rest,
            })
        return whole_job
    if not _TEST_FILE_RE.fullmatch(test_file) or ".." in test_file.split("/"):
        return f"{test_file!r} is not a test file path"

    # Run only the failing test file, repeatedly, when the job lets the
    # dispatch inputs select it. Otherwise (a job with a fixed test list, or a
    # suite the inputs cannot isolate) rerun the whole job once.
    suite = next((token for prefix, token in _SUITES if test_file.startswith(prefix)), "")
    if suite not in step_tokens:
        return whole_job
    skiptests = _skip_list(other_suites - {suite}, job_only | {suite})
    if skiptests is None:
        return whole_job

    unit = test_file.removeprefix("tests/").removesuffix(".tcl")
    targeted = {**plan_inputs, "skiptests": skiptests}
    if suite == "sentinel":
        # The sentinel harness selects files with --single but cannot repeat
        # them and prints no "[ok]: <name>" lines: run the file once, judged
        # by the job, with every other suite of the job skipped.
        if "cluster_test_args" not in inputs:
            return whole_job
        targeted["cluster_test_args"] = f"--single {PurePosixPath(test_file).stem}"
        return DailyPlan(job=job_name, job_id=resolved.job_id, inputs=targeted)
    job_text = json.dumps(resolved.job, sort_keys=True)
    if "valgrind" in resolved.job_id:
        if "valgrind_test" not in inputs or "valgrind_test" not in job_text:
            return whole_job
        loops = min(loops, VALGRIND_LOOPS)
        # The valgrind jobs switch to a single "targeted" shard for this input,
        # which renames the matrix leg; evaluate whichever leg the run creates.
        targeted.update(valgrind_test=unit, test_args=f"--loops {loops} --fastfail")
        return DailyPlan(job="", job_id=resolved.job_id, inputs=targeted, test_name=test_name)
    targeted["test_args"] = f"--single {unit} --loops {loops} --fastfail"
    return DailyPlan(job=job_name, job_id=resolved.job_id, inputs=targeted, test_name=test_name)


def _narrowest(plan: DailyPlan, doc: dict[str, Any]) -> DailyPlan | str:
    """Keep the fewest ``skipjobs`` tokens that still start the target job.

    The workflow gates jobs on ``contains(skipjobs, 'x')`` in arbitrary
    combinations (e.g. ``ubuntu || arm``), so every subset of the tokens the
    plan keeps is evaluated against every job's ``if:``, and the one starting
    the fewest other jobs wins. A plan that would not start the target job at
    all is refused. An expression this module cannot model leaves the plan as
    it was.
    """
    jobs = {key: job for key, job in (doc.get("jobs") or {}).items() if isinstance(job, dict)}
    target = jobs.get(plan.job_id)
    if target is None:
        return plan
    every = set(_SKIPJOBS_RE.findall(" ".join(str(job.get("if", "")) for job in jobs.values())))
    every |= set(filter(None, plan.inputs.get("skipjobs", "").split(",")))
    kept = sorted(set(_SKIPJOBS_RE.findall(str(target.get("if", "")))))
    # Jobs that only aggregate others' results start with any inputs.
    summaries = {key for key, job in jobs.items() if job.get("needs")}
    best: tuple[tuple[int, int], DailyPlan, tuple[str, ...]] | None = None
    for size in range(len(kept) + 1):
        for keep in itertools.combinations(kept, size):
            candidate = replace(plan, inputs={**plan.inputs, "skipjobs": ",".join(sorted(every - set(keep)))})
            try:
                started = {key for key, job in jobs.items() if evaluate(job.get("if"), _dispatch_context(candidate))}
            except UnsupportedExpression as exc:
                logger.info("Not narrowing the Daily dispatch: %s", exc)
                return plan
            if plan.job_id not in started:
                continue
            others = tuple(sorted(started - {plan.job_id} - summaries))
            key = (len(others), size)
            if best is None or key < best[0]:
                best = (key, candidate, others)
    if best is None:
        return f"job {plan.job_id!r} would not run when {DAILY_WORKFLOW} is dispatched with any skipjobs value"
    if best[2]:
        logger.info("The dispatch for %s also starts: %s", plan.job_id, ", ".join(best[2]))
    return best[1]


def _dispatch_context(plan: DailyPlan) -> dict[str, Any]:
    inputs = {**plan.inputs, "use_repo": "", "use_git_ref": ""}
    return {"github": {"event_name": "workflow_dispatch", "event": {"inputs": inputs}}, "inputs": inputs}


def _valgrind_shard_selection(job: dict[str, Any], job_name: str, inputs: dict[str, Any]) -> tuple[str, str] | None:
    """The shard's ``--single`` selection as (valgrind_test, test_args), if expressible.

    A valgrind job's display name ends in its shard (``test-valgrind-test
    (unit)``) and its test step maps each shard to ``--single`` arguments in a
    shell ``case``. The targeted leg runs ``--single "$valgrind_test"
    <test_args>``, so the same selection runs as one leg.
    """
    if "valgrind_test" not in inputs or not job_name.endswith(")") or " (" not in job_name:
        return None
    shard = job_name.rsplit(" (", 1)[1][:-1]
    arm = re.compile(rf"(?m)^\s*{re.escape(shard)}\)\s*shard_args=\(([^)]*)\)\s*;;")
    for step in job.get("steps") or ():
        match = arm.search(str(step.get("run", ""))) if isinstance(step, dict) else None
        if match is None:
            continue
        try:
            args = shlex.split(match.group(1))
        except ValueError:
            return None
        pairs = list(zip(args[::2], args[1::2]))
        if len(args) % 2 or not pairs or any(
            flag != "--single" or not _SHARD_PATH_RE.fullmatch(path) for flag, path in pairs
        ):
            return None
        return pairs[0][1], " ".join(f"--single {path}" for _flag, path in pairs[1:])
    return None


_SHARD_PATH_RE = re.compile(r"tests(?:/[A-Za-z0-9_-]+)*")


def dispatch_daily(
    gh: Any, repo_full_name: str, *, ref: str, plan: DailyPlan, sha: str,
) -> tuple[int, str]:
    """Dispatch the plan against ``sha`` and return the new run's id and URL.

    Not retried: a retried dispatch whose first attempt did start a run would
    start a second one, and Daily's concurrency group cancels the first.
    """
    repo = gh.get_repo(repo_full_name)
    inputs = {**plan.inputs, "use_repo": repo_full_name, "use_git_ref": sha}
    _headers, data = repo._requester.requestJsonAndCheck(  # noqa: SLF001 - the run id needs the raw response
        "POST",
        f"/repos/{repo_full_name}/actions/workflows/{DAILY_WORKFLOW}/dispatches",
        input={"ref": ref, "inputs": inputs},
        headers={"X-GitHub-Api-Version": _DISPATCH_API_VERSION},
    )
    run_id = data.get("workflow_run_id") if isinstance(data, dict) else None
    if not isinstance(run_id, int) or run_id <= 0:
        raise RuntimeError(f"dispatching {DAILY_WORKFLOW} returned no run id")
    url = data.get("html_url") if isinstance(data, dict) else None
    return run_id, str(url or f"https://github.com/{repo_full_name}/actions/runs/{run_id}")


def evaluate_daily_run(
    gh: Any,
    artifact_client: ArtifactClient,
    repo_full_name: str,
    run_id: int,
    plan: DailyPlan,
) -> DailyResult | None:
    """Return the target job's result, or None while it has not finished."""
    run = retry_github_call(
        lambda: gh.get_repo(repo_full_name).get_workflow_run(run_id),
        retries=3, description=f"get run {run_id}",
    )
    run_url = str(getattr(run, "html_url", "") or "")
    jobs = retry_github_call(lambda: list(run.jobs()), retries=3, description=f"list jobs of {run_id}")
    job = _target_job(jobs, plan)
    if job is None:
        if str(getattr(run, "status", "") or "") != "completed":
            return None
        if str(getattr(run, "conclusion", "") or "") == "cancelled":
            return DailyResult("cancelled", run_url, detail="the run was cancelled")
        return DailyResult("not-run", run_url, detail=f"the run has no {plan.job or plan.job_id} job")
    if str(getattr(job, "status", "") or "") != "completed":
        return None
    job_url = str(getattr(job, "html_url", "") or "")
    conclusion = str(getattr(job, "conclusion", "") or "")
    if conclusion == "cancelled":
        # Daily cancels a run when another starts on the same ref, so a
        # cancellation says nothing about the code.
        return DailyResult("cancelled", run_url, job_url, detail="the job was cancelled")
    if not plan.test_name:
        if conclusion != "success":
            return DailyResult("failed", run_url, job_url, detail=f"the job concluded {conclusion or 'unknown'}")
        return DailyResult("passed", run_url, job_url, detail="the job passed")
    log = strip_ansi(artifact_client.download_job_log(repo_full_name, int(job.id)))
    name = re.escape(plan.test_name)
    # "[ok]: <name> (12 ms)" exactly: a sibling test "<name> (variant)" is a
    # different test. A hang prints "[TIMEOUT]: <name> in <file>" instead of err.
    passes = len(re.findall(rf"\[ok\]: {name} \(\d+ ms\)", log))
    failures = log.count(f"[err]: {plan.test_name} in ") + log.count(f"[TIMEOUT]: {plan.test_name} in ")
    skips = len(re.findall(rf"\[skip\]: {name}(?: in |$)", log, re.MULTILINE))
    counts = {"test_failures": failures}
    if conclusion != "success":
        detail = (
            f"the test failed {failures} time(s)" if failures
            else f"the job concluded {conclusion or 'unknown'} without the test failing"
        )
        return DailyResult("failed", run_url, job_url, detail=detail, **counts)
    if passes:
        return DailyResult("passed", run_url, job_url, detail=f"the test passed {passes} time(s)", **counts)
    if skips:
        return DailyResult("skipped", run_url, job_url, detail="the test is skipped in this job", **counts)
    return DailyResult("not-run", run_url, job_url, detail="the job passed but never ran the test", **counts)


def _target_job(jobs: list[Any], plan: DailyPlan) -> Any | None:
    """The single job the plan evaluates: by exact name, or the one leg of ``job_id``."""
    named = [(str(getattr(job, "name", "") or ""), job) for job in jobs]
    if plan.job:
        matches = [job for name, job in named if name == plan.job]
    else:
        matches = [
            job for name, job in named
            if name == plan.job_id or name.startswith(f"{plan.job_id} (")
        ]
    return matches[0] if len(matches) == 1 else None


def _step_suite_tokens(job: dict[str, Any]) -> set[str]:
    """Suite tokens that gate one of the job's steps, by ``if:`` or in shell."""
    tokens: set[str] = set()
    for step in job.get("steps") or ():
        if not isinstance(step, dict):
            continue
        tokens |= set(_SKIPTESTS_RE.findall(str(step.get("if", ""))))
        env = step.get("env")
        if isinstance(env, dict) and any("inputs.skiptests" in str(value) for value in env.values()):
            tokens |= set(_SHELL_SKIP_RE.findall(str(step.get("run", ""))))
    return tokens


def _all_suite_tokens(jobs: dict[str, Any]) -> set[str]:
    return {token for job in jobs.values() if isinstance(job, dict) for token in _step_suite_tokens(job)}


def _skip_list(skip: set[str], keep: set[str]) -> str | None:
    """``skip`` as a skiptests value, or None if it would also match a kept token.

    The workflow tests ``contains(skiptests, 'x')``, a substring match, so a
    skipped token containing a kept one would also skip what must run.
    """
    value = ",".join(sorted(skip))
    return None if any(token in value for token in keep) else value


def _tokens(value: Any) -> set[str]:
    default = value.get("default") if isinstance(value, dict) else ""
    return {token.strip() for token in str(default or "").split(",") if token.strip()}
