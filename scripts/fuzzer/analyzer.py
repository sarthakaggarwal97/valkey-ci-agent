"""Fuzzer batch analysis: deterministic pattern matching + Claude Code triage.

The scheduled fuzzer workflow runs a batch and uploads its folder under
``batch/``. The monitor reads ``batch/summary.json`` and, for each of its
runs, the run report at ``batch/<runs[i].report>`` (see
:mod:`scripts.fuzzer.batch`), and turns the batch into findings, one issue
each:

- each failure signature: the non-PASS runs the fuzzer grouped as one
  failure, so a bug seen in three runs is one issue; its serial re-runs'
  verdict is evidence for or against a real bug;
- each run that passed but whose report or logs show something;
- each bug zoo case that no longer holds: the fuzzer missed a bug it is known
  to catch, a valkey-fuzzer problem and never a Valkey bug;
- what runs left on the host, also a valkey-fuzzer bug;
- the batch itself, when its summary cannot be read or it did not finish.
"""

from __future__ import annotations

import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from scripts.ai.runtime import run_agent
from scripts.common.ai_output import extract_json_object
from scripts.common.git_clone import shallow_clone_at_sha
from scripts.common.incidents import compute_fingerprint
from scripts.common.text_utils import strip_ansi
from scripts.common.workflow_artifacts import ArtifactClient
from scripts.fuzzer.batch import BatchFolder, BatchRun, SignatureGroup, read_batch
from scripts.fuzzer.models import FuzzerBatchAnalysis, FuzzerFinding, FuzzerRunContext, FuzzerSignal

logger = logging.getLogger(__name__)

# Where fuzzer-run.yml's artifact holds the batch folder.
BATCH_PREFIX = "batch/"

# Total attempts for the AI analysis: one retry covers a transient Bedrock
# stall (the per-attempt timeout lives in the fuzzer_analysis_readonly profile).
_CLAUDE_MAX_ATTEMPTS = 2
# Findings per batch that get a Claude analysis; the rest keep the quick
# scan's verdict. A batch of many failures still files every issue.
_MAX_CLAUDE_FINDINGS = 5
# Runs of one signature whose folders Claude gets.
_MAX_CLAUDE_RUNS = 3

# (title, severity, pattern, is_bug_indicator)
# A "bug indicator" upgrades the verdict from possible-core-valkey-bug to
# likely-core-valkey-bug. RDB/AOF failures are anomalous but not necessarily
# bugs (could be disk errors).
_ANOMALY_PATTERNS: list[tuple[str, str, str, bool]] = [
    ("Node crash or assertion", "critical",
     r"ASSERTION FAILED|Assertion failed|BUG REPORT START|STACK TRACE|crashed by signal|Guru Meditation", True),
    ("Sanitizer failure", "critical",
     r"AddressSanitizer|LeakSanitizer|ThreadSanitizer|UndefinedBehaviorSanitizer|runtime error:", True),
    ("Segfault", "critical", r"segmentation fault|signal 11", True),
    ("OOM", "critical", r"Out Of Memory|Can't allocate|OOM command not allowed", False),
    ("Failover timeout", "critical",
     r"Failover attempt expired|Manual failover timed out", True),
    ("Split-brain or slot loss", "critical",
     r"split.?brain|slots still assigned to killed nodes", True),
    ("RDB/AOF failure", "warning",
     r"Background saving error|Failed opening.*rdb|AOF rewrite.*failed", False),
]

# Signal titles that send a finding to a human rather than to a bug verdict:
# the analyzer could not trust a report, or the run tested nothing.
_REPORT_UNREADABLE = "Run report unreadable"
_RUN_ERROR = "Run error"
_RUN_SKIPPED = "Run skipped as unsupported"
_HUMAN_TRIAGE_TITLES = frozenset({_REPORT_UNREADABLE, _RUN_ERROR, _RUN_SKIPPED})


def _object(value: Any) -> dict[str, Any]:
    """``value`` when it is a JSON object, else an empty one."""
    return value if isinstance(value, dict) else {}


def _component_verdict(result: dict[str, Any]) -> str:
    """A failed component's verdict: the exception that stopped it, else its summary."""
    summary = str(result.get("summary") or "").strip() or "no summary"
    diagnostics = result.get("diagnostics")
    exception = diagnostics.get("exception") if isinstance(diagnostics, dict) else None
    if isinstance(exception, dict) and exception.get("type"):
        where = f" during {exception['phase']}" if exception.get("phase") else ""
        message = f": {exception['message']}" if exception.get("message") else ""
        return f"{exception['type']}{where}{message}"[:300]
    return summary[:300]


def _scan_report(context: FuzzerRunContext) -> list[FuzzerSignal]:
    """Signals from the run report: its outcome, failed components and causes."""
    if context.report_problem:
        return [FuzzerSignal(_REPORT_UNREADABLE, "critical", context.report_problem)]
    report = context.report
    if report is None:  # _context_for always sets one of the two
        return [FuzzerSignal(_REPORT_UNREADABLE, "critical", "no run report was loaded")]

    status = report["status"]
    summary = str(report.get("summary") or "")[:300]
    outcome = report["outcome"]
    diagnostics = _object(outcome.get("diagnostics"))
    signals: list[FuzzerSignal] = []
    if status == "FAIL":
        signals.append(FuzzerSignal("Run failed", "critical", summary or "FAIL"))
    elif status == "ERROR":
        error = diagnostics.get("error")
        detail = summary or "ERROR"
        if isinstance(error, dict) and error.get("type"):
            phase = f" during {error['phase']}" if error.get("phase") else ""
            detail = f"{detail} ({error['type']}{phase}: {str(error.get('message') or '')[:200]})"
        signals.append(FuzzerSignal(_RUN_ERROR, "critical", detail))
    elif status == "SKIP_UNSUPPORTED":
        signals.append(FuzzerSignal(_RUN_SKIPPED, "warning", summary or "SKIP_UNSUPPORTED"))

    # In a run that passed, a failed component was not a required one, so it
    # did not decide the outcome: worth a look, not a finding by itself.
    component_severity = "warning" if status == "PASS" else "critical"
    for result in outcome["component_results"]:
        if not isinstance(result, dict):
            continue
        component_status = result.get("status")
        if component_status not in ("FAIL", "ERROR"):
            continue
        component = str(result.get("component_id") or "unknown")
        signals.append(FuzzerSignal(
            f"Component {component} {'failed' if component_status == 'FAIL' else 'errored'}",
            component_severity, _component_verdict(result),
        ))

    for item in diagnostics.get("missing_required_evidence") or []:
        if isinstance(item, dict):
            source = f" from {item['source']}" if item.get("source") else ""
            signals.append(FuzzerSignal(
                "Required evidence missing", "critical",
                f"{item.get('evidence_id')}{source}: observed "
                f"{item.get('observed_count', 0)} of {item.get('min_count', 1)}",
            ))
    for name in diagnostics.get("missing_required_events") or []:
        signals.append(FuzzerSignal("Required event missing", "critical", str(name)))
    for item in diagnostics.get("cleanup_errors") or []:
        if isinstance(item, dict):
            exception = _object(item.get("exception"))
            signals.append(FuzzerSignal(
                "Cleanup error", "warning",
                f"{item.get('owner')} during {item.get('phase')}: {exception.get('type', 'error')}",
            ))
    for item in diagnostics.get("abandoned_calls") or []:
        if isinstance(item, dict):
            signals.append(FuzzerSignal("Abandoned call", "warning", str(item.get("call"))))
    return signals


def _scan_logs(context: FuzzerRunContext) -> list[FuzzerSignal]:
    """Deterministic scan of one run: its report and its instance logs."""
    anomalies = _scan_report(context)
    texts = dict(context.node_logs)
    if context.report is not None:
        # An oracle's summary can quote a crash report from a log that did not
        # make it into the bundle.
        for result in context.report["outcome"]["component_results"]:
            if isinstance(result, dict) and result.get("status") in ("FAIL", "ERROR"):
                texts[f"component {result.get('component_id')}"] = str(result.get("summary") or "")
    for name, text in texts.items():
        cleaned = strip_ansi(text)
        for title, severity, pattern, _ in _ANOMALY_PATTERNS:
            m = re.search(pattern, cleaned, re.I)
            if m:
                anomalies.append(FuzzerSignal(title, severity, f"{name}: {m.group(0)[:200]}"))

    return _dedupe_signals(anomalies)


def _dedupe_signals(signals: list[FuzzerSignal]) -> list[FuzzerSignal]:
    seen: set[tuple[str, str]] = set()
    out: list[FuzzerSignal] = []
    for s in signals:
        key = (s.title, s.evidence)
        if key not in seen:
            seen.add(key)
            out.append(s)
    return out


def _context_for(base: dict[str, Any], run: BatchRun, prefix: str) -> FuzzerRunContext:
    """One run's evidence: its report, scenario, error and logs, and who it is."""
    context = FuzzerRunContext(
        **base, job_id=run.id, batch_prefix=prefix,
        report=run.report, report_text=run.report_text,
        report_problem=f"run {run.id}: {run.report_problem}" if run.report_problem else None,
        scenario_yaml=run.scenario_yaml, error_text=run.error_text, node_logs=dict(run.logs),
        experiment_id=run.experiment, seed=run.seed,
    )
    if run.report is not None:
        _read_run_identity(context, run.report)
    return context


def _read_run_identity(context: FuzzerRunContext, report: dict[str, Any]) -> None:
    """Scenario, seed, experiment and tested Valkey commit from the report.

    The execution plan always names them; the scenario can be null.
    """
    plan = _object(report["outcome"].get("plan"))
    scenario = _object(report.get("scenario"))
    reproducer = _object(report.get("reproducer"))
    scenario_id = plan.get("scenario_id") or scenario.get("scenario_id")
    if scenario_id:
        context.scenario_id = str(scenario_id)
    seed = plan.get("seed", scenario.get("seed"))
    if seed is not None:
        context.seed = str(seed)
    experiment_id = plan.get("experiment_id") or _object(scenario.get("experiment")).get("id")
    if experiment_id:
        context.experiment_id = str(experiment_id)
    sha = report["outcome"].get("valkey_sha") or reproducer.get("valkey_sha")
    if isinstance(sha, str) and sha:
        context.tested_valkey_sha = sha


def _reproduction_hint(context: FuzzerRunContext) -> str | None:
    """How to run the same thing again from the downloaded artifact."""
    folder = f"{context.batch_prefix}runs/{context.job_id}"
    if context.report is not None:
        return f"valkey-fuzzer replay {folder}/report.json --valkey-binary /path/to/valkey-server"
    if context.scenario_yaml is not None:
        return f"valkey-fuzzer run {folder}/scenario.yaml --valkey-binary /path/to/valkey-server"
    return None


def _triage(anomalies: list[FuzzerSignal]) -> tuple[str, str]:
    if not anomalies:
        return "normal", "expected-chaos-noise"
    status = "anomalous" if any(s.severity == "critical" for s in anomalies) else "warning"
    bug_titles = {t for t, _, _, is_bug in _ANOMALY_PATTERNS if is_bug}
    titles = {s.title for s in anomalies}
    if titles & bug_titles:
        return status, "likely-core-valkey-bug"
    if titles & _HUMAN_TRIAGE_TITLES:
        return status, "needs-human-triage"
    return status, "possible-core-valkey-bug"


def _rerun_signal(group: SignatureGroup) -> FuzzerSignal | None:
    """The serial re-runs' verdict as evidence."""
    counted = f"{group.reruns_reproduced}/{group.reruns_ran}"
    if group.rerun_verdict == "reproduced":
        return FuzzerSignal("Reproduced in serial re-runs", "warning",
                            f"{counted} serial re-runs failed with the same signature")
    if group.rerun_verdict == "intermittent":
        return FuzzerSignal("Intermittent in serial re-runs", "warning",
                            f"{counted} serial re-runs failed with the same signature")
    if group.rerun_verdict == "failed differently":
        return FuzzerSignal("Failed differently in serial re-runs", "warning",
                            f"{counted} serial re-runs failed with the same signature, but some failed "
                            f"another way: {'; '.join(group.rerun_other_labels)[:300] or 'see summary.json'}")
    if group.rerun_verdict == "not reproduced":
        return FuzzerSignal("Not reproduced in serial re-runs", "warning",
                            f"{counted} serial re-runs failed with the same signature; "
                            "the failure may be flaky or caused by load")
    return None


def _rerun_adjusted(verdict: str, rerun_verdict: str | None, run_status: str | None) -> str:
    """Serial re-runs as evidence: reproduced raises confidence, not reproduced lowers it."""
    if rerun_verdict == "reproduced" and run_status == "FAIL" and verdict == "possible-core-valkey-bug":
        return "likely-core-valkey-bug"
    if rerun_verdict == "not reproduced" and verdict == "likely-core-valkey-bug":
        return "possible-core-valkey-bug"
    return verdict


_CLAUDE_PROMPT_TEMPLATE = """\
You analyze Valkey fuzzer workflow runs (chaos testing for Valkey clusters).
Distinguish expected chaos behavior from real bugs. Be conservative.

Chaos-expected (NOT bugs): CLUSTERDOWN, replication link loss, cluster state FAIL,
slot migration errors during node kills. These are normal side-effects of killing
nodes. Only flag them if they persist after the cluster should have recovered.

Real bugs: crashes/assertions on nodes NOT targeted by chaos, sanitizer errors,
segfaults, permanent slot loss, split-brain, data inconsistency after recovery.

## Workflow run
{run_url}
The fuzzer ran a batch of runs; this analysis covers one finding of it.

## The finding
{finding_note}
Start with _artifacts/runs/{focus}/: fuzzer outcome {run_status}; experiment
{experiment_id}, scenario {scenario_id}, seed {seed}, Valkey SHA {valkey_sha}.

## How to read the artifacts
_artifacts/ is the batch folder valkey-fuzzer wrote, with the runs of this
finding:
- summary.json: the batch summary. `runs` lists every run (`id`, `source`,
  `status`, `summary`, `experiment`, `seed`, and `report`, the run report's
  path, null when the run left none). `signatures` groups the non-PASS runs
  by failure signature (the same outcome, failing components and verdicts,
  missing events and evidence); its `serial_reruns` ran some of them again
  one at a time, with no other run competing for the host: `reproduced`
  means the failure is deterministic, `not reproduced` (every re-run passed)
  points at flakiness or load, `intermittent` is in between, and `failed
  differently` means no re-run failed the same way but some failed another
  way (each re-run's `signature_label` says how). `zoo` and `leaks` concern
  the fuzzer itself.
- runs/<id>/report.json: that run's run report:
  - `status` and `summary`: the outcome. PASS; FAIL (a required oracle or
    action judged the system under test and found a problem, or required
    events or evidence are missing); ERROR (the harness could not reach a
    trustworthy verdict: a fault that never fired, a component that raised or
    timed out, a cleanup error); SKIP_UNSUPPORTED (the build or host could
    not run it).
  - `scenario`: the scenario as run (experiment, inputs, deployment shape),
    or null for a replay of a bare plan.
  - `outcome.plan`: the execution plan: `scenario_id`, `experiment_id`,
    `seed`, the resolved `inputs`, the `components` with their config, the
    `deployment`, and the `required_events` and `required_evidence`.
  - `outcome.component_results`: one entry per component (`component_id`,
    `status`, `summary`, `diagnostics`, `evidence`). The FAIL and ERROR
    entries are the findings; `diagnostics.exception` names an exception and
    its phase.
  - `outcome.diagnostics`: `deployment` (each instance's ID, role, shard,
    port, PID and binary), `events` (every published event, in order, with
    timestamps; the chaos action's kills and failovers are here), and, when
    the run did not pass, the cause: `missing_required_events`,
    `missing_required_evidence`, `required_component_failures`,
    `untrusted_component_failures`, `cleanup_errors`, `abandoned_calls`,
    `error`.
  - `outcome.evidence`: what the components recorded, such as
    `valkey.instances.expected-down` for the instances the chaos action
    killed.
  - `reproducer`: what replay checks before running the plan again: the
    experiment's digest, the plugins and components, the plan digest and the
    Valkey SHA.
- runs/<id>/scenario.yaml: the scenario, which `valkey-fuzzer run` accepts.
- runs/<id>/error.json: when the job could not be generated or compiled, and
  so left no report, why.
- runs/<id>/logs/: each instance's server log,
  `<seed>-<deployment id>-valkey-N.log`, and its captured stdout and stderr,
  `<seed>-<deployment id>-valkey-N.stdio.log` (sanitizer reports land
  there). `valkey-N` is the instance ID used in the report. An instance
  listed as expected-down was killed on purpose; a crash report from any
  instance is not.

## Working directory layout
{source_note}

## Deterministic findings
{deterministic_summary}

## Task
Read the artifacts and source as needed (use Grep to find assertion text or
crash handlers in valkey/src/ for context). Return ONLY a single JSON object:
{{
  "overall_status": "normal|warning|anomalous",
  "triage_verdict": "likely-core-valkey-bug|possible-core-valkey-bug|expected-chaos-noise|environmental-or-infra|needs-human-triage",
  "root_cause_category": "short-label or null",
  "summary": "2-3 sentence maintainer-facing explanation",
  "anomalies": [{{"title": "...", "severity": "warning|critical", "evidence": "..."}}],
  "reproduction_hint": "command or null"
}}
"""


def _write_run(art_dir: Path, context: FuzzerRunContext) -> None:
    run_dir = art_dir / "runs" / context.job_id
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    if context.report_text is not None:
        (run_dir / "report.json").write_text(context.report_text)
    if context.scenario_yaml is not None:
        (run_dir / "scenario.yaml").write_text(context.scenario_yaml)
    if context.error_text is not None:
        (run_dir / "error.json").write_text(context.error_text)
    for name, text in context.node_logs.items():
        (logs_dir / name).write_text(text)


def _invoke_claude(
    focus: FuzzerRunContext, anomalies: list[FuzzerSignal], workdir: Path, *,
    others: list[FuzzerRunContext] | None = None, summary_text: str | None = None,
    finding_note: str = "",
) -> dict[str, Any]:
    """Drop the finding's run folders in workdir/_artifacts, clone source, and run Claude."""
    art_dir = workdir / "_artifacts"
    art_dir.mkdir(parents=True)
    if summary_text is not None:
        (art_dir / "summary.json").write_text(summary_text)
    for context in [focus, *(others or [])]:
        _write_run(art_dir, context)

    # Clone valkey at the tested commit (skipping if the report didn't record
    # one - cloning the default branch would have Claude triage a different
    # tree than the one that crashed). Clone the fuzzer at the run's HEAD too.
    if focus.tested_valkey_sha:
        valkey_ok = shallow_clone_at_sha(
            "valkey-io/valkey", workdir / "valkey", focus.tested_valkey_sha,
        )
    else:
        valkey_ok = False
    fuzzer_ok = shallow_clone_at_sha(
        focus.repo, workdir / "valkey-fuzzer", focus.head_sha or None,
    )

    source_note = _format_source_note(focus, valkey_ok=valkey_ok, fuzzer_ok=fuzzer_ok)
    det_lines = [f"- [{a.severity}] {a.title}: {a.evidence}" for a in anomalies[:15]] or ["- none"]
    prompt = _CLAUDE_PROMPT_TEMPLATE.format(
        run_url=focus.run_url,
        finding_note=finding_note or f"Run {focus.job_id}.",
        focus=focus.job_id,
        run_status=(focus.report or {}).get("status") or "unknown (the run report could not be read)",
        experiment_id=focus.experiment_id or "unknown",
        valkey_sha=focus.tested_valkey_sha or "unknown",
        scenario_id=focus.scenario_id or "unknown",
        seed=focus.seed or "unknown",
        source_note=source_note,
        deterministic_summary="\n".join(det_lines),
    )

    # Retry once on a transient Bedrock stall (non-zero exit, typically a
    # throttle or timeout). The monitor only ever looks at the latest run, so
    # a dropped analysis is never revisited. A successful exit with
    # unparseable output is deterministic - the same prompt yields the same
    # bad output - so it is not retried. Mirrors backport.conflict_resolver.
    last_error: RuntimeError | None = None
    for attempt in range(_CLAUDE_MAX_ATTEMPTS):
        result = run_agent("fuzzer_analysis_readonly", prompt, cwd=str(workdir))
        if result.returncode == 0:
            return _parse_claude_response(result.stdout)
        last_error = RuntimeError(
            f"Claude Code failed (rc={result.returncode}): {result.stderr[:300]}"
        )
        if attempt + 1 < _CLAUDE_MAX_ATTEMPTS:
            logger.warning("Claude analysis failed, retrying: %s", last_error)
    if last_error is None:  # _CLAUDE_MAX_ATTEMPTS was misconfigured to 0
        raise RuntimeError("_CLAUDE_MAX_ATTEMPTS must be >= 1")
    raise last_error


def _format_source_note(context: FuzzerRunContext, *, valkey_ok: bool, fuzzer_ok: bool) -> str:
    """Tell Claude exactly which source trees are available and at what SHA."""
    if context.report_problem:
        lines = [
            "- _artifacts/ - summary.json and the finding's run folders; the run "
            f"report of the first could not be used ({context.report_problem}). "
            "Read whatever is there, including error.json, scenario.yaml and logs/."
        ]
    else:
        lines = ["- _artifacts/ - summary.json and the finding's run folders under runs/."]
    if valkey_ok:
        lines.append(
            f"- valkey/ - Valkey source at commit {context.tested_valkey_sha}. "
            "Grep for assertion text, crash handlers, BUG REPORT lines."
        )
    elif not context.tested_valkey_sha:
        lines.append(
            "- valkey/ - NOT AVAILABLE (the run report did not record the "
            "tested commit). Do not cite source line numbers."
        )
    else:
        lines.append(
            "- valkey/ - NOT AVAILABLE (clone failed). Do not cite source line numbers."
        )
    if fuzzer_ok:
        lines.append(
            "- valkey-fuzzer/ - Fuzzer source at the run's HEAD. The core is in "
            "valkey_fuzzer/, the Valkey components and experiments in "
            "valkey_fuzzer_experiments/, and docs/ explains both. Check a "
            "component's logic there when it failed."
        )
    else:
        lines.append("- valkey-fuzzer/ - NOT AVAILABLE (clone failed).")
    return "\n".join(lines)


def _parse_claude_response(stdout: str) -> dict[str, Any]:
    """Find the analysis JSON object in the Claude response."""
    obj = extract_json_object(stdout, required_key="overall_status")
    if obj is None:
        raise ValueError("No analysis JSON object in Claude response")
    return obj


_OVERALL_STATUSES = ("normal", "warning", "anomalous")
_TRIAGE_VERDICTS = (
    "likely-core-valkey-bug", "possible-core-valkey-bug", "expected-chaos-noise",
    "environmental-or-infra", "needs-human-triage",
)
_BUG_VERDICTS = frozenset({"likely-core-valkey-bug", "possible-core-valkey-bug"})
# Verdicts that close a finding without an issue. Everything else, including
# needs-human-triage, is published by scripts/fuzzer/main.py.
DISMISSING_VERDICTS = frozenset({"expected-chaos-noise", "environmental-or-infra"})


def _final_verdict(
    claude_payload: dict[str, Any], anomalies: list[FuzzerSignal], *,
    unreadable: bool, run_status: str | None,
) -> tuple[str, str]:
    """Settle the status and verdict, checking what Claude returned.

    Claude's answer is used only when it names a known status and verdict.
    Two floors hold whatever Claude says: a finding with a run report that
    could not be used goes to a human (``needs-human-triage``, never
    ``normal``), and a run the fuzzer did not pass is never "normal".
    """
    if claude_payload:
        status = claude_payload.get("overall_status")
        verdict = claude_payload.get("triage_verdict")
        overall_status = status if status in _OVERALL_STATUSES else "warning"
        triage_verdict = verdict if verdict in _TRIAGE_VERDICTS else "needs-human-triage"
    else:
        overall_status, triage_verdict = _triage(anomalies)

    if unreadable:
        triage_verdict = "needs-human-triage"
        if overall_status == "normal":
            overall_status = "warning"
    elif run_status != "PASS" and overall_status == "normal":
        overall_status = "warning"
    return overall_status, triage_verdict


def _fuzzer_side_finding(
    base: dict[str, Any], *, kind: str, title: str, summary: str, anomalies: list[FuzzerSignal],
    namespace: tuple[str, ...], runs: list[str], experiment_id: str | None = None,
    hint: str | None = None,
) -> FuzzerFinding:
    """A problem in valkey-fuzzer itself: always a human's, never a Valkey bug."""
    return FuzzerFinding(
        **base, kind=kind, title=title, overall_status="anomalous", triage_verdict="needs-human-triage",
        summary=summary, anomalies=anomalies, experiment_id=experiment_id, reproduction_hint=hint,
        incident_fingerprint=compute_fingerprint(
            namespace=(base["repo"], base["workflow_file"], *namespace), shapes=[],
        ),
        fuzzer_side=True, batch_runs=runs,
    )


def _zoo_findings(base: dict[str, Any], batch: BatchFolder) -> list[FuzzerFinding]:
    """A zoo case that does not hold: the fuzzer missed a bug it is known to catch."""
    zoo = batch.zoo
    if not zoo:
        return []
    findings = []
    for case in zoo["cases"]:
        if not isinstance(case, dict) or case.get("holds") is True:
            continue
        if case.get("holds") is None and not batch.complete:
            continue  # pending: the unfinished batch is its own finding
        case_id = str(case.get("case") or "unknown")
        signals: list[FuzzerSignal] = []
        runs: list[str] = []
        for control in case.get("controls") or []:
            if isinstance(control, dict) and control.get("status") != "PASS":
                signals.append(FuzzerSignal(
                    "Zoo control did not pass", "critical",
                    f"{control.get('file')} seed {control.get('seed')} on the clean build: {control.get('status')}",
                ))
        minimum = case.get("min_detection_rate")
        for name, scenario in _object(case.get("scenarios")).items():
            rows = [r for r in _object(scenario).get("runs") or [] if isinstance(r, dict)]
            runs += [str(r["report"]).split("/")[1] for r in rows if str(r.get("report") or "").startswith("runs/")]
            rate = _object(scenario).get("detection_rate")
            if isinstance(rate, (int, float)) and isinstance(minimum, (int, float)) and rate < minimum:
                expected = _object(_object(scenario).get("expected"))
                signals.append(FuzzerSignal(
                    "Zoo bug missed", "critical",
                    f"{name}: caught {rate:.0%} of runs on the broken build, needs {minimum:.0%} "
                    f"(expected failing {expected.get('fail')} and missing {expected.get('missing')})",
                ))
        if case.get("holds") is None:
            signals.append(FuzzerSignal("Zoo verdict missing", "critical",
                                        f"case {case_id} has no verdict although the batch finished"))
        elif not signals:
            signals.append(FuzzerSignal("Zoo case does not hold", "critical", f"case {case_id}"))
        findings.append(_fuzzer_side_finding(
            base, kind="zoo", title=f"[fuzzer-zoo] Bug zoo case {case_id} no longer caught",
            summary=(
                f"The bug zoo case `{case_id}` does not hold on Valkey {zoo.get('valkey_commit') or 'unknown'}: "
                f"the fuzzer no longer catches a bug it is known to catch "
                f"({case.get('catches') or 'see the case'}). Its oracles, workloads or faults may have "
                "gone blind. This is a valkey-fuzzer problem, not a Valkey bug."
            ),
            anomalies=signals, namespace=("zoo", case_id), runs=runs,
            hint=f"valkey-fuzzer batch --zoo BUILDS --zoo-case {case_id}",
        ))
    return findings


def _leak_findings(base: dict[str, Any], batch: BatchFolder) -> list[FuzzerFinding]:
    """What runs left on the host: the fuzzer's cleanup missed it."""
    by_kind: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for leak in batch.leaks:
        run = batch.run(str(leak.get("run")))
        experiment = (run.experiment if run else None) or "unknown"
        by_kind.setdefault((experiment, str(leak.get("kind") or "unknown")), []).append(leak)
    listed = {str(leak.get("run")) for leak in batch.leaks}
    for run in batch.runs:
        if run.leaks and run.id not in listed:
            by_kind.setdefault((run.experiment or "unknown", "unknown"), []).append(
                {"run": run.id, "kind": "unknown", "target": f"{run.leaks} leftover(s)"},
            )
    findings = []
    for (experiment, kind), leaks in sorted(by_kind.items()):
        runs = sorted({str(leak.get("run")) for leak in leaks})
        signals = [
            FuzzerSignal(
                f"Leaked {kind}", "critical",
                f"run {leak.get('run')}: {leak.get('target')}"
                + (f" ({leak['detail']})" if leak.get("detail") else "")
                + (f"; {leak['action']}" if leak.get("action") else "")
                + (f": {leak['reason']}" if leak.get("reason") else ""),
            )
            for leak in leaks
        ]
        findings.append(_fuzzer_side_finding(
            base, kind="leak", title=f"[fuzzer-leak] {experiment} runs left a {kind} behind",
            summary=(
                f"{len(runs)} run(s) of `{experiment}` left {len(leaks)} {kind} leftover(s) on the host, "
                "which the batch's sweep found and removed. The fuzzer's own cleanup missed them: "
                "a valkey-fuzzer bug, not a Valkey bug."
            ),
            anomalies=signals, namespace=("leak", experiment, kind), runs=runs, experiment_id=experiment,
            hint=(f"valkey-fuzzer run {batch.prefix}runs/{runs[0]}/scenario.yaml "
                  "--valkey-binary /path/to/valkey-server; valkey-fuzzer sweep --dry-run"),
        ))
    return findings


class FuzzerRunAnalyzer:
    """Analyzes fuzzer workflow runs: pattern matching + Claude Code."""

    def __init__(self, github_client: Any, *, github_token: str,
                 artifact_client: ArtifactClient | None = None) -> None:
        self._gh = github_client
        self._client = artifact_client or ArtifactClient(github_client, token=github_token)

    def analyze(self, repo: str, run_id: int, *, workflow_file: str) -> FuzzerBatchAnalysis:
        gh_repo = self._gh.get_repo(repo)
        run = gh_repo.get_workflow_run(run_id)
        base: dict[str, Any] = dict(
            repo=repo, workflow_file=workflow_file, run_id=run_id,
            run_url=run.html_url,
            conclusion=str(run.conclusion or ""),
            head_sha=str(run.head_sha or ""),
        )
        analysis = FuzzerBatchAnalysis(**base)

        artifacts = self._client.list_run_artifacts(repo, run_id)
        bundle = next(
            (a for a in artifacts if a.name.startswith("fuzzer-run-artifacts") and not a.expired),
            None,
        )
        if bundle is None:
            analysis.findings = [_build_error_analysis(base, "no fuzzer artifact bundle found")]
            return analysis
        files = self._client.download_artifact(repo, bundle.artifact_id)
        if not files:
            analysis.findings = [_build_error_analysis(base, "fuzzer artifact bundle was empty or unreadable")]
            return analysis

        batch = read_batch(files, BATCH_PREFIX)
        if batch.problem:
            analysis.findings = [_build_error_analysis(base, batch.problem)]
            return analysis
        analysis.batch_status = batch.status
        analysis.counts = dict(batch.counts)
        analysis.findings = self._findings(base, batch)
        return analysis

    def _findings(self, base: dict[str, Any], batch: BatchFolder) -> list[FuzzerFinding]:
        findings: list[FuzzerFinding] = []
        if not batch.complete:
            findings.append(_incomplete_batch(base, batch))
        budget = [_MAX_CLAUDE_FINDINGS]
        for group in batch.groups:
            members = [batch.run(run_id) for run_id in group.runs]
            if members and all(m is not None and m.leak_only for m in members):
                continue  # the runs passed; what they left behind is a leak finding
            findings.append(self._signature_finding(base, batch, group, budget))
        for run in batch.main_runs:
            if run.status != "PASS":
                continue
            context = _context_for(base, run, batch.prefix)
            signals = _scan_logs(context)
            if signals:
                findings.append(self._run_finding(base, batch, context, signals, budget))
        findings += _zoo_findings(base, batch)
        findings += _leak_findings(base, batch)
        return findings

    def _ask_claude(
        self, budget: list[int], focus: FuzzerRunContext, signals: list[FuzzerSignal], *,
        others: list[FuzzerRunContext], batch: BatchFolder, note: str,
    ) -> tuple[dict[str, Any], str | None]:
        if budget[0] <= 0:
            return {}, None
        budget[0] -= 1
        try:
            with tempfile.TemporaryDirectory(prefix="fuzzer-") as td:
                return _invoke_claude(
                    focus, signals, Path(td), others=others, summary_text=batch.summary_text, finding_note=note,
                ), None
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
            logger.warning("Claude analysis failed for %s: %s", focus.job_id, exc, exc_info=True)
            return {}, str(exc)

    def _signature_finding(
        self, base: dict[str, Any], batch: BatchFolder, group: SignatureGroup, budget: list[int],
    ) -> FuzzerFinding:
        contexts: list[FuzzerRunContext] = []
        signals: list[FuzzerSignal] = []
        for run_id in group.runs:
            member = batch.run(run_id)
            if member is None:
                signals.append(FuzzerSignal(
                    _REPORT_UNREADABLE, "critical",
                    f"the signature group names run {run_id}, which summary.json does not list",
                ))
                continue
            context = _context_for(base, member, batch.prefix)
            contexts.append(context)
            if context.report_problem:
                signals.append(FuzzerSignal(_REPORT_UNREADABLE, "critical", context.report_problem))
        readable = [c for c in contexts if c.report is not None]
        focus = (readable or contexts or [FuzzerRunContext(**base, batch_prefix=batch.prefix)])[0]
        signals = _dedupe_signals(signals + _scan_logs(focus))
        rerun = _rerun_signal(group)
        if rerun:
            signals.append(rerun)
        unreadable = any(s.title == _REPORT_UNREADABLE for s in signals)

        rerun_detail = f"{group.reruns_reproduced}/{group.reruns_ran}" if group.rerun_verdict else None
        note = (
            f"{len(group.runs)} run(s) failed with one failure signature, `{group.key}`: {group.label}\n"
            f"Runs: {', '.join(group.runs)}."
            + (f"\nSerial re-runs: {group.rerun_verdict} ({rerun_detail} reproduced the signature)."
               if group.rerun_verdict else "\nNo serial re-runs were made.")
        )
        others = [c for c in contexts if c is not focus][: _MAX_CLAUDE_RUNS - 1]
        payload, claude_error = self._ask_claude(budget, focus, signals, others=others, batch=batch, note=note)
        signals = _with_claude_anomalies(signals, payload)
        run_status = group.status or (focus.report or {}).get("status")
        overall_status, verdict = _final_verdict(payload, signals, unreadable=unreadable, run_status=run_status)
        verdict = _rerun_adjusted(verdict, group.rerun_verdict, run_status)

        default_summary = (
            f"{len(group.runs)} run(s) of {group.experiment or 'an unknown experiment'} failed with the same "
            f"signature: {group.label}"
        )
        return FuzzerFinding(
            **base, kind="signature", overall_status=overall_status, triage_verdict=verdict,
            summary=_summary(payload, default_summary, claude_error),
            anomalies=signals,
            scenario_id=focus.scenario_id, seed=focus.seed, tested_valkey_sha=focus.tested_valkey_sha,
            experiment_id=focus.experiment_id or group.experiment, run_status=run_status,
            root_cause_category=_root_cause(payload),
            reproduction_hint=_hint(payload) or _reproduction_hint(focus),
            incident_fingerprint=compute_fingerprint(
                namespace=(base["repo"], base["workflow_file"], "signature", group.key), shapes=[],
            ),
            suggested_labels=["possible-valkey-bug"] if verdict in _BUG_VERDICTS else [],
            batch_runs=list(group.runs), signature_key=group.key, signature_label=group.label,
            rerun_verdict=group.rerun_verdict, rerun_detail=rerun_detail,
        )

    def _run_finding(
        self, base: dict[str, Any], batch: BatchFolder, context: FuzzerRunContext,
        signals: list[FuzzerSignal], budget: list[int],
    ) -> FuzzerFinding:
        """A run the fuzzer passed whose report or logs still show something."""
        note = f"Run {context.job_id} passed, but the quick scan found the signals below."
        payload, claude_error = self._ask_claude(budget, context, signals, others=[], batch=batch, note=note)
        signals = _with_claude_anomalies(signals, payload)
        unreadable = context.report_problem is not None
        run_status = (context.report or {}).get("status")
        overall_status, verdict = _final_verdict(payload, signals, unreadable=unreadable, run_status=run_status)
        root_cause = _root_cause(payload)
        return FuzzerFinding(
            **base, kind="run", overall_status=overall_status, triage_verdict=verdict,
            summary=_summary(payload, f"Run {context.job_id}: {len(signals)} anomalies", claude_error),
            anomalies=signals,
            scenario_id=context.scenario_id, seed=context.seed, tested_valkey_sha=context.tested_valkey_sha,
            experiment_id=context.experiment_id, run_status=run_status,
            root_cause_category=root_cause,
            reproduction_hint=_hint(payload) or _reproduction_hint(context),
            incident_fingerprint=compute_fingerprint(
                namespace=(base["repo"], base["workflow_file"], root_cause or ""),
                shapes=[f"{a.title}:{a.evidence}" for a in signals],
            ),
            suggested_labels=["possible-valkey-bug"] if verdict in _BUG_VERDICTS else [],
            batch_runs=[context.job_id],
        )


def _with_claude_anomalies(signals: list[FuzzerSignal], payload: dict[str, Any]) -> list[FuzzerSignal]:
    out = list(signals)
    for raw in payload.get("anomalies") or []:
        if isinstance(raw, dict) and raw.get("title"):
            severity = raw.get("severity")
            out.append(FuzzerSignal(
                str(raw["title"]),
                severity if severity in ("warning", "critical") else "warning",
                str(raw.get("evidence", "")),
            ))
    return _dedupe_signals(out)


def _summary(payload: dict[str, Any], default: str, claude_error: str | None) -> str:
    raw = payload.get("summary")
    summary = (raw if isinstance(raw, str) else "").strip() or default
    if claude_error:
        summary += f" The AI analysis failed, so this is the quick scan's verdict: {claude_error}"
    return summary


def _root_cause(payload: dict[str, Any]) -> str | None:
    value = payload.get("root_cause_category")
    return value if isinstance(value, str) and value else None


def _hint(payload: dict[str, Any]) -> str | None:
    value = payload.get("reproduction_hint")
    return value if isinstance(value, str) and value else None


def _incomplete_batch(base: dict[str, Any], batch: BatchFolder) -> FuzzerFinding:
    """The batch stopped before every job finished."""
    return FuzzerFinding(
        **base, kind="batch", title="[fuzzer-run] Batch did not finish",
        overall_status="warning", triage_verdict="needs-human-triage",
        summary=(
            f"The batch did not finish: {batch.finished} of {batch.jobs} jobs ran (status {batch.status}). "
            "It may have hit the workflow's timeout or been stopped; the runs it finished are analyzed "
            "on their own, and the rest were never run."
        ),
        anomalies=[FuzzerSignal("Batch incomplete", "warning", f"{batch.finished} of {batch.jobs} jobs finished")],
        incident_fingerprint=compute_fingerprint(
            namespace=(base["repo"], base["workflow_file"], "incomplete"), shapes=[],
        ),
    )


def _build_error_analysis(base: dict[str, Any], reason: str) -> FuzzerFinding:
    """The artifact or its summary was missing or unreadable, so nothing could be analyzed."""
    return FuzzerFinding(
        **base, kind="batch", overall_status="warning", triage_verdict="needs-human-triage",
        summary=f"Run {base['run_id']}: {reason}",
        incident_fingerprint=compute_fingerprint(
            namespace=(base["repo"], base["workflow_file"], "error"), shapes=[reason],
        ),
        analyzer_incomplete=True,
    )
