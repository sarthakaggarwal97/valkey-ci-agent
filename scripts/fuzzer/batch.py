"""Read a valkey-fuzzer batch folder: its summary and every run's report.

``valkey-fuzzer batch --out DIR`` writes ``DIR/summary.json`` and, for each
run, ``DIR/runs/<id>/`` with ``report.json`` (an ordinary run report),
``scenario.yaml`` and ``logs/``. ``summary.json`` lists the runs (``runs``,
where ``report`` is the run report's path in the folder, or null when the run
left none), the non-PASS runs grouped by failure signature with their serial
re-run verdicts (``signatures``), the bug zoo's verdicts (``zoo``) and what
the sweeps removed after the runs (``leaks``).

The fuzzer monitor reads the scheduled workflow's artifact, which holds the
folder under ``batch/``; the campaign artifact is the folder itself. Both go
through :func:`read_batch`, which fails closed: a summary that is missing or
not shaped like one sets ``BatchFolder.problem``, and a run whose report is
missing or not a run report sets that run's ``report_problem``.
"""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass, field
from typing import Any

# Outcomes a run report, a run or a batch can carry.
RUN_STATUSES = frozenset({"PASS", "FAIL", "ERROR", "SKIP_UNSUPPORTED"})
RERUN_VERDICTS = frozenset({"reproduced", "intermittent", "failed differently", "not reproduced"})

# Every key a run report always carries.
REPORT_KEYS = (
    "generated_at", "status", "summary", "scenario", "outcome", "coverage", "reproducer", "provenance",
)

SUMMARY_FILE = "summary.json"


def report_problem(data: Any) -> str | None:
    """Why ``data`` is not a run report, or None when it is one.

    A run report is recognised by its structure, not by any version field:
    the top-level keys every report carries, a status the fuzzer can produce,
    and an outcome with its own status and the component results.
    """
    if not isinstance(data, dict):
        return f"the run report holds a {type(data).__name__}, not an object"
    missing = [key for key in REPORT_KEYS if key not in data]
    if missing:
        return f"the run report lacks {', '.join(missing)}"
    if data["status"] not in RUN_STATUSES:
        return f"the run report has unknown status {str(data['status'])[:40]!r}"
    outcome = data["outcome"]
    if not isinstance(outcome, dict):
        return "the run report's outcome is not an object"
    if "status" not in outcome:
        return "the run report's outcome lacks status"
    if not isinstance(outcome.get("component_results"), list):
        return "the run report's outcome has no component_results list"
    if outcome["status"] != data["status"]:
        return "the run report's status and outcome status disagree"
    return None


@dataclass
class BatchRun:
    """One entry of the summary's ``runs``, with its run folder's files."""

    id: str
    source: str
    status: str
    summary: str
    experiment: str | None = None
    seed: str | None = None
    # The run report's path in the batch folder; None when the run left none.
    report_path: str | None = None
    signature_key: str | None = None
    leaks: int = 0
    # The job a serial re-run repeats.
    origin: str | None = None
    report: dict[str, Any] | None = None
    # The report as downloaded, so Claude sees exactly what the fuzzer wrote.
    report_text: str | None = None
    # Why the report cannot be used. Set means the run goes to a human.
    report_problem: str | None = None
    scenario_yaml: str | None = None
    # error.json: why a job could not be generated or compiled.
    error_text: str | None = None
    # Instance logs under logs/, by file name.
    logs: dict[str, str] = field(default_factory=dict)

    @property
    def folder(self) -> str:
        return f"runs/{self.id}"

    @property
    def is_zoo(self) -> bool:
        return self.source == "zoo"

    @property
    def is_serial(self) -> bool:
        return self.source == "serial"

    @property
    def leak_only(self) -> bool:
        """The run passed; the batch made it an ERROR only for what it left behind."""
        return bool(self.leaks) and self.status == "ERROR" and (self.report or {}).get("status") == "PASS"


@dataclass
class SignatureGroup:
    """Non-PASS runs that share the fuzzer's failure signature."""

    key: str
    label: str
    status: str
    experiment: str | None
    runs: list[str]
    # reproduced, intermittent, failed differently or not reproduced; None
    # without serial re-runs.
    rerun_verdict: str | None = None
    reruns_ran: int = 0
    reruns_reproduced: int = 0
    # The signatures of the re-runs that failed another way.
    rerun_other_labels: list[str] = field(default_factory=list)


@dataclass
class BatchFolder:
    """A batch folder as read; ``problem`` set means the summary is unusable."""

    # Where the folder sits in the artifact: "batch/" or "".
    prefix: str
    problem: str | None = None
    summary_text: str | None = None
    batch_id: str | None = None
    status: str | None = None
    complete: bool = False
    jobs: int = 0
    finished: int = 0
    counts: dict[str, int] = field(default_factory=dict)
    runs: list[BatchRun] = field(default_factory=list)
    groups: list[SignatureGroup] = field(default_factory=list)
    zoo: dict[str, Any] | None = None
    leaks: list[dict[str, Any]] = field(default_factory=list)

    def run(self, run_id: str) -> BatchRun | None:
        return next((r for r in self.runs if r.id == run_id), None)

    @property
    def main_runs(self) -> list[BatchRun]:
        """The batch's own jobs: not zoo runs, not serial re-runs."""
        return [r for r in self.runs if not r.is_zoo and not r.is_serial]


def _text(payload: bytes) -> str:
    return payload.decode("utf-8", errors="replace")


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _safe_id(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and "/" not in value and "\\" not in value and value not in (".", "..")


def _summary_problem(data: Any) -> str | None:
    """Why ``data`` is not a batch summary, or None when it is one."""
    if not isinstance(data, dict):
        return f"summary.json holds a {type(data).__name__}, not an object"
    missing = [key for key in ("status", "complete", "counts", "runs", "signatures") if key not in data]
    if missing:
        return f"summary.json lacks {', '.join(missing)}"
    if data["status"] not in RUN_STATUSES:
        return f"summary.json has unknown status {str(data['status'])[:40]!r}"
    if not isinstance(data["runs"], list) or not isinstance(data["signatures"], list):
        return "summary.json runs and signatures must be lists"
    if not isinstance(data["counts"], dict):
        return "summary.json counts is not an object"
    for entry in data["runs"]:
        if not isinstance(entry, dict) or not _safe_id(entry.get("id")):
            return "summary.json has a run without a plain id"
        if entry.get("status") not in RUN_STATUSES:
            return f"summary.json run {entry['id']} has unknown status {str(entry.get('status'))[:40]!r}"
    ids = [entry["id"] for entry in data["runs"]]
    if len(set(ids)) != len(ids):
        return "summary.json lists a run twice"
    for group in data["signatures"]:
        if not isinstance(group, dict) or not isinstance(group.get("key"), str) \
                or not isinstance(group.get("runs"), list):
            return "summary.json has a signature group without a key and runs"
    if data.get("zoo") is not None:
        zoo = data["zoo"]
        if not isinstance(zoo, dict) or not isinstance(zoo.get("cases"), list):
            return "summary.json zoo is not an object with cases"
    if data.get("leaks") is not None and not isinstance(data["leaks"], list):
        return "summary.json leaks is not a list"
    return None


def _load_run(entry: dict[str, Any], files: dict[str, bytes], prefix: str) -> BatchRun:
    run = BatchRun(
        id=entry["id"], source=str(entry.get("source") or ""), status=entry["status"],
        summary=str(entry.get("summary") or ""),
        experiment=_optional_str(entry.get("experiment")), seed=_optional_str(entry.get("seed")),
        report_path=_optional_str(entry.get("report")),
        signature_key=_optional_str(entry.get("signature_key")),
        leaks=entry["leaks"] if isinstance(entry.get("leaks"), int) else 0,
        origin=_optional_str((entry.get("origin") or {}).get("job")) if isinstance(entry.get("origin"), dict) else None,
    )
    folder = f"{prefix}{run.folder}/"
    for path, payload in files.items():
        if not path.startswith(folder):
            continue
        rest = path[len(folder):]
        if rest == "scenario.yaml":
            run.scenario_yaml = _text(payload)
        elif rest == "error.json":
            run.error_text = _text(payload)
        elif rest.startswith("logs/") and rest.endswith(".log") and "/" not in rest[len("logs/"):]:
            run.logs[rest[len("logs/"):]] = _text(payload)

    if run.report_path is None:
        run.report_problem = f"the run left no run report ({run.status}: {run.summary[:200]})"
        return run
    normalized = posixpath.normpath(run.report_path)
    if normalized != run.report_path or not normalized.startswith(f"{run.folder}/"):
        run.report_problem = f"the summary points its report outside {run.folder}/: {run.report_path[:100]!r}"
        return run
    report_bytes = files.get(prefix + normalized)
    if report_bytes is None:
        run.report_problem = f"{normalized} is missing from the artifact"
        return run
    run.report_text = _text(report_bytes)
    try:
        data = json.loads(run.report_text)
    except ValueError as exc:
        run.report_problem = f"{normalized} is not valid JSON: {exc}"
        return run
    problem = report_problem(data)
    if problem is None and data["status"] != run.status and not (run.leaks and run.status == "ERROR"):
        problem = f"the summary says {run.status} but the run report says {data['status']}"
    if problem:
        run.report_problem = f"{normalized}: {problem}"
        return run
    run.report = data
    return run


def _groups(data: dict[str, Any], batch: BatchFolder) -> list[SignatureGroup]:
    """The summary's signature groups, plus one per non-PASS run none of them holds."""
    groups: list[SignatureGroup] = []
    grouped: set[str] = set()
    for raw in data["signatures"]:
        runs = [str(r) for r in raw["runs"]]
        grouped.update(runs)
        reruns = raw.get("serial_reruns") if isinstance(raw.get("serial_reruns"), dict) else {}
        verdict = reruns.get("verdict")
        ran = reruns.get("runs")
        reproduced = reruns.get("reproduced")
        others = [
            str(r.get("signature_label") or r.get("signature_key") or r.get("status"))
            for r in (ran if isinstance(ran, list) else [])
            if isinstance(r, dict) and r.get("same_signature") is False and r.get("status") != "PASS"
        ]
        groups.append(SignatureGroup(
            key=raw["key"], label=str(raw.get("label") or raw["key"]),
            status=str(raw.get("status") or ""), experiment=_optional_str(raw.get("experiment")),
            runs=runs,
            rerun_verdict=verdict if verdict in RERUN_VERDICTS else None,
            reruns_ran=len(ran) if isinstance(ran, list) else 0,
            reruns_reproduced=reproduced if isinstance(reproduced, int) else 0,
            rerun_other_labels=list(dict.fromkeys(others)),
        ))
    for run in batch.main_runs:
        if run.status != "PASS" and run.id not in grouped:
            groups.append(SignatureGroup(
                key=run.signature_key or f"run-{run.id}", label=f"{run.status} | {run.experiment} | {run.summary}",
                status=run.status, experiment=run.experiment, runs=[run.id],
            ))
    return groups


def read_batch(files: dict[str, bytes], prefix: str = "") -> BatchFolder:
    """Read the batch folder at ``prefix`` ("batch/" or "") in ``files``."""
    batch = BatchFolder(prefix=prefix)
    payload = files.get(prefix + SUMMARY_FILE)
    if payload is None:
        batch.problem = f"the artifact has no {prefix}{SUMMARY_FILE}"
        return batch
    batch.summary_text = _text(payload)
    try:
        data = json.loads(batch.summary_text)
    except ValueError as exc:
        batch.problem = f"{prefix}{SUMMARY_FILE} is not valid JSON: {exc}"
        return batch
    problem = _summary_problem(data)
    if problem:
        batch.problem = problem
        return batch
    batch.batch_id = _optional_str(data.get("batch_id"))
    batch.status = data["status"]
    batch.complete = data["complete"] is True
    batch.jobs = data["jobs"] if isinstance(data.get("jobs"), int) else 0
    batch.finished = data["finished"] if isinstance(data.get("finished"), int) else 0
    batch.counts = {str(k): v for k, v in data["counts"].items() if isinstance(v, int)}
    batch.runs = [_load_run(entry, files, prefix) for entry in data["runs"]]
    batch.groups = _groups(data, batch)
    batch.zoo = data.get("zoo")
    batch.leaks = [item for item in data.get("leaks") or [] if isinstance(item, dict)]
    return batch

