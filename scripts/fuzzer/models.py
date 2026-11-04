"""Data models for the fuzzer analysis pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# The ID prefix of an experiment the proposals wrote for one change; a PR batch
# keeps such experiments in its folder's experiments/.
PROPOSED_PREFIX = "proposed."


@dataclass
class FuzzerSignal:
    """A single anomaly observed in a fuzzer run.

    `severity` is "critical" or "warning".
    """

    title: str
    severity: str
    evidence: str


@dataclass
class ChangeOutcome:
    """How a run of ``valkey.action.commands`` or ``valkey.action.tools`` met the change it tests.

    `reason` is the component's ``diagnostics.reason``: ``unreached-change``
    (a command marked as changed replied the same on the merge commit and
    its parent), ``unexplained-difference`` (an unmarked command replied
    differently; `commands` names them) or ``expect-disputed`` (an expect
    both builds violate alike; `commands` names them), or None; for tools, a
    command is a tool run. `sequence` is its `evidence_id` evidence,
    ``valkey.command.sequence`` or ``valkey.tool.runs``: both builds' replies
    or runs, and the ``reached`` and ``unexplained`` indexes.
    """

    component_id: str
    reason: str | None = None
    commands: list[Any] = field(default_factory=list)
    sequence: dict[str, Any] | None = None
    evidence_id: str = "valkey.command.sequence"

    @property
    def reached(self) -> bool:
        """A marked command reproduced the old behaviour on the parent build."""
        return bool(self.sequence and self.sequence.get("reached"))

    @property
    def candidate_exited(self) -> bool:
        """The merge commit's server died during the sequence or the tool runs."""
        candidate = (self.sequence or {}).get("candidate")
        return isinstance(candidate, dict) and (
            candidate.get("exited") is True or candidate.get("server_exited") is not None
        )


@dataclass
class FuzzerRunContext:
    """Mutable evidence bag for one run of the batch, built from its run folder."""

    repo: str
    workflow_file: str
    run_id: int
    run_url: str
    conclusion: str
    head_sha: str
    # The batch's ID for the run (its folder under runs/), and where the batch
    # folder sits in the artifact ("batch/").
    job_id: str = ""
    batch_prefix: str = ""
    scenario_id: str | None = None
    seed: str | None = None
    tested_valkey_sha: str | None = None
    experiment_id: str | None = None
    # The fuzzer's run report (runs/<id>/report.json), once it was read and has
    # the shape of a run report. `report_text` keeps the file as downloaded so
    # Claude sees exactly what the fuzzer wrote.
    report: dict[str, Any] | None = None
    report_text: str | None = None
    # Why the report could not be used: missing, not JSON, or not shaped like
    # a run report. Set means the run must go to a human, never "normal".
    report_problem: str | None = None
    scenario_yaml: str | None = None
    error_text: str | None = None
    node_logs: dict[str, str] = field(default_factory=dict)


@dataclass
class FuzzerFinding:
    """One problem the batch showed, published as one issue.

    `kind` is "signature" (non-PASS runs sharing the fuzzer's failure
    signature), "run" (a run that passed but whose report or logs show
    something), "zoo" (a bug zoo case the fuzzer no longer catches), "leak"
    (what runs left on the host) or "batch" (the batch itself could not be
    read or did not finish).
    """

    repo: str
    workflow_file: str
    run_id: int
    run_url: str
    conclusion: str
    head_sha: str
    kind: str
    overall_status: str  # "normal", "warning", "anomalous"
    triage_verdict: str
    summary: str
    anomalies: list[FuzzerSignal] = field(default_factory=list)
    # The issue title, when the finding names its own.
    title: str | None = None
    # What the batch tested, when the caller names it, such as a merged pull request.
    change: str | None = None
    scenario_id: str | None = None
    seed: str | None = None
    tested_valkey_sha: str | None = None
    experiment_id: str | None = None
    # The fuzzer's own outcome: PASS, FAIL, ERROR or SKIP_UNSUPPORTED.
    run_status: str | None = None
    root_cause_category: str | None = None
    reproduction_hint: str | None = None
    incident_fingerprint: str | None = None
    suggested_labels: list[str] = field(default_factory=list)
    # True when the analyzer itself could not produce a verdict (missing or
    # unreadable artifacts). Distinct from a completed analysis whose verdict
    # happens to be "needs-human-triage".
    analyzer_incomplete: bool = False
    # A problem in valkey-fuzzer itself (a zoo case it missed, a leak): never
    # a Valkey bug.
    fuzzer_side: bool = False
    # The batch's runs this finding covers, and its failure signature.
    batch_runs: list[str] = field(default_factory=list)
    signature_key: str | None = None
    signature_label: str | None = None
    # The serial re-runs' verdict ("reproduced", "intermittent", "failed
    # differently", "not reproduced") and how many reproduced, e.g. "2/3".
    rerun_verdict: str | None = None
    rerun_detail: str | None = None


@dataclass
class FuzzerBatchAnalysis:
    """Everything the monitor found in one scheduled fuzzer workflow run."""

    repo: str
    workflow_file: str
    run_id: int
    run_url: str
    conclusion: str
    head_sha: str
    # The batch's own worst result and runs by status; None when unreadable.
    batch_status: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    findings: list[FuzzerFinding] = field(default_factory=list)
    # Runs that tested a change with valkey.action.commands: how many reached
    # it (a marked command reproduced on the parent build), errored without
    # reaching it, showed an unexplained difference, and how many FAILs of a
    # proposed experiment were withheld as unvetted.
    change_runs: dict[str, int] = field(
        default_factory=lambda: {"reached": 0, "unreached": 0, "unexplained": 0, "disputed": 0, "unvetted": 0},
    )
    # The runs the monitor judged but did not file as findings (an unreached
    # change, an intended or untriaged difference, an unvetted FAIL), with
    # why and their evidence, for the replay's results.
    recorded: list[dict[str, Any]] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        order = ("normal", "warning", "anomalous")
        return max((f.overall_status for f in self.findings), key=order.index, default="normal")
