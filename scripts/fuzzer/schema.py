"""Versioned schemas for fuzzer AI output and phase results."""

from __future__ import annotations

import re
from typing import Any

from scripts.fuzzer.models import FuzzerRunAnalysis, FuzzerSignal

AI_SCHEMA_VERSION = 1
ANALYSIS_SCHEMA_VERSION = 1
OVERALL_STATUSES = frozenset({"normal", "warning", "anomalous"})
TRIAGE_VERDICTS = frozenset({
    "likely-core-valkey-bug",
    "possible-core-valkey-bug",
    "expected-chaos-noise",
    "environmental-or-infra",
    "needs-human-triage",
})
SIGNAL_SEVERITIES = frozenset({"warning", "critical"})

MAX_AI_ANOMALIES = 25
MAX_SIGNAL_TITLE_BYTES = 240
MAX_SIGNAL_EVIDENCE_BYTES = 2_000
MAX_SUMMARY_BYTES = 4_000
MAX_CATEGORY_BYTES = 120
MAX_REPRODUCTION_BYTES = 2_000

_AI_KEYS = {
    "schema_version",
    "overall_status",
    "triage_verdict",
    "root_cause_category",
    "summary",
    "anomalies",
    "reproduction_hint",
}
_ANOMALY_KEYS = {"title", "severity", "evidence"}
_CATEGORY_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.ya?ml$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{20}$")
_LABEL_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_ALLOWED_LABELS = {"possible-valkey-bug"}
_ANALYSIS_KEYS = {
    "schema_version",
    "repo",
    "workflow_file",
    "run_id",
    "run_url",
    "conclusion",
    "head_sha",
    "overall_status",
    "triage_verdict",
    "summary",
    "anomalies",
    "scenario_id",
    "seed",
    "tested_valkey_sha",
    "root_cause_category",
    "reproduction_hint",
    "incident_fingerprint",
    "suggested_labels",
    "analyzer_incomplete",
}


class FuzzerSchemaError(ValueError):
    """Raised when untrusted fuzzer analysis data violates its schema."""


def validate_ai_payload(value: Any) -> dict[str, Any]:
    """Validate and normalize the complete Claude response."""
    data = _exact_mapping(value, _AI_KEYS, "AI response")
    if data["schema_version"] != AI_SCHEMA_VERSION:
        raise FuzzerSchemaError("AI response has an unsupported schema_version")
    overall_status = _enum(
        data["overall_status"], OVERALL_STATUSES, "overall_status",
    )
    triage_verdict = _enum(
        data["triage_verdict"], TRIAGE_VERDICTS, "triage_verdict",
    )
    summary = _text(data["summary"], "summary", MAX_SUMMARY_BYTES, allow_empty=False)
    category = _optional_text(
        data["root_cause_category"], "root_cause_category", MAX_CATEGORY_BYTES,
    )
    if category is not None and not _CATEGORY_RE.fullmatch(category):
        raise FuzzerSchemaError(
            "root_cause_category must be a lowercase short label",
        )
    reproduction = _optional_text(
        data["reproduction_hint"], "reproduction_hint", MAX_REPRODUCTION_BYTES,
    )

    raw_anomalies = data["anomalies"]
    if not isinstance(raw_anomalies, list) or len(raw_anomalies) > MAX_AI_ANOMALIES:
        raise FuzzerSchemaError(
            f"anomalies must be a list with at most {MAX_AI_ANOMALIES} entries",
        )
    anomalies: list[dict[str, str]] = []
    for index, raw in enumerate(raw_anomalies):
        anomaly = _exact_mapping(raw, _ANOMALY_KEYS, f"anomalies[{index}]")
        anomalies.append({
            "title": _text(
                anomaly["title"],
                f"anomalies[{index}].title",
                MAX_SIGNAL_TITLE_BYTES,
                allow_empty=False,
            ),
            "severity": _enum(
                anomaly["severity"],
                SIGNAL_SEVERITIES,
                f"anomalies[{index}].severity",
            ),
            "evidence": _text(
                anomaly["evidence"],
                f"anomalies[{index}].evidence",
                MAX_SIGNAL_EVIDENCE_BYTES,
                allow_empty=False,
            ),
        })

    return {
        "schema_version": AI_SCHEMA_VERSION,
        "overall_status": overall_status,
        "triage_verdict": triage_verdict,
        "root_cause_category": category,
        "summary": summary,
        "anomalies": anomalies,
        "reproduction_hint": reproduction,
    }


def analysis_to_dict(analysis: FuzzerRunAnalysis) -> dict[str, Any]:
    """Serialize an analysis and validate the publisher-facing representation."""
    raw = {
        "schema_version": ANALYSIS_SCHEMA_VERSION,
        "repo": analysis.repo,
        "workflow_file": analysis.workflow_file,
        "run_id": analysis.run_id,
        "run_url": analysis.run_url,
        "conclusion": analysis.conclusion,
        "head_sha": analysis.head_sha,
        "overall_status": analysis.overall_status,
        "triage_verdict": analysis.triage_verdict,
        "summary": analysis.summary,
        "anomalies": [
            {
                "title": signal.title,
                "severity": signal.severity,
                "evidence": signal.evidence,
            }
            for signal in analysis.anomalies
        ],
        "scenario_id": analysis.scenario_id,
        "seed": analysis.seed,
        "tested_valkey_sha": analysis.tested_valkey_sha,
        "root_cause_category": analysis.root_cause_category,
        "reproduction_hint": analysis.reproduction_hint,
        "incident_fingerprint": analysis.incident_fingerprint,
        "suggested_labels": analysis.suggested_labels,
        "analyzer_incomplete": analysis.analyzer_incomplete,
    }
    return _parse_analysis(raw)[1]


def analysis_from_dict(value: Any) -> FuzzerRunAnalysis:
    """Parse an analysis crossing from an untrusted phase into a publisher."""
    return _parse_analysis(value)[0]


def _parse_analysis(value: Any) -> tuple[FuzzerRunAnalysis, dict[str, Any]]:
    data = _exact_mapping(value, _ANALYSIS_KEYS, "fuzzer analysis")
    if data["schema_version"] != ANALYSIS_SCHEMA_VERSION:
        raise FuzzerSchemaError("fuzzer analysis has an unsupported schema_version")
    repo = _text(data["repo"], "repo", 200, allow_empty=False)
    if not _REPO_RE.fullmatch(repo):
        raise FuzzerSchemaError("repo is not an owner/repository name")
    workflow = _text(data["workflow_file"], "workflow_file", 200, allow_empty=False)
    if not _WORKFLOW_RE.fullmatch(workflow):
        raise FuzzerSchemaError("workflow_file is invalid")
    run_id = data["run_id"]
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
        raise FuzzerSchemaError("run_id must be a positive integer")
    run_url = _text(data["run_url"], "run_url", 2_048, allow_empty=False)
    if not run_url.startswith("https://github.com/"):
        raise FuzzerSchemaError("run_url must be an https://github.com URL")
    conclusion = _text(data["conclusion"], "conclusion", 100, allow_empty=False)
    head_sha = _sha(data["head_sha"], "head_sha")
    overall_status = _enum(data["overall_status"], OVERALL_STATUSES, "overall_status")
    triage_verdict = _enum(data["triage_verdict"], TRIAGE_VERDICTS, "triage_verdict")
    summary = _text(data["summary"], "summary", MAX_SUMMARY_BYTES, allow_empty=False)

    raw_anomalies = data["anomalies"]
    if not isinstance(raw_anomalies, list) or len(raw_anomalies) > MAX_AI_ANOMALIES + 20:
        raise FuzzerSchemaError("analysis anomalies has too many entries")
    anomalies: list[FuzzerSignal] = []
    normalized_anomalies: list[dict[str, str]] = []
    for index, raw in enumerate(raw_anomalies):
        item = _exact_mapping(raw, _ANOMALY_KEYS, f"analysis anomalies[{index}]")
        title = _text(
            item["title"], f"analysis anomalies[{index}].title",
            MAX_SIGNAL_TITLE_BYTES, allow_empty=False,
        )
        severity = _enum(
            item["severity"], SIGNAL_SEVERITIES,
            f"analysis anomalies[{index}].severity",
        )
        evidence = _text(
            item["evidence"], f"analysis anomalies[{index}].evidence",
            MAX_SIGNAL_EVIDENCE_BYTES, allow_empty=False,
        )
        anomalies.append(FuzzerSignal(title, severity, evidence))
        normalized_anomalies.append({
            "title": title,
            "severity": severity,
            "evidence": evidence,
        })

    scenario_id = _optional_text(data["scenario_id"], "scenario_id", 240)
    seed = _optional_text(data["seed"], "seed", 240)
    tested_sha = data["tested_valkey_sha"]
    if tested_sha is not None:
        tested_sha = _sha(tested_sha, "tested_valkey_sha")
    category = _optional_text(
        data["root_cause_category"], "root_cause_category", MAX_CATEGORY_BYTES,
    )
    if category is not None and not _CATEGORY_RE.fullmatch(category):
        raise FuzzerSchemaError("root_cause_category is invalid")
    reproduction = _optional_text(
        data["reproduction_hint"], "reproduction_hint", MAX_REPRODUCTION_BYTES,
    )
    fingerprint = data["incident_fingerprint"]
    if fingerprint is not None and (
        not isinstance(fingerprint, str) or not _FINGERPRINT_RE.fullmatch(fingerprint)
    ):
        raise FuzzerSchemaError("incident_fingerprint is invalid")
    labels = data["suggested_labels"]
    if (
        not isinstance(labels, list)
        or len(labels) > 10
        or not all(isinstance(label, str) and _LABEL_RE.fullmatch(label) for label in labels)
        or len(set(labels)) != len(labels)
        or not set(labels).issubset(_ALLOWED_LABELS)
    ):
        raise FuzzerSchemaError("suggested_labels is invalid")
    incomplete = data["analyzer_incomplete"]
    if not isinstance(incomplete, bool):
        raise FuzzerSchemaError("analyzer_incomplete must be boolean")

    normalized = {
        "schema_version": ANALYSIS_SCHEMA_VERSION,
        "repo": repo,
        "workflow_file": workflow,
        "run_id": run_id,
        "run_url": run_url,
        "conclusion": conclusion,
        "head_sha": head_sha,
        "overall_status": overall_status,
        "triage_verdict": triage_verdict,
        "summary": summary,
        "anomalies": normalized_anomalies,
        "scenario_id": scenario_id,
        "seed": seed,
        "tested_valkey_sha": tested_sha,
        "root_cause_category": category,
        "reproduction_hint": reproduction,
        "incident_fingerprint": fingerprint,
        "suggested_labels": list(labels),
        "analyzer_incomplete": incomplete,
    }
    analysis = FuzzerRunAnalysis(
        repo=repo,
        workflow_file=workflow,
        run_id=run_id,
        run_url=run_url,
        conclusion=conclusion,
        head_sha=head_sha,
        overall_status=overall_status,
        triage_verdict=triage_verdict,
        summary=summary,
        anomalies=anomalies,
        scenario_id=scenario_id,
        seed=seed,
        tested_valkey_sha=tested_sha,
        root_cause_category=category,
        reproduction_hint=reproduction,
        incident_fingerprint=fingerprint,
        suggested_labels=list(labels),
        analyzer_incomplete=incomplete,
    )
    return analysis, normalized


def _exact_mapping(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise FuzzerSchemaError(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        raise FuzzerSchemaError(
            f"{label} keys invalid: unknown={sorted(actual - expected)}, "
            f"missing={sorted(expected - actual)}",
        )
    return value


def _enum(value: Any, allowed: frozenset[str], label: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise FuzzerSchemaError(f"{label} is not an allowed value")
    return value


def _text(
    value: Any,
    label: str,
    max_bytes: int,
    *,
    allow_empty: bool,
) -> str:
    if not isinstance(value, str):
        raise FuzzerSchemaError(f"{label} must be a string")
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise FuzzerSchemaError(f"{label} must not be empty")
    if len(normalized.encode("utf-8")) > max_bytes:
        raise FuzzerSchemaError(f"{label} exceeds {max_bytes} bytes")
    return normalized


def _optional_text(value: Any, label: str, max_bytes: int) -> str | None:
    if value is None:
        return None
    return _text(value, label, max_bytes, allow_empty=False)


def _sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA_RE.fullmatch(value):
        raise FuzzerSchemaError(f"{label} must be a lowercase 40-character SHA")
    return value
