"""Strict schemas for fuzzer AI output and publisher-facing analysis."""

from __future__ import annotations

import re
from typing import Any

from scripts.fuzzer.models import FuzzerRunAnalysis, FuzzerSignal

AI_SCHEMA_VERSION = 1

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
MAX_ANALYSIS_ANOMALIES = 100
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
_TESTED_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{20}$")
_RESERVED_MARKER_RE = re.compile(r"<!--\s*valkey-ci-agent:", re.IGNORECASE)
_ALLOWED_LABELS = frozenset({"possible-valkey-bug"})


class FuzzerSchemaError(ValueError):
    """Raised when fuzzer analysis data violates its schema."""


def validate_ai_payload(value: Any) -> dict[str, Any]:
    """Validate and normalize the complete Claude response."""
    data = _exact_mapping(value, _AI_KEYS, "AI response")
    if type(data["schema_version"]) is not int or data["schema_version"] != AI_SCHEMA_VERSION:
        raise FuzzerSchemaError("AI response has an unsupported schema_version")

    category = _optional_text(
        data["root_cause_category"], "root_cause_category", MAX_CATEGORY_BYTES,
    )
    if category is not None and not _CATEGORY_RE.fullmatch(category):
        raise FuzzerSchemaError("root_cause_category must be a lowercase short label")

    raw_anomalies = data["anomalies"]
    if not isinstance(raw_anomalies, list) or len(raw_anomalies) > MAX_AI_ANOMALIES:
        raise FuzzerSchemaError(
            f"anomalies must be a list with at most {MAX_AI_ANOMALIES} entries",
        )
    anomalies = [
        _validate_ai_anomaly(raw, index)
        for index, raw in enumerate(raw_anomalies)
    ]

    return {
        "schema_version": AI_SCHEMA_VERSION,
        "overall_status": _enum(
            data["overall_status"], OVERALL_STATUSES, "overall_status",
        ),
        "triage_verdict": _enum(
            data["triage_verdict"], TRIAGE_VERDICTS, "triage_verdict",
        ),
        "root_cause_category": category,
        "summary": _text(
            data["summary"], "summary", MAX_SUMMARY_BYTES, allow_empty=False,
        ),
        "anomalies": anomalies,
        "reproduction_hint": _optional_text(
            data["reproduction_hint"], "reproduction_hint", MAX_REPRODUCTION_BYTES,
        ),
    }


def validate_analysis(
    analysis: FuzzerRunAnalysis,
    *,
    expected_repo: str | None = None,
    expected_workflow_file: str | None = None,
    expected_run_id: int | None = None,
) -> None:
    """Reject invalid data before an analysis is rendered or published."""
    if not isinstance(analysis, FuzzerRunAnalysis):
        raise FuzzerSchemaError("analysis must be a FuzzerRunAnalysis")
    if not _REPO_RE.fullmatch(analysis.repo):
        raise FuzzerSchemaError("repo is not an owner/repository name")
    if not _WORKFLOW_RE.fullmatch(analysis.workflow_file):
        raise FuzzerSchemaError("workflow_file is invalid")
    if type(analysis.run_id) is not int or analysis.run_id <= 0:
        raise FuzzerSchemaError("run_id must be a positive integer")
    _bounded_text(analysis.run_url, "run_url", 2_048, allow_empty=False)
    expected_run_url = (
        f"https://github.com/{analysis.repo}/actions/runs/{analysis.run_id}"
    )
    if analysis.run_url != expected_run_url:
        raise FuzzerSchemaError("run_url does not match repo and run_id")
    if expected_repo is not None and analysis.repo != expected_repo:
        raise FuzzerSchemaError("analysis repo does not match the requested run")
    if (
        expected_workflow_file is not None
        and analysis.workflow_file != expected_workflow_file
    ):
        raise FuzzerSchemaError("analysis workflow does not match the requested run")
    if expected_run_id is not None and analysis.run_id != expected_run_id:
        raise FuzzerSchemaError("analysis run_id does not match the requested run")
    _bounded_text(analysis.conclusion, "conclusion", 100, allow_empty=True)
    if not _SHA_RE.fullmatch(analysis.head_sha):
        raise FuzzerSchemaError("head_sha must be a lowercase 40-character SHA")

    _enum(analysis.overall_status, OVERALL_STATUSES, "overall_status")
    _enum(analysis.triage_verdict, TRIAGE_VERDICTS, "triage_verdict")
    _bounded_text(analysis.summary, "summary", MAX_SUMMARY_BYTES, allow_empty=False)

    if (
        not isinstance(analysis.anomalies, list)
        or len(analysis.anomalies) > MAX_ANALYSIS_ANOMALIES
    ):
        raise FuzzerSchemaError(
            f"analysis anomalies must have at most {MAX_ANALYSIS_ANOMALIES} entries",
        )
    for index, signal in enumerate(analysis.anomalies):
        _validate_signal(signal, index)

    _optional_bounded_text(analysis.scenario_id, "scenario_id", 240)
    _optional_bounded_text(analysis.seed, "seed", 240)
    if analysis.tested_valkey_sha is not None and not (
        isinstance(analysis.tested_valkey_sha, str)
        and _TESTED_SHA_RE.fullmatch(analysis.tested_valkey_sha)
    ):
        raise FuzzerSchemaError("tested_valkey_sha must be a 7-40 character lowercase SHA")

    _optional_bounded_text(
        analysis.root_cause_category, "root_cause_category", MAX_CATEGORY_BYTES,
    )
    if (
        analysis.root_cause_category is not None
        and not _CATEGORY_RE.fullmatch(analysis.root_cause_category)
    ):
        raise FuzzerSchemaError("root_cause_category must be a lowercase short label")
    _optional_bounded_text(
        analysis.reproduction_hint, "reproduction_hint", MAX_REPRODUCTION_BYTES,
    )

    if not (
        isinstance(analysis.incident_fingerprint, str)
        and _FINGERPRINT_RE.fullmatch(analysis.incident_fingerprint)
    ):
        raise FuzzerSchemaError("incident_fingerprint must be a lowercase 20-character hash")
    labels = analysis.suggested_labels
    if (
        not isinstance(labels, list)
        or not all(isinstance(label, str) for label in labels)
        or len(set(labels)) != len(labels)
        or not set(labels).issubset(_ALLOWED_LABELS)
    ):
        raise FuzzerSchemaError("suggested_labels is invalid")
    if not isinstance(analysis.analyzer_incomplete, bool):
        raise FuzzerSchemaError("analyzer_incomplete must be boolean")


def _validate_ai_anomaly(value: Any, index: int) -> dict[str, str]:
    label = f"anomalies[{index}]"
    anomaly = _exact_mapping(value, _ANOMALY_KEYS, label)
    return {
        "title": _single_line_text(
            anomaly["title"], f"{label}.title",
            MAX_SIGNAL_TITLE_BYTES, allow_empty=False,
        ),
        "severity": _enum(
            anomaly["severity"], SIGNAL_SEVERITIES, f"{label}.severity",
        ),
        "evidence": _text(
            anomaly["evidence"], f"{label}.evidence",
            MAX_SIGNAL_EVIDENCE_BYTES, allow_empty=False,
        ),
    }


def _validate_signal(signal: Any, index: int) -> None:
    if not isinstance(signal, FuzzerSignal):
        raise FuzzerSchemaError(f"analysis anomalies[{index}] must be a FuzzerSignal")
    _single_line_bounded_text(
        signal.title, f"analysis anomalies[{index}].title",
        MAX_SIGNAL_TITLE_BYTES, allow_empty=False,
    )
    _enum(
        signal.severity, SIGNAL_SEVERITIES,
        f"analysis anomalies[{index}].severity",
    )
    _bounded_text(
        signal.evidence, f"analysis anomalies[{index}].evidence",
        MAX_SIGNAL_EVIDENCE_BYTES, allow_empty=False,
    )


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
    _bounded_text(normalized, label, max_bytes, allow_empty=allow_empty)
    return normalized


def _optional_text(value: Any, label: str, max_bytes: int) -> str | None:
    if value is None:
        return None
    return _text(value, label, max_bytes, allow_empty=False)


def _single_line_text(
    value: Any,
    label: str,
    max_bytes: int,
    *,
    allow_empty: bool,
) -> str:
    normalized = _text(
        value,
        label,
        max_bytes,
        allow_empty=allow_empty,
    )
    if "\n" in normalized or "\r" in normalized:
        raise FuzzerSchemaError(f"{label} must be a single line")
    return normalized


def _bounded_text(
    value: Any,
    label: str,
    max_bytes: int,
    *,
    allow_empty: bool,
) -> None:
    if not isinstance(value, str):
        raise FuzzerSchemaError(f"{label} must be a string")
    if not allow_empty and not value:
        raise FuzzerSchemaError(f"{label} must not be empty")
    if "\x00" in value:
        raise FuzzerSchemaError(f"{label} must not contain NUL")
    if _RESERVED_MARKER_RE.search(value):
        raise FuzzerSchemaError(
            f"{label} must not contain reserved automation markers"
        )
    if len(value.encode("utf-8")) > max_bytes:
        raise FuzzerSchemaError(f"{label} exceeds {max_bytes} bytes")


def _single_line_bounded_text(
    value: Any,
    label: str,
    max_bytes: int,
    *,
    allow_empty: bool,
) -> None:
    _bounded_text(value, label, max_bytes, allow_empty=allow_empty)
    if "\n" in value or "\r" in value:
        raise FuzzerSchemaError(f"{label} must be a single line")


def _optional_bounded_text(value: Any, label: str, max_bytes: int) -> None:
    if value is not None:
        _bounded_text(value, label, max_bytes, allow_empty=False)
