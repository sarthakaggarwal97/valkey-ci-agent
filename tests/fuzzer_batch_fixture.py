"""The real batch folder under tests/fixtures/fuzzer/batch, as an artifact holds it.

valkey-fuzzer wrote it with

    valkey-fuzzer batch --random valkey.aof-durability --seeds 2 --first-seed 11 \\
      --scenarios scen --experiment-dir exp --workers 1 --serial-reruns 1

where scen/ held two scenarios of an experiment whose action crashes the
primary with DEBUG SEGFAULT (FAIL, one signature, reproduced by its serial
re-run), one whose server cannot start (ERROR with a report) and one naming
an experiment that does not exist (ERROR, no report). Only summary.json and
each run's report.json, scenario.yaml, error.json and logs/ were kept, with
paths rewritten to the scheduled workflow's.
"""
from __future__ import annotations

import json
from pathlib import Path

BATCH = Path(__file__).parent / "fixtures" / "fuzzer" / "batch"
CRASH = "scenario-crash-777"
CRASH_REPORT = f"runs/{CRASH}/report.json"


def batch_files(prefix: str = "batch/") -> dict[str, bytes]:
    """The fixture batch folder as an artifact holds it, under ``prefix``."""
    return {f"{prefix}{p.relative_to(BATCH).as_posix()}": p.read_bytes() for p in BATCH.rglob("*") if p.is_file()}


def batch_summary() -> dict:
    return json.loads((BATCH / "summary.json").read_text())


def with_summary(files: dict[str, bytes], summary, prefix: str = "batch/") -> dict[str, bytes]:
    files = dict(files)
    files[f"{prefix}summary.json"] = summary.encode() if isinstance(summary, str) else json.dumps(summary).encode()
    return files


def with_report(files: dict[str, bytes], path: str, report, prefix: str = "batch/") -> dict[str, bytes]:
    files = dict(files)
    files[f"{prefix}{path}"] = report.encode() if isinstance(report, str) else json.dumps(report).encode()
    return files


def crash_report() -> dict:
    return json.loads((BATCH / CRASH_REPORT).read_text())
