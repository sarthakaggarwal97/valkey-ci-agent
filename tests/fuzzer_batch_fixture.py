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
import shutil
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


# A batch folder with spec conform output, from two runs of a valkey-fuzzer soak (19b-model-conformance-soak)
# of the cluster-election bundle: summary.json, conformance/cluster-election.json and, per run, report.json,
# scenario.yaml and conform.json. SPEC_VIOLATED broke NoVoteLostInCrash under its claim, at a ReceiveVote step
# read from a Valkey log line; SPEC_HOLDS broke it only outside its claim. Kept as written, but: the summary and
# the bundle report count only these two runs, host paths are the workflow's, each report lost its plan's
# resolved components, component detail, reproducer and coverage, and the bundle is marked approved (the soak
# ran it as a draft) so the folder holds one violated approved claim.
SPEC_BATCH = Path(__file__).parent / "fixtures" / "fuzzer" / "spec-batch"
SPEC_VIOLATED = "random-valkey.cluster-vote-save-crash-s19103"
SPEC_HOLDS = "random-valkey.cluster-election-durability-s19102"
SPEC_LOG_LINE = ("15362:S 13 Nov 2026 01:22:18.460 * Failover auth ACK from aeae0a6e4f4a6777f5097d5d758b83c096a2cd1a "
                 "(127.0.0.1:11000) for epoch 15 (ACKs 1, quorum 3)")


def spec_batch_as_draft(root: Path) -> Path:
    """A copy of the spec batch under ``root`` with the bundle a draft again, as the soak ran it."""
    folder = root / "spec-batch"
    shutil.copytree(SPEC_BATCH, folder)
    for path in [folder / "summary.json", *folder.glob("runs/*/conform.json")]:
        data = json.loads(path.read_text())
        entry = data["conformance"] if path.name == "summary.json" else data
        entry["cluster-election"]["approved"] = False
        path.write_text(json.dumps(data))
    return folder
