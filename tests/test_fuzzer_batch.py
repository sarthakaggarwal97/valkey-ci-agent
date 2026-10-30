"""Tests for reading a valkey-fuzzer batch folder (see tests/fuzzer_batch_fixture.py)."""
from __future__ import annotations

import copy

import pytest

from scripts.fuzzer.batch import REPORT_KEYS, read_batch
from tests.fuzzer_batch_fixture import (
    BATCH,
    CRASH,
    CRASH_REPORT,
    batch_files,
    batch_summary,
    crash_report,
    with_report,
    with_summary,
)


def test_reads_the_real_batch():
    batch = read_batch(batch_files(), "batch/")
    assert batch.problem is None
    assert (batch.batch_id, batch.status, batch.complete, batch.jobs, batch.finished) == (
        "ciagent-fixture", "FAIL", True, 6, 6,
    )
    assert batch.counts == {"PASS": 2, "FAIL": 2, "ERROR": 2}
    assert [r.id for r in batch.main_runs] == [
        "random-valkey.aof-durability-s11", "random-valkey.aof-durability-s12",
        "scenario-crash-777", "scenario-crash-778", "scenario-noboot", "scenario-unknown",
    ]
    crash = batch.run(CRASH)
    assert crash is not None and crash.report_problem is None
    assert crash.report["status"] == "FAIL" and crash.report_text == (BATCH / CRASH_REPORT).read_text()
    assert set(crash.logs) == {"777-3088e5fa-valkey-0.log", "777-3088e5fa-valkey-0.stdio.log"}
    assert "scenario_id: ciagent-crash-777" in crash.scenario_yaml
    rerun = batch.run("serial-scenario-crash-777")
    assert rerun.is_serial and rerun.origin == CRASH

    groups = {g.key: g for g in batch.groups}
    assert set(groups) == {"cc08b3931ccc", "846478c93288", "9ab128c1af68"}
    crash_group = groups["cc08b3931ccc"]
    assert crash_group.runs == [CRASH, "scenario-crash-778"]
    assert (crash_group.status, crash_group.experiment) == ("FAIL", "ciagent.debug-crash")
    assert (crash_group.rerun_verdict, crash_group.reruns_reproduced, crash_group.reruns_ran) == ("reproduced", 1, 1)
    assert crash_group.label.startswith("FAIL | ciagent.debug-crash | liveness:FAIL")


def test_a_run_that_left_no_report_fails_closed_but_keeps_its_error():
    unknown = read_batch(batch_files(), "batch/").run("scenario-unknown")
    assert unknown.report is None and unknown.report_path is None
    assert unknown.report_problem.startswith("the run left no run report (ERROR: ")
    assert "UnknownExperimentError" in unknown.error_text


def test_a_campaign_artifact_is_the_folder_itself():
    batch = read_batch(batch_files(prefix=""))
    assert batch.problem is None and batch.prefix == ""
    assert batch.run(CRASH).report is not None
    # The scheduled workflow's layout is not read at the root.
    assert read_batch(batch_files(), "").problem == "the artifact has no summary.json"


def _summary_with(**changes) -> dict:
    summary = batch_summary()
    summary.update(changes)
    return summary


def _first_run(**changes) -> dict:
    summary = batch_summary()
    summary["runs"][0].update(changes)
    return summary


_DUPLICATE = batch_summary()
_DUPLICATE["runs"].append(copy.deepcopy(_DUPLICATE["runs"][0]))


@pytest.mark.parametrize("summary,problem", [
    (None, "the artifact has no batch/summary.json"),
    ("{not json", "summary.json is not valid JSON"),
    ([], "holds a list"),
    ({"results": []}, "lacks status, complete, counts, runs, signatures"),
    (_summary_with(status="OK"), "unknown status 'OK'"),
    (_summary_with(runs={}), "runs and signatures must be lists"),
    (_summary_with(counts=[]), "counts is not an object"),
    (_first_run(id=None), "a run without a plain id"),
    (_first_run(id="../escape"), "a run without a plain id"),
    (_first_run(status="DONE"), "unknown status 'DONE'"),
    (_DUPLICATE, "lists a run twice"),
    (_summary_with(signatures=[{"label": "x"}]), "a signature group without a key and runs"),
    (_summary_with(zoo=[]), "zoo is not an object with cases"),
    (_summary_with(leaks={}), "leaks is not a list"),
])
def test_an_unusable_summary_fails_closed(summary, problem):
    files = batch_files()
    if summary is None:
        del files["batch/summary.json"]
    else:
        files = with_summary(files, summary)
    batch = read_batch(files, "batch/")
    assert problem in batch.problem
    assert batch.runs == [] and batch.groups == []


def _crash_without(key: str) -> dict:
    report = crash_report()
    del report[key]
    return report


def _crash_outcome(**changes) -> dict:
    report = crash_report()
    report["outcome"].update(changes)
    return report


def _crash_outcome_without(key: str) -> dict:
    report = crash_report()
    del report["outcome"][key]
    return report


@pytest.mark.parametrize("report,problem", [
    (None, f"{CRASH_REPORT} is missing from the artifact"),
    ("{not json", "is not valid JSON"),
    ([], "holds a list"),
    *[(_crash_without(key), f"lacks {key}") for key in REPORT_KEYS],
    ({**crash_report(), "status": "OK"}, "unknown status 'OK'"),
    ({**crash_report(), "outcome": "FAIL"}, "outcome is not an object"),
    (_crash_outcome_without("status"), "outcome lacks status"),
    (_crash_outcome_without("component_results"), "no component_results list"),
    (_crash_outcome(status="PASS"), "status and outcome status disagree"),
    ({**_crash_outcome(status="PASS"), "status": "PASS"}, "the summary says FAIL but the run report says PASS"),
])
def test_an_unreadable_run_report_fails_closed(report, problem):
    files = batch_files()
    if report is None:
        del files[f"batch/{CRASH_REPORT}"]
    else:
        files = with_report(files, CRASH_REPORT, report)
    run = read_batch(files, "batch/").run(CRASH)
    assert run.report is None
    assert problem in run.report_problem


@pytest.mark.parametrize("path", ["runs/scenario-crash-778/report.json", "runs/scenario-crash-777/../x/report.json",
                                  "/etc/passwd"])
def test_a_report_outside_its_run_folder_is_not_read(path):
    summary = batch_summary()
    next(r for r in summary["runs"] if r["id"] == CRASH)["report"] = path
    run = read_batch(with_summary(batch_files(), summary), "batch/").run(CRASH)
    assert run.report is None and "outside runs/scenario-crash-777/" in run.report_problem


def test_a_run_made_an_error_by_its_leaks_keeps_its_passing_report():
    summary = batch_summary()
    entry = summary["runs"][0]
    entry.update(status="ERROR", leaks=1)
    run = read_batch(with_summary(batch_files(), summary), "batch/").run(entry["id"])
    assert run.report_problem is None and run.report["status"] == "PASS"
    assert run.leak_only


def test_a_group_whose_reruns_failed_differently_names_their_signatures():
    summary = batch_summary()
    group = next(g for g in summary["signatures"] if g.get("serial_reruns"))
    group["serial_reruns"] = {"reproduced": 0, "verdict": "failed differently", "runs": [
        {"id": "serial-a", "status": "ERROR", "same_signature": False, "signature_key": "k1",
         "signature_label": "ERROR | x | Deadline during batch"},
        {"id": "serial-b", "status": "PASS", "same_signature": False, "signature_key": None, "signature_label": None},
    ]}
    read = next(g for g in read_batch(with_summary(batch_files(), summary), "batch/").groups if g.key == group["key"])
    assert (read.rerun_verdict, read.reruns_reproduced, read.reruns_ran) == ("failed differently", 0, 2)
    assert read.rerun_other_labels == ["ERROR | x | Deadline during batch"]


def test_a_non_pass_run_no_signature_holds_gets_a_group_of_its_own():
    summary = batch_summary()
    summary["signatures"] = [g for g in summary["signatures"] if g["key"] != "846478c93288"]
    groups = {g.key: g for g in read_batch(with_summary(batch_files(), summary), "batch/").groups}
    assert groups["846478c93288"].runs == ["scenario-noboot"]
    assert groups["846478c93288"].rerun_verdict is None
