"""Tests for the PR batch workflow's inputs and for reading the batch it uploads."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scripts.fuzzer.pr_batch as pr_batch_mod
from scripts.fuzzer.batch import read_batch, read_batch_dir
from scripts.fuzzer.pr_batch import (
    BATCH_INPUTS,
    OPTIONAL_INPUTS,
    RESULTS_FILE,
    batch_dispatch_inputs,
    batch_names,
    bundle_path_allowed,
    check_batch_inputs,
    render_results,
    scenario_name,
    summarize,
)
from tests.fuzzer_batch_fixture import BATCH, batch_files, batch_summary, with_summary

HEAD = "a" * 40
SCENARIO = textwrap.dedent("""\
    scenario_id: ai-0123456789ab
    seed: 7
    experiment: {id: valkey.cluster-chaos}
    deployment: {mode: cluster, primaries: 3, replicas_per_primary: 1}
""")
WITH_BINARY = SCENARIO.replace("replicas_per_primary: 1}", "replicas_per_primary: 1, valkey_binary: /tmp/v}")
# The smallest bundle the input check takes; the workflow also loads it with
# the fuzzer's bundle checks, which the real-workflow tests run on a real one.
BUNDLE = {"claims.yaml": "protocol: toy\n", "model.tla": "---- MODULE model ----\n====\n", "mapping.yaml": "a: 1\n"}


def _inputs(**changes) -> dict[str, str]:
    inputs = {
        "scenarios": json.dumps({"ai-0123456789ab": SCENARIO}),
        "valkey_repository": "valkey-io/valkey",
        "valkey_ref": "refs/pull/123/head",
        "valkey_sha": HEAD,
    }
    inputs.update(changes)
    return inputs


def _scenarios(value) -> dict[str, str]:
    return _inputs(scenarios=value if isinstance(value, str) else json.dumps(value))


def _bundles(value) -> dict[str, str]:
    return _inputs(spec_bundles=value if isinstance(value, str) else json.dumps(value))


def test_batch_inputs_are_the_ones_the_workflow_takes():
    assert BATCH_INPUTS == ("scenarios", "valkey_repository", "valkey_ref", "valkey_sha")
    assert OPTIONAL_INPUTS == ("spec_bundles",)
    check_batch_inputs(_inputs())
    check_batch_inputs(_bundles({"toy": BUNDLE}))
    check_batch_inputs(_bundles(""))
    change = {"repo": "valkey-io/valkey", "pr_number": 123, "head_sha": HEAD}
    inputs = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO})
    assert inputs == _inputs(scenarios=json.dumps({"ai-0123456789ab": SCENARIO}, separators=(",", ":")))
    with_bundle = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO}, {"toy": BUNDLE})
    assert json.loads(with_bundle["spec_bundles"]) == {"toy": BUNDLE}
    for_range = batch_dispatch_inputs({"repo": "valkey-io/valkey", "pr_number": None, "head_sha": HEAD},
                                      {"ai-0123456789ab": SCENARIO})
    assert for_range["valkey_ref"] == HEAD


def test_batch_names_keep_a_fitting_id_and_fix_the_rest():
    ids = ["ai-0123456789ab", "spec-register:lost-write", "spec-register-lost-write", "..x..y", ":::", "s" * 120,
           "s" * 130]
    names = batch_names(ids)
    assert names["ai-0123456789ab"] == "ai-0123456789ab"
    assert names["spec-register:lost-write"] == "spec-register-lost-write"
    assert names["spec-register-lost-write"] == "spec-register-lost-write-2"
    assert names["..x..y"] == "x.y"
    assert names[":::"] == "scenario"
    assert names["s" * 120] == "s" * 100
    assert names["s" * 130] == "s" * 98 + "-2"
    assert len(set(names.values())) == len(ids)
    for name in names.values():
        check_batch_inputs(_scenarios({name: SCENARIO}))


@pytest.mark.parametrize("path,allowed", [
    ("claims.yaml", True), ("model.tla", True), ("mapping.yaml", True), ("model.cfg", True),
    ("negative-controls/ack.tla", True), ("negative-controls/ack.cfg", True), ("experiments/toy.register.yaml", True),
    ("experiments/toy.yml", True), ("notes.md", False), ("negative-controls/x.yaml", False),
    ("experiments/../x.yaml", False), ("sub/claims.yaml", False), ("experiments/a..b.yaml", False),
])
def test_bundle_paths_the_batch_takes(path, allowed):
    assert bundle_path_allowed(path) is allowed


@pytest.mark.parametrize("inputs,message", [
    ({k: v for k, v in _inputs().items() if k != "valkey_ref"}, "takes scenarios, valkey_repository"),
    ({**_inputs(), "profile": "candidate"}, "takes scenarios, valkey_repository"),
    (_inputs(valkey_ref=None), "valkey_ref must be a string"),
    (_scenarios("x" * 60_001), "scenarios is 60001 characters; the limit is 60,000"),
    (_bundles("x" * 60_001), "spec_bundles is 60001 characters; the limit is 60,000"),
    (_inputs(valkey_repository="valkey"), "valkey_repository must be OWNER/NAME"),
    (_inputs(valkey_repository="valkey-io/valkey/x"), "valkey_repository must be OWNER/NAME"),
    (_inputs(valkey_sha="a" * 12), "valkey_sha must be a full 40-character lowercase commit SHA"),
    (_inputs(valkey_sha="A" * 40), "valkey_sha must be a full 40-character lowercase commit SHA"),
    (_scenarios("{not json"), "scenarios is not JSON"),
    (_scenarios({}), "must be a non-empty JSON object"),
    (_scenarios([SCENARIO]), "must be a non-empty JSON object"),
    (_scenarios({"../x": SCENARIO}), "scenario name '../x' must be"),
    (_scenarios({"a..b": SCENARIO}), "scenario name 'a..b' must be"),
    (_scenarios({"-x": SCENARIO}), "scenario name '-x' must be"),
    (_scenarios({"a b": SCENARIO}), "scenario name 'a b' must be"),
    (_scenarios({"a" * 101: SCENARIO}), "must be letters, digits"),
    (_scenarios({"x": {"seed": 1}}), "scenario x: the value must be YAML text"),
    (_scenarios({"x": "a: [b"}), "scenario x is not YAML"),
    (_scenarios({"x": "- a"}), "scenario x must be a mapping"),
    (_scenarios({"x": WITH_BINARY}), "sets deployment.valkey_binary; the batch runs valkey_sha's build"),
    (_bundles("{not json"), "spec_bundles is not JSON"),
    (_bundles([BUNDLE]), "spec_bundles must be a JSON object mapping a bundle name to its files"),
    (_bundles({"Toy": BUNDLE}), "spec bundle name 'Toy' must be lowercase letters, digits, or '-'"),
    (_bundles({"toy_1": BUNDLE}), "spec bundle name 'toy_1' must be"),
    (_bundles({"t" * 65: BUNDLE}), "must be lowercase letters"),
    (_bundles({"toy": {}}), "spec bundle toy: the value must map paths in the bundle to file text"),
    (_bundles({"toy": "claims.yaml"}), "spec bundle toy: the value must map paths"),
    (_bundles({"toy": {k: v for k, v in BUNDLE.items() if k != "model.tla"}}), "spec bundle toy has no model.tla"),
    (_bundles({"toy": {**BUNDLE, "notes.md": "x"}}), "spec bundle toy: 'notes.md' must be claims.yaml"),
    (_bundles({"toy": {**BUNDLE, "experiments/a..b.yaml": "id: a\n"}}), "'experiments/a..b.yaml' must be"),
    (_bundles({"toy": {**BUNDLE, "model.cfg": 1}}), "spec bundle toy: model.cfg must be file text"),
    (_bundles({"toy": {**BUNDLE, "mapping.yaml": "a: [b"}}), "spec bundle toy: mapping.yaml is not YAML"),
    (_bundles({"toy": {**BUNDLE, "experiments/x.yaml": "- a"}}), "spec bundle toy: experiments/x.yaml must be a mapping"),
    (_inputs(valkey_ref="r" * 7_000, scenarios=json.dumps({"x": SCENARIO + "#" * 59_000})),
     "over GitHub's limit of 65,535"),
])
def test_inputs_the_pr_batch_would_refuse_are_refused(inputs, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        check_batch_inputs(inputs)


# --- Against the real workflow ---------------------------------------------------
# Set VALKEY_FUZZER_DIR to a valkey-fuzzer checkout to check against its own
# fuzzer-pr-batch.yml, and its register bundle.

_FUZZER_DIR = os.environ.get("VALKEY_FUZZER_DIR", "")
_WORKFLOW = Path(_FUZZER_DIR) / ".github" / "workflows" / "fuzzer-pr-batch.yml"
_real_workflow = pytest.mark.skipif(
    not (_FUZZER_DIR and _WORKFLOW.is_file()), reason="needs VALKEY_FUZZER_DIR with its fuzzer-pr-batch.yml",
)


def _workflow() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text())


def _register() -> dict[str, str]:
    root = Path(_FUZZER_DIR) / "tests" / "fixtures" / "spec" / "register"
    return {p.relative_to(root).as_posix(): p.read_text() for p in sorted(root.rglob("*")) if p.is_file()}


def _workflow_check(inputs: dict[str, str], tmp_path: Path) -> subprocess.CompletedProcess:
    """Run the workflow's own input check on ``inputs``, as its step runs it."""
    steps = _workflow()["jobs"]["batch"]["steps"]
    step = next(s for s in steps if s.get("name") == "Check the inputs and write the scenarios and bundles")
    script = step["run"].split("<<'PY'\n", 1)[1].rsplit("PY", 1)[0]
    env = {k: os.environ[k] for k in ("PATH", "HOME") if k in os.environ}
    # An input left out reaches the step as its default, "".
    env.update({f"INPUT_{k.upper()}": inputs.get(k, "") for k in BATCH_INPUTS + OPTIONAL_INPUTS})
    env.update(CANDIDATES_DIR=str(tmp_path / "candidates"), SPEC_DIR=str(tmp_path / "spec"), PYTHONPATH=_FUZZER_DIR)
    return subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=False)


@_real_workflow
def test_the_real_workflow_takes_exactly_these_inputs():
    workflow = _workflow()
    assert workflow["name"] == "Valkey Fuzzer PR Batch"
    on = workflow.get("on", workflow.get(True))
    inputs = on["workflow_dispatch"]["inputs"]
    assert set(inputs) == set(BATCH_INPUTS + OPTIONAL_INPUTS)
    assert {name for name, spec in inputs.items() if spec.get("required")} == {"scenarios", "valkey_sha"}
    assert inputs["spec_bundles"]["default"] == ""
    upload = next(s for s in workflow["jobs"]["batch"]["steps"] if s.get("name") == "Upload the batch folder")
    assert upload["with"]["name"] == "fuzzer-batch-${{ github.run_id }}"
    assert upload["with"]["path"] == "${{ env.BATCH_DIR }}"
    assert pr_batch_mod.BATCH_ARTIFACT_PREFIX == "fuzzer-batch-"
    assert pr_batch_mod.PR_BATCH_WORKFLOW == _WORKFLOW.name


def _real_cases() -> list:
    if not _FUZZER_DIR or not _WORKFLOW.is_file():
        return []
    register = _register()
    return [
        _inputs(),
        _bundles({"register": register}),
        _bundles(""),
        _scenarios({"a..b": SCENARIO}),
        _inputs(valkey_sha="a" * 12),
        _inputs(valkey_sha="A" * 40),
        _scenarios({"x": WITH_BINARY}),
        _scenarios({"x": "- a"}),
        _scenarios({}),
        _bundles("x" * 60_001),
        _bundles("[]"),
        _bundles({"Register": register}),
        _bundles({"register": {k: v for k, v in register.items() if k != "mapping.yaml"}}),
        _bundles({"register": {**register, "notes.md": "x"}}),
        _bundles({"register": {**register, "../model.cfg": "x"}}),
        _bundles({"register": {**register, "experiments/x.yaml": "- a"}}),
        _bundles({"register": {**register, "model.cfg": 1}}),
    ]


@_real_workflow
@pytest.mark.parametrize("inputs", _real_cases())
def test_the_real_workflow_agrees_with_the_check(inputs, tmp_path):
    ours: str | None = None
    try:
        check_batch_inputs(inputs)
    except ValueError as exc:
        ours = str(exc)
    theirs = _workflow_check(inputs, tmp_path)
    if ours is None:
        assert theirs.returncode == 0, theirs.stderr
        assert sorted(p.name for p in (tmp_path / "candidates").iterdir()) == ["ai-0123456789ab.yaml"]
        bundles = json.loads(inputs.get("spec_bundles") or "{}")
        for name, files in bundles.items():
            written = tmp_path / "spec" / name
            assert {p.relative_to(written).as_posix() for p in written.rglob("*") if p.is_file()} == set(files)
    else:
        assert theirs.returncode != 0
        assert theirs.stderr.strip().splitlines()[-1] == ours


# --- Results -------------------------------------------------------------------


def _pr_batch(summary=None) -> dict[str, bytes]:
    """The fixture batch as a PR batch artifact holds it: the folder at the root."""
    files = batch_files(prefix="")
    return with_summary(files, summary, prefix="") if summary is not None else files


def _without_unknown() -> dict:
    summary = batch_summary()
    summary["runs"] = [r for r in summary["runs"] if "unknown" not in r["id"]]
    summary["signatures"] = [g for g in summary["signatures"] if g["key"] != "9ab128c1af68"]
    return summary


def _passing() -> dict:
    summary = batch_summary()
    summary["runs"] = [r for r in summary["runs"] if r["status"] == "PASS"]
    summary.update(status="PASS", counts={"PASS": 2}, signatures=[], jobs=2, finished=2)
    return summary


def test_scenario_names_come_back_from_the_job_ids():
    assert scenario_name("scenario-ai-0123456789ab") == "ai-0123456789ab"
    assert scenario_name("serial-scenario-ai-0123456789ab") is None
    assert scenario_name("random-valkey.cluster-chaos-s1") is None


def test_a_pr_batch_is_read_as_a_batch_folder():
    results = summarize(read_batch(_pr_batch(_without_unknown())))
    assert results["verdict"] == "fail" and results["problems"] == []
    assert results["batch"]["counts"] == {"PASS": 2, "FAIL": 2, "ERROR": 2}
    crash = next(r for r in results["runs"] if r["run"] == "scenario-crash-777")
    assert (crash["scenario"], crash["status"], crash["report"]) == (
        "crash-777", "FAIL", "runs/scenario-crash-777/report.json",
    )
    group = next(g for g in results["signatures"] if g["key"] == "cc08b3931ccc")
    assert group["runs"] == ["scenario-crash-777", "scenario-crash-778"]
    assert (group["rerun_verdict"], group["reruns"]) == ("reproduced", "1/1")
    summary = render_results(results, "run 5")
    assert "Verdict: **fail**" in summary
    assert "| `scenario-crash-777` | crash-777 | FAIL |" in summary
    assert "serial-scenario-crash-777" not in summary
    assert "| 2 | reproduced (1/1) |" in summary


def test_a_passing_pr_batch_passes():
    results = summarize(read_batch(_pr_batch(_passing())))
    assert (results["verdict"], results["problems"]) == ("pass", [])


def test_a_run_without_a_readable_report_needs_a_human():
    results = summarize(read_batch(_pr_batch()))
    assert results["verdict"] == "needs-human-triage"
    assert results["problems"][0].startswith("run scenario-unknown: the run left no run report")


@pytest.mark.parametrize("change,problem", [
    (lambda s: s.update(complete=False, finished=1), "the batch did not finish: 1 of 2 jobs ran"),
    (lambda s: s.update(zoo={"valkey_commit": HEAD, "cases": [{"case": "c1", "holds": False}], "skipped": []}),
     "zoo case c1 does not hold"),
    (lambda s: s.update(leaks=[{"run": s["runs"][0]["id"], "kind": "server", "target": "1"}]),
     "left 1 leftover(s) on the host"),
])
def test_a_pr_batch_the_fuzzer_cannot_vouch_for_needs_a_human(change, problem):
    summary = _passing()
    change(summary)
    results = summarize(read_batch(_pr_batch(summary)))
    assert results["verdict"] == "needs-human-triage"
    assert any(problem in p for p in results["problems"])


def test_an_unreadable_pr_batch_summary_is_an_error():
    with pytest.raises(ValueError, match="the batch folder cannot be read: summary.json is not valid JSON"):
        summarize(read_batch(_pr_batch("{not json")))


def _downloaded(tmp_path: Path, summary=None) -> Path:
    folder = tmp_path / "pr-batch"
    shutil.copytree(BATCH, folder)
    (folder / "runs" / "scenario-crash-777" / "components").mkdir()
    (folder / "runs" / "scenario-crash-777" / "components" / "ledger.jsonl").write_text("{}\n")
    if summary is not None:
        (folder / "summary.json").write_text(json.dumps(summary))
    return folder


def test_a_downloaded_folder_is_read_without_its_component_artifacts(tmp_path):
    batch = read_batch_dir(_downloaded(tmp_path))
    assert batch.problem is None
    assert batch.run("scenario-crash-777").report is not None
    assert read_batch_dir(tmp_path / "missing").problem.endswith("holds no summary.json")


def test_results_command_reads_a_downloaded_folder(tmp_path, monkeypatch):
    step_summary = tmp_path / "step-summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(step_summary))
    out = tmp_path / "out"
    assert pr_batch_mod.main(["--batch", str(_downloaded(tmp_path)), "--out", str(out)]) == 1
    results = json.loads((out / RESULTS_FILE).read_text())
    assert results["verdict"] == "needs-human-triage"
    assert "## Fuzzer PR batch" in step_summary.read_text()

    passing = _downloaded(tmp_path / "p", _passing())
    assert pr_batch_mod.main(["--batch", str(passing), "--out", str(out)]) == 0
    assert pr_batch_mod.main(["--batch", str(tmp_path / "nothing"), "--out", str(out)]) == 1


def test_results_command_downloads_a_pr_batch_run(tmp_path, monkeypatch):
    monkeypatch.delenv("TARGET_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        pr_batch_mod.main(["--run-id", "5"])
    seen = {}

    def download(gh, token, repo, run_id):
        seen.update(token=token, repo=repo, run_id=run_id)
        return _pr_batch(_passing())

    monkeypatch.setenv("TARGET_TOKEN", "t")
    monkeypatch.setattr(pr_batch_mod, "download_batch", download)
    import github
    monkeypatch.setattr(github, "Github", MagicMock())
    assert pr_batch_mod.main(["--run-id", "5", "--out", str(tmp_path)]) == 0
    assert seen == {"token": "t", "repo": "valkey-io/valkey-fuzzer", "run_id": 5}


def test_download_batch_takes_only_that_runs_artifact(monkeypatch):
    from scripts.common import workflow_artifacts
    from scripts.common.workflow_artifacts import WorkflowArtifact

    client = MagicMock()
    monkeypatch.setattr(workflow_artifacts, "ArtifactClient", lambda gh, token: client)
    client.list_run_artifacts.return_value = [
        WorkflowArtifact(1, "fuzzer-batch-4", 1, False), WorkflowArtifact(2, "fuzzer-batch-5", 1, True),
    ]
    with pytest.raises(ValueError, match="has no fuzzer-batch-5 artifact"):
        pr_batch_mod.download_batch(MagicMock(), "t", "valkey-io/valkey-fuzzer", 5)
    client.list_run_artifacts.return_value = [WorkflowArtifact(3, "fuzzer-batch-5", 1, False)]
    client.download_artifact.return_value = {"summary.json": b"{}"}
    assert pr_batch_mod.download_batch(MagicMock(), "t", "valkey-io/valkey-fuzzer", 5) == {"summary.json": b"{}"}
    client.download_artifact.assert_called_with("valkey-io/valkey-fuzzer", 3)
    client.download_artifact.return_value = {}
    with pytest.raises(ValueError, match="empty or unreadable"):
        pr_batch_mod.download_batch(MagicMock(), "t", "valkey-io/valkey-fuzzer", 5)
