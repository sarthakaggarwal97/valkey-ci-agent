"""Tests for fuzzer scenario proposals."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scripts.fuzzer.propose as propose_mod
from scripts.ai.runtime import AgentRunResult
from scripts.fuzzer.pr_batch import ModuleBuild
from scripts.fuzzer.propose import (
    PARENT_ARTIFACT_BLOCK,
    Change,
    ChangeRequest,
    artifacts_block,
    batch_inputs,
    built_test_modules,
    check_experiment,
    check_scenario,
    compiled_experiment_error,
    dispatch_batch,
    fetch_change,
    fuzzer_lister,
    fuzzer_validator,
    load_catalog,
    parse_request,
    pinned_valkey,
    propose,
    render_summary,
    touches_module_api,
    validated_scenarios,
    write_results,
)

HEAD = "a" * 40
BASE = "b" * 40

GOOD = textwrap.dedent("""\
    seed: 7
    experiment:
      id: valkey.cluster-chaos
      inputs:
        operations: [{shard: 0, force: false, timeout_s: 30}]
    deployment: {mode: cluster, primaries: 3, replicas_per_primary: 1, base_port: 7000}
""")
OTHER_GOOD = GOOD.replace("seed: 7", "seed: 8").replace("7000", "7100")
# The stub validator, like the fuzzer, rejects a shard at or above primaries.
BAD = GOOD.replace("shard: 0", "shard: 9")
COMPILER_ERROR = "Error: operations[0].shard 9 is not below primaries 3"

# A proposed experiment: a command sequence compared with the parent build.
PROPOSED = textwrap.dedent("""\
    extends: [valkey.liveness]
    id: proposed.lset
    deployment: {modes: [standalone]}
    components:
      - use: valkey.action.commands
        id: lset
        config:
          compare_with: {$ref: artifacts.parent.binary}
          commands: [{command: [PING], expect: {equals: PONG}}]
""")
USES_PROPOSED = "seed: 9\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone, base_port: 7200}\n"
# The stub lister, like the fuzzer, knows no such fragment.
BROKEN = PROPOSED.replace("valkey.liveness", "valkey.nope")
FRAGMENT_ERROR = "Error: proposed.lset.yaml: experiment extends unknown fragment 'valkey.nope'"
# What the installed fragments bring after expansion, as the stub lister sees them.
FRAGMENTS = {
    "valkey.liveness": [{"use": "valkey.oracle.liveness", "id": "liveness", "config": {}}],
    "valkey.reconnect-replicas": [
        {"use": "valkey.action.command", "id": "drop-replica-links",
         "config": {"command": ["CLIENT", "KILL", "TYPE", "replica"]}},
    ],
}


def _stub_validator(path: Path, experiment_dir: Path) -> str | None:
    text = path.read_text()
    if "shard: 9" in text:
        return COMPILER_ERROR
    experiment = yaml.safe_load(text)["experiment"]["id"]
    if experiment.startswith("proposed."):
        loaded = experiment_dir / f"{experiment}.yaml"
        if not loaded.is_file():
            return f"Error: scenario validation failed: unknown experiment '{experiment}'"
        if "artifacts.parent" in loaded.read_text() and "artifacts:" not in text:
            return "Error: scenario validation failed: references unknown environment target 'artifacts.parent'"
    return None


def _stub_lister(directory: Path) -> tuple[list[dict], str | None]:
    """The fuzzer's listing of a directory's experiments, with the fragments expanded."""
    entries = []
    for path in sorted(directory.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text())
        components: list[dict] = []
        for fragment in doc.get("extends", []):
            if fragment not in FRAGMENTS:
                return [], f"Error: {path.name}: experiment extends unknown fragment {fragment!r}"
            components += FRAGMENTS[fragment]
        components += doc.get("components", [])
        entries.append({"id": doc["id"], "components": components, "directory": str(directory)})
    return entries, None


def _answer(scenarios: list[dict], experiments: list[dict] | None = None, **rest) -> AgentRunResult:
    payload = {"scenarios": scenarios, **rest}
    if experiments is not None:
        payload["experiments"] = experiments
    return _result(json.dumps(payload))


def _result(stdout: str, rc: int = 0) -> AgentRunResult:
    return AgentRunResult(
        profile="fuzzer_proposal_readonly", stdout=stdout, stderr="" if rc == 0 else "boom",
        returncode=rc, prompt_sha256="x", cwd="", allowed_tools="Read,Grep,Glob", model="",
        started_at="", finished_at="",
    )


class StubRunner:
    """Stands in for Claude: hands back the queued answers, records the prompts."""

    def __init__(self, *answers: AgentRunResult) -> None:
        self.answers = list(answers)
        self.prompts: list[str] = []
        self.workdirs: list[Path] = []

    def __call__(self, prompt: str, workdir: Path) -> AgentRunResult:
        self.prompts.append(prompt)
        self.workdirs.append(workdir)
        # What Claude can see, captured while the workspace still exists.
        self.seen = sorted(
            str(p.relative_to(workdir)) for p in workdir.rglob("*")
            if p.is_file() and "valkey-fuzzer" not in p.parts[len(workdir.parts):][:1]
        )
        return self.answers.pop(0)


CATALOG = {
    "experiments": [{"id": "valkey.cluster-chaos", "deployment_modes": ["cluster"]}],
    "components": [{"component_id": "valkey.action.cluster-chaos", "role": "action"}],
}


def _change(**kw) -> Change:
    defaults = dict(repo="valkey-io/valkey", pr_number=123, title="Fix failover", base_sha=BASE,
                    head_sha=HEAD, diff="diff --git a/src/cluster.c b/src/cluster.c\n",
                    fetch_ref="refs/pull/123/head")
    defaults.update(kw)
    return Change(**defaults)


@pytest.fixture
def fuzzer_dir(tmp_path) -> Path:
    d = tmp_path / "valkey-fuzzer"
    (d / "docs").mkdir(parents=True)
    (d / "docs" / "scenarios-and-experiments.md").write_text("# Scenarios\n")
    (d / ".git").mkdir()
    (d / ".git" / "config").write_text("[core]\n")
    return d


@pytest.fixture(autouse=True)
def no_real_claude(monkeypatch):
    def refuse(*_a, **_kw):
        raise AssertionError("a test reached the real Claude runner")

    monkeypatch.setattr(propose_mod, "run_agent", refuse)


@pytest.fixture(autouse=True)
def no_clone(monkeypatch):
    calls: list[tuple] = []

    def fake_clone(repo, dest, sha=None, *, fetch_ref=None):
        calls.append((repo, sha, fetch_ref))
        return True

    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", fake_clone)
    return calls


def _propose(runner, tmp_path, fuzzer_dir, *, count=5, change=None, valkey_sha="", validator=None, extra=""):
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    run = propose(change or _change(), count=count, workdir=work, out_dir=out, catalog=CATALOG,
                  fuzzer_dir=fuzzer_dir, validator=validator or _stub_validator, lister=_stub_lister, runner=runner,
                  valkey_sha=valkey_sha, extra=extra)
    return run, write_results(run, out), out


# --- Requests ------------------------------------------------------------------


def test_parse_request_accepts_a_pr_or_a_range():
    assert parse_request("valkey-io/valkey", 42, None) == ChangeRequest("valkey-io/valkey", pr_number=42)
    assert parse_request("valkey-io/valkey", None, "9.0.0..unstable") == ChangeRequest(
        "valkey-io/valkey", base="9.0.0", head="unstable",
    )


@pytest.mark.parametrize("repo,pr,commit_range", [
    ("valkey-io/valkey", 42, "a..b"),
    ("valkey-io/valkey", None, None),
    ("valkey-io/valkey", 0, None),
    ("not a repo", 42, None),
    ("valkey-io/valkey", None, "a...b"),
    ("valkey-io/valkey", None, "--upload-pack=x..main"),
    ("valkey-io/valkey", None, "main"),
    ("valkey-io/valkey", None, "a..b..c"),
])
def test_parse_request_rejects_malformed_input(repo, pr, commit_range):
    with pytest.raises(ValueError):
        parse_request(repo, pr, commit_range)


def test_fetch_change_pins_a_pr_to_its_merge_base_and_fetches_its_head_ref(monkeypatch):
    gh = MagicMock()
    repo = gh.get_repo.return_value
    pull = repo.get_pull.return_value
    pull.head.sha, pull.base.sha, pull.title = HEAD, "c" * 40, "Fix failover"
    # The base branch moved on since the PR branched: the parent to build is the merge base.
    repo.compare.return_value.merge_base_commit.sha = BASE
    paths = []
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: paths.append(path) or b"diff --git x\n")
    change = fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=9))
    assert (change.head_sha, change.base_sha, change.fetch_ref) == (HEAD, BASE, "refs/pull/9/head")
    repo.compare.assert_called_once_with("c" * 40, HEAD)
    assert paths == ["/repos/valkey-io/valkey/pulls/9"]


def test_fetch_change_pins_a_range_to_its_merge_base_and_last_commit(monkeypatch):
    gh = MagicMock()
    comparison = gh.get_repo.return_value.compare.return_value
    comparison.merge_base_commit.sha = BASE
    comparison.commits = [MagicMock(sha="c" * 40), MagicMock(sha=HEAD)]
    paths = []
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: paths.append(path) or b"diff\n")
    change = fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", base="9.0", head="unstable"))
    assert (change.base_sha, change.head_sha, change.fetch_ref) == (BASE, HEAD, None)
    assert paths == [f"/repos/valkey-io/valkey/compare/{BASE}...{HEAD}"]


def test_fetch_change_cuts_a_huge_diff_and_rejects_an_empty_one(monkeypatch):
    gh = MagicMock()
    pull = gh.get_repo.return_value.get_pull.return_value
    pull.head.sha, pull.base.sha, pull.title = HEAD, BASE, ""
    gh.get_repo.return_value.compare.return_value.merge_base_commit.sha = BASE
    monkeypatch.setattr(propose_mod, "_MAX_DIFF_BYTES", 10)
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"x" * 11)
    change = fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))
    assert change.diff_truncated and len(change.diff) == 10
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"  \n")
    with pytest.raises(ValueError, match="empty diff"):
        fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))


@pytest.mark.parametrize("head,merge_base", [("--upload-pack=evil", BASE), (HEAD, "g" * 40)])
def test_fetch_change_rejects_a_malformed_commit(monkeypatch, head, merge_base):
    gh = MagicMock()
    pull = gh.get_repo.return_value.get_pull.return_value
    pull.head.sha, pull.base.sha = head, BASE
    gh.get_repo.return_value.compare.return_value.merge_base_commit.sha = merge_base
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"diff\n")
    with pytest.raises(ValueError, match="malformed commit"):
        fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))


# --- Checking experiments ------------------------------------------------------


def test_check_experiment_takes_a_proposed_experiment():
    doc, error = check_experiment(PROPOSED)
    assert error is None and doc["id"] == "proposed.lset"


@pytest.mark.parametrize("text,error", [
    (None, "not a YAML string"),
    ("", "not a YAML string"),
    ("- a", "not a YAML mapping"),
    ("id: [", "not valid YAML"),
    (PROPOSED.replace("proposed.lset", "valkey.lset"), "id must start with proposed."),
    (PROPOSED.replace("id: proposed.lset", "id: 7"), "id must start with proposed."),
    (PROPOSED.replace("proposed.lset", "proposed.Lset"), "lowercase letters"),
    (PROPOSED.replace("proposed.lset", "proposed.a..b"), "lowercase letters"),
    (PROPOSED.replace("proposed.lset", "proposed." + "l" * 100), "at most 100 characters"),
    (PROPOSED + "artifacts: {parent: {use: valkey.artifact.local-binary}}\n", "declares artifacts"),
    (PROPOSED + "resources: {}\npolicies: {}\n", "declares policies, resources"),
    (PROPOSED + "#" * 20_001, "longer than 20000"),
])
def test_check_experiment_rejects(text, error):
    doc, problem = check_experiment(text)
    assert doc is None
    assert error in problem


def _compiled(*components: dict, **rest) -> dict:
    return {"id": "proposed.x", "components": list(components), **rest}


def test_compiled_experiment_may_hold_only_the_parent_reference():
    assert compiled_experiment_error(_compiled(
        {"use": "valkey.action.commands", "config": {"compare_with": {"$ref": "artifacts.parent.binary"},
                                                     "resp": {"$ref": "inputs.resp"}}},
        {"use": "valkey.fault.kill", "config": {}},
    )) is None
    assert compiled_experiment_error(_compiled()) is None


@pytest.mark.parametrize("entry,error", [
    (_compiled({"use": "valkey.action.command", "config": {}}), "uses valkey.action.command, which sends any command"),
    (_compiled({"use": "valkey.fault.command", "config": {}}, {"use": "valkey.action.command", "config": {}}),
     "uses valkey.action.command, valkey.fault.command"),
    (_compiled({"use": "valkey.action.commands", "config": {"compare_with": {"$ref": "artifacts.baseline.binary"}}}),
     "references artifacts.baseline.binary; the only environment reference"),
    (_compiled({"use": "valkey.action.commands", "config": {"modules": [{"$ref": "resources.tmp.path"}]}}),
     "references resources.tmp.path"),
    # A reference anywhere in the compiled experiment, not only in a component's config.
    (_compiled(startup_overlays=[{"server_config": {"dir": {"$ref": "resources.disk.path"}}}]),
     "references resources.disk.path"),
    ({"id": "proposed.x"}, "without its components"),
])
def test_compiled_experiment_rejects(entry, error):
    assert error in compiled_experiment_error(entry)


# --- Checking scenarios --------------------------------------------------------


def test_check_scenario_names_it_by_content():
    doc, error = check_scenario(GOOD)
    assert error is None
    assert doc["scenario_id"].startswith("ai-") and len(doc["scenario_id"]) == 15
    # Claude's own scenario_id is replaced; the name depends only on the content.
    again, _ = check_scenario("scenario_id: mine\n" + GOOD)
    assert again["scenario_id"] == doc["scenario_id"]
    assert check_scenario(OTHER_GOOD)[0]["scenario_id"] != doc["scenario_id"]


@pytest.mark.parametrize("text,error", [
    (None, "not a YAML string"),
    ("", "not a YAML string"),
    ("[1, 2]", "not a YAML mapping"),
    ("seed: [", "not valid YAML"),
    (GOOD + "artifacts: {b: {use: valkey.artifact.local-binary}}\n", "sets artifacts"),
    (GOOD + "policies: {}\n", "sets policies"),
    (GOOD.replace("seed: 7", "seed: seven"), "seed must be an integer"),
    (GOOD.replace("seed: 7", "seed: true"), "seed must be an integer"),
    (GOOD.replace("base_port: 7000}", "base_port: 7000, valkey_binary: /tmp/evil}"), "valkey_binary"),
    (GOOD.replace("base_port: 7000}", "base_port: 7000, base_data_dir: /}"), "base_data_dir"),
    ("seed: 1\nexperiment: valkey.cluster-chaos\ndeployment: {mode: cluster}\n", "experiment must be"),
    ("seed: 1\nexperiment: {id: x}\ndeployment: cluster\n", "deployment must be"),
    (GOOD + "#" * 20_001, "longer than"),
])
def test_check_scenario_rejects(text, error):
    doc, problem = check_scenario(text)
    assert doc is None
    assert error in problem


# --- Proposing -----------------------------------------------------------------


def test_invalid_scenario_is_repaired_once_then_dropped(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "drives failover", "scenario": GOOD},
                 {"rationale": "shard out of range", "scenario": BAD}]),
        # The repair still fails to compile.
        _answer([{"index": 2, "rationale": "still wrong", "scenario": BAD.replace("seed: 7", "seed: 70")}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)

    assert len(runner.prompts) == 2  # one proposal round, one repair round, no more
    repair_prompt = runner.prompts[1]
    assert COMPILER_ERROR in repair_prompt
    assert "### Scenario 2" in repair_prompt and "### Scenario 1" not in repair_prompt
    assert "### Experiment" not in repair_prompt
    assert [s["rationale"] for s in result["scenarios"]] == ["drives failover"]
    assert result["scenarios"][0]["experiment"] == "valkey.cluster-chaos"
    assert result["dropped"] == [
        {"index": 2, "rationale": "still wrong", "error": COMPILER_ERROR, "repaired": True},
    ]
    assert (result["experiments"], result["dropped_experiments"], result["reason"]) == ([], [], None)
    assert sorted(p.name for p in (out / "scenarios").iterdir()) == [
        f"{result['scenarios'][0]['scenario_id']}.yaml",
    ]
    assert list((out / "experiments").iterdir()) == []


def test_repaired_scenario_is_kept_and_marked(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "a", "scenario": BAD}]),
        _answer([{"index": 1, "scenario": OTHER_GOOD}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert result["dropped"] == []
    assert result["scenarios"][0]["repaired"] is True
    assert result["scenarios"][0]["rationale"] == "a"


def test_only_validated_scenarios_are_output(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([
            {"rationale": "good", "scenario": GOOD},
            {"rationale": "binary", "scenario": GOOD.replace("base_port: 7000}", "base_port: 7000, valkey_binary: /x}")},
            {"rationale": "duplicate", "scenario": "scenario_id: copy\n" + GOOD},
            {"rationale": "no yaml"},
            "not even an object",
            {"rationale": "compiler", "scenario": BAD},
            {"rationale": "over the count", "scenario": OTHER_GOOD},
        ]),
        # The repair round answers with junk, an unknown index, and an index twice.
        _answer([{"index": 99, "scenario": OTHER_GOOD}, {"index": "2", "scenario": OTHER_GOOD},
                 {"index": 6, "scenario": BAD}, {"index": 6, "scenario": OTHER_GOOD}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, count=6)

    files = sorted(p.name for p in (out / "scenarios").iterdir())
    listed = sorted(Path(s["file"]).name for s in result["scenarios"])
    assert files == listed
    assert [s["rationale"] for s in result["scenarios"]] == ["good"]
    assert sorted(d["index"] for d in result["dropped"]) == [2, 3, 4, 5, 6]
    # Everything written is exactly what passed validation.
    for name in files:
        text = (out / "scenarios" / name).read_text()
        assert _stub_validator(out / "scenarios" / name, out / "experiments") is None
        assert yaml.safe_load(text)["scenario_id"] == name[:-len(".yaml")]
    assert "over the count" not in json.dumps(result)


def test_no_repair_round_when_everything_validates(tmp_path, fuzzer_dir, no_clone):
    runner = StubRunner(_answer([{"rationale": "a", "scenario": GOOD}, {"rationale": "b", "scenario": OTHER_GOOD}]))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, count=2)
    assert len(runner.prompts) == 1
    assert len(result["scenarios"]) == 2
    assert "up to 2 scenarios" in runner.prompts[0]
    # Claude works on a copy of the fuzzer, the diff and the catalog, and the
    # Valkey checkout at the PR's head.
    assert {"change.diff", "change.json", "catalog/experiments.json", "catalog/components.json"} <= set(runner.seen)
    workdir = runner.workdirs[0]
    assert no_clone == [("valkey-io/valkey", HEAD, "refs/pull/123/head")]
    assert str(fuzzer_dir) not in runner.prompts[0]


def test_an_extra_section_goes_before_the_answers_format(tmp_path, fuzzer_dir, no_clone):
    runner = StubRunner(_answer([{"rationale": "a", "scenario": GOOD}]))
    _propose(runner, tmp_path, fuzzer_dir, extra="## Lines the tests did not reach\nsrc/t_list.c:12\n")
    prompt = runner.prompts[0]
    assert "a resync.\n\n## Lines the tests did not reach\nsrc/t_list.c:12\n\nReturn ONLY one JSON object" in prompt


def test_failed_repair_round_drops_the_invalid_ones(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "a", "scenario": GOOD}, {"rationale": "b", "scenario": BAD}]),
        _result("", rc=1), _result("", rc=1),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert len(result["scenarios"]) == 1
    assert result["dropped"][0]["error"] == COMPILER_ERROR
    assert result["dropped"][0]["repaired"] is False


def test_a_timed_out_answer_is_not_asked_again(tmp_path, fuzzer_dir):
    # valkey-search #1542: the same prompt timed out twice, after 50 minutes each.
    timed_out = AgentRunResult(profile="fuzzer_proposal_readonly", stdout="", stderr="timeout after 3000s",
                               returncode=1, prompt_sha256="x", cwd="", allowed_tools="", model="",
                               started_at="", finished_at="")
    runner = StubRunner(timed_out, _answer([{"rationale": "a", "scenario": GOOD}]))
    with pytest.raises(RuntimeError, match="timeout after 3000s"):
        _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 1


def test_a_refused_request_says_why_and_is_not_asked_again(tmp_path, fuzzer_dir):
    # valkey #4214: the API refused the request under its usage policy after 26 minutes, and the walk recorded
    # "Claude Code failed (rc=1): " with nothing after it, the CLI's stderr being folded into its stdout.
    refusal = "API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy"
    stream = "\n".join([json.dumps({"type": "assistant"}), json.dumps({"type": "result", "result": refusal})])
    refused = AgentRunResult(profile="fuzzer_proposal_readonly", stdout=stream, stderr="", returncode=1,
                             prompt_sha256="x", cwd="", allowed_tools="", model="", started_at="", finished_at="")
    runner = StubRunner(refused, _answer([{"rationale": "a", "scenario": GOOD}]))
    with pytest.raises(RuntimeError, match=r"Claude Code failed \(rc=1\): API Error: .* Usage Policy"):
        _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 1


def test_unusable_first_answer_raises(tmp_path, fuzzer_dir):
    # Prose, and prose again when asked to restate it as the object.
    runner = StubRunner(_result("I could not find anything"), _result("Still nothing to say"))
    with pytest.raises(ValueError, match="scenarios list"):
        _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 2 and "I could not find anything" in runner.prompts[1]
    (tmp_path / "again").mkdir()
    with pytest.raises(ValueError, match="experiments are not a list"):
        _propose(StubRunner(_result(json.dumps({"scenarios": [], "experiments": {"experiment": PROPOSED}}))),
                 tmp_path / "again", fuzzer_dir)


def test_a_change_that_needs_no_test_gets_a_reason_and_nothing_else(tmp_path, fuzzer_dir):
    runner = StubRunner(_answer([], reason="  A comment-only change:\n nothing runs differently.  "))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 1
    assert result["reason"] == "A comment-only change: nothing runs differently."
    assert (result["scenarios"], result["dropped"], result["experiments"]) == ([], [], [])
    assert "needs no test. A comment-only change" in render_summary(result)
    assert batch_inputs(out) is None


def test_an_answer_that_only_gives_a_reason_needs_no_scenarios_key(tmp_path, fuzzer_dir):
    runner = StubRunner(_result("A performance change.\n" + json.dumps({"reason": "INFO replies the same"})))
    _, result, _ = _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 1
    assert (result["reason"], result["scenarios"]) == ("INFO replies the same", [])


def test_an_answer_that_ends_in_prose_is_restated_as_the_object(tmp_path, fuzzer_dir):
    runner = StubRunner(_result("## Conclusion\nThe change needs no test: it only pre-allocates."),
                        _result(json.dumps({"reason": "it only pre-allocates", "scenarios": []})))
    _, result, _ = _propose(runner, tmp_path, fuzzer_dir)
    assert len(runner.prompts) == 2
    assert "Do not read any file again" in runner.prompts[1] and "it only pre-allocates" in runner.prompts[1]
    assert result["reason"] == "it only pre-allocates"


def test_the_validator_and_lister_pass_absolute_paths(tmp_path, monkeypatch):
    calls = []
    monkeypatch.chdir(tmp_path)
    (tmp_path / "out" / "experiments").mkdir(parents=True)
    (tmp_path / "out" / "s.yaml").write_text("seed: 1\n")

    def run(args, **kw):
        calls.append((args, kw.get("cwd")))
        return subprocess.CompletedProcess(args, 0, "[]", "")

    monkeypatch.setattr(propose_mod.subprocess, "run", run)
    propose_mod.fuzzer_validator("vf")(Path("out/s.yaml"), Path("out/experiments"))
    propose_mod.fuzzer_lister("vf")(Path("out/experiments"))
    (validate, cwd), (listing, _) = calls
    assert validate[3] == str(tmp_path / "out" / "experiments") and validate[4] == str(tmp_path / "out" / "s.yaml")
    assert cwd == str(tmp_path / "out") and listing[-1] == str(tmp_path / "out" / "experiments")


# --- Proposing experiments -----------------------------------------------------


def _experiment_files(out: Path) -> list[str]:
    return sorted(p.name for p in (out / "experiments").iterdir())


def test_a_proposed_experiment_is_compiled_written_and_used(tmp_path, fuzzer_dir):
    seen: dict[str, str] = {}

    def validator(path: Path, experiment_dir: Path) -> str | None:
        seen[path.name] = path.read_text()
        seen["experiment_dir"] = str(experiment_dir)
        return _stub_validator(path, experiment_dir)

    runner = StubRunner(_answer(
        [{"rationale": "compares LSET with the parent", "scenario": USES_PROPOSED},
         {"rationale": "installed", "scenario": GOOD}],
        [{"experiment": PROPOSED}],
    ))
    out = tmp_path / "out"
    (tmp_path / "work").mkdir()
    run = propose(_change(), count=5, workdir=tmp_path / "work", out_dir=out, catalog=CATALOG,
                  fuzzer_dir=fuzzer_dir, validator=validator, lister=_stub_lister, runner=runner)
    result = write_results(run, out)

    assert len(runner.prompts) == 1
    assert _experiment_files(out) == ["proposed.lset.yaml"]
    experiment_text = (out / "experiments" / "proposed.lset.yaml").read_text()
    assert experiment_text.endswith(PROPOSED) and experiment_text.startswith("# Proposed by valkey-ci-agent")
    [uses, installed] = result["scenarios"]
    assert (uses["experiment"], installed["experiment"]) == ("proposed.lset", "valkey.cluster-chaos")
    assert result["experiments"] == [{
        "experiment_id": "proposed.lset", "file": "experiments/proposed.lset.yaml", "repaired": False,
        "sha256": hashlib.sha256(experiment_text.encode()).hexdigest(), "used_by": [uses["scenario_id"]],
    }]
    # The validated copy declares the parent build; the accepted scenario does not, nor does the installed one's copy.
    assert seen["experiment_dir"] == str(out / "experiments")
    assert seen[f"{uses['scenario_id']}.yaml"].endswith(PARENT_ARTIFACT_BLOCK)
    assert seen[f"{installed['scenario_id']}.yaml"].endswith(PARENT_ARTIFACT_BLOCK)
    assert "artifacts" not in (out / uses["file"]).read_text()
    assert "`proposed.lset`" in render_summary(result)
    # The dispatch carries the experiment and builds the parent for it.
    inputs = batch_inputs(out)
    assert json.loads(inputs["experiments"]) == {"proposed.lset.yaml": experiment_text}
    assert inputs["valkey_parent_sha"] == BASE
    assert "build" not in inputs


def test_an_experiment_that_does_not_compile_drops_its_scenarios_and_the_repair_sees_both(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "uses it", "scenario": USES_PROPOSED}, {"rationale": "fine", "scenario": GOOD}],
                [{"experiment": BROKEN}]),
        # Claude fixes the experiment only; the scenario that failed because of it is tried again as it is.
        _answer([], [{"index": 1, "experiment": PROPOSED}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)

    repair_prompt = runner.prompts[1]
    assert "### Experiment 1" in repair_prompt and FRAGMENT_ERROR in repair_prompt
    assert "### Scenario 1" in repair_prompt and "which did not compile: " + FRAGMENT_ERROR in repair_prompt
    assert "### Scenario 2" not in repair_prompt
    assert [s["experiment"] for s in result["scenarios"]] == ["proposed.lset", "valkey.cluster-chaos"]
    assert result["scenarios"][0]["repaired"] is False
    assert result["experiments"][0]["repaired"] is True
    assert result["dropped"] == [] and result["dropped_experiments"] == []
    assert _experiment_files(out) == ["proposed.lset.yaml"]


def test_an_experiment_that_still_does_not_compile_is_dropped_with_its_scenarios(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "uses it", "scenario": USES_PROPOSED}], [{"experiment": BROKEN}]),
        _answer([{"index": 1, "scenario": USES_PROPOSED.replace("seed: 9", "seed: 10")}],
                [{"index": 1, "experiment": BROKEN}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert result["scenarios"] == [] and result["experiments"] == []
    assert result["dropped_experiments"] == [
        {"index": 1, "experiment_id": "proposed.lset", "error": FRAGMENT_ERROR, "repaired": True},
    ]
    assert result["dropped"][0]["error"].startswith("uses experiment proposed.lset, which did not compile")
    assert result["dropped"][0]["repaired"] is True
    assert _experiment_files(out) == []
    assert "Dropped 1 experiment(s)" in render_summary(result)


def test_forbidden_components_are_caught_after_fragment_expansion(tmp_path, fuzzer_dir):
    reintroduced = PROPOSED.replace("[valkey.liveness]", "[valkey.liveness, valkey.reconnect-replicas]")
    runner = StubRunner(
        _answer([{"rationale": "r", "scenario": USES_PROPOSED}], [{"experiment": reintroduced}]),
        _answer([], []),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert "uses valkey.action.command, which sends any command" in result["dropped_experiments"][0]["error"]
    assert result["scenarios"] == []


def test_experiments_the_batch_would_refuse_or_no_scenario_uses_are_dropped(tmp_path, fuzzer_dir):
    unused = PROPOSED.replace("proposed.lset", "proposed.unused")
    elsewhere = PROPOSED.replace("artifacts.parent.binary", "artifacts.baseline.binary").replace("lset", "other")
    runner = StubRunner(
        _answer(
            [{"rationale": "r", "scenario": USES_PROPOSED},
             {"rationale": "undefined", "scenario": USES_PROPOSED.replace("proposed.lset", "proposed.ghost")}],
            [{"experiment": PROPOSED}, {"experiment": unused}, {"experiment": elsewhere},
             {"experiment": PROPOSED}, {"experiment": "- a"}, {"experiment": "x" * 21_000}, {"experiment": unused}],
        ),
        _answer([], []),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert [e["experiment_id"] for e in result["experiments"]] == ["proposed.lset"]
    assert _experiment_files(out) == ["proposed.lset.yaml"]
    dropped = {d["index"]: d["error"] for d in result["dropped_experiments"]}
    assert dropped[2] == "no validated scenario uses it"
    assert dropped[3].startswith("the experiment references artifacts.baseline.binary")
    assert dropped[4] == "the same experiment ID as an earlier proposal"
    assert dropped[5] == "the experiment is not a YAML mapping"
    assert sorted(dropped) == [2, 3, 4, 5]  # the sixth and seventh are past the limit of 5
    assert result["dropped"] == [
        {"index": 2, "rationale": "undefined", "repaired": False,
         "error": "uses experiment proposed.ghost, which no proposed experiment defines"},
    ]


def test_a_scenario_may_reference_only_the_parent_build(tmp_path, fuzzer_dir):
    parent = USES_PROPOSED.replace("base_port: 7200}", "base_port: 7200, server_config: {x: {$ref: artifacts.parent.binary}}}")
    other = GOOD.replace("base_port: 7000}", "base_port: 7000, server_config: {x: {$ref: artifacts.baseline.binary}}}")
    runner = StubRunner(
        _answer([{"rationale": "parent", "scenario": parent}, {"rationale": "other", "scenario": other}],
                [{"experiment": PROPOSED}]),
        _answer([], []),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert [s["rationale"] for s in result["scenarios"]] == ["parent"]
    assert result["dropped"][0]["error"].startswith(
        "the scenario references artifacts.baseline.binary; the only environment reference"
    )


def test_experiments_together_are_limited_to_what_the_batch_takes(tmp_path, fuzzer_dir):
    # Under the limit as Claude wrote them, over it as the batch is sent them: JSON, with the header.
    lines = "\n".join(f"# {'x' * 60}" for _ in range(150)) + "\n"
    first = PROPOSED.replace("proposed.lset", "proposed.first") + lines
    second = PROPOSED + lines
    assert len(first) + len(second) < 20_000
    uses_both = [{"rationale": r, "scenario": USES_PROPOSED.replace("proposed.lset", f"proposed.{r}")}
                 for r in ("first", "lset")]
    runner = StubRunner(_answer(uses_both, [{"experiment": first}, {"experiment": second}]), _answer([], []))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert [e["experiment_id"] for e in result["experiments"]] == ["proposed.first"]
    assert result["dropped_experiments"][0]["error"].startswith(
        "with the experiments accepted before it, the PR batch would be sent"
    )
    # What is accepted, the batch takes.
    assert len(batch_inputs(out)["experiments"]) <= 20_000


def test_a_repair_that_renames_an_experiment_compiles_alone(tmp_path, fuzzer_dir):
    renamed = PROPOSED.replace("proposed.lset", "proposed.lset2")
    runner = StubRunner(
        _answer([{"rationale": "r", "scenario": USES_PROPOSED.replace("proposed.lset", "proposed.lset2")}],
                [{"experiment": BROKEN}]),
        _answer([], [{"index": 1, "experiment": renamed}]),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    # The broken first version is gone from the experiment's directory, so it no longer fails the listing.
    assert [e["experiment_id"] for e in result["experiments"]] == ["proposed.lset2"]
    assert _experiment_files(out) == ["proposed.lset2.yaml"]


def test_the_parent_is_built_for_a_scenario_that_references_it(tmp_path, fuzzer_dir):
    parent = GOOD.replace("base_port: 7000}", "base_port: 7000, server_config: {x: {$ref: artifacts.parent.binary}}}")
    _, _, out = _proposed(tmp_path, fuzzer_dir, parent)
    inputs = batch_inputs(out)
    assert inputs["valkey_parent_sha"] == BASE and "experiments" not in inputs


def test_batch_inputs_refuse_an_experiment_changed_after_it_compiled(tmp_path, fuzzer_dir):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": USES_PROPOSED}], [{"experiment": PROPOSED}]))
    _, result, out = _propose(runner, tmp_path, fuzzer_dir)
    path = out / "experiments" / "proposed.lset.yaml"
    path.write_text(path.read_text().replace("PING", "SHUTDOWN"))
    with pytest.raises(ValueError, match="changed since it compiled"):
        validated_scenarios(out)
    path.write_text(path.read_text().replace("SHUTDOWN", "PING"))
    data = json.loads((out / "proposals.json").read_text())
    data["experiments"][0]["file"] = "../proposals.json"
    (out / "proposals.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="experiment file outside"):
        validated_scenarios(out)


def test_summary_lists_validated_scenarios():
    result = {
        "change": _change().summary(),
        "scenarios": [{"scenario_id": "ai-1", "file": "scenarios/ai-1.yaml", "rationale": "a | b", "repaired": True,
                       "experiment": "valkey.forkless-save"}],
        "dropped": [{"index": 2}],
    }
    text = render_summary(result)
    assert "valkey-io/valkey#123" in text
    assert "`ai-1` (repaired) | `valkey.forkless-save` | a \\| b |" in text
    assert "Dropped 1" in text


# --- The fuzzer command --------------------------------------------------------


@pytest.fixture
def fake_fuzzer(tmp_path) -> str:
    """A stand-in valkey-fuzzer with the same list and validate interface."""
    script = tmp_path / "bin" / "valkey-fuzzer"
    script.parent.mkdir()
    script.write_text(textwrap.dedent(f"""\
        #!{sys.executable}
        import json, os, sys
        args = sys.argv[1:]
        directory = args[args.index("--experiment-dir") + 1] if "--experiment-dir" in args else None
        if args[:1] == ["list"]:
            entries = list({json.dumps(CATALOG)}[args[1]])
            for name in sorted(os.listdir(directory)) if directory else []:
                text = open(os.path.join(directory, name)).read()
                if "valkey.nope" in text:
                    print("Error: " + os.path.join(directory, name) + ": experiment extends unknown fragment 'valkey.nope'")
                    sys.exit(2)
                entries.append({{"id": name[:-5], "directory": directory, "components": [
                    {{"use": "valkey.action.commands", "id": "lset", "config": {{"commands": []}}}}]}})
            print(json.dumps(entries))
        elif args[:1] == ["validate"]:
            text = open(args[-1]).read()
            if "shard: 9" in text:
                print("Validating scenario: " + args[-1])
                print("{COMPILER_ERROR}")
                sys.exit(2)
            print("Scenario passed structural validation!")
        else:
            sys.exit(1)
    """))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script)


def test_load_catalog_and_validate_use_the_fuzzer(fake_fuzzer, tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET_TOKEN", "secret")
    assert load_catalog(fake_fuzzer) == CATALOG
    validate = fuzzer_validator(fake_fuzzer)
    good, bad = tmp_path / "good.yaml", tmp_path / "bad.yaml"
    good.write_text(GOOD)
    bad.write_text(BAD)
    assert validate(good, tmp_path / "experiments") is None
    assert validate(bad, tmp_path / "experiments") == COMPILER_ERROR


def test_list_experiments_uses_the_fuzzer(fake_fuzzer, tmp_path):
    lister = fuzzer_lister(fake_fuzzer)
    directory = tmp_path / "experiment-1"
    directory.mkdir()
    (directory / "proposed.lset.yaml").write_text(PROPOSED)
    entries, error = lister(directory)
    assert error is None
    # Only the directory's own experiments, not the installed ones.
    assert [e["id"] for e in entries] == ["proposed.lset"]
    (directory / "proposed.lset.yaml").write_text(BROKEN)
    entries, error = lister(directory)
    # The error names the file, not the temporary directory.
    assert (entries, error) == ([], FRAGMENT_ERROR)


def test_load_catalog_rejects_a_failed_listing(tmp_path):
    script = tmp_path / "broken"
    script.write_text(f"#!{sys.executable}\nimport sys\nsys.exit(2)\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    with pytest.raises(RuntimeError, match="list experiments failed"):
        load_catalog(str(script))


# --- Dry runs ------------------------------------------------------------------


@pytest.fixture
def block_remote_calls(monkeypatch):
    """From the call on, fail the test on any network, GitHub, clone or Claude call."""
    def refuse(*_a, **_kw):
        raise AssertionError("a dry run made a remote call")

    def block() -> None:
        _block(monkeypatch, refuse)

    return block


def _block(monkeypatch, refuse) -> None:
    # Imported before sockets are refused: importing it builds a socket class.
    import github
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(propose_mod, "urlopen", refuse)
    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", refuse)
    monkeypatch.setattr(propose_mod, "run_agent", refuse)
    monkeypatch.setattr(propose_mod, "fetch_change", refuse)
    monkeypatch.setattr(propose_mod, "dispatch_batch", refuse)
    monkeypatch.setattr(github, "Github", refuse)
    monkeypatch.delenv("TARGET_TOKEN", raising=False)


def test_propose_dry_run_makes_no_remote_calls(block_remote_calls, fake_fuzzer, fuzzer_dir, tmp_path):
    block_remote_calls()
    rc = propose_mod.main([
        "propose", "--pr", "123", "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer,
        "--out", str(tmp_path / "out"), "--dry-run",
    ])
    assert rc == 0
    assert not (tmp_path / "out").exists()


def test_dispatch_dry_run_makes_no_remote_calls(block_remote_calls, tmp_path, fuzzer_dir):
    _, result, out = _propose(
        StubRunner(_answer([{"rationale": "a", "scenario": GOOD}])), tmp_path, fuzzer_dir,
    )
    block_remote_calls()
    assert propose_mod.main(["dispatch", "--out", str(out), "--dry-run"]) == 0


def test_propose_rejects_a_directory_that_is_not_the_fuzzer(tmp_path, fake_fuzzer):
    with pytest.raises(SystemExit):
        propose_mod.main(["propose", "--pr", "1", "--fuzzer-dir", str(tmp_path), "--fuzzer-bin", fake_fuzzer,
                          "--dry-run"])


@pytest.mark.parametrize("count", ["0", "11", "x"])
def test_propose_bounds_the_count(count, fuzzer_dir, fake_fuzzer):
    with pytest.raises(SystemExit):
        propose_mod.main(["propose", "--pr", "1", "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer,
                          "--count", count, "--dry-run"])


def test_propose_for_real_writes_the_artifact(monkeypatch, fake_fuzzer, fuzzer_dir, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "t")
    monkeypatch.setattr(propose_mod, "fetch_change", lambda gh, token, request: _change())
    monkeypatch.setattr(propose_mod, "_default_runner", StubRunner(_answer([{"rationale": "a", "scenario": GOOD}])))
    import github
    monkeypatch.setattr(github, "Github", MagicMock())
    out = tmp_path / "out"
    rc = propose_mod.main(["propose", "--pr", "123", "--fuzzer-dir", str(fuzzer_dir),
                           "--fuzzer-bin", fake_fuzzer, "--out", str(out)])
    assert rc == 0
    result = json.loads((out / "proposals.json").read_text())
    assert len(result["scenarios"]) == 1
    assert (out / result["scenarios"][0]["file"]).is_file()


def test_propose_failure_exits_nonzero(monkeypatch, fake_fuzzer, fuzzer_dir, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "t")

    def down(*_a, **_kw):
        raise RuntimeError("GitHub is down")

    monkeypatch.setattr(propose_mod, "fetch_change", down)
    import github
    monkeypatch.setattr(github, "Github", MagicMock())
    assert propose_mod.main(["propose", "--pr", "1", "--fuzzer-dir", str(fuzzer_dir),
                             "--fuzzer-bin", fake_fuzzer, "--out", str(tmp_path / "o")]) == 1


# --- Dispatch ------------------------------------------------------------------


def _proposed(tmp_path, fuzzer_dir, *scenarios):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": s} for s in scenarios]))
    return _propose(runner, tmp_path, fuzzer_dir)


def test_batch_inputs_carry_the_validated_scenarios(tmp_path, fuzzer_dir):
    _, result, out = _proposed(tmp_path, fuzzer_dir, GOOD, OTHER_GOOD)
    inputs = batch_inputs(out)
    scenarios = json.loads(inputs["scenarios"])
    assert sorted(scenarios) == sorted(s["scenario_id"] for s in result["scenarios"])
    assert inputs["valkey_ref"] == "refs/pull/123/head"
    assert inputs["valkey_sha"] == HEAD
    assert inputs["valkey_repository"] == "valkey-io/valkey"
    # Without a proposed experiment nothing compares with the parent, so it is not built.
    assert "valkey_parent_sha" not in inputs and "experiments" not in inputs


def test_batch_inputs_for_a_range_use_the_commit(tmp_path, fuzzer_dir):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": GOOD}]))
    _, _, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(pr_number=None, fetch_ref=None))
    assert batch_inputs(out)["valkey_ref"] == HEAD


def test_batch_inputs_refuse_a_scenario_changed_after_validation(tmp_path, fuzzer_dir):
    _, result, out = _proposed(tmp_path, fuzzer_dir, GOOD)
    path = out / result["scenarios"][0]["file"]
    path.write_text(path.read_text().replace("seed: 7", "seed: 99"))
    with pytest.raises(ValueError, match="changed since it was validated"):
        batch_inputs(out)


def test_batch_inputs_refuse_a_file_outside_the_scenarios(tmp_path, fuzzer_dir):
    _, result, out = _proposed(tmp_path, fuzzer_dir, GOOD)
    data = json.loads((out / "proposals.json").read_text())
    data["scenarios"][0]["file"] = "../../etc/passwd"
    (out / "proposals.json").write_text(json.dumps(data))
    with pytest.raises(ValueError, match="outside"):
        batch_inputs(out)


def test_batch_inputs_none_when_nothing_validated(tmp_path, fuzzer_dir):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": BAD}]), _answer([]))
    _, _, out = _propose(runner, tmp_path, fuzzer_dir)
    assert batch_inputs(out) is None


def test_dispatch_batch_dispatches_the_workflow():
    inputs = {"scenarios": json.dumps({"ai-1": GOOD}), "valkey_repository": "valkey-io/valkey",
              "valkey_ref": "refs/pull/1/head", "valkey_sha": HEAD}
    gh = MagicMock()
    workflow = gh.get_repo.return_value.get_workflow.return_value
    workflow.create_dispatch.return_value = True
    dispatch_batch(gh, inputs, fuzzer_repo="valkey-io/valkey-fuzzer",
                      workflow="fuzzer-pr-batch.yml", ref="main")
    gh.get_repo.assert_called_with("valkey-io/valkey-fuzzer")
    gh.get_repo.return_value.get_workflow.assert_called_with("fuzzer-pr-batch.yml")
    workflow.create_dispatch.assert_called_with("main", inputs)
    workflow.create_dispatch.return_value = False
    with pytest.raises(RuntimeError, match="refused"):
        dispatch_batch(gh, inputs, fuzzer_repo="valkey-io/valkey-fuzzer", workflow="w.yml", ref="main")
    # Inputs the PR batch would refuse are never sent.
    workflow.create_dispatch.reset_mock()
    with pytest.raises(ValueError, match="takes scenarios"):
        dispatch_batch(gh, {"scenarios": "{}"}, fuzzer_repo="valkey-io/valkey-fuzzer",
                          workflow="fuzzer-pr-batch.yml", ref="main")
    workflow.create_dispatch.assert_not_called()


def test_dispatch_for_real_needs_a_token_and_dispatches(monkeypatch, tmp_path, fuzzer_dir):
    _, _, out = _proposed(tmp_path, fuzzer_dir, GOOD)
    sent = {}
    monkeypatch.setattr(propose_mod, "dispatch_batch", lambda gh, inputs, **kw: sent.update(inputs=inputs, **kw))
    import github
    monkeypatch.setattr(github, "Github", MagicMock())
    monkeypatch.delenv("TARGET_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        propose_mod.main(["dispatch", "--out", str(out)])
    monkeypatch.setenv("TARGET_TOKEN", "t")
    assert propose_mod.main(["dispatch", "--out", str(out)]) == 0
    assert sent["fuzzer_repo"] == "valkey-io/valkey-fuzzer"
    assert sent["workflow"] == "fuzzer-pr-batch.yml"
    assert len(json.loads(sent["inputs"]["scenarios"])) == 1


# --- Modules -------------------------------------------------------------------

PINNED = "e" * 40
# A proposed experiment comparing two builds of a module on one Valkey build, and a scenario that uses it.
MODULE_PROPOSED = textwrap.dedent("""\
    extends: [valkey.liveness]
    id: proposed.json-get
    deployment: {modes: [standalone]}
    components:
      - use: valkey.action.commands
        id: json
        config:
          modules: [{$ref: artifacts.module.path}]
          modules_parent: [{$ref: artifacts.module_parent.path}]
          commands: [{command: [JSON.GET, k], compare: json}]
    required_evidence:
      - {evidence_id: valkey.command.sequence, source: json}
""")
USES_MODULE = "seed: 9\nexperiment: {id: proposed.json-get}\ndeployment: {mode: standalone, base_port: 7200}\n"
# A module change: a valkey-json pull request.
JSON_CHANGE = dict(repo="valkey-io/valkey-json", pr_number=42, title="Fix JSON.GET", fetch_ref="refs/pull/42/head",
                   diff="diff --git a/src/json/json.cc b/src/json/json.cc\n")
# A Valkey change to the module API, and the tests/modules Makefile its checkout holds.
MODULE_API_DIFF = "diff --git a/src/module.c b/src/module.c\n"
MAKEFILE = "TEST_MODULES = \\\n    commandfilter.so \\\n    datatype.so\n\nclean:\n\trm -f $(TEST_MODULES:.so=.xo)\n"


def _validating(seen: dict):
    def validator(path: Path, experiment_dir: Path) -> str | None:
        seen[path.name] = path.read_text()
        return _stub_validator(path, experiment_dir)
    return validator


@pytest.fixture
def valkey_checkout(monkeypatch):
    """The clone Claude reads, with tests/modules/Makefile in a Valkey one; the destinations, as cloned."""
    cloned: list[Path] = []

    def clone(repo, dest, sha=None, *, fetch_ref=None):
        cloned.append(dest)
        if dest.name == "valkey":
            (dest / "tests" / "modules").mkdir(parents=True)
            (dest / "tests" / "modules" / "Makefile").write_text(MAKEFILE)
        return True

    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", clone)
    return cloned


def test_a_module_change_loads_its_two_builds_into_the_pinned_valkey(tmp_path, fuzzer_dir, valkey_checkout):
    seen: dict[str, str] = {}
    parent = GOOD.replace("base_port: 7000}", "base_port: 7000, server_config: {x: {$ref: artifacts.parent.binary}}}")
    runner = StubRunner(
        _answer([{"rationale": "compares JSON.GET", "scenario": USES_MODULE},
                 {"rationale": "the parent", "scenario": parent}],
                [{"experiment": MODULE_PROPOSED}, {"experiment": PROPOSED}]),
        _answer([], []),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(**JSON_CHANGE), valkey_sha=PINNED,
                                validator=_validating(seen))

    prompt = runner.prompts[0]
    # Claude reads the module at the change, under its own name.
    assert [path.name for path in valkey_checkout] == ["valkey-json"]
    assert f"- valkey-json/ - valkey-io/valkey-json at {HEAD}, with the change applied." in prompt
    assert propose_mod._MODULE_RULE in prompt and propose_mod._PARENT_RULE not in prompt
    assert f"one pinned Valkey build, valkey-io/valkey at {PINNED}" in prompt
    assert "modules: [{$ref: artifacts.module.path}]" in prompt and "compare: json" in prompt
    # The module's builds are the only environment references; there is no Valkey parent.
    assert [e["experiment_id"] for e in result["experiments"]] == ["proposed.json-get"]
    assert result["dropped_experiments"][0]["error"] == (
        "the experiment references artifacts.parent.binary; the only environment reference a proposed experiment "
        "may hold is artifacts.module.path or artifacts.module_parent.path"
    )
    assert result["dropped"][0]["error"].startswith("the scenario references artifacts.parent.binary; the only "
                                                    "environment reference a proposal may hold is artifacts.module.path")
    assert "no environment reference but\n{$ref: artifacts.module.path} or {$ref: artifacts.module_parent.path};" in (
        runner.prompts[1])
    [uses] = result["scenarios"]
    assert seen[f"{uses['scenario_id']}.yaml"].endswith(artifacts_block(("module", "module_parent")))
    assert "artifacts" not in (out / uses["file"]).read_text()
    assert (result["module"], result["valkey_sha"]) == (
        {"repository": "valkey-io/valkey-json", "sha": HEAD, "parent_sha": BASE, "file": ""}, PINNED,
    )
    assert "Loads `valkey-io/valkey-json`, built at the change and its parent, into the pinned Valkey `eeeeeeeeeeee`" in (
        render_summary(result))
    # The batch runs on the pinned Valkey, with the module's two builds and no Valkey parent, experiments or not.
    inputs = batch_inputs(out)
    assert (inputs["valkey_repository"], inputs["valkey_ref"], inputs["valkey_sha"]) == ("valkey-io/valkey", PINNED,
                                                                                         PINNED)
    assert (inputs["module_repository"], inputs["module_sha"], inputs["module_parent_sha"]) == (
        "valkey-io/valkey-json", HEAD, BASE,
    )
    assert "experiments" in inputs and not {"valkey_parent_sha", "module_file"} & set(inputs)


def test_a_module_change_needs_the_pinned_valkey(tmp_path, fuzzer_dir):
    with pytest.raises(ValueError, match="valkey-io/valkey-json is a module: its proposals need the pinned Valkey"):
        _propose(StubRunner(), tmp_path, fuzzer_dir, change=_change(**JSON_CHANGE))


def test_a_change_to_the_module_api_may_load_one_of_the_test_modules(tmp_path, fuzzer_dir, valkey_checkout):
    seen: dict[str, str] = {}
    loads = USES_MODULE.replace("proposed.json-get", "proposed.datatype")
    experiment = MODULE_PROPOSED.replace("proposed.json-get", "proposed.datatype").replace(
        "          modules:", "          compare_with: {$ref: artifacts.parent.binary}\n          modules:")
    runner = StubRunner(_answer([{"rationale": "datatype on both builds", "scenario": loads}],
                                [{"experiment": experiment}], module_file="tests/modules/datatype.so"))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(diff=MODULE_API_DIFF),
                                validator=_validating(seen))

    prompt = runner.prompts[0]
    assert propose_mod._TEST_MODULES_RULE in prompt
    assert ('"module_file" to your JSON answer, beside\n"experiments" and "scenarios" and never inside their YAML, one of\n'
            'tests/modules/commandfilter.so, tests/modules/datatype.so, as in') in prompt
    assert result["module"] == {"repository": "valkey-io/valkey", "sha": HEAD, "parent_sha": BASE,
                                "file": "tests/modules/datatype.so"}
    assert result["valkey_sha"] is None and result["module_file_error"] is None
    [uses] = result["scenarios"]
    assert seen[f"{uses['scenario_id']}.yaml"].endswith(artifacts_block(("parent", "module", "module_parent")))
    # Valkey's parent build beside the change's, each loading its own commit's tests/modules.
    inputs = batch_inputs(out)
    assert (inputs["valkey_sha"], inputs["valkey_parent_sha"]) == (HEAD, BASE)
    assert (inputs["module_repository"], inputs["module_sha"], inputs["module_parent_sha"], inputs["module_file"]) == (
        "valkey-io/valkey", HEAD, BASE, "tests/modules/datatype.so",
    )


def test_the_repair_round_may_name_the_test_module_the_first_answer_left_out(tmp_path, fuzzer_dir, valkey_checkout):
    # The full replay's module-API merges lost every test this way: the scenarios loaded a test module
    # and the answer never named it.
    loads = USES_MODULE.replace("proposed.json-get", "proposed.datatype")
    experiment = MODULE_PROPOSED.replace("proposed.json-get", "proposed.datatype").replace(
        "          modules:", "          compare_with: {$ref: artifacts.parent.binary}\n          modules:")
    runner = StubRunner(_answer([{"rationale": "datatype on both builds", "scenario": loads}],
                                [{"experiment": experiment}]),
                        _answer([], [], module_file="tests/modules/datatype.so"))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(diff=MODULE_API_DIFF))

    assert ('"module_file" to the JSON object\nyou answer with, beside "experiments" and "scenarios" and never '
            'inside their\nYAML, one of tests/modules/commandfilter.so, tests/modules/datatype.so') in runner.prompts[1]
    assert result["module"] == {"repository": "valkey-io/valkey", "sha": HEAD, "parent_sha": BASE,
                                "file": "tests/modules/datatype.so"}
    assert [e["experiment_id"] for e in result["experiments"]] == ["proposed.datatype"]
    assert len(result["scenarios"]) == 1 and result["dropped"] == []
    assert batch_inputs(out)["module_file"] == "tests/modules/datatype.so"


def test_a_module_file_written_into_the_yaml_is_taken_from_there(tmp_path, fuzzer_dir, valkey_checkout):
    # The nine-merge rerun lost #4424's and #3645's tests this way, in the first answer and the repair.
    experiment = MODULE_PROPOSED.replace("proposed.json-get", "proposed.datatype").replace(
        "      modules:", "      module_file: tests/modules/datatype.so\n      modules:")
    loads = USES_MODULE.replace("proposed.json-get", "proposed.datatype")
    runner = StubRunner(_answer([{"rationale": "datatype on both builds", "scenario": loads}],
                                [{"experiment": experiment}]))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(diff=MODULE_API_DIFF))
    assert result["module"]["file"] == "tests/modules/datatype.so"
    assert len(result["scenarios"]) == 1 and result["dropped"] == [] and len(runner.prompts) == 1
    assert run.compiled[0].text == experiment.replace("      module_file: tests/modules/datatype.so\n", "")


def test_a_module_file_tests_modules_does_not_build_loads_nothing(tmp_path, fuzzer_dir, valkey_checkout):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": USES_MODULE}], [{"experiment": MODULE_PROPOSED}],
                                module_file="tests/modules/nope.so"),
                        _answer([], []))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir, change=_change(diff=MODULE_API_DIFF))
    assert result["module"] is None
    assert result["module_file_error"] == "module_file 'tests/modules/nope.so' is not a module tests/modules builds"
    assert result["dropped_experiments"][0]["error"].startswith(
        "the experiment references artifacts.module.path, artifacts.module_parent.path; the only environment "
        "reference a proposed experiment may hold is artifacts.parent.binary"
    )
    assert result["scenarios"] == []


def test_a_change_off_the_module_api_is_offered_no_module(tmp_path, fuzzer_dir, valkey_checkout):
    runner = StubRunner(_answer([{"rationale": "r", "scenario": GOOD}], module_file="tests/modules/datatype.so"))
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert "Valkey's test modules" not in runner.prompts[0] and propose_mod._PARENT_RULE in runner.prompts[0]
    assert (result["module"], result["module_file_error"]) == (None, None)
    assert "module_repository" not in batch_inputs(out)


@pytest.mark.parametrize("diff,touches", [
    ("diff --git a/src/module.c b/src/module.c\n", True),
    ("diff --git a/src/valkeymodule.h b/src/valkeymodule.h\n", True),
    ("diff --git a/src/t_list.c b/src/t_list.c\ndiff --git a/tests/modules/list.c b/tests/modules/list.c\n", True),
    ("diff --git a/src/t_list.c b/src/t_list.c\n+ src/module.c\n", False),
    ("diff --git a/tests/unit/moduleapi/list.tcl b/tests/unit/moduleapi/list.tcl\n", False),
])
def test_the_module_api_is_module_c_valkeymodule_h_and_tests_modules(diff, touches):
    assert touches_module_api(diff) is touches


def test_the_test_modules_are_the_ones_the_makefile_builds(tmp_path):
    assert built_test_modules(tmp_path) == []
    (tmp_path / "tests" / "modules").mkdir(parents=True)
    (tmp_path / "tests" / "modules" / "Makefile").write_text(MAKEFILE + "hellovalkey.so: hellovalkey.xo\n")
    assert built_test_modules(tmp_path) == ["tests/modules/commandfilter.so", "tests/modules/datatype.so",
                                            "tests/modules/hellovalkey.so"]


def test_the_pinned_valkey_is_a_full_commit():
    gh = MagicMock()
    gh.get_repo.return_value.get_commit.return_value.sha = PINNED
    assert pinned_valkey(gh, "unstable") == PINNED
    gh.get_repo.assert_called_with("valkey-io/valkey")
    gh.get_repo.return_value.get_commit.assert_called_with("unstable")
    for ref in ("--upload-pack=x", "a..b"):
        with pytest.raises(ValueError, match="not a commit or branch name"):
            pinned_valkey(gh, ref)
    gh.get_repo.return_value.get_commit.return_value.sha = "abc"
    with pytest.raises(ValueError, match="malformed commit"):
        pinned_valkey(gh, "unstable")


def test_propose_for_a_module_resolves_the_pinned_valkey(monkeypatch, fake_fuzzer, fuzzer_dir, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "t")
    requests = []
    monkeypatch.setattr(propose_mod, "fetch_change",
                        lambda gh, token, request: requests.append(request) or _change(**JSON_CHANGE))
    monkeypatch.setattr(propose_mod, "_default_runner", StubRunner(_answer([{"rationale": "a", "scenario": GOOD}])))
    import github
    gh = MagicMock()
    gh.get_repo.return_value.get_commit.return_value.sha = PINNED
    monkeypatch.setattr(github, "Github", lambda **kw: gh)
    out = tmp_path / "out"
    assert propose_mod.main(["propose", "--repo", "valkey-io/valkey-json", "--pr", "42", "--valkey-ref", "9.0",
                             "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer, "--out", str(out)]) == 0
    assert requests == [ChangeRequest("valkey-io/valkey-json", pr_number=42)]
    gh.get_repo.return_value.get_commit.assert_called_with("9.0")
    assert json.loads((out / "proposals.json").read_text())["valkey_sha"] == PINNED


# Set VALKEY_FUZZER_DIR to a valkey-fuzzer checkout to check the prompts
# against it, and VALKEY_FUZZER_BIN to its installed command to run the real
# compile and validate steps on a proposed experiment.
_FUZZER_DIR = os.environ.get("VALKEY_FUZZER_DIR", "")
_FUZZER_BIN = shutil.which(os.environ.get("VALKEY_FUZZER_BIN", "") or "") or ""


@pytest.mark.skipif(not _FUZZER_DIR, reason="needs VALKEY_FUZZER_DIR, a valkey-fuzzer checkout")
@pytest.mark.parametrize("prompt", [propose_mod._PROPOSE_PROMPT, propose_mod._REPAIR_PROMPT,
                                    propose_mod._MODULE_SECTION, propose_mod._TEST_MODULES_SECTION])
def test_the_prompts_name_only_files_the_fuzzer_has(prompt):
    # docs/*.md, valkey_fuzzer/... and examples/... are read by Claude in its
    # copy of the fuzzer; a renamed one leaves it looking for nothing.
    named = set(re.findall(r"\b((?:docs|valkey_fuzzer\w*|examples)/[\w./<>-]*\w/?)", prompt))
    assert named
    for path in named:
        if "<" in path:
            path = path.split("<", 1)[0]
        assert (Path(_FUZZER_DIR) / path).exists(), f"the prompt names {path}, which valkey-fuzzer does not have"


def test_the_prompts_render():
    prompt = propose_mod._PROPOSE_PROMPT.format(change_line="c", count=5, max_experiments=5, source_note="s",
                                                truncated_note="", references_rule=propose_mod._PARENT_RULE,
                                                module_section="", extra="")
    assert "compare_with: {$ref: artifacts.parent.binary}" in prompt
    assert "may hold is\n  {$ref: artifacts.parent.binary}, the build of the change's parent commit" in prompt
    assert "expect_difference: true, expect: {equals: [a, b, c]}" in prompt
    assert "valkey-io/valkey#4604" in prompt and "RESP 2 and 3" in prompt
    assert "`<node.1.id>`, `<node.1.host>`, `<node.1.port>`" in prompt and "`wait_s: 30`" in prompt
    assert '`raw: "SET k v\\0 EX 100\\r\\n"`' in prompt and "`read: true`" in prompt
    assert "`files: {users.acl:" in prompt and "`restart: true`" in prompt
    assert "### valkey.action.tools" in prompt and "with\n   valkey.action.tools (below)" in prompt
    assert "null\\n\"}}}\n```\n\n### Faults" in prompt and "\n### Adversarial inputs" in prompt
    assert "one scenario at\n   least runs the change under a fault (below)" in prompt
    assert "examples/valkey/experiments/replica-killed-under-writes.yaml" in prompt
    repair = propose_mod._REPAIR_PROMPT.format(change_line="c", source_note="s", failures="f",
                                               references="{$ref: artifacts.parent.binary}", module_note="")
    assert '"experiments": [{"index": <its number>' in repair
    assert "no environment reference but\n{$ref: artifacts.parent.binary};" in repair
    module = propose_mod._MODULE_SECTION.format(repo="valkey-io/valkey-json", head=HEAD, base=BASE,
                                                valkey_sha="e" * 40, checkout="valkey-json", note="N.")
    assert "      modules: [{$ref: artifacts.module.path}]\n      modules_parent: [{$ref: artifacts.module_parent.path}]" in module
    tests = propose_mod._TEST_MODULES_SECTION.format(names="tests/modules/datatype.so")
    assert "      compare_with: {$ref: artifacts.parent.binary}\n      modules: [{$ref: artifacts.module.path}]" in tests


# A proposed experiment that references the parent build through an installed
# component's reference-taking field (the compare action lands with the fuzzer's
# own step), and one that brings valkey.action.command back through a fragment.
_REAL_PROPOSED = textwrap.dedent("""\
    extends: [valkey.base]
    id: proposed.probe
    deployment: {modes: [standalone]}
    components:
      - use: valkey.capability.build-probe
        id: build
        config:
          binaries: [{$ref: artifacts.parent.binary}]
""")
_REAL_SCENARIO = ("scenario_id: ai-probe\nseed: 1\nexperiment: {id: proposed.probe}\n"
                  "deployment: {mode: standalone, base_port: 7000}\n")


@pytest.mark.skipif(not _FUZZER_BIN, reason="needs VALKEY_FUZZER_BIN, the installed valkey-fuzzer command")
def test_the_real_fuzzer_compiles_and_validates_a_proposed_experiment(tmp_path):
    lister, validator = fuzzer_lister(_FUZZER_BIN), fuzzer_validator(_FUZZER_BIN)
    directory = tmp_path / "experiments"
    directory.mkdir()
    (directory / "proposed.probe.yaml").write_text(_REAL_PROPOSED)
    entries, error = lister(directory)
    assert error is None and [e["id"] for e in entries] == ["proposed.probe"]
    assert compiled_experiment_error(entries[0]) is None
    assert {c["use"] for c in entries[0]["components"]} >= {"valkey.capability.build-probe", "valkey.oracle.ledger"}
    # The scenario validates only with the parent declared, which the validated copy carries.
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(_REAL_SCENARIO)
    assert "references unknown environment target 'artifacts.parent'" in validator(scenario, directory)
    scenario.write_text(_REAL_SCENARIO + PARENT_ARTIFACT_BLOCK)
    assert validator(scenario, directory) is None
    assert validator(scenario, tmp_path / "empty") is not None  # the experiment is loaded from the directory


@pytest.mark.skipif(not _FUZZER_BIN, reason="needs VALKEY_FUZZER_BIN, the installed valkey-fuzzer command")
def test_the_real_fuzzer_validates_a_module_experiment_with_the_module_builds_declared(tmp_path):
    directory = tmp_path / "experiments"
    directory.mkdir()
    (directory / "proposed.json-get.yaml").write_text(MODULE_PROPOSED)
    entries, error = fuzzer_lister(_FUZZER_BIN)(directory)
    assert error is None
    assert compiled_experiment_error(entries[0], (propose_mod.MODULE_REFERENCE,
                                                  propose_mod.MODULE_PARENT_REFERENCE)) is None
    assert "references artifacts.module.path" in compiled_experiment_error(entries[0])
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text("scenario_id: ai-json\n" + USES_MODULE)
    validator = fuzzer_validator(_FUZZER_BIN)
    assert "artifacts.module" in validator(scenario, directory)
    scenario.write_text("scenario_id: ai-json\n" + USES_MODULE + artifacts_block(("module", "module_parent")))
    assert validator(scenario, directory) is None


@pytest.mark.skipif(not _FUZZER_BIN, reason="needs VALKEY_FUZZER_BIN, the installed valkey-fuzzer command")
@pytest.mark.parametrize("text,error", [
    (_REAL_PROPOSED.replace("valkey.base", "valkey.reconnect-replicas"), "uses valkey.action.command"),
    (_REAL_PROPOSED.replace("valkey.base", "valkey.nope"), "extends unknown fragment 'valkey.nope'"),
    (_REAL_PROPOSED.replace("artifacts.parent.binary", "artifacts.baseline.binary"),
     "references artifacts.baseline.binary"),
])
def test_the_real_fuzzer_shows_what_a_fragment_brings(tmp_path, text, error):
    (tmp_path / "proposed.probe.yaml").write_text(text)
    entries, problem = fuzzer_lister(_FUZZER_BIN)(tmp_path)
    if problem is None:
        problem = compiled_experiment_error(entries[0])
    assert error in problem
