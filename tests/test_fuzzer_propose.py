"""Tests for fuzzer scenario proposals."""
from __future__ import annotations

import json
import os
import socket
import stat
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scripts.fuzzer.propose as propose_mod
from scripts.ai.runtime import AgentRunResult
from scripts.fuzzer.propose import (
    Change,
    ChangeRequest,
    batch_inputs,
    check_scenario,
    dispatch_batch,
    fetch_change,
    fuzzer_validator,
    load_catalog,
    parse_request,
    propose,
    render_summary,
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


def _stub_validator(path: Path) -> str | None:
    return COMPILER_ERROR if "shard: 9" in path.read_text() else None


def _answer(scenarios: list[dict]) -> AgentRunResult:
    return _result(json.dumps({"scenarios": scenarios}))


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


def _propose(runner, tmp_path, fuzzer_dir, *, count=5, change=None):
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    run = propose(change or _change(), count=count, workdir=work, out_dir=out, catalog=CATALOG,
                  fuzzer_dir=fuzzer_dir, validator=_stub_validator, runner=runner)
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


def test_fetch_change_pins_a_pr_and_fetches_its_head_ref(monkeypatch):
    gh = MagicMock()
    pull = gh.get_repo.return_value.get_pull.return_value
    pull.head.sha, pull.base.sha, pull.title = HEAD, BASE, "Fix failover"
    paths = []
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: paths.append(path) or b"diff --git x\n")
    change = fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=9))
    assert (change.head_sha, change.base_sha, change.fetch_ref) == (HEAD, BASE, "refs/pull/9/head")
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
    monkeypatch.setattr(propose_mod, "_MAX_DIFF_BYTES", 10)
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"x" * 11)
    change = fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))
    assert change.diff_truncated and len(change.diff) == 10
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"  \n")
    with pytest.raises(ValueError, match="empty diff"):
        fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))


def test_fetch_change_rejects_a_malformed_commit(monkeypatch):
    gh = MagicMock()
    pull = gh.get_repo.return_value.get_pull.return_value
    pull.head.sha, pull.base.sha = "--upload-pack=evil", BASE
    monkeypatch.setattr(propose_mod, "_download_diff", lambda token, path: b"diff\n")
    with pytest.raises(ValueError, match="malformed commit"):
        fetch_change(gh, "t", ChangeRequest("valkey-io/valkey", pr_number=1))


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
    assert [s["rationale"] for s in result["scenarios"]] == ["drives failover"]
    assert result["dropped"] == [
        {"index": 2, "rationale": "still wrong", "error": COMPILER_ERROR, "repaired": True},
    ]
    assert sorted(p.name for p in (out / "scenarios").iterdir()) == [
        f"{result['scenarios'][0]['scenario_id']}.yaml",
    ]


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
        assert _stub_validator(out / "scenarios" / name) is None
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


def test_failed_repair_round_drops_the_invalid_ones(tmp_path, fuzzer_dir):
    runner = StubRunner(
        _answer([{"rationale": "a", "scenario": GOOD}, {"rationale": "b", "scenario": BAD}]),
        _result("", rc=1), _result("", rc=1),
    )
    run, result, out = _propose(runner, tmp_path, fuzzer_dir)
    assert len(result["scenarios"]) == 1
    assert result["dropped"][0]["error"] == COMPILER_ERROR
    assert result["dropped"][0]["repaired"] is False


def test_unusable_first_answer_raises(tmp_path, fuzzer_dir):
    with pytest.raises(ValueError, match="scenarios list"):
        _propose(StubRunner(_result("I could not find anything")), tmp_path, fuzzer_dir)


def test_summary_lists_validated_scenarios():
    result = {
        "change": _change().summary(),
        "scenarios": [{"scenario_id": "ai-1", "file": "scenarios/ai-1.yaml", "rationale": "a | b", "repaired": True}],
        "dropped": [{"index": 2}],
    }
    text = render_summary(result)
    assert "valkey-io/valkey#123" in text
    assert "`ai-1` (repaired) | a \\| b |" in text
    assert "Dropped 1" in text


# --- The fuzzer command --------------------------------------------------------


@pytest.fixture
def fake_fuzzer(tmp_path) -> str:
    """A stand-in valkey-fuzzer with the same list and validate interface."""
    script = tmp_path / "bin" / "valkey-fuzzer"
    script.parent.mkdir()
    script.write_text(textwrap.dedent(f"""\
        #!{sys.executable}
        import json, sys
        args = sys.argv[1:]
        if args[:1] == ["list"]:
            print(json.dumps({json.dumps(CATALOG)}[args[1]]))
        elif args[:1] == ["validate"]:
            text = open(args[1]).read()
            if "shard: 9" in text:
                print("Validating scenario: " + args[1])
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
    assert validate(good) is None
    assert validate(bad) == COMPILER_ERROR


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
