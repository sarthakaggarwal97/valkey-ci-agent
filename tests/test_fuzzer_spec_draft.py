"""Tests for AI-drafted TLA+ spec bundles."""
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
import scripts.fuzzer.spec_draft as spec_mod
from scripts.ai.runtime import AgentRunResult
from scripts.fuzzer.propose import Change
from scripts.fuzzer.spec_draft import (
    AnswerError,
    HarnessError,
    SpecTools,
    assurance_problems,
    batch_inputs,
    check_draft,
    draft_spec,
    module_references,
    parse_answer,
)

HEAD = "a" * 40
BASE = "b" * 40
_FAKE_FUZZER = Path(__file__).parent / "fixtures" / "fake_spec_fuzzer.py"

# The fuzzer's own toy bundle (valkey-fuzzer tests/fixtures/spec/register/).
REGISTER = {
    "claims.yaml": textwrap.dedent("""\
        protocol: register
        summary: A register that saves each write before acknowledging it never loses an acknowledged write.
        status: draft
        scope:
          - file: register.c
            functions: [registerWrite, registerAck, registerSave]
        properties:
          - name: AckedDurable
            kind: invariant
            statement: An acknowledged write survives a crash.
            holds_when: {SyncBeforeAck: true}
            negative_controls: [ack-before-save]
            conform: true
        targets:
          - name: AckAheadOfDisk
            meaning: An acknowledged write is not on disk yet.
        assumptions:
          - The disk keeps what was saved across a crash.
        bounds:
          MaxValue: 3
          MaxCrashes: 1
        exclusions:
          - Partial writes.
    """),
    "mapping.yaml": textwrap.dedent("""\
        evidence:
          wrote:
            event: toy.write
            fields: {value: int}
            action: Write
            updates:
              - {set: mem, value: $value}
          acked:
            event: toy.ack
            log: 'acked (?P<value>\\d+)'
            fields: {instance_id: node, value: int}
            action: Ack
            updates:
              - {set: acked, value: $value}
          crash:
            event: '*.injected'
            where: {signal: SIGKILL}
            fields:
              disk_value: {type: int, path: disk}
            action: Crash
            updates:
              - {set: mem, value: $disk_value}
              - {set: disk, value: $disk_value}
              - {incr: crashes}
          disk:
            sample: true
            fields: {disk: int}
            updates:
              - {max: disk, value: $disk}
        unobservable:
          Save: The register does not log its saves; disk samples raise disk.
        state:
          mem: {init: 0}
          disk: {init: 0}
          acked: {init: 0}
          crashes: {init: 0}
        constants:
          SyncBeforeAck: {config: sync-before-ack, equals: "yes"}
          MaxValue: {max_of: [mem, acked, disk], at_least: 1}
          MaxCrashes: {max_of: [crashes], at_least: 1}
        scenario:
          experiment: toy.register
          deployment: {mode: standalone}
          inputs:
            sync: {constant: SyncBeforeAck, map: {"TRUE": true, "FALSE": false}}
          faults:
            - {action: Crash, after: acked, occurrence_input: crash_after_ack}
          variants:
            occurrence_shift: [1]
            inputs:
              crash_delay_s: [0, 0.01]
    """),
    "model.cfg": textwrap.dedent("""\
        \\* Writes are saved before they are acknowledged.
        CONSTANTS
            SyncBeforeAck = TRUE
            MaxValue = 3
            MaxCrashes = 1
        SPECIFICATION Spec
        INVARIANT AckedDurable
        CHECK_DEADLOCK FALSE
    """),
    "model.tla": textwrap.dedent("""\
        ---- MODULE model ----
        \\* A toy register that acknowledges writes, saves them, and may crash.
        EXTENDS Naturals

        CONSTANTS
            SyncBeforeAck,  \\* TRUE: a write is acknowledged only once it is saved
            MaxValue,
            MaxCrashes

        VARIABLES mem, disk, acked, crashes

        vars == <<mem, disk, acked, crashes>>

        Init == mem = 0 /\\ disk = 0 /\\ acked = 0 /\\ crashes = 0

        Write == mem < MaxValue /\\ mem' = mem + 1 /\\ UNCHANGED <<disk, acked, crashes>>

        Ack == /\\ acked < mem
               /\\ SyncBeforeAck => disk = mem
               /\\ acked' = mem
               /\\ UNCHANGED <<mem, disk, crashes>>

        Save == disk # mem /\\ disk' = mem /\\ UNCHANGED <<mem, acked, crashes>>

        Crash == /\\ crashes < MaxCrashes
                 /\\ mem' = disk
                 /\\ crashes' = crashes + 1
                 /\\ UNCHANGED <<disk, acked>>

        Next == Write \\/ Ack \\/ Save \\/ Crash

        Spec == Init /\\ [][Next]_vars

        \\* An acknowledged write is never lost.
        AckedDurable == acked <= mem

        \\* Target: an acknowledged write is not on disk yet.
        AckAheadOfDisk == acked > disk
        ====
    """),
    "negative-controls/ack-before-save.cfg": textwrap.dedent("""\
        \\* A write may be acknowledged before it is saved; a crash then loses it.
        CONSTANTS
            SyncBeforeAck = FALSE
            MaxValue = 3
            MaxCrashes = 1
        SPECIFICATION Spec
        INVARIANT AckedDurable
        CHECK_DEADLOCK FALSE
    """),
}
NOTES = {"rationale": "The change reorders the save and the acknowledgement.",
         "counterexamples": {"ack-before-save": "A write acknowledged before its save is lost by a crash."}}


def _vacuous(files: dict[str, str]) -> dict[str, str]:
    """The same bundle, with a property that cannot fail."""
    return {**files, "model.tla": files["model.tla"].replace("AckedDurable == acked <= mem", "AckedDurable == TRUE")}


def _syntax_error(files: dict[str, str]) -> dict[str, str]:
    return {**files, "model.tla": files["model.tla"].replace("AckedDurable == acked <= mem",
                                                             "AckedDurable == (acked <= mem")}


def _bundle_text(files: dict[str, str], notes: dict | None = None) -> str:
    parts = ["Here is the bundle.\n"]
    if notes is not None:
        parts.append("<<<NOTES>>>\n" + yaml.safe_dump(notes) + "<<<END>>>\n")
    for name, text in files.items():
        parts.append(f"<<<FILE {name}>>>\n{text}<<<END>>>\n")
    return "".join(parts)


def _result(stdout: str, rc: int = 0) -> AgentRunResult:
    # Claude Code's stream-json: the final text is in the result event.
    stream = json.dumps({"type": "result", "result": stdout}) if rc == 0 else ""
    return AgentRunResult(
        profile="fuzzer_spec_draft_readonly", stdout=stream, stderr="" if rc == 0 else "boom",
        returncode=rc, prompt_sha256="x", cwd="", allowed_tools="Read,Grep,Glob", model="",
        started_at="", finished_at="",
    )


def _bundle(files: dict[str, str], notes: dict | None = NOTES) -> AgentRunResult:
    return _result(_bundle_text(files, notes))


class StubRunner:
    """Stands in for Claude: hands back the queued answers, records the prompts."""

    def __init__(self, *answers: AgentRunResult) -> None:
        self.answers = list(answers)
        self.prompts: list[str] = []
        self.drafts: list[dict[str, str]] = []

    def __call__(self, prompt: str, workdir: Path) -> AgentRunResult:
        self.prompts.append(prompt)
        draft = workdir / "draft"
        self.drafts.append({
            str(p.relative_to(draft)): p.read_text() for p in draft.rglob("*") if p.is_file()
        } if draft.is_dir() else {})
        return self.answers.pop(0)


def _change(**kw) -> Change:
    defaults = dict(repo="valkey-io/valkey", pr_number=123, title="Save before ack", base_sha=BASE,
                    head_sha=HEAD, diff="diff --git a/src/register.c b/src/register.c\n",
                    fetch_ref="refs/pull/123/head")
    defaults.update(kw)
    return Change(**defaults)


@pytest.fixture
def fake_fuzzer(tmp_path) -> str:
    d = tmp_path / "fake-bin"
    d.mkdir()
    script = d / "valkey-fuzzer"
    script.write_text(f"#!{sys.executable}\n" + _FAKE_FUZZER.read_text())
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script)


def _calls(fake_fuzzer: str) -> list[list[str]]:
    log = Path(fake_fuzzer).with_name("calls.log")
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


@pytest.fixture
def jar(tmp_path) -> Path:
    path = tmp_path / "tla2tools.jar"
    path.write_bytes(b"PK")
    return path


@pytest.fixture
def fuzzer_dir(tmp_path) -> Path:
    d = tmp_path / "valkey-fuzzer"
    (d / "docs").mkdir(parents=True)
    (d / "docs" / "spec-bridge.md").write_text("# Spec bridge\n")
    (d / "docs" / "scenarios-and-experiments.md").write_text("# Scenarios\n")
    example = d / "tests" / "fixtures" / "spec" / "register"
    for name, text in REGISTER.items():
        (example / name).parent.mkdir(parents=True, exist_ok=True)
        (example / name).write_text(text)
    return d


@pytest.fixture(autouse=True)
def no_real_claude(monkeypatch):
    def refuse(*_a, **_kw):
        raise AssertionError("a test reached the real Claude runner")

    monkeypatch.setattr(spec_mod, "run_agent", refuse)


@pytest.fixture(autouse=True)
def no_clone(monkeypatch):
    calls: list[tuple] = []

    def fake_clone(repo, dest, sha=None, *, fetch_ref=None):
        calls.append((repo, sha, fetch_ref))
        return True

    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", fake_clone)
    return calls


CATALOG = {"experiments": [{"id": "toy.register"}], "components": []}


def _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar, **tool_kw):
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    tools = SpecTools(fake_fuzzer, str(jar), **tool_kw)
    run = draft_spec(_change(), workdir=work, out_dir=out, catalog=CATALOG, fuzzer_dir=fuzzer_dir,
                     tools=tools, runner=runner)
    return run, json.loads((out / "spec-draft.json").read_text()), out


# --- Claude's answer --------------------------------------------------------------


def test_parse_answer_reads_the_blocks_from_stream_json():
    answer = parse_answer(_bundle(REGISTER).stdout)
    assert answer.files == REGISTER
    assert answer.notes["counterexamples"] == NOTES["counterexamples"]
    assert answer.no_change is None


def test_parse_answer_keeps_tla_backslashes_verbatim():
    answer = parse_answer(_bundle_text(REGISTER))
    assert "Init == mem = 0 /\\ disk = 0" in answer.files["model.tla"]
    assert answer.files["model.cfg"].startswith("\\* Writes are saved")


def test_parse_answer_no_protocol_change():
    answer = parse_answer("<<<NO-PROTOCOL-CHANGE>>>\nOnly a log message\nchanged.\n<<<END>>>\n")
    assert answer.no_change == "Only a log message changed."
    assert answer.files == {}


@pytest.mark.parametrize("text, error", [
    ("Nothing to see.", "no FILE blocks"),
    ("<<<FILE model.tla>>>\n---- MODULE model ----\n", "has no <<<END>>>"),
    ("<<<FILE ../evil.sh>>>\nrm -rf /\n<<<END>>>\n", "not a bundle path"),
    ("<<<FILE src/model.tla>>>\nx\n<<<END>>>\n", "not a bundle path"),
    ("<<<FILE model.tla>>>\na\n<<<END>>>\n<<<FILE model.tla>>>\nb\n<<<END>>>\n", "appears twice"),
    ("<<<FILE model.tla>>>\n\n<<<END>>>\n", "is empty"),
    ("<<<NO-PROTOCOL-CHANGE>>>\nnone\n<<<END>>>\n<<<FILE model.tla>>>\nx\n<<<END>>>\n", "both"),
])
def test_parse_answer_rejects(text, error):
    with pytest.raises(AnswerError, match=error):
        parse_answer(text)


# --- The draft gate ---------------------------------------------------------------


def test_check_draft_accepts_the_register_bundle():
    assert check_draft(REGISTER) is None


@pytest.mark.parametrize("change, error", [
    (lambda f: {k: v for k, v in f.items() if k != "mapping.yaml"}, "no mapping.yaml"),
    (lambda f: {k: v for k, v in f.items() if k != "model.cfg"}, "no expected-safe .cfg"),
    (lambda f: {k: v for k, v in f.items() if not k.startswith("negative-controls/")}, "negative-controls"),
    (lambda f: {**f, "claims.yaml": f["claims.yaml"].replace("status: draft", "status: approved")},
     "status must be draft"),
    (lambda f: {**f, "claims.yaml": f["claims.yaml"] + "approved_by: someone\n"}, "approved_by"),
    (lambda f: {**f, "claims.yaml": f["claims.yaml"].replace("protocol: register", "protocol: ../x")},
     "protocol must be"),
    (lambda f: {**f, "model.tla": f["model.tla"].replace("EXTENDS Naturals", "EXTENDS Naturals, IOUtils")},
     "IOUtils"),
    (lambda f: {**f, "negative-controls/ack-before-save.tla":
                "---- MODULE ack-before-save ----\nINSTANCE Json\n====\n"}, "Json"),
])
def test_check_draft_rejects(change, error):
    assert error in (check_draft(change(REGISTER)) or "")


def test_module_references_ignore_comments_and_allow_own_modules():
    text = "(* EXTENDS IOUtils *)\n\\* INSTANCE Json\nEXTENDS Naturals,\n  Sequences\nM == INSTANCE model\n"
    assert module_references(text) == {"Naturals", "Sequences", "model"}
    files = {**REGISTER, "negative-controls/ack-before-save.tla":
             "---- MODULE ack-before-save ----\nEXTENDS model, TLC\n====\n"}
    assert check_draft(files) is None


# --- Assurance reports ------------------------------------------------------------


def _report(**gates) -> dict:
    base = {name: {"status": "pass"} for name in spec_mod.REQUIRED_ASSURANCE_GATES}
    base.update(gates)
    return {"kind": "spec-assurance", "status": "pass", "claims": {"status": "draft"}, "gates": base}


def test_assurance_problems_none_when_every_gate_passed():
    assert assurance_problems(_report()) == []


def test_assurance_problems_name_a_vacuous_control():
    report = _report(negative_controls={"status": "fail", "controls": {"ack-before-save": {
        "status": "fail", "violates": "AckedDurable",
        "error": "expected TLC to report AckedDurable violated; it found no error"}}})
    report["status"] = "fail"
    assert assurance_problems(report) == [
        "negative_controls: ack-before-save (must violate AckedDurable): "
        "expected TLC to report AckedDurable violated; it found no error"]


def test_assurance_problems_require_every_gate_and_a_draft():
    report = _report()
    del report["gates"]["negative_controls"]
    assert "did not run the gates negative_controls" in assurance_problems(report)[0]
    approved = _report()
    approved["claims"]["status"] = "approved"
    assert assurance_problems(approved) == ["the checked claims are not a draft"]
    assert assurance_problems(None) == ["spec check did not write a spec-assurance report"]


# --- The run ----------------------------------------------------------------------


def test_a_good_draft_passes_every_gate(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_bundle(REGISTER))
    run, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert run.verdict == "passed" and result["verdict"] == "passed"
    assert len(runner.prompts) == 1
    assert [g["gate"] for g in result["gates"]] == list(spec_mod.GATES)
    assert all(g["passed"] for g in result["gates"])
    assert (out / "bundle" / "model.tla").read_text() == REGISTER["model.tla"]
    assert (out / "assurance.json").is_file() and (out / "validate.json").is_file()
    [scenario] = result["scenarios"]
    assert scenario["file"] == "scenarios/ack-before-save/spec-register-ack-before-save.yaml"
    assert (out / scenario["file"]).is_file()
    assert result["claims"]["status"] == "draft"
    assert result["counterexamples"][0]["meaning"] == NOTES["counterexamples"]["ack-before-save"]
    summary = (out / "summary.md").read_text()
    assert "passed every gate" in summary and "--require-approved" in summary
    assert "AckedDurable" in summary and "Partial writes." in summary
    assert "| SyncBeforeAck = true |" in summary
    # The gates ran in the attempt directory with relative paths and in order.
    commands = [c[:2] for c in _calls(fake_fuzzer)]
    assert commands == [["spec", "check"], ["spec", "translate"], ["validate", "scenarios/ack-before-save/"
                                                                    "spec-register-ack-before-save.yaml"]]


def test_a_vacuous_claim_is_rejected_by_the_negative_control_gate(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    vacuous = _bundle(_vacuous(REGISTER))
    runner = StubRunner(vacuous, vacuous, vacuous)
    run, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "rejected"
    assert [r["failed_gate"] for r in result["rounds"]] == ["spec_check"] * 3
    assert "negative_controls: ack-before-save (must violate AckedDurable)" in result["rounds"][0]["output"]
    assert "spec_check gate still failed after 2 repair round(s)" in result["reason"]
    # Two repairs, each a fresh session given the gate's output and the failed bundle.
    assert len(runner.prompts) == 3
    assert all("vacuous" in p and "failed the spec_check gate" in p for p in runner.prompts[1:])
    assert runner.drafts[1]["model.tla"] == _vacuous(REGISTER)["model.tla"]
    # Kept as rejected: the bundle and its report, but no scenario.
    assert (out / "bundle" / "model.tla").is_file() and (out / "assurance.json").is_file()
    assert not (out / "scenarios").exists() and result["scenarios"] == []
    assert batch_inputs(out) is None
    assert "Rejected" in (out / "summary.md").read_text()


def test_a_syntax_error_is_repaired_once_and_then_passes(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_bundle(_syntax_error(REGISTER)), _bundle(REGISTER))
    run, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "passed"
    assert [r["failed_gate"] for r in result["rounds"]] == ["spec_check", None]
    assert [r["session"] for r in result["rounds"]] == ["draft", "repair"]
    assert "parsed: SANY rejected model.tla" in runner.prompts[1]
    assert "***Parse Error***" in runner.prompts[1]
    assert "(acked <= mem" in runner.drafts[1]["model.tla"]
    assert (out / "bundle" / "model.tla").read_text() == REGISTER["model.tla"]


def test_a_draft_that_never_passes_is_rejected_and_never_dispatched(
        tmp_path, fuzzer_dir, fake_fuzzer, jar, monkeypatch):
    bad = _bundle(_syntax_error(REGISTER))
    runner = StubRunner(bad, _bundle(_vacuous(REGISTER)), bad)
    _, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "rejected"
    assert [r["failed_gate"] for r in result["rounds"]] == ["spec_check"] * 3
    dispatched = MagicMock()
    monkeypatch.setattr(spec_mod, "dispatch_batch", dispatched)
    monkeypatch.setenv("TARGET_TOKEN", "t")
    assert spec_mod.main(["dispatch", "--out", str(out)]) == 0
    dispatched.assert_not_called()


def test_an_approved_draft_fails_the_draft_gate_and_goes_back(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    approved = {**REGISTER, "claims.yaml": REGISTER["claims.yaml"].replace("status: draft", "status: approved")
                + "approved_by: claude\n"}
    runner = StubRunner(_bundle(approved), _bundle(REGISTER))
    _, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert [r["failed_gate"] for r in result["rounds"]] == ["draft", None]
    assert "only a maintainer approves claims" in runner.prompts[1]
    assert _calls(fake_fuzzer)[0][:2] == ["spec", "check"]  # the fuzzer never saw the approved draft
    assert result["verdict"] == "passed"


def test_an_unparseable_answer_is_repaired(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_result("I could not decide."), _bundle(REGISTER))
    _, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert [r["failed_gate"] for r in result["rounds"]] == ["answer", None]
    assert "failed the answer gate" in runner.prompts[1]
    assert result["verdict"] == "passed"


def test_a_scenario_that_does_not_validate_fails_the_validate_gate(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    broken = {**REGISTER, "mapping.yaml": REGISTER["mapping.yaml"].replace(
        "experiment: toy.register", "experiment: missing.experiment")}
    runner = StubRunner(_bundle(broken), _bundle(broken), _bundle(broken))
    _, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "rejected"
    assert result["rounds"][0]["failed_gate"] == "validate"
    assert "experiment 'missing.experiment' is not registered" in result["rounds"][0]["output"]
    assert not (out / "scenarios").exists()


def test_no_protocol_change_stops_after_one_session(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_result("<<<NO-PROTOCOL-CHANGE>>>\nIt only renames a log field.\n<<<END>>>\n"))
    run, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "no_protocol_change"
    assert result["reason"] == "It only renames a log field."
    assert len(runner.prompts) == 1 and _calls(fake_fuzzer) == []
    assert "nothing to model" in (out / "summary.md").read_text()


def test_a_failed_repair_session_rejects_the_draft(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_bundle(_vacuous(REGISTER)), _result("", rc=1), _result("", rc=1))
    _, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "rejected"
    assert result["reason"].startswith("the repair session failed")


def test_a_broken_harness_is_an_error_not_a_rejection(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    broken = {**REGISTER, "model.tla": REGISTER["model.tla"].replace("EXTENDS Naturals",
                                                                     "EXTENDS Naturals \\* HARNESS-BROKEN")}
    with pytest.raises(HarnessError, match="java not found"):
        _draft(StubRunner(_bundle(broken)), tmp_path, fuzzer_dir, fake_fuzzer, jar)


def test_a_model_too_big_for_tlc_times_out_as_a_gate_failure(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    slow = {**REGISTER, "model.tla": REGISTER["model.tla"].replace("EXTENDS Naturals",
                                                                   "EXTENDS Naturals \\* SLOW")}
    runner = StubRunner(_bundle(slow), _bundle(REGISTER))
    _, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar, check_timeout_s=1)
    assert result["rounds"][0]["failed_gate"] == "spec_check"
    assert "did not finish within 1s" in result["rounds"][0]["output"]
    assert result["verdict"] == "passed"


def test_the_fuzzer_gets_no_credentials(tmp_path, fuzzer_dir, jar, monkeypatch):
    seen = tmp_path / "env.json"
    script = tmp_path / "env-fuzzer"
    script.write_text(f"#!{sys.executable}\nimport json, os\n"
                      f"open({str(seen)!r}, 'w').write(json.dumps(dict(os.environ)))\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    for name in ("TARGET_TOKEN", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "secret")
    (tmp_path / "attempt").mkdir()
    SpecTools(str(script), str(jar)).check(tmp_path / "attempt")
    env = json.loads(seen.read_text())
    assert not {"TARGET_TOKEN", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} & set(env)


# --- Dispatch ---------------------------------------------------------------------


def test_batch_inputs_carry_a_passing_drafts_scenarios(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, _, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    inputs = batch_inputs(out)
    assert inputs is not None
    assert list(json.loads(inputs["scenarios"])) == ["spec-register-ack-before-save"]
    assert inputs["valkey_ref"] == "refs/pull/123/head" and inputs["valkey_sha"] == HEAD


def test_batch_inputs_refuse_a_scenario_changed_after_validation(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, result, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    path = out / result["scenarios"][0]["file"]
    path.write_text(path.read_text().replace("seed: 1", "seed: 2"))
    with pytest.raises(ValueError, match="changed since it was validated"):
        batch_inputs(out)


def test_batch_inputs_refuse_a_file_outside_the_scenarios(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, result, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    result["scenarios"][0]["file"] = "bundle/model.tla"
    (out / "spec-draft.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match="outside"):
        batch_inputs(out)


# --- Dry runs ---------------------------------------------------------------------


def _block_remote_calls(monkeypatch) -> None:
    def refuse(*_a, **_kw):
        raise AssertionError("a dry run made a remote call")

    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(propose_mod, "urlopen", refuse)
    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", refuse)
    monkeypatch.setattr(spec_mod, "run_agent", refuse)
    monkeypatch.setattr(spec_mod, "fetch_change", refuse)
    monkeypatch.setattr(spec_mod, "dispatch_batch", refuse)
    import github
    monkeypatch.setattr(github, "Github", refuse)
    monkeypatch.delenv("TARGET_TOKEN", raising=False)


def test_draft_dry_run_makes_no_remote_calls(monkeypatch, fake_fuzzer, fuzzer_dir, jar, tmp_path):
    _block_remote_calls(monkeypatch)
    rc = spec_mod.main([
        "draft", "--pr", "123", "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer,
        "--tla2tools", str(jar), "--out", str(tmp_path / "out"), "--dry-run",
    ])
    assert rc == 0
    assert not (tmp_path / "out").exists()
    # The dry run still checked the tools locally, on the fuzzer's own register bundle.
    assert [c[:2] for c in _calls(fake_fuzzer)] == [["list", "experiments"], ["list", "components"],
                                                    ["spec", "check"]]


def test_draft_dry_run_fails_when_the_tools_do_not_work(monkeypatch, fake_fuzzer, fuzzer_dir, tmp_path):
    _block_remote_calls(monkeypatch)
    rc = spec_mod.main([
        "draft", "--pr", "123", "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer,
        "--tla2tools", str(tmp_path / "missing.jar"), "--dry-run",
    ])
    assert rc == 1


def test_dispatch_dry_run_makes_no_remote_calls(monkeypatch, tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, _, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    _block_remote_calls(monkeypatch)
    assert spec_mod.main(["dispatch", "--out", str(out), "--dry-run"]) == 0


def test_draft_rejects_a_fuzzer_without_the_spec_bridge(tmp_path, fake_fuzzer, jar):
    with pytest.raises(SystemExit):
        spec_mod.main(["draft", "--pr", "1", "--fuzzer-dir", str(tmp_path), "--fuzzer-bin", fake_fuzzer,
                       "--tla2tools", str(jar), "--dry-run"])


def test_draft_for_real_writes_the_artifact(monkeypatch, fake_fuzzer, fuzzer_dir, jar, tmp_path):
    monkeypatch.setenv("TARGET_TOKEN", "t")
    monkeypatch.setattr(spec_mod, "fetch_change", lambda gh, token, request: _change())
    monkeypatch.setattr(spec_mod, "_default_runner", StubRunner(_bundle(REGISTER)))
    import github
    monkeypatch.setattr(github, "Github", MagicMock())
    out = tmp_path / "out"
    rc = spec_mod.main(["draft", "--pr", "123", "--fuzzer-dir", str(fuzzer_dir), "--fuzzer-bin", fake_fuzzer,
                        "--tla2tools", str(jar), "--out", str(out)])
    assert rc == 0
    assert json.loads((out / "spec-draft.json").read_text())["verdict"] == "passed"


# --- End to end, with the real fuzzer and TLC -------------------------------------
#
# Set VALKEY_FUZZER_DIR (a valkey-fuzzer checkout with the spec bridge),
# VALKEY_FUZZER_BIN (its installed command) and TLA2TOOLS_JAR to run these.

_E2E = {name: os.environ.get(name, "") for name in ("VALKEY_FUZZER_DIR", "VALKEY_FUZZER_BIN", "TLA2TOOLS_JAR")}
e2e = pytest.mark.skipif(
    not all(_E2E.values()), reason="needs VALKEY_FUZZER_DIR, VALKEY_FUZZER_BIN and TLA2TOOLS_JAR",
)


def _real_bundle() -> dict[str, str]:
    root = Path(_E2E["VALKEY_FUZZER_DIR"]) / "tests" / "fixtures" / "spec" / "register"
    return {str(p.relative_to(root)): p.read_text() for p in sorted(root.rglob("*")) if p.is_file()}


def _real_draft(runner, tmp_path):
    fuzzer_dir = Path(_E2E["VALKEY_FUZZER_DIR"])
    tools = SpecTools(_E2E["VALKEY_FUZZER_BIN"], _E2E["TLA2TOOLS_JAR"],
                      experiment_dirs=(str(fuzzer_dir / "tests" / "fixtures" / "spec" / "experiments"),))
    tools.preflight(fuzzer_dir)
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    run = draft_spec(_change(), workdir=work, out_dir=out, catalog=CATALOG, fuzzer_dir=fuzzer_dir,
                     tools=tools, runner=runner)
    return run, json.loads((out / "spec-draft.json").read_text()), out


@e2e
def test_e2e_the_fuzzers_register_bundle_passes(tmp_path):
    bundle = _real_bundle()
    run, result, out = _real_draft(StubRunner(_bundle(bundle)), tmp_path)
    assert result["verdict"] == "passed", result["rounds"]
    assurance = json.loads((out / "assurance.json").read_text())
    assert assurance["status"] == "pass"
    assert assurance["gates"]["negative_controls"]["controls"]["ack-before-save"]["violated"] == "AckedDurable"
    assert assurance["gates"]["approved_claim"]["claims_status"] == "draft"
    ids = sorted(s["scenario_id"] for s in result["scenarios"])
    assert "spec-register-ack-before-save" in ids and len(ids) >= 2
    assert json.loads((out / "validate.json").read_text())["ok"] is True
    assert result["counterexamples"][0]["actions"][-1] == "Crash"
    assert batch_inputs(out) is not None


@e2e
def test_e2e_a_vacuous_register_bundle_is_rejected(tmp_path, monkeypatch):
    vacuous = _bundle(_vacuous(_real_bundle()))
    runner = StubRunner(vacuous, vacuous, vacuous)
    run, result, out = _real_draft(runner, tmp_path)
    assert result["verdict"] == "rejected"
    assert [r["failed_gate"] for r in result["rounds"]] == ["spec_check"] * 3
    assert "the claim cannot fail as configured (vacuous)" in result["rounds"][0]["output"]
    assurance = json.loads((out / "assurance.json").read_text())
    assert assurance["gates"]["negative_controls"]["status"] == "fail"
    assert not (out / "scenarios").exists()
    dispatched = MagicMock()
    monkeypatch.setattr(spec_mod, "dispatch_batch", dispatched)
    monkeypatch.setenv("TARGET_TOKEN", "t")
    assert spec_mod.main(["dispatch", "--out", str(out)]) == 0
    dispatched.assert_not_called()
