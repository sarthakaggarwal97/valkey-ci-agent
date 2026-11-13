"""Tests for AI-drafted TLA+ spec bundles."""
from __future__ import annotations

import hashlib
import json
import os
import re
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
    parse_answer,
    translatable_controls,
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
            - {action: Crash, after: acked, component: crash, occurrence_input: crash_after_ack}
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
# The data-only experiment a bundle may bring, as the fuzzer's register does.
OWN_EXPERIMENT = textwrap.dedent("""\
    id: toy.own
    deployment:
      modes: [standalone]
    inputs:
      type: object
      properties:
        appendonly: {type: string, enum: ["yes", "no"], default: "yes"}
    components:
      - {use: valkey.fault.process-signal, id: crash, config: {when_event: writes.seeded, signal: SIGKILL}}
      - {use: valkey.observer.log-trigger, id: reload-log, config: {spec: register}}
""")
ONBOARDED = {
    **REGISTER,
    "mapping.yaml": REGISTER["mapping.yaml"].replace("experiment: toy.register", "experiment: toy.own"),
    "experiments/toy.own.yaml": OWN_EXPERIMENT,
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


def _log(fake_fuzzer: str) -> list[dict]:
    log = Path(fake_fuzzer).with_name("calls.log")
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def _calls(fake_fuzzer: str) -> list[list[str]]:
    return [entry["argv"] for entry in _log(fake_fuzzer)]


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
    for name, text in ONBOARDED.items():
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


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(event) for event in events)


def _assistant(*blocks: dict) -> dict:
    return {"type": "assistant", "message": {"content": list(blocks)}}


def test_parse_answer_reads_a_bundle_that_went_on_in_a_second_message():
    # The first message is cut off inside mapping.yaml; Claude Code has the
    # model go on, and the next message writes mapping.yaml again. The result
    # event holds only that last message.
    files = list(REGISTER.items())
    first = "".join(f"<<<FILE {name}>>>\n{text}<<<END>>>\n" for name, text in files if name != "mapping.yaml")
    first += "<<<FILE mapping.yaml>>>\n" + REGISTER["mapping.yaml"][:40]
    second = f"Continuing.\n<<<FILE mapping.yaml>>>\n{REGISTER['mapping.yaml']}<<<END>>>\n"
    stdout = _stream(
        _assistant({"type": "text", "text": "<<<FILE model.tla>>>\nnot the answer\n<<<END>>>\n"},
                   {"type": "tool_use", "name": "Read", "input": {}}),
        {"type": "user", "message": {"content": [{"type": "tool_result", "content": "..."}]}},
        _assistant({"type": "text", "text": first}),
        {"type": "user", "message": {"content": [{"type": "text", "text": "Output token limit hit. Resume."}]}},
        _assistant({"type": "text", "text": second}),
        {"type": "result", "result": second},
    )
    assert parse_answer(stdout).files == REGISTER


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
])
def test_check_draft_rejects(change, error):
    assert error in (check_draft(change(REGISTER)) or "")


def test_check_draft_leaves_the_modules_to_the_fuzzer():
    files = {**REGISTER, "model.tla": REGISTER["model.tla"].replace("EXTENDS Naturals", "EXTENDS Naturals, IOUtils")}
    assert check_draft(files) is None


def test_parse_answer_accepts_a_bundles_own_experiment():
    assert parse_answer(_bundle_text(ONBOARDED)).files["experiments/toy.own.yaml"] == OWN_EXPERIMENT
    with pytest.raises(AnswerError, match="not a bundle path"):
        parse_answer("<<<FILE experiments/x/y.yaml>>>\nid: y\n<<<END>>>\n")


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
    assert (out / "bundle" / "register" / "model.tla").read_text() == REGISTER["model.tla"]
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
    calls = _log(fake_fuzzer)
    assert [c["argv"][:3] for c in calls] == [
        ["spec", "check", "./register"], ["spec", "translate", "./register"],
        ["validate", "scenarios/ack-before-save/spec-register-ack-before-save.yaml", "--format"]]
    # The fuzzer's own --timeout bounds TLC; the protocol's name finds the draft, before any installed bundle.
    assert calls[0]["argv"][calls[0]["argv"].index("--timeout") + 1] == "1800"
    assert "--timeout" in calls[1]["argv"]
    assert all(c["spec_path"] == calls[0]["spec_path"] and c["spec_path"].endswith("round-0") for c in calls)
    assert "--experiment-dir" not in calls[2]["argv"]


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
    assert (out / "bundle" / "register" / "model.tla").is_file() and (out / "assurance.json").is_file()
    assert not (out / "scenarios").exists() and result["scenarios"] == []
    assert batch_inputs(out) is None
    assert "Rejected" in (out / "summary.md").read_text()


def test_a_syntax_error_is_repaired_once_and_then_passes(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    runner = StubRunner(_bundle(_syntax_error(REGISTER)), _bundle(REGISTER))
    run, result, out = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "passed"
    assert [r["failed_gate"] for r in result["rounds"]] == ["spec_check", None]
    assert [r["session"] for r in result["rounds"]] == ["draft", "repair"]
    assert "parsed: model.tla: SANY rejected it" in runner.prompts[1]
    assert "***Parse Error***" in runner.prompts[1]
    assert "(acked <= mem" in runner.drafts[1]["model.tla"]
    assert (out / "bundle" / "register" / "model.tla").read_text() == REGISTER["model.tla"]


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
    output = result["rounds"][0]["output"]
    # Read from the report: its error, and the run marked timed_out. The
    # fuzzer exits 2 for it, as for a broken harness, but this one is the draft's.
    assert "spec check stopped: TLC was stopped after 1s" in output
    assert "make the bounds smaller" in output
    assert "model_checked: model.cfg: TLC was stopped after 1s" in output
    assert result["verdict"] == "passed"
    argv = _calls(fake_fuzzer)[0]
    assert argv[argv.index("--timeout") + 1] == "1"


def test_a_bundles_own_experiment_is_found_by_protocol_and_validated(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    run, result, out = _draft(StubRunner(_bundle(ONBOARDED)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "passed", result["rounds"]
    attempt = tmp_path / "work" / "attempts" / "round-0"
    check, translate, validate = _log(fake_fuzzer)
    # Its components name the bundle by protocol, in validate as in the spec tools.
    assert check["spec_path"] == translate["spec_path"] == validate["spec_path"] == str(attempt.resolve())
    argv = validate["argv"]
    assert argv[argv.index("--experiment-dir") + 1] == str((attempt / "register" / "experiments").resolve())
    assert (out / "bundle" / "register" / "experiments" / "toy.own.yaml").read_text() == OWN_EXPERIMENT


def test_a_no_fault_control_is_not_translated(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    files = {
        **REGISTER,
        "claims.yaml": REGISTER["claims.yaml"].replace("targets:", textwrap.dedent("""\
              - name: AckAheadOfDisk
                kind: invariant
                statement: A write is on disk before it is acknowledged.
                holds_when: {SyncBeforeAck: true}
                negative_controls: [nofault-ack]
                conform: true
            targets:""")),
        "negative-controls/nofault-ack.cfg": REGISTER["negative-controls/ack-before-save.cfg"],
    }
    _, result, _ = _draft(StubRunner(_bundle(files)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["verdict"] == "passed", result["rounds"]
    translated = [c[c.index("--control") + 1] for c in _calls(fake_fuzzer) if c[:2] == ["spec", "translate"]]
    assert translated == ["ack-before-save"]
    assert [s["control"] for s in result["scenarios"]] == ["ack-before-save"]


def test_translatable_controls_are_those_the_experiment_gate_passed():
    report = {"gates": {"experiment": {"controls": {
        "a": {"status": "pass"}, "b": {"status": "no-fault"}, "c": {"status": "fail"}}}}}
    assert translatable_controls(report) == ["a"]
    assert translatable_controls(None) == []


def test_the_fuzzers_module_check_is_what_rejects_a_module(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    io = {**REGISTER, "model.tla": REGISTER["model.tla"].replace("EXTENDS Naturals", "EXTENDS Naturals, IOUtils")}
    runner = StubRunner(_bundle(io), _bundle(REGISTER))
    _, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["rounds"][0]["failed_gate"] == "spec_check"
    assert "parsed: model.tla: uses modules IOUtils" in runner.prompts[1]
    assert result["verdict"] == "passed"


def test_the_fuzzer_gets_no_credentials(tmp_path, fuzzer_dir, jar, monkeypatch):
    seen = tmp_path / "env.json"
    script = tmp_path / "env-fuzzer"
    script.write_text(f"#!{sys.executable}\nimport json, os\n"
                      f"open({str(seen)!r}, 'w').write(json.dumps(dict(os.environ)))\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    for name in ("TARGET_TOKEN", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "secret")
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", "/somewhere/else")
    (tmp_path / "attempt" / "register" / "experiments").mkdir(parents=True)
    SpecTools(str(script), str(jar)).check(tmp_path / "attempt", "register")
    env = json.loads(seen.read_text())
    assert not {"TARGET_TOKEN", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} & set(env)
    # The spec path is the attempt's, never the caller's.
    assert env["VALKEY_FUZZER_SPEC_PATH"] == str((tmp_path / "attempt").resolve())


# --- Routing a change to the bundles it touches -----------------------------------


REGISTER_C = textwrap.dedent("""\
    static int helper(int x);

    int registerWrite(int v) {
        int x = v;
        return x;
    }

    static void
    registerSave(void) {
        save();
    }

    int other(void) {
        return 0;
    }
""")
SCOPES = {"register": [{"file": "src/register.c", "functions": ["registerWrite", "registerAck", "registerSave"]},
                       {"file": "src/register.h"}]}


def _diff(path: str, hunks: str, old: str | None = None) -> str:
    return f"diff --git a/{old or path} b/{path}\n--- a/{old or path}\n+++ b/{path}\n" + textwrap.dedent(hunks)


def _route(diff: str, files: dict[str, str] | None = None) -> dict[str, list[str]]:
    files = {"src/register.c": REGISTER_C} if files is None else files
    return spec_mod.route(diff, files.get, SCOPES)


def test_functions_at_finds_definitions_not_declarations():
    assert spec_mod.functions_at(REGISTER_C) == [("registerWrite", 3, 6), ("registerSave", 9, 11), ("other", 13, 15)]


def test_a_change_inside_a_scoped_function_routes_to_its_bundle():
    inside = _diff("src/register.c", """\
        @@ -4 +4 @@ int registerWrite(int v) {
        -    int x = v;
        +    int x = v + 1;
    """)
    assert _route(inside) == {"register": ["src/register.c:registerWrite"]}
    # A deletion counts at the line before it: still inside registerSave.
    deleted = _diff("src/register.c", """\
        @@ -10,2 +9,0 @@ registerSave(void) {
        -    save();
        -    save();
    """)
    assert _route(deleted) == {"register": ["src/register.c:registerSave"]}
    # The same file, outside every scoped function: no bundle.
    outside = _diff("src/register.c", """\
        @@ -14 +14 @@ int other(void) {
        -    return 0;
        +    return 1;
    """)
    assert _route(outside) == {}


def test_a_changed_line_that_names_a_scoped_function_routes_to_its_bundle():
    call = _diff("src/server.c", """\
        @@ -1,0 +2 @@ void serverCron(void) {
        +    registerAck(); registerWriteAll();
    """)
    assert _route(call) == {"register": ["src/register.c:registerAck"]}
    # Only C sources: a test that names it changes no code.
    assert _route(_diff("tests/unit/register.tcl", "@@ -1 +1 @@\n-registerAck\n+registerAck 1\n")) == {}


def test_a_scope_file_without_functions_or_gone_at_the_head_is_touched_whole():
    assert _route(_diff("src/register.h", "@@ -1 +1 @@\n-#define A 1\n+#define A 2\n")) == {
        "register": ["src/register.h"]}
    renamed = _diff("src/store.c", "@@ -1 +1 @@\n-x\n+y\n", old="src/register.c")
    assert _route(renamed, {"src/store.c": "x\n"}) == {"register": ["src/register.c"]}


def test_bundle_scopes_are_the_installed_bundles_claims(fuzzer_dir, installed):
    assert spec_mod.bundle_scopes(fuzzer_dir) == {
        "register": [{"file": "register.c", "functions": ["registerWrite", "registerAck", "registerSave"]}]}


# --- Updating an installed bundle --------------------------------------------------


APPROVED = REGISTER["claims.yaml"].replace(
    "status: draft\n", "# Approved after review.\nstatus: approved\napproved_by: maintainer\n")
# The change: a save also syncs, and the claims name the new function.
UPDATED = {
    "claims.yaml": REGISTER["claims.yaml"].replace("registerSave]", "registerSave, registerSync]"),
    "model.tla": REGISTER["model.tla"].replace("\\* A toy register", "\\* A toy register that syncs its saves"),
}


@pytest.fixture
def installed(fuzzer_dir) -> Path:
    """The fuzzer's own bundle formal/register/, approved, with a README."""
    root = fuzzer_dir / "formal" / "register"
    for name, text in {**REGISTER, "claims.yaml": APPROVED, "README.md": "# register\n"}.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    return root


def _update(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar, change=None):
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    run = draft_spec(change or _change(), workdir=work, out_dir=out, catalog=CATALOG, fuzzer_dir=fuzzer_dir,
                     tools=SpecTools(fake_fuzzer, str(jar)), runner=runner, update="register")
    return run, json.loads((out / "spec-draft.json").read_text()), out


def test_an_update_writes_its_files_over_the_installed_bundle_and_passes_the_gates(
        tmp_path, fuzzer_dir, installed, fake_fuzzer, jar, no_clone, monkeypatch):
    # The head's register.c, where the clone put it: the change is inside registerSave.
    def clone(repo, dest, sha=None, *, fetch_ref=None):
        (dest / "register.c").parent.mkdir(parents=True, exist_ok=True)
        (dest / "register.c").write_text("void registerSave(void) {\n    sync();\n}\n")
        return True

    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", clone)
    change = _change(diff=_diff("register.c", "@@ -2 +2 @@ void registerSave(void) {\n-    save();\n+    sync();\n"))
    runner = StubRunner(_bundle(UPDATED))
    run, result, out = _update(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar, change)
    assert run.verdict == result["verdict"] == "passed"
    [prompt] = runner.prompts
    assert "valkey-fuzzer/formal/register/" in prompt and "register.c:registerSave" in prompt
    assert "NO-MODEL-CHANGE" in prompt and "status: draft" in prompt
    assert result["update"] == {"protocol": "register", "touched": ["register.c:registerSave"],
                                "changed": ["claims.yaml", "model.tla"]}
    # The gates judged the whole bundle: the update's files and the installed ones it left alone.
    bundle = out / "bundle" / "register"
    assert (bundle / "model.tla").read_text() == UPDATED["model.tla"]
    assert (bundle / "mapping.yaml").read_text() == REGISTER["mapping.yaml"]
    assert (bundle / "README.md").read_text() == "# register\n"
    # The record of the bundle's files holds its bundle paths only, as a batch or a pull request takes them.
    assert set(result["bundle"]["files"]) == set(REGISTER)
    summary = (out / "summary.md").read_text()
    assert "TLA+ model update of formal/register" in summary and "`model.tla`" in summary
    # The protocol's name finds the draft, before the installed bundle.
    assert all(c["spec_path"].endswith("round-0") for c in _log(fake_fuzzer))


def test_an_update_the_model_does_not_need_ends_after_one_session(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar):
    runner = StubRunner(_result("<<<NO-MODEL-CHANGE>>>\nThe change renames a local; Save is unchanged.\n<<<END>>>\n"))
    run, result, out = _update(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert (run.verdict, result["verdict"]) == ("no_model_change", "no_model_change")
    assert result["reason"] == "The change renames a local; Save is unchanged."
    assert result["update"]["changed"] == [] and _calls(fake_fuzzer) == []
    assert "the model needs no change" in (out / "summary.md").read_text()


@pytest.mark.parametrize("files, error", [
    # The installed claims are approved; an update is a draft, so it always brings claims.yaml.
    ({"model.tla": UPDATED["model.tla"]}, "status must be draft"),
    ({"claims.yaml": REGISTER["claims.yaml"]}, "changes nothing but the claims' approval"),
    ({**UPDATED, "claims.yaml": UPDATED["claims.yaml"].replace("[ack-before-save]", "[]")},
     "drops AckedDurable's control ack-before-save"),
    ({**UPDATED, "claims.yaml": UPDATED["claims.yaml"].replace("protocol: register", "protocol: register-two")},
     "keeps the protocol name register"),
])
def test_an_update_that_is_no_draft_or_drops_a_claim_goes_back(files, error, tmp_path, fuzzer_dir, installed,
                                                                fake_fuzzer, jar):
    # The repair answers only with the file it fixes; the rest stay as the failed attempt had them.
    runner = StubRunner(_bundle(files), _bundle({"claims.yaml": UPDATED["claims.yaml"]}))
    if "model.tla" not in files:
        runner.answers[1] = _bundle(UPDATED)
    run, result, _ = _update(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert result["rounds"][0]["failed_gate"] == "draft" and error in result["rounds"][0]["output"]
    assert run.verdict == "passed"
    # The repair saw the whole failed bundle, and was told to send only what it changes.
    assert set(runner.drafts[1]) == {*REGISTER, "README.md"}
    assert "Return the files you change from draft/" in runner.prompts[1]


def test_a_new_draft_never_replaces_an_installed_bundle(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar):
    runner = StubRunner(*[_bundle(REGISTER)] * 3)
    run, result, _ = _draft(runner, tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert run.verdict == "rejected" and "update" not in result
    assert "formal/register is the fuzzer's bundle of register already" in result["rounds"][0]["output"]
    assert "already models register" in runner.prompts[0]


def test_an_update_of_a_bundle_the_fuzzer_does_not_have_is_refused(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    with pytest.raises(ValueError, match="no bundle formal/register/"):
        _update(StubRunner(), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    with pytest.raises(SystemExit):
        spec_mod.main(["draft", "--pr", "1", "--update", "register", "--fuzzer-dir", str(fuzzer_dir),
                       "--fuzzer-bin", fake_fuzzer, "--tla2tools", str(jar), "--dry-run"])


# --- The pull request of an update -------------------------------------------------


@pytest.fixture
def fake_github(monkeypatch):
    """A GitHub repository that records what open_pull_request asks of it."""
    import github

    repo = MagicMock()
    repo.get_pulls.return_value = []
    repo.get_git_ref.side_effect = github.GithubException(404, {"message": "Not Found"}, None)
    repo.get_branch.return_value.commit.sha = "base-sha"
    repo.create_pull.return_value.html_url = "https://github.com/me/valkey-fuzzer/pull/7"
    # Each file the commit writes, as the tree element's fields.
    monkeypatch.setattr(github, "InputGitTreeElement",
                        lambda path, mode, type, content: {"path": path, "mode": mode, "type": type, "content": content})
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh, repo


def _passed_update(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, _, out = _update(StubRunner(_bundle(UPDATED)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    return out


def test_an_update_that_passed_is_opened_as_a_pull_request_once(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar,
                                                                fake_github):
    gh, repo = fake_github
    out = _passed_update(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    url = spec_mod.open_pull_request(gh, out, fuzzer_repo="me/valkey-fuzzer", base="generic-fuzzer")
    assert url == "https://github.com/me/valkey-fuzzer/pull/7"
    gh.get_repo.assert_called_with("me/valkey-fuzzer")
    # One commit on the base's head that writes only the files the update changed.
    elements = repo.create_git_tree.call_args.args[0]
    assert [e["path"] for e in elements] == ["formal/register/claims.yaml", "formal/register/model.tla"]
    assert elements[1] == {"path": "formal/register/model.tla", "mode": "100644", "type": "blob",
                           "content": UPDATED["model.tla"]}
    repo.get_branch.assert_called_with("generic-fuzzer")
    branch = f"spec-update/register-{HEAD[:12]}"
    repo.create_git_ref.assert_called_once_with(f"refs/heads/{branch}", repo.create_git_commit.return_value.sha)
    kwargs = repo.create_pull.call_args.kwargs
    assert (kwargs["base"], kwargs["head"]) == ("generic-fuzzer", branch)
    assert kwargs["title"] == "formal/register: update the model for valkey-io/valkey#123"
    assert "status: approved" in kwargs["body"] and "TLA+ model update of formal/register" in kwargs["body"]
    assert json.loads((out / "spec-draft.json").read_text())["pull_request"] == url

    # A rerun finds the branch's pull request, whatever its state, and changes nothing.
    repo.get_pulls.return_value = [MagicMock(html_url="https://github.com/me/valkey-fuzzer/pull/7")]
    repo.reset_mock(return_value=False, side_effect=False)
    assert spec_mod.open_pull_request(gh, out, fuzzer_repo="me/valkey-fuzzer", base="generic-fuzzer") == url
    repo.get_pulls.assert_called_with(state="all", head=f"me:{branch}")
    repo.create_git_commit.assert_not_called()
    repo.create_pull.assert_not_called()


def test_an_update_is_never_opened_on_a_valkey_io_repository(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar,
                                                           fake_github):
    gh, _ = fake_github
    out = _passed_update(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    with pytest.raises(ValueError, match="fork only"):
        spec_mod.open_pull_request(gh, out, fuzzer_repo="valkey-io/valkey-fuzzer", base="main")
    gh.get_repo.assert_not_called()
    # The command names its fork: there is no default to fall back on.
    with pytest.raises(SystemExit):
        spec_mod.main(["pr", "--out", str(out)])


def test_an_update_the_model_does_not_need_is_no_pull_request(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar,
                                                               fake_github):
    gh, _ = fake_github
    _, _, out = _update(StubRunner(_result("<<<NO-MODEL-CHANGE>>>\nnothing\n<<<END>>>\n")), tmp_path, fuzzer_dir,
                        fake_fuzzer, jar)
    assert spec_mod.open_pull_request(gh, out, fuzzer_repo="me/valkey-fuzzer", base="b") is None
    gh.get_repo.assert_not_called()


def test_a_new_draft_that_passed_is_no_pull_request(tmp_path, fuzzer_dir, fake_fuzzer, jar, fake_github):
    gh, _ = fake_github
    run, _, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert run.verdict == "passed"
    assert spec_mod.open_pull_request(gh, out, fuzzer_repo="me/valkey-fuzzer", base="b") is None
    gh.get_repo.assert_not_called()


def test_an_update_file_changed_after_it_passed_is_not_opened(tmp_path, fuzzer_dir, installed, fake_fuzzer, jar,
                                                              fake_github):
    gh, repo = fake_github
    out = _passed_update(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    (out / "bundle" / "register" / "model.tla").write_text("---- MODULE model ----\n====\n")
    with pytest.raises(ValueError, match="changed since the update passed"):
        spec_mod.open_pull_request(gh, out, fuzzer_repo="me/valkey-fuzzer", base="b")
    repo.create_git_tree.assert_not_called()


# --- Dispatch ---------------------------------------------------------------------


def test_batch_inputs_carry_a_passing_drafts_scenarios(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, _, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    inputs = batch_inputs(out)
    assert inputs is not None
    assert list(json.loads(inputs["scenarios"])) == ["spec-register-ack-before-save"]
    assert inputs["valkey_ref"] == "refs/pull/123/head" and inputs["valkey_sha"] == HEAD


def test_batch_inputs_name_a_translated_scenario_as_the_pr_batch_allows(
    tmp_path, fuzzer_dir, fake_fuzzer, jar,
):
    """spec translate keeps ':' in a scenario ID; the PR batch's names may not hold it."""
    _, result, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    item = result["scenarios"][0]
    path = out / item["file"]
    path.write_text(path.read_text().replace(item["scenario_id"], "spec-register:ack-before-save"))
    item.update(scenario_id="spec-register:ack-before-save", sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    (out / "spec-draft.json").write_text(json.dumps(result))
    scenarios = json.loads(batch_inputs(out)["scenarios"])
    assert list(scenarios) == ["spec-register-ack-before-save"]
    assert "scenario_id: spec-register:ack-before-save" in scenarios["spec-register-ack-before-save"]


def test_scenarios_record_whether_their_experiment_is_installed():
    scenarios = [{"experiment": "valkey.cluster-chaos"}, {"experiment": "toy.register"}, {"experiment": "x.local"}]
    spec_mod._mark_installed(scenarios, {"experiments": [
        {"id": "valkey.cluster-chaos", "plugin": "valkey"}, {"id": "x.local", "plugin": None, "directory": "/d"},
    ]})
    assert [s["installed"] for s in scenarios] == [True, False, False]


def _own_experiment(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    """A passing draft whose scenario runs the bundle's own toy.register, which is not installed."""
    _, result, out = _draft(StubRunner(_bundle(REGISTER)), tmp_path, fuzzer_dir, fake_fuzzer, jar)
    assert [(s["experiment"], s["installed"]) for s in result["scenarios"]] == [("toy.register", True)]
    assert result["bundle"]["protocol"] == "register"
    assert set(result["bundle"]["files"]) == set(REGISTER)
    # The stand-in fuzzer lists toy.register as installed; here the bundle brings it.
    own = out / "bundle" / "register" / "experiments" / "toy.register.yaml"
    own.parent.mkdir(parents=True, exist_ok=True)
    own.write_text(TOY_EXPERIMENT)
    result["bundle"]["files"]["experiments/toy.register.yaml"] = hashlib.sha256(own.read_bytes()).hexdigest()
    result["scenarios"][0]["installed"] = False
    (out / "spec-draft.json").write_text(json.dumps(result))
    return result, out


TOY_EXPERIMENT = "id: toy.register\ncomponents: []\n"


def test_a_draft_running_its_own_experiment_is_sent_with_its_bundle(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    result, out = _own_experiment(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    inputs = batch_inputs(out)
    assert json.loads(inputs["spec_bundles"]) == {
        "register": {**REGISTER, "experiments/toy.register.yaml": TOY_EXPERIMENT},
    }
    assert list(json.loads(inputs["scenarios"])) == ["spec-register-ack-before-save"]
    # An installed experiment's scenarios need no bundle.
    result["scenarios"][0]["installed"] = True
    (out / "spec-draft.json").write_text(json.dumps(result))
    assert "spec_bundles" not in batch_inputs(out)


def test_a_bundle_changed_after_the_draft_passed_is_not_sent(tmp_path, fuzzer_dir, fake_fuzzer, jar):
    _, out = _own_experiment(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    model = out / "bundle" / "register" / "model.tla"
    model.write_text(model.read_text() + "\n")
    with pytest.raises(ValueError, match="bundle/register/model.tla changed since the draft passed"):
        batch_inputs(out)


@pytest.mark.parametrize("bundle,message", [
    (None, "records no bundle"),
    ({"protocol": "Register", "files": {"claims.yaml": "x"}}, "records no bundle"),
    ({"protocol": "register", "files": {}}, "records no bundle"),
    ({"protocol": "register", "files": {"../../spec-draft.json": "x"}}, "bundle file outside"),
    ({"protocol": "register", "files": {"notes.md": "x"}}, "bundle file outside"),
])
def test_a_bundle_record_that_cannot_be_trusted_is_refused(bundle, message, tmp_path, fuzzer_dir, fake_fuzzer, jar):
    result, out = _own_experiment(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    result["bundle"] = bundle
    (out / "spec-draft.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match=message):
        batch_inputs(out)


def test_a_scenario_of_an_experiment_the_batch_cannot_load_is_never_dispatched(
    tmp_path, fuzzer_dir, fake_fuzzer, jar, monkeypatch,
):
    """Neither installed nor in the bundle, such as one from --experiment-dir."""
    result, out = _own_experiment(tmp_path, fuzzer_dir, fake_fuzzer, jar)
    result["scenarios"][0]["experiment"] = "local.only"
    (out / "spec-draft.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match=r"loads only installed experiments and those of the bundles it is sent.*"
                                         r"spec-register-ack-before-save \(local.only\)"):
        batch_inputs(out)
    dispatched = MagicMock()
    monkeypatch.setattr(spec_mod, "dispatch_batch", dispatched)
    monkeypatch.setenv("TARGET_TOKEN", "t")
    assert spec_mod.main(["dispatch", "--out", str(out)]) == 1
    dispatched.assert_not_called()


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

    # Imported before sockets are refused: importing it builds a socket class.
    import github
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(propose_mod, "urlopen", refuse)
    monkeypatch.setattr(propose_mod, "shallow_clone_at_sha", refuse)
    monkeypatch.setattr(spec_mod, "run_agent", refuse)
    monkeypatch.setattr(spec_mod, "fetch_change", refuse)
    monkeypatch.setattr(spec_mod, "dispatch_batch", refuse)
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
    calls = _log(fake_fuzzer)
    assert [c["argv"][:3] for c in calls] == [["list", "experiments", "--format"], ["list", "components", "--format"],
                                             ["spec", "check", "./register"]]
    # Copied under its protocol name with the spec path set, so its experiment gate passes.
    assert calls[2]["spec_path"] is not None


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
    # The register brings its own experiment; nothing else is loaded.
    tools = SpecTools(_E2E["VALKEY_FUZZER_BIN"], _E2E["TLA2TOOLS_JAR"])
    tools.preflight(fuzzer_dir)
    out = tmp_path / "out"
    work = tmp_path / "work"
    work.mkdir()
    run = draft_spec(_change(), workdir=work, out_dir=out, catalog=propose_mod.load_catalog(_E2E["VALKEY_FUZZER_BIN"]),
                     fuzzer_dir=fuzzer_dir, tools=tools, runner=runner)
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
    assert assurance["gates"]["experiment"]["controls"]["ack-before-save"]["faults"] == {"Crash": "crash"}
    assert (out / "bundle" / "register" / "experiments" / "toy.register.yaml").is_file()
    ids = sorted(s["scenario_id"] for s in result["scenarios"])
    assert "spec-register-ack-before-save" in ids and len(ids) >= 2
    assert json.loads((out / "validate.json").read_text())["ok"] is True
    assert result["counterexamples"][0]["actions"][-1] == "Crash"
    # Its scenarios run the register's own experiment, so the bundle goes with them.
    assert {(s["experiment"], s["installed"]) for s in result["scenarios"]} == {("toy.register", False)}
    inputs = batch_inputs(out)
    bundles = json.loads(inputs["spec_bundles"])
    assert list(bundles) == ["register"] and "experiments/toy.register.yaml" in bundles["register"]
    # The real fuzzer-pr-batch.yml takes them, bundle checks included.
    from tests.test_fuzzer_pr_batch import _workflow_check

    checked = _workflow_check(inputs, tmp_path / "pr-batch")
    assert checked.returncode == 0, checked.stderr
    assert (tmp_path / "pr-batch" / "spec" / "register" / "experiments" / "toy.register.yaml").is_file()


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


@pytest.mark.skipif(not _E2E["VALKEY_FUZZER_DIR"], reason="needs VALKEY_FUZZER_DIR, a valkey-fuzzer checkout")
@pytest.mark.parametrize("prompt", [spec_mod._DRAFT_PROMPT, spec_mod._REPAIR_PROMPT, spec_mod._RULES,
                                    spec_mod._UPDATE_PROMPT])
def test_the_prompts_name_only_what_the_fuzzer_has(prompt):
    # Claude reads these in its copy of the fuzzer; a renamed one leaves it looking for nothing.
    fuzzer_dir = Path(_E2E["VALKEY_FUZZER_DIR"])
    named = set(re.findall(r"\b((?:docs|formal|tests/fixtures|valkey_fuzzer_experiments)/[\w./<>-]*\w/?)", prompt))
    assert named
    for path in named:
        path = path.split("<", 1)[0]
        assert (fuzzer_dir / path).exists(), f"the prompt names {path}, which valkey-fuzzer does not have"
    if '"Onboarding a protocol with data only"' in prompt:
        assert "## Onboarding a protocol with data only" in (fuzzer_dir / "docs" / "spec-bridge.md").read_text()
