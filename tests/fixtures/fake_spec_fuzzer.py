"""A stand-in for ``valkey-fuzzer`` in the spec draft tests.

It answers ``list``, ``spec check``, ``spec translate`` and ``validate`` the
way the real fuzzer does, in the same report shapes, without Java or TLC:

- ``spec check`` fails the ``parsed`` gate when model.tla's parentheses don't
  balance, and the ``negative_controls`` gate for a property defined as
  ``TRUE`` (a claim that cannot fail);
- a model containing ``HARNESS-BROKEN`` makes it exit 2, like a missing jar;
- a model containing ``SLOW`` makes it sleep, to test the timeout.

Every call is appended to ``calls.log`` next to this file's copy.
"""

import json
import re
import sys
import time
from pathlib import Path

import yaml

CATALOG = {
    "experiments": [{"id": "toy.register", "deployment_modes": ["standalone"]}],
    "components": [{"component_id": "toy.writer", "role": "workload"}],
}


def _arg(args, name):
    return args[args.index(name) + 1] if name in args else None


def _claims(bundle):
    return yaml.safe_load((bundle / "claims.yaml").read_text())


def _check(args):
    bundle = Path(args[0])
    output = Path(_arg(args, "--output"))
    model = (bundle / "model.tla").read_text()
    if "HARNESS-BROKEN" in model:
        print("Error: java not found on PATH", file=sys.stderr)
        return 2
    if "SLOW" in model:
        time.sleep(30)
    claims = _claims(bundle)
    gates = {"bundle": {"status": "pass"}}
    report = {"kind": "spec-assurance", "bundle": str(bundle), "gates": gates,
              "claims": {"status": claims.get("status")}, "protocol": claims.get("protocol")}

    def finish(status):
        report["status"] = status
        output.write_text(json.dumps(report))
        return 0 if status == "pass" else 1

    if model.count("(") != model.count(")"):
        gates["parsed"] = {"status": "fail", "modules": {"model.tla": {"status": "fail", "output": [
            "***Parse Error***", "Encountered \"Beginning of definition\" at line 40, column 1",
            "Could not parse module model from file model.tla"]}}}
        return finish("fail")
    gates["parsed"] = {"status": "pass", "modules": {"model.tla": {"status": "pass"}}}
    gates["claims_checked"] = {"status": "pass", "problems": []}
    gates["model_checked"] = {"status": "pass", "configs": {"model.cfg": {"status": "pass", "exit_code": 0}}}
    gates["model_coverage"] = {"status": "pass", "actions": {}, "never_fired": []}
    controls = {}
    for prop in claims["properties"]:
        vacuous = re.search(rf"^{prop['name']}\s*==\s*TRUE\s*$", model, re.MULTILINE)
        for control in prop["negative_controls"]:
            entry = {"status": "pass", "violates": prop["name"], "config": f"negative-controls/{control}.cfg",
                     "exit_code": 12, "violation": "invariant", "violated": prop["name"], "trace_length": 4}
            if vacuous:
                entry.update(status="fail", exit_code=0, violation=None, violated=None, trace_length=0,
                             error=f"expected TLC to report {prop['name']} violated; it found no error: the claim "
                                   "cannot fail as configured (vacuous) or the control is broken")
            controls[control] = entry
    failed = any(entry["status"] != "pass" for entry in controls.values())
    gates["negative_controls"] = {"status": "fail" if failed else "pass", "controls": controls}
    gates["mapping_complete"] = {"status": "pass", "unmapped_actions": [], "unknown_actions": []}
    gates["approved_claim"] = {"status": "pass", "claims_status": claims.get("status")}
    return finish("fail" if failed else "pass")


def _translate(args):
    bundle = Path(args[0])
    control = _arg(args, "--control")
    out = Path(_arg(args, "--out"))
    out.mkdir(parents=True, exist_ok=True)
    claims = _claims(bundle)
    scenario_id = f"spec-{claims['protocol']}-{control}"
    experiment = "missing.experiment" if "missing.experiment" in (bundle / "mapping.yaml").read_text() \
        else "toy.register"
    scenario = {"scenario_id": scenario_id, "seed": 1,
                "experiment": {"id": experiment, "inputs": {"sync": False, "crash_after_ack": 1}},
                "deployment": {"mode": "standalone"}}
    (out / f"{scenario_id}.yaml").write_text(
        f"# Translated by `valkey-fuzzer spec translate` from the {control} counterexample of bundle.\n"
        + yaml.safe_dump(scenario, sort_keys=False))
    (out / "translation.json").write_text(json.dumps({
        "kind": "spec-translation", "protocol": claims["protocol"],
        "counterexample": {"name": control, "violated": "AckedDurable", "length": 6,
                           "actions": ["Init", "Write", "Ack", "Crash"], "constants": {"SyncBeforeAck": "FALSE"}},
        "alignment": [{"fault": "Crash", "trigger": "acked", "trigger_event": "toy.ack", "occurrence": 1,
                       "occurrence_input": "crash_after_ack", "between": []}],
        "scenarios": [scenario],
    }))
    return 0


def _validate(args):
    files = [a for a in args if a.endswith(".yaml")]
    results = []
    for name in files:
        doc = yaml.safe_load(Path(name).read_text())
        experiment = doc["experiment"]["id"]
        if experiment == "toy.register":
            results.append({"file": name, "ok": True, "errors": [], "experiment": experiment,
                            "scenario_id": doc["scenario_id"], "converted": False})
        else:
            results.append({"file": name, "ok": False, "errors": [
                {"path": "experiment.id", "message": f"experiment {experiment!r} is not registered"}]})
    ok = all(item["ok"] for item in results)
    print("Planning a standalone deployment with 0 replica(s)", file=sys.stderr)
    print(json.dumps({"ok": ok, "files": results}))
    return 0 if ok else 2


def main(argv):
    with open(Path(__file__).with_name("calls.log"), "a") as log:
        log.write(json.dumps(argv) + "\n")
    if argv[:1] == ["list"]:
        print(json.dumps(CATALOG[argv[1]]))
        return 0
    if argv[:2] == ["spec", "check"]:
        return _check(argv[2:])
    if argv[:2] == ["spec", "translate"]:
        return _translate(argv[2:])
    if argv[:1] == ["validate"]:
        return _validate(argv[1:])
    print(f"unknown command {argv}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
