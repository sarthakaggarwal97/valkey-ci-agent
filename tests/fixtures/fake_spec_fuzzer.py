"""A stand-in for ``valkey-fuzzer`` in the spec draft tests.

It answers ``list``, ``spec check``, ``spec translate`` and ``validate`` the
way the real fuzzer does, in the same report shapes, without Java or TLC:

- ``spec check`` fails the ``parsed`` gate when model.tla's parentheses don't
  balance or it extends IOUtils, the ``negative_controls`` gate for a
  property defined as ``TRUE`` (a claim that cannot fail), and the
  ``experiment`` gate for a bundle with its own experiments/ when
  VALKEY_FUZZER_SPEC_PATH does not find it; a control whose name starts with
  ``nofault`` is ``no-fault``, and ``spec translate`` exits 1 on it;
- a model containing ``HARNESS-BROKEN`` makes it exit 2, like a missing jar;
- a model containing ``SLOW`` is reported as cut short by ``--timeout``;
- a bundle is a directory when the argument holds a ``/`` (``./register``),
  and otherwise a name looked up only on VALKEY_FUZZER_SPEC_PATH, as the
  real fuzzer does: never in the working directory;
- ``validate`` knows toy.register plus the experiments in its
  ``--experiment-dir`` directories, and refuses one whose ``spec: <name>``
  VALKEY_FUZZER_SPEC_PATH does not find.

Every call, with its VALKEY_FUZZER_SPEC_PATH, is appended to ``calls.log``
next to this file's copy.
"""

import json
import os
import re
import sys
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


def _find(reference):
    """The bundle directory a spec command's argument names, or None."""
    if "/" in reference:
        return Path(reference) if Path(reference).is_dir() else None
    for directory in filter(None, os.environ.get("VALKEY_FUZZER_SPEC_PATH", "").split(os.pathsep)):
        if (Path(directory) / reference).is_dir():
            return Path(directory) / reference
    return None


def _check(args):
    bundle = _find(args[0])
    output = Path(_arg(args, "--output"))
    if bundle is None:
        error = f"no spec bundle named {args[0]!r} (write ./{args[0]} for a directory here)"
        output.write_text(json.dumps({"kind": "spec-assurance", "bundle": args[0], "status": "error",
                                      "gates": {"bundle": {"status": "error", "error": error}}}))
        print(f"[ERROR] bundle: {error}")
        return 2
    model = (bundle / "model.tla").read_text()
    if "HARNESS-BROKEN" in model:
        print("Error: java not found on PATH", file=sys.stderr)
        return 2
    claims = _claims(bundle)
    gates = {"bundle": {"status": "pass"}}
    report = {"kind": "spec-assurance", "bundle": str(bundle), "gates": gates,
              "claims": {"status": claims.get("status")}, "protocol": claims.get("protocol")}

    def finish(status):
        report["status"] = status
        output.write_text(json.dumps(report))
        return 0 if status == "pass" else 1

    if "IOUtils" in model:
        gates["parsed"] = {"status": "fail", "modules": {"model.tla": {"status": "fail", "error": (
            "uses modules IOUtils; a bundle's modules may use only the standard modules (Bags, FiniteSets, "
            "Integers, Naturals, RealTime, Reals, Sequences, TLC) and the bundle's own")}}}
        return finish("fail")
    if model.count("(") != model.count(")"):
        gates["parsed"] = {"status": "fail", "modules": {"model.tla": {"status": "fail", "output": [
            "***Parse Error***", "Encountered \"Beginning of definition\" at line 40, column 1",
            "Could not parse module model from file model.tla"]}}}
        return finish("fail")
    gates["parsed"] = {"status": "pass", "modules": {"model.tla": {"status": "pass"}}}
    gates["claims_checked"] = {"status": "pass", "problems": []}
    if "SLOW" in model:
        error = f"TLC was stopped after {_arg(args, '--timeout')}s: the time limit ran out before it finished"
        gates["model_checked"] = {"status": "fail", "configs": {"model.cfg": {
            "status": "fail", "exit_code": -9, "timed_out": True, "errors": [error], "error": error}}}
        report["error"] = f"spec check stopped: the {_arg(args, '--timeout')}s time limit ran out"
        return finish("fail")
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
    if failed:
        return finish("fail")
    experiment = {"status": "pass", "experiment": "toy.register", "controls": {}}
    own = bundle / "experiments"
    search = os.environ.get("VALKEY_FUZZER_SPEC_PATH", "")
    if own.is_dir() and not (search and (Path(search) / claims["protocol"]).resolve() == bundle.resolve()):
        experiment.update(status="fail", error=(
            f"the protocol name {claims['protocol']!r} does not resolve to this bundle; put the directory "
            "holding it on $VALKEY_FUZZER_SPEC_PATH"))
    else:
        for control in controls:
            experiment["controls"][control] = (
                {"status": "no-fault", "reason": "the counterexample has no mapped fault action"}
                if control.startswith("nofault") else {"status": "pass", "faults": {"Crash": "crash"}})
        if not any(e["status"] == "pass" for e in experiment["controls"].values()):
            experiment["status"] = "fail"
    gates["experiment"] = experiment
    if experiment["status"] != "pass":
        return finish("fail")
    gates["mapping_complete"] = {"status": "pass", "unmapped_actions": [], "unknown_actions": []}
    gates["approved_claim"] = {"status": "pass", "claims_status": claims.get("status")}
    return finish("fail" if failed else "pass")


def _translate(args):
    bundle = _find(args[0])
    if bundle is None:
        print(f"Error: no spec bundle named {args[0]!r}", file=sys.stderr)
        return 2
    control = _arg(args, "--control")
    if control.startswith("nofault"):
        print(f"Error: the {control} counterexample has no mapped fault action", file=sys.stderr)
        return 1
    out = Path(_arg(args, "--out"))
    out.mkdir(parents=True, exist_ok=True)
    claims = _claims(bundle)
    scenario_id = f"spec-{claims['protocol']}-{control}"
    match = re.search(r"^  experiment: (\S+)", (bundle / "mapping.yaml").read_text(), re.MULTILINE)
    experiment = match.group(1) if match else "toy.register"
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
    known = {"toy.register"}
    unfound = {}
    for index, arg in enumerate(args):
        if arg == "--experiment-dir":
            for path in Path(args[index + 1]).glob("*.yaml"):
                known.add(path.stem)
                for name in re.findall(r"spec: ([a-z0-9-]+)", path.read_text()):
                    if _find(name) is None:
                        unfound[path.stem] = name
    files = [a for i, a in enumerate(args) if a.endswith(".yaml") and (i == 0 or args[i - 1] != "--experiment-dir")]
    results = []
    for name in files:
        doc = yaml.safe_load(Path(name).read_text())
        experiment = doc["experiment"]["id"]
        if experiment in unfound:
            results.append({"file": name, "ok": False, "errors": [
                {"path": "components.reload-log.config", "message": (
                    f"component 'valkey.observer.log-trigger' rejected 'reload-log': spec "
                    f"'{unfound[experiment]}': no spec bundle named '{unfound[experiment]}'")}]})
        elif experiment in known:
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
        log.write(json.dumps({"argv": argv, "spec_path": os.environ.get("VALKEY_FUZZER_SPEC_PATH")}) + "\n")
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
