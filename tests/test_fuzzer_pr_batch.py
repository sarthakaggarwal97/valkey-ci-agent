"""Tests for the PR batch workflow's inputs and for reading the batch it uploads."""
from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scripts.fuzzer.pr_batch as pr_batch_mod
from scripts.fuzzer.batch import read_batch, read_batch_dir
from scripts.fuzzer.pr_batch import (
    BATCH_INPUTS,
    MODULE_FILES,
    OPTIONAL_INPUTS,
    RESULTS_FILE,
    ModuleBuild,
    batch_dispatch_inputs,
    batch_names,
    bundle_path_allowed,
    check_batch_inputs,
    module_repository,
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
WITH_INSTANCE_BINARIES = SCENARIO.replace("replicas_per_primary: 1}", "replicas_per_primary: 1, instance_binaries: {}}")
WITH_ARTIFACTS = SCENARIO + "artifacts: {parent: {use: valkey.artifact.local-binary, with: {path: /v}}}\n"
# A scenario that reaches the parent build through a reference, and one that names an artifact no batch declares.
REFERENCES_PARENT = SCENARIO.replace("replicas_per_primary: 1}",
                                     "replicas_per_primary: 1, server_config: {x: {$ref: artifacts.parent.binary}}}")
REFERENCES_OTHER = REFERENCES_PARENT.replace("artifacts.parent.binary", "artifacts.baseline.binary")
# Scenarios that load the module builds the batch declares when it is given a module.
REFERENCES_MODULE = REFERENCES_PARENT.replace("artifacts.parent.binary", "artifacts.module.path")
REFERENCES_MODULE_PARENT = REFERENCES_PARENT.replace("artifacts.parent.binary", "artifacts.module_parent.path")
# A flow-style top-level mapping: the batch cannot append the parent block to it.
FLOW_SCENARIO = "{scenario_id: ai-0123456789ab, seed: 7, experiment: {id: valkey.cluster-chaos}, deployment: {mode: cluster}}\n"
PARENT = "b" * 40
MODULE_SHA, MODULE_PARENT = "c" * 40, "d" * 40
JSON_MODULE = {"module_repository": "valkey-io/valkey-json", "module_sha": MODULE_SHA, "module_parent_sha": MODULE_PARENT}
TEST_MODULES = {"module_repository": "valkey-io/valkey", "module_sha": HEAD, "module_parent_sha": PARENT,
                "module_file": "tests/modules/datatype.so", "valkey_parent_sha": PARENT}
# A proposed experiment the fuzzer's catalog loads: the installed fragment brings its components.
EXPERIMENT = "extends: [valkey.base]\nid: proposed.probe\ndeployment: {modes: [standalone]}\ncomponents: []\n"
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


def _experiments(value, **changes) -> dict[str, str]:
    return _inputs(experiments=value if isinstance(value, str) else json.dumps(value), **changes)


def test_batch_inputs_are_the_ones_the_workflow_takes():
    assert BATCH_INPUTS == ("scenarios", "valkey_repository", "valkey_ref", "valkey_sha")
    assert OPTIONAL_INPUTS == ("spec_bundles", "random_seeds", "request_id", "exclude_experiments",
                               "valkey_parent_sha", "build", "experiments", "module_repository", "module_sha",
                               "module_parent_sha", "module_file")
    check_batch_inputs(_inputs())
    check_batch_inputs(_bundles({"toy": BUNDLE}))
    check_batch_inputs(_bundles(""))
    check_batch_inputs(_experiments({"proposed.probe.yaml": EXPERIMENT}, valkey_parent_sha=PARENT, build="asan"))
    check_batch_inputs(_experiments("", valkey_parent_sha="", build="default"))
    check_batch_inputs(_scenarios({"x": REFERENCES_PARENT}) | {"valkey_parent_sha": PARENT})
    change = {"repo": "valkey-io/valkey", "pr_number": 123, "head_sha": HEAD}
    inputs = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO})
    assert inputs == _inputs(scenarios=json.dumps({"ai-0123456789ab": SCENARIO}, separators=(",", ":")))
    with_bundle = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO}, {"toy": BUNDLE})
    assert json.loads(with_bundle["spec_bundles"]) == {"toy": BUNDLE}
    for_range = batch_dispatch_inputs({"repo": "valkey-io/valkey", "pr_number": None, "head_sha": HEAD},
                                      {"ai-0123456789ab": SCENARIO})
    assert for_range["valkey_ref"] == HEAD
    baseline = batch_dispatch_inputs({"repo": "valkey-io/valkey", "pr_number": None, "head_sha": HEAD}, {},
                                     random_seeds=4, request_id="replay-007-aaaaaaaaa")
    assert (baseline["scenarios"], baseline["random_seeds"], baseline["request_id"]) == ("{}", "4", "replay-007-aaaaaaaaa")
    compared = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO}, parent_sha=PARENT, build="ubsan",
                                     experiments={"proposed.probe.yaml": EXPERIMENT})
    assert (compared["valkey_parent_sha"], compared["build"]) == (PARENT, "ubsan")
    assert json.loads(compared["experiments"]) == {"proposed.probe.yaml": EXPERIMENT}
    # The defaults are left out, as the workflow would fill them in.
    plain = batch_dispatch_inputs(change, {"ai-0123456789ab": SCENARIO}, parent_sha=None, build="default",
                                  experiments={})
    assert not {"valkey_parent_sha", "build", "experiments", "module_repository"} & set(plain)


def test_a_module_is_sent_as_its_four_inputs():
    pinned = {"repo": "valkey-io/valkey", "pr_number": None, "head_sha": HEAD}
    loads = {"x": REFERENCES_MODULE, "y": REFERENCES_MODULE_PARENT}
    inputs = batch_dispatch_inputs(pinned, loads, module=ModuleBuild("valkey-io/valkey-json", MODULE_SHA, MODULE_PARENT))
    assert {k: inputs[k] for k in JSON_MODULE} == JSON_MODULE
    # A module change needs no Valkey parent: one Valkey build, two module builds; valkey-json's file is its own.
    assert not {"valkey_parent_sha", "module_file"} & set(inputs)
    assert (inputs["valkey_sha"], inputs["valkey_ref"]) == (HEAD, HEAD)
    core = batch_dispatch_inputs({"repo": "valkey-io/valkey", "pr_number": 9, "head_sha": HEAD}, loads,
                                 parent_sha=PARENT,
                                 module=ModuleBuild("valkey-io/valkey", HEAD, PARENT, "tests/modules/datatype.so"))
    assert {k: core[k] for k in TEST_MODULES} == TEST_MODULES
    # One module build, without its parent: artifacts.module only.
    only = batch_dispatch_inputs(pinned, {"x": REFERENCES_MODULE}, module=ModuleBuild("o/valkey-json", MODULE_SHA))
    assert "module_parent_sha" not in only
    with pytest.raises(ValueError, match="references artifacts.module_parent, which the batch does not declare"):
        batch_dispatch_inputs(pinned, loads, module=ModuleBuild("o/valkey-json", MODULE_SHA))


def test_module_repositories_are_the_module_table_and_the_batch_builds_some():
    assert MODULE_FILES == {"valkey-json": "build/src/libjson.so", "valkey": None}
    assert module_repository("valkey-io/valkey-json") and module_repository("someone/valkey-json")
    assert module_repository("valkey-io/valkey-search") and module_repository("valkey-io/valkey-bloom")
    assert not module_repository("valkey-io/valkey") and not module_repository("valkey-io/other")


def test_valkey_search_build_carries_the_741_declaration_fix(tmp_path, monkeypatch):
    """valkey-search #621 (9ea236a) dropped schema_manager's declaration but kept its uses, so no commit until
    #741 (b0e8a26) restored it compiles: the recipe applies that one-line fix first, and only where it is missing."""
    broken = "  auto status = [&]() -> absl::Status {\n    vmsdk::ArgsIterator itr{argv + 1, argc - 1};\n"
    fixed = broken.replace("absl::Status {\n", "absl::Status {\n    auto &schema_manager = SchemaManager::Instance();\n")
    patch = textwrap.dedent("""\
        diff --git a/src/commands/commands.cc b/src/commands/commands.cc
        --- a/src/commands/commands.cc
        +++ b/src/commands/commands.cc
        @@ -1,2 +1,3 @@
           auto status = [&]() -> absl::Status {
        +    auto &schema_manager = SchemaManager::Instance();
             vmsdk::ArgsIterator itr{argv + 1, argc - 1};
    """)
    # A git that answers the recipe's fetch and diff of #741 without the network, and delegates the rest.
    stub = tmp_path / "bin" / "git"
    stub.parent.mkdir()
    stub.write_text(f'#!/bin/bash\ncase "$1" in\nfetch) exit 0 ;;\ndiff) cat {tmp_path}/741.patch ;;\n'
                    f'*) exec {shutil.which("git")} "$@" ;;\nesac\n')
    stub.chmod(0o755)
    (tmp_path / "741.patch").write_text(patch)
    monkeypatch.setenv("PATH", f"{stub.parent}:{os.environ['PATH']}")

    def build(source: str) -> str:
        src = tmp_path / "src"
        shutil.rmtree(src, ignore_errors=True)
        (src / "src" / "commands").mkdir(parents=True)
        (src / "src" / "commands" / "commands.cc").write_text(source + "        schema_manager.GetIndexSchema(\n")
        (src / "build.sh").write_text("#!/bin/bash\nexit 0\n")
        (src / "build.sh").chmod(0o755)
        subprocess.run(["bash", "-euo", "pipefail", "-c", pr_batch_mod.MODULES["valkey-search"].build],
                       cwd=src, check=True, capture_output=True, text=True)
        return (src / "src" / "commands" / "commands.cc").read_text()

    assert "auto &schema_manager" in build(broken)
    assert build(fixed).count("auto &schema_manager") == 1  # already declared: the patch is not applied twice


def test_valkey_search_build_defines_ninja_tool_for_the_483_window(tmp_path):
    """valkey-search #483 (19977d4) set BUILD_TOOL from the undefined NINJA_TOOL for the Ninja default, so no
    commit until #494 (cdfe0c4) ran the build (it left no libsearch.so): the recipe exports NINJA_TOOL so a
    build.sh reading it finds ninja."""
    src = tmp_path / "src"
    src.mkdir()
    # A stand-in for the broken window's build.sh: the build tool is whatever NINJA_TOOL holds.
    (src / "build.sh").write_text(
        '#!/bin/bash\nif [[ "${CMAKE_GENERATOR:-Ninja}" == "Ninja" ]]; then BUILD_TOOL="${NINJA_TOOL}"; fi\n'
        'echo "${BUILD_TOOL}" > tool\n'
    )
    (src / "build.sh").chmod(0o755)
    subprocess.run(["bash", "-euo", "pipefail", "-c", pr_batch_mod.MODULES["valkey-search"].build],
                   cwd=src, check=True, capture_output=True, text=True)
    assert (src / "tool").read_text().strip() == "ninja"


def test_valkey_search_build_retries_once_after_the_pre_172_grpc_header_race(tmp_path):
    """Before valkey-search #172 (0730baa), nothing ordered the compiles after protoc's gRPC output: ninja could
    stop on a missing coordinator.grpc.pb.h that the same build generated. The recipe retries once when that
    header landed, and lets any other failure stand."""
    src = tmp_path / "src"
    src.mkdir()
    # A stand-in for the racy build.sh: the first run fails after generating the header; the second succeeds.
    (src / "build.sh").write_text(
        '#!/bin/bash\nif [ ! -f .build-release/src/coordinator/coordinator.grpc.pb.h ]; then\n'
        '  mkdir -p .build-release/src/coordinator && touch .build-release/src/coordinator/coordinator.grpc.pb.h\n'
        '  exit 1\nfi\necho "$@" > built\n'
    )
    (src / "build.sh").chmod(0o755)
    subprocess.run(["bash", "-euo", "pipefail", "-c", pr_batch_mod.MODULES["valkey-search"].build],
                   cwd=src, check=True, capture_output=True, text=True)
    assert (src / "built").exists()

    # A failure that leaves no gRPC header is not the race: it stands, with no retry.
    shutil.rmtree(src / ".build-release")
    (src / "build.sh").write_text('#!/bin/bash\necho attempt >> attempts\nexit 1\n')
    done = subprocess.run(["bash", "-euo", "pipefail", "-c", pr_batch_mod.MODULES["valkey-search"].build],
                          cwd=src, capture_output=True, text=True)
    assert done.returncode != 0
    assert (src / "attempts").read_text() == "attempt\n"


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
    (_scenarios({}), "scenarios is empty and random_seeds is 0: there is nothing to run"),
    (_scenarios([SCENARIO]), "must be a JSON object"),
    (_inputs(random_seeds="5"), "random_seeds must be 0 to 4"),
    (_inputs(random_seeds="-1"), "random_seeds must be 0 to 4"),
    (_inputs(request_id="a b"), "request_id must be at most 100 letters"),
    (_inputs(random_seeds="2", exclude_experiments="x;rm"), "exclude_experiments must be"),
    (_inputs(exclude_experiments="valkey.replication-chaos"), "which random_seeds 0 skips"),
    (_scenarios({"../x": SCENARIO}), "scenario name '../x' must be"),
    (_scenarios({"a..b": SCENARIO}), "scenario name 'a..b' must be"),
    (_scenarios({"-x": SCENARIO}), "scenario name '-x' must be"),
    (_scenarios({"a b": SCENARIO}), "scenario name 'a b' must be"),
    (_scenarios({"a" * 101: SCENARIO}), "must be letters, digits"),
    (_scenarios({"x": {"seed": 1}}), "scenario x: the value must be YAML text"),
    (_scenarios({"x": "a: [b"}), "scenario x is not YAML"),
    (_scenarios({"x": "- a"}), "scenario x must be a mapping"),
    (_scenarios({"x": WITH_BINARY}), "sets deployment.valkey_binary; the batch runs valkey_sha's build"),
    (_scenarios({"x": WITH_INSTANCE_BINARIES}), "sets deployment.instance_binaries; the batch runs valkey_sha's build"),
    (_scenarios({"x": WITH_ARTIFACTS}),
     "scenario x declares artifacts; the batch declares them (artifacts.parent, artifacts.module, artifacts.module_parent)"),
    (_scenarios({"x": REFERENCES_PARENT}),
     "scenario x references artifacts.parent, which the batch does not declare (no valkey_parent_sha)"),
    (_scenarios({"x": REFERENCES_OTHER}) | {"valkey_parent_sha": PARENT},
     "scenario x references artifacts.baseline, which the batch does not declare"),
    (_scenarios({"x": FLOW_SCENARIO}) | {"valkey_parent_sha": PARENT},
     "scenario x: appending the artifacts block does not add exactly that key"),
    (_scenarios({"x": SCENARIO.rstrip("\n")}) | {"valkey_parent_sha": PARENT},
     "appending the artifacts block does not add exactly that key"),
    (_scenarios({"x": FLOW_SCENARIO}) | JSON_MODULE, "scenario x: appending the artifacts block does not add"),
    (_scenarios({"x": REFERENCES_MODULE}),
     "scenario x references artifacts.module, which the batch does not declare (no module_repository)"),
    (_scenarios({"x": REFERENCES_MODULE_PARENT}) | {**JSON_MODULE, "module_parent_sha": ""},
     "scenario x references artifacts.module_parent, which the batch does not declare (no module_parent_sha)"),
    (_scenarios({"x": REFERENCES_PARENT}) | JSON_MODULE,
     "scenario x references artifacts.parent, which the batch does not declare (no valkey_parent_sha)"),
    (_scenarios({"x": REFERENCES_OTHER}),
     "scenario x references artifacts.baseline, which the batch does not declare"),
    (_inputs(module_sha=MODULE_SHA), "module_sha, module_parent_sha, and module_file need module_repository"),
    (_inputs(module_file="x.so"), "module_sha, module_parent_sha, and module_file need module_repository"),
    (_inputs(**{**JSON_MODULE, "module_repository": "valkey-json"}), "module_repository must be empty or OWNER/NAME"),
    (_inputs(**{**JSON_MODULE, "module_repository": "valkey-io/valkey-search"}),
     "module_repository valkey-io/valkey-search is not a module the batch builds; it builds valkey-json, valkey"),
    (_inputs(**{**JSON_MODULE, "module_sha": "C" * 40}), "module_sha must be a full 40-character lowercase commit SHA"),
    (_inputs(**{**JSON_MODULE, "module_sha": ""}), "module_sha must be a full 40-character lowercase commit SHA"),
    (_inputs(**{**JSON_MODULE, "module_parent_sha": "d" * 12}),
     "module_parent_sha must be empty or a full 40-character lowercase commit SHA"),
    (_inputs(**{**JSON_MODULE, "module_parent_sha": MODULE_SHA}),
     "module_parent_sha is module_sha: a comparison with the same build shows nothing"),
    (_inputs(**{**TEST_MODULES, "module_file": ""}),
     "valkey builds many modules: module_file must name one, such as tests/modules/datatype.so"),
    (_inputs(**{**TEST_MODULES, "module_file": "tests/modules/../x.so"}),
     "module_file must be a relative path to a .so in the module's checkout"),
    (_inputs(**{**TEST_MODULES, "module_file": "/abs/x.so"}), "module_file must be a relative path to a .so"),
    (_inputs(**{**TEST_MODULES, "module_file": "tests/modules/datatype.c"}), "module_file must be a relative path"),
    # The module inputs are checked after the build and before random_seeds, as the workflow checks them.
    (_inputs(build="valgrind", module_sha="x"), "build must be one of"),
    (_inputs(random_seeds="5", module_sha="x"), "module_sha, module_parent_sha, and module_file need module_repository"),
    (_inputs(valkey_parent_sha="b" * 12), "valkey_parent_sha must be empty or a full 40-character lowercase commit SHA"),
    (_inputs(valkey_parent_sha=HEAD), "valkey_parent_sha is valkey_sha: a comparison with the same build shows nothing"),
    (_inputs(build="valgrind"), "build must be one of default, asan, ubsan"),
    (_inputs(build=""), "build must be one of default, asan, ubsan"),
    (_experiments("x" * 20_001), "experiments is 20001 characters; the limit is 20,000"),
    (_experiments("{not json"), "experiments is not JSON"),
    (_experiments([EXPERIMENT]), "experiments must be a JSON object mapping a file name to experiment YAML"),
    (_experiments({"Proposed.probe.yaml": EXPERIMENT}), "experiment file name 'Proposed.probe.yaml' must be <name>.yaml"),
    (_experiments({"proposed.probe": EXPERIMENT}), "experiment file name 'proposed.probe' must be <name>.yaml"),
    (_experiments({"a..b.yaml": EXPERIMENT}), "experiment file name 'a..b.yaml' must be"),
    (_experiments({"proposed.probe.yaml": 1}), "experiment proposed.probe.yaml: the value must be YAML text"),
    (_experiments({"proposed.probe.yaml": "a: [b"}), "experiment proposed.probe.yaml is not YAML"),
    (_experiments({"proposed.probe.yaml": "- a"}), "experiment proposed.probe.yaml must be a mapping"),
    (_experiments({"proposed.probe.yaml": EXPERIMENT.replace("proposed.", "valkey.")}),
     "experiment proposed.probe.yaml: id must start with 'proposed.'"),
    (_experiments({"proposed.probe.yaml": "components: []\n"}), "id must start with 'proposed.'"),
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
# fuzzer-pr-batch.yml, and its register bundle, and VALKEY_FUZZER_BIN to its
# installed command: the workflow's step runs where the fuzzer is installed,
# with its dependencies.

_FUZZER_DIR = os.environ.get("VALKEY_FUZZER_DIR", "")
_FUZZER_BIN = shutil.which(os.environ.get("VALKEY_FUZZER_BIN", "") or "") or ""
_WORKFLOW = Path(_FUZZER_DIR) / ".github" / "workflows" / "fuzzer-pr-batch.yml"
_real_workflow = pytest.mark.skipif(
    not (_FUZZER_DIR and _WORKFLOW.is_file() and _FUZZER_BIN),
    reason="needs VALKEY_FUZZER_DIR with its fuzzer-pr-batch.yml, and VALKEY_FUZZER_BIN",
)


def _fuzzer_python() -> str:
    """The interpreter VALKEY_FUZZER_BIN runs on, from its #! line."""
    first = Path(_FUZZER_BIN).read_text(encoding="utf-8", errors="replace").splitlines()[0]
    return first[2:].strip() if first.startswith("#!") else sys.executable


def _workflow() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text())


def _register() -> dict[str, str]:
    root = Path(_FUZZER_DIR) / "tests" / "fixtures" / "spec" / "register"
    return {p.relative_to(root).as_posix(): p.read_text() for p in sorted(root.rglob("*")) if p.is_file()}


def _workflow_check(inputs: dict[str, str], tmp_path: Path) -> subprocess.CompletedProcess:
    """Run the workflow's own input check on ``inputs``, as its step runs it."""
    steps = _workflow()["jobs"]["batch"]["steps"]
    step = next(s for s in steps if "ci.py check-inputs" in s.get("run", ""))
    assert step["run"].strip() == "python .github/scripts/ci.py check-inputs"
    env = {k: os.environ[k] for k in ("PATH", "HOME") if k in os.environ}
    # An input left out reaches the step as the workflow's default for it.
    specs = _workflow()["on" if "on" in _workflow() else True]["workflow_dispatch"]["inputs"]
    env.update({f"INPUT_{k.upper()}": inputs.get(k, str(specs[k].get("default", "")))
                for k in BATCH_INPUTS + OPTIONAL_INPUTS})
    env.update(CANDIDATES_DIR=str(tmp_path / "candidates"), SPEC_DIR=str(tmp_path / "spec"),
               EXPERIMENTS_DIR=str(tmp_path / "experiments"), PARENT_BINARY="/work/valkey-parent/src/valkey-server",
               MODULE_DIR="/work/module-src", MODULE_PARENT_DIR="/work/module-parent", PYTHONPATH=_FUZZER_DIR)
    return subprocess.run([_fuzzer_python(), ".github/scripts/ci.py", "check-inputs"], env=env, cwd=_FUZZER_DIR,
                          capture_output=True, text=True, check=False)


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
        _experiments({"proposed.probe.yaml": EXPERIMENT}, valkey_parent_sha=PARENT, build="asan"),
        _scenarios({"x": REFERENCES_PARENT}) | {"valkey_parent_sha": PARENT},
        _scenarios({"x": WITH_INSTANCE_BINARIES}),
        _scenarios({"x": WITH_ARTIFACTS}),
        _scenarios({"x": REFERENCES_PARENT}),
        _scenarios({"x": REFERENCES_OTHER}) | {"valkey_parent_sha": PARENT},
        _scenarios({"x": FLOW_SCENARIO}) | {"valkey_parent_sha": PARENT},
        _inputs(valkey_parent_sha="b" * 12),
        _inputs(valkey_parent_sha=HEAD),
        _inputs(build="valgrind"),
        _experiments("x" * 20_001),
        _experiments([EXPERIMENT]),
        _experiments({"Proposed.probe.yaml": EXPERIMENT}),
        _experiments({"proposed.probe.yaml": "- a"}),
        _experiments({"proposed.probe.yaml": EXPERIMENT.replace("proposed.", "valkey.")}),
        _scenarios({"x": REFERENCES_MODULE, "y": REFERENCES_MODULE_PARENT}) | JSON_MODULE,
        _scenarios({"x": REFERENCES_MODULE, "y": REFERENCES_PARENT}) | TEST_MODULES,
        _scenarios({"x": REFERENCES_MODULE}) | {**JSON_MODULE, "module_parent_sha": ""},
        _scenarios({"x": REFERENCES_MODULE}),
        _scenarios({"x": REFERENCES_MODULE_PARENT}) | {**JSON_MODULE, "module_parent_sha": ""},
        _scenarios({"x": REFERENCES_PARENT}) | JSON_MODULE,
        _scenarios({"x": REFERENCES_OTHER}),
        _scenarios({"x": FLOW_SCENARIO}) | JSON_MODULE,
        _scenarios({"x": WITH_ARTIFACTS}) | JSON_MODULE,
        _inputs(module_sha=MODULE_SHA),
        _inputs(module_parent_sha=MODULE_PARENT),
        _inputs(**{**JSON_MODULE, "module_repository": "valkey-json"}),
        _inputs(**{**JSON_MODULE, "module_repository": "valkey-io/valkey-search"}),
        _inputs(**{**JSON_MODULE, "module_sha": "C" * 40}),
        _inputs(**{**JSON_MODULE, "module_parent_sha": "d" * 12}),
        _inputs(**{**JSON_MODULE, "module_parent_sha": MODULE_SHA}),
        _inputs(**{**JSON_MODULE, "module_file": "build/src/other.so"}),
        _inputs(**{**TEST_MODULES, "module_file": ""}),
        _inputs(**{**TEST_MODULES, "module_file": "tests/modules/../x.so"}),
        _inputs(**{**TEST_MODULES, "module_file": "/abs/x.so"}),
        _inputs(**{**TEST_MODULES, "module_file": "tests/modules/a..b.so"}),
        _inputs(build="valgrind", module_sha="x"),
        _inputs(random_seeds="5", module_sha="x"),
    ]


@pytest.mark.skipif(not _FUZZER_DIR, reason="needs VALKEY_FUZZER_DIR, a valkey-fuzzer checkout")
def test_the_module_table_is_the_workflows():
    tree = ast.parse((Path(_FUZZER_DIR) / ".github" / "scripts" / "ci.py").read_text())
    table = next(node.value for node in tree.body if isinstance(node, ast.Assign)
                 and [getattr(t, "id", None) for t in node.targets] == ["MODULE_FILES"])
    assert ast.literal_eval(table) == MODULE_FILES


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
        written = sorted(p.name for p in (tmp_path / "candidates").iterdir())
        assert written == [f"{name}.yaml" for name in sorted(json.loads(inputs["scenarios"]))]
        experiments = json.loads(inputs.get("experiments") or "{}")
        if experiments:
            assert sorted(p.name for p in (tmp_path / "experiments").iterdir()) == sorted(experiments)
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


SINCE = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


def _runs(*runs) -> MagicMock:
    github = MagicMock()
    github.get_repo.return_value.get_workflow.return_value.get_runs.return_value = list(runs)
    return github


def _run(title: str, minutes: int, status: str = "completed") -> MagicMock:
    return MagicMock(display_title=title, created_at=SINCE + timedelta(minutes=minutes), status=status,
                     html_url=f"https://x/{title}")


def test_find_run_matches_the_title_among_runs_since_the_dispatch():
    mine = _run("Valkey Fuzzer PR Batch replay-003-bbbbbbbbb", 1)
    github = _runs(_run("Valkey Fuzzer PR Batch replay-004-ccccccccc", 2), mine,
                   _run("Valkey Fuzzer PR Batch replay-003-bbbbbbbbb", -5))
    found = pr_batch_mod.find_run(github, fuzzer_repo="o/f", workflow="fuzzer-pr-batch.yml",
                                  request_id="replay-003-bbbbbbbbb", since=SINCE)
    assert found is mine
    github.get_repo.return_value.get_workflow.return_value.get_runs.assert_called_with(event="workflow_dispatch")
    # Only an older run of the same name: the dispatch has not shown up yet.
    older = _runs(_run("Valkey Fuzzer PR Batch replay-003-bbbbbbbbb", -5))
    assert pr_batch_mod.find_run(older, fuzzer_repo="o/f", workflow="fuzzer-pr-batch.yml",
                                 request_id="replay-003-bbbbbbbbb", since=SINCE) is None


def test_wait_for_run_waits_until_the_run_completes():
    run = _run("Valkey Fuzzer PR Batch r", 1, status="queued")
    statuses = iter(["in_progress", "completed"])
    run.update.side_effect = lambda: setattr(run, "status", next(statuses))
    sleeps = []
    found = pr_batch_mod.wait_for_run(_runs(run), fuzzer_repo="o/f", workflow="w", request_id="r", since=SINCE,
                                      timeout_s=3600, poll_s=5, sleep=sleeps.append)
    assert found is run and sleeps == [5]


def test_wait_for_run_gives_up_at_its_timeout():
    with pytest.raises(TimeoutError, match="no run named 'r' showed up"):
        pr_batch_mod.wait_for_run(_runs(), fuzzer_repo="o/f", workflow="w", request_id="r", since=SINCE,
                                  timeout_s=0.01, poll_s=0.01, sleep=lambda s: None)


def test_wait_for_run_reports_the_run_once_found_and_survives_a_failed_poll():
    from github import GithubException

    run = _run("Valkey Fuzzer PR Batch r", 1, status="queued")
    updates = iter([GithubException(502, "bad gateway", None), "in_progress", "completed"])

    def update():
        outcome = next(updates)
        if isinstance(outcome, Exception):
            raise outcome
        run.status = outcome

    run.update.side_effect = update
    found_runs, sleeps = [], []
    found = pr_batch_mod.wait_for_run(_runs(run), fuzzer_repo="o/f", workflow="w", request_id="r", since=SINCE,
                                      timeout_s=3600, poll_s=5, sleep=sleeps.append, on_found=found_runs.append)
    assert found is run and found_runs == [run] and sleeps == [5, 5]
