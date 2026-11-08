"""valkey-fuzzer's PR batch workflow: what it takes, and the batch it hands back.

``fuzzer-pr-batch.yml`` ("Valkey Fuzzer PR Batch", job ``batch``) in
valkey-fuzzer is started by hand with ``scenarios`` (a JSON object mapping a
scenario name to its YAML), ``valkey_repository``, ``valkey_ref``,
``valkey_sha`` and, optionally, ``spec_bundles`` (a JSON object mapping a
spec bundle name to ``{path in the bundle: file text}``, for scenarios
translated from a bundle that brings its own data-only experiments),
``random_seeds`` (that many seeds of each one-build experiment run beside the
scenarios, which may then be empty), ``request_id`` (shown in the run's
title, so :func:`find_run` finds the run a dispatch started),
``valkey_parent_sha`` (a second build, of the change's parent, that the
scenarios reach as the artifact ``parent``), ``build`` (``default``,
``asan`` or ``ubsan``, for both commits), ``experiments`` (a JSON object
mapping a file name to the YAML of a ``proposed.`` experiment the scenarios
use) and ``module_repository``, ``module_sha``, ``module_parent_sha`` and
``module_file`` (a module the batch builds at one or two commits and
declares as the artifacts ``module`` and ``module_parent``, loaded into the
Valkey build; see :class:`ModuleBuild`). It builds that Valkey commit, runs the scenarios through
``valkey-fuzzer batch`` with the candidate profile, the bundles on
``VALKEY_FUZZER_SPEC_PATH`` and their ``experiments/`` loaded, the proposed
experiments loaded too, and uploads the batch folder as
``fuzzer-batch-<run id>``. :func:`check_batch_inputs` refuses what the
workflow's own input check would refuse, before anything is dispatched.

The results are read with the same reader the fuzzer monitor uses,
:mod:`scripts.fuzzer.batch`, with the folder at the artifact's root:
``summary.json`` and, for each run, its run report. The command line reads a
downloaded folder (``--batch``) or downloads the artifact of a PR batch run
(``--run-id``), writes ``batch-results.json`` and a job summary, and exits
non-zero unless every run passed. It fails closed: a summary that cannot be
read is an error, and a run whose report cannot be read, a batch that did
not finish, a zoo case that does not hold, or a leak makes the verdict
``needs-human-triage``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import yaml
from github import GithubException

from scripts.common.job_summary import emit_job_summary
from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.fuzzer.batch import BatchFolder, read_batch, read_batch_dir

logger = logging.getLogger(__name__)

FUZZER_REPO = "valkey-io/valkey-fuzzer"
PR_BATCH_WORKFLOW = "fuzzer-pr-batch.yml"
BATCH_ARTIFACT_PREFIX = "fuzzer-batch-"
RESULTS_FILE = "batch-results.json"

# The workflow's inputs: the four always sent, spec_bundles when a scenario
# needs a bundle, the random baseline and the run's name when asked for, the
# parent build and the proposed experiments when the scenarios need them, and
# the build when it is not the default, and the module builds when the scenarios load a module.
BATCH_INPUTS = ("scenarios", "valkey_repository", "valkey_ref", "valkey_sha")
OPTIONAL_INPUTS = (
    "spec_bundles", "random_seeds", "request_id", "exclude_experiments", "valkey_parent_sha", "build", "experiments",
    "module_repository", "module_sha", "module_parent_sha", "module_file",
)
# The builds the workflow offers; both commits get the same one.
BUILDS = ("default", "asan", "ubsan")
# A proposed experiment's ID starts with this; the batch loads no other from `experiments`.
PROPOSED_PREFIX = "proposed."
# The title the workflow gives a run: its run-name, with the request_id.
RUN_TITLE = "Valkey Fuzzer PR Batch {request_id}"
# The workflow's own limits on `scenarios`, on `spec_bundles` and on `experiments`.
MAX_SCENARIOS_CHARS = 60_000
MAX_SPEC_BUNDLES_CHARS = 60_000
MAX_EXPERIMENTS_CHARS = 20_000
# GitHub limits all of a dispatch's inputs together to 65,535 characters.
MAX_DISPATCH_CHARS = 65_535
# The workflow's patterns, exactly.
_REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
_RANDOM_SEEDS_RE = re.compile(r"[0-4]")
_REQUEST_ID_RE = re.compile(r"[A-Za-z0-9_.-]{0,100}")
_EXPERIMENT_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,99}")
# A proposed experiment's file in `experiments`: its ID and .yaml.
EXPERIMENT_FILE_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,99}\.yaml")
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}")
_BUNDLE_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
_FILE_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}"
_BUNDLE_PATH_RE = re.compile(
    rf"(?:claims|mapping)\.yaml|model\.tla|{_FILE_NAME}\.cfg"
    rf"|negative-controls/{_FILE_NAME}\.(?:cfg|tla)|experiments/{_FILE_NAME}\.ya?ml"
)
BUNDLE_REQUIRED_FILES = ("claims.yaml", "model.tla", "mapping.yaml")
# A batch job running a scenario file is named scenario-<file stem>.
_SCENARIO_JOB = "scenario-"


@dataclass(frozen=True)
class Module:
    """A Valkey module repository: its default branch, the file its build leaves in its checkout, the bash
    commands that build it there against a Valkey checkout (``$VALKEY_SRC``), what the proposal prompt says of
    its commands and pitfalls, and what the runner needs first: ``setup``, bash run once before any build, which
    may keep downloads in ``$DEPS_DIR``, a directory cached under ``deps`` (the version of what it fetches)."""

    branch: str
    file: str
    build: str
    note: str
    setup: str = ""
    deps: str = ""


# The module repositories, by repository name (a fork builds as its upstream). Adding a module is one entry.
MODULES = {
    "valkey-json": Module(
        branch="unstable", file="build/src/libjson.so",
        setup="command -v cmake > /dev/null || sudo apt-get install -y cmake",
        # build.sh takes the Valkey build's valkeymodule.h since 2026. Before, it fetched the header of
        # SERVER_VERSION itself, and a parallel build could compile before the header landed, so it is put in
        # place first; and it also ran the module's own tests, from a test framework commit that is gone, so only
        # the module is built. Its flags end in -Werror, which fails on what newer GCC warns of: a build type of
        # its own adds -Wno-error after them. Older RapidJSON asks for a CMake older than CMake 4 accepts. Before
        # #41 (4fb72a9), its allocator failed to compile on GCC 12 and later: that fix is applied first, as a bisect
        # carries a build fix.
        build="""export CMAKE_POLICY_VERSION_MINIMUM=3.5
if ! grep -q "operator==(const stl_allocator" src/json/memory.h; then
  git fetch -q --depth 2 https://github.com/valkey-io/valkey-json.git 4fb72a926987b941ebde88d5aab750dea7652b5e
  git diff 4fb72a926987b941ebde88d5aab750dea7652b5e^ 4fb72a926987b941ebde88d5aab750dea7652b5e -- src \
    | git apply --whitespace=nowarn
fi
mkdir -p src/include && cp "${VALKEY_SRC}/src/valkeymodule.h" src/include/
if grep -q VALKEY_MODULE_H_PATH build.sh; then
  SERVER_VERSION=unstable VALKEY_MODULE_H_PATH="${VALKEY_SRC}/src/valkeymodule.h" ./build.sh
else
  mkdir -p build && cd build
  cmake .. -DVALKEY_VERSION=unstable -DENABLE_UNIT_TESTS=OFF -DENABLE_INTEGRATION_TESTS=OFF \
    -DCMAKE_BUILD_TYPE=Walk -DCMAKE_C_FLAGS_WALK=-Wno-error -DCMAKE_CXX_FLAGS_WALK=-Wno-error
  make -j"$(nproc)" json
fi""",
        note="For valkey-json, give JSON.GET `compare: json`: both replies are parsed and compared as JSON "
             "values, so key order and spacing do not count.",
    ),
    "valkey-search": Module(
        branch="main", file=".build-release/libsearch.so", deps="1.0.0-rc1",
        # The project's own prebuilt gRPC, Protobuf and Abseil (its devcontainer installs the same package).
        setup="""sudo apt-get install -y cmake ninja-build
deb="${DEPS_DIR}/valkey-search-deps-ubuntu-noble-amd64.deb"
if [ ! -f "${deb}" ]; then
  mkdir -p "${DEPS_DIR}"
  curl -fsSL -o "${deb}" "https://github.com/valkey-io/valkey-search/releases/download/${DEPS_VERSION}/${deb##*/}"
fi
sudo dpkg -i "${deb}"
""",
        # build.sh finds /opt/valkey-search-deps itself since August 2026; before, it took --use-system-modules.
        # #621 (9ea236a) dropped the schema_manager declaration from QueryCommand::Execute but kept its uses, so
        # no commit until #741 (b0e8a26) restored it compiles: that fix is applied first, as a bisect carries a
        # build fix. #483 (19977d4) set BUILD_TOOL from an undefined NINJA_TOOL for the Ninja default, so no
        # commit until #494 (cdfe0c4) fixed that ran the build at all (it left no libsearch.so); NINJA_TOOL is
        # exported so build.sh finds ninja on that window, which is harmless once it stops reading the variable.
        # Before #172 (0730baa) rewrote the protobuf rules, nothing ordered the compiles after protoc's gRPC
        # output: ninja can compile an includer of coordinator.grpc.pb.h first and stop, with the header
        # generated by the time it does, so one more pass succeeds; the retry runs only when that header landed.
        build="""export NINJA_TOOL=ninja
deps=/opt/valkey-search-deps
export CMAKE_PREFIX_PATH="${deps}/lib/cmake/protobuf:${deps}/lib/cmake/absl:${deps}/lib/cmake/grpc:\
${deps}/lib/cmake/GTest:${deps}/lib/cmake/utf8_range:${deps}/lib/cmake/benchmark:${deps}"
if grep -q 'schema_manager\\.' src/commands/commands.cc 2> /dev/null \\
    && ! grep -q '&schema_manager =' src/commands/commands.cc; then
  git fetch -q --depth 2 https://github.com/valkey-io/valkey-search.git b0e8a26b5a2b1131062875be9e26153dd315aaaf
  git diff b0e8a26b5a2b1131062875be9e26153dd315aaaf^ b0e8a26b5a2b1131062875be9e26153dd315aaaf -- src \\
    | git apply --whitespace=nowarn
fi
if grep -q -- '--use-system-modules)' build.sh; then args=--use-system-modules; else args=; fi
if ! ./build.sh ${args}; then
  ls .build-release/src/coordinator/coordinator.grpc.pb.h
  ./build.sh ${args}
fi""",
        note="For valkey-search, FT.SEARCH returns its results in no fixed order: give it `compare: unordered`. "
             "Indexing is asynchronous: after writing the documents, send FT.INFO with `wait_s` and an `expect` "
             "on its num_docs or indexing state before any FT.SEARCH.",
    ),
    "valkey-bloom": Module(
        branch="unstable", file="target/release/libvalkey_bloom.so",
        # Without a Cargo.lock, the module SDK resolves to its newest release, whose API older merges predate
        # (valkey-module 0.1.15 renamed get_redis_version, redis-module 2.0.8 changed its command macro): the SDK
        # crates, valkey-module now and redis-module before it, are taken as they were at the commit.
        build="""if [ ! -f Cargo.lock ]; then
  cargo generate-lockfile -q
  python3 - "$(git log -1 --format=%cI)" <<'EOF'
import json, subprocess, sys, urllib.request
from datetime import datetime

before = datetime.fromisoformat(sys.argv[1])
for crate in ("valkey-module", "valkey-module-macros", "valkey-module-macros-internals",
              "redis-module", "redis-module-macros", "redis-module-macros-internals"):
    pkgid = subprocess.run(["cargo", "pkgid", crate], capture_output=True, text=True)
    if pkgid.returncode != 0:
        continue
    series = pkgid.stdout.strip().rsplit("@", 1)[1].split(".")[:2]
    request = urllib.request.Request(f"https://crates.io/api/v1/crates/{crate}/versions",
                                     headers={"User-Agent": "valkey-ci-agent-walk"})
    versions = [v for v in json.load(urllib.request.urlopen(request))["versions"]
                if not v["yanked"] and v["num"].split(".")[:2] == series
                and datetime.fromisoformat(v["created_at"]) <= before]
    if versions:
        newest = max(versions, key=lambda v: v["created_at"])["num"]
        subprocess.run(["cargo", "update", "-q", "-p", crate, "--precise", newest], check=True)
EOF
fi
cargo build --release""",
        note="For valkey-bloom (BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS, BF.CARD, BF.RESERVE, BF.INFO, BF.INSERT, "
             "BF.LOAD), a filter is probabilistic: BF.EXISTS of an item never added may answer 1, so never expect "
             "a reply about an absent item; BF.INFO's sizes follow the build's internals.",
    ),
}
# The module repositories the PR batch builds and the file each build leaves in its checkout: the workflow's
# MODULE_FILES (.github/scripts/ci.py), exactly. valkey means its tests/modules, many modules, so module_file
# names the one.
MODULE_FILES = {"valkey-json": MODULES["valkey-json"].file, "valkey": None}
_MODULE_FILE_RE = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*\.so")
# The artifacts the batch may declare, and the input each needs, for the refusal of a reference to one it does not.
_DECLARED_BY = {
    "artifacts.parent": "valkey_parent_sha",
    "artifacts.module": "module_repository",
    "artifacts.module_parent": "module_parent_sha",
}


def module_repository(repo: str) -> bool:
    """Whether ``repo`` (OWNER/NAME) is a module repository, not Valkey itself."""
    return repo.split("/")[-1] in MODULES


@dataclass(frozen=True)
class ModuleBuild:
    """A module the PR batch builds against its Valkey build: ``repository`` at ``sha`` is ``artifacts.module``,
    at ``parent_sha`` (when given) ``artifacts.module_parent``; ``file`` is the built module in its checkout,
    empty for the module's own (Valkey's tests/modules needs one, such as ``tests/modules/datatype.so``)."""

    repository: str
    sha: str
    parent_sha: str = ""
    file: str = ""


def check_batch_inputs(inputs: dict[str, str]) -> None:
    """Raise ValueError for inputs fuzzer-pr-batch.yml would refuse.

    The checks, their order and their messages are the workflow's: the sizes
    of ``scenarios`` and ``spec_bundles``, the repository, the commits, the
    build, the module inputs, each scenario's name, YAML, build, artifact
    references and the shape the artifacts block is appended to, then each bundle's name, required
    files, paths and YAML, then the size of ``experiments`` and each
    experiment's file name, YAML and ID. GitHub's limit on all inputs
    together comes last.
    The workflow then loads each bundle with the fuzzer's own bundle checks
    (a spec draft's bundle passed those in its spec check gate) and compiles
    the experiments (a proposal's passed that too).
    """
    keys = set(inputs)
    if not set(BATCH_INPUTS) <= keys <= set(BATCH_INPUTS + OPTIONAL_INPUTS):
        raise ValueError(
            f"the PR batch takes {', '.join(BATCH_INPUTS)} and optionally {', '.join(OPTIONAL_INPUTS)}; "
            f"got {', '.join(sorted(keys))}"
        )
    for key, value in inputs.items():
        if not isinstance(value, str):
            raise ValueError(f"PR batch input {key} must be a string")
    raw = inputs["scenarios"]
    if len(raw) > MAX_SCENARIOS_CHARS:
        raise ValueError(f"scenarios is {len(raw)} characters; the limit is 60,000")
    raw_bundles = inputs.get("spec_bundles", "")
    if len(raw_bundles) > MAX_SPEC_BUNDLES_CHARS:
        raise ValueError(f"spec_bundles is {len(raw_bundles)} characters; the limit is 60,000")
    if not _REPOSITORY_RE.fullmatch(inputs["valkey_repository"]):
        raise ValueError("valkey_repository must be OWNER/NAME")
    if not _SHA_RE.fullmatch(inputs["valkey_sha"]):
        raise ValueError("valkey_sha must be a full 40-character lowercase commit SHA")
    parent_sha = inputs.get("valkey_parent_sha", "")
    if parent_sha and not _SHA_RE.fullmatch(parent_sha):
        raise ValueError("valkey_parent_sha must be empty or a full 40-character lowercase commit SHA")
    if parent_sha == inputs["valkey_sha"]:
        raise ValueError("valkey_parent_sha is valkey_sha: a comparison with the same build shows nothing")
    if inputs.get("build", "default") not in BUILDS:
        raise ValueError(f"build must be one of {', '.join(BUILDS)}")
    module_repository, module_parent_sha = _check_module(inputs)
    # What the batch declares to every scenario: the parent build, and the module builds.
    declared = [name for name, given in (("artifacts.parent", parent_sha), ("artifacts.module", module_repository),
                                         ("artifacts.module_parent", module_parent_sha)) if given]
    random_seeds = inputs.get("random_seeds", "0")
    if not _RANDOM_SEEDS_RE.fullmatch(random_seeds):
        raise ValueError("random_seeds must be 0 to 4")
    if not _REQUEST_ID_RE.fullmatch(inputs.get("request_id", "")):
        raise ValueError("request_id must be at most 100 letters, digits, '.', '_', or '-'")
    excluded = inputs.get("exclude_experiments", "").split()
    if len(excluded) > 50 or not all(_EXPERIMENT_ID_RE.fullmatch(item) for item in excluded):
        raise ValueError("exclude_experiments must be at most 50 experiment IDs separated by spaces")
    if excluded and random_seeds == "0":
        raise ValueError(
            "exclude_experiments leaves experiments out of the random baseline, which random_seeds 0 skips"
        )
    try:
        scenarios = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"scenarios is not JSON: {exc}") from exc
    if not isinstance(scenarios, dict):
        raise ValueError("scenarios must be a JSON object mapping a name to scenario YAML")
    if not scenarios and random_seeds == "0":
        raise ValueError("scenarios is empty and random_seeds is 0: there is nothing to run")
    for name, text in scenarios.items():
        if not _NAME_RE.fullmatch(name) or ".." in name:
            raise ValueError(f"scenario name {name!r} must be letters, digits, '.', '_', or '-'")
        if not isinstance(text, str):
            raise ValueError(f"scenario {name}: the value must be YAML text")
        _yaml_mapping(text, f"scenario {name} is not YAML", f"scenario {name} must be a mapping")
        document = yaml.safe_load(text)
        # Each of these names a binary the batch would run instead of valkey_sha's build.
        for field in ("valkey_binary", "instance_binaries"):
            if field in (document.get("deployment") or {}):
                raise ValueError(f"scenario {name} sets deployment.{field}; the batch runs valkey_sha's build")
        if "artifacts" in document:
            raise ValueError(
                f"scenario {name} declares artifacts; the batch declares them (artifacts.parent, artifacts.module, "
                "artifacts.module_parent)"
            )
        # The batch declares only what it built.
        for reference in _artifact_references(document):
            if reference not in declared:
                needs = _DECLARED_BY.get(reference)
                raise ValueError(
                    f"scenario {name} references {reference}, which the batch does not declare"
                    + (f" (no {needs})" if needs else "")
                )
        if declared:
            # The batch appends the block to the text, so the scenario runs as sent; the append must add
            # exactly that key. The paths do not matter here, only the shape.
            block = _artifacts_block(declared)
            try:
                after = yaml.safe_load(text + block)
            except yaml.YAMLError:
                after = None
            if after != {**document, "artifacts": yaml.safe_load(block)["artifacts"]}:
                raise ValueError(
                    f"scenario {name}: appending the artifacts block does not add exactly that key (a flow-style "
                    "top-level mapping, or no final newline?)"
                )
    _check_spec_bundles(raw_bundles)
    _check_experiments(inputs.get("experiments", ""))
    size = sum(len(value) for value in inputs.values())
    if size > MAX_DISPATCH_CHARS:
        raise ValueError(f"the PR batch inputs are {size} characters, over GitHub's limit of 65,535")


def _check_module(inputs: dict[str, str]) -> tuple[str, str]:
    """The workflow's check of the module inputs: the module repository and its parent commit, empty without."""
    repository, sha = inputs.get("module_repository", ""), inputs.get("module_sha", "")
    parent_sha, module_file = inputs.get("module_parent_sha", ""), inputs.get("module_file", "")
    if not repository:
        if sha or parent_sha or module_file:
            raise ValueError("module_sha, module_parent_sha, and module_file need module_repository")
        return "", ""
    if not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError("module_repository must be empty or OWNER/NAME")
    name = repository.split("/")[1]
    if name not in MODULE_FILES:
        raise ValueError(
            f"module_repository {repository} is not a module the batch builds; it builds {', '.join(MODULE_FILES)}"
        )
    if not _SHA_RE.fullmatch(sha):
        raise ValueError("module_sha must be a full 40-character lowercase commit SHA")
    if parent_sha and not _SHA_RE.fullmatch(parent_sha):
        raise ValueError("module_parent_sha must be empty or a full 40-character lowercase commit SHA")
    if parent_sha == sha:
        raise ValueError("module_parent_sha is module_sha: a comparison with the same build shows nothing")
    module_file = module_file or MODULE_FILES[name] or ""
    if not module_file:
        raise ValueError(f"{name} builds many modules: module_file must name one, such as tests/modules/datatype.so")
    if not _MODULE_FILE_RE.fullmatch(module_file) or ".." in module_file.split("/"):
        raise ValueError("module_file must be a relative path to a .so in the module's checkout")
    return repository, parent_sha


# The provider of each artifact the batch declares.
_ARTIFACT_PROVIDERS = {
    "artifacts.parent": "valkey.artifact.local-binary",
    "artifacts.module": "valkey.artifact.local-module",
    "artifacts.module_parent": "valkey.artifact.local-module",
}


def _artifacts_block(declared: Sequence[str]) -> str:
    """The shape of the block the batch appends, declaring ``declared``; the batch fills in its builds' paths."""
    lines = ["artifacts:"]
    for reference in declared:
        lines += [f"  {reference.split('.')[1]}:", f"    use: {_ARTIFACT_PROVIDERS[reference]}", "    with:",
                  '      path: "build"']
    return "\n".join(lines) + "\n"


def _artifact_references(value: Any) -> list[str]:
    """Every ``artifacts.<id>`` an authored ``{$ref: artifacts.<id>.<output>}`` in ``value`` names, in order seen."""
    found: list[str] = []
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("artifacts."):
            found.append(".".join(reference.split(".")[:2]))
        for item in value.values():
            found += _artifact_references(item)
    elif isinstance(value, list):
        for item in value:
            found += _artifact_references(item)
    return found


def _yaml_mapping(text: str, not_yaml: str, not_mapping: str) -> None:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"{not_yaml}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(not_mapping)


def _check_spec_bundles(raw: str) -> None:
    """The workflow's check of ``spec_bundles``: {bundle name: {path in the bundle: text}}."""
    try:
        bundles = json.loads(raw) if raw.strip() else {}
    except ValueError as exc:
        raise ValueError(f"spec_bundles is not JSON: {exc}") from exc
    if not isinstance(bundles, dict):
        raise ValueError("spec_bundles must be a JSON object mapping a bundle name to its files")
    for bundle, files in bundles.items():
        if not _BUNDLE_NAME_RE.fullmatch(bundle):
            raise ValueError(f"spec bundle name {bundle!r} must be lowercase letters, digits, or '-'")
        if not isinstance(files, dict) or not files:
            raise ValueError(f"spec bundle {bundle}: the value must map paths in the bundle to file text")
        for required in BUNDLE_REQUIRED_FILES:
            if required not in files:
                raise ValueError(f"spec bundle {bundle} has no {required}")
        for path, text in files.items():
            if not _BUNDLE_PATH_RE.fullmatch(path) or ".." in path:
                raise ValueError(
                    f"spec bundle {bundle}: {path!r} must be claims.yaml, model.tla, mapping.yaml, <name>.cfg, "
                    "negative-controls/<name>.cfg or .tla, or experiments/<name>.yaml"
                )
            if not isinstance(text, str):
                raise ValueError(f"spec bundle {bundle}: {path} must be file text")
            if path.endswith((".yaml", ".yml")):
                _yaml_mapping(text, f"spec bundle {bundle}: {path} is not YAML",
                              f"spec bundle {bundle}: {path} must be a mapping")


def _check_experiments(raw: str) -> None:
    """The workflow's check of ``experiments``: {file name: the YAML of a proposed experiment}.

    The workflow then loads them into the fuzzer's catalog; a proposal's
    experiments compiled there before they were accepted.
    """
    if len(raw) > MAX_EXPERIMENTS_CHARS:
        raise ValueError(f"experiments is {len(raw)} characters; the limit is 20,000")
    try:
        experiments = json.loads(raw) if raw.strip() else {}
    except ValueError as exc:
        raise ValueError(f"experiments is not JSON: {exc}") from exc
    if not isinstance(experiments, dict):
        raise ValueError("experiments must be a JSON object mapping a file name to experiment YAML")
    for name, text in experiments.items():
        if not EXPERIMENT_FILE_RE.fullmatch(name) or ".." in name:
            raise ValueError(
                f"experiment file name {name!r} must be <name>.yaml, the name lowercase letters, digits, '.', '_', "
                "or '-'"
            )
        if not isinstance(text, str):
            raise ValueError(f"experiment {name}: the value must be YAML text")
        _yaml_mapping(text, f"experiment {name} is not YAML", f"experiment {name} must be a mapping")
        experiment_id = yaml.safe_load(text).get("id")
        if not isinstance(experiment_id, str) or not experiment_id.startswith(PROPOSED_PREFIX):
            raise ValueError(f"experiment {name}: id must start with '{PROPOSED_PREFIX}'")


def bundle_path_allowed(path: str) -> bool:
    """Whether the PR batch takes ``path`` in a spec bundle."""
    return bool(_BUNDLE_PATH_RE.fullmatch(path)) and ".." not in path


def batch_names(scenario_ids: list[str]) -> dict[str, str]:
    """A name the PR batch accepts for each scenario ID, the ID itself when it fits.

    A scenario ID outside the workflow's name pattern (``spec translate``
    keeps ``:`` and has no length limit) has every other character replaced
    with ``-``, a leading one dropped, ``..`` collapsed, and is cut to 100
    characters; a name that collides gets ``-2``, ``-3``, ... instead of its
    tail. The batch runs each as the job ``scenario-<name>``.
    """
    names: dict[str, str] = {}
    taken: set[str] = set()
    for scenario_id in scenario_ids:
        name = scenario_id
        if not _NAME_RE.fullmatch(name) or ".." in name:
            name = re.sub(r"[^A-Za-z0-9_.-]", "-", name).lstrip("_.-")
            while ".." in name:
                name = name.replace("..", ".")
            name = (name or "scenario")[:100]
        base, count = name, 1
        while name in taken:
            count += 1
            suffix = f"-{count}"
            name = base[: 100 - len(suffix)] + suffix
        names[scenario_id] = name
        taken.add(name)
    return names


def batch_dispatch_inputs(
    change: dict[str, Any], scenarios: dict[str, str], spec_bundles: dict[str, dict[str, str]] | None = None,
    *, random_seeds: int = 0, request_id: str = "", exclude_experiments: Sequence[str] = (),
    parent_sha: str | None = None, build: str = "default", experiments: dict[str, str] | None = None,
    module: ModuleBuild | None = None,
) -> dict[str, str]:
    """The PR batch's inputs for ``scenarios`` ({name: YAML}) on ``change``, with any bundles they need,
    ``random_seeds`` seeds of each one-build experiment but ``exclude_experiments`` beside them, and the
    run named ``request_id``. With ``parent_sha`` the batch also builds that commit, as the artifact
    ``parent``; ``build`` picks the build of both; ``experiments`` ({file name: YAML}) are the proposed
    experiments the scenarios use; ``module`` is a module the batch builds and loads into ``change``'s
    build, as the artifacts ``module`` and ``module_parent``. For a change to a module, ``change`` is the
    pinned Valkey commit the module builds load into."""
    pr_number = change.get("pr_number")
    head_sha = str(change["head_sha"])
    inputs = {
        "scenarios": json.dumps(scenarios, separators=(",", ":")),
        "valkey_repository": str(change["repo"]),
        "valkey_ref": f"refs/pull/{int(pr_number)}/head" if pr_number else head_sha,
        "valkey_sha": head_sha,
    }
    if spec_bundles:
        inputs["spec_bundles"] = json.dumps(spec_bundles, separators=(",", ":"))
    if random_seeds:
        inputs["random_seeds"] = str(random_seeds)
    if request_id:
        inputs["request_id"] = request_id
    if exclude_experiments:
        inputs["exclude_experiments"] = " ".join(exclude_experiments)
    if parent_sha:
        inputs["valkey_parent_sha"] = parent_sha
    if build != "default":
        inputs["build"] = build
    if experiments:
        inputs["experiments"] = json.dumps(experiments, separators=(",", ":"))
    if module is not None:
        inputs["module_repository"] = module.repository
        inputs["module_sha"] = module.sha
        if module.parent_sha:
            inputs["module_parent_sha"] = module.parent_sha
        if module.file:
            inputs["module_file"] = module.file
    check_batch_inputs(inputs)
    return inputs


def dispatch_batch(
    gh: Any, inputs: dict[str, str], *, fuzzer_repo: str, workflow: str, ref: str,
) -> None:
    check_batch_inputs(inputs)
    target = gh.get_repo(fuzzer_repo).get_workflow(workflow)
    if not target.create_dispatch(ref, inputs):
        raise RuntimeError(f"GitHub refused to dispatch {workflow} on {fuzzer_repo}")


def find_run(gh: Any, *, fuzzer_repo: str, workflow: str, request_id: str, since: datetime) -> Any | None:
    """The run of ``workflow`` a dispatch named ``request_id`` started at or after ``since``, if it shows yet."""
    title = RUN_TITLE.format(request_id=request_id)
    target = gh.get_repo(fuzzer_repo).get_workflow(workflow)
    for run in target.get_runs(event="workflow_dispatch"):
        if run.created_at < since:
            return None  # newest first: the rest are older still
        if run.display_title == title:
            return run
    return None


def wait_for_run(
    gh: Any, *, fuzzer_repo: str, workflow: str, request_id: str, since: datetime,
    timeout_s: float, poll_s: float = 60.0, sleep: Callable[[float], None] = time.sleep,
    on_found: Callable[[Any], None] | None = None,
) -> Any:
    """Find the run a dispatch named ``request_id`` started, and wait until it completes.

    ``on_found`` is called once, with the run, as soon as it shows. A GitHub
    or network error during a poll is logged and the next poll tries again:
    one failed call must not lose a wait of several hours.
    """
    deadline = time.monotonic() + timeout_s
    run = None
    while time.monotonic() < deadline:
        try:
            if run is None:
                run = find_run(gh, fuzzer_repo=fuzzer_repo, workflow=workflow, request_id=request_id, since=since)
                if run is not None and on_found is not None:
                    on_found(run)
            if run is not None:
                run.update()
                if run.status == "completed":
                    return run
        except (GithubException, OSError) as exc:
            logger.warning("Polling the PR batch %s failed, retrying: %s", request_id, exc)
        sleep(poll_s)
    where = run.html_url if run is not None else f"no run named {request_id!r} showed up"
    raise TimeoutError(f"the PR batch did not complete within {timeout_s:.0f}s: {where}")


# --- Results -----------------------------------------------------------------


def scenario_name(run_id: str) -> str | None:
    """The scenario a PR batch job ran: ``scenario-<name>`` is the file ``<name>.yaml``."""
    return run_id[len(_SCENARIO_JOB):] if run_id.startswith(_SCENARIO_JOB) else None


def summarize(batch: BatchFolder) -> dict[str, Any]:
    """The PR batch's results, from its batch folder.

    ``verdict`` is ``pass`` when every run passed, ``fail`` when one did not,
    and ``needs-human-triage`` whenever the folder cannot be trusted: a run
    without a readable report, an unfinished batch, a zoo case that does
    not hold, or a leak.
    """
    if batch.problem:
        raise ValueError(f"the batch folder cannot be read: {batch.problem}")
    problems: list[str] = []
    if not batch.complete:
        problems.append(f"the batch did not finish: {batch.finished} of {batch.jobs} jobs ran")
    runs = []
    for run in batch.runs:
        if run.report_problem and not run.is_zoo:
            problems.append(f"run {run.id}: {run.report_problem}")
        runs.append({
            "run": run.id, "scenario": scenario_name(run.id), "source": run.source, "status": run.status,
            "summary": run.summary, "report": run.report_path, "report_problem": run.report_problem,
            "signature_key": run.signature_key, "leaks": run.leaks, "origin": run.origin,
        })
    signatures = [
        {"key": g.key, "label": g.label, "status": g.status, "experiment": g.experiment, "runs": g.runs,
         "rerun_verdict": g.rerun_verdict,
         "reruns": f"{g.reruns_reproduced}/{g.reruns_ran}" if g.rerun_verdict else None}
        for g in batch.groups
    ]
    zoo = batch.zoo or {}
    for case in zoo.get("cases") or []:
        if isinstance(case, dict) and case.get("holds") is False:
            problems.append(f"zoo case {case.get('case')} does not hold: the fuzzer missed a bug it catches")
    if batch.leaks:
        problems.append(f"the runs left {len(batch.leaks)} leftover(s) on the host: a fuzzer cleanup bug")
    failed = any(run.status != "PASS" for run in batch.main_runs)
    verdict = "needs-human-triage" if problems else "fail" if failed else "pass"
    return {
        "batch": {"batch_id": batch.batch_id, "status": batch.status, "complete": batch.complete,
                  "jobs": batch.jobs, "finished": batch.finished, "counts": batch.counts},
        "verdict": verdict,
        "problems": problems,
        "runs": runs,
        "signatures": signatures,
        "leaks": batch.leaks,
    }


def _cell(text: Any) -> str:
    return " ".join(str(text or "").split()).replace("|", "\\|")[:200]


def render_results(results: dict[str, Any], where: str) -> str:
    batch = results["batch"]
    lines = [
        f"## Fuzzer PR batch {where}", "",
        f"Verdict: **{results['verdict']}**. Batch status `{batch['status']}`, runs by status `{batch['counts']}`.",
    ]
    if results["problems"]:
        lines += ["", "Needs a human:", *[f"- {_cell(p)}" for p in results["problems"]]]
    lines += ["", "| Run | Scenario | Status | Summary |", "|---|---|---|---|"]
    for run in results["runs"]:
        if run["source"] in ("zoo", "serial"):
            continue
        lines.append(f"| `{run['run']}` | {run['scenario'] or '-'} | {run['status']} | {_cell(run['summary'])} |")
    if results["signatures"]:
        lines += ["", "| Failure signature | Runs | Serial re-runs |", "|---|---|---|"]
        for group in results["signatures"]:
            reruns = f"{group['rerun_verdict']} ({group['reruns']})" if group["rerun_verdict"] else "none"
            lines.append(f"| {_cell(group['label'])} | {len(group['runs'])} | {reruns} |")
    return "\n".join(lines)


def download_batch(gh: Any, token: str, fuzzer_repo: str, run_id: int) -> dict[str, bytes]:
    """The ``fuzzer-batch-<run id>`` artifact of a PR batch run, as files."""
    from scripts.common.workflow_artifacts import ArtifactClient

    client = ArtifactClient(gh, token=token)
    name = f"{BATCH_ARTIFACT_PREFIX}{run_id}"
    artifact = next((a for a in client.list_run_artifacts(fuzzer_repo, run_id)
                     if a.name == name and not a.expired), None)
    if artifact is None:
        raise ValueError(f"PR batch run {run_id} on {fuzzer_repo} has no {name} artifact")
    files = client.download_artifact(fuzzer_repo, artifact.artifact_id)
    if not files:
        raise ValueError(f"the {name} artifact was empty or unreadable")
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read a valkey-fuzzer PR batch's results, a batch folder")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run-id", type=int, help="the fuzzer-pr-batch.yml run to download the results of")
    source.add_argument("--batch", help="a downloaded fuzzer-batch-<run id> artifact: the folder holding summary.json")
    parser.add_argument("--fuzzer-repo", default=FUZZER_REPO)
    parser.add_argument("--out", default="fuzzer-batch-results", help="where to write batch-results.json")
    parser.add_argument("--target-token", default=None, help="GitHub token (falls back to TARGET_TOKEN)")
    args = parser.parse_args(argv)
    configure_logging()
    try:
        if args.batch:
            where = args.batch
            batch = read_batch_dir(Path(args.batch))
        else:
            token = args.target_token or os.environ.get("TARGET_TOKEN", "")
            if not token:
                parser.error("--target-token or TARGET_TOKEN env var is required with --run-id")
            from github import Auth, Github

            where = f"run {args.run_id} on {args.fuzzer_repo}"
            batch = read_batch(download_batch(Github(auth=Auth.Token(token)), token, args.fuzzer_repo, args.run_id))
        results = summarize(batch)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / RESULTS_FILE).write_text(json.dumps(results, indent=2), encoding="utf-8")
        emit_job_summary(render_results(results, where))
        passed = results["verdict"] == "pass"
        log_outcome(logger, logging.INFO if passed else logging.WARNING,
                    f"Fuzzer PR batch {where}: {results['verdict']}, "
                    f"runs by status {batch.counts}; written to {out / RESULTS_FILE}")
        return 0 if passed else 1
    except Exception as exc:
        log_outcome(logger, logging.ERROR, f"Reading the fuzzer PR batch failed: {' '.join(str(exc).split())}")
        logger.debug("Traceback", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
