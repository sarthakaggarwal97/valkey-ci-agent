"""valkey-fuzzer's PR batch workflow: what it takes, and the batch it hands back.

``fuzzer-pr-batch.yml`` ("Valkey Fuzzer PR Batch", job ``batch``) in
valkey-fuzzer is started by hand with ``scenarios`` (a JSON object mapping a
scenario name to its YAML), ``valkey_repository``, ``valkey_ref``,
``valkey_sha`` and, optionally, ``spec_bundles`` (a JSON object mapping a
spec bundle name to ``{path in the bundle: file text}``, for scenarios
translated from a bundle that brings its own data-only experiments). It
builds that Valkey commit, runs the scenarios through ``valkey-fuzzer
batch`` with the candidate profile, the bundles on
``VALKEY_FUZZER_SPEC_PATH`` and their ``experiments/`` loaded, and uploads
the batch folder as ``fuzzer-batch-<run id>``. :func:`check_batch_inputs`
refuses what the workflow's own input check would refuse, before anything is
dispatched.

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
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import yaml

from scripts.common.job_summary import emit_job_summary
from scripts.common.logging_utils import configure_logging, log_outcome
from scripts.fuzzer.batch import BatchFolder, read_batch, read_batch_dir

logger = logging.getLogger(__name__)

FUZZER_REPO = "valkey-io/valkey-fuzzer"
PR_BATCH_WORKFLOW = "fuzzer-pr-batch.yml"
BATCH_ARTIFACT_PREFIX = "fuzzer-batch-"
RESULTS_FILE = "batch-results.json"

# The workflow's inputs: the four always sent, and spec_bundles when a
# scenario needs a bundle.
BATCH_INPUTS = ("scenarios", "valkey_repository", "valkey_ref", "valkey_sha")
OPTIONAL_INPUTS = ("spec_bundles",)
# The workflow's own limits on `scenarios` and on `spec_bundles`.
MAX_SCENARIOS_CHARS = 60_000
MAX_SPEC_BUNDLES_CHARS = 60_000
# GitHub limits all of a dispatch's inputs together to 65,535 characters.
MAX_DISPATCH_CHARS = 65_535
# The workflow's patterns, exactly.
_REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
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


def check_batch_inputs(inputs: dict[str, str]) -> None:
    """Raise ValueError for inputs fuzzer-pr-batch.yml would refuse.

    The checks, their order and their messages are the workflow's: the sizes
    of ``scenarios`` and ``spec_bundles``, the repository, the commit, each
    scenario's name, YAML and build, then each bundle's name, required files,
    paths and YAML. GitHub's limit on all inputs together comes last. The
    workflow then loads each bundle with the fuzzer's own bundle checks; a
    spec draft's bundle passed those in its spec check gate.
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
    try:
        scenarios = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"scenarios is not JSON: {exc}") from exc
    if not isinstance(scenarios, dict) or not scenarios:
        raise ValueError("scenarios must be a non-empty JSON object mapping a name to scenario YAML")
    for name, text in scenarios.items():
        if not _NAME_RE.fullmatch(name) or ".." in name:
            raise ValueError(f"scenario name {name!r} must be letters, digits, '.', '_', or '-'")
        if not isinstance(text, str):
            raise ValueError(f"scenario {name}: the value must be YAML text")
        _yaml_mapping(text, f"scenario {name} is not YAML", f"scenario {name} must be a mapping")
        document = yaml.safe_load(text)
        if "valkey_binary" in (document.get("deployment") or {}):
            raise ValueError(f"scenario {name} sets deployment.valkey_binary; the batch runs valkey_sha's build")
    _check_spec_bundles(raw_bundles)
    size = sum(len(value) for value in inputs.values())
    if size > MAX_DISPATCH_CHARS:
        raise ValueError(f"the PR batch inputs are {size} characters, over GitHub's limit of 65,535")


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
) -> dict[str, str]:
    """The PR batch's inputs for ``scenarios`` ({name: YAML}) on ``change``, with any bundles they need."""
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
    check_batch_inputs(inputs)
    return inputs


def dispatch_batch(
    gh: Any, inputs: dict[str, str], *, fuzzer_repo: str, workflow: str, ref: str,
) -> None:
    check_batch_inputs(inputs)
    target = gh.get_repo(fuzzer_repo).get_workflow(workflow)
    if not target.create_dispatch(ref, inputs):
        raise RuntimeError(f"GitHub refused to dispatch {workflow} on {fuzzer_repo}")


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
