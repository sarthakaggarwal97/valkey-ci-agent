"""Contract tests for the workflow that replays merged pull requests through the fuzzer."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_WORKFLOW = Path(".github/workflows/fuzzer-replay.yml")


def _workflow() -> dict:
    # BaseLoader keeps the literal `on` key and every scalar as a string.
    return yaml.load(_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _step(job: str, name: str) -> dict:
    return next(step for step in _workflow()["jobs"][job]["steps"] if step.get("name") == name)


def test_dispatch_only_with_its_inputs() -> None:
    on = _workflow()["on"]
    assert list(on) == ["workflow_dispatch"]
    inputs = on["workflow_dispatch"]["inputs"]
    assert list(inputs) == ["range", "merges", "first", "count", "random_seeds", "max_parallel", "fuzzer_ref",
                            "build"]
    # A range or the merges given by SHA: neither is required on its own, the plan step wants one of them.
    assert [name for name, spec in inputs.items() if spec.get("required") == "true"] == []
    assert (inputs["range"]["default"], inputs["merges"]["default"]) == ("", "")
    assert (inputs["build"]["type"], inputs["build"]["options"], inputs["build"]["default"]) == (
        "choice", ["default", "asan", "ubsan"], "default",
    )


def test_the_build_is_checked_and_handed_to_the_replay() -> None:
    check = _step("plan", "Check inputs")["run"]
    assert '"${BUILD}" =~ ^(default|asan|ubsan)$' in check
    replay = _step("replay", "Replay the merge")["run"]
    assert '--build "${BUILD}"' in replay
    for job in ("plan", "replay"):
        assert _workflow()["jobs"][job]["env"]["BUILD"] == "${{ inputs.build }}"


def test_job_permissions_are_minimal() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {}
    jobs = workflow["jobs"]
    assert jobs["plan"]["permissions"] == {"contents": "read"}
    assert jobs["replay"]["permissions"] == {"contents": "read", "id-token": "write"}
    # actions: read downloads this run's replays.
    assert jobs["triage"]["permissions"] == {"contents": "read", "id-token": "write", "actions": "read"}


def test_inputs_reach_the_scripts_only_through_the_environment() -> None:
    for job in _workflow()["jobs"].values():
        for step in job["steps"]:
            assert "${{" not in step.get("run", ""), step.get("name")


def test_each_job_asks_for_only_the_token_it_needs() -> None:
    expected = {
        "replay": {"permission-actions": "write", "permission-contents": "read", "permission-metadata": "read"},
        "triage": {"permission-actions": "read", "permission-contents": "read", "permission-issues": "write",
                   "permission-metadata": "read"},
    }
    for job, permissions in expected.items():
        app = _step(job, "Generate GitHub App token")
        assert app["if"] == "env.HAS_APP == 'true'"
        assert app["with"]["repositories"] == "valkey-fuzzer"
        assert {k: v for k, v in app["with"].items() if k.startswith("permission-")} == permissions
        # A fork without the App uses its PAT, and says what the App would have been given.
        pat = _step(job, "Use the rehearsal PAT")
        assert pat["if"] == "env.HAS_APP != 'true'"
        scope = re.search(r"\(production\)::repositories: valkey-fuzzer; (.*)'", pat["run"]).group(1)
        assert scope == "; ".join(f"{k}: {v}" for k, v in permissions.items())


def test_the_batches_run_in_parallel_and_the_triage_in_merge_order() -> None:
    jobs = _workflow()["jobs"]
    assert jobs["replay"]["strategy"]["max-parallel"] == "${{ fromJSON(inputs.max_parallel) }}"
    assert jobs["triage"]["needs"] == ["plan", "replay"]
    assert jobs["triage"]["strategy"]["max-parallel"] == "1"
    triage = _step("triage", "Triage each merge's batch, in merge order")["run"]
    assert "python -m scripts.fuzzer.main" in triage and '--pr-batch-run "${run_id}"' in triage


def test_the_plan_takes_a_range_or_the_merges_given() -> None:
    check = _step("plan", "Check inputs")["run"]
    assert "give range or merges, not both" in check
    assert "merges must be 1 to 256 commit SHAs" in check and "sha='[0-9a-f]{7,40}'" in check
    assert '--range "${RANGE}" --merges "${MERGES}"' in _step("plan", "List the merges")["run"]
    assert _workflow()["jobs"]["plan"]["env"]["MERGES"] == "${{ inputs.merges }}"
