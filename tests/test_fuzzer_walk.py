"""Tests for the fuzzer walk: its plan, its state, its filing rules, and the template that turns checks on."""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

import scripts.fuzzer.walk as walk
from scripts.ai.runtime import AgentRunResult
from scripts.fuzzer.propose import Change, ExperimentProposal, Proposal, ProposalRun, _render_scenario, check_scenario
from scripts.fuzzer.walk import (
    CHECKS,
    MAX_ATTEMPTS,
    cli_steps,
    dump_state,
    files,
    findings,
    initial_state,
    log_cause,
    merge_folder,
    no_code_change,
    plan,
    progress,
    record,
    render_issue,
    update,
    verdict,
    with_checks,
)

JSON = "valkey-io/valkey-json"
BLOOM = "valkey-io/valkey-bloom"
NOW = "2026-11-04T12:00:00+00:00"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


def _repo(tmp_path: Path, name: str, commits: list[tuple[str, str]]) -> Path:
    """A repository whose first-parent history is ``commits`` (subject, path it changes), oldest first."""
    repo = tmp_path / name
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    for index, (subject, path) in enumerate(commits):
        _commit(repo, subject, path, index)
    return repo


def _commit(repo: Path, subject: str, path: str, index: int) -> None:
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(f"{index}\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", subject)


def _shas(repo: Path) -> list[str]:
    """Newest first."""
    return _git(repo, "log", "--first-parent", "--format=%H").splitlines()


@pytest.fixture
def json_repo(tmp_path) -> Path:
    return _repo(tmp_path, "valkey-json", [("Initial commit", "README"), ("Add JSON.GET (#1)", "src/a.c"),
                                           ("Fix JSON.SET (#2)", "src/b.c"), ("Document it (#3)", "docs/x.md"),
                                           ("Fix JSON.DEL (#4)", "src/c.c"), ("Fix JSON.ARR (#5)", "src/d.c")])


def _jobs(planned: dict) -> list[int | None]:
    return [m["pr"] for m in planned["merges"]]


def test_the_first_run_starts_at_the_head_and_walks_back_to_the_first_commit(json_repo):
    planned = plan({JSON: json_repo}, initial_state(), 2)
    shas = _shas(json_repo)
    assert _jobs(planned) == [5, 4]
    assert planned["merges"][0] == {"repo": JSON, "sha": shas[0], "parent": shas[1], "pr": 5, "pr_repo": JSON,
                                    "subject": "Fix JSON.ARR (#5)", "date": planned["merges"][0]["date"],
                                    "name": "valkey-json", "deps": ""}
    pointer = planned["repos"][JSON]
    assert (pointer["head"], pointer["cursor"], pointer["end"]) == (shas[0], shas[2], shas[-1])
    assert (pointer["remaining"], pointer["cursor_pr"]) == (3, 3)
    assert planned["heads"] == {JSON: shas[0]}

    # The docs-only merge is done without a job; the backlog stops before the first commit.
    state = update(initial_state(), planned, [], now=NOW)
    state["done"].update({m["sha"]: {"repo": JSON} for m in planned["merges"]})
    state["failed"].clear()
    rest = plan({JSON: json_repo}, state, 10)
    assert _jobs(rest) == [2, 1]
    assert [m["pr"] for m in rest["skipped"]] == [3]
    assert rest["repos"][JSON]["cursor"] == shas[-1] and rest["repos"][JSON]["remaining"] == 0
    assert _jobs(plan({JSON: json_repo}, {**state, "repos": rest["repos"]}, 10)) == []


def test_new_merges_come_first_oldest_first_then_retries_then_the_backlog(json_repo):
    shas = _shas(json_repo)
    state = initial_state()
    state["repos"][JSON] = {"head": shas[0], "cursor": shas[2], "end": shas[-1]}
    retry = {"repo": JSON, "sha": "f" * 40, "parent": "e" * 40, "pr": 9, "pr_repo": JSON, "subject": "s", "date": "d",
             "attempts": 1, "error": "x"}
    state["failed"]["f" * 40] = retry
    _commit(json_repo, "Fix JSON.MSET (#6)", "src/e.c", 6)
    _commit(json_repo, "Fix JSON.TYPE (#7)", "src/f.c", 7)
    planned = plan({JSON: json_repo}, state, 3)
    assert _jobs(planned) == [6, 7, 9]
    # The retry keeps only the merge's own fields.
    assert planned["merges"][2] == {**{k: retry[k] for k in walk.MERGE_KEYS}, "name": "valkey-json", "deps": ""}
    assert planned["repos"][JSON]["head"] == _shas(json_repo)[0]
    assert planned["repos"][JSON]["cursor"] == shas[2]
    # --max bounds the run: the head moves only past the new merges taken.
    first = plan({JSON: json_repo}, state, 1)
    assert _jobs(first) == [6] and first["repos"][JSON]["head"] == _shas(json_repo)[1]


def test_the_repositories_share_the_matrix_in_turn_and_skip_what_is_done(tmp_path, json_repo):
    bloom = _repo(tmp_path, "valkey-bloom", [("Initial commit", "README"), ("Add BF.ADD (#10)", "src/lib.rs"),
                                             ("Add BF.CARD (#11)", "src/card.rs"), ("CI (#12)", ".github/ci.yml")])
    state = initial_state()
    state["repos"][JSON] = {"head": _shas(json_repo)[0], "cursor": _shas(json_repo)[0], "end": _shas(json_repo)[-1]}
    state["done"][_shas(json_repo)[1]] = {"repo": JSON}
    planned = plan({JSON: json_repo, BLOOM: bloom}, state, 4)
    assert [(m["repo"], m["pr"]) for m in planned["merges"]] == [(JSON, 5), (BLOOM, 11), (JSON, 2), (BLOOM, 10)]
    assert [m["pr"] for m in planned["skipped"]] == [12, 3]
    assert planned["merges"][1]["deps"] == "" and planned["merges"][1]["name"] == "valkey-bloom"
    # Only the repositories walked move; the others give their head.
    only = plan({JSON: json_repo, BLOOM: bloom}, state, 4, [BLOOM])
    assert {m["repo"] for m in only["merges"]} == {BLOOM} and set(only["repos"]) == {BLOOM}
    assert set(only["heads"]) == {JSON, BLOOM}


def test_a_pointer_off_the_history_stops_the_plan(json_repo):
    state = initial_state()
    state["repos"][JSON] = {"head": "0" * 40, "cursor": _shas(json_repo)[0], "end": _shas(json_repo)[-1]}
    with pytest.raises(ValueError, match="head of valkey-io/valkey-json"):
        plan({JSON: json_repo}, state, 1)


@pytest.mark.parametrize(("paths", "repo", "expected"), [
    (["README.md", "docs/a.txt", ".github/workflows/ci.yml"], JSON, True),
    (["tests/unit/a.tcl", "integration/test.py"], JSON, True),
    (["tests/modules/datatype.c"], walk.VALKEY_REPO, False),
    (["tests/modules/datatype.c"], JSON, True),
    (["src/server.c", "README.md"], walk.VALKEY_REPO, False),
    (["CMakeLists.txt"], JSON, False),
])
def test_a_merge_of_docs_ci_or_tests_changes_no_code(paths, repo, expected):
    assert no_code_change(paths, repo) is expected


def _merge(**fields) -> dict:
    repo = fields.get("repo", walk.VALKEY_REPO)
    return {"repo": repo, "sha": "b" * 40, "parent": "a" * 40, "pr": 4716, "pr_repo": repo,
            "subject": "Fix LSET (#4716)", "date": "2026-11-01", **fields}


def test_tests_are_kept_by_repository_pr_and_short_sha():
    assert merge_folder(_merge()) == "tests/valkey/4716-bbbbbbbbb"
    assert merge_folder(_merge(repo=BLOOM, pr=None)) == "tests/valkey-bloom/bbbbbbbbb"


def _result(merge: dict, **fields) -> dict:
    return {**merge, "outcome": "tested", "proposed": 3, "proven": 2, "hunted": 2, "recall": True, "cost_usd": 1.5,
            "findings": [{"rule": "crash", "verdict": "introduced by #4716", "issue": "https://x/issues/1"}], **fields}


def test_the_state_marks_done_retries_and_gives_up():
    ok, failed, died = _merge(), _merge(sha="c" * 40, pr=5), _merge(sha="d" * 40, pr=6)
    skipped = _merge(sha="e" * 40, pr=7)
    planned = {"merges": [ok, failed, died], "skipped": [skipped], "repos": {walk.VALKEY_REPO: {"head": "h"}}}
    results = [_result(ok), _result(failed, error="the build failed", cost_usd=0.5)]
    state = update(initial_state(), planned, results, now=NOW, run_url="u")

    assert state["repos"] == {walk.VALKEY_REPO: {"head": "h"}}
    assert state["done"][ok["sha"]]["outcome"] == "tested" and state["done"][ok["sha"]]["folder"] == merge_folder(ok)
    assert state["done"][skipped["sha"]] == {"repo": walk.VALKEY_REPO, "pr": 7, "pr_repo": walk.VALKEY_REPO,
                                             "subject": "Fix LSET (#4716)",
                                             "at": NOW, "outcome": "no-code-change"}
    # A run that failed counts an attempt; a job that left nothing does not.
    assert (state["failed"][failed["sha"]]["attempts"], state["failed"][died["sha"]]["attempts"]) == (1, 0)
    assert state["failed"][died["sha"]]["error"] == "the job left no result"
    assert state["last_run"] == {"at": NOW, "url": "u", "merges": [ok["sha"], failed["sha"], died["sha"]],
                                 "skipped": 1, "cost_usd": 2.0}

    for _ in range(MAX_ATTEMPTS - 1):
        state = update(state, {**planned, "merges": [failed], "skipped": []}, results, now=NOW)
    assert failed["sha"] not in state["failed"]
    assert state["done"][failed["sha"]]["outcome"] == "error"
    assert state["done"][failed["sha"]]["reason"] == "the build failed"
    assert state["done"][failed["sha"]]["cost_usd"] == 1.5
    # A merge whose job never leaves a result is given up too, later.
    for _ in range(walk.MAX_MISSES - 1):
        assert died["sha"] in state["failed"]
        state = update(state, {**planned, "merges": [died], "skipped": []}, [], now=NOW)
    assert died["sha"] not in state["failed"]
    assert (state["done"][died["sha"]]["outcome"], state["done"][died["sha"]]["reason"]) == (
        "error", "the job left no result")

    # One line per merge, and it reads back.
    text = dump_state(state)
    assert json.loads(text) == state
    assert sum(1 for line in text.splitlines() if line.startswith('  "')) == len(state["done"]) + len(state["failed"])


def test_record_keeps_the_tests_of_the_merges_done(tmp_path):
    ok, failed = _merge(), _merge(sha="c" * 40, pr=5)
    results = tmp_path / "results"
    for merge, result in ((ok, _result(ok)), (failed, _result(failed, error="boom"))):
        folder = results / merge_folder(merge)
        (folder / "scenarios").mkdir(parents=True)
        (folder / "scenarios" / "ai-1.yaml").write_text("seed: 1\n")
        (folder / "result.json").write_text(json.dumps(result))
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    planned = {"merges": [ok, failed], "skipped": [], "repos": {}}
    record(state_dir, planned, results, now=datetime(2026, 11, 4, tzinfo=timezone.utc), run_url="u")

    assert (state_dir / "tests/valkey/4716-bbbbbbbbb/scenarios/ai-1.yaml").read_text() == "seed: 1\n"
    assert json.loads((state_dir / "tests/valkey/4716-bbbbbbbbb/result.json").read_text())["proven"] == 2
    assert not (state_dir / merge_folder(failed)).exists()
    state = json.loads((state_dir / "walk-state.json").read_text())
    assert ok["sha"] in state["done"] and failed["sha"] in state["failed"]
    assert ("[valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) "
            "([tests](tests/valkey/4716-bbbbbbbbb/))") in (state_dir / "PROGRESS.md").read_text()


def test_progress_shows_each_repository_the_last_run_totals_and_retries():
    ok, other, failed = _merge(), _merge(sha="c" * 40, pr=5, subject="Add a command (#5)"), _merge(sha="d" * 40, pr=6)
    planned = {"merges": [ok, other, failed], "skipped": [],
               "repos": {walk.VALKEY_REPO: {"head": "h", "cursor": "f" * 40, "end": "0" * 40, "remaining": 40,
                                           "cursor_pr": 1200, "cursor_date": "2025-01-02"}}}
    results = [_result(ok), _result(other, proven=0, recall=False, findings=[], cost_usd=0.25)]
    state = update(initial_state(), planned, results, now=NOW, run_url="https://run")
    text = progress(state, datetime(2026, 11, 4, 18, tzinfo=timezone.utc))

    assert "| valkey-io/valkey | 2 | 41 | #1200 (2025-01-02) | 2 | 0.3 |" in text
    assert "[2026-11-04T12:00:00+00:00](https://run): 3 merge(s) tested" in text
    assert ("- [valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) "
            "([tests](tests/valkey/4716-bbbbbbbbb/)): Fix LSET (#4716): 2 of 3 test(s) reached the "
            "change; [crash, introduced by #4716](https://x/issues/1)") in text
    assert "- [valkey-io/valkey#6](https://github.com/valkey-io/valkey/pull/6): to retry, the job left no result" in text
    assert "- Merges whose tests reached the change: 1 of 2 tested (50%)" in text
    assert "- Recall: 1 parents flagged of 1 fix merges" in text
    assert "- Claude: $1.75 in the last 24 hours, $1.75 in the last 7 days, $1.75 in all" in text
    assert "- Issues filed: 1\n  - https://x/issues/1" in text
    assert "attempt 0 of 3, the job left no result" in text


@pytest.mark.parametrize(("parent", "merge", "unstable", "expected", "filed"), [
    (False, True, True, "introduced by #4716", True),
    (True, True, True, "predates #4716", True),
    (False, False, True, "appeared after #4716", True),
    (False, True, False, "introduced and since fixed", False),
    (True, True, False, "predates #4716, since fixed", False),
    (True, False, False, "fixed by #4716", False),
])
def test_only_what_unstable_shows_is_filed_and_each_build_names_the_verdict(parent, merge, unstable, expected, filed):
    builds = {"parent": parent, "merge": merge, "unstable": unstable}
    assert verdict(builds, "#4716") == expected
    assert files({"builds": builds}) is filed


def _report(*results: dict) -> dict:
    return {"outcome": {"component_results": list(results)}}


def test_findings_are_the_candidates_crashes_and_failed_checks(tmp_path):
    report = _report(
        {"component_id": "seq", "status": "FAIL", "summary": "the replica's digest differs after 12 commands",
         "diagnostics": {"reason": "replica-diverged"}},
        {"component_id": "seq2", "status": "FAIL", "summary": "crashed",
         "diagnostics": {"crashes": {"candidate": "signal 11 in lsetCommand", "parent": "signal 6 in other"}}},
        {"component_id": "seq3", "status": "ERROR", "diagnostics": {"reason": "unexplained-difference"}},
    )
    found = findings(report, tmp_path)
    assert sorted((f["rule"], f["detail"]) for f in found.values()) == [
        ("check", "replica-diverged: the replica's digest differs after 12 commands"),
        ("crash", "crash: signal 11 in lsetCommand"),
    ]
    # Numbers and addresses do not change a signature.
    again = findings(_report({"component_id": "seq", "status": "FAIL", "diagnostics": {"reason": "replica-diverged"},
                              "summary": "the replica's digest differs after 13 commands"}), tmp_path)
    assert set(again) < set(found)


def test_without_a_named_crash_the_candidates_logs_are_scanned(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "7-valkey-0.log").write_text(
        "==9==ERROR: AddressSanitizer: heap-use-after-free on address 0x6020\n"
        "    #0 0x55d in lsetCommand t_list.c:10\n"
        "SUMMARY: AddressSanitizer: heap-use-after-free t_list.c:10 in lsetCommand\n")
    (tmp_path / "parent.server.log").write_text("crashed by signal: 11\n")
    found = list(findings(_report(), tmp_path).values())
    assert [f["detail"] for f in found] == [
        "Sanitizer failure: SUMMARY: AddressSanitizer: heap-use-after-free t_list.c:10 in lsetCommand"]
    assert found[0]["lines"][0].startswith("==9==ERROR: AddressSanitizer")


def test_a_log_names_its_crash_by_summary_assertion_or_signal():
    leak = ("SUMMARY: AddressSanitizer: 64 byte(s) leaked in 1 allocation(s).\n"
            "Direct leak of 64 byte(s)\n    #0 0x1 in malloc\n    #1 0x2 in ztrymalloc_usable\n    #2 0x3 in sdsnew\n")
    assert log_cause(leak) == "SUMMARY: AddressSanitizer: 64 byte(s) leaked in 1 allocation(s). from sdsnew"
    assert log_cause("=== ASSERTION FAILED ===\n==> t_list.c:10 'x' is not true\n") == \
        "assertion failed: ==> t_list.c:10 'x' is not true"
    assert log_cause("valkey crashed by signal: 11, si_code: 1\nEIP:\n./valkey-server *:7000(lsetCommand+0x1a)[0x5]\n") \
        == "crashed by signal: 11 in lsetCommand"
    assert log_cause("nothing\n") is None


EXPERIMENT = """\
id: proposed.lset
deployment: {modes: [standalone]}
components:
  - use: valkey.action.commands
    id: compared
    config:
      compare_with: {$ref: artifacts.parent.binary}
      server_config: {list-max-listpack-size: 4}
      commands:
        - {command: [RPUSH, l, a], expect: {equals: 1}}
        - {command: [LSET, l, 9223372036854775807, x], expect_difference: true, expect: {error: "out of range"}}
  - use: valkey.action.commands
    id: plain
    config:
      commands: [{command: [PING]}]
  - use: valkey.action.commands
    id: module
    config:
      modules: [{$ref: artifacts.module.path}]
      modules_parent: [{$ref: artifacts.module_parent.path}]
      commands: [{command: [PING]}]
required_evidence: [{evidence_id: valkey.command.sequence, source: compared}]
"""


def test_the_template_turns_every_check_on_where_a_sequence_compares_two_builds():
    doc = yaml.safe_load(with_checks(EXPERIMENT))
    assert CHECKS == {"variants": True, "replica": True, "acl": True, "concurrency": 4, "compat": True}
    # A module's two builds load into one Valkey: every check but compat, which compares two Valkey builds.
    module_checks = {"variants": True, "replica": True, "acl": True, "concurrency": 4}
    assert [c["config"].get("checks") for c in doc["components"]] == [CHECKS, None, module_checks]
    assert doc["components"][0]["config"]["compare_with"] == {"$ref": "artifacts.parent.binary"}


def test_a_plain_sequence_replays_as_valkey_cli_steps():
    single = EXPERIMENT.split("  - use: valkey.action.commands\n    id: plain")[0]
    doc = {"experiment": {"id": "proposed.lset"}}
    assert cli_steps(doc, {"proposed.lset.yaml": single}, "") == [
        "valkey-server --port 6379 --list-max-listpack-size 4 &",
        "valkey-cli -p 6379 RPUSH l a",
        "valkey-cli -p 6379 LSET l 9223372036854775807 x",
    ]
    assert cli_steps(doc, {"proposed.lset.yaml": EXPERIMENT}, "") is None  # three sequences
    raw = single.replace("{command: [RPUSH, l, a], expect: {equals: 1}}", "{raw: \"PING\\r\\n\", connection: r}")
    assert cli_steps(doc, {"proposed.lset.yaml": raw}, "") is None
    assert cli_steps({"experiment": {"id": "valkey.forkless-save"}}, {}, "") is None


def test_an_issue_shows_each_build_the_scenario_and_an_upstream_draft():
    finding = {"rule": "crash", "detail": "crash: signal 11 in lsetCommand", "lines": ["crashed by signal: 11"],
               "builds": {"parent": False, "merge": True, "unstable": True}, "verdict": "introduced by #4716"}
    single = EXPERIMENT.split("  - use: valkey.action.commands\n    id: plain")[0]
    scenario = "seed: 1\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n"
    context = {"doc": yaml.safe_load(scenario), "scenario": scenario, "experiment": single,
               "experiments": {"proposed.lset.yaml": single}, "module": None, "unstable": "c" * 40,
               "module_head": "", "tests_url": "https://github.com/o/a/tree/walk-state/tests/valkey/4716-bbbbbbbbb"}
    content = render_issue(finding, _merge(), context)("<!-- m -->", 1)
    assert content.title == "[fuzzer-walk] crash: signal 11 in lsetCommand"
    body = content.body
    assert body.startswith("<!-- m -->\n<!-- valkey-ci-agent:fuzzer-walk:occurrences:1 -->")
    assert "| parent | `aaaaaaaaaaaa` | no |" in body and "| merge, valkey-io/valkey#4716 | `bbbbbbbbbbbb` | yes |" in body
    assert "| latest unstable (on Valkey unstable with ASan) | `cccccccccccc` | yes |" in body
    assert ("Introduced by #4716: [valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) Fix LSET "
            "(#4716) (commit [`bbbbbbbbbbbb`](https://github.com/valkey-io/valkey/commit/" + "b" * 40 + "))") in body
    assert scenario in body and "crashed by signal: 11" in body
    draft = body.split("### Upstream report (draft)")[1]
    assert "**Title:** crash: signal 11 in lsetCommand" in draft
    assert "**Affected builds:** merge, valkey-io/valkey#4716 `bbbbbbbbbbbb`, latest unstable" in draft
    assert "**Since:** introduced by #4716" in draft
    assert "valkey-cli -p 6379 LSET l 9223372036854775807 x" in draft
    assert "4716-bbbbbbbbb" in render_issue(finding, _merge(), context)("<!-- m -->", 2).comment


def test_claudes_cost_is_read_from_its_stream():
    stream = "\n".join([json.dumps({"type": "system"}), json.dumps({"type": "result", "total_cost_usd": 1.25}),
                        "not json", json.dumps({"type": "result", "total_cost_usd": 0.5})])
    assert walk.claude_cost(stream) == 1.75


def test_a_merge_is_proven_hunted_rerun_and_filed_by_the_rule(monkeypatch, tmp_path):
    merge = _merge()
    change = Change(repo=merge["repo"], pr_number=None, title="", base_sha=merge["parent"], head_sha=merge["sha"],
                    diff="diff")
    single = EXPERIMENT.split("  - use: valkey.action.commands\n    id: plain")[0]
    reached, _ = check_scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    missed, _ = check_scenario("seed: 20\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n")
    assert reached is not None and missed is not None

    def propose(change_, *, out_dir, runner, **kw):
        runner("propose", tmp_path)
        run = ProposalRun(change=change_)
        for directory in ("scenarios", "experiments"):
            (out_dir / directory).mkdir(parents=True)
        for index, doc in enumerate((reached, missed), 1):
            (out_dir / "scenarios" / f"{doc['scenario_id']}.yaml").write_text(_render_scenario(doc, change_))
            run.proposals.append(Proposal(index=index, rationale="", text="", scenario_id=doc["scenario_id"],
                                          file=f"scenarios/{doc['scenario_id']}.yaml",
                                          experiment=doc["experiment"]["id"]))
        (out_dir / "experiments" / "proposed.lset.yaml").write_text(single)
        run.experiments.append(ExperimentProposal(index=1, text=single, experiment_id="proposed.lset",
                                                  file="experiments/proposed.lset.yaml"))
        return run

    crash = {"rule": "crash", "detail": "crash: signal 11 in lsetCommand", "lines": []}
    flaky = {"rule": "crash", "detail": "crash: signal 6 in other", "lines": []}
    fixed = {"rule": "check", "detail": "replica-diverged: x", "lines": []}
    calls = []

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        again = run_dir.name.endswith("-again")
        build = run_dir.name.removesuffix("-again").rsplit("-", 2)[1]
        calls.append((doc["scenario_id"], build, seed, again, Path(candidate["server"]).parts[-3]))
        if doc["scenario_id"] == missed["scenario_id"]:
            return {"status": "ERROR", "reason": "unreached-change", "reached": False, "parent_exited": False,
                    "found": {}}
        found = {}
        if build in ("merge", "unstable") and seed == 11:
            found["c"] = crash
        if build == "unstable" and seed == 12 and not again:
            found["f"] = flaky
        if build == "parent" and seed == 10:
            found["p"] = fixed
        return {"status": "PASS" if build != "unstable" else "ERROR", "reached": build == "merge",
                "reason": "unreached-change" if build == "unstable" and seed == 10 else None,
                "parent_exited": False, "found": found}

    monkeypatch.setattr(walk, "fetch", lambda repo, sha, dest: dest)
    monkeypatch.setattr(walk, "build_valkey", lambda src, asan=False: src / "src" / "valkey-server")
    monkeypatch.setattr(walk, "fetch_change", lambda gh, token, request: change)
    monkeypatch.setattr(walk, "load_catalog", lambda fuzzer_bin: {})
    monkeypatch.setattr(walk, "propose", propose)
    monkeypatch.setattr(walk, "fuzz", fuzz)
    publisher = MagicMock()
    publisher.upsert.side_effect = lambda repo, **kw: ("created", f"https://x/issues/{publisher.upsert.call_count}")
    cost = json.dumps({"type": "result", "total_cost_usd": 0.75})
    answer = AgentRunResult(profile="fuzzer_proposal_readonly", stdout=cost, stderr="", returncode=0,
                            prompt_sha256="", cwd="", allowed_tools="", model="", started_at="", finished_at="")

    result = walk.run(merge, gh=MagicMock(), token="t", publisher=publisher, fuzzer_repo="o/valkey-fuzzer",
                      fuzzer_dir=tmp_path, fuzzer_bin="valkey-fuzzer", heads={walk.VALKEY_REPO: "c" * 40},
                      unstable=tmp_path / "unstable", module_head=None, out=tmp_path / "out", work=tmp_path / "work",
                      count=5, tests_url="https://t", runner=lambda prompt, workdir: answer)

    assert "error" not in result
    assert (result["proposed"], result["proven"], result["hunted"], result["dropped"]) == (2, 1, 1, 1)
    assert result["recall"] is True and result["cost_usd"] == 0.75 and result["unstable"] == "c" * 40
    by_detail = {f["detail"]: f for f in result["findings"]}
    assert by_detail[crash["detail"]]["verdict"] == "introduced by #4716"
    assert by_detail[crash["detail"]]["builds"] == {"parent": False, "merge": True, "unstable": True}
    assert by_detail[crash["detail"]]["issue"] == "https://x/issues/1"
    # Not on the rerun: recorded, not filed. On the parent only: the merge fixed it.
    assert by_detail[flaky["detail"]]["verdict"] == "not reproduced" and by_detail[flaky["detail"]]["issue"] is None
    assert by_detail[fixed["detail"]]["verdict"] == "fixed by #4716" and by_detail[fixed["detail"]]["issue"] is None
    regressed = next(f for f in result["findings"] if f["rule"] == "regressed-fix")
    assert regressed["verdict"] == "the fix of #4716 regressed" and regressed["issue"] == "https://x/issues/2"
    assert [c.kwargs["idempotency_key"] for c in publisher.upsert.call_args_list] == [merge["sha"]] * 2
    # Each build runs against its own: the merge against the parent, unstable from the run's build.
    test = reached["scenario_id"]
    assert (test, "merge", 10, False, "merge") in calls and (test, "unstable", 12, False, "unstable") in calls
    assert (test, "unstable", 11, True, "unstable") in calls and (test, "merge", 11, False, "merge") in calls
    folder = tmp_path / "out" / merge_folder(merge)
    assert sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file()) == [
        "experiments/proposed.lset.yaml", "result.json", f"scenarios/{test}.yaml"]
    assert json.loads((folder / "result.json").read_text()) == result


def _workflow() -> dict:
    # BaseLoader keeps the literal `on` key and every scalar as a string.
    return yaml.load(Path(".github/workflows/fuzzer-walk.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_the_workflow_runs_on_a_schedule_and_by_hand_one_run_at_a_time():
    workflow = _workflow()
    assert list(workflow["on"]) == ["schedule", "workflow_dispatch"]
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert list(inputs) == ["repository", "max"] and inputs["max"]["default"] == "20"
    assert inputs["repository"]["options"] == ["all", *walk.REPOSITORIES]
    assert workflow["concurrency"] == {"group": "fuzzer-walk", "cancel-in-progress": "false"}
    assert workflow["permissions"] == {}
    assert workflow["jobs"]["plan"]["if"] == "github.repository_owner != 'valkey-io'"


def test_the_workflow_walks_each_merge_in_its_own_job_and_records_the_run():
    jobs = _workflow()["jobs"]
    assert list(jobs) == ["plan", "heads", "walk", "record"]
    matrix = jobs["walk"]["strategy"]
    assert (matrix["fail-fast"], matrix["max-parallel"]) == ("false", "16")
    assert jobs["walk"]["permissions"] == {"contents": "read", "id-token": "write"}
    step = next(s for s in jobs["walk"]["steps"] if s.get("name") == "Walk the merge")
    # The uploads after it still run when it times out, so the run counts an attempt.
    assert int(step["timeout-minutes"]) < int(jobs["walk"]["timeout-minutes"]) <= 360
    assert "allocator_may_return_null=1:quarantine_size_mb=16" == step["env"]["ASAN_OPTIONS"]
    uploads = {s["with"]["name"] for s in jobs["walk"]["steps"] if "upload-artifact" in s.get("uses", "")}
    assert "walk-test-${{ matrix.merge.sha }}" in uploads
    download = next(s for s in jobs["record"]["steps"] if s.get("name") == "Download the merges' tests and results")
    assert (download["with"]["pattern"], download["with"]["merge-multiple"]) == ("walk-test-*", "true")
    # Only the record job writes, and only to this repository's walk-state branch.
    assert jobs["record"]["permissions"] == {"contents": "write"}
    assert all(job["permissions"].get("contents") == "read" for name, job in jobs.items() if name != "record")
    push = next(s for s in jobs["record"]["steps"] if s.get("name") == "Push the state")["run"]
    assert 'HEAD:walk-state' in push and '"${GITHUB_REPOSITORY}" != valkey-io/*' in push


def test_merges_before_the_fork_name_their_redis_pull_requests(tmp_path):
    repo = _repo(tmp_path, "valkey", [("Base (#12477)", "src/a.c"), ("Redis fix (#13154)", "src/b.c")])
    _commit(repo, "A single commit to get stuff building", "src/c.c", 2)
    fork = _shas(repo)[0]
    _commit(repo, "Fix LSET (#4716)", "src/d.c", 3)
    history = {m["pr"] or m["subject"]: m for m in _with_fork(fork, lambda: walk.first_parent(repo, walk.VALKEY_REPO))}
    assert history[4716]["pr_repo"] == walk.VALKEY_REPO
    assert history["A single commit to get stuff building"]["pr_repo"] == walk.VALKEY_REPO
    assert (history[13154]["pr_repo"], history[12477]["pr_repo"]) == ("redis/redis", "redis/redis")

    redis = _merge(pr=13154, pr_repo="redis/redis", subject="Redis fix (#13154)")
    assert walk._short(redis) == "redis/redis#13154" and walk._short(_merge()) == "#4716"
    finding = {"rule": "crash", "detail": "crash: x", "lines": [], "verdict": "predates redis/redis#13154",
               "builds": {"parent": True, "merge": True, "unstable": True}}
    scenario = "seed: 1\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n"
    context = {"doc": yaml.safe_load(scenario), "scenario": scenario, "experiment": "", "experiments": {},
               "module": None, "unstable": "c" * 40, "module_head": "", "tests_url": "u"}
    body = render_issue(finding, redis, context)("<!-- m -->", 1).body
    assert "Predates redis/redis#13154: [redis/redis#13154](https://github.com/redis/redis/pull/13154)" in body
    assert "| merge, redis/redis#13154 |" in body
    state = update(initial_state(), {"merges": [redis], "skipped": [], "repos": {}}, [_result(redis)], now=NOW)
    assert "- [redis/redis#13154](https://github.com/redis/redis/pull/13154) ([tests](tests/valkey/13154-bbbbbbbbb/))" \
        in progress(state, datetime(2026, 11, 4, 18, tzinfo=timezone.utc))


def _with_fork(fork, call):
    saved = walk.VALKEY_FORK
    walk.VALKEY_FORK = fork
    try:
        return call()
    finally:
        walk.VALKEY_FORK = saved
