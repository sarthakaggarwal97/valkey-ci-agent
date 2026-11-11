"""Tests for the fuzzer walk: its plan, its state, its filing rules, and the template that turns checks on."""
from __future__ import annotations

import dataclasses
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


UNSTABLE = "c" * 40


def test_the_first_run_starts_at_the_head_and_walks_back_to_the_first_commit(json_repo):
    planned = plan(json_repo, JSON, initial_state(JSON), 2, UNSTABLE, now=NOW)
    shas = _shas(json_repo)
    assert _jobs(planned) == [5, 4] and planned["repo"] == JSON
    assert planned["merges"][0] == {"repo": JSON, "sha": shas[0], "parent": shas[1], "pr": 5, "pr_repo": JSON,
                                    "subject": "Fix JSON.ARR (#5)", "date": planned["merges"][0]["date"],
                                    "name": "valkey-json", "deps": ""}
    pointer = planned["pointer"]
    assert (pointer["head"], pointer["cursor"], pointer["end"]) == (shas[0], shas[2], shas[-1])
    assert (pointer["remaining"], pointer["cursor_pr"]) == (3, 3)
    # A module's merges load into Valkey unstable: the run builds both heads.
    assert planned["heads"] == {walk.VALKEY_REPO: UNSTABLE, JSON: shas[0]}

    # The docs-only merge is done without a job; the backlog stops before the first commit.
    state = update(initial_state(JSON), planned, [], now=NOW)
    state["done"].update({m["sha"]: {"repo": JSON} for m in planned["merges"]})
    state["failed"].clear()
    rest = plan(json_repo, JSON, state, 10, UNSTABLE, now=NOW)
    assert _jobs(rest) == [2, 1]
    assert [m["pr"] for m in rest["skipped"]] == [3]
    assert rest["pointer"]["cursor"] == shas[-1] and rest["pointer"]["remaining"] == 0
    assert _jobs(plan(json_repo, JSON, {**state, "pointer": rest["pointer"]}, 10, UNSTABLE, now=NOW)) == []


def test_new_merges_come_first_oldest_first_then_retries_then_the_backlog(json_repo):
    shas = _shas(json_repo)
    state = initial_state(JSON)
    state["pointer"] = {"head": shas[0], "cursor": shas[2], "end": shas[-1]}
    retry = {"repo": JSON, "sha": "f" * 40, "parent": "e" * 40, "pr": 9, "pr_repo": JSON, "subject": "s", "date": "d",
             "attempts": 1, "error": "x"}
    state["failed"]["f" * 40] = retry
    state["done"][shas[3]] = {"repo": JSON}
    _commit(json_repo, "Fix JSON.MSET (#6)", "src/e.c", 6)
    _commit(json_repo, "Fix JSON.TYPE (#7)", "src/f.c", 7)
    planned = plan(json_repo, JSON, state, 4, UNSTABLE, now=NOW)
    # In the backlog, the docs-only #3 is skipped and the done #2 passed over.
    assert _jobs(planned) == [6, 7, 9, 1]
    assert [m["pr"] for m in planned["skipped"]] == [3]
    # The retry keeps only the merge's own fields.
    assert planned["merges"][2] == {**{k: retry[k] for k in walk.MERGE_KEYS}, "name": "valkey-json", "deps": ""}
    assert planned["pointer"]["head"] == _shas(json_repo)[0]
    # --max bounds the run: the head moves only past the new merges taken.
    first = plan(json_repo, JSON, state, 1, UNSTABLE, now=NOW)
    assert _jobs(first) == [6] and first["pointer"]["head"] == _shas(json_repo)[1]
    assert first["pointer"]["cursor"] == shas[2]


def test_a_valkey_run_builds_only_valkeys_head(tmp_path):
    repo = _repo(tmp_path, "valkey", [("Base", "src/a.c"), ("Fix (#1)", "src/b.c")])
    planned = _with_base(_shas(repo)[-1], lambda: plan(repo, walk.VALKEY_REPO, initial_state(walk.VALKEY_REPO), 5, "", now=NOW))
    assert planned["heads"] == {walk.VALKEY_REPO: _shas(repo)[0]}
    assert _jobs(planned) == [1] and planned["merges"][0]["name"] == "valkey"


def _with_base(base, call):
    saved = walk.VALKEY_BASE
    walk.VALKEY_BASE = base
    try:
        return call()
    finally:
        walk.VALKEY_BASE = saved


def test_a_modules_first_code_drop_on_a_readme_stub_is_skipped(tmp_path):
    # valkey-bloom 5237a100 ("Initial commit") and valkey-json #1: the parent is a README-only stub, so there is
    # nothing to build or compare with; the merge is done without a job, as no-parent-code.
    repo = _repo(tmp_path, "valkey-bloom", [("Initial stub", "README.md"), ("Initial commit", "src/lib.rs"),
                                            ("Fix BF.ADD (#2)", "src/lib.rs")])
    planned = plan(repo, BLOOM, initial_state(BLOOM), 5, UNSTABLE, now=NOW)
    assert _jobs(planned) == [2]
    assert [m["subject"] for m in planned["skipped"]] == ["Initial commit"]
    assert planned["skipped"][0]["skip_outcome"] == "no-parent-code"
    state = update(initial_state(BLOOM), planned, [], now=NOW)
    assert state["done"][planned["skipped"][0]["sha"]]["outcome"] == "no-parent-code"


def test_a_merge_predating_the_build_recipe_is_skipped(tmp_path, monkeypatch):
    # valkey-search before #33 (0805ebc) builds only with Bazel: the recipe needs build.sh, so those merges (and
    # #33 itself, whose parent lacks it) are done without a job instead of failing the build until given up.
    search = "valkey-io/valkey-search"
    repo = _repo(tmp_path, "valkey-search", [("Initial commit", "src/a.cc"), ("Bazel fix (#2)", "src/b.cc"),
                                             ("CMake support (#3)", "build.sh"), ("Fix FT.SEARCH (#4)", "src/c.cc")])
    monkeypatch.setitem(walk.MODULES, "valkey-search",
                        dataclasses.replace(walk.MODULES["valkey-search"], buildable="build.sh"))
    planned = plan(repo, search, initial_state(search), 5, UNSTABLE, now=NOW)
    assert _jobs(planned) == [4]
    assert [(m["pr"], m["skip_outcome"]) for m in planned["skipped"] if m.get("skip_outcome")] == [
        (3, "predates-build-recipe"), (2, "predates-build-recipe")]
    state = update(initial_state(search), planned, [], now=NOW)
    assert state["done"][planned["skipped"][0]["sha"]]["outcome"] == "predates-build-recipe"

    # A merge already waiting to be retried (its builds failed before the recipe learnt this) is skipped too,
    # and leaves the retries.
    shas = _shas(repo)
    state = initial_state(search)
    state["pointer"] = {"head": shas[0], "cursor": shas[-1], "end": shas[-1]}
    state["failed"][shas[2]] = {**{k: None for k in walk.MERGE_KEYS}, "repo": search, "sha": shas[2],
                                "parent": shas[3], "attempts": 2, "error": "build.sh: No such file or directory"}
    planned = plan(repo, search, state, 5, UNSTABLE, now=NOW)
    assert _jobs(planned) == []
    assert [m["skip_outcome"] for m in planned["skipped"]] == ["predates-build-recipe"]
    state = update(state, planned, [], now=NOW)
    assert shas[2] not in state["failed"]
    assert state["done"][shas[2]]["outcome"] == "predates-build-recipe"


def test_a_pointer_off_the_history_stops_the_plan(json_repo):
    state = initial_state(JSON)
    state["pointer"] = {"head": "0" * 40, "cursor": _shas(json_repo)[0], "end": _shas(json_repo)[-1]}
    with pytest.raises(ValueError, match="head of valkey-io/valkey-json"):
        plan(json_repo, JSON, state, 1, UNSTABLE, now=NOW)


@pytest.mark.parametrize(("paths", "repo", "expected"), [
    (["README.md", "docs/a.txt", ".github/workflows/ci.yml"], JSON, True),
    (["LICENSE"], JSON, True),
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
    planned = {"repo": walk.VALKEY_REPO, "merges": [ok, failed, died], "skipped": [skipped], "pointer": {"head": "h"}}
    results = [_result(ok), _result(failed, error="the build failed", cost_usd=0.5)]
    state = update(initial_state(walk.VALKEY_REPO), planned, results, now=NOW, run_url="u")

    assert state["repo"] == walk.VALKEY_REPO and state["pointer"] == {"head": "h"}
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


REFUSED = "Claude Code failed (rc=1): API Error: ... appears to violate our Usage Policy"


def test_a_refused_merge_is_retried_a_day_later_without_counting_an_attempt():
    # valkey #4214: the API refused the request; the same request may be answered another time.
    refused = _merge(sha="c" * 40, pr=4214)
    planned = {"repo": walk.VALKEY_REPO, "merges": [refused], "skipped": [], "pointer": {"head": "h"}}
    state = initial_state(walk.VALKEY_REPO)
    for refusals in range(1, walk.MAX_REFUSALS):
        state = update(state, planned, [_result(refused, error=REFUSED, cost_usd=4.0)], now=NOW)
        entry = state["failed"][refused["sha"]]
        assert (entry["attempts"], entry["refusals"]) == (0, refusals)
        assert entry["retry_after"] == "2026-11-05T12:00:00+00:00"
    state = update(state, planned, [_result(refused, error=REFUSED, cost_usd=4.0)], now=NOW)
    assert refused["sha"] not in state["failed"]
    assert state["done"][refused["sha"]]["reason"] == REFUSED


def test_a_merge_waiting_to_be_retried_leaves_the_run_to_others(json_repo):
    shas = _shas(json_repo)
    state = initial_state(JSON)
    state["pointer"] = {"head": shas[0], "cursor": shas[0], "end": shas[-1]}
    fields = {"repo": JSON, "parent": "e" * 40, "pr_repo": JSON, "subject": "s", "date": "d", "attempts": 0}
    state["failed"]["f" * 40] = {**fields, "sha": "f" * 40, "pr": 9, "retry_after": "2026-11-05T12:00:00+00:00"}
    state["failed"]["9" * 40] = {**fields, "sha": "9" * 40, "pr": 8, "retry_after": "2026-11-04T11:00:00+00:00"}

    assert _jobs(plan(json_repo, JSON, state, 2, UNSTABLE, now=NOW)) == [8, 5]
    assert _jobs(plan(json_repo, JSON, state, 2, UNSTABLE, now="2026-11-05T12:00:00+00:00")) == [9, 8]


def _results(tmp_path: Path, *pairs: tuple[dict, dict]) -> Path:
    """The jobs' uploads, merged: each merge's tests folder with a scenario and its result.json."""
    results = tmp_path / "results"
    for merge, result in pairs:
        folder = results / merge_folder(merge)
        (folder / "scenarios").mkdir(parents=True)
        (folder / "scenarios" / "ai-1.yaml").write_text("seed: 1\n")
        (folder / "result.json").write_text(json.dumps(result))
    return results


def test_record_keeps_one_state_per_repository_and_the_tests_of_the_merges_done(tmp_path):
    ok, failed = _merge(), _merge(sha="c" * 40, pr=5)
    results = _results(tmp_path, (ok, _result(ok)), (failed, _result(failed, error="boom")))
    branch = tmp_path / "branch"
    # Another repository's run recorded first.
    json_state = update(initial_state(JSON), {"repo": JSON, "merges": [], "skipped": [_merge(repo=JSON, pr=3)],
                                              "pointer": {"cursor": "f" * 40}}, [], now=NOW)
    (branch / "state").mkdir(parents=True)
    (branch / "state" / "valkey-json.json").write_text(dump_state(json_state))
    planned = {"repo": walk.VALKEY_REPO, "merges": [ok, failed], "skipped": [], "pointer": {"cursor": "f" * 40}}
    record(branch, planned, results, now=datetime(2026, 11, 4, tzinfo=timezone.utc), run_url="u")

    assert (branch / "tests/valkey/4716-bbbbbbbbb/scenarios/ai-1.yaml").read_text() == "seed: 1\n"
    assert json.loads((branch / "tests/valkey/4716-bbbbbbbbb/result.json").read_text())["proven"] == 2
    assert not (branch / merge_folder(failed)).exists()
    state = json.loads((branch / "state/valkey.json").read_text())
    assert state["repo"] == walk.VALKEY_REPO
    assert ok["sha"] in state["done"] and failed["sha"] in state["failed"]
    # The other repository's state is untouched, and PROGRESS.md covers both.
    assert json.loads((branch / "state/valkey-json.json").read_text()) == json_state
    text = (branch / "PROGRESS.md").read_text()
    assert ("[valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) "
            "([tests](tests/valkey/4716-bbbbbbbbb/))") in text
    assert "| valkey-io/valkey | 1 |" in text and "| valkey-io/valkey-json | 1 |" in text


def test_progress_shows_each_repository_the_last_runs_totals_and_retries():
    ok, other, failed = _merge(), _merge(sha="c" * 40, pr=5, subject="Add a command (#5)"), _merge(sha="d" * 40, pr=6)
    planned = {"repo": walk.VALKEY_REPO, "merges": [ok, other, failed], "skipped": [],
               "pointer": {"head": "h", "cursor": "f" * 40, "end": "0" * 40, "remaining": 40, "cursor_pr": 1200,
                           "cursor_date": "2025-01-02"}}
    results = [_result(ok), _result(other, proven=0, recall=False, findings=[], cost_usd=0.25)]
    state = update(initial_state(walk.VALKEY_REPO), planned, results, now=NOW, run_url="https://run")
    bloom_merge = _merge(repo=BLOOM, sha="e" * 40, pr=11, subject="Fix BF.ADD (#11)")
    bloom = update(initial_state(BLOOM), {"repo": BLOOM, "merges": [bloom_merge], "skipped": [],
                                          "pointer": {"cursor": "a" * 40, "remaining": 0}},
                   [_result(bloom_merge, cost_usd=0.5)], now=NOW, run_url="https://bloom-run")
    text = progress([state, bloom], datetime(2026, 11, 4, 18, tzinfo=timezone.utc))

    assert "| valkey-io/valkey | 2 | 41 | #1200 (2025-01-02) | 2 | 0.3 |" in text
    assert "| valkey-io/valkey-bloom | 1 | 0 | aaaaaaaaaaaa (?) | 1 | 0.1 |" in text
    # Each repository's last run under its own heading, in repository order.
    assert text.index("### valkey-io/valkey\n") < text.index("### valkey-io/valkey-bloom\n")
    assert "[2026-11-04T12:00:00+00:00](https://run): 3 merge(s) tested" in text
    assert "[2026-11-04T12:00:00+00:00](https://bloom-run): 1 merge(s) tested" in text
    assert "- Merges whose tests reached the change: 2 of 3 tested (67%)" in text
    assert "- Claude: $2.25 in the last 24 hours, $2.25 in the last 7 days, $2.25 in all" in text
    assert ("- [valkey-io/valkey#4716](https://github.com/valkey-io/valkey/pull/4716) "
            "([tests](tests/valkey/4716-bbbbbbbbb/)): Fix LSET (#4716): 2 of 3 test(s) reached the "
            "change; [crash, introduced by #4716](https://x/issues/1)") in text
    assert "- [valkey-io/valkey#6](https://github.com/valkey-io/valkey/pull/6): to retry, the job left no result" in text
    assert "- Recall: 2 parents flagged of 2 fix merges" in text
    assert "- Issues filed: 1\n  - https://x/issues/1" in text
    assert "attempt 0 of 3, the job left no result" in text


def _write(name: str, text: str):
    def apply(branch: Path) -> None:
        (branch / "state").mkdir(exist_ok=True)
        (branch / "state" / name).write_text(text)
    return apply


def _branch_files(remote: Path) -> list[str]:
    return _git(remote, "ls-tree", "-r", "--name-only", walk.STATE_BRANCH).splitlines()


def test_publish_creates_the_branch_and_starts_again_when_another_run_got_there_first(tmp_path):
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    walk.publish(str(remote), tmp_path / "first", _write("valkey.json", "{}"), "first")
    assert _branch_files(remote) == ["state/valkey.json"]

    seen = []

    def racing(branch: Path) -> None:
        seen.append(sorted(p.name for p in (branch / "state").iterdir()))
        if len(seen) == 1:
            # Another repository's run pushes between this one's fetch and its push.
            walk.publish(str(remote), tmp_path / "other", _write("valkey-json.json", "{}"), "other")
        _write("valkey-bloom.json", "{}")(branch)

    walk.publish(str(remote), tmp_path / "bloom", racing, "bloom")
    # Applied again on the new fetch, so nothing the other run pushed is lost.
    assert seen == [["valkey.json"], ["valkey-json.json", "valkey.json"]]
    assert _branch_files(remote) == ["state/valkey-bloom.json", "state/valkey-json.json", "state/valkey.json"]
    assert _git(remote, "log", "--format=%s", walk.STATE_BRANCH).splitlines() == ["bloom", "other", "first"]


def test_publish_gives_up_after_its_retries(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    walk.publish(str(remote), tmp_path / "first", _write("valkey.json", "{}"), "first")
    monkeypatch.setattr(walk, "MAX_PUSHES", 2)
    count = []

    def always_beaten(branch: Path) -> None:
        count.append(1)
        walk.publish(str(remote), tmp_path / f"other{len(count)}", _write(f"x{len(count)}.json", "{}"), "other")
        _write("valkey-bloom.json", "{}")(branch)

    with pytest.raises(RuntimeError, match="rejected 2 pushes"):
        walk.publish(str(remote), tmp_path / "bloom", always_beaten, "bloom")
    assert len(count) == 2 and "state/valkey-bloom.json" not in _branch_files(remote)


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


def test_a_sanitizer_finding_on_unstable_alone_names_no_merge():
    # Only unstable is built with ASan: fuzzer#55 read "appeared after #5153" for a bug older than 7.2.
    builds = {"parent": False, "merge": False, "unstable": True}
    assert verdict(builds, "#5153", sanitizer=True) == "shown by ASan on unstable, the one build with it"
    assert verdict({**builds, "merge": True}, "#5153", sanitizer=True) == "introduced by #5153"


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
    assert log_cause(leak) == "SUMMARY: AddressSanitizer: leak from sdsnew"
    # As in valkey-json's: past the library frames to the module's own, and the same however much leaked.
    json = ("SUMMARY: AddressSanitizer: 4797 byte(s) leaked in 36 allocation(s).\n    #0 0x1 in malloc\n"
            "    #1 0x2 in dom_alloc(unsigned long)\n    #2 0x3 in rapidjson::GenericMember<X>* rapidjson::Realloc<Y>\n"
            "    #3 0x4 in merge_values(rapidjson::GenericValue<X> const&)\n")
    assert log_cause(json) == "SUMMARY: AddressSanitizer: leak from merge_values"
    assert log_cause(json.replace("4797", "404")) == log_cause(json)
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
    assert CHECKS == {
        "variants": True, "replica": True, "acl": True, "concurrency": 4, "compat": True, "faults": True}
    # A module's two builds load into one Valkey: every check but compat, which compares two Valkey builds.
    module_checks = {"variants": True, "replica": True, "acl": True, "concurrency": 4, "faults": True}
    assert [c["config"].get("checks") for c in doc["components"]] == [CHECKS, None, module_checks]
    assert doc["components"][0]["config"]["compare_with"] == {"$ref": "artifacts.parent.binary"}


def test_a_valkey_test_module_runs_no_concurrency_or_replica_check():
    # valkey #5159: blockedclient's postponed_unmatched_free frees its one global context; four clients freed it twice.
    # valkey #3911: basics' test.call_argv_s writes without replicating, and the replica diverged (fuzzer#60).
    doc = yaml.safe_load(with_checks(EXPERIMENT, test_module=True))
    checks = [c["config"].get("checks") for c in doc["components"]]
    assert [c is None or "concurrency" in c or "replica" in c or "faults" in c for c in checks] == [False, True, False]
    assert checks[0] == {key: value for key, value in CHECKS.items() if key not in ("concurrency", "replica", "faults")}


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


def test_a_reply_the_client_cannot_parse_is_filed_by_the_rule_with_its_raw_bytes(tmp_path):
    report = _report({"component_id": "seq", "status": "FAIL", "summary": "command 3 (GET k): unparsable reply",
                      "diagnostics": {"reason": "protocol-error", "command": 3, "raw": "$5\\r\\nab\\r\\n"}})
    (fingerprint, finding), = findings(report, tmp_path).items()
    assert (finding["rule"], finding["detail"]) == ("check", "protocol-error: command 3 (GET k): unparsable reply")
    finding.update(builds={"parent": False, "merge": True, "unstable": True}, verdict="introduced by #4716")
    assert files(finding)
    scenario = "seed: 1\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n"
    context = {"doc": yaml.safe_load(scenario), "scenario": scenario, "experiment": "", "experiments": {},
               "module": None, "unstable": "c" * 40, "module_head": "", "tests_url": "https://t"}
    body = render_issue(finding, _merge(), context)("<!-- m -->", 1).body
    assert '"raw": "$5\\\\r\\\\nab\\\\r\\\\n"' in body.split("Key log lines:")[1]


def test_claudes_cost_is_read_from_its_stream():
    stream = "\n".join([json.dumps({"type": "system"}), json.dumps({"type": "result", "total_cost_usd": 1.25}),
                        "not json", json.dumps({"type": "result", "total_cost_usd": 0.5})])
    assert walk.claude_cost(stream) == 1.75


SINGLE = EXPERIMENT.split("  - use: valkey.action.commands\n    id: plain")[0]


def _scenario(text: str) -> dict:
    doc, _ = check_scenario(text)
    assert doc is not None
    return doc


def _walk_merge(monkeypatch, tmp_path, fuzz, *rounds, merge=None, diff="diff", extras=None, counts=None,
                build=None, runner=None):
    """walk.run on one merge with fakes: Claude proposes each round's docs (using proposed.lset), the last again when
    asked more often, and ``fuzz`` runs them. ``extras`` collects the extra section of each round's prompt."""
    merge = merge or _merge()
    change = Change(repo=merge["repo"], pr_number=None, title="", base_sha=merge["parent"], head_sha=merge["sha"],
                    diff=diff)
    asked = [] if extras is None else extras

    def propose(change_, *, out_dir, runner, count, extra="", **kw):
        runner("propose", tmp_path)
        asked.append(extra)
        docs = rounds[min(len(asked), len(rounds)) - 1]
        run = ProposalRun(change=change_)
        for directory in ("scenarios", "experiments"):
            (out_dir / directory).mkdir(parents=True)
        for index, doc in enumerate(docs, 1):
            (out_dir / "scenarios" / f"{doc['scenario_id']}.yaml").write_text(_render_scenario(doc, change_))
            run.proposals.append(Proposal(index=index, rationale="", text="", scenario_id=doc["scenario_id"],
                                          file=f"scenarios/{doc['scenario_id']}.yaml",
                                          experiment=doc["experiment"]["id"]))
        (out_dir / "experiments" / "proposed.lset.yaml").write_text(SINGLE)
        run.experiments.append(ExperimentProposal(index=1, text=SINGLE, experiment_id="proposed.lset",
                                                  file="experiments/proposed.lset.yaml"))
        return run

    monkeypatch.setattr(walk, "fetch", lambda repo, sha, dest: dest)
    monkeypatch.setattr(walk, "build_valkey", build or (lambda src, asan=False, coverage=False: src / "src" / "valkey-server"))
    monkeypatch.setattr(walk, "gcov", counts or (lambda tree, path: None))
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
                      tests_url="https://t", runner=runner or (lambda prompt, workdir: answer))
    return result, publisher


def _build(run_dir: Path) -> str:
    return run_dir.name.removesuffix("-again").rsplit("-", 2)[1]


def test_a_merge_is_proven_hunted_rerun_and_filed_by_the_rule(monkeypatch, tmp_path):
    merge = _merge()
    reached = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    missed = _scenario("seed: 20\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n")
    crash = {"rule": "crash", "detail": "crash: signal 11 in lsetCommand", "lines": []}
    flaky = {"rule": "crash", "detail": "crash: signal 6 in other", "lines": []}
    fixed = {"rule": "check", "detail": "replica-diverged: x", "lines": []}
    # fuzzer#94: a tool's crash names ASan without the log's SUMMARY line.
    asan = {"rule": "crash", "detail": "crash: AddressSanitizer: SEGV", "lines": []}
    calls, dirs = [], []

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        again = run_dir.name.endswith("-again")
        build = _build(run_dir)
        calls.append((doc["scenario_id"], build, seed, again, Path(candidate["server"]).parts[-3]))
        dirs.append((seed == doc["seed"], experiment_dir.name))
        if doc["scenario_id"] == missed["scenario_id"]:
            return {"status": "ERROR", "reason": "unreached-change", "reached": False, "parent_exited": False,
                    "reached_commands": set(), "found": {}}
        found = {}
        if build in ("merge", "unstable") and seed == 11:
            found["c"] = crash
        if build == "unstable" and seed == 12 and not again:
            found["f"] = flaky
        if build == "parent" and seed == 10:
            found["p"] = fixed
        if build == "unstable" and seed == 12:
            found["a"] = asan
        return {"status": "PASS" if build != "unstable" else "ERROR", "reached": build == "merge",
                "reason": "unreached-change" if build == "unstable" and seed == 10 else None,
                "reached_commands": {("lset", 1), ("lset", 2)} if build == "merge" else {("lset", 2)},
                "parent_exited": False, "found": found}

    result, publisher = _walk_merge(monkeypatch, tmp_path, fuzz, (reached, missed), merge=merge)

    assert "error" not in result
    assert (result["proposed"], result["proven"], result["hunted"], result["dropped"]) == (2, 1, 2, 0)
    assert result["recall"] is True and result["cost_usd"] == 0.75 and result["unstable"] == "c" * 40
    by_detail = {f["detail"]: f for f in result["findings"]}
    assert by_detail[crash["detail"]]["verdict"] == "introduced by #4716"
    assert by_detail[crash["detail"]]["builds"] == {"parent": False, "merge": True, "unstable": True}
    assert by_detail[crash["detail"]]["issue"] == "https://x/issues/1"
    # Not on the rerun: recorded, not filed. On the parent only: the merge fixed it.
    assert by_detail[flaky["detail"]]["verdict"] == "not reproduced" and by_detail[flaky["detail"]]["issue"] is None
    assert by_detail[fixed["detail"]]["verdict"] == "fixed by #4716" and by_detail[fixed["detail"]]["issue"] is None
    # On unstable alone, the one build with ASan: not when it appeared.
    assert by_detail[asan["detail"]]["verdict"] == "shown by ASan on unstable, the one build with it"
    assert by_detail[asan["detail"]]["issue"] == "https://x/issues/2"
    # Command 1 reached the change at the merge and no longer does on unstable.
    regressed = next(f for f in result["findings"] if f["rule"] == "regressed-fix")
    assert regressed["verdict"] == "the fix of #4716 regressed" and regressed["issue"] == "https://x/issues/3"
    assert regressed["detail"].endswith("(commands lset 1)")
    assert [c.kwargs["idempotency_key"] for c in publisher.upsert.call_args_list] == [merge["sha"]] * 3
    # Each build runs against its own: the merge against the parent, unstable from the run's build.
    test = reached["scenario_id"]
    assert (test, "merge", 10, False, "merge") in calls and (test, "unstable", 12, False, "unstable") in calls
    # The checks run at the test's own seed; the hunt's further seeds run without them.
    assert set(dirs) == {(True, "experiments"), (False, "experiments-plain")}
    assert (test, "unstable", 11, True, "unstable") in calls and (test, "merge", 11, False, "merge") in calls
    folder = tmp_path / "out" / merge_folder(merge)
    # The missed test ran to its end, so it hunts and is kept, though it proves nothing.
    assert sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file()) == sorted([
        "experiments/proposed.lset.yaml", "result.json", f"scenarios/{test}.yaml",
        f"scenarios/{missed['scenario_id']}.yaml"])
    assert json.loads((folder / "result.json").read_text()) == result


def test_claude_cut_off_by_its_timeout_answers_in_the_same_session(monkeypatch, tmp_path):
    # valkey-search #1650: Claude read the change for 50 minutes, timed out, and everything it read was lost.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    prompts = []

    def runner(prompt, workdir):
        prompts.append(prompt)
        cut = len(prompts) == 1
        stream = [json.dumps({"type": "system", "subtype": "init", "session_id": "s-1"})]
        if not cut:
            stream.append(json.dumps({"type": "result", "total_cost_usd": 0.5}))
        return AgentRunResult(profile="fuzzer_proposal_readonly", stdout="\n".join(stream),
                              stderr="timeout after 3000s" if cut else "", returncode=int(cut), prompt_sha256="",
                              cwd="", allowed_tools="", model="", started_at="", finished_at="")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "PASS", "reason": None, "reached": True, "parent_exited": False,
                "reached_commands": set(), "found": {}}

    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (test,), runner=runner)
    assert prompts[:2] == ["propose", walk.TIME_UP_PROMPT]
    assert result["cost_usd"] >= 0.5


def test_a_test_whose_expectation_both_builds_violate_hunts_and_keeps_what_it_expected(monkeypatch, tmp_path):
    # valkey-bloom #110 (fuzzer#69): both builds broke a correct expectation, BF.CARD at least 140, a bug older
    # than the merge; the test was dropped unseen, and never ran on unstable.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    summary = "expectations both builds violate alike, so they are not about the change: 34 (BF.CARD k9)"
    ran = []

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        ran.append(_build(run_dir))
        return {"status": "ERROR", "reason": "expect-disputed", "reached": False, "parent_exited": False,
                "reached_commands": set(), "found": {}, "summary": summary}

    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (test,))
    assert (result["hunted"], result["dropped"]) == (1, 0) and "unstable" in ran
    assert {"scenario": test["scenario_id"], "build": "merge", "reason": "expect-disputed",
            "summary": summary} in result["recorded"]


def test_a_test_skipped_as_unsupported_is_dropped_and_its_reason_shown(monkeypatch, tmp_path):
    # valkey #5110: the forkless experiments set a config current Valkey renamed, and every test skipped unseen.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    summary = "required capability 'build' is unsupported"

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "SKIP_UNSUPPORTED", "reason": None, "reached": False, "parent_exited": False,
                "reached_commands": set(), "found": {}, "summary": summary}

    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (test,))
    assert (result["hunted"], result["dropped"]) == (0, 1)
    assert walk._summary(_merge(), result, "now")["skipped"] == {"count": 1, "reason": summary}


def test_a_skip_reasons_rejected_directive_survives_in_the_summary():
    # valkey #2777/#2986: the rejected config sits at the tail of the FATAL CONFIG error, past the capability
    # prefix, so a 200-char cut dropped the ">>> 'name'" and PROGRESS.md read uselessly "...at line ".
    reason = (
        "required capability 'save-behavior-support' is unsupported: default: build rejects startup "
        "configuration: *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** Reading the configuration file, "
        "at line 11 >>> 'cluster-config-save-behavior sync' Bad directive or wrong number of arguments"
    )
    result = {"recorded": [{"scenario": "s", "status": "SKIP_UNSUPPORTED", "reason": reason}]}
    assert "cluster-config-save-behavior" in walk._summary(_merge(), result, "now")["skipped"]["reason"]


def test_a_skip_reason_led_by_a_long_log_path_keeps_its_directive():
    # valkey #1252: a provision error leads with the instance's log path, so a plain [:300] stopped inside it
    # and the rejected directive at the tail was lost; the head and the tail are both kept.
    reason = (
        "the run could not start: runner execution failed during deployment.provision: LocalDeploymentError: "
        "Instance valkey-0 failed to start within 60s (process output from /home/runner/work/valkey-ci-agent/"
        "valkey-ci-agent/walk-work/runs/ai-d5812612ef67-merge-7101/artifacts/data/logs/7101-dae728eb-valkey-0"
        ".stdio.log: | *** FATAL CONFIG FILE ERROR (Version 255.255.255) *** | Reading the configuration file, "
        "at line 18 | >>> 'cluster-replica-no-failover \"if-empty\"' | argument must be 'yes' or 'no')"
    )
    result = {"recorded": [{"scenario": "s", "status": "SKIP_UNSUPPORTED", "reason": reason}]}
    kept = walk._summary(_merge(), result, "now")["skipped"]["reason"]
    assert "cluster-replica-no-failover" in kept and len(kept) <= 300


def test_the_same_commands_unreached_on_both_builds_is_no_regressed_fix(monkeypatch, tmp_path):
    # valkey-json #123: its test reached the change on 24 commands at the merge and on unstable, with the same few
    # marked commands unreached on both, and was filed as a regressed fix.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "ERROR", "reason": "unreached-change", "reached": True, "parent_exited": False,
                "reached_commands": {("compared", index) for index in range(24)}, "found": {}}

    result, publisher = _walk_merge(monkeypatch, tmp_path, fuzz, (test,))
    assert result["proven"] == 1 and result["findings"] == []
    publisher.upsert.assert_not_called()


def test_a_change_that_still_replies_as_at_the_merge_is_no_regressed_fix(monkeypatch, tmp_path):
    # valkey-search #1287 (fuzzer#68): unstable replied to the marked FT.SEARCH as the merge did; the parent, whose
    # order is no order, happened to reply alike on unstable's run, so the change looked lost.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")
    scored = {("search", 23): {"reply": [3, "p:1", "p:3", "p:2"]}}

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        merge = _build(run_dir) == "merge"
        return {"status": "PASS" if merge else "ERROR", "reason": None if merge else "unreached-change",
                "reached": merge, "parent_exited": False, "found": {},
                "reached_commands": {("search", 23)} if merge else set(), "replies": scored}

    result, publisher = _walk_merge(monkeypatch, tmp_path, fuzz, (test,))
    assert result["proven"] == 1 and result["findings"] == []
    publisher.upsert.assert_not_called()


def test_a_change_the_merge_got_wrong_is_no_regressed_fix(monkeypatch, tmp_path):
    # valkey-search #872 (fuzzer#82): the merge broke a suffix search the parent got right, its replies violating
    # their expect, and unstable fixed it again: the change is gone from unstable, but it was no fix.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        merge = _build(run_dir) == "merge"
        return {"status": "FAIL" if merge else "ERROR", "reason": None if merge else "unreached-change",
                "reached": merge, "parent_exited": False, "found": {},
                "reached_commands": {("suffix", 5), ("suffix", 6)} if merge else set(),
                "violating_commands": {("suffix", 5), ("suffix", 6)} if merge else set(),
                "replies": {("suffix", 5): {"reply": [0]}} if merge else {("suffix", 5): {"reply": [1, "doc:2"]}}}

    result, publisher = _walk_merge(monkeypatch, tmp_path, fuzz, (test,))
    assert result["proven"] == 1 and result["findings"] == []
    publisher.upsert.assert_not_called()


def test_the_commands_violating_their_expect_are_known_by_component_and_index():
    # Tool runs count too (fuzzer#113): at valkey#2177's merge the marked cli runs differed only by crashing
    # against their expect, and without their violating list the lost difference read as a regressed fix.
    report = {"outcome": {"component_results": [
        {"component_id": "seq", "evidence": [{"evidence_id": walk.SEQUENCE_EVIDENCE, "value": {"violating": [5, 27]}}]},
        {"component_id": "old", "evidence": [{"evidence_id": walk.SEQUENCE_EVIDENCE, "value": {"reached": [1]}}]},
        {"component_id": "cli", "evidence": [{"evidence_id": walk.TOOL_RUNS_EVIDENCE, "value": {"violating": [0]}}]},
    ]}}
    assert walk.violating_commands(report) == {("seq", 5), ("seq", 27), ("cli", 0)}


def test_the_replies_of_a_runs_sequence_are_known_by_component_and_index():
    report = {"outcome": {"component_results": [
        {"component_id": "seq", "evidence": [{"evidence_id": walk.SEQUENCE_EVIDENCE, "value": {"candidate": {
            "replies": [{"index": 0, "reply": "OK"}, {"index": 1, "error": "WRONGTYPE x"}]}}}]},
        {"component_id": "tools", "evidence": [{"evidence_id": walk.TOOL_RUNS_EVIDENCE, "value": {}}]},
    ]}}
    assert walk.sequence_replies(report) == {("seq", 0): {"reply": "OK"}, ("seq", 1): {"error": "WRONGTYPE x"}}


def test_a_finding_of_several_tests_is_one_line_per_merge():
    merge = _merge()
    same = {"rule": "regressed-fix", "verdict": "the fix of #4716 regressed", "issue": "https://x/issues/40",
            "fingerprint": "f1"}
    other = {"rule": "crash", "verdict": "introduced by #4716", "issue": None, "fingerprint": "f2"}
    planned = {"repo": walk.VALKEY_REPO, "merges": [merge], "skipped": [], "pointer": {}}
    state = update(initial_state(walk.VALKEY_REPO), planned, [_result(merge, findings=[same, other, same, same])],
                   now=NOW)
    assert state["done"][merge["sha"]]["findings"] == [
        {key: f[key] for key in ("rule", "verdict", "issue")} for f in (same, other)]


def _workflow() -> dict:
    # BaseLoader keeps the literal `on` key and every scalar as a string.
    return yaml.load(Path(".github/workflows/fuzzer-walk.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_the_workflow_is_dispatched_per_repository_by_name_and_never_overlaps_itself():
    workflow = _workflow()
    # The fork's main dispatches it, once per repository; it has no schedule of its own.
    assert list(workflow["on"]) == ["workflow_dispatch"]
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert list(inputs) == ["repository", "max"] and inputs["max"]["default"] == "20"
    assert inputs["repository"]["options"] == list(walk.NAMES) and inputs["repository"]["default"] == "valkey"
    assert workflow["concurrency"] == {"group": "fuzzer-walk-${{ inputs.repository || 'valkey' }}",
                                       "cancel-in-progress": "false"}
    assert workflow["env"]["NAME"] == "${{ inputs.repository || 'valkey' }}"
    assert workflow["permissions"] == {}
    assert workflow["jobs"]["plan"]["if"] == "github.repository_owner != 'valkey-io'"
    planning = next(s for s in workflow["jobs"]["plan"]["steps"] if s.get("name") == "Plan the run")["run"]
    assert '--repo "${NAME}" --state "walk-state/state/${NAME}.json"' in planning


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
    # Only the record job writes: this repository's walk-state branch, and the next run's dispatch.
    assert jobs["record"]["permissions"] == {"contents": "write", "actions": "write"}
    following = next(s for s in jobs["record"]["steps"] if s.get("name") == "Start the next run")
    assert "!cancelled()" in following["if"] and "vars.WALK_PAUSED != 'true'" in following["if"]
    assert 'gh workflow run fuzzer-walk.yml -R "${GITHUB_REPOSITORY}"' in following["run"]
    assert '"${GITHUB_REPOSITORY}" != valkey-io/*' in following["run"]
    assert all(job["permissions"].get("contents") == "read" for name, job in jobs.items() if name != "record")
    recording = next(s for s in jobs["record"]["steps"] if s.get("name") == "Record the run")["run"]
    assert '"${GITHUB_REPOSITORY}" != valkey-io/*' in recording
    assert 'walk update --remote "https://github.com/${GITHUB_REPOSITORY}.git"' in recording



def test_each_walk_run_starts_the_walks_maintenance():
    record = _workflow()["jobs"]["record"]
    names = [s.get("name") for s in record["steps"]]
    # Before the next run's dispatch, which may wait half an hour.
    assert names.index("Start the walk's maintenance") < names.index("Start the next run")
    step = record["steps"][names.index("Start the walk's maintenance")]
    following = record["steps"][names.index("Start the next run")]
    assert step["if"] == following["if"] == "${{ !cancelled() && vars.WALK_PAUSED != 'true' }}"
    # A dispatch that fails does not fail the walk.
    assert step["continue-on-error"] == "true" and step["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert '"${GITHUB_REPOSITORY}" != valkey-io/*' in step["run"]
    assert 'gh workflow run fuzzer-walk-maintain.yml -R "${GITHUB_REPOSITORY}" --ref "${GITHUB_REF_NAME}"' in step["run"]

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
    state = update(initial_state(walk.VALKEY_REPO), {"repo": walk.VALKEY_REPO, "merges": [redis], "skipped": [],
                                                     "pointer": {}}, [_result(redis)], now=NOW)
    assert "- [redis/redis#13154](https://github.com/redis/redis/pull/13154) ([tests](tests/valkey/13154-bbbbbbbbb/))" \
        in progress([state], datetime(2026, 11, 4, 18, tzinfo=timezone.utc))


def _with_fork(fork, call):
    saved = walk.VALKEY_FORK
    walk.VALKEY_FORK = fork
    try:
        return call()
    finally:
        walk.VALKEY_FORK = saved


def test_a_run_that_could_not_start_tested_nothing(monkeypatch, tmp_path):
    # The first walk's valkey-json runs: the servers never started, and the merge read as tested.
    summary = "runner execution failed during deployment.provision: LocalDeploymentError: valkey-0 failed to start"

    def run(command, **kw):
        report = {key: None for key in ("generated_at", "scenario", "coverage", "reproducer", "provenance")}
        report.update(status="ERROR", summary=summary, outcome={"status": "ERROR", "component_results": []})
        Path(command[command.index("--output") + 1]).write_text(json.dumps(report))

    monkeypatch.setattr(walk.subprocess, "run", run)
    doc = {"scenario_id": "s", "experiment": {"id": "proposed.x"}, "deployment": {"mode": "standalone"}}
    done = walk.fuzz("valkey-fuzzer", doc, 1, candidate={"server": "/m"}, compare={"server": "/p"}, core=True,
                     experiment_dir=tmp_path / "e", run_dir=tmp_path / "r")
    assert done["unstarted"] is True and done["status"] is None
    assert done["error"] == f"the run could not start: {summary}"


def test_a_build_that_rejects_the_server_config_is_unsupported_not_unstarted(monkeypatch, tmp_path):
    # valkey #4005, #4076, #4425: an old build died on a config it does not know ('priority-subnets ...' | Bad
    # directive), and the merge erred as "could not start" on every attempt instead of skipping the test.
    summary = ("runner execution failed during deployment.provision: LocalDeploymentError: Instance valkey-0 "
               "failed to start within 30s (process output: | *** FATAL CONFIG FILE ERROR (Version 255.255.255) "
               "*** | Reading the configuration file, at line 18 | >>> 'priority-subnets \"127.0.0.0/8\"' | "
               "Bad directive or wrong number of arguments)")

    def run(command, **kw):
        report = {key: None for key in ("generated_at", "scenario", "coverage", "reproducer", "provenance")}
        report.update(status="ERROR", summary=summary, outcome={"status": "ERROR", "component_results": []})
        Path(command[command.index("--output") + 1]).write_text(json.dumps(report))

    monkeypatch.setattr(walk.subprocess, "run", run)
    doc = {"scenario_id": "s", "experiment": {"id": "proposed.x"}, "deployment": {"mode": "standalone"}}
    done = walk.fuzz("valkey-fuzzer", doc, 1, candidate={"server": "/m"}, compare={"server": "/p"}, core=True,
                     experiment_dir=tmp_path / "e", run_dir=tmp_path / "r")
    assert done["status"] == "SKIP_UNSUPPORTED" and "unstarted" not in done and "error" not in done
    assert done["found"] == {} and done["reached"] is False


def test_the_walk_builds_porcupine_for_the_history_oracle(monkeypatch, tmp_path):
    # valkey 54980ece3's ai-f1548181d555: the history oracle errored every run with "porcupine is not on PATH".
    (tmp_path / "fuzzer" / "porcupine").mkdir(parents=True)
    commands = []

    def sh(command, cwd, log, env=None):
        commands.append(command)
        (tmp_path / "work" / "bin").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(walk, "_sh", sh)
    monkeypatch.setattr(walk.shutil, "which", lambda name: "/usr/bin/go" if name == "go" else None)
    monkeypatch.setenv("PATH", "/usr/bin")
    walk.ensure_porcupine(tmp_path / "fuzzer", tmp_path / "work")
    (command,) = commands
    assert command.startswith("go -C ") and command.endswith(f"build -o {tmp_path / 'work' / 'bin' / 'porcupine'}")
    assert walk.os.environ["PATH"] == f"{tmp_path / 'work' / 'bin'}:/usr/bin"


def test_the_walk_builds_porcupine_with_absolute_paths_from_a_relative_work_dir(monkeypatch, tmp_path):
    # The walk's work dir is relative (walk-work); go -C refused it: "GOPATH entry is relative".
    (tmp_path / "fuzzer" / "porcupine").mkdir(parents=True)
    calls = []
    monkeypatch.setattr(walk, "_sh", lambda command, cwd, log, env=None: calls.append((command, env)))
    monkeypatch.setattr(walk.shutil, "which", lambda name: "/usr/bin/go" if name == "go" else None)
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.chdir(tmp_path)
    walk.ensure_porcupine(Path("fuzzer"), Path("work"))
    ((command, env),) = calls
    work = (tmp_path / "work").resolve()
    assert command.endswith(f"build -o {work / 'bin' / 'porcupine'}")
    assert env == {"GOPATH": str(work / "go"), "GOCACHE": str(work / "go" / "cache")}
    assert walk.os.environ["PATH"] == f"{work / 'bin'}:/usr/bin"


def test_porcupine_already_on_path_or_without_go_is_left_alone(monkeypatch, tmp_path):
    (tmp_path / "fuzzer" / "porcupine").mkdir(parents=True)
    monkeypatch.setattr(walk, "_sh", lambda *a, **kw: pytest.fail("built anyway"))
    monkeypatch.setattr(walk.shutil, "which", lambda name: "/usr/bin/porcupine" if name == "porcupine" else None)
    walk.ensure_porcupine(tmp_path / "fuzzer", tmp_path / "work")
    monkeypatch.setattr(walk.shutil, "which", lambda name: None)
    walk.ensure_porcupine(tmp_path / "fuzzer", tmp_path / "work")
    walk.ensure_porcupine(tmp_path / "no-such-fuzzer", tmp_path / "work")


def test_a_run_suppresses_the_leaks_leaksanitizer_cannot_follow_in_valkey_json(monkeypatch, tmp_path):
    envs = []

    def run(command, **kw):
        envs.append(kw["env"])
        raise walk.subprocess.TimeoutExpired(command, 1)

    monkeypatch.setattr(walk.subprocess, "run", run)
    doc = {"scenario_id": "s", "experiment": {"id": "proposed.x"}, "deployment": {"mode": "standalone"}}
    walk.fuzz("valkey-fuzzer", doc, 1, candidate={"server": "/m"}, compare={"server": "/p"}, core=True,
              experiment_dir=tmp_path / "e", run_dir=tmp_path / "r")
    (env,) = envs
    path = env["LSAN_OPTIONS"].removeprefix("suppressions=")
    assert "leak:libjson.so" in Path(path).read_text().splitlines()


def test_the_commands_that_reached_the_change_are_read_from_the_sequence_and_tool_evidence():
    report = _report(
        {"component_id": "seq", "evidence": [{"evidence_id": "valkey.command.sequence", "value": {"reached": [3, 5]}}]},
        {"component_id": "cli", "evidence": [{"evidence_id": "valkey.tool.runs", "value": {"reached": [0]}}]},
        {"component_id": "other", "evidence": [{"evidence_id": "valkey.ledger", "value": {"reached": [9]}}]})
    assert walk.reached_commands(report) == {("seq", 3), ("seq", 5), ("cli", 0)}


# --- Coverage -------------------------------------------------------------------

DIFF = """\
diff --git a/src/t_list.c b/src/t_list.c
index 1..2 100644
--- a/src/t_list.c
+++ b/src/t_list.c
@@ -10,4 +10,5 @@ void lsetCommand(client *c) {
     long index;
-    if (getLongFromObject(c->argv[2], &index) != C_OK) return;
+    if (getRangeLongFromObjectOrReply(c, c->argv[2], LONG_MIN, LONG_MAX, &index, NULL) != C_OK)
+        return;
     robj *o = lookupKeyWrite(c->db, c->argv[1]);
@@ -40,2 +41,3 @@ void lsetCommand(client *c) {
 }
+-- not a header
\\ No newline at end of file
diff --git a/src/gone.c b/src/gone.c
deleted file mode 100644
--- a/src/gone.c
+++ /dev/null
@@ -1,2 +0,0 @@
-int gone;
---x;
diff --git a/tests/unit/type/list.tcl b/tests/unit/type/list.tcl
--- a/tests/unit/type/list.tcl
+++ b/tests/unit/type/list.tcl
@@ -1 +1,2 @@
 start_server {
+    test {LSET out of range} {}
"""


def test_a_diff_gives_each_files_added_lines_and_its_removed_count():
    added, removed = walk.diff_lines(DIFF)
    assert added == {"src/t_list.c": [11, 12, 42], "src/gone.c": [], "tests/unit/type/list.tcl": [2]}
    assert removed == {"src/t_list.c": 1, "src/gone.c": 2, "tests/unit/type/list.tcl": 0}
    # Coverage measures the added lines of C files under src/ only.
    assert walk.coverage_lines(DIFF) == {"src/t_list.c": [11, 12, 42]}


def _diff(files: int, lines: int, path: str = "src/f{}.c") -> str:
    return "".join(f"diff --git a/{path.format(i)} b/{path.format(i)}\n@@ -1 +1,{lines} @@\n" + "+x\n" * lines
                   for i in range(files))


@pytest.mark.parametrize("diff, expected", [
    (_diff(1, 4), 3),  # a few lines: the minimum
    (DIFF, 4),  # 6 changed lines in 2 files of code; the test file does not count
    (_diff(1, 4) + _diff(5, 400, "tests/unit/t{}.tcl") + _diff(2, 9, "docs/d{}.md"), 3),
    (_diff(3, 40), 3 + 120 // 50 + 2),  # more lines, more files
    (_diff(1, 249), 3 + 4),
    (_diff(20, 300), walk.MAX_TESTS),  # capped
])
def test_the_tests_asked_for_grow_with_the_changed_code(diff, expected):
    assert walk.budget(diff, walk.VALKEY_REPO) == expected


def test_coverage_counts_the_changed_lines_gcov_marks_as_code():
    counts = walk.gcov_counts((Path(__file__).parent / "fixtures" / "hyperloglog.c.gcov").read_text())
    # Lines of no code are left out; ##### never ran; 100* ran (some of its blocks did not).
    assert (counts[856], counts[864], counts[888], counts[891]) == (84, 0, 100, 100)
    assert 0 not in counts and 857 not in counts and 880 not in counts
    found = walk.coverage({"src/hyperloglog.c": [857, 864, 865, 888, 891, 999], "src/other.c": [5]},
                          {"src/hyperloglog.c": counts, "src/other.c": None})
    assert found == {"lines": 3, "executed": 2, "percent": 66, "uncovered": ["src/hyperloglog.c:864"],
                     "unnoted": ["src/other.c"]}
    assert walk.coverage({"src/a.c": [1]}, {"src/a.c": {}})["percent"] is None


def test_gcov_of_a_file_without_functions_is_measured_as_no_executable_lines(monkeypatch, tmp_path):
    # valkey #2309 touched cli_commands.c, tables from commands.def with no functions: gcov exits 0 with "No
    # executable lines" and writes no .gcov report, and the merge's coverage went unmeasured on the missing file.
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "cli_commands.c").write_text("int table[1] = {0};\n")
    (tmp_path / "src" / "cli_commands.gcno").write_bytes(b"")

    def run(command, **kw):
        return subprocess.CompletedProcess(command, 0, stdout="No executable lines\n", stderr="")

    monkeypatch.setattr(walk.subprocess, "run", run)
    assert walk.gcov(tmp_path, "src/cli_commands.c") == {}


def _rounds(executed: list[int], lines: int = 10, tests: list[list[str]] | None = None, started: float | None = None):
    """cover() with fakes: each measure reports the next of ``executed``; each round walks the next of ``tests``."""
    measured: list[list[str]] = []
    walked: list[dict] = []
    more_tests = iter(tests if tests is not None else [["t"]] * 10)

    def measure(tests):
        measured.append(tests)
        return {"lines": lines, "executed": executed[len(measured) - 1], "uncovered": []}

    def more(found):
        walked.append(found)
        return next(more_tests)

    found = walk.cover(measure, more, ["first"], walk.time.monotonic() if started is None else started)
    return found, measured, walked


def test_more_rounds_run_while_the_coverage_is_low_and_growing():
    # At the target: no round.
    found, measured, walked = _rounds([8])
    assert (found["executed"], found["rounds"], len(walked)) == (8, 0, 0)
    # Under it and growing: rounds until the target...
    found, measured, walked = _rounds([2, 5, 8, 9])
    assert (found["executed"], found["rounds"], measured) == (8, 2, [["first"], ["t"], ["t"]])
    # ...or COVERAGE_ROUNDS of them.
    found, measured, walked = _rounds([1, 2, 3, 4, 5, 6])
    assert (found["executed"], found["rounds"], len(walked)) == (4, walk.COVERAGE_ROUNDS, walk.COVERAGE_ROUNDS)
    # A round that covers no new line ends them, and one with no test is not measured.
    found, measured, walked = _rounds([2, 4, 4, 9])
    assert (found["executed"], found["rounds"]) == (4, 2)
    found, measured, walked = _rounds([2, 9], tests=[[]])
    assert (found["executed"], found["rounds"], len(measured)) == (2, 1, 1)
    # No changed line of code: nothing to ask for.
    found, measured, walked = _rounds([0], lines=0)
    assert (found["rounds"], walked) == (0, [])
    # A walk that has run long starts no round.
    found, measured, walked = _rounds([2], started=walk.time.monotonic() - walk.ROUNDS_WITHIN_S)
    assert walked == []


def test_a_merge_gets_rounds_aimed_at_its_unrun_lines_until_one_covers_nothing_new(monkeypatch, tmp_path):
    first, second, third = (_scenario(f"seed: {seed}\nexperiment: {{id: proposed.lset}}\n"
                                      "deployment: {mode: standalone}\n") for seed in (10, 20, 30))
    on_coverage: set[int] = set()

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        if run_dir.name.endswith("-coverage"):
            # Checks off, against the parent, on the coverage build.
            assert experiment_dir.name == "experiments-plain" and "checks" not in (experiment_dir / "proposed.lset.yaml").read_text()
            assert Path(candidate["server"]).parts[-3] == "coverage" and Path(compare["server"]).parts[-3] == "parent"
            on_coverage.add(seed)
        return {"status": "PASS", "reason": None, "reached": True, "parent_exited": False,
                "reached_commands": {("lset", 1)}, "found": {}}

    def built(src, asan=False, coverage=False):
        if coverage:
            (src / "src").mkdir(parents=True)
            (src / "src" / "t_list.c").write_text("".join(f"line {n}\n" for n in range(1, 50)))
        return src / "src" / "valkey-server"

    def counts(tree, path):
        # The first test runs line 11, the second 12, the third nothing more; 42 never runs.
        assert tree.name == "coverage" and path == "src/t_list.c"
        return {11: int(10 in on_coverage), 12: int(20 in on_coverage), 42: 0}

    extras: list[str] = []
    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (first,), (second,), (third,), diff=DIFF, extras=extras,
                            counts=counts, build=built)
    assert "error" not in result
    assert result["coverage"] == {"lines": 3, "executed": 2, "percent": 66, "uncovered": ["src/t_list.c:42"],
                                  "unnoted": [], "rounds": 2}
    assert on_coverage == {10, 20, 30}
    assert (result["proposed"], result["hunted"]) == (3, 3)
    # The first round asks plainly; the next show the lines no test ran, with code around them.
    assert extras[0] == "" and len(extras) == 3
    assert "ran 1 of the change's 3 changed lines of code (33%)" in extras[1]
    assert ">    12 | line 12" in extras[1] and "     13 | line 13" in extras[1] and "...\n     39 | line 39" in extras[1]
    assert "Experiment ids already used: proposed.lset" in extras[1]
    assert ">    12 | line 12" not in extras[2] and ">    42 | line 42" in extras[2]
    # Every round's tests were walked like the first's.
    folder = tmp_path / "out" / merge_folder(_merge())
    assert sorted(p.name for p in (folder / "scenarios").iterdir()) == sorted(
        f"{doc['scenario_id']}.yaml" for doc in (first, second, third))


def test_a_coverage_of_no_measured_line_says_why(monkeypatch, tmp_path):
    # 66 valkey merges had coverage None with no reason: the gcov build left no notes of the changed file
    # (modules/lua before the CFLAGS fix), so percent was None and nothing showed in PROGRESS.md.
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "PASS", "reason": None, "reached": True, "parent_exited": False,
                "reached_commands": {("lset", 1)}, "found": {}}

    result, _ = _walk_merge(monkeypatch, tmp_path / "unnoted", fuzz, (test,), diff=DIFF,
                            counts=lambda tree, path: None)
    assert result["coverage"]["percent"] is None
    assert result["coverage_reason"] == "the build left no gcov notes of src/t_list.c"

    # All changed lines non-code (a table, a declaration): nothing to measure, not a failure.
    result, _ = _walk_merge(monkeypatch, tmp_path / "no-code", fuzz, (test,), diff=DIFF,
                            counts=lambda tree, path: {})
    assert result["coverage"]["percent"] is None
    assert result["coverage_reason"] == walk.COVERAGE_NO_CODE


def test_a_coverage_build_that_fails_leaves_the_coverage_unknown_and_the_merge_tested(monkeypatch, tmp_path):
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "PASS", "reason": None, "reached": True, "parent_exited": False,
                "reached_commands": {("lset", 1)}, "found": {}}

    def built(src, asan=False, coverage=False):
        if coverage:
            raise RuntimeError("`make gcov` failed in coverage (exit 2)")
        return src / "src" / "valkey-server"

    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (test,), diff=DIFF, build=built)
    assert "error" not in result and result["outcome"] == "tested" and result["proven"] == 1
    assert result["coverage"] is None and result["coverage_reason"] == "`make gcov` failed in coverage (exit 2)"


def test_the_coverage_build_carries_the_gcov_flags_to_sub_makes(monkeypatch, tmp_path):
    # valkey #5117 (src/modules/lua/debug_lua.c): `make gcov` puts its flags in SERVER_CFLAGS, which the
    # modules/lua sub-make does not read, so the Lua engine had no .gcno and its coverage was silently None:
    # the environment carries them where SERVER_CFLAGS does not reach. LDFLAGS too: deps/lua links its own
    # binary from the instrumented objects, and without them the link fails on __gcov_init (the walk's #4526
    # and #5183: `make gcov` failed in coverage, exit 2).
    calls = []
    monkeypatch.setattr(walk, "_sh", lambda command, cwd, log, env=None: calls.append((command, env)))
    (tmp_path / "src" / "valkey-server").parent.mkdir(parents=True)
    (tmp_path / "src" / "valkey-server").touch()
    walk.build_valkey(tmp_path, coverage=True)
    walk.build_valkey(tmp_path)
    walk.build_valkey(tmp_path, asan=True)
    gcov_flags = "-fprofile-arcs -ftest-coverage"
    assert "gcov" in calls[0][0] and calls[0][1] == {"CFLAGS": gcov_flags, "LDFLAGS": gcov_flags}
    assert calls[1][1] is None and calls[2][1] is None


def test_a_failed_walks_error_keeps_the_head_and_the_tail(monkeypatch, tmp_path):
    # The valkey-search builds of 2026-11-08: _sh raises with the build log's last 30 lines, over 1000
    # characters of ninja progress, and a plain [:1000] cut off the compiler's error at the end.
    tail = " ".join(f"[{step}/206] Building CXX object step{step}.o" for step in range(1, 40))
    error = f"`export NINJA_TOOL=ninja` failed in merge (exit 1): {tail} " \
            "FAILED: fatal error: src/coordinator/coordinator.grpc.pb.h: No such file or directory"

    def built(src, asan=False, coverage=False):
        raise RuntimeError(error)

    result, _ = _walk_merge(monkeypatch, tmp_path, None, (), build=built)
    assert len(result["error"]) <= 1000
    assert result["error"].startswith("`export NINJA_TOOL=ninja` failed in merge (exit 1):")
    assert result["error"].endswith("fatal error: src/coordinator/coordinator.grpc.pb.h: No such file or directory")


def test_progress_shows_each_merges_coverage_and_the_median():
    merges = [_merge(sha=c * 40, pr=i) for i, c in enumerate("bcdef", 1)]
    covered = [{"lines": 10, "executed": e, "percent": e * 10, "uncovered": []} for e in (9, 4, 7)] + [None, None]
    reasons = [None, None, None, "the merge adds no line to a C file under src/",
               "`make gcov` failed in coverage (exit 2)"]
    planned = {"repo": walk.VALKEY_REPO, "merges": merges, "skipped": [], "pointer": {}}
    state = update(initial_state(walk.VALKEY_REPO), planned,
                   [_result(m, coverage=c, coverage_reason=r, findings=[])
                    for m, c, r in zip(merges, covered, reasons)], now=NOW)
    text = progress([state], datetime(2026, 11, 4, 18, tzinfo=timezone.utc))
    assert "2 of 3 test(s) reached the change, coverage 90%" in text
    assert "2 of 3 test(s) reached the change\n" in text  # nothing to measure: no coverage shown
    # A measurement that failed is not silent: the merge's line says why (fuzzer-walk maintenance, 2026-11-07).
    assert "2 of 3 test(s) reached the change, coverage not measured (`make gcov` failed in coverage (exit 2))" in text
    assert "- Changed-line coverage: median 70% of 3 merge(s) measured, 1 under 50%" in text


def test_checks_leave_every_scalar_as_written():
    # As in the walk's test of valkey-io/valkey-bloom#110: PyYAML reads `no` as false, which the fuzzer refuses.
    text = EXPERIMENT.replace("{command: [RPUSH, l, a]", "{command: [CONFIG, SET, bf.bloom-use-random-seed, no]")
    assert "bf.bloom-use-random-seed, no]" in text
    out = walk.with_checks(text)
    assert yaml.safe_load(out)["components"][0]["config"]["checks"] == CHECKS
    assert "bf.bloom-use-random-seed, no]" in out and "false" not in out


def test_a_panic_the_change_adds_is_intended_not_filed():
    # valkey-io/valkey#5155 panics on purpose when ValkeyModule_Calloc's nmemb * size overflows.
    diff = ('+    if (nmemb && size > SIZE_MAX / nmemb)\n'
            '+        serverPanic("ValkeyModule_Calloc(%zu, %zu) overflows", nmemb, size);\n')
    cause = "crash: ValkeyModule_Calloc(9223372036854775809, 2) overflows #module.c:590"
    assert walk.intended_crash(diff, cause)
    assert not walk.intended_crash(diff.replace("+  ", "   "), cause)  # a line the change keeps, not adds
    assert not walk.intended_crash('+    serverAssert(x > 0);\n', "crash: assertion failed: 'x > 0' is not true")
    finding = {"builds": {"parent": False, "merge": True, "unstable": True}, "verdict": "intended by #5155: a panic"}
    assert not walk.files(finding) and walk.files({**finding, "verdict": "introduced by #5155"})


def test_the_marked_commands_are_those_an_experiment_expects_to_differ():
    assert walk.marked_commands(EXPERIMENT) == [("compared", 1, ["LSET", "l", 9223372036854775807, "x"])]
    assert walk.marked_commands("not: [yaml") == [] and walk.marked_commands("") == []


def test_the_recall_section_shows_each_tests_marked_commands_and_both_builds_replies():
    tests = [{"scenario": "ai-1", "experiment": "proposed.lset", "summary": "unreached-change",
              "commands": [{"command": ["LSET", "l", 1, "x"], "merge": {"error": "ERR index out of range"},
                            "parent": {"error": "ERR index out of range"}}]},
             {"scenario": "ai-2", "experiment": "valkey.forkless-save", "summary": "PASS", "commands": []}]
    text = walk.unreached_section(tests, ["proposed.lset"])
    assert text.startswith("## The tests never showed the fix")
    assert '- `["LSET", "l", 1, "x"]`: the merge replied `{"error": "ERR index out of range"}`, the parent ' in text
    assert "marks no command with `expect_difference`" in text.split("### ai-2")[1]
    assert "Experiment ids already used: proposed.lset" in text
    long = [{**tests[0], "commands": [{"command": "x" * 500, "merge": None, "parent": None}] * 30}]
    assert walk.unreached_section(long, []).count("- `") == walk.MARKED_KEPT
    assert "x" * walk.REPLY_CHARS + "..." in walk.unreached_section(long, [])


@pytest.mark.parametrize("subject, shown", [
    ("Fix LSET (#4716)", True), ("Avoid double close on repl_transfer_fd (#1349)", True),
    ("Reject integer overflow of length fields (#3920)", True), ("Optimize LSET (#4716)", False),
    ("Add a prefix option (#1)", False)])
def test_a_fix_merge_whose_tests_never_showed_the_change_gets_one_more_round_shown_their_replies(
        monkeypatch, tmp_path, subject, shown):
    first, second = (_scenario(f"seed: {seed}\nexperiment: {{id: proposed.lset}}\ndeployment: {{mode: standalone}}\n")
                     for seed in (10, 20))

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        # The first test's marked LSET replies alike on both builds; the second round's test reaches the change.
        reached = doc["seed"] == 20 and _build(run_dir) == "merge"
        return {"status": "PASS" if reached else "ERROR", "reason": None if reached else "unreached-change",
                "reached": reached, "parent_exited": False, "reached_commands": {("compared", 1)} if reached else set(),
                "replies": {("compared", 1): {"error": "ERR index out of range"}}, "found": {},
                "summary": "ok" if reached else "command 1 replied alike"}

    extras: list[str] = []
    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (first,), (second,), merge=_merge(subject=subject),
                            extras=extras)
    assert "error" not in result
    if not shown:
        # Not a fix: no round for its unshown change.
        assert extras == [""] and "recall_round" not in result and result["recall"] is False
        return
    assert len(extras) == 2 and extras[1].startswith("## The tests never showed the fix")
    assert '`["LSET", "l", 9223372036854775807, "x"]`: the merge replied `{"error": "ERR index out of range"}`' in extras[1]
    assert "Its run on the merge: command 1 replied alike" in extras[1]
    assert result["recall"] is True and result["recall_round"] == {"tests": 1, "recall": True}
    assert (result["proposed"], result["proven"]) == (2, 1)
    assert walk._summary(_merge(subject=subject), result, "now")["recall_round"] == {"tests": 1, "recall": True}


def test_a_fix_merge_already_shown_gets_no_recall_round(monkeypatch, tmp_path):
    test = _scenario("seed: 10\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n")

    def fuzz(fuzzer_bin, doc, seed, *, candidate, compare, core, experiment_dir, run_dir):
        return {"status": "PASS", "reason": None, "reached": True, "parent_exited": False,
                "reached_commands": {("compared", 1)}, "found": {}}

    extras: list[str] = []
    result, _ = _walk_merge(monkeypatch, tmp_path, fuzz, (test,), extras=extras)
    assert extras == [""] and result["recall"] is True and "recall_round" not in result
