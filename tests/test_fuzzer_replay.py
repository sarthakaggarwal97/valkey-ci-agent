"""Tests for replaying merged Valkey pull requests through the fuzzer."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import scripts.fuzzer.replay as replay_mod
from scripts.fuzzer.pr_batch import ModuleBuild
from scripts.fuzzer.propose import (
    Change,
    ExperimentProposal,
    Proposal,
    ProposalRun,
    _render_scenario,
    check_scenario,
)
from scripts.fuzzer.replay import describe, plan, request_id, resolve_commit

SCENARIO = "seed: 7\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n"
PROPOSED_SCENARIO = "seed: 8\nexperiment: {id: proposed.lset}\ndeployment: {mode: standalone}\n"
EXPERIMENT = "id: proposed.lset\ndeployment: {modes: [standalone]}\ncomponents: []\n"
PINNED = "e" * 40


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def valkey(tmp_path) -> Path:
    """A repository whose main line is squash merges, with one merge commit from a side branch."""
    repo = tmp_path / "valkey"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "unstable")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    for subject in ("Cut 9.2.0-rc1", "Fix the forkless save (#4716)", "Add PHPATHCOUNT (#5071)"):
        _git(repo, "commit", "-q", "--allow-empty", "-m", subject)
    _git(repo, "checkout", "-q", "-b", "side", "HEAD~1")
    _git(repo, "commit", "-q", "--allow-empty", "-m", "side work")
    _git(repo, "checkout", "-q", "unstable")
    _git(repo, "merge", "-q", "--no-ff", "-m", "Merge the side branch", "side")
    return repo


def test_plan_lists_the_first_parent_merges_oldest_first(valkey, monkeypatch):
    shas = _git(valkey, "log", "--first-parent", "--format=%H", "unstable").splitlines()[::-1]
    # The second merge brings the rules the repl-compression oracle models.
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", shas[2])

    merges = plan(valkey, f"{shas[0]}..unstable")

    assert [(m["index"], m["sha"], m["parent"], m["pr"]) for m in merges] == [
        (0, shas[1], shas[0], 4716), (1, shas[2], shas[1], 5071), (2, shas[3], shas[2], None),
    ]
    assert [m["exclude_experiments"] for m in merges] == [list(replay_mod.REPL_COMPRESSION_EXPERIMENTS), [], []]
    assert [m["subject"] for m in merges] == ["Fix the forkless save (#4716)", "Add PHPATHCOUNT (#5071)",
                                             "Merge the side branch"]
    assert plan(valkey, f"{shas[0]}..unstable", first=1) == merges[:1]
    with pytest.raises(ValueError, match="a range is BASE..TIP"):
        plan(valkey, "unstable")


def test_plan_takes_the_merges_given_in_their_order(valkey, monkeypatch):
    shas = _git(valkey, "log", "--first-parent", "--format=%H", "unstable").splitlines()[::-1]
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", shas[2])

    # Out of history order, one abbreviated: kept in the order given, each resolved with its first parent.
    merges = plan(valkey, merges=[shas[3], shas[1][:9]])

    assert [(m["index"], m["sha"], m["parent"], m["pr"]) for m in merges] == [
        (0, shas[3], shas[2], None), (1, shas[1], shas[0], 4716),
    ]
    assert [m["exclude_experiments"] for m in merges] == [[], list(replay_mod.REPL_COMPRESSION_EXPERIMENTS)]
    assert merges[1]["subject"] == "Fix the forkless save (#4716)"
    assert plan(valkey, merges=[shas[3], shas[1]], first=1) == merges[:1]


@pytest.mark.parametrize("sha,error", [
    ("f" * 40, "is not a commit in"),
    ("abc", "7 to 40 lowercase hex"),
    ("HEAD~1", "7 to 40 lowercase hex"),
])
def test_plan_refuses_a_merge_it_cannot_resolve(valkey, monkeypatch, sha, error):
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", _git(valkey, "rev-parse", "HEAD"))
    with pytest.raises(ValueError, match=error):
        plan(valkey, merges=[sha])


def test_plan_command_takes_merges_separated_by_commas_or_spaces(valkey, monkeypatch, tmp_path):
    shas = _git(valkey, "log", "--first-parent", "--format=%H", "unstable").splitlines()[::-1]
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", shas[0])
    out = tmp_path / "merges.json"
    assert replay_mod.main(["plan", "--repo-dir", str(valkey), "--merges", f"{shas[2]}, {shas[1]} {shas[3]}",
                            "--out", str(out)]) == 0
    assert [m["sha"] for m in json.loads(out.read_text())] == [shas[2], shas[1], shas[3]]
    for args in (["--merges", shas[1], "--range", f"{shas[0]}..unstable"], []):
        with pytest.raises(SystemExit):
            replay_mod.main(["plan", "--repo-dir", str(valkey), "--out", str(out), *args])


def test_a_merge_is_named_by_its_pull_request_and_commit():
    merge = {"index": 7, "sha": "e082696cf" + "0" * 31, "pr": 4716, "subject": "Fix the forkless save (#4716)"}
    assert describe(merge, "valkey-io/valkey") == (
        "valkey-io/valkey#4716, merged as e082696cf000: Fix the forkless save (#4716)"
    )
    assert describe({**merge, "pr": None}, "valkey-io/valkey").startswith("valkey-io/valkey, merged as e082696cf000")
    assert request_id(merge) == "replay-007-e082696cf"
    assert request_id(merge, "41764158096") == "replay-41764158096-007-e082696cf"


def _merge() -> dict:
    return {"index": 3, "sha": "b" * 40, "parent": "a" * 40, "pr": 4716, "subject": "Fix the forkless save (#4716)"}


def _change() -> Change:
    return Change(repo="valkey-io/valkey", pr_number=None, title="1 commit(s)", base_sha="a" * 40,
                  head_sha="b" * 40, diff="diff --git a/x b/x\n")


def _replay(monkeypatch, tmp_path, *, propose, merge=None, random_seeds=2, build="default", repo="valkey-io/valkey"):
    # The request ID names the replay run in CI; a test does not run in one.
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    fetched, dispatched, waited = [], [], []
    monkeypatch.setattr(replay_mod, "fetch_change", lambda gh, token, request: fetched.append(request) or _change())
    monkeypatch.setattr(replay_mod, "load_catalog", lambda fuzzer_bin: {"experiments": [], "components": []})
    monkeypatch.setattr(replay_mod, "fuzzer_validator", lambda fuzzer_bin: None)
    monkeypatch.setattr(replay_mod, "propose", propose)
    monkeypatch.setattr(replay_mod, "dispatch_batch", lambda gh, inputs, **kw: dispatched.append((inputs, kw)))
    batch = MagicMock(id=99, html_url="https://x/runs/99", conclusion="failure")
    monkeypatch.setattr(replay_mod, "wait_for_run", lambda gh, **kw: waited.append(kw) or batch)
    result = replay_mod.run(
        merge or _merge(), gh=MagicMock(), token="t", repo=repo, fuzzer_repo="owner/valkey-fuzzer",
        fuzzer_ref="generic-fuzzer", fuzzer_dir=tmp_path, fuzzer_bin="valkey-fuzzer", count=5,
        random_seeds=random_seeds, build=build,
        out_dir=tmp_path / "out", timeout_s=60,
    )
    return result, fetched, dispatched, waited


def _write(out_dir: Path, change: Change, *docs: dict) -> ProposalRun:
    """What propose leaves on disk for ``docs``: the scenarios, and the experiment the proposed ones use."""
    run = ProposalRun(change=change)
    (out_dir / "scenarios").mkdir(parents=True)
    (out_dir / "experiments").mkdir(parents=True)
    for index, doc in enumerate(docs, 1):
        name = doc["scenario_id"]
        (out_dir / "scenarios" / f"{name}.yaml").write_text(_render_scenario(doc, change))
        run.proposals.append(Proposal(index=index, rationale="why", text="x", scenario_id=name,
                                      file=f"scenarios/{name}.yaml", experiment=doc["experiment"]["id"]))
    if any(doc["experiment"]["id"] == "proposed.lset" for doc in docs):
        (out_dir / "experiments" / "proposed.lset.yaml").write_text(EXPERIMENT)
        run.experiments.append(ExperimentProposal(index=1, text=EXPERIMENT, experiment_id="proposed.lset",
                                                  file="experiments/proposed.lset.yaml"))
    return run


def test_a_merge_runs_its_proposals_beside_a_random_baseline_on_the_merge_commit(monkeypatch, tmp_path):
    doc, error = check_scenario(SCENARIO)
    assert error is None
    name = doc["scenario_id"]

    def propose(change, *, out_dir, **kw):
        assert change.title == "Fix the forkless save (#4716)"
        (out_dir / "scenarios").mkdir(parents=True)
        (out_dir / "scenarios" / f"{name}.yaml").write_text(_render_scenario(doc, change))
        run = ProposalRun(change=change)
        run.proposals.append(Proposal(index=1, rationale="why", text=SCENARIO, scenario_id=name,
                                      file=f"scenarios/{name}.yaml"))
        run.proposals.append(Proposal(index=2, rationale="bad", text="x", error="does not validate"))
        return run

    result, fetched, dispatched, waited = _replay(monkeypatch, tmp_path, propose=propose)

    # The change is the merge against its parent, so the batch tests what merged.
    assert (fetched[0].base, fetched[0].head) == ("a" * 40, "b" * 40)
    ((inputs, where),) = dispatched
    assert list(json.loads(inputs["scenarios"])) == [name]
    assert (inputs["valkey_sha"], inputs["valkey_ref"], inputs["random_seeds"], inputs["request_id"]) == (
        "b" * 40, "b" * 40, "2", "replay-003-bbbbbbbbb",
    )
    # The parent is always built, so a proposed experiment can compare with it; the default build is implied.
    assert inputs["valkey_parent_sha"] == "a" * 40
    assert "build" not in inputs and "experiments" not in inputs
    assert where == {"fuzzer_repo": "owner/valkey-fuzzer", "workflow": "fuzzer-pr-batch.yml",
                     "ref": "generic-fuzzer"}
    assert waited[0]["request_id"] == "replay-003-bbbbbbbbb"
    assert (result["proposed"], result["dropped"], result["run_id"], result["conclusion"]) == (1, 1, 99, "failure")
    assert (result["experiments"], result["dropped_experiments"], result["build"]) == (0, 0, "default")
    assert "reason" not in result
    written = json.loads((tmp_path / "out" / "replay.json").read_text())
    assert written["change"] == "valkey-io/valkey#4716, merged as bbbbbbbbbbbb: Fix the forkless save (#4716)"
    # The merge's diff, for the monitor's --change-diff.
    assert (tmp_path / "out" / "change.diff").read_text() == _change().diff


def test_a_merge_with_proposed_experiments_sends_them_on_the_chosen_build(monkeypatch, tmp_path):
    docs = [check_scenario(text)[0] for text in (SCENARIO, PROPOSED_SCENARIO)]
    installed, proposed = (doc["scenario_id"] for doc in docs)

    def propose(change, *, out_dir, **kw):
        return _write(out_dir, change, *docs)

    result, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose, build="asan")

    ((inputs, _),) = dispatched
    assert sorted(json.loads(inputs["scenarios"])) == sorted([installed, proposed])
    assert json.loads(inputs["experiments"]) == {"proposed.lset.yaml": EXPERIMENT}
    assert (inputs["valkey_parent_sha"], inputs["build"]) == ("a" * 40, "asan")
    assert (result["experiments"], result["build"]) == (1, "asan")
    written = json.loads((tmp_path / "out" / "replay.json").read_text())
    assert (written["experiments"], written["build"]) == (1, "asan")


def test_a_change_that_needs_no_test_is_recorded(monkeypatch, tmp_path):
    def propose(change, *, out_dir, **kw):
        (out_dir / "scenarios").mkdir(parents=True)
        run = ProposalRun(change=change)
        run.reason = "A comment-only change."
        return run

    result, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose)
    assert (result["reason"], result["proposed"]) == ("A comment-only change.", 0)
    assert dispatched[0][0]["scenarios"] == "{}"


def test_plan_needs_the_rules_commit_in_the_clone(valkey, monkeypatch):
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", "f" * 40)
    with pytest.raises(RuntimeError, match="cannot tell whether"):
        plan(valkey, "HEAD~1..unstable")


def test_a_merge_before_the_rules_leaves_their_experiments_out(monkeypatch, tmp_path):
    chaos = "seed: 8\nexperiment: {id: valkey.replication-chaos}\ndeployment: {mode: standalone}\n"
    docs = [check_scenario(text)[0] for text in (SCENARIO, chaos)]

    def propose(change, *, out_dir, **kw):
        (out_dir / "scenarios").mkdir(parents=True)
        run = ProposalRun(change=change)
        for index, doc in enumerate(docs, 1):
            name = doc["scenario_id"]
            (out_dir / "scenarios" / f"{name}.yaml").write_text(_render_scenario(doc, change))
            run.proposals.append(Proposal(index=index, rationale="why", text="x", scenario_id=name,
                                          file=f"scenarios/{name}.yaml"))
        return run

    merge = {**_merge(), "exclude_experiments": list(replay_mod.REPL_COMPRESSION_EXPERIMENTS)}
    result, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose, merge=merge)

    ((inputs, _),) = dispatched
    assert list(json.loads(inputs["scenarios"])) == [docs[0]["scenario_id"]]
    assert inputs["valkey_parent_sha"] == "a" * 40
    assert inputs["exclude_experiments"] == " ".join(replay_mod.REPL_COMPRESSION_EXPERIMENTS)
    assert (result["proposed"], result["excluded_proposals"]) == (2, 1)


def test_plan_refuses_more_merges_than_a_matrix_runs(valkey, monkeypatch):
    monkeypatch.setattr(replay_mod, "MAX_MERGES", 2)
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", _git(valkey, "rev-parse", "HEAD~3"))
    with pytest.raises(ValueError, match="holds 3 merges; a replay runs at most 2"):
        plan(valkey, "HEAD~3..unstable")
    assert len(plan(valkey, "HEAD~3..unstable", first=2)) == 2


def test_without_a_random_baseline_the_exclusion_is_not_sent(monkeypatch, tmp_path):
    doc = check_scenario(SCENARIO)[0]

    def propose(change, *, out_dir, **kw):
        (out_dir / "scenarios").mkdir(parents=True)
        (out_dir / "scenarios" / f"{doc['scenario_id']}.yaml").write_text(_render_scenario(doc, change))
        run = ProposalRun(change=change)
        run.proposals.append(Proposal(index=1, rationale="why", text=SCENARIO, scenario_id=doc["scenario_id"],
                                      file=f"scenarios/{doc['scenario_id']}.yaml"))
        return run

    merge = {**_merge(), "exclude_experiments": list(replay_mod.REPL_COMPRESSION_EXPERIMENTS)}
    _, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose, merge=merge, random_seeds=0)
    ((inputs, _),) = dispatched
    assert "exclude_experiments" not in inputs and "random_seeds" not in inputs


def test_replay_json_names_the_batch_as_soon_as_it_is_found(monkeypatch, tmp_path):
    def wait(gh, **kw):
        kw["on_found"](MagicMock(id=99, html_url="https://x/runs/99"))
        raise TimeoutError("the PR batch did not complete")

    monkeypatch.setattr(replay_mod, "fetch_change", lambda gh, token, request: _change())
    monkeypatch.setattr(replay_mod, "load_catalog", lambda fuzzer_bin: {"experiments": [], "components": []})
    monkeypatch.setattr(replay_mod, "fuzzer_validator", lambda fuzzer_bin: None)
    monkeypatch.setattr(replay_mod, "propose", lambda change, **kw: ProposalRun(change=change))
    monkeypatch.setattr(replay_mod, "dispatch_batch", lambda gh, inputs, **kw: None)
    monkeypatch.setattr(replay_mod, "wait_for_run", wait)
    with pytest.raises(TimeoutError):
        replay_mod.run(_merge(), gh=MagicMock(), token="t", repo="valkey-io/valkey", fuzzer_repo="o/f",
                       fuzzer_ref="r", fuzzer_dir=tmp_path, fuzzer_bin="f", count=5, random_seeds=2,
                       out_dir=tmp_path / "out", timeout_s=1)
    written = json.loads((tmp_path / "out" / "replay.json").read_text())
    assert (written["run_id"], written["conclusion"], written["dispatched"]) == (99, None, True)
    assert written["wait_error"].startswith("the PR batch did not complete")


def test_a_failed_proposal_still_runs_the_random_baseline(monkeypatch, tmp_path):
    def propose(change, **kw):
        raise RuntimeError("Claude Code failed (rc=1)")

    result, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose)

    ((inputs, _),) = dispatched
    assert (inputs["scenarios"], inputs["random_seeds"], inputs["valkey_parent_sha"]) == ("{}", "2", "a" * 40)
    assert "experiments" not in inputs
    assert result["proposal_error"] == "Claude Code failed (rc=1)"
    assert result["run_id"] == 99


def test_a_merge_needs_full_commits(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="merge sha must be a full commit SHA"):
        replay_mod.run({**_merge(), "sha": "bbbb"}, gh=MagicMock(), token="t", repo="valkey-io/valkey",
                       fuzzer_repo="o/f", fuzzer_ref="r", fuzzer_dir=tmp_path, fuzzer_bin="f", count=5,
                       random_seeds=2, out_dir=tmp_path, timeout_s=1)


# --- Modules -------------------------------------------------------------------


def test_a_module_repository_plans_its_merges_onto_one_valkey_and_leaves_nothing_out(valkey, monkeypatch):
    # Not a commit of the module: a Valkey merge would fail to tell whether it has the rules.
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", "f" * 40)
    shas = _git(valkey, "log", "--first-parent", "--format=%H", "unstable").splitlines()[::-1]
    merges = plan(valkey, f"{shas[0]}..unstable", repository="valkey-io/valkey-json", valkey_sha=PINNED)
    assert [(m["sha"], m["parent"], m["pr"]) for m in merges] == [
        (shas[1], shas[0], 4716), (shas[2], shas[1], 5071), (shas[3], shas[2], None),
    ]
    assert [(m["exclude_experiments"], m["valkey_sha"]) for m in merges] == [([], PINNED)] * 3
    assert plan(valkey, merges=[shas[2]], repository="someone/valkey-json", valkey_sha=PINNED)[0]["sha"] == shas[2]
    with pytest.raises(ValueError, match="needs the full Valkey commit its merges load into"):
        plan(valkey, f"{shas[0]}..unstable", repository="valkey-io/valkey-json", valkey_sha="unstable")
    with pytest.raises(ValueError, match="valkey-io/valkey-search is neither Valkey nor a module the PR batch builds"):
        plan(valkey, f"{shas[0]}..unstable", repository="valkey-io/valkey-search")
    # A Valkey merge carries no pinned Valkey: it is the build.
    monkeypatch.setattr(replay_mod, "REPL_COMPRESSION_RULES", shas[0])
    assert "valkey_sha" not in plan(valkey, f"{shas[0]}..unstable", repository="someone/valkey")[0]


def test_the_valkey_a_module_loads_into_is_resolved_to_a_commit(valkey):
    url = valkey.as_uri()
    _git(valkey, "tag", "-a", "9.1.0", "-m", "9.1.0", "HEAD~1")
    assert resolve_commit("unstable", url) == _git(valkey, "rev-parse", "unstable")
    assert resolve_commit("9.1.0", url) == _git(valkey, "rev-parse", "HEAD~1")
    full = _git(valkey, "rev-parse", "HEAD~2")
    assert resolve_commit(full, url) == full
    with pytest.raises(ValueError, match="cannot fetch nope from"):
        resolve_commit("nope", url)
    for ref in ("--upload-pack=x", "a..b", ""):
        with pytest.raises(ValueError, match="not a branch, tag or commit"):
            resolve_commit(ref, url)


def test_plan_command_pins_a_module_replay_once(valkey, monkeypatch, tmp_path):
    shas = _git(valkey, "log", "--first-parent", "--format=%H", "unstable").splitlines()[::-1]
    monkeypatch.setattr(replay_mod, "VALKEY_URL", valkey.as_uri())
    out = tmp_path / "merges.json"
    assert replay_mod.main(["plan", "--repo-dir", str(valkey), "--repository", "valkey-io/valkey-json",
                            "--valkey-ref", "unstable", "--range", f"{shas[1]}..unstable", "--out", str(out)]) == 0
    assert {m["valkey_sha"] for m in json.loads(out.read_text())} == {shas[3]}
    with pytest.raises(SystemExit):
        replay_mod.main(["plan", "--repo-dir", str(valkey), "--repository", "valkey-json", "--range", "a..b",
                         "--out", str(out)])


def test_a_module_merge_runs_on_the_pinned_valkey_with_the_module_built_at_the_merge_and_its_parent(
    monkeypatch, tmp_path,
):
    docs = [check_scenario(text)[0] for text in (SCENARIO, PROPOSED_SCENARIO)]
    seen = {}

    def propose(change, *, out_dir, valkey_sha, **kw):
        seen["valkey_sha"] = valkey_sha
        return _write(out_dir, change, *docs)

    merge = {**_merge(), "valkey_sha": PINNED, "exclude_experiments": []}
    result, fetched, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose, merge=merge,
                                             repo="valkey-io/valkey-json")

    assert fetched[0].repo == "valkey-io/valkey-json" and seen["valkey_sha"] == PINNED
    ((inputs, _),) = dispatched
    assert (inputs["valkey_repository"], inputs["valkey_sha"], inputs["valkey_ref"]) == ("valkey-io/valkey", PINNED,
                                                                                         PINNED)
    assert (inputs["module_repository"], inputs["module_sha"], inputs["module_parent_sha"]) == (
        "valkey-io/valkey-json", "b" * 40, "a" * 40,
    )
    # One Valkey build: no Valkey parent, even with a proposed experiment; the module's own file.
    assert "experiments" in inputs and not {"valkey_parent_sha", "module_file", "exclude_experiments"} & set(inputs)
    assert result["change"].startswith("valkey-io/valkey-json#4716, merged as bbbbbbbbbbbb")
    assert json.loads((tmp_path / "out" / "replay.json").read_text())["valkey_sha"] == PINNED


def test_a_module_merge_whose_proposal_failed_still_loads_the_module(monkeypatch, tmp_path):
    def propose(change, **kw):
        raise RuntimeError("Claude Code failed (rc=1)")

    merge = {**_merge(), "valkey_sha": PINNED, "exclude_experiments": []}
    _, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose, merge=merge, repo="valkey-io/valkey-json")
    ((inputs, _),) = dispatched
    assert (inputs["scenarios"], inputs["valkey_sha"], inputs["module_sha"]) == ("{}", PINNED, "b" * 40)
    assert "valkey_parent_sha" not in inputs


def test_a_module_merge_needs_its_pinned_valkey(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="merge valkey_sha must be a full commit SHA, got None"):
        replay_mod.run(_merge(), gh=MagicMock(), token="t", repo="valkey-io/valkey-json", fuzzer_repo="o/f",
                       fuzzer_ref="r", fuzzer_dir=tmp_path, fuzzer_bin="f", count=5, random_seeds=2,
                       out_dir=tmp_path, timeout_s=1)


def test_a_valkey_merge_that_loads_a_test_module_sends_it(monkeypatch, tmp_path):
    doc = check_scenario(SCENARIO)[0]

    def propose(change, *, out_dir, **kw):
        run = _write(out_dir, change, doc)
        run.module = ModuleBuild("valkey-io/valkey", change.head_sha, change.base_sha, "tests/modules/datatype.so")
        return run

    _, _, dispatched, _ = _replay(monkeypatch, tmp_path, propose=propose)
    ((inputs, _),) = dispatched
    assert (inputs["valkey_sha"], inputs["valkey_parent_sha"]) == ("b" * 40, "a" * 40)
    assert (inputs["module_repository"], inputs["module_sha"], inputs["module_parent_sha"], inputs["module_file"]) == (
        "valkey-io/valkey", "b" * 40, "a" * 40, "tests/modules/datatype.so",
    )
