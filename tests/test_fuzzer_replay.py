"""Tests for replaying merged Valkey pull requests through the fuzzer."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import scripts.fuzzer.replay as replay_mod
from scripts.fuzzer.propose import Change, Proposal, ProposalRun, _render_scenario, check_scenario
from scripts.fuzzer.replay import describe, plan, request_id

SCENARIO = "seed: 7\nexperiment: {id: valkey.forkless-save}\ndeployment: {mode: standalone}\n"


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


def test_a_merge_is_named_by_its_pull_request_and_commit():
    merge = {"index": 7, "sha": "e082696cf" + "0" * 31, "pr": 4716, "subject": "Fix the forkless save (#4716)"}
    assert describe(merge, "valkey-io/valkey") == (
        "valkey-io/valkey#4716, merged as e082696cf000: Fix the forkless save (#4716)"
    )
    assert describe({**merge, "pr": None}, "valkey-io/valkey").startswith("valkey-io/valkey, merged as e082696cf000")
    assert request_id(merge) == "replay-007-e082696cf"


def _merge() -> dict:
    return {"index": 3, "sha": "b" * 40, "parent": "a" * 40, "pr": 4716, "subject": "Fix the forkless save (#4716)"}


def _change() -> Change:
    return Change(repo="valkey-io/valkey", pr_number=None, title="1 commit(s)", base_sha="a" * 40,
                  head_sha="b" * 40, diff="diff --git a/x b/x\n")


def _replay(monkeypatch, tmp_path, *, propose, merge=None, random_seeds=2):
    fetched, dispatched, waited = [], [], []
    monkeypatch.setattr(replay_mod, "fetch_change", lambda gh, token, request: fetched.append(request) or _change())
    monkeypatch.setattr(replay_mod, "load_catalog", lambda fuzzer_bin: {"experiments": [], "components": []})
    monkeypatch.setattr(replay_mod, "fuzzer_validator", lambda fuzzer_bin: None)
    monkeypatch.setattr(replay_mod, "propose", propose)
    monkeypatch.setattr(replay_mod, "dispatch_batch", lambda gh, inputs, **kw: dispatched.append((inputs, kw)))
    batch = MagicMock(id=99, html_url="https://x/runs/99", conclusion="failure")
    monkeypatch.setattr(replay_mod, "wait_for_run", lambda gh, **kw: waited.append(kw) or batch)
    result = replay_mod.run(
        merge or _merge(), gh=MagicMock(), token="t", repo="valkey-io/valkey", fuzzer_repo="owner/valkey-fuzzer",
        fuzzer_ref="generic-fuzzer", fuzzer_dir=tmp_path, fuzzer_bin="valkey-fuzzer", count=5,
        random_seeds=random_seeds,
        out_dir=tmp_path / "out", timeout_s=60,
    )
    return result, fetched, dispatched, waited


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
    assert where == {"fuzzer_repo": "owner/valkey-fuzzer", "workflow": "fuzzer-pr-batch.yml",
                     "ref": "generic-fuzzer"}
    assert waited[0]["request_id"] == "replay-003-bbbbbbbbb"
    assert (result["proposed"], result["dropped"], result["run_id"], result["conclusion"]) == (1, 1, 99, "failure")
    written = json.loads((tmp_path / "out" / "replay.json").read_text())
    assert written["change"] == "valkey-io/valkey#4716, merged as bbbbbbbbbbbb: Fix the forkless save (#4716)"


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
    assert (inputs["scenarios"], inputs["random_seeds"]) == ("{}", "2")
    assert result["proposal_error"] == "Claude Code failed (rc=1)"
    assert result["run_id"] == 99


def test_a_merge_needs_full_commits(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="merge sha must be a full commit SHA"):
        replay_mod.run({**_merge(), "sha": "bbbb"}, gh=MagicMock(), token="t", repo="valkey-io/valkey",
                       fuzzer_repo="o/f", fuzzer_ref="r", fuzzer_dir=tmp_path, fuzzer_bin="f", count=5,
                       random_seeds=2, out_dir=tmp_path, timeout_s=1)
