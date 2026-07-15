"""Tests for credential-separated fuzzer monitoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.common.ai_evidence import finalize_ai_evidence
from scripts.common.phase_artifact import ArtifactError, sha256_bytes, write_json
from scripts.common.publication_manifest import load_publication_manifest
from scripts.fuzzer.models import FuzzerRunAnalysis, FuzzerSignal
from scripts.fuzzer.phase_artifact import load_analyzed, load_discovery
from scripts.fuzzer.phased import _select_unprocessed_runs, publish
from scripts.fuzzer.schema import analysis_to_dict

SHA_A = "a" * 40
SHA_B = "b" * 40


def _run(run_id: int, *, status: str = "completed") -> MagicMock:
    return MagicMock(
        id=run_id,
        status=status,
        conclusion="failure",
        html_url=f"https://github.com/valkey-io/valkey-fuzzer/actions/runs/{run_id}",
        head_sha=SHA_A,
    )


def test_selects_oldest_completed_runs_after_cursor():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(105),
        _run(104),
        _run(103),
        _run(102),
        _run(100),
    ]
    selected = _select_unprocessed_runs(workflow, cursor=100, max_runs=2)
    assert [run.id for run in selected] == [102, 103]


def test_pending_run_blocks_later_completed_run_from_advancing_cursor():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(104),
        _run(103, status="in_progress"),
        _run(102),
        _run(100),
    ]
    selected = _select_unprocessed_runs(workflow, cursor=100)
    assert [run.id for run in selected] == [102]


def test_bootstrap_uses_newest_completed_run():
    workflow = MagicMock()
    workflow.get_runs.return_value = [
        _run(105, status="in_progress"),
        _run(104),
        _run(103),
    ]
    selected = _select_unprocessed_runs(workflow, cursor=0)
    assert [run.id for run in selected] == [104]


def _write_artifact(root, *, run_ids=(101, 102)):
    run_entries = []
    for run_id in run_ids:
        payload = b'{"results": [{"success": true}]}'
        relative = f"runs/{run_id}/artifacts/results.json"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        run_entries.append({
            "run_id": run_id,
            "run_url": (
                "https://github.com/valkey-io/valkey-fuzzer/"
                f"actions/runs/{run_id}"
            ),
            "conclusion": "failure",
            "head_sha": SHA_A,
            "evidence_status": "ready",
            "evidence_error": "",
            "files": [{
                "path": relative,
                "sha256": sha256_bytes(payload),
                "bytes": len(payload),
            }],
        })
    write_json(root / "discovery.json", {
        "schema_version": 1,
        "kind": "fuzzer-discovery",
        "repository": "valkey-io/valkey-fuzzer",
        "workflow_file": "fuzzer-run.yml",
        "expected_cursor": 100,
        "bootstrap": False,
        "runs": run_entries,
    })
    discovery = load_discovery(root)

    refs = []
    for index, run_id in enumerate(run_ids):
        anomalous = index == len(run_ids) - 1
        analysis = FuzzerRunAnalysis(
            repo="valkey-io/valkey-fuzzer",
            workflow_file="fuzzer-run.yml",
            run_id=run_id,
            run_url=(
                "https://github.com/valkey-io/valkey-fuzzer/"
                f"actions/runs/{run_id}"
            ),
            conclusion="failure",
            head_sha=SHA_A,
            overall_status="anomalous" if anomalous else "normal",
            triage_verdict=(
                "likely-core-valkey-bug" if anomalous
                else "expected-chaos-noise"
            ),
            summary="A bounded analysis result.",
            anomalies=(
                [FuzzerSignal("Node crash", "critical", "one match")]
                if anomalous else []
            ),
            tested_valkey_sha=SHA_B,
            incident_fingerprint="1" * 20,
            suggested_labels=["possible-valkey-bug"] if anomalous else [],
        )
        relative = f"analyses/{run_id}.json"
        digest = write_json(root / relative, analysis_to_dict(analysis))
        refs.append({"run_id": run_id, "file": relative, "sha256": digest})
    evidence_file, evidence_sha = finalize_ai_evidence(root)
    write_json(root / "analyzed.json", {
        "schema_version": 1,
        "kind": "fuzzer-analyzed",
        "discovery_sha256": discovery.manifest_sha256,
        "analyses": refs,
        "ai_evidence_file": evidence_file,
        "ai_evidence_sha256": evidence_sha,
    })


def test_phase_artifact_round_trip_and_identity_binding(tmp_path):
    _write_artifact(tmp_path)
    artifact = load_analyzed(tmp_path)
    assert [item.analysis.run_id for item in artifact.analyses] == [101, 102]

    analysis_path = tmp_path / "analyses/101.json"
    payload = analysis_path.read_text()
    analysis_path.write_text(payload.replace('"run_id":101', '"run_id":999'))
    with pytest.raises(ArtifactError, match="digest"):
        load_analyzed(tmp_path)


def test_discovery_rejects_unknown_keys(tmp_path):
    _write_artifact(tmp_path, run_ids=(101,))
    manifest = (tmp_path / "discovery.json").read_text()
    (tmp_path / "discovery.json").write_text(
        manifest.replace('"bootstrap":false', '"bootstrap":false,"invented":true'),
    )
    with pytest.raises(ArtifactError, match="unknown"):
        load_discovery(tmp_path)


def test_publisher_advances_cursor_only_after_each_reconciled_event(
    tmp_path,
    monkeypatch,
):
    _write_artifact(tmp_path)
    state_store = MagicMock()
    state_store.read.return_value = MagicMock(cursor=100)
    issue_publisher = MagicMock()
    issue_publisher.upsert.return_value = ("created", "https://github.com/x/issues/1")
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    with patch("scripts.fuzzer.phased.Github", return_value=MagicMock()), \
         patch("scripts.fuzzer.phased.FuzzerStateStore", return_value=state_store), \
         patch(
             "scripts.fuzzer.phased.IssueDedupPublisher",
             return_value=issue_publisher,
         ):
        result = publish(token="token", artifact_directory=tmp_path)

    assert result["final_cursor"] == 102
    assert state_store.advance.call_count == 2
    assert state_store.advance.call_args_list[0].kwargs["expected_cursor"] == 100
    assert state_store.advance.call_args_list[1].kwargs["expected_cursor"] == 101
    issue_publisher.upsert.assert_called_once()
    assert (
        issue_publisher.upsert.call_args.kwargs["idempotency_key"]
        == "fuzzer-run.yml:102"
    )
    publication = load_publication_manifest(
        tmp_path,
        expected_kind="fuzzer-publication",
        final_state_keys={
            "repository",
            "workflow_file",
            "initial_cursor",
            "final_cursor",
            "runs",
        },
        expected_source_file="analyzed.json",
        expected_source_sha256=load_analyzed(tmp_path).manifest_sha256,
    )
    assert publication.final_state["final_cursor"] == 102


def test_publisher_resumes_after_partial_batch(tmp_path, monkeypatch):
    _write_artifact(tmp_path)
    state_store = MagicMock()
    state_store.read.return_value = MagicMock(cursor=101)
    issue_publisher = MagicMock()
    issue_publisher.upsert.return_value = ("updated", "https://github.com/x/issues/1")
    monkeypatch.setenv("PUBLISHER_IDENTITY", "publisher[bot]")

    with patch("scripts.fuzzer.phased.Github", return_value=MagicMock()), \
         patch("scripts.fuzzer.phased.FuzzerStateStore", return_value=state_store), \
         patch(
             "scripts.fuzzer.phased.IssueDedupPublisher",
             return_value=issue_publisher,
         ):
        result = publish(token="token", artifact_directory=tmp_path)

    assert result["runs"][0]["action"] == "already-processed"
    state_store.advance.assert_called_once()
    assert state_store.advance.call_args.kwargs["expected_cursor"] == 101
