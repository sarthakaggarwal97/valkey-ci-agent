"""Tests for default-branch issue-to-draft-PR diagnosis."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from scripts.ci_fix import diagnose
from scripts.ci_fix.diagnose import diagnose_issue_failure
from scripts.ci_fix.models import FixPath


def _stream(payload: dict) -> str:
    return json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "result": json.dumps(payload),
        }
    )


def _payload(path: str = "author") -> dict:
    return {
        "path": path,
        "failing_check": "flaky replication test",
        "failing_job": "test-valgrind",
        "root_cause": "the test observes state before replication converges",
        "reasoning": "wait for the causal state transition",
        "confidence": 0.9,
        "build_command": "make",
        "verify_command": "./runtest --single integration/replication",
        "workdir": "",
        "unstable_fix_commit": "",
        "other_failing_checks": [],
    }


def test_issue_diagnosis_accepts_causal_flaky_fix(monkeypatch):
    run_agent = MagicMock(
        return_value=MagicMock(
            returncode=0,
            stdout=_stream(_payload()),
            stderr="",
        )
    )
    monkeypatch.setattr(diagnose, "run_agent", run_agent)

    proposal = diagnose_issue_failure(
        "/logs",
        "/repo",
        issue_title="[TEST-FAILURE] flaky replication test",
        issue_body="failure evidence",
        hint="inspect the state transition",
    )

    assert proposal.path is FixPath.AUTHOR
    prompt = run_agent.call_args.args[1]
    assert "draft pull request" in prompt
    assert "failure evidence" in prompt
    assert "arbitrary sleep" in prompt
    assert "build, lint, configuration" in prompt


def test_issue_diagnosis_refuses_port_path(monkeypatch):
    monkeypatch.setattr(
        diagnose,
        "run_agent",
        MagicMock(
            return_value=MagicMock(
                returncode=0,
                stdout=_stream(_payload("port")),
                stderr="",
            )
        ),
    )

    proposal = diagnose_issue_failure(
        "/logs",
        "/repo",
        issue_title="issue",
        issue_body="body",
    )

    assert proposal.path is FixPath.REFUSE
    assert proposal.verify_command == ""
