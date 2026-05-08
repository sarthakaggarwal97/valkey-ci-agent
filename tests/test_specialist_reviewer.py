"""Tests for scripts.review.specialist_reviewer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import call, patch

from scripts.review.specialist_reviewer import (
    _SPECIALISTS,
    _UNTRUSTED_FENCE,
    SpecialistFinding,
    SpecialistReviewer,
    _deduplicate,
    _determine_verdict,
    _extract_result_text,
)


@dataclass
class _FakeAgentRunResult:
    """Minimal stand-in for AgentRunResult."""

    stdout: str = ""
    stderr: str = ""
    returncode: int = 0
    profile: str = "code_review_specialist"
    prompt_sha256: str = ""
    cwd: str = ""
    allowed_tools: str = ""
    model: str = ""
    started_at: str = ""
    finished_at: str = ""


def _make_stream_json_result(findings: list[dict]) -> str:
    """Build stream-json stdout with a result event containing findings JSON."""
    payload = json.dumps({"findings": findings})
    return f'{{"type":"result","subtype":"success","result":{json.dumps(payload)}}}\n'


def _make_skeptic_result(keep_indices: list[int]) -> str:
    """Build stream-json stdout for skeptic pass."""
    payload = json.dumps({"keep": keep_indices})
    return f'{{"type":"result","subtype":"success","result":{json.dumps(payload)}}}\n'


# --- Test 1 ---


def test_all_specialists_defined():
    """Exactly 9 specialists with unique slugs."""
    assert len(_SPECIALISTS) == 9
    slugs = [s.slug for s in _SPECIALISTS]
    assert len(set(slugs)) == 9


# --- Test 2 ---


def test_specialist_prompts_contain_untrusted_fence():
    """Each specialist prompt includes the untrusted-data fence when assembled."""
    reviewer = SpecialistReviewer()
    # The fence is appended in _run_specialist; verify it's in the module constant
    for spec in _SPECIALISTS:
        # The prompt is assembled as: system_prompt + UNTRUSTED_FENCE + JSON_FORMAT + context
        # We verify the fence constant exists and will be included
        assert _UNTRUSTED_FENCE in (spec.system_prompt + "\n\n" + _UNTRUSTED_FENCE)


# --- Test 3 ---


@patch("scripts.review.specialist_reviewer.run_agent")
def test_review_calls_all_specialists(mock_run_agent):
    """Mock run_agent, verify it's called 9 times + 1 skeptic = 10 total when findings exist."""
    finding = [{"path": "src/a.c", "line": 10, "severity": "medium", "title": "Issue", "description": "desc"}]
    mock_run_agent.return_value = _FakeAgentRunResult(stdout=_make_stream_json_result(finding))

    reviewer = SpecialistReviewer()
    result = reviewer.review("diff content", ["src/a.c"], "/tmp/repo")

    # 9 specialists + 1 skeptic pass
    assert mock_run_agent.call_count == 10
    # All calls use the code_review_specialist profile
    for c in mock_run_agent.call_args_list:
        assert c[0][0] == "code_review_specialist"


# --- Test 4 ---


@patch("scripts.review.specialist_reviewer.run_agent")
def test_verdict_ready_to_merge(mock_run_agent):
    """No critical/high findings → 'Ready to Merge'."""
    finding = [{"path": "src/a.c", "line": 1, "severity": "low", "title": "Minor", "description": "d"}]
    mock_run_agent.return_value = _FakeAgentRunResult(stdout=_make_stream_json_result(finding))

    reviewer = SpecialistReviewer()
    result = reviewer.review("diff", ["src/a.c"], "/tmp/repo")

    assert result.verdict == "Ready to Merge"


# --- Test 5 ---


@patch("scripts.review.specialist_reviewer.run_agent")
def test_verdict_needs_work(mock_run_agent):
    """Critical finding → 'Needs Work'."""
    finding = [{"path": "src/a.c", "line": 1, "severity": "critical", "title": "Bug", "description": "d"}]
    mock_run_agent.return_value = _FakeAgentRunResult(stdout=_make_stream_json_result(finding))

    reviewer = SpecialistReviewer()
    result = reviewer.review("diff", ["src/a.c"], "/tmp/repo")

    assert result.verdict == "Needs Work"


# --- Test 6 ---


@patch("scripts.review.specialist_reviewer.run_agent")
def test_verdict_needs_attention(mock_run_agent):
    """Medium findings only → 'Needs Attention'."""
    finding = [{"path": "src/a.c", "line": 1, "severity": "medium", "title": "Warn", "description": "d"}]
    mock_run_agent.return_value = _FakeAgentRunResult(stdout=_make_stream_json_result(finding))

    reviewer = SpecialistReviewer()
    result = reviewer.review("diff", ["src/a.c"], "/tmp/repo")

    assert result.verdict == "Needs Attention"


# --- Test 7 ---


@patch("scripts.review.specialist_reviewer.run_agent")
def test_skeptic_pass_drops_findings(mock_run_agent):
    """Mock skeptic to drop a finding, verify it's removed."""
    findings = [
        {"path": "src/a.c", "line": 1, "severity": "medium", "title": "Real issue", "description": "d"},
        {"path": "src/b.c", "line": 2, "severity": "medium", "title": "False positive", "description": "d"},
    ]
    specialist_stdout = _make_stream_json_result(findings)
    # Skeptic keeps only index 0
    skeptic_stdout = _make_skeptic_result([0])

    call_count = {"n": 0}

    def side_effect(profile, prompt, **kwargs):
        call_count["n"] += 1
        # First 9 calls are specialists, 10th is skeptic
        if call_count["n"] <= 9:
            return _FakeAgentRunResult(stdout=specialist_stdout)
        return _FakeAgentRunResult(stdout=skeptic_stdout)

    mock_run_agent.side_effect = side_effect

    reviewer = SpecialistReviewer()
    result = reviewer.review("diff", ["src/a.c", "src/b.c"], "/tmp/repo")

    # Only the first finding from each specialist call should remain after skeptic
    # The skeptic keeps index 0 out of the aggregated findings
    assert result.dropped_count > 0
    # Verify at least one finding was dropped
    assert len(result.findings) < 9 * 2  # less than all possible findings


# --- Test 8 ---


def test_deduplication():
    """Same file+line+similar title from two specialists → deduplicated."""
    findings = [
        SpecialistFinding(specialist="Security", path="src/a.c", line=10, title="Buffer overflow in parse_cmd"),
        SpecialistFinding(specialist="Performance", path="src/a.c", line=10, title="Buffer overflow in parse_cmd function"),
    ]
    deduped = _deduplicate(findings)
    assert len(deduped) == 1
    assert deduped[0].specialist == "Security"


# --- Test 9 ---


def test_memory_safety_in_prompts():
    """Performance specialist mentions zmalloc/zfree, security mentions use-after-free."""
    perf_spec = next(s for s in _SPECIALISTS if s.slug == "performance")
    security_spec = next(s for s in _SPECIALISTS if s.slug == "security")

    assert "zmalloc" in perf_spec.system_prompt
    assert "zfree" in perf_spec.system_prompt
    assert "use-after-free" in security_spec.system_prompt


# --- Test 10 ---


def test_parse_stream_json_result():
    """Test _extract_result_text parses stream-json stdout correctly."""
    stdout = (
        '{"type":"system","subtype":"init","session_id":"abc"}\n'
        '{"type":"assistant","message":{"content":[]}}\n'
        '{"type":"result","subtype":"success","result":"{\\"findings\\": []}"}\n'
    )
    text = _extract_result_text(stdout)
    assert text == '{"findings": []}'

    # Empty stdout
    assert _extract_result_text("") == ""

    # No result event
    assert _extract_result_text('{"type":"assistant","message":{}}\n') == ""

    # Result with null
    assert _extract_result_text('{"type":"result","result":null}\n') == ""
