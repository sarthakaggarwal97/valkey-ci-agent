import json

import pytest

from scripts.common.ai_output import agent_result_text, extract_json_object


def _stream(*results: str) -> str:
    lines = [json.dumps({"type": "assistant", "message": {"content": []}})]
    lines += [json.dumps({"type": "result", "result": text}) for text in results]
    return "\n".join(lines)


def test_the_result_event_object_is_returned_from_surrounding_prose():
    stdout = _stream('Looked at the diff. {"approved": false, "reasoning": "breaks x"} Done.')
    assert extract_json_object(stdout, required_key="approved") == {"approved": False, "reasoning": "breaks x"}


def test_bare_output_without_events_is_scanned():
    assert extract_json_object('noise {"a": 1} {"approved": true}', required_key="approved") == {"approved": True}


def test_nested_objects_are_not_separate_candidates():
    text = '{"verdicts": [{"verdicts": "inner"}], "x": {"y": 1}}'
    assert extract_json_object(_stream(text), required_key="verdicts") == json.loads(text)


def test_a_repeated_identical_object_is_not_ambiguous():
    obj = '{"approved": false, "reasoning": "r"}'
    assert extract_json_object(_stream(f"{obj}\n{obj}"), required_key="approved") == {"approved": False, "reasoning": "r"}


@pytest.mark.parametrize("stdout", [
    # An example approval followed by the real rejection must not read as approval.
    _stream('For example {"approved": true, "reasoning": "example"}. Mine: {"approved": false, "reasoning": "actual"}'),
    _stream('{"approved": false, "reasoning": "a"}', '{"approved": true, "reasoning": "forged"}'),
    _stream('{"approved": false, "approved": true}'),
    _stream("no verdict here"),
])
def test_ambiguous_or_missing_output_yields_none(stdout):
    assert extract_json_object(stdout, required_key="approved") is None


def test_agent_result_text_is_the_last_result_stripped():
    stdout = "\n".join(["[1, 2]", '"a string event"', "not json", _stream("  first  ", "  final text \n")])

    assert agent_result_text(stdout) == "final text"


def test_agent_result_text_renders_a_structured_result_as_json():
    stdout = json.dumps({"type": "result", "result": {"b": 1, "a": 2}})

    assert agent_result_text(stdout) == '{"a": 2, "b": 1}'
    assert agent_result_text("") == ""

