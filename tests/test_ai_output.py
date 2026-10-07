from scripts.common.ai_output import extract_json_object, extract_result_text


def test_extract_result_text_uses_the_last_valid_result_event() -> None:
    stdout = "\n".join(
        (
            "not json",
            '{"type":"result","result":" first "}',
            '{"type":"assistant","text":"ignored"}',
            '{"type":"result","result":{"status":"ok"}}',
        )
    )

    assert extract_result_text(stdout) == '{"status": "ok"}'


def test_extract_json_object_reads_a_structured_result_value() -> None:
    stdout = '{"type":"result","result":{"approved":true}}'

    assert extract_json_object(stdout, required_key="approved") == {"approved": True}


def test_extract_json_object_fails_closed_on_an_empty_result() -> None:
    stdout = "\n".join(
        (
            '{"type":"assistant","message":{"input":{"approved":true}}}',
            '{"type":"result","result":"  "}',
        )
    )

    assert extract_json_object(stdout, required_key="approved") is None


def test_extract_json_object_reads_bare_output_without_a_result_event() -> None:
    assert extract_json_object('verdict: {"approved": false}', required_key="approved") == {
        "approved": False
    }
