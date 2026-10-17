from __future__ import annotations

import pytest

from scripts.common.job_summary import write_outputs


def test_outputs_append_to_the_github_output_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    output = tmp_path / "outputs"
    output.write_text("earlier=1\n")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))

    write_outputs({"tag": "9.0.1", "plan": "[]"})

    assert output.read_text() == "earlier=1\ntag=9.0.1\nplan=[]\n"


def test_outputs_are_printed_outside_actions(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)

    write_outputs({"mode": "local"})

    assert capsys.readouterr().out == "mode=local\n"


@pytest.mark.parametrize("value", ["safe\nforged=true", "safe\rforged=true"])
def test_outputs_refuse_multiline_values_before_writing_any(
    monkeypatch: pytest.MonkeyPatch, tmp_path, value: str,
) -> None:
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))

    with pytest.raises(ValueError, match="multiline workflow output refused for issue_url"):
        write_outputs({"issue_number": "42", "issue_url": value})
    assert not output.exists()
