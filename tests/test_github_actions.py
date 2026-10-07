import pytest

from scripts.common.github_actions import write_outputs


def test_write_outputs_appends_single_line_values(monkeypatch, tmp_path) -> None:
    path = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(path))

    assert write_outputs({"first": "one", "second": "two"})
    assert path.read_text(encoding="utf-8") == "first=one\nsecond=two\n"


def test_write_outputs_refuses_multiline_values(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))

    with pytest.raises(ValueError, match="multiline workflow output refused"):
        write_outputs({"unsafe": "one\nforged=true"})
