"""Tests for the shared log format, run annotations, and groups."""

from __future__ import annotations

import logging

from scripts.common import logging_utils
from scripts.common.logging_utils import (
    _TagFormatter,
    capture_untrusted_output,
    emit_untrusted_output,
    log_group,
    log_outcome,
)


def _record(path: str) -> logging.LogRecord:
    return logging.LogRecord("x", logging.INFO, path, 1, "hello %s", ("world",), None)


def test_records_are_tagged_by_feature_area_not_module_path():
    fmt = _TagFormatter("[%(tag)s] %(message)s")
    assert fmt.format(_record("/w/scripts/release/reconcile.py")) == "[release] hello world"
    assert fmt.format(_record("/w/scripts/ci_fix/main.py")) == "[ci-fix] hello world"
    # Shared helpers act for their caller, so the file is the useful tag.
    assert fmt.format(_record("/w/scripts/common/proc.py")) == "[proc] hello world"


def test_every_physical_log_line_is_prefixed_and_terminal_safe():
    record = logging.LogRecord(
        "x",
        logging.INFO,
        "/w/scripts/ci_fix/engine.py",
        1,
        "::notice::first\r\n::stop-commands::TOKEN\n\x1b[2K::add-mask::secret\x00\u202e",
        (),
        None,
    )

    rendered = _TagFormatter("%(message)s").format(record)

    assert rendered.splitlines() == [
        "| ::notice::first",
        "| ::stop-commands::TOKEN",
        r"| \x1b[2K::add-mask::secret\x00\u202e",
    ]
    assert all(not line.startswith("::") for line in rendered.splitlines())


def test_captured_child_output_cannot_emit_a_workflow_command(capsys):
    emit_untrusted_output(
        b"ordinary\n::add-mask::secret\r::stop-commands::TOKEN\x00\n",
        label="target setup",
    )

    lines = capsys.readouterr().err.splitlines()
    assert lines == [
        "[target setup] ordinary",
        "[target setup] : :add-mask: :secret",
        r"[target setup] : :stop-commands: :TOKEN\x00",
    ]
    assert all("::" not in line and not line.startswith("::") for line in lines)


def test_untrusted_output_capture_is_bounded_but_keeps_draining(capsys):
    with capture_untrusted_output(label="child", limit=12) as output:
        output.write(b"::error::forged\n" + b"x" * 100_000)

    rendered = capsys.readouterr().err
    assert "::" not in rendered
    assert len(rendered) < 1_000
    assert "output truncated after 12 bytes" in rendered


def test_outcome_is_annotated_only_in_actions(monkeypatch, capsys, caplog):
    log = logging.getLogger("test.outcome")
    caplog.set_level(logging.INFO, logger="test.outcome")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    log_outcome(log, logging.INFO, "done %d", 1)
    assert capsys.readouterr().err == ""

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    log_outcome(log, logging.WARNING, "half %s", "done")
    log_outcome(log, logging.ERROR, "failed")
    log_outcome(log, logging.INFO, "ok")
    err = capsys.readouterr().err.splitlines()
    assert err == ["::warning::half done", "::error::failed", "::notice::ok"]
    assert [r.getMessage() for r in caplog.records][-3:] == ["half done", "failed", "ok"]


def test_outcome_cannot_forge_a_second_workflow_command(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    log_outcome(logging.getLogger("t"), logging.INFO, "%s", "a\n::error::forged 100%")
    assert capsys.readouterr().err.splitlines() == ["::notice::a ::error::forged 100%25"]


def test_annotate_escapes_command_data_for_direct_callers(monkeypatch, capsys):
    from scripts.common.logging_utils import annotate

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    annotate(logging.ERROR, "a\r\n::error::forged 100%")
    assert capsys.readouterr().err.splitlines() == ["::error::a%0D%0A::error::forged 100%25"]


def test_groups_do_not_nest(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with log_group("outer"), log_group("inner"):
        pass
    assert capsys.readouterr().err.splitlines() == ["::group::outer", "::endgroup::"]
    assert logging_utils._group_open is False


def test_group_closes_when_the_body_raises(monkeypatch, capsys):
    import pytest

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(RuntimeError), log_group("work"):
        raise RuntimeError("boom")
    assert capsys.readouterr().err.splitlines()[-1] == "::endgroup::"
    assert logging_utils._group_open is False


def test_outcome_log_line_cannot_forge_a_workflow_command(monkeypatch, capsys, caplog):
    """The raw log line is printed before the annotation; it must be one line too."""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    caplog.set_level(logging.INFO, logger="t.forge")
    log_outcome(logging.getLogger("t.forge"), logging.INFO, "PR %s", "x\n::warning::forged")
    assert caplog.records[-1].getMessage() == "PR x ::warning::forged"
    assert capsys.readouterr().err.splitlines() == ["::notice::PR x ::warning::forged"]


def test_outcome_with_a_literal_percent_and_no_args(caplog):
    caplog.set_level(logging.INFO, logger="t.pct")
    log_outcome(logging.getLogger("t.pct"), logging.INFO, "100% done")
    assert caplog.records[-1].getMessage() == "100% done"
