"""Tests for the shared log format, highlights and run annotations."""

from __future__ import annotations

import logging

from scripts.common import logging_utils
from scripts.common.logging_utils import LOG_HIGHLIGHT_RULE, log_outcome


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


def test_line_is_time_level_and_message():
    import re

    record = logging.LogRecord("x", logging.WARNING, "/w/a.py", 1, "PR #%d", (7,), None)
    line = logging.Formatter(logging_utils.LOG_FORMAT).format(record)
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} WARNING PR #7", line)


def test_highlight_frames_the_record_with_rules_at_its_level(caplog):
    from scripts.common.logging_utils import log_highlight

    caplog.set_level(logging.INFO, logger="t.hl")
    log_highlight(logging.getLogger("t.hl"), "PR #%d", 7, level=logging.WARNING)
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, LOG_HIGHLIGHT_RULE),
        (logging.WARNING, "PR #7"),
        (logging.WARNING, LOG_HIGHLIGHT_RULE),
    ]


def test_configure_logging_quiets_http_clients_and_honours_runner_debug(monkeypatch):
    root = logging.getLogger()
    saved = root.handlers[:], root.level
    try:
        root.handlers = []
        monkeypatch.setenv("RUNNER_DEBUG", "1")
        logging_utils.configure_logging()
        assert root.level == logging.DEBUG
        assert logging.getLogger("urllib3").level == logging.WARNING
        assert logging.getLogger("github").level == logging.INFO

        root.handlers = []
        monkeypatch.delenv("RUNNER_DEBUG")
        logging_utils.configure_logging()
        assert root.level == logging.INFO
    finally:
        root.handlers, level = saved
        root.setLevel(level)


def test_outcome_is_printed_once_in_actions(monkeypatch):
    from scripts.common.logging_utils import _SkipAnnotated

    record = logging.LogRecord("x", logging.INFO, "/w/a.py", 1, "done", (), None)
    record.annotated = True
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert _SkipAnnotated().filter(record) is False
    monkeypatch.delenv("GITHUB_ACTIONS")
    assert _SkipAnnotated().filter(record) is True


def test_box_is_the_title_and_lines_between_rules(caplog):
    from scripts.common.logging_utils import log_box

    caplog.set_level(logging.INFO, logger="t.box")
    log_box(logging.getLogger("t.box"), "TITLE 100%", ["a", "b %s"], level=logging.WARNING)
    assert caplog.messages == [LOG_HIGHLIGHT_RULE, "TITLE 100%", "a", "b %s", LOG_HIGHLIGHT_RULE]
    assert {r.levelno for r in caplog.records} == {logging.WARNING}
