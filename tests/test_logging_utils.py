"""Tests for the shared log format, run annotations, and groups."""

from __future__ import annotations

import logging

from scripts.common import logging_utils
from scripts.common.logging_utils import _TagFormatter, log_outcome


def _record(path: str) -> logging.LogRecord:
    return logging.LogRecord("x", logging.INFO, path, 1, "hello %s", ("world",), None)


def test_records_are_tagged_by_feature_area_not_module_path():
    fmt = _TagFormatter("[%(tag)s] %(message)s")
    assert fmt.format(_record("/w/scripts/release/reconcile.py")) == "[release] hello world"
    assert fmt.format(_record("/w/scripts/ci_fix/main.py")) == "[ci-fix] hello world"
    # Shared helpers act for their caller, so the file is the useful tag.
    assert fmt.format(_record("/w/scripts/common/proc.py")) == "[proc] hello world"


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


def _console_record(level: int, msg: str, **extra: object) -> logging.LogRecord:
    record = logging.LogRecord("x", level, "/w/scripts/backport/sweep.py", 1, msg, (), None)
    record.__dict__.update(extra)
    return record


def test_actions_line_is_tag_and_message_with_level_only_when_not_info():
    from scripts.common.logging_utils import _ConsoleFormatter

    fmt = _ConsoleFormatter(actions=True, colour=False)
    assert fmt.format(_console_record(logging.INFO, "hello")) == "[backport] hello"
    assert fmt.format(_console_record(logging.WARNING, "careful")) == "[backport] WARNING careful"
    assert (fmt.format(_console_record(logging.INFO, "PR #1", highlight="success"))
            == "[backport] ▶ PR #1")


def test_actions_colours_warnings_and_highlights():
    from scripts.common.logging_utils import _ConsoleFormatter

    fmt = _ConsoleFormatter(actions=True, colour=True)
    warning = fmt.format(_console_record(logging.WARNING, "careful"))
    assert "\x1b[33mWARNING careful\x1b[0m" in warning
    accepted = fmt.format(_console_record(logging.INFO, "ok", highlight="success"))
    assert "\x1b[1;32m▶ ok\x1b[0m" in accepted
    rejected = fmt.format(_console_record(logging.WARNING, "no", highlight="notice"))
    assert "\x1b[1;33mWARNING ▶ no\x1b[0m" in rejected


def test_local_line_keeps_time_and_level():
    from scripts.common.logging_utils import _ConsoleFormatter

    line = _ConsoleFormatter(actions=False, colour=False).format(
        _console_record(logging.INFO, "hello", highlight="notice"))
    assert line.endswith(" INFO    [backport] ▶ hello")


def test_highlight_is_one_record_at_the_requested_level(caplog):
    from scripts.common.logging_utils import log_highlight

    caplog.set_level(logging.INFO, logger="t.hl")
    log_highlight(logging.getLogger("t.hl"), "PR #%d", 7, level=logging.WARNING)
    assert [(r.levelno, r.getMessage(), r.highlight) for r in caplog.records] == [
        (logging.WARNING, "PR #7", "notice"),
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

    record = _console_record(logging.INFO, "done", annotated=True)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert _SkipAnnotated().filter(record) is False
    monkeypatch.delenv("GITHUB_ACTIONS")
    assert _SkipAnnotated().filter(record) is True


def test_box_is_a_rule_a_highlighted_title_the_lines_and_a_rule(caplog):
    from scripts.common.logging_utils import BOX_RULE, log_box

    caplog.set_level(logging.INFO, logger="t.box")
    log_box(logging.getLogger("t.box"), "TITLE 100%", ["a", "b %s"], level=logging.WARNING)
    assert caplog.messages == [BOX_RULE, "TITLE 100%", "a", "b %s", BOX_RULE]
    assert {r.levelno for r in caplog.records} == {logging.WARNING}
    assert [getattr(r, "highlight", None) for r in caplog.records] == [
        None, "notice", None, None, None]
