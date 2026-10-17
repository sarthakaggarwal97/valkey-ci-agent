from __future__ import annotations

from scripts.common.text_utils import compact_text, escape_cell, split_table_row, strip_ansi


def test_strip_ansi_removes_color_codes():
    assert strip_ansi("\x1b[31mERROR\x1b[0m") == "ERROR"


def test_strip_ansi_noop_on_clean_text():
    assert strip_ansi("hello world") == "hello world"


def test_strip_ansi_empty_string():
    assert strip_ansi("") == ""


def test_compact_text_normalizes_and_truncates():
    assert compact_text("  one \n two  ", 20) == "one two"
    assert compact_text("one two three", 8) == "one two…"


class TestEscapeCell:
    def test_escapes_pipe_and_newline(self) -> None:
        assert escape_cell("a | b\nc") == "a \\| b c"

    def test_plain_text_unchanged(self) -> None:
        assert escape_cell("normal title") == "normal title"

    def test_collapses_cr_and_crlf(self) -> None:
        # A raw \r (CRLF or lone CR) in a contributor PR title must not survive
        # into the cell, or it breaks the Markdown table row. Any run of CR/LF
        # collapses to one space.
        assert escape_cell("fix\r\nbug") == "fix bug"      # CRLF
        assert escape_cell("fix\rbug") == "fix bug"        # lone CR
        assert escape_cell("a\r\n\r\nb") == "a b"          # consecutive breaks
        assert "\r" not in escape_cell("fix\r\nbug")

    def test_backslash_before_pipe_stays_escaped(self) -> None:
        # A pre-existing backslash right before a pipe must not consume the pipe's
        # escape. "a\|b" -> "a\\\|b" renders as literal "a\|b" (no cell break),
        # not "a\\|b" (literal backslash + a live delimiter that splits the row).
        assert escape_cell("a\\|b") == "a\\\\\\|b"

    def test_lone_backslash_escaped(self) -> None:
        assert escape_cell("C:\\path") == "C:\\\\path"

    def test_cells_round_trip_through_split_table_row(self) -> None:
        values = ["a | b", "C:\\path", "a\\|b", "trailing \\", "plain"]
        row = "| " + " | ".join(escape_cell(value) for value in values) + " |"
        assert split_table_row(row) == values

