from __future__ import annotations

from scripts.common.text_utils import strip_ansi


def test_strip_ansi_removes_color_codes():
    assert strip_ansi("\x1b[31mERROR\x1b[0m") == "ERROR"


def test_strip_ansi_noop_on_clean_text():
    assert strip_ansi("hello world") == "hello world"


def test_strip_ansi_empty_string():
    assert strip_ansi("") == ""


def test_strip_ansi_removes_erase_in_line():
    """CSI K (erase-in-line) is common in progress-bar output."""
    assert strip_ansi("oops\x1b[Kdone") == "oopsdone"


def test_strip_ansi_removes_cursor_movement():
    """CSI A/B/C/D move the cursor and shouldn't pollute log analysis."""
    assert strip_ansi("\x1b[2A\x1b[5Cdone") == "done"


def test_strip_ansi_removes_osc_title():
    """OSC sequences (terminal title) terminated by BEL."""
    assert strip_ansi("\x1b]0;my title\x07hello") == "hello"
