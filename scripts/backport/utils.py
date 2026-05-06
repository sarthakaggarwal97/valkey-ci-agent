"""Naming conventions, conflict detection, and validation helpers."""

from __future__ import annotations

import ast
import json
import re
from pathlib import PurePosixPath

import yaml  # type: ignore[import-untyped]

_CONFLICT_MARKERS = re.compile(r"<{7}|={7}|>{7}")


def build_branch_name(source_pr_number: int, target_branch: str) -> str:
    return f"backport/{source_pr_number}-to-{target_branch}"


def build_pr_title(source_pr_title: str, target_branch: str) -> str:
    return f"[Backport {target_branch}] {source_pr_title}"


def has_conflict_markers(content: str) -> bool:
    """Check whether *content* contains git conflict markers.

    Returns ``True`` if any of ``<<<<<<<``, ``=======``, or ``>>>>>>>``
    (seven characters each) appear anywhere in the string.

    """
    return bool(_CONFLICT_MARKERS.search(content))


def validate_c_syntax(content: str) -> bool:
    """Basic C syntax validation — checks for balanced curly braces.

    Returns ``True`` when the number of ``{`` equals the number of ``}``
    and the brace depth never goes negative (i.e. no ``}`` before its
    matching ``{``).

    """
    depth = 0
    for ch in content:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0



def validate_resolved_content(path: str, content: str) -> bool:
    suffix = PurePosixPath(path).suffix.lower()
    if suffix in {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".hxx"}:
        return validate_c_syntax(content)
    if suffix == ".py":
        try:
            ast.parse(content)
        except SyntaxError:
            return False
        return True
    if suffix == ".json":
        try:
            json.loads(content)
        except json.JSONDecodeError:
            return False
        return True
    if suffix in {".yml", ".yaml"}:
        try:
            yaml.safe_load(content)
        except yaml.YAMLError:
            return False
        return True
    return True


def is_whitespace_only_conflict(target_content: str, source_content: str) -> bool:
    """Return ``True`` when *target_content* and *source_content* differ only in whitespace.

    Whitespace differences include spaces, tabs, indentation, trailing
    whitespace, and line endings.  The comparison strips all whitespace
    from both strings before checking equality.

    """
    return _strip_all_whitespace(target_content) == _strip_all_whitespace(source_content)


def _strip_all_whitespace(s: str) -> str:
    return re.sub(r"\s+", "", s)
