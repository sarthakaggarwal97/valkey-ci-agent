"""Static enforcement for the single audited production Git execution API."""

from __future__ import annotations

import ast
from pathlib import Path


def test_production_has_no_direct_subprocess_git_invocations() -> None:
    offenders: list[str] = []
    for path in sorted(Path("scripts").rglob("*.py")):
        if path == Path("scripts/common/proc.py"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            function = node.func
            if not (
                isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Name)
                and function.value.id == "subprocess"
                and function.attr
                in {"run", "Popen", "check_call", "check_output", "call"}
            ):
                continue
            argv = node.args[0]
            if not isinstance(argv, (ast.List, ast.Tuple)) or not argv.elts:
                continue
            executable = argv.elts[0]
            if (
                isinstance(executable, ast.Constant)
                and executable.value == "git"
            ):
                offenders.append(f"{path}:{node.lineno}")
    assert offenders == []
