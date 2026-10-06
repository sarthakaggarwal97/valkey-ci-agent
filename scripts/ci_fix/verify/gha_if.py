"""Evaluate a GitHub Actions ``if:`` expression against known inputs.

The Daily planner uses this to check which jobs a dispatch would start, so it
can pick the narrowest ``skipjobs`` value and refuse one that would not start
the target job at all. It covers the expression language the workflows use
(literals, context paths, ``!``, ``&&``, ``||``, ``==``, ``!=``, parentheses
and the common functions). Anything else raises :class:`UnsupportedExpression`,
and the caller falls back to not narrowing.

Semantics follow GitHub's: ``&&``/``||`` short-circuit and return an operand,
string comparison and ``contains`` ignore case, a missing context value is
null.
"""

from __future__ import annotations

import re
from typing import Any

_TOKEN_RE = re.compile(
    r"\s*(?:(?P<op>\|\||&&|==|!=|!|\(|\)|,)|(?P<str>'(?:[^']|'')*')|(?P<num>\d+(?:\.\d+)?)"
    r"|(?P<name>[A-Za-z_][A-Za-z0-9_-]*(?:\.(?:[A-Za-z_*][A-Za-z0-9_*-]*))*))"
)


class UnsupportedExpression(ValueError):
    """The expression uses syntax or a function this evaluator does not model."""


def evaluate(expression: Any, context: dict[str, Any]) -> bool:
    """Whether ``expression`` (an ``if:`` value) is truthy in ``context``."""
    if isinstance(expression, bool):
        return expression
    if expression is None or str(expression).strip() == "":
        return True
    text = str(expression).strip()
    if text.startswith("${{") and text.endswith("}}"):
        text = text[3:-2]
    parser = _Parser(_tokenize(text), context)
    value = parser.expression()
    if parser.peek() is not None:
        raise UnsupportedExpression(f"unexpected {parser.peek()!r}")
    return _truthy(value)


def _tokenize(text: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    position = 0
    while position < len(text):
        if text[position:].strip() == "":
            break
        match = _TOKEN_RE.match(text, position)
        if match is None:
            raise UnsupportedExpression(f"cannot read {text[position:position + 30]!r}")
        kind = match.lastgroup or ""
        tokens.append((kind, match.group(kind)))
        position = match.end()
    return tokens


class _Parser:
    def __init__(self, tokens: list[tuple[str, str]], context: dict[str, Any]) -> None:
        self._tokens = tokens
        self._index = 0
        self._context = context

    def peek(self) -> str | None:
        return self._tokens[self._index][1] if self._index < len(self._tokens) else None

    def _take(self) -> tuple[str, str]:
        if self._index >= len(self._tokens):
            raise UnsupportedExpression("unexpected end of expression")
        token = self._tokens[self._index]
        self._index += 1
        return token

    def _expect(self, value: str) -> None:
        if self._take()[1] != value:
            raise UnsupportedExpression(f"expected {value!r}")

    def expression(self) -> Any:
        value = self._and()
        while self.peek() == "||":
            self._take()
            right = self._and()
            value = value if _truthy(value) else right
        return value

    def _and(self) -> Any:
        value = self._compare()
        while self.peek() == "&&":
            self._take()
            right = self._compare()
            value = right if _truthy(value) else value
        return value

    def _compare(self) -> Any:
        value = self._unary()
        while self.peek() in ("==", "!="):
            operator = self._take()[1]
            equal = _equal(value, self._unary())
            value = equal if operator == "==" else not equal
        return value

    def _unary(self) -> Any:
        if self.peek() == "!":
            self._take()
            return not _truthy(self._unary())
        return self._primary()

    def _primary(self) -> Any:
        kind, value = self._take()
        if value == "(" and kind == "op":
            inner = self.expression()
            self._expect(")")
            return inner
        if kind == "str":
            return value[1:-1].replace("''", "'")
        if kind == "num":
            return float(value)
        if kind != "name":
            raise UnsupportedExpression(f"unexpected {value!r}")
        if value in ("true", "false"):
            return value == "true"
        if value == "null":
            return None
        if self.peek() == "(":
            self._take()
            args: list[Any] = []
            if self.peek() != ")":
                args.append(self.expression())
                while self.peek() == ",":
                    self._take()
                    args.append(self.expression())
            self._expect(")")
            return _call(value, args)
        return _lookup(self._context, value)


def _truthy(value: Any) -> bool:
    if isinstance(value, float):
        return value != 0
    return bool(value)


def _equal(left: Any, right: Any) -> bool:
    if isinstance(left, str) and isinstance(right, str):
        return left.lower() == right.lower()
    return bool(left == right)


def _call(name: str, args: list[Any]) -> Any:
    function = name.lower()
    if function in ("always", "success") and not args:
        return True
    if function in ("failure", "cancelled") and not args:
        return False
    if function == "contains" and len(args) == 2:
        haystack, needle = args
        if isinstance(haystack, list):
            return any(_equal(item, needle) for item in haystack)
        return str(needle if needle is not None else "").lower() in str(haystack if haystack is not None else "").lower()
    if function in ("startswith", "endswith") and len(args) == 2:
        text, part = (str(arg if arg is not None else "").lower() for arg in args)
        return text.startswith(part) if function == "startswith" else text.endswith(part)
    raise UnsupportedExpression(f"function {name}() is not modelled")


def _lookup(context: dict[str, Any], path: str) -> Any:
    current: Any = context
    for part in path.split("."):
        if part == "*" or not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
