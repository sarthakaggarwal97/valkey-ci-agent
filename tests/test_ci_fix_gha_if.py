"""Tests for the GitHub Actions if: evaluator used by the Daily planner."""

from __future__ import annotations

import pytest

from scripts.ci_fix.verify.gha_if import UnsupportedExpression, evaluate

_ARM = (
    "(github.event_name == 'workflow_call' || github.event_name == 'workflow_dispatch' ||\n"
    "  (github.event_name == 'pull_request' && contains(github.event.pull_request.labels.*.name, 'run-extra-tests')))"
    " &&\n(!contains(github.event.inputs.skipjobs, 'ubuntu') || !contains(github.event.inputs.skipjobs, 'arm'))"
)


def _ctx(**inputs):
    return {"github": {"event_name": "workflow_dispatch", "event": {"inputs": inputs}}, "inputs": inputs}


@pytest.mark.parametrize(("skipjobs", "runs"), [
    ("ubuntu,arm", False), ("ubuntu", True), ("arm", True), ("", True), ("valgrind", True),
])
def test_an_or_gate_runs_when_either_token_is_kept(skipjobs, runs):
    assert evaluate(_ARM, _ctx(skipjobs=skipjobs)) is runs


def test_contains_is_a_case_insensitive_substring_match_as_in_github():
    # The workflow relies on this: "tls" is inside "tls-module".
    assert evaluate("contains(github.event.inputs.skipjobs, 'TLS')", _ctx(skipjobs="x,tls-module")) is True
    assert evaluate("!contains(github.event.inputs.skiptests, 'valkey')", _ctx(skiptests="")) is True


def test_operators_short_circuit_and_compare_strings_ignoring_case():
    assert evaluate("github.event_name == 'WORKFLOW_DISPATCH'", _ctx()) is True
    assert evaluate("github.event_name != 'schedule' && !(true && false)", _ctx()) is True
    assert evaluate("always() && github.event_name == 'schedule'", _ctx()) is False
    assert evaluate("${{ github.event.inputs.missing }}", _ctx()) is False
    assert evaluate(None, _ctx()) is True and evaluate(True, _ctx()) is True


@pytest.mark.parametrize("expression", ["hashFiles('x') != ''", "github.event_name ==", "a ? b : c"])
def test_anything_unmodelled_is_refused_not_guessed(expression):
    with pytest.raises(UnsupportedExpression):
        evaluate(expression, _ctx())
