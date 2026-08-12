"""Shared test fixtures."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def allow_upstream_publish_in_tests(request: pytest.FixtureRequest) -> None:
    """No-op fixture kept for the ``disable_publish_autouse`` marker.

    The legacy publish guard has been removed; this fixture is retained so
    tests that explicitly opt out via the marker continue to work without
    requiring per-test changes.
    """
    if "disable_publish_autouse" in request.keywords:
        return


@pytest.fixture(autouse=True)
def block_real_ai_entry_point(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail closed: no unit test may launch the real Claude CLI.

    ``scripts.release_notes.triage.triage`` and
    ``scripts.release_notes.generate.generate`` both take a ``run_fn``
    keyword-only argument whose default is
    :func:`scripts.ai.claude_code.run_claude_code`. Python resolves that
    default at import time and stores it on the function object's
    ``__kwdefaults__``, so patching the module-level ``run_claude_code``
    attribute is not enough — the function still holds a reference to the
    real function. When a test's PRs are label-less (or carry a non-
    ``release-notes`` label) they enter the triage stage, and if the test
    forgets to stub ``triage_mod.triage`` and does not pass ``run_fn=``
    explicitly, the subprocess call at that seam would spawn the actual
    claude CLI on any developer or CI machine that happens to have it
    installed. Turning that into a loud ``AssertionError`` is cheaper than
    the intermittent real network / model calls.

    The guard rewrites the ``run_fn`` slot on the function's
    ``__kwdefaults__`` narrowly, and ONLY when it still points at the real
    function: tests that pass ``run_fn=`` explicitly are unaffected
    (default lookup never happens), and tests that legitimately mock the
    triage/generate function at a higher level are unaffected too (the
    default is never consulted because the whole function is replaced).
    """
    from scripts.ai import claude_code as _claude_code
    from scripts.release_notes import generate as _generate_mod
    from scripts.release_notes import triage as _triage_mod

    real = _claude_code.run_claude_code

    def _refuse(*_args: object, **_kwargs: object) -> tuple[str, str, int]:
        raise AssertionError("real AI entry point reached in unit test")

    for fn in (_triage_mod.triage, _generate_mod.generate):
        defaults = fn.__kwdefaults__ or {}
        if defaults.get("run_fn") is real:
            monkeypatch.setitem(defaults, "run_fn", _refuse)
