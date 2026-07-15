"""Fable 5 Bedrock smoke test (TEST HARNESS ONLY, never merged).

Invokes Claude Code exactly the way the agent does (same env building,
same CLI flags, same model resolution) and fails unless the conversation
actually ran on a Fable model. A plain exit-code check is not enough:
the CLI can silently fall back to another model and still return 0, so
this asserts the model id found in the stream-json events.
"""

from __future__ import annotations

import json
import sys

from scripts.ai.claude_code import run_claude_code


def _collect_models(stdout: str) -> set[str]:
    """Return every "model" string found anywhere in the stream-json events."""
    models: set[str] = set()

    def walk(obj: object) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "model" and isinstance(value, str):
                    models.add(value)
                walk(value)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        walk(event)
    return models


def main() -> int:
    stdout, stderr, rc = run_claude_code(
        "Reply with exactly: OK",
        timeout=600,
        max_turns=1,
        allowed_tools="Read",
    )
    models = _collect_models(stdout)
    print(f"claude exit={rc} models={sorted(models)}")
    if rc != 0:
        print(f"FAIL: claude exited non-zero. stderr: {stderr[:2000]}", file=sys.stderr)
        return 1
    if not any("fable" in model.lower() for model in models):
        print(
            "FAIL: no Fable model in stream events — silent fallback to another model?",
            file=sys.stderr,
        )
        return 1
    print("PASS: Fable model confirmed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
