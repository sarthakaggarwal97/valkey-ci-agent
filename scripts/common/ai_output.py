"""Parse a JSON object out of a Claude Code subprocess's output.

Claude Code emits stream-json: a sequence of event lines ending in a
``result`` event whose ``result`` field holds the model's final text. The
model is asked to return a single JSON object; this finds it whether it
arrives wrapped in the stream-json ``result`` event or as bare output.

Shared by every workflow that asks Claude for a structured verdict.
"""

from __future__ import annotations

import json
from typing import Any


def extract_json_object(stdout: str, *, required_key: str) -> dict[str, Any] | None:
    """Return the one ``{...}`` object containing ``required_key``, or None.

    Reads the stream-json ``result`` event's text when there is one, then scans
    it for top-level JSON objects carrying ``required_key`` so unrelated braces
    in surrounding prose are ignored. Output that is ambiguous returns None
    rather than a guess: more than one ``result`` event, two different objects
    carrying the key (an example verdict followed by the real one), or an
    object with a duplicated key.
    """
    text = stdout
    results = 0
    for line in stdout.strip().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get("type") == "result":
            results += 1
            result = event.get("result")
            if isinstance(result, str):
                text = result
    if results > 1:
        return None

    decoder = json.JSONDecoder(object_pairs_hook=_unique_keys)
    found: list[dict[str, Any]] = []
    start = text.find("{")
    while start != -1:
        try:
            obj, length = decoder.raw_decode(text[start:])
        except _DuplicateKey:
            return None
        except ValueError:
            start = text.find("{", start + 1)
            continue
        if isinstance(obj, dict) and required_key in obj:
            found.append(obj)
            # Skip the object's body so its nested objects are not candidates.
            start = text.find("{", start + length)
        else:
            start = text.find("{", start + 1)
    if not found or any(obj != found[0] for obj in found[1:]):
        return None
    return found[0]


class _DuplicateKey(ValueError):
    pass


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _value in pairs]
    if len(set(keys)) != len(keys):
        raise _DuplicateKey(f"duplicate key in {keys}")
    return dict(pairs)


def last_agent_text(stdout: str, *, limit: int = 500) -> str:
    """Best-effort final assistant text from a stream-json transcript.

    Scans for the last ``result``/``text`` field. Used to surface an agent's
    own explanation when it stops without the structured output (out of turns,
    or an edit agent that deliberately made no change). Returns "" when nothing
    parseable is found.
    """
    last = ""
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(event, dict):
            text = event.get("result") or event.get("text")
            if isinstance(text, str) and text.strip():
                last = text.strip()
    return " ".join(last.split())[:limit]
