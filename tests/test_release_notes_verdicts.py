"""The verdicts artifact: the durable per-PR audit the body's counts point at."""

from __future__ import annotations

import json
from types import SimpleNamespace

from scripts.release_notes import verdicts


def _pr(number: int, **kw) -> SimpleNamespace:
    defaults = dict(
        title=f"PR {number}", url=f"https://x/{number}", reason="r",
        uncertain=False, guardrail=False, unreleased_code=False,
    )
    defaults.update(kw)
    return SimpleNamespace(number=number, **defaults)


def _regen() -> SimpleNamespace:
    return SimpleNamespace(
        base_tag="9.1.2",
        ai_included=(_pr(1),),
        guardrail_included=(_pr(2, guardrail=True),),
        ai_excluded=(_pr(3, unreleased_code=True),),
        label_excluded=(_pr(4),),
        triage=(_pr(5),),
    )


def test_writes_every_verdict_class_with_reasons(tmp_path) -> None:
    path = tmp_path / "verdicts.json"
    verdicts.write_verdicts(str(path), _regen(), version="9.2.0", stage="rc1")
    data = json.loads(path.read_text())
    assert data["version"] == "9.2.0"
    assert data["base_tag"] == "9.1.2"
    assert data["ai_included"][0]["pr"] == 1
    assert data["guardrail_included"][0]["guardrail"] is True
    assert data["ai_excluded"][0]["unreleased_code"] is True
    assert data["ai_excluded"][0]["reason"] == "r"
    assert data["undecided"][0]["pr"] == 5


def test_unwritable_path_does_not_raise(tmp_path) -> None:
    # The artifact is an audit aid; failing the cut over it would invert its
    # purpose.
    verdicts.write_verdicts(str(tmp_path / "no" / "dir" / "v.json"), _regen(),
                            version="9.2.0", stage="rc1")
