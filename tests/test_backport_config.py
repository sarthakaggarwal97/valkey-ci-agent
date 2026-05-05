"""Tests for backport configuration loading."""

from __future__ import annotations

from dataclasses import asdict

from hypothesis import given, settings
from hypothesis import strategies as st

from scripts.backport.config import load_backport_config
from scripts.backport.models import BackportConfig

safe_text = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N", "P", "Z"),
        min_codepoint=32,
        max_codepoint=126,
    ),
    min_size=1,
    max_size=50,
)

positive_int = st.integers(min_value=1, max_value=10_000_000)

backport_config_strategy = st.fixed_dictionaries({
    "backport_label": safe_text,
    "llm_conflict_label": safe_text,
    "max_conflicting_files": positive_int,
})




@settings(max_examples=100, deadline=None)
@given(config_data=backport_config_strategy)
def test_config_round_trip(config_data: dict) -> None:
    cfg = load_backport_config(config_data)

    assert cfg.backport_label == config_data["backport_label"]
    assert cfg.llm_conflict_label == config_data["llm_conflict_label"]
    assert cfg.max_conflicting_files == config_data["max_conflicting_files"]


def test_empty_dict_returns_defaults() -> None:
    """Empty dict should give all default values."""
    cfg = load_backport_config({})
    defaults = BackportConfig()
    assert asdict(cfg) == asdict(defaults)


def test_none_returns_defaults() -> None:
    """None should give all default values."""
    cfg = load_backport_config(None)
    defaults = BackportConfig()
    assert asdict(cfg) == asdict(defaults)


def test_non_dict_returns_defaults() -> None:
    """Non-dict input (list, int, string) should give all default values."""
    defaults = BackportConfig()
    for bad_input in [[], [1, 2, 3], 42, "not a dict", 0.5]:
        cfg = load_backport_config(bad_input)
        assert asdict(cfg) == asdict(defaults), f"Failed for input: {bad_input!r}"


def test_unrecognized_fields_ignored() -> None:
    """Fields not in the schema should be ignored."""
    cfg = load_backport_config({
        "backport_label": "my-label",
        "unknown_field": "should be ignored",
        "another_unknown": 42,
    })
    assert cfg.backport_label == "my-label"


def test_wrong_types_fall_back_to_defaults() -> None:
    """Values with wrong types fall back to default."""
    defaults = BackportConfig()
    cfg = load_backport_config({
        "backport_label": 42,           # should be string
        "llm_conflict_label": ["list"], # should be string
        "max_conflicting_files": "str", # should be int
    })
    assert cfg.backport_label == defaults.backport_label
    assert cfg.llm_conflict_label == defaults.llm_conflict_label
    assert cfg.max_conflicting_files == defaults.max_conflicting_files
