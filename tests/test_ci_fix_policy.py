"""Tests for deterministic CI-fix publication policy."""

from scripts.ci_fix.policy import (
    authored_publication_decision,
    port_publication_decision,
)

_PROTECTED = (".github/workflows/**", ".github/actions/**", "CODEOWNERS")
_AUTO = ("tests/**", "src/unit/**")


def test_authored_test_patch_can_auto_publish():
    decision = authored_publication_decision(
        ("tests/unit/cluster.tcl", "src/unit/test_cluster.c"),
        protected_patterns=_PROTECTED,
        auto_publish_patterns=_AUTO,
    )
    assert decision.auto_publish is True


def test_authored_product_change_becomes_handoff():
    decision = authored_publication_decision(
        ("src/server.c",),
        protected_patterns=_PROTECTED,
        auto_publish_patterns=_AUTO,
    )
    assert decision.auto_publish is False
    assert "src/server.c" in decision.reason


def test_protected_workflow_change_never_auto_publishes():
    authored = authored_publication_decision(
        (".github/workflows/daily.yml",),
        protected_patterns=_PROTECTED,
        auto_publish_patterns=("**",),
    )
    port = port_publication_decision(
        (".github/workflows/daily.yml",),
        protected_patterns=_PROTECTED,
    )
    assert authored.auto_publish is False
    assert port.auto_publish is False
    assert "protected" in authored.reason
    assert "protected" in port.reason


def test_historical_source_port_can_publish_outside_test_allowlist():
    decision = port_publication_decision(
        ("src/server.c",),
        protected_patterns=_PROTECTED,
    )
    assert decision.auto_publish is True


def test_empty_unknown_or_malformed_paths_fail_closed():
    assert not authored_publication_decision(
        (),
        protected_patterns=_PROTECTED,
        auto_publish_patterns=_AUTO,
    ).auto_publish
    assert not port_publication_decision(
        (),
        protected_patterns=_PROTECTED,
    ).auto_publish
    assert not authored_publication_decision(
        ("../outside",),
        protected_patterns=_PROTECTED,
        auto_publish_patterns=("**",),
    ).auto_publish
