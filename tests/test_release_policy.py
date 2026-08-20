from __future__ import annotations

from pathlib import Path

import pytest

from scripts.release.policy import load_policy, validate_branch


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "policy.yml"
    path.write_text(body, encoding="utf-8")
    return path


VALID = """\
schema_version: 1
repo: valkey-io/valkey
authorized_team: valkey-io/core-team
checks_workflow: ci.yml
branches: ['9.1']
required_checks: [test]
"""


def test_loads_small_policy(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, VALID))
    assert policy.repo == "valkey-io/valkey"
    assert policy.team_slug == "core-team"
    assert policy.require_tag_ruleset is True
    assert policy.allow_version_override is False
    assert validate_branch(policy, " 9.1 ") == "9.1"


def test_explicit_fork_policy_can_disable_tag_ruleset(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, VALID + "require_tag_ruleset: false\n"))
    assert policy.require_tag_ruleset is False


def test_non_boolean_tag_ruleset_policy_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="require_tag_ruleset must be a boolean"):
        load_policy(_write(tmp_path, VALID + "require_tag_ruleset: 'false'\n"))


@pytest.mark.parametrize(
    "replacement, message",
    [
        ("schema_version: 2", "schema_version"),
        ("authorized_team: core-team", "org/team-slug"),
        ("branches: []", "non-empty list"),
        ("required_checks: [test, test]", "duplicates"),
        ("checks_workflow: .github/workflows/ci.yml", "filename"),
    ],
)
def test_invalid_policy_fails_closed(tmp_path: Path, replacement: str, message: str) -> None:
    key = replacement.split(":", 1)[0]
    body = "\n".join(
        replacement if line.startswith(f"{key}:") else line
        for line in VALID.splitlines()
    )
    with pytest.raises(ValueError, match=message):
        load_policy(_write(tmp_path, body))


def test_unknown_key_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown"):
        load_policy(_write(tmp_path, VALID + "surprise: true\n"))


def test_unlisted_branch_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not releasable"):
        validate_branch(load_policy(_write(tmp_path, VALID)), "unstable")


def test_loads_explicit_user_policy(tmp_path: Path) -> None:
    body = VALID.replace("valkey-io/core-team", "user:sarthakaggarwal97")
    policy = load_policy(_write(tmp_path, body))
    assert policy.authorized_user == "sarthakaggarwal97"


@pytest.mark.parametrize("value", ["user:", "user:two/logins", "user:has space"])
def test_invalid_explicit_user_policy_is_refused(tmp_path: Path, value: str) -> None:
    body = VALID.replace(
        "authorized_team: valkey-io/core-team", f"authorized_team: '{value}'"
    )
    with pytest.raises(ValueError, match="user:LOGIN"):
        load_policy(_write(tmp_path, body))


def test_explicit_fork_policy_can_enable_version_override(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, VALID + "allow_version_override: true\n"))
    assert policy.allow_version_override is True


def test_non_boolean_version_override_policy_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="allow_version_override must be a boolean"):
        load_policy(_write(tmp_path, VALID + "allow_version_override: 'true'\n"))
