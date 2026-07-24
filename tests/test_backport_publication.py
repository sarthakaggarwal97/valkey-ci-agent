from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.backport import publication


def test_capture_target_head_uses_remote_tracking_ref(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_git_output(repo_dir: str, *args: str) -> str:
        calls.append((repo_dir, *args))
        return "a" * 40

    monkeypatch.setattr(publication, "git_output", fake_git_output)

    assert publication.capture_target_head("/repo", "release/1.0") == "a" * 40
    assert calls == [
        (
            "/repo",
            "rev-parse",
            "--verify",
            "refs/remotes/origin/release/1.0^{commit}",
        )
    ]


def test_capture_target_head_rejects_missing_ref(monkeypatch) -> None:
    monkeypatch.setattr(publication, "git_output", lambda *_args: "")

    with pytest.raises(RuntimeError, match="could not capture origin/1.0"):
        publication.capture_target_head("/repo", "1.0")


def test_assert_target_head_unchanged_accepts_same_sha(monkeypatch) -> None:
    monkeypatch.setattr(
        publication,
        "get_target_head",
        lambda *_args: "a" * 40,
    )

    publication.assert_target_head_unchanged(
        object(),
        "org/repo",
        "1.0",
        "a" * 40,
    )


def test_assert_target_head_unchanged_rejects_moved_branch(monkeypatch) -> None:
    monkeypatch.setattr(
        publication,
        "get_target_head",
        lambda *_args: "b" * 40,
    )

    with pytest.raises(publication.TargetHeadChanged) as exc:
        publication.assert_target_head_unchanged(
            object(),
            "org/repo",
            "1.0",
            "a" * 40,
        )

    message = str(exc.value)
    assert "validated " + ("a" * 40) in message
    assert "current " + ("b" * 40) in message
    assert "refusing to publish" in message


def test_get_target_head_reads_authoritative_repository() -> None:
    branch = SimpleNamespace(commit=SimpleNamespace(sha="c" * 40))
    repo = SimpleNamespace(get_branch=lambda name: branch)
    gh = SimpleNamespace(get_repo=lambda name: repo)

    assert publication.get_target_head(gh, "org/repo", "1.0") == "c" * 40


def test_get_target_head_fails_closed_without_sha() -> None:
    repo = SimpleNamespace(
        get_branch=lambda name: SimpleNamespace(commit=SimpleNamespace(sha=None))
    )
    gh = SimpleNamespace(get_repo=lambda name: repo)

    with pytest.raises(RuntimeError, match="returned no commit SHA"):
        publication.get_target_head(gh, "org/repo", "1.0")
