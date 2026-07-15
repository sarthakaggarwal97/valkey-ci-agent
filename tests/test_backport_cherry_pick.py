"""Unit tests for cherry_pick().

"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TypeVar
from unittest.mock import MagicMock, mock_open, patch

import pytest

from scripts.backport.cherry_pick import cherry_pick, complete_resolved_cherry_pick
from scripts.backport.models import ResolutionResult
from scripts.common.proc import LOCKED_GIT_CONFIG

_Output = TypeVar("_Output", str, bytes)


def _ok(
    stdout: _Output = "",
    stderr: _Output = "",
) -> subprocess.CompletedProcess[_Output]:
    """Return a successful CompletedProcess."""
    return subprocess.CompletedProcess(
        args=["git"], returncode=0, stdout=stdout, stderr=stderr,
    )


def _fail(
    stdout: _Output = "",
    stderr: _Output = "",
) -> subprocess.CompletedProcess[_Output]:
    """Return a failed CompletedProcess."""
    return subprocess.CompletedProcess(
        args=["git"], returncode=1, stdout=stdout, stderr=stderr,
    )


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _git_command(call_args) -> list[str]:
    command = call_args[0][0]
    assert command[1:1 + len(LOCKED_GIT_CONFIG)] == list(LOCKED_GIT_CONFIG)
    return ["git", *command[1 + len(LOCKED_GIT_CONFIG):]]


def test_empty_cherry_pick_does_not_create_empty_commit(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "file.txt").write_text("already present\n", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-q", "-m", "already applied")
    sha = _git(repo, "rev-parse", "HEAD")
    before_count = _git(repo, "rev-list", "--count", "HEAD")

    result = cherry_pick(str(repo), "main", sha, [])

    assert result.success is True
    assert result.applied_commits == []
    assert _git(repo, "rev-list", "--count", "HEAD") == before_count
    assert _git(repo, "status", "--porcelain") == ""


class TestCleanCherryPickWithMergeCommit:
    """Scenario 1: Clean cherry-pick using merge commit SHA."""

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_returns_success(self, mock_run: MagicMock) -> None:
        # checkout succeeds, cherry-pick -m 1 succeeds
        mock_run.side_effect = [_ok(), _ok()]

        result = cherry_pick("/repo", "8.1", "abc123merge", ["sha1", "sha2"])

        assert result.success is True
        assert result.applied_commits == ["abc123merge"]
        assert result.conflicting_files == []

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_calls_checkout_then_cherry_pick(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [_ok(), _ok()]

        cherry_pick("/repo", "8.1", "abc123merge", ["sha1"])

        calls = mock_run.call_args_list
        # First call: git checkout 8.1
        assert _git_command(calls[0]) == ["git", "checkout", "8.1"]
        # Second call: git cherry-pick -m 1 <merge_sha>
        assert _git_command(calls[1]) == ["git", "cherry-pick", "-m", "1", "abc123merge"]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_retries_without_mainline_for_squash_merge_commit(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="error: commit abc123 is not a merge but no -m option was given?\nfatal: mainline was specified but commit abc123 is not a merge."),
            _ok(),
        ]

        result = cherry_pick("/repo", "8.1", "abc123", ["sha1"])

        assert result.success is True
        assert result.applied_commits == ["abc123"]
        calls = [_git_command(call_args) for call_args in mock_run.call_args_list]
        assert ["git", "cherry-pick", "-m", "1", "abc123"] in calls
        assert ["git", "cherry-pick", "abc123"] in calls


class TestCleanCherryPickSequential:
    """Scenario 2: Clean cherry-pick with sequential commits."""

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_returns_success_all_commits(self, mock_run: MagicMock) -> None:
        # checkout + 3 cherry-picks all succeed
        mock_run.side_effect = [_ok(), _ok(), _ok(), _ok()]

        result = cherry_pick("/repo", "7.2", None, ["sha1", "sha2", "sha3"])

        assert result.success is True
        assert result.applied_commits == ["sha1", "sha2", "sha3"]
        assert result.conflicting_files == []

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_calls_cherry_pick_per_commit(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [_ok(), _ok(), _ok()]

        cherry_pick("/repo", "8.1", None, ["sha1", "sha2"])

        calls = mock_run.call_args_list
        assert _git_command(calls[0]) == ["git", "checkout", "8.1"]
        assert _git_command(calls[1]) == ["git", "cherry-pick", "sha1"]
        assert _git_command(calls[2]) == ["git", "cherry-pick", "sha2"]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_empty_sequential_cherry_pick_is_skipped(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="The previous cherry-pick is now empty"),
            _ok(stdout=""),
            _ok(),
            _ok(),
        ]

        result = cherry_pick("/repo", "8.1", None, ["sha1", "sha2"])

        assert result.success is True
        assert result.applied_commits == ["sha2"]
        assert result.conflicting_files == []
        calls = [_git_command(call_args) for call_args in mock_run.call_args_list]
        assert ["git", "cherry-pick", "--abort"] in calls
        assert ["git", "cherry-pick", "--allow-empty", "sha1"] not in calls

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_empty_merge_cherry_pick_is_skipped(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="The previous cherry-pick is now empty"),
            _ok(stdout=""),
            _ok(),
        ]

        result = cherry_pick("/repo", "8.1", "merge_sha", ["sha1"])

        assert result.success is True
        assert result.applied_commits == []
        assert result.conflicting_files == []
        calls = [_git_command(call_args) for call_args in mock_run.call_args_list]
        assert ["git", "cherry-pick", "--abort"] in calls
        assert ["git", "cherry-pick", "-m", "1", "--allow-empty", "merge_sha"] not in calls

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_merge_failure_without_conflicts_is_not_counted_as_applied(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="fatal: bad revision"),
            _ok(stdout=""),
        ]

        result = cherry_pick("/repo", "8.1", "missing_sha", ["sha1"])

        assert result.success is False
        assert result.conflicting_files == []
        assert result.applied_commits == []


class TestConflictDetection:
    """Scenario 3: Cherry-pick with conflicts — conflict detection and file parsing."""

    @patch("builtins.open", mock_open(read_data="<<<<<<< HEAD\nold\n=======\nnew\n>>>>>>> abc123\n"))
    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_merge_commit_conflict_returns_conflicted_files(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),                                      # checkout
            _fail(stderr="conflict"),                    # cherry-pick -m 1 fails
            _ok(stdout=b"src/server.c\0src/config.c\0"),
            _ok(stdout=b"target content"),
            _ok(stdout=b"source content"),
            _ok(stdout=b"target content 2"),
            _ok(stdout=b"source content 2"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", ["sha1"])

        assert result.success is False
        assert len(result.conflicting_files) == 2
        assert result.conflicting_files[0].path == "src/server.c"
        assert result.conflicting_files[1].path == "src/config.c"
        assert result.applied_commits == []

    @patch("builtins.open", mock_open(read_data="conflict content"))
    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_sequential_conflict_stops_at_failing_commit(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),                              # checkout
            _ok(),                              # cherry-pick sha1 succeeds
            _fail(stderr="conflict"),           # cherry-pick sha2 fails
            _ok(stdout=b"file.c\0"),
            _ok(stdout=b"target ver"),
            _ok(stdout=b"source ver"),
        ]

        result = cherry_pick("/repo", "8.1", None, ["sha1", "sha2", "sha3"])

        assert result.success is False
        assert result.applied_commits == ["sha1"]
        assert len(result.conflicting_files) == 1
        assert result.conflicting_files[0].path == "file.c"

    @patch("builtins.open", mock_open(read_data="markers here"))
    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_conflicted_file_reads_all_versions(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),                                  # checkout
            _fail(),                                # cherry-pick fails
            _ok(stdout=b"src/main.c\0"),
            _ok(stdout=b"target branch content"),
            _ok(stdout=b"source branch content"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        cf = result.conflicting_files[0]
        assert cf.path == "src/main.c"
        assert cf.target_branch_content == "target branch content"
        assert cf.source_branch_content == "source branch content"

    @patch("builtins.open", mock_open(read_data="content"))
    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_git_show_failure_returns_empty_string(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),                          # checkout
            _fail(),                        # cherry-pick fails
            _ok(stdout=b"new_file.c\0"),
            _fail(stderr=b"not found"),
            _fail(stderr=b"not found"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        cf = result.conflicting_files[0]
        assert cf.target_branch_content == ""
        assert cf.source_branch_content == ""

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_binary_conflict_requires_human_handoff(self, mock_run: MagicMock) -> None:
        # One text conflict and one binary conflict (NUL byte in content).
        mock_run.side_effect = [
            _ok(),                                  # checkout
            _fail(stderr="conflict"),                # cherry-pick fails
            _ok(stdout=b"src/main.c\0fixture.gz\0"),
            _ok(stdout=b"target text"),
            _ok(stdout=b"source text"),
            _ok(stdout=b"binary\0blob"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        assert result.success is False
        assert result.conflicting_files == []
        assert result.handoff_reason == "binary conflict requires human handling: fixture.gz"

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_only_binary_conflict_records_handoff_reason(
        self,
        mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),                          # checkout
            _fail(stderr="conflict"),        # cherry-pick fails
            _ok(stdout=b"fixture.gz\0"),
            _ok(stdout=b"bin\0a"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        assert result.success is False
        assert result.conflicting_files == []
        assert result.handoff_reason == "binary conflict requires human handling: fixture.gz"

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_newline_in_conflict_path_is_not_split(
        self,
        mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="conflict"),
            _ok(stdout=b"src/line\nbreak.c\0"),
            _ok(stdout=b"target"),
            _ok(stdout=b"source"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        assert [item.path for item in result.conflicting_files] == [
            "src/line\nbreak.c"
        ]
        show_calls = [_git_command(call) for call in mock_run.call_args_list[3:]]
        assert show_calls == [
            [
                "git",
                "show",
                "--no-ext-diff",
                "--no-textconv",
                "8.1:src/line\nbreak.c",
            ],
            [
                "git",
                "show",
                "--no-ext-diff",
                "--no-textconv",
                "CHERRY_PICK_HEAD:src/line\nbreak.c",
            ],
        ]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_non_utf8_conflict_path_requires_handoff(
        self,
        mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="conflict"),
            _ok(stdout=b"src/bad-\xff.c\0"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        assert result.conflicting_files == []
        assert result.handoff_reason is not None
        assert "non-UTF-8 path" in result.handoff_reason
        assert "\ufffd" not in result.handoff_reason

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_non_utf8_conflict_content_requires_handoff(
        self,
        mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [
            _ok(),
            _fail(stderr="conflict"),
            _ok(stdout=b"src/main.c\0"),
            _ok(stdout=b"target-\xff"),
        ]

        result = cherry_pick("/repo", "8.1", "mergesha", [])

        assert result.conflicting_files == []
        assert result.handoff_reason == (
            "non-UTF-8 conflict requires human handling: src/main.c"
        )


def test_complete_resolution_rejects_non_utf8_surrogate(tmp_path: Path) -> None:
    resolution = ResolutionResult(
        path="file.txt",
        resolved_content="bad\udcff",
        resolution_summary="invalid",
    )

    with pytest.raises(ValueError, match="not valid UTF-8"):
        complete_resolved_cherry_pick(str(tmp_path), [resolution])


class TestMergeCommitPreference:
    """Scenario 4 & 5: Merge commit SHA is preferred; sequential fallback."""

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_uses_m1_flag_when_merge_sha_provided(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [_ok(), _ok()]

        cherry_pick("/repo", "8.1", "merge_sha_abc", ["sha1", "sha2"])

        cherry_pick_call = mock_run.call_args_list[1]
        cmd = _git_command(cherry_pick_call)
        assert cmd == ["git", "cherry-pick", "-m", "1", "merge_sha_abc"]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_ignores_individual_commits_when_merge_sha_provided(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [_ok(), _ok()]

        result = cherry_pick("/repo", "8.1", "merge_sha", ["sha1", "sha2", "sha3"])

        # Only 2 subprocess calls: checkout + single cherry-pick
        assert mock_run.call_count == 2
        assert result.applied_commits == ["merge_sha"]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_falls_back_to_sequential_when_no_merge_sha(
        self, mock_run: MagicMock,
    ) -> None:
        mock_run.side_effect = [_ok(), _ok(), _ok()]

        result = cherry_pick("/repo", "8.1", None, ["sha1", "sha2"])

        # checkout + 2 individual cherry-picks
        assert mock_run.call_count == 3
        calls = mock_run.call_args_list
        assert _git_command(calls[1]) == ["git", "cherry-pick", "sha1"]
        assert _git_command(calls[2]) == ["git", "cherry-pick", "sha2"]
        assert result.applied_commits == ["sha1", "sha2"]

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_empty_merge_sha_string_treated_as_none(
        self, mock_run: MagicMock,
    ) -> None:
        """An empty string for merge_commit_sha is falsy, so sequential path is used."""
        mock_run.side_effect = [_ok(), _ok()]

        result = cherry_pick("/repo", "8.1", "", ["sha1"])

        calls = mock_run.call_args_list
        # Should use sequential path (no -m 1)
        assert _git_command(calls[1]) == ["git", "cherry-pick", "sha1"]
        assert result.applied_commits == ["sha1"]


class TestSubprocessCwd:
    """Verify that all git commands use the configured repo_dir as cwd."""

    @patch("scripts.backport.cherry_pick.subprocess.run")
    def test_all_calls_use_repo_dir(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [_ok(), _ok()]

        cherry_pick("/my/repo/path", "8.1", "sha", [])

        for c in mock_run.call_args_list:
            assert c[1]["cwd"] == "/my/repo/path"
