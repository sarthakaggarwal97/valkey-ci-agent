"""The code-age check: a fix for code that never shipped is not a user fix.

A feature introduced in this same release arrives correct, so no released
version ever had the bug. The check is one-sided by design: it reports
"unreleased" only on positive evidence that every modified line is new, and
anything unknown keeps the note, because under-reporting a real fix is worse.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.release_notes import code_age


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repo whose baseline tag predates one feature and its later fix."""
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "T")

    old = tmp_path / "old.c"
    old.write_text("int shipped(void) { return 1; }\n")
    stable = tmp_path / "stable.c"
    stable.write_text("int stable(void) { return 1; }\n")
    _git(tmp_path, "add", "old.c", "stable.c")
    _git(tmp_path, "commit", "-q", "-m", "shipped code")
    _git(tmp_path, "tag", "9.1.2")

    new = tmp_path / "new.c"
    new.write_text("int feature(void) { return 0; }\n")
    _git(tmp_path, "add", "new.c")
    _git(tmp_path, "commit", "-q", "-m", "add feature (#100)")

    new.write_text("int feature(void) { return 1; }\n")
    _git(tmp_path, "commit", "-q", "-am", "fix the new feature (#101)")

    old.write_text("int shipped(void) { return 2; }\n")
    _git(tmp_path, "commit", "-q", "-am", "fix shipped code (#102)")
    return tmp_path


def _sha(repo: Path, subject: str) -> str:
    return _git(repo, "log", "--format=%H", "--grep", subject).splitlines()[0]


class TestReleasedCodeOracle:
    def test_fix_to_code_added_after_the_tag_is_unreleased(self, repo: Path) -> None:
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code(_sha(repo, "fix the new feature")) is True

    def test_fix_to_code_present_at_the_tag_is_released(self, repo: Path) -> None:
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code(_sha(repo, "fix shipped code")) is False

    def test_pure_addition_is_unknown_not_unreleased(self, repo: Path) -> None:
        # The feature commit only adds lines, so there is no pre-image to blame
        # and the answer must be None rather than a guess.
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code(_sha(repo, "add feature")) is None

    def test_without_a_baseline_tag_the_check_is_disabled(self, repo: Path) -> None:
        oracle = code_age.ReleasedCodeOracle(str(repo), "")
        assert not oracle.usable
        assert oracle.modifies_only_unreleased_code(_sha(repo, "fix shipped code")) is None

    def test_unreadable_commit_is_unknown(self, repo: Path) -> None:
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code("0" * 40) is None

    def test_ancestry_answers_are_cached_per_commit(self, repo: Path, monkeypatch) -> None:
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        sha = _sha(repo, "fix shipped code")
        oracle.modifies_only_unreleased_code(sha)
        calls = []
        real = code_age.git_output

        def counting(repo_dir, *args, **kwargs):
            if args and args[0] == "merge-base":
                calls.append(args)
            return real(repo_dir, *args, **kwargs)

        monkeypatch.setattr(code_age, "git_output", counting)
        oracle.modifies_only_unreleased_code(sha)
        assert calls == []  # served from the cache built by the first call

    def test_deleting_released_code_is_released(self, repo: Path) -> None:
        # Regression: the parser once tracked files by the post-image path, so a
        # deletion ("+++ /dev/null") left the previous file current and blamed
        # the wrong file; editing a new file while deleting a released one then
        # returned True and silently dropped a real note.
        (repo / "new.c").write_text("int feature(void) { return 3; }\n")
        _git(repo, "rm", "-q", "stable.c")
        _git(repo, "commit", "-qam", "edit new and delete released (#103)")
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code(_sha(repo, "edit new and delete")) is False

    def test_rename_of_released_code_blames_the_old_path(self, repo: Path) -> None:
        # A rename's pre-image path is the old name, which is what exists at
        # <sha>^; blaming it must find the released introduction.
        _git(repo, "mv", "stable.c", "renamed.c")
        (repo / "renamed.c").write_text("int stable(void) { return 3; }\n")
        _git(repo, "commit", "-qam", "rename and edit released code (#104)")
        oracle = code_age.ReleasedCodeOracle(str(repo), "9.1.2")
        assert oracle.modifies_only_unreleased_code(_sha(repo, "rename and edit")) is False
