"""Replay the real dry-run cut at the historical head preceding Valkey #5494.

Only remote-head resolution is replaced with the pinned historical snapshot.
Discovery, GitHub reads, triage, generation, rendering, version promotion,
consolidation, and dry-run reporting use the production code and live AI.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from unittest.mock import patch

from github import Auth, Github

from scripts.common.git_auth import GitAuth, github_https_url
from scripts.common.logging_utils import configure_logging
from scripts.common.proc import git_output, run_git
from scripts.release_notes import consolidate, main, projects, release_cut
from scripts.release_notes import release_format as rn

REPO = "valkey-io/valkey"
HEAD = "d15aa798e1ca53c880a5338d8c94203c1a354648"
VERSION = "9.2.0"
DATE = "2026-11-18"
AGENT_CHANGE = "3bf0d252b03f7d8cddca725a1ea5c9861a531006"
HEADING_RE = re.compile(r"^Valkey[ \t]+\d+\.\d+\.\d+[^\n]*$", re.MULTILINE)
TARGET_RE = re.compile(r"^Valkey[ \t]+9\.2\.0(?:-|[ \t])")


def sections(text: str) -> tuple[str, list[str], str]:
    headings = list(HEADING_RE.finditer(text))
    assert headings, "Missing dated release sections"
    footers = list(re.finditer(r"^### Contributors[ \t]*$", text, re.MULTILINE))
    end = footers[-1].start() if footers else len(text)
    return (
        text[:headings[0].start()],
        [
            text[heading.start():headings[index + 1].start()
                 if index + 1 < len(headings) else end]
            for index, heading in enumerate(headings)
        ],
        text[end:],
    )


def main_replay() -> None:
    assert os.environ["GITHUB_REPOSITORY"] == "sarthakaggarwal97/valkey-ci-agent"
    token = os.environ["RELEASE_NOTES_GITHUB_TOKEN"]
    artifact_dir = Path(os.environ["REPLAY_ARTIFACT_DIR"])
    artifact_dir.mkdir(parents=True, exist_ok=True)
    profile = projects.profile_for(REPO)
    configure_logging()
    captured: dict = {}
    original_consolidate = consolidate.consolidate_release_notes
    original_report = release_cut._print_dry_run

    def capture_consolidation(text: str, **kwargs):
        captured["input_notes"] = text
        (artifact_dir / "before-consolidation-00-RELEASENOTES").write_text(text)
        result = original_consolidate(text, **kwargs)
        captured["consolidation"] = result
        return result

    def capture_report(plan, version, notes, version_h, notes_meta, **kwargs):
        captured["notes"] = notes
        captured["version_h"] = version_h
        captured["metadata"] = notes_meta
        (artifact_dir / "00-RELEASENOTES").write_text(notes)
        (artifact_dir / "src-version.h").write_text(version_h)
        original_report(plan, version, notes, version_h, notes_meta, **kwargs)

    def refuse_publication(*args, **kwargs):
        raise AssertionError("A historical dry run must never attempt publication")

    with Github(auth=Auth.Token(token)) as gh, GitAuth(token) as auth:
        repo = gh.get_repo(REPO)
        with tempfile.TemporaryDirectory(prefix="ga-replay-") as clone_dir:
            # Read upstream once. The fixture changes only this runner's clone.
            run_git(
                None, "clone", "--filter=blob:none", "--branch", "9.2",
                github_https_url(REPO), clone_dir, env=auth.env(),
            )
            run_git(clone_dir, "checkout", "--force", "-B", "9.2", HEAD)
            run_git(clone_dir, "update-ref", "refs/remotes/origin/9.2", HEAD)
            # Restore the historical tag view used by the normal preflight.
            removed_tags = []
            for tag in git_output(clone_dir, "tag", "--list", "9.2.*").splitlines():
                if re.fullmatch(r"9\.2\.0-rc[12]", tag):
                    continue
                run_git(clone_dir, "tag", "--delete", tag)
                removed_tags.append(tag)
            main._validate_release_target(clone_dir, "9.2", VERSION, "ga", profile)
            before_status = git_output(clone_dir, "status", "--porcelain")

            def historical_tip(repo_dir, branch, git_env):
                assert Path(repo_dir) == Path(clone_dir)
                assert branch == "9.2"
                actual = git_output(repo_dir, "rev-parse", "refs/heads/9.2").strip()
                assert actual == HEAD, "Historical replay snapshot changed"
                return actual

            # The live 9.2 branch has already shipped GA. Pin remote-head
            # resolution to #5494's base; every release transformation stays real.
            with (
                patch.object(release_cut, "_fetch_remote_branch_tip", historical_tip),
                patch.object(release_cut, "_commit_push_release_pr", refuse_publication),
                patch.object(consolidate, "consolidate_release_notes", capture_consolidation),
                patch.object(release_cut, "_print_dry_run", capture_report),
            ):
                result = release_cut.cut(
                    repo, repo_full_name=REPO, source_clone_dir=clone_dir,
                    version=VERSION, stage="ga", urgency="LOW", date=DATE,
                    tag_glob="9.2.*", base_ref=None, contrib_base_ref=None,
                    security_fixes=None, token=token, git_env=auth.env(),
                    dry_run=True, profile=profile,
                )
            assert result == 0, f"Dry run returned {result}"
            assert git_output(clone_dir, "status", "--porcelain") == before_status
            assert not Path(clone_dir, ".release-dest").exists()

    consolidated = captured["consolidation"]
    assert consolidated is not None
    assert consolidated.rc_stages == ("rc1", "rc2")
    notes = captured["notes"]
    original = captured["input_notes"]
    prefix, previous_sections, footer = sections(original)
    new_prefix, final_sections, new_footer = sections(notes)
    assert prefix == new_prefix
    assert [s for s in previous_sections if not TARGET_RE.match(s)] == final_sections[1:]
    assert footer == new_footer
    assert final_sections[0].startswith(
        "Valkey 9.2.0  -  Released Wed 18 November 2026\n"
    )
    assert len([s for s in final_sections if TARGET_RE.match(s)]) == 1
    assert "Valkey 9.2.0-rc" not in notes
    assert "Valkey 9.2.0 GA" not in notes
    assert "These notes include changes from all release candidates for this version." in notes
    assert "diff the `00-RELEASENOTES` files" in notes
    assert rn.credited_pr_numbers(original) == rn.credited_pr_numbers(notes)
    assert profile.bumper.current_release_state(captured["version_h"]) == (VERSION, "ga")

    source_security = []
    rc_only_count = 0
    final_bullets = [line for lines in consolidated.grouped.values() for line in lines]
    for section in previous_sections:
        if not TARGET_RE.match(section):
            continue
        entries, fixes, _contributors = consolidate._read_entries(section, "replay", profile)
        source_security.extend(fixes)
        for entry in entries:
            if not consolidate._RC_ONLY_RE.search(entry.text):
                continue
            rc_only_count += 1
            assert any(
                set(entry.prs).issubset(rn.trailing_pr_numbers(bullet))
                and consolidate._RC_SCOPE_RE.search(bullet)
                and all(
                    name.casefold() in bullet.casefold()
                    for name in consolidate._CODE_NAME_RE.findall(entry.text)
                )
                for bullet in final_bullets
            ), f"RC-only effect lost: {entry.text}"
    assert tuple(dict.fromkeys(source_security)) == consolidated.security_fixes
    for security in consolidated.security_fixes:
        assert f"* {security}" in notes

    metadata = captured["metadata"]
    summary = {
        "agent_change": AGENT_CHANGE,
        "workflow_commit": os.environ["GITHUB_SHA"],
        "model": os.environ["CI_AGENT_CLAUDE_MODEL"],
        "source_repo": REPO,
        "historical_head": HEAD,
        "baseline": metadata.regen.base_tag,
        "version": VERSION,
        "stage": "ga",
        "dry_run": True,
        "publication_attempted": False,
        "fixture_override": "Remote-head resolution pinned to the pre-GA snapshot",
        "removed_local_post_rc_tags": removed_tags,
        "rc_stages": list(consolidated.rc_stages),
        "source_entries": consolidated.source_count,
        "final_bullets": len(final_bullets),
        "credited_prs": len(rn.credited_pr_numbers(final_sections[0])),
        "security_entries_preserved": len(consolidated.security_fixes),
        "rc_only_entries_verified": rc_only_count,
        "new_prs_included": metadata.regen.included,
        "older_sections_preserved": len(final_sections) - 1,
        "contributor_footer_preserved": True,
        "clone_unchanged": True,
        "checks": "passed",
    }
    (artifact_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("\nReplay assertions passed\n" + json.dumps(summary, indent=2))
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as handle:
        handle.write(
            f"### {VERSION} GA dry run using {summary['model']}\n\n"
            f"Replayed `{HEAD}` with live discovery and AI. "
            "Only remote-head resolution was pinned to the historical state.\n\n"
            f"- Combined {', '.join(consolidated.rc_stages)}.\n"
            f"- {summary['source_entries']} source entries became "
            f"{summary['final_bullets']} bullets with {summary['credited_prs']} PR credits.\n"
            "- Verified older sections, contributors, security wording, RC-only effects, "
            "version promotion, and an unchanged clone.\n"
            "- Dry run only. No publication attempted.\n"
        )


if __name__ == "__main__":
    main_replay()
