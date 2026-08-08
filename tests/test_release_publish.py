"""Tests for protected publication: revalidation, create-at-SHA, post-verify."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from github.GithubException import GithubException

from scripts.release.models import ReleasePhase
from scripts.release.publish import plan_publication, publish_release, render_plan_summary
from scripts.release.reconcile import ReleaseControlError
from tests.release_fixtures import (
    MERGE_SHA,
    gh_mock,
    make_policy,
    qualification_run,
    repo_mock,
    tracker,
)

_POLICY = make_policy()

_VERSION_H = (
    '#define VALKEY_VERSION "9.1.1"\n'
    "#define VALKEY_VERSION_NUM 0x00090101\n"
    '#define VALKEY_RELEASE_STAGE "ga"\n'
)
_NOTES = (
    "Valkey 9.1.1  -  Released Tue 21 July 2026\n"
    "=====\n"
    "Upgrade urgency MODERATE.\n\n"
    "### Bug Fixes\n* Fix a thing by @someone (#1234)\n\n"
    "Valkey 9.1.0  -  Released earlier\n=====\nolder section\n"
)


def _ready_repo(**overrides: object) -> MagicMock:
    """A repo mock in the READY state whose contents serve the publish reads."""
    repo = repo_mock(tags=["9.1.0"], **overrides)  # type: ignore[arg-type]

    def _contents(path: str, **kw: object) -> MagicMock:
        f = MagicMock()
        f.decoded_content = (_VERSION_H if path.endswith("version.h") else _NOTES).encode()
        return f

    repo.get_contents.side_effect = _contents
    repo.get_latest_release.return_value = MagicMock(tag_name="9.1.0")
    return repo


def _with_tracker(repo: MagicMock) -> MagicMock:
    repo.get_issues.return_value = [tracker()]
    return repo


class TestPlanPublication:
    def test_ready_release_produces_a_complete_plan(self) -> None:
        repo = _with_tracker(_ready_repo())
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")
        assert plan.tag == "9.1.1"
        assert plan.sha == MERGE_SHA
        assert not plan.prerelease
        assert plan.make_latest == "true"  # 9.1.1 > latest 9.1.0
        assert "Upgrade urgency MODERATE" in plan.body
        assert "compare/9.1.0...9.1.1" in plan.body
        assert "older section" not in plan.body
        summary = render_plan_summary(plan)
        assert "9.1.1" in summary and MERGE_SHA in summary

    def test_not_ready_release_is_refused_with_blockers(self) -> None:
        repo = _with_tracker(_ready_repo(qual_runs=[]))  # qualification missing
        with pytest.raises(ReleaseControlError, match="not ready to publish"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_existing_tag_is_refused(self) -> None:
        # A tag without a release now surfaces upstream in compute_status as
        # an unshippable alert, so planning refuses at the readiness gate
        # (publish.py's own tag check remains as defense-in-depth).
        repo = _with_tracker(_ready_repo())
        repo.get_git_ref.side_effect = None
        ref = MagicMock()
        ref.object.type = "commit"
        ref.object.sha = "f" * 40
        repo.get_git_ref.return_value = ref  # tag exists
        with pytest.raises(ReleaseControlError, match="unshippable"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_version_h_mismatch_is_refused(self) -> None:
        repo = _with_tracker(_ready_repo())
        wrong = _VERSION_H.replace("9.1.1", "9.1.0").replace("0x00090101", "0x00090100")

        def _contents(path: str, **kw: object) -> MagicMock:
            f = MagicMock()
            f.decoded_content = (wrong if path.endswith("version.h") else _NOTES).encode()
            return f

        repo.get_contents.side_effect = _contents
        with pytest.raises(ReleaseControlError, match="version.h"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_missing_notes_section_is_refused(self) -> None:
        repo = _with_tracker(_ready_repo())

        def _contents(path: str, **kw: object) -> MagicMock:
            f = MagicMock()
            f.decoded_content = (
                _VERSION_H if path.endswith("version.h") else "Valkey 9.1.0 only\n"
            ).encode()
            return f

        repo.get_contents.side_effect = _contents
        with pytest.raises(ReleaseControlError, match="no dated section"):
            plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")

    def test_old_line_patch_never_steals_latest(self) -> None:
        repo = _with_tracker(_ready_repo())
        repo.get_latest_release.return_value = MagicMock(tag_name="9.2.0")
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")
        assert plan.make_latest == "false"

    def test_unauthorized_actor_cannot_plan(self) -> None:
        from scripts.release.authorize import NotAuthorizedError
        repo = _with_tracker(_ready_repo())
        with pytest.raises(NotAuthorizedError):
            plan_publication(gh_mock(repo, member=False), _POLICY,
                             branch="9.1", actor="drive-by")


class TestPublishRelease:
    def _publishable_repo(self) -> MagicMock:
        repo = _with_tracker(_ready_repo())
        release = MagicMock(html_url="https://x/releases/9.1.1")
        repo.create_git_release.return_value = release
        repo.get_issue.return_value = repo.get_issues.return_value[0]

        def _tag_after_create(*args: object, **kwargs: object) -> MagicMock:
            ref = MagicMock()
            ref.object.type = "commit"
            ref.object.sha = MERGE_SHA
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = ref
            return release

        repo.create_git_release.side_effect = _tag_after_create
        return repo

    def test_publishes_at_exact_sha_with_explicit_flags(self) -> None:
        repo = self._publishable_repo()
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")
        assert url == "https://x/releases/9.1.1"
        kwargs = repo.create_git_release.call_args.kwargs
        assert repo.create_git_release.call_args.args[0] == "9.1.1"
        assert kwargs["target_commitish"] == MERGE_SHA
        assert kwargs["prerelease"] is False
        assert kwargs["make_latest"] == "true"
        assert kwargs["draft"] is False
        # Publication is recorded on the tracker.
        comment = repo.get_issue.return_value.create_comment.call_args.kwargs["body"]
        assert "9.1.1" in comment and MERGE_SHA in comment

    def test_expected_tag_mismatch_refuses_before_any_write(self) -> None:
        repo = self._publishable_repo()
        with pytest.raises(ReleaseControlError, match="approval was for"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.2")
        repo.create_git_release.assert_not_called()

    def test_tag_pointing_elsewhere_after_create_is_critical(self) -> None:
        repo = self._publishable_repo()

        def _wrong_tag(*args: object, **kwargs: object) -> MagicMock:
            ref = MagicMock()
            ref.object.type = "commit"
            ref.object.sha = "e" * 40
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = ref
            return MagicMock(html_url="https://x/releases/9.1.1")

        repo.create_git_release.side_effect = _wrong_tag
        with pytest.raises(ReleaseControlError, match="CRITICAL"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1", actor="madolson")


class TestRCPublication:
    def test_rc_plan_is_prerelease_and_never_latest(self) -> None:
        from tests.release_fixtures import notes_pr
        rc_notes = (
            "Valkey 9.2.0-rc1  -  Released today\n=====\nUpgrade urgency LOW.\n"
        )
        rc_version_h = (
            '#define VALKEY_VERSION "9.2.0"\n'
            "#define VALKEY_VERSION_NUM 0x00090200\n"
            '#define VALKEY_RELEASE_STAGE "rc1"\n'
        )
        repo = repo_mock(
            pulls=[notes_pr(head_ref="agent/release-cut/9.2.0-rc1")],
            issues=[tracker(branch="9.2")],
            tags=["9.1.0"],
            qual_runs=[qualification_run(tag="9.2.0-rc1")],
        )
        repo.get_workflow.return_value.get_runs.return_value = [
            qualification_run(tag="9.2.0-rc1")
        ]

        def _contents(path: str, **kw: object) -> MagicMock:
            f = MagicMock()
            f.decoded_content = (
                rc_version_h if path.endswith("version.h") else rc_notes
            ).encode()
            return f

        repo.get_contents.side_effect = _contents
        repo.get_latest_release.return_value = MagicMock(tag_name="9.1.0")
        policy = make_policy(branches=("9.2",))

        plan = plan_publication(gh_mock(repo), policy, branch="9.2", actor="madolson")
        assert plan.tag == "9.2.0-rc1"
        assert plan.prerelease
        assert plan.make_latest == "false"


def _env_repo(protection_rules: "list | None" = None, *,
              can_admins_bypass: bool = False, exists: bool = True) -> MagicMock:
    repo = MagicMock()
    if not exists:
        repo.get_environment.side_effect = GithubException(404, "missing", {})
        return repo
    env = MagicMock()
    env.raw_data = {
        "protection_rules": protection_rules if protection_rules is not None else [
            {"type": "required_reviewers", "prevent_self_review": True,
             "reviewers": [{"type": "Team"}]},
        ],
        "can_admins_bypass": can_admins_bypass,
    }
    repo.get_environment.return_value = env
    return repo


class TestEnvironmentProtection:
    def _gh(self, repo: MagicMock) -> MagicMock:
        gh = MagicMock()
        gh.get_repo.return_value = repo
        return gh

    def test_fully_protected_environment_passes(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        ensure_environment_protected(self._gh(_env_repo()), _POLICY, "o/agent")

    def test_missing_environment_is_refused(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        with pytest.raises(ReleaseControlError, match="does not exist"):
            ensure_environment_protected(self._gh(_env_repo(exists=False)),
                                         _POLICY, "o/agent")

    def test_no_reviewer_rules_is_refused(self) -> None:
        # The exact live state GitHub auto-creates: no rules, bypass on.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([], can_admins_bypass=True)
        with pytest.raises(ReleaseControlError, match="no required reviewers"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_self_review_not_prevented_is_refused(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([{"type": "required_reviewers",
                           "prevent_self_review": False,
                           "reviewers": [{"type": "User"}]}])
        with pytest.raises(ReleaseControlError, match="self-review"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_admin_bypass_is_refused(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo(can_admins_bypass=True)
        with pytest.raises(ReleaseControlError, match="bypass"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_fork_user_policy_skips_the_check_entirely(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        gh = MagicMock()
        policy = make_policy(repo="sarthakaggarwal97/valkey",
                             authorized_team="user:sarthakaggarwal97")
        ensure_environment_protected(gh, policy, "o/agent")
        gh.get_repo.assert_not_called()


class TestShaBinding:
    def test_expected_sha_mismatch_refuses_before_any_write(self) -> None:
        repo = TestPublishRelease._publishable_repo(TestPublishRelease())
        with pytest.raises(ReleaseControlError, match="candidate changed"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha="f" * 40)
        repo.create_git_release.assert_not_called()

    def test_matching_tag_and_sha_publish(self) -> None:
        repo = TestPublishRelease._publishable_repo(TestPublishRelease())
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url


def test_unattended_planning_skips_the_actor_check_only() -> None:
    # The controller dispatches validate as the Actions bot; planning is
    # read-only and the human gate stays at approval + execute.
    repo = _with_tracker(_ready_repo())
    gh = gh_mock(repo, member=False)  # the bot is in no team
    plan = plan_publication(gh, _POLICY, branch="9.1", actor="github-actions[bot]",
                            skip_authorization=True)
    assert plan.tag == "9.1.1"
    from scripts.release.authorize import NotAuthorizedError
    with pytest.raises(NotAuthorizedError):
        plan_publication(gh, _POLICY, branch="9.1", actor="github-actions[bot]")
