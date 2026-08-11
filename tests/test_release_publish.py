"""Tests for protected publication: revalidation, create-at-SHA, post-verify."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from github.GithubException import GithubException

from scripts.release.models import ReleasePhase
from scripts.release.publish import (
    PublishPlan,
    plan_digest,
    plan_publication,
    publish_release,
    render_plan_summary,
)
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


def _with_manifest(run: MagicMock) -> MagicMock:
    """Append the qualification-manifest artifact to a qual-run mock.

    The shared fixture predates the manifest requirement in
    qualification.py; idempotent so it stays harmless once the fixture
    carries the manifest itself.
    """
    artifacts = list(run.get_artifacts.return_value)
    if not any(a.name == "qualification-manifest" for a in artifacts):
        manifest = MagicMock(expired=False, size_in_bytes=64)
        manifest.name = "qualification-manifest"
        run.get_artifacts.return_value = artifacts + [manifest]
    return run


def _ready_repo(**overrides: object) -> MagicMock:
    """A repo mock in the READY state whose contents serve the publish reads."""
    repo = repo_mock(tags=["9.1.0"], **overrides)  # type: ignore[arg-type]
    for run in repo.get_workflow.return_value.get_runs.return_value:
        _with_manifest(run)

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


def _publishable_repo() -> MagicMock:
    """A READY repo mock whose create-release call makes the tag resolvable."""
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
    def test_publishes_at_exact_sha_with_explicit_flags(self) -> None:
        # Bindings matching the revalidated plan exactly must publish: this
        # is the approved path (main.py always passes both bindings).
        repo = _publishable_repo()
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
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
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="approval was for"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.2")
        repo.create_git_release.assert_not_called()

    def test_tag_pointing_elsewhere_after_create_is_critical(self) -> None:
        repo = _publishable_repo()

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


class TestPlanDigestBinding:
    def test_matching_digest_publishes(self) -> None:
        # The digest the approver saw, recomputed from an unchanged world,
        # must match and publish.
        repo = _publishable_repo()
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson", controller_sha="f" * 40)
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA,
                              expected_digest=plan_digest(plan),
                              controller_sha="f" * 40)
        assert url == "https://x/releases/9.1.1"

    def test_digest_mismatch_refuses_before_any_write(self) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError,
                           match="plan changed after approval"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA,
                            expected_digest="0" * 64)
        repo.create_git_release.assert_not_called()

    def test_controller_sha_drift_changes_the_digest_and_refuses(self) -> None:
        # Approval bound one controller commit; execution from another
        # commit (a force-push between validate and publish) must refuse.
        repo = _publishable_repo()
        plan = plan_publication(gh_mock(repo), _POLICY, branch="9.1",
                                actor="madolson", controller_sha="f" * 40)
        with pytest.raises(ReleaseControlError,
                           match="plan changed after approval"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA,
                            expected_digest=plan_digest(plan),
                            controller_sha="e" * 40)
        repo.create_git_release.assert_not_called()

    def test_absent_digest_is_legacy_and_the_tag_sha_binding_still_holds(self) -> None:
        repo = _publishable_repo()
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"

    def test_digest_is_stable_and_covers_every_bound_dimension(self) -> None:
        base = _plan()
        assert plan_digest(base) == plan_digest(_plan())
        import re as _re
        assert _re.fullmatch(r"[0-9a-f]{64}", plan_digest(base))
        variants = [
            _plan(tag="9.1.2"),
            _plan(sha="b" * 40),
            _plan(prerelease=True),
            _plan(make_latest="false"),
            _plan(body="different notes"),
            _plan(qualification_run_id=901),
            _plan(tag_protected=True),
            _plan(controller_sha="f" * 40),
        ]
        digests = {plan_digest(v) for v in variants}
        assert plan_digest(base) not in digests
        assert len(digests) == len(variants)  # every dimension binds

    def test_summary_shows_the_short_digest(self) -> None:
        plan = _plan()
        summary = render_plan_summary(plan)
        assert f"Plan digest: `{plan_digest(plan)[:12]}`" in summary


class TestCreateReleaseRecovery:
    """create_git_release is never blind-retried: one attempt, then
    read-after-write recovery for the lost-response case only."""

    def test_lost_response_with_the_release_at_the_approved_sha_recovers(self) -> None:
        repo = _publishable_repo()

        def _lost_response(*args: object, **kwargs: object) -> None:
            # The create landed server-side; only the response was lost.
            ref = MagicMock()
            ref.object.type = "commit"
            ref.object.sha = MERGE_SHA
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = ref
            repo.get_release.side_effect = None
            repo.get_release.return_value = MagicMock(
                html_url="https://x/releases/9.1.1")
            raise GithubException(502, "bad gateway", {})

        repo.create_git_release.side_effect = _lost_response
        url = publish_release(gh_mock(repo), _POLICY, branch="9.1",
                              actor="madolson", expected_tag="9.1.1",
                              expected_sha=MERGE_SHA)
        assert url == "https://x/releases/9.1.1"
        repo.create_git_release.assert_called_once()

    def test_genuinely_failed_create_re_raises_without_a_retry(self) -> None:
        repo = _publishable_repo()
        repo.create_git_release.side_effect = GithubException(422, "boom", {})
        with pytest.raises(GithubException, match="boom"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_called_once()  # no blind retry

    def test_release_existing_at_the_wrong_sha_re_raises(self) -> None:
        # Someone else's release under the same tag is not our success.
        repo = _publishable_repo()

        def _foreign_release(*args: object, **kwargs: object) -> None:
            ref = MagicMock()
            ref.object.type = "commit"
            ref.object.sha = "e" * 40
            repo.get_git_ref.side_effect = None
            repo.get_git_ref.return_value = ref
            repo.get_release.side_effect = None
            repo.get_release.return_value = MagicMock(
                html_url="https://x/releases/9.1.1")
            raise GithubException(502, "bad gateway", {})

        repo.create_git_release.side_effect = _foreign_release
        with pytest.raises(GithubException, match="bad gateway"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_called_once()


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
            qual_runs=[_with_manifest(qualification_run(tag="9.2.0-rc1"))],
        )
        repo.get_workflow.return_value.get_runs.return_value = [
            _with_manifest(qualification_run(tag="9.2.0-rc1"))
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
              can_admins_bypass: bool = False, exists: bool = True,
              deployment_branch_policy: "dict | None | str" = "default") -> MagicMock:
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
        "deployment_branch_policy": (
            {"protected_branches": True, "custom_branch_policies": False}
            if deployment_branch_policy == "default" else deployment_branch_policy
        ),
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

    def test_reviewer_rule_with_empty_reviewers_list_is_refused(self) -> None:
        # A required_reviewers rule whose reviewers were all removed still
        # exists as a rule; it must count as "no required reviewers", not
        # as protection.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([{"type": "required_reviewers",
                           "prevent_self_review": True, "reviewers": []}])
        with pytest.raises(ReleaseControlError, match="no required reviewers"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_absent_prevent_self_review_key_fails_closed(self) -> None:
        # Older API payloads may omit prevent_self_review entirely; absence
        # must read as "not prevented", never as protected.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([{"type": "required_reviewers",
                           "reviewers": [{"type": "User"}]}])
        with pytest.raises(ReleaseControlError, match="self-review"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_one_lax_rule_among_compliant_rules_is_refused(self) -> None:
        # Every reviewer rule must prevent self-review: an approval can
        # satisfy whichever rule is laxest.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([
            {"type": "required_reviewers", "prevent_self_review": True,
             "reviewers": [{"type": "Team"}]},
            {"type": "required_reviewers",
             "reviewers": [{"type": "User"}]},  # key absent on this rule
        ])
        with pytest.raises(ReleaseControlError, match="self-review"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_empty_200_payload_fails_closed_on_every_dimension(self) -> None:
        # An environment endpoint answering 200 with {} (proxy stripping,
        # API drift) carries no evidence of protection; both the reviewer
        # requirement and the bypass default must fail closed.
        from scripts.release.publish import ensure_environment_protected
        repo = MagicMock()
        env = MagicMock()
        env.raw_data = {}
        repo.get_environment.return_value = env
        with pytest.raises(ReleaseControlError) as excinfo:
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")
        assert "no required reviewers" in str(excinfo.value)
        assert "bypass" in str(excinfo.value)

    def test_absent_can_admins_bypass_key_fails_closed(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo()
        del repo.get_environment.return_value.raw_data["can_admins_bypass"]
        with pytest.raises(ReleaseControlError, match="bypass"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_team_only_reviewers_satisfy_the_reviewer_requirement(self) -> None:
        # A Team reviewer is a real gate (any team member can approve, and
        # prevent_self_review still applies); it must be accepted, not
        # refused for lacking a User entry.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo([{"type": "required_reviewers",
                           "prevent_self_review": True,
                           "reviewers": [{"type": "Team", "reviewer": {"slug": "core-team"}}]}])
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

    def test_null_deployment_branch_policy_is_refused(self) -> None:
        # A null policy means ANY branch (including an attacker's topic
        # branch) can deploy to the gated environment.
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo(deployment_branch_policy=None)
        with pytest.raises(ReleaseControlError,
                           match="not restricted to specific branches"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_absent_deployment_branch_policy_key_fails_closed(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo()
        del repo.get_environment.return_value.raw_data["deployment_branch_policy"]
        with pytest.raises(ReleaseControlError,
                           match="not restricted to specific branches"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_branch_policy_with_neither_restriction_is_refused(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo(deployment_branch_policy={
            "protected_branches": False, "custom_branch_policies": False})
        with pytest.raises(ReleaseControlError,
                           match="not restricted to specific branches"):
            ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_custom_branch_policies_satisfy_the_restriction(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo(deployment_branch_policy={
            "protected_branches": False, "custom_branch_policies": True})
        ensure_environment_protected(self._gh(repo), _POLICY, "o/agent")

    def test_protected_branches_satisfy_the_restriction(self) -> None:
        from scripts.release.publish import ensure_environment_protected
        repo = _env_repo(deployment_branch_policy={
            "protected_branches": True, "custom_branch_policies": False})
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
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="candidate changed"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha="f" * 40)
        repo.create_git_release.assert_not_called()

    @pytest.mark.parametrize("mangled_sha", [
        MERGE_SHA.upper(),        # case-changed copy of the same commit
        MERGE_SHA[:12],           # abbreviated: prefixes must not bind
        f"{MERGE_SHA}\n",         # trailing newline from a copy-paste
        f" {MERGE_SHA}",          # leading whitespace
    ])
    def test_any_deviation_in_the_expected_sha_refuses(self, mangled_sha: str) -> None:
        # The binding must be an exact string compare of the full SHA: no
        # case folding, no prefix acceptance, no whitespace tolerance.
        # Refusing the uppercase twin of the right commit is deliberate
        # fail-closed behavior; the operator re-copies the exact value.
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="candidate changed"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=mangled_sha)
        repo.create_git_release.assert_not_called()

    @pytest.mark.parametrize("mangled_tag", ["9.1.1\n", " 9.1.1", "9.1.1 "])
    def test_whitespace_in_the_expected_tag_refuses(self, mangled_tag: str) -> None:
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError, match="approval was for"):
            publish_release(gh_mock(repo), _POLICY, branch="9.1",
                            actor="madolson", expected_tag=mangled_tag,
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_not_called()

    def test_plan_approved_for_one_branch_cannot_execute_on_another(self) -> None:
        # A plan produced for 9.1 (tag 9.1.1) executed with --branch 8.0:
        # even when the 9.1 tracker leaks through the issue lookup (the
        # mock ignores label filters, mimicking a mislabeled tracker), the
        # notes-PR version must not match the 8.0 line, so revalidation
        # refuses before the tag/SHA binding is even consulted.
        repo = _publishable_repo()
        with pytest.raises(ReleaseControlError):
            publish_release(gh_mock(repo), _POLICY, branch="8.0",
                            actor="madolson", expected_tag="9.1.1",
                            expected_sha=MERGE_SHA)
        repo.create_git_release.assert_not_called()


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


def _plan(tag: str = "9.1.1", **overrides: object) -> PublishPlan:
    values: "dict[str, object]" = dict(
        tag=tag, sha=MERGE_SHA, prerelease=False,
        make_latest="true", body="notes body", issue_number=11,
        tracker_url="https://x/issues/11",
        qualification_url="https://x/qruns/900")
    values.update(overrides)
    return PublishPlan(**values)  # type: ignore[arg-type]


def _ruleset_repo(rulesets: "list[dict]", details: "dict[int, dict]") -> MagicMock:
    """A repo mock whose rulesets endpoints serve the given payloads."""
    repo = MagicMock()
    repo.url = "https://api.github.com/repos/o/r"

    def _request(verb: str, url: str) -> "tuple[dict, object]":
        if url.endswith("/rulesets"):
            return {}, rulesets
        return {}, details[int(url.rsplit("/", 1)[1])]

    repo._requester.requestJsonAndCheck.side_effect = _request
    return repo


_FULL_RULES = [{"type": "deletion"}, {"type": "update"}]


class TestTagRulesetProbe:
    def test_active_tag_ruleset_with_full_rules_is_protected(self) -> None:
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES, "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is True

    def test_non_fast_forward_counts_as_the_update_rule(self) -> None:
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": [{"type": "deletion"}, {"type": "non_fast_forward"}],
                 "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is True

    def test_matching_but_ruleless_ruleset_is_not_protected(self) -> None:
        # The ruleset covers the ref but carries no deletion/update rules:
        # it restricts nothing, so the tag must never claim immutability.
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": [], "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is False

    def test_deletion_rule_alone_is_not_protected(self) -> None:
        # The tag could still be MOVED: immutability needs both halves.
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": [{"type": "deletion"}], "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is False

    def test_app_bypass_actor_is_unprotected_for_us(self) -> None:
        # The publishing identity is a GitHub App; a ruleset any App can
        # bypass constrains exactly nothing we rely on.
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES,
                 "bypass_actors": [{"actor_id": 5, "actor_type": "Integration",
                                    "bypass_mode": "always"}]}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is False

    def test_invisible_bypass_data_degrades_to_unknown(self) -> None:
        # No bypass_actors key in the payload: an App bypass cannot be
        # ruled out, so the verdict is unknown, never protected.
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {"include": ["refs/tags/*"],
                                             "exclude": []}},
                 "rules": _FULL_RULES}},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is None

    def test_excluded_tag_is_not_protected(self) -> None:
        # Mirrors upstream: the ruleset excludes 1-7.* tags, so a 7.x
        # release must never claim immutability.
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 1, "target": "tag", "enforcement": "active"}],
            {1: {"conditions": {"ref_name": {
                "include": ["~ALL"],
                "exclude": ["refs/tags/[1-7].*"]}},
                "rules": _FULL_RULES, "bypass_actors": []}},
        )
        assert tag_ruleset_protected(repo, "7.2.11") is False
        assert tag_ruleset_protected(repo, "9.1.1") is True

    def test_no_tag_ruleset_is_not_protected(self) -> None:
        from scripts.release.publish import tag_ruleset_protected
        repo = _ruleset_repo(
            [{"id": 2, "target": "branch", "enforcement": "active"},
             {"id": 3, "target": "tag", "enforcement": "disabled"}],
            {},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is False

    def test_api_failure_is_unknown(self) -> None:
        from scripts.release.publish import tag_ruleset_protected
        repo = MagicMock()
        repo.url = "https://api.github.com/repos/o/r"
        repo._requester.requestJsonAndCheck.side_effect = GithubException(
            403, "forbidden", {},
        )
        assert tag_ruleset_protected(repo, "9.1.1") is None


class TestPlanSummaryHonesty:
    def test_protected_tag_states_verified_immutability(self) -> None:
        summary = render_plan_summary(_plan(tag_protected=True))
        assert ("The created tag is ruleset-protected and cannot be moved "
                "or deleted.") in summary
        assert "NOT ruleset-protected" not in summary
        assert "creates the release tag" in summary
        assert "cannot be moved or deleted. Verify" not in summary

    @pytest.mark.parametrize("protection", [False, None],
                             ids=["unprotected", "unknown"])
    def test_unprotected_or_unknown_tag_warns(self, protection: "bool | None") -> None:
        summary = render_plan_summary(_plan(tag_protected=protection))
        assert ("**WARNING:** The created tag is NOT ruleset-protected in "
                "this repository; extend tag protection before relying on "
                "immutability.") in summary
        assert "ruleset-protected and cannot be moved" not in summary

    def test_controller_sha_is_in_the_checklist_when_provided(self) -> None:
        summary = render_plan_summary(_plan(), controller_sha="f" * 40)
        assert f"- [ ] Controller code: `{'f' * 12}`" in summary

    def test_controller_sha_line_is_omitted_when_empty(self) -> None:
        assert "Controller code:" not in render_plan_summary(_plan())


class TestApprovalEvidence:
    def _issue_repo(self) -> "tuple[MagicMock, MagicMock]":
        repo = MagicMock()
        issue = MagicMock()
        repo.get_issue.return_value = issue
        gh = MagicMock()
        gh.get_repo.return_value = repo
        return gh, issue

    def test_first_post_mentions_the_approvers(self) -> None:
        # Creating the comment is what fires the notification; the plan
        # summary alone pings nobody.
        from scripts.release.publish import _APPROVAL_MARKER, post_approval_evidence
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/7")
        body = issue.create_comment.call_args.kwargs["body"]
        assert _APPROVAL_MARKER in body
        assert "> [!IMPORTANT]" in body
        assert "**@valkey-io/core-team: Approval Needed to Publish `9.1.1`.**" in body
        assert "**Approve here:** https://x/runs/7" in body
        assert "\u2014" not in body

    def test_revalidation_edits_in_place_without_a_new_ping(self) -> None:
        # An edit does not re-notify: one ping per approval wait, not one
        # per cron re-validation.
        from scripts.release.publish import _APPROVAL_MARKER, post_approval_evidence
        gh, issue = self._issue_repo()
        existing = MagicMock(body=f"{_APPROVAL_MARKER}\nstale evidence")
        with patch("scripts.release.issue.trusted_comments", return_value=[existing]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/8")
        issue.create_comment.assert_not_called()
        body = existing.edit.call_args.kwargs["body"]
        assert "**@valkey-io/core-team: Approval Needed to Publish `9.1.1`.**" in body
        assert "https://x/runs/8" in body

    def test_evidence_carries_the_controller_sha_when_provided(self) -> None:
        from scripts.release.publish import post_approval_evidence
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/9",
                                   controller_sha="f" * 40)
        body = issue.create_comment.call_args.kwargs["body"]
        assert f"- [ ] Controller code: `{'f' * 12}`" in body

    def test_evidence_omits_the_controller_line_when_unknown(self) -> None:
        from scripts.release.publish import post_approval_evidence
        gh, issue = self._issue_repo()
        with patch("scripts.release.issue.trusted_comments", return_value=[]):
            post_approval_evidence(gh, _POLICY, _plan(), "https://x/runs/9")
        assert "Controller code:" not in issue.create_comment.call_args.kwargs["body"]


class TestMakeLatestDecision:
    def test_first_ever_release_becomes_latest(self) -> None:
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.side_effect = GithubException(404, "no releases", {})
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_non_version_latest_tag_is_taken_over(self) -> None:
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.return_value = MagicMock(tag_name="nightly-build")
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_latest_equal_to_the_publishing_tag_does_not_move_the_pointer(self) -> None:
        # Latest already IS this tag (a republish attempt that slipped past
        # the tag-exists gate): equal is not greater, so the decision must
        # be false, not a >= slip.
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.return_value = MagicMock(tag_name="9.1.1")
        assert _make_latest_decision(repo, "9.1.1", "ga") == "false"

    def test_ga_above_a_numerically_lower_line_becomes_latest(self) -> None:
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.return_value = MagicMock(tag_name="8.2.9")
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_two_digit_patch_compares_numerically_not_lexically(self) -> None:
        # A string sort would call '9.1.10' < '9.1.9' and strand the latest
        # pointer on the older release.
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.return_value = MagicMock(tag_name="9.1.9")
        assert _make_latest_decision(repo, "9.1.10", "ga") == "true"

    def test_rc_shaped_latest_tag_is_treated_as_unparseable_takeover(self) -> None:
        # parse_version is anchored, so '9.2.0-rc1' raises and the GA takes
        # over the pointer. GitHub never marks a prerelease as the latest
        # release, so a higher-line rc sitting at latest is unreachable
        # through the API; the takeover branch is the documented fallback
        # for any non-M.m.p tag.
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        repo.get_latest_release.return_value = MagicMock(tag_name="9.2.0-rc1")
        assert _make_latest_decision(repo, "9.1.1", "ga") == "true"

    def test_rc_stage_never_asks_github_and_never_takes_latest(self) -> None:
        from scripts.release.publish import _make_latest_decision
        repo = MagicMock()
        assert _make_latest_decision(repo, "9.2.0", "rc1") == "false"
        repo.get_latest_release.assert_not_called()


class TestPreviousTag:
    def test_rc_follows_the_previous_rc(self) -> None:
        from scripts.release.publish import _previous_tag
        repo = repo_mock(tags=["9.2.0-rc1"])
        assert _previous_tag(repo, "9.2.0", "rc2") == "9.2.0-rc1"

    def test_rc_without_its_predecessor_tag_is_not_guessed(self) -> None:
        from scripts.release.publish import _previous_tag
        repo = repo_mock(tags=["9.1.0"])
        assert _previous_tag(repo, "9.2.0", "rc2") is None

    def test_ga_follows_its_last_rc(self) -> None:
        from scripts.release.publish import _previous_tag
        repo = repo_mock(tags=["9.2.0-rc1", "9.2.0-rc2"])
        assert _previous_tag(repo, "9.2.0", "ga") == "9.2.0-rc2"
