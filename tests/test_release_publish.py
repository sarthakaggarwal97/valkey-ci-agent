from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from github.GithubException import GithubException

from scripts.release import publish as publish_mod
from scripts.release.models import PublishPlan, ReleasePolicy

SHA = "a" * 40
POLICY = ReleasePolicy(
    repo="valkey-io/valkey",
    authorized_teams=("valkey-io/core-team",),
    branches=("9.1",),
    checks_workflow="ci.yml",
    required_checks=("test",),
)
VERSION_H = """\
#define VALKEY_VERSION "9.1.2"
#define VALKEY_VERSION_NUM 0x00090102
#define VALKEY_RELEASE_STAGE "ga"
"""
NOTES = """\
Valkey 9.1 release notes
========================

Valkey 9.1.2  -  Released Wed 20 August 2026
------------------------------------------------

### Bug Fixes
* Fixed a thing

Valkey 9.1.1  -  Released Tue 01 July 2026
---------------------------------------------
older
"""


def _content(text: str):
    return SimpleNamespace(decoded_content=text.encode())


def _repo(*, head: str = SHA, notes: str = NOTES):
    repo = MagicMock()
    repo.get_branch.return_value.commit.sha = head
    repo.get_contents.side_effect = lambda path, ref: _content(VERSION_H if path == "src/version.h" else notes)
    repo.get_releases.return_value = []
    repo.get_tags.return_value = [SimpleNamespace(name="9.1.1")]
    repo.get_git_ref.side_effect = GithubException(404, "missing", {})
    repo.get_commit.return_value.get_pulls.return_value = [
        SimpleNamespace(
            merged=True,
            merge_commit_sha=SHA,
            html_url="https://example/pr/1",
            base=SimpleNamespace(ref="9.1"),
            head=SimpleNamespace(
                ref="agent/release-cut/9.1.2-ga",
                repo=SimpleNamespace(full_name="valkey-io/valkey"),
            ),
        )
    ]
    repo._requester.requestJsonAndCheck.return_value = ({}, [])
    return repo


def _gh(repo):
    gh = MagicMock()
    gh.get_repo.return_value = repo
    return gh


def test_plan_binds_branch_head_version_checks_and_notes(monkeypatch: pytest.MonkeyPatch) -> None:
    checked = []
    monkeypatch.setattr(publish_mod, "require_green_checks", lambda repo, policy, sha: checked.append(sha))
    monkeypatch.setattr(
        publish_mod,
        "tag_ruleset_protected",
        lambda *a: publish_mod.TagRulesetVerdict(True),
    )
    plan = publish_mod.plan_publication(_gh(_repo()), POLICY, branch="9.1", candidate_sha=SHA)
    assert plan.tag == "9.1.2"
    assert plan.sha == SHA
    assert plan.make_latest == "true"
    assert "Fixed a thing" in plan.body
    assert "older" not in plan.body
    assert checked == [SHA]


@pytest.mark.parametrize(
    ("verdict", "message"),
    [
        (publish_mod.TagRulesetVerdict(False), "cannot verify an active immutable-tag ruleset"),
        (publish_mod.TagRulesetVerdict(None), "cannot verify an active immutable-tag ruleset"),
    ],
)
def test_plan_fails_closed_on_tag_ruleset_drift(
    monkeypatch: pytest.MonkeyPatch,
    verdict: publish_mod.TagRulesetVerdict,
    message: str,
) -> None:
    monkeypatch.setattr(publish_mod, "require_green_checks", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "tag_ruleset_protected", lambda *a: verdict)
    with pytest.raises(publish_mod.ReleaseError, match=message):
        publish_mod.plan_publication(_gh(_repo()), POLICY, branch="9.1", candidate_sha=SHA)


def test_moved_branch_refuses_before_publication(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(publish_mod.ReleaseError, match="not candidate"):
        publish_mod.plan_publication(_gh(_repo(head="b" * 40)), POLICY, branch="9.1", candidate_sha=SHA)


def test_missing_release_notes_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publish_mod, "require_green_checks", lambda *a, **k: None)
    with pytest.raises(publish_mod.ReleaseError, match="no section"):
        publish_mod.plan_publication(_gh(_repo(notes="no release here")), POLICY, branch="9.1", candidate_sha=SHA)


def test_plan_requires_candidate_from_canonical_merged_preparation_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo()
    repo.get_commit.return_value.get_pulls.return_value = []
    with pytest.raises(publish_mod.ReleaseError, match="canonical agent/release-cut"):
        publish_mod.plan_publication(_gh(repo), POLICY, branch="9.1", candidate_sha=SHA)
    repo.get_commit.assert_called_once_with(SHA)


def test_plan_refuses_version_that_skips_the_derived_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # version.h records 9.1.2 (ga) but the line's tags derive 9.1.4 as the
    # next patch: a handcrafted canonical-looking preparation PR must not
    # publish out of sequence.
    monkeypatch.setattr(publish_mod, "require_green_checks", lambda *a: None)
    repo = _repo()
    repo.get_tags.return_value = [SimpleNamespace(name="9.1.3"), SimpleNamespace(name="9.1.1")]
    with pytest.raises(publish_mod.ReleaseError, match="derives 9.1.4 as the next release"):
        publish_mod.plan_publication(_gh(repo), POLICY, branch="9.1", candidate_sha=SHA)


def test_plan_accepts_rerun_after_interrupted_attempt_created_the_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An interrupted attempt already created tag 9.1.2 at this exact
    # candidate. Re-derivation would now compute 9.1.3, so the existing
    # same-SHA tag is the sequence evidence and the rerun must proceed.
    monkeypatch.setattr(publish_mod, "require_green_checks", lambda *a: None)
    monkeypatch.setattr(
        publish_mod,
        "tag_ruleset_protected",
        lambda *a: publish_mod.TagRulesetVerdict(True),
    )
    repo = _repo()
    repo.get_tags.return_value = [SimpleNamespace(name="9.1.2"), SimpleNamespace(name="9.1.1")]
    monkeypatch.setattr(publish_mod, "resolve_tag_commit", lambda *a: SHA)
    plan = publish_mod.plan_publication(_gh(repo), POLICY, branch="9.1", candidate_sha=SHA)
    assert plan.tag == "9.1.2"


def _plan() -> PublishPlan:
    return PublishPlan(
        branch="9.1",
        tag="9.1.2",
        version="9.1.2",
        stage="ga",
        sha=SHA,
        body="notes\n",
        prerelease=False,
        make_latest="true",
        tag_protected=True,
    )


def test_plan_digest_binds_release_body() -> None:
    plan = _plan()
    changed = PublishPlan(**{**plan.__dict__, "body": "different\n"})
    assert publish_mod.plan_digest(plan) != publish_mod.plan_digest(changed)


def test_publish_revalidates_digest_and_creates_exact_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan()
    repo = _repo()
    repo.create_git_ref.side_effect = None
    repo.create_git_ref.return_value = object()
    repo.get_git_ref.side_effect = None
    repo.get_git_ref.return_value.object = SimpleNamespace(type="commit", sha=SHA)
    repo.create_git_release.return_value.html_url = "https://example/release"
    gh = _gh(repo)
    authorized = []
    monkeypatch.setattr(
        publish_mod,
        "ensure_authorized",
        lambda gh, policy, actor: authorized.append(actor),
    )
    monkeypatch.setattr(publish_mod, "plan_publication", lambda *a, **k: plan)
    url = publish_mod.publish_release(
        gh,
        POLICY,
        branch="9.1",
        candidate_sha=SHA,
        actor="approver",
        expected_digest=publish_mod.plan_digest(plan),
    )
    assert url == "https://example/release"
    assert authorized == ["approver"]
    repo.create_git_ref.assert_called_once_with(ref="refs/tags/9.1.2", sha=SHA)
    repo.create_git_release.assert_called_once()


def test_publish_refuses_plan_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publish_mod, "ensure_authorized", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "plan_publication", lambda *a, **k: _plan())
    with pytest.raises(publish_mod.ReleaseError, match="changed after approval"):
        publish_mod.publish_release(
            _gh(_repo()),
            POLICY,
            branch="9.1",
            candidate_sha=SHA,
            actor="approver",
            expected_digest="0" * 64,
        )


def test_publish_recovers_a_lost_release_creation_response(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan()
    repo = _repo()
    repo.create_git_release.side_effect = ConnectionError("response lost")
    release = SimpleNamespace(html_url="https://example/release")
    monkeypatch.setattr(publish_mod, "ensure_authorized", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "plan_publication", lambda *a, **k: plan)
    monkeypatch.setattr(publish_mod, "_ensure_tag", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "_find_release", lambda *a, **k: release)
    monkeypatch.setattr(publish_mod, "resolve_tag_commit", lambda *a, **k: SHA)

    url = publish_mod.publish_release(
        _gh(repo),
        POLICY,
        branch="9.1",
        candidate_sha=SHA,
        actor="approver",
        expected_digest=publish_mod.plan_digest(plan),
    )

    assert url == release.html_url


def test_publish_verifies_tag_sha_after_release_creation(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan()
    repo = _repo()
    repo.create_git_release.return_value = SimpleNamespace(html_url="https://example/release")
    monkeypatch.setattr(publish_mod, "ensure_authorized", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "plan_publication", lambda *a, **k: plan)
    monkeypatch.setattr(publish_mod, "_ensure_tag", lambda *a, **k: None)
    monkeypatch.setattr(publish_mod, "resolve_tag_commit", lambda *a, **k: "b" * 40)

    with pytest.raises(publish_mod.ReleaseError, match="not created at approved SHA"):
        publish_mod.publish_release(
            _gh(repo),
            POLICY,
            branch="9.1",
            candidate_sha=SHA,
            actor="approver",
            expected_digest=publish_mod.plan_digest(plan),
        )


def test_make_latest_uses_numeric_ga_release_order() -> None:
    repo = MagicMock()
    repo.get_releases.return_value = [
        SimpleNamespace(tag_name="9.1.10", draft=False, prerelease=False),
        SimpleNamespace(tag_name="10.0.0-rc1", draft=False, prerelease=True),
        SimpleNamespace(tag_name="not-a-version", draft=False, prerelease=False),
    ]

    assert publish_mod._make_latest(repo, "9.1.11", "ga") == "true"
    assert publish_mod._make_latest(repo, "9.1.9", "ga") == "false"
    assert publish_mod._make_latest(repo, "10.0.0", "rc1") == "false"


def test_ruleset_immutability_verified_without_bypass_visibility() -> None:
    # GitHub omits bypass_actors from ruleset reads for tokens that cannot
    # edit rulesets. Immutability verification must not depend on that field:
    # requiring it forced administration:write onto an unattended credential,
    # to detect drift only a repository admin could cause and could equally
    # cause by editing the ruleset itself.
    repo = MagicMock()
    repo.url = "https://api.github.com/repos/valkey-io/valkey"
    repo._requester.requestJsonAndCheck.side_effect = [
        ({}, [{"id": 7, "target": "tag", "enforcement": "active"}]),
        (
            {},
            {
                "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
                "rules": [
                    {"type": "creation"},
                    {"type": "deletion"},
                    {"type": "update"},
                ],
            },
        ),
    ]
    assert publish_mod.tag_ruleset_protected(repo, "9.1.2") == publish_mod.TagRulesetVerdict(True)


def test_non_fast_forward_rule_does_not_prove_tag_immutability() -> None:
    repo = MagicMock()
    repo.url = "https://api.github.com/repos/valkey-io/valkey"
    repo._requester.requestJsonAndCheck.side_effect = [
        ({}, [{"id": 7, "target": "tag", "enforcement": "active"}]),
        ({}, {
            "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
            "rules": [
                {"type": "creation"}, {"type": "deletion"}, {"type": "non_fast_forward"},
            ],
            "bypass_actors": [{"actor_type": "Integration", "actor_id": 123}],
        }),
    ]
    assert publish_mod.tag_ruleset_protected(repo, "9.1.2").protected is False
