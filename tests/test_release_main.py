"""Release transaction, security-boundary, and producer-contract tests."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import re
import subprocess
import sys
import urllib.error
import zipfile
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Iterator, Mapping

import pytest
import yaml

import scripts.release.main as release

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / "ci-agent"
AUTO = ROOT / "rel-auto"
FORK = ROOT / "valkey-fork"
SHA = "a" * 40
AUTOMATION_SHA = "b" * 40
NONCE = "c" * 32
DIGEST = "d" * 64
APP = 101
NOTES_APP = 102
RELEASE_APP = 103


def identity(**changes: str) -> str:
    fields = {
        "release_id": "r0123456789abcdef0123",
        "tag": "9.1.0",
        "source_sha": SHA,
        "qualification_nonce": NONCE,
        "automation_sha": AUTOMATION_SHA,
        "plan_digest": DIGEST,
    }
    fields.update(changes)
    return release._identity(fields)


def policy() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "repo": "valkey-io/valkey",
        "authorized_team": "valkey-io/core-team",
        "checks_workflow": "ci.yml",
        "branches": ["8.0", "8.1", "9.1"],
        "required_checks": ["required"],
        "downstream": {
            "automation_repo": "valkey-io/valkey-release-automation",
            "build_workflow": "build-release.yml",
            "qualification_workflow": "qualify-release.yml",
            "tarball_targets": [
                "jammy/x86_64",
                "noble/x86_64",
                "jammy/arm64",
                "noble/arm64",
            ],
            "hashes_repo": "valkey-io/valkey-hashes",
            "container_repo": "valkey-io/valkey-container",
            "doc_repo": "valkey-io/valkey-doc",
            "website_repo": "valkey-io/valkey-io.github.io",
            "bundle_repo": "valkey-io/valkey-bundle",
            "dockerhub_repo": "valkey/valkey",
            "bundle_dockerhub_repo": "valkey/valkey-bundle",
            "ghcr_image_repo": "valkey-io/valkey",
            "ecr_namespace": "valkey",
            "helm_index_url": "https://valkey.io/valkey-helm/index.yaml",
        },
    }


def package_targets() -> dict[str, list[str]]:
    config = json.loads((AUTO / ".github/package-platforms.json").read_text())
    return {
        kind: sorted(
            f"{kind}/{platform['id']}/{arch}"
            for platform in config[kind]["platform"] for arch in config[kind]["arch"]
        )
        for kind in ("rpm", "deb")
    }


class API:
    def __init__(self) -> None:
        self.gets: dict[str, Any] = {}
        self.maybes: dict[str, Any] = {}
        self.page_values: list[Any] = []
        self.object_values: dict[str, list[Any]] = {}
        self.object_paths: list[str] = []
        self.posts: list[tuple[str, Mapping[str, Any]]] = []
        self.patches: list[tuple[str, Mapping[str, Any]]] = []
        self.dispatches: list[tuple[str, str, str, Mapping[str, str]]] = []
        self.token = "token"

    def get(self, path: str) -> Any:
        if path not in self.gets:
            raise AssertionError(f"unexpected GET {path}")
        return self.gets[path]

    def maybe(self, path: str) -> Any:
        return self.maybes.get(path)

    def pages(self, path: str) -> Iterator[Any]:
        yield from self.page_values

    def objects(self, path: str, key: str) -> list[Any]:
        self.object_paths.append(path)
        return self.object_values.get(key, [])

    def post(self, path: str, data: Mapping[str, Any]) -> Any:
        self.posts.append((path, data))
        return {}

    def patch(self, path: str, data: Mapping[str, Any]) -> Any:
        self.patches.append((path, data))
        return {}

    def workflow_dispatch(
        self, repo: str, workflow: str, ref: str, inputs: Mapping[str, str]
    ) -> None:
        self.dispatches.append((repo, workflow, ref, inputs))


def world(source: Any | None = None, automation: Any | None = None, agent: Any | None = None) -> release._World:
    return release._World(
        policy(),
        source or API(),
        automation or API(),
        agent or API(),
        agent_repo="valkey-io/valkey-ci-agent",
        controller_app_id=APP,
        notes_app_id=NOTES_APP,
        release_app_id=RELEASE_APP,
    )


def state(*, stage: str = "ga", branch: str = "9.1") -> dict[str, Any]:
    token = identity(tag="9.1.0" if stage == "ga" else "9.1.0-rc1")
    fields = release._parse_identity(token)
    return {
        "schema": 1,
        "release_id": fields["release_id"],
        "source_run_id": 7,
        "owner": "maintainer",
        "branch": branch,
        "intent": "ga" if stage == "ga" else "rc",
        "urgency": "HIGH",
        "version": "9.1.0",
        "stage": stage,
        "tag": fields["tag"],
        "status": "Ready to publish",
        "source_sha": SHA,
        "identity": token,
        "plan": {
            **fields,
            "schema": 1,
            "make_latest": True,
            "notes_sha256": release._sha256("Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"),
            "outputs": [],
            "packages": package_targets() if stage == "ga" else {"rpm": [], "deb": []},
        },
        "events": [],
        "checks": {"summary": "passed"},
        "qualification": {
            "summary": "passed",
            "run_id": 44,
            "manifest_sha256": "e" * 64,
        },
        "outputs": {},
        "alerts": [],
        "dispatches": {},
    }


def _merged_notes() -> dict[str, Any]:
    return {"number": 2, "html_url": "https://pr", "merged_at": "now", "merge_commit_sha": SHA}


def trusted_comment(body: str, app: int = RELEASE_APP, comment_id: int = 9) -> dict[str, Any]:
    return {
        "id": comment_id,
        "body": body,
        "performed_via_github_app": {"id": app},
        "user": {"id": 777, "type": "Bot"},
    }


@pytest.mark.parametrize(
    ("branch", "intent", "tags", "expected"),
    [
        ("9.1", "rc", [], ("9.1.0", "rc1")),
        ("9.1", "rc", ["9.1.0-rc1", "9.1.0-rc3"], ("9.1.0", "rc4")),
        ("9.1", "ga", ["9.1.0-rc2"], ("9.1.0", "ga")),
        ("9.1", "patch", ["9.1.0", "9.1.9", "9.1.10"], ("9.1.11", "ga")),
    ],
)
def test_release_version_rules(
    branch: str, intent: str, tags: list[str], expected: tuple[str, str]
) -> None:
    assert release._derive(branch, intent, tags) == expected


@pytest.mark.parametrize(
    ("intent", "tags", "message"),
    [
        ("ga", [], "prior release candidate"),
        ("rc", ["9.1.1"], "stable release"),
        ("ga", ["9.1.1", "9.1.0-rc1"], "stable release"),
        ("patch", ["9.1.0-rc1"], "no stable release"),
    ],
)
def test_release_version_refuses_invalid_progression(
    intent: str, tags: list[str], message: str
) -> None:
    with pytest.raises(release._Error, match=message):
        release._derive("9.1", intent, tags)


@pytest.mark.parametrize("value", ["09.1", "9.01", "9.1.00", "9.1.0-rc01"])
def test_release_versions_are_canonical(value: str) -> None:
    with pytest.raises(release._Error):
        release._tag_parts(value) if value.count(".") == 2 else release._derive(value, "rc", [])


def test_identity_is_one_strict_immutable_shape() -> None:
    token = identity()
    assert release._parse_identity(token) == {
        "release_id": "r0123456789abcdef0123",
        "tag": "9.1.0",
        "source_sha": SHA,
        "qualification_nonce": NONCE,
        "automation_sha": AUTOMATION_SHA,
        "plan_digest": DIGEST,
    }
    for bad in (
        token.replace("v1|", "v2|"),
        token.replace(SHA, "x" * 40),
        token.replace(NONCE, "0"),
        token.replace(DIGEST, "f" * 63),
        token + "|extra",
    ):
        with pytest.raises(release._Error):
            release._parse_identity(bad)


def test_release_type_checklists_are_exact() -> None:
    ga = state()
    rc = state(stage="rc")
    old_rc = state(stage="rc", branch="8.0")
    assert set(release._expected(ga)) == set(release._OUTPUTS)
    assert {
        "packages",
        "download-page",
        "documentation",
        "try-valkey",
        "helm-chart",
    }.isdisjoint(release._expected(rc))
    assert "bundle-images" in release._expected(rc)
    assert "bundle-images" not in release._expected(old_rc)


def test_state_accepts_exactly_one_controller_app_comment() -> None:
    item = trusted_comment(release._comment_payload("state", state()), APP)
    parsed, comment_id = release._state_from_comments([item], APP)
    assert parsed["release_id"] == state()["release_id"]
    assert comment_id == item["id"]
    with pytest.raises(release._Error, match="found 0"):
        release._state_from_comments([{**item, "performed_via_github_app": None}], APP)
    with pytest.raises(release._Error, match="found 2"):
        release._state_from_comments([item, {**item, "id": 10}], APP)
    source = API()
    source.page_values = [{"number": 1}]
    assert list(release._issue_states(world(source=source))) == [({"number": 1}, None, 0)]


def test_malformed_tracker_issue_fails_closed_without_blocking_other_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad = {"number": 1, "state": "open", "html_url": "https://example/1"}
    reconciled: list[int] = []

    def reconcile_one(_world: Any, issue: Mapping[str, Any], *_a: Any) -> None:
        if issue["number"] == 2:
            raise RuntimeError("boom")
        reconciled.append(int(issue["number"]))

    def broken_save(*_a: Any) -> None:
        raise RuntimeError("comment gone")

    monkeypatch.setattr(
        release,
        "_issue_states",
        lambda *_a, **_k: iter([(bad, None, 0), ({"number": 2}, state(), 3), ({"number": 4}, state(), 4)]),
    )
    monkeypatch.setattr(release, "_reconcile_one", reconcile_one)
    monkeypatch.setattr(release, "_notify", lambda *_: None)
    monkeypatch.setattr(release, "_save", broken_save)
    with pytest.raises(release._Error) as failure:
        release._reconcile(world(), argparse.Namespace(branch=""))
    message = str(failure.value)
    assert "#1: no single trusted controller state" in message
    assert "#2: boom" in message
    assert "#2: failure record not saved: comment gone" in message
    assert reconciled == [4]
    monkeypatch.setattr(release, "_authorize_start", lambda *_: "alice")
    with pytest.raises(release._Error, match="no single trusted controller state"):
        release._start(
            world(),
            argparse.Namespace(
                branch="9.1", intent="rc", urgency="HIGH", source_run="13", dry_run=True
            ),
        )


def test_start_authorizes_the_live_source_run_actor(monkeypatch: pytest.MonkeyPatch) -> None:
    source = API()
    source.gets = {
        "/repos/valkey-io/valkey": {"default_branch": "unstable"},
        "/repos/valkey-io/valkey/actions/runs/77": {
            "event": "workflow_dispatch",
            "head_branch": "unstable",
            "head_sha": SHA,
            "path": ".github/workflows/release-start.yml@refs/heads/unstable",
            "head_repository": {"full_name": "valkey-io/valkey"},
            "triggering_actor": {"login": "alice"},
        },
        "/repos/valkey-io/valkey/branches/unstable": {"commit": {"sha": SHA}},
    }
    source.maybes["/orgs/valkey-io/teams/core-team/memberships/alice"] = {
        "state": "active"
    }
    monkeypatch.setenv("START_RELAY_BOT_ID", "900")
    args = argparse.Namespace(
        source_run="77",
        event_sender="900",
    )
    audited: list[Any] = []
    monkeypatch.setattr(
        release, "_environment_policy", lambda *args, **kwargs: audited.append((args[2], kwargs["name"]))
    )
    assert release._authorize_start(world(source=source), args) == "alice"
    assert audited == [([("unstable", "branch")], "release-start")]
    args.event_sender = "901"
    with pytest.raises(release._Error, match="relay App"):
        release._authorize_start(world(source=source), args)
    args.event_sender = "900"
    source.gets["/repos/valkey-io/valkey/actions/runs/77"]["head_branch"] = "side"
    with pytest.raises(release._Error, match="current default-branch"):
        release._authorize_start(world(source=source), args)


def test_duplicate_active_branch_and_source_run_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = state()
    existing["source_run_id"] = 12
    issue = {"number": 3, "state": "open", "html_url": "https://example/3"}
    monkeypatch.setattr(release, "_authorize_start", lambda *_: "alice")
    monkeypatch.setattr(release, "_issue_states", lambda *_a, **_k: iter([(issue, existing, 4)]))
    monkeypatch.setattr(release, "_repo_tags", lambda *_: [])
    monkeypatch.setattr(release, "_save", lambda *_: None)
    args = argparse.Namespace(
        branch="9.1",
        intent="rc",
        urgency="HIGH",
        source_run="13",
        dry_run=True,
    )
    with pytest.raises(release._Error, match="already has active issue"):
        release._start(world(), args)
    args.source_run = "12"
    with pytest.raises(release._Error, match="already consumed"):
        release._start(world(), args)


def test_different_release_branches_can_start_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = state(branch="8.1")
    issue = {"number": 3, "state": "open", "html_url": "https://example/3"}
    monkeypatch.setattr(release, "_authorize_start", lambda *_: "alice")
    monkeypatch.setattr(
        release, "_issue_states", lambda *_a, **_k: iter([(issue, existing, 4)])
    )
    monkeypatch.setattr(release, "_repo_tags", lambda *_: [])
    release._start(
        world(),
        argparse.Namespace(
            branch="9.1",
            intent="rc",
            urgency="HIGH",
            source_run="13",
            dry_run=True,
        ),
    )


def test_start_opens_the_durable_issue_and_trusted_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = API()

    def post(path: str, data: Mapping[str, Any]) -> Mapping[str, Any]:
        source.posts.append((path, data))
        if path.endswith("/issues"):
            return {"number": 4, "html_url": "https://example/issues/4"}
        return trusted_comment(str(data["body"]), APP, comment_id=5)

    source.post = post  # type: ignore[method-assign]
    source.gets["/repos/valkey-io/valkey/branches/9.1"] = {"commit": {"sha": SHA}}
    agent = API()
    agent.gets["/repos/valkey-io/valkey-ci-agent"] = {"default_branch": "main"}
    monkeypatch.setattr(release, "_authorize_start", lambda *_: "alice")
    monkeypatch.setattr(release, "_issue_states", lambda *_a, **_k: iter(()))
    monkeypatch.setattr(release, "_repo_tags", lambda *_: [])
    monkeypatch.setattr(release, "_ensure_label", lambda *_: None)
    release._start(
        world(source=source, agent=agent),
        argparse.Namespace(
            branch="9.1",
            intent="rc",
            urgency="HIGH",
            source_run="13",
            dry_run=False,
        ),
    )
    assert source.posts[0][1]["title"] == "Release 9.1.0-rc1"
    assert release._STATE in str(source.posts[1][1]["body"])
    assert agent.dispatches[0][1:] == (
        "release-notes-cut.yml",
        "main",
        {"version": "9.1.0", "stage": "rc1", "urgency": "HIGH", "dry_run": "false"},
    )
    assert any("notes:" + SHA in str(data) for _, data in source.patches)


def test_exact_commit_ci_requires_every_job_and_required_check() -> None:
    source = API()
    source.gets["/repos/valkey-io/valkey/actions/workflows/ci.yml/runs?branch=9.1&event=push&head_sha=" + SHA] = {
        "workflow_runs": [{
            "id": 5, "head_sha": SHA, "head_branch": "9.1", "event": "push", "status": "completed", "conclusion": "success",
            "created_at": "2026-01-01", "html_url": "https://example/ci",
        }]}
    check = {"id": 1, "name": "required", "status": "completed", "conclusion": "success", "app": {"id": 7}}
    source.object_values = {
        "jobs": [
            {"name": "required", "status": "completed", "conclusion": "success"},
            {"name": "compat", "status": "completed", "conclusion": "success"},
        ],
        "check_runs": [check],
    }
    source.maybes["/repos/valkey-io/valkey/branches/9.1/protection/required_status_checks"] = {"checks": [{"context": "required", "app_id": 7}]}
    assert release._ci_status(world(source=source), state(), SHA)[0]
    source.object_values["check_runs"].append({**check, "id": 2, "conclusion": "failure", "app": {"id": 8}})
    assert release._ci_status(world(source=source), state(), SHA)[0]
    check["app"]["id"] = 8
    assert not release._ci_status(world(source=source), state(), SHA)[0]
    check["app"]["id"], source.object_values["jobs"][0]["name"] = 7, "unit"
    assert not release._ci_status(world(source=source), state(), SHA)[0]
    source.object_values["jobs"][0]["name"] = "required"
    source.object_values["jobs"][1]["conclusion"] = "failure"
    assert not release._ci_status(world(source=source), state(), SHA)[0]
    source.object_values["jobs"][1]["conclusion"] = "success"
    source.object_values["check_runs"] = [
        {"id": 1, "name": "required", "status": "completed", "conclusion": "success"},
        {"id": 2, "name": "required", "status": "completed", "conclusion": "failure"},
    ]
    ok, result = release._ci_status(world(source=source), state(), SHA)
    assert not ok and "required checks missing/red" in result["summary"]


def test_plan_binds_candidate_notes_automation_outputs_and_latest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release, "current_release_state", lambda _text: ("9.1.0", "ga")
    )
    monkeypatch.setattr(
        release,
        "_file",
        lambda _client, _repo, path, _ref: (
            (AUTO / path).read_text()
            if path == ".github/package-platforms.json"
            else "version"
            if path == release._VERSION_FILE
            else "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"
        ),
    )
    monkeypatch.setattr(release, "_latest", lambda *_: True)
    value = release._plan(world(), state(), SHA, AUTOMATION_SHA)
    assert value["source_sha"] == SHA
    assert value["automation_sha"] == AUTOMATION_SHA
    assert value["notes_sha256"] == release._sha256("Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n")
    assert value["outputs"] == release._expected(state())
    assert value["packages"] == package_targets()
    digest_input = {key: item for key, item in value.items() if key != "plan_digest"}
    assert value["plan_digest"] == release._sha256(release._json(digest_input))
    with pytest.raises(release._Error, match="urgency"):
        release._plan(world(), {**state(), "urgency": "LOW"}, SHA, AUTOMATION_SHA)


def test_notes_pr_uses_issue_app_attribution_not_an_unavailable_pull_field() -> None:
    source = API()
    source.page_values = [
        {
            "number": 12,
            "base": {"ref": "9.1"},
            "head": {
                "ref": "agent/release-cut/9.1.0-ga",
                "repo": {"full_name": "valkey-io/valkey"},
            },
        }
    ]
    source.gets["/repos/valkey-io/valkey/issues/12"] = {
        "performed_via_github_app": {"id": NOTES_APP}
    }
    assert release._notes_pr(world(source=source), state())["number"] == 12
    source.gets["/repos/valkey-io/valkey/issues/12"][
        "performed_via_github_app"
    ] = {"id": APP}
    assert release._notes_pr(world(source=source), state()) is None


@pytest.mark.parametrize("stage", ["ga", "rc"])
def test_qualification_manifest_matches_real_stage_contract(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    current = state(stage=stage)
    run = {
        "id": 8,
        "status": "completed",
        "conclusion": "success",
        "html_url": "https://example/qual",
    }
    manifest = {
        "schema": 1,
        "kind": "qualification",
        "identity": current["identity"],
        "result": "passed",
        "coverage": {
            "archives": sorted(policy()["downstream"]["tarball_targets"]),
            **current["plan"]["packages"],
        },
    }
    monkeypatch.setattr(release, "_run", lambda *_: run)
    monkeypatch.setattr(release, "_signed_artifact", lambda *_: manifest)
    result, evidence = release._qualification(world(), current)
    assert result == "passed"
    assert evidence["run_id"] == 8
    if stage == "ga":
        manifest["coverage"]["rpm"] = 30
    else:
        manifest["identity"] = identity(qualification_nonce="f" * 32)
    with pytest.raises(release._Error, match="mismatched manifest"):
        release._qualification(world(), current)


def test_github_client_authenticates_every_request(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[urllib.request.Request] = []

    def urlopen(request: urllib.request.Request, timeout: int) -> Any:
        requests.append(request)
        return contextlib.nullcontext(SimpleNamespace(read=lambda _n: b"{}", headers={}))

    monkeypatch.setattr(release.urllib.request, "urlopen", urlopen)
    assert release._GitHub("read-token").get("/repos/valkey-io/valkey") == {}
    assert requests[0].get_header("Authorization") == "Bearer read-token"


def test_artifact_redirect_drops_auth_and_refuses_downgrade() -> None:
    handler = release._HttpsRedirect()
    original = urllib.request.Request(
        "https://api.github.com/artifact",
        headers={"Authorization": "Bearer secret"},
    )
    redirected = handler.redirect_request(
        original, None, 302, "Found", {}, "https://objects.example/artifact"
    )
    assert redirected is not None
    assert "Authorization" not in redirected.headers
    with pytest.raises(release._Error, match="leave HTTPS"):
        handler.redirect_request(
            original, None, 302, "Found", {}, "http://objects.example/artifact"
        )


def test_signed_artifact_never_forwards_api_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("manifest.json", '{"schema":1}')
    requests: list[urllib.request.Request] = []

    class RedirectOpener:
        def open(self, request: urllib.request.Request, timeout: int) -> Any:
            requests.append(request)
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://objects.example/artifact"},
                None,
            )

    class SignedOpener:
        def open(self, request: urllib.request.Request, timeout: int) -> io.BytesIO:
            requests.append(request)
            return io.BytesIO(payload.getvalue())

    openers = iter([RedirectOpener(), SignedOpener()])
    monkeypatch.setattr(release, "_artifact", lambda *_: {"archive_download_url": "https://api.github.com/a"})
    monkeypatch.setattr(
        release.urllib.request, "build_opener", lambda *_: next(openers)
    )
    assert release._signed_artifact(world(automation=API()), 4, "manifest") == {"schema": 1}
    assert requests[0].get_header("Authorization") == "Bearer token"
    assert requests[1].get_header("Authorization") is None


def test_signed_artifact_enforces_download_size_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RedirectOpener:
        def open(self, request: urllib.request.Request, timeout: int) -> Any:
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://objects.example/artifact"},
                None,
            )

    class SignedOpener:
        def open(self, _request: Any, timeout: int) -> io.BytesIO:
            return io.BytesIO(b"x" * (release._MAX_DOWNLOAD + 1))

    openers = iter([RedirectOpener(), SignedOpener()])
    monkeypatch.setattr(
        release,
        "_artifact",
        lambda *_: {"archive_download_url": "https://api.github.com/a"},
    )
    monkeypatch.setattr(
        release.urllib.request, "build_opener", lambda *_: next(openers)
    )
    with pytest.raises(release._Error, match="exceeds"):
        release._signed_artifact(world(automation=API()), 4, "manifest")


def ruleset(
    rule_types: list[str], bypass: list[dict[str, Any]], ruleset_id: int
) -> dict[str, Any]:
    return {
        "id": ruleset_id,
        "target": "tag",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["refs/tags/*"], "exclude": []}},
        "rules": [{"type": item} for item in rule_types],
        "bypass_actors": bypass,
    }


def test_tag_rules_require_exclusive_creation_and_no_bypass_immutability() -> None:
    client = API()
    client.page_values = [
        {"id": 1, "target": "tag", "enforcement": "active"},
        {"id": 2, "target": "tag", "enforcement": "active"},
    ]
    client.gets["/repos/o/r/rulesets/1"] = ruleset(
        ["creation"],
        [
            {
                "actor_type": "Integration",
                "actor_id": RELEASE_APP,
                "bypass_mode": "always",
            }
        ],
        1,
    )
    client.gets["/repos/o/r/rulesets/2"] = ruleset(
        ["update", "deletion"], [], 2
    )
    assert release._matching_rulesets(client, "o/r", "release-automation/r1") == []
    release._tag_rules(client, "o/r", "9.1.0", RELEASE_APP)
    for actor_type in ("Team", "RepositoryRole", "OrganizationAdmin", "User"):
        client.gets["/repos/o/r/rulesets/2"]["bypass_actors"] = [
            {"actor_type": actor_type, "actor_id": 1, "bypass_mode": "always"}
        ]
        with pytest.raises(release._Error, match="no-bypass"):
            release._tag_rules(client, "o/r", "9.1.0", RELEASE_APP)
    client.gets["/repos/o/r/rulesets/2"]["bypass_actors"] = []
    client.gets["/repos/o/r/rulesets/1"]["rules"] = []
    with pytest.raises(release._Error, match="restricts creation"):
        release._tag_rules(client, "o/r", "9.1.0", RELEASE_APP)


@pytest.mark.parametrize(
    ("policies", "reviewer"),
    [
        ([("main", "branch")], "core-team"),
        ([("release-automation/*", "tag")], ""),
    ],
)
def test_release_environment_admits_only_exact_trusted_refs(
    policies: list[tuple[str, str]], reviewer: str
) -> None:
    client = API()
    rules = (
        [
            {
                "type": "required_reviewers",
                "reviewers": [{"type": "Team", "reviewer": {"id": 1, "slug": reviewer}}],
                "prevent_self_review": True,
            }
        ]
        if reviewer
        else []
    )
    client.gets["/repos/o/r/environments/release"] = {
        "protection_rules": rules,
        "can_admins_bypass": False,
        "deployment_branch_policy": {
            "custom_branch_policies": True,
            "protected_branches": False,
        },
    }
    client.gets[
        "/repos/o/r/environments/release/deployment-branch-policies"
    ] = {
        "total_count": len(policies),
        "branch_policies": [
            {"name": name, "type": kind} for name, kind in policies
        ],
    }
    release._environment_policy(client, "o/r", policies, reviewer=reviewer)
    if reviewer:
        rules[0]["reviewers"][0]["reviewer"]["slug"] = "wrong-team"
        with pytest.raises(release._Error, match="incorrect"):
            release._environment_policy(client, "o/r", policies, reviewer=reviewer)
        rules[0]["reviewers"][0]["reviewer"]["slug"] = reviewer
    client.gets[
        "/repos/o/r/environments/release/deployment-branch-policies"
    ]["branch_policies"].append({"name": "*", "type": "branch"})
    client.gets[
        "/repos/o/r/environments/release/deployment-branch-policies"
    ]["total_count"] += 1
    with pytest.raises(release._Error, match="incorrect"):
        release._environment_policy(client, "o/r", policies, reviewer=reviewer)


def _prewrite_receipt(current: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": 1,
        "identity": current["identity"],
        "source_sha": SHA,
        "plan_digest": DIGEST,
        "qualification_run_id": 44,
        "qualification_manifest_sha256": "e" * 64,
        "controller_sha": "f" * 40,
        "run_id": "80",
        "approver": "alice",
        "recorded_at": "2026-01-01T00:00:00+00:00",
    }


def publication_receipt(current: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": 1,
        "identity": current["identity"],
        "source_sha": SHA,
        "plan_digest": DIGEST,
        "release_id": 55,
        "release_url": "https://example/release",
        "release_author_id": 777,
        "controller_actor_id": 500,
        "make_latest": True,
        "controller_sha": "f" * 40,
        "run_id": "80",
        "approver": "alice",
        "recorded_at": "2026-01-01T00:00:00+00:00",
    }


def test_published_state_requires_trusted_field_bound_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state()
    payload = publication_receipt(current)
    prewrite = trusted_comment(
        release._comment_payload("prewrite", _prewrite_receipt(current)), comment_id=8
    )
    comment = trusted_comment(release._comment_payload("publication", payload))
    source = API()
    source.maybes[
        "/repos/valkey-io/valkey/releases/tags/9.1.0"
    ] = {
        "id": 55,
        "html_url": "https://example/release",
        "draft": False,
        "prerelease": False,
        "body": f"notes\n\n<!-- valkey-release-identity:v1 {current['identity']} -->\n",
        "author": {"id": 777},
    }
    monkeypatch.setattr(release, "_tag_sha", lambda *_: SHA)
    comments = [prewrite, comment]
    assert release._published(world(source=source), current, comments)["receipt_id"] == 9
    comment["performed_via_github_app"]["id"] = APP
    assert release._published(world(source=source), current, comments) is None
    comment["performed_via_github_app"]["id"] = RELEASE_APP
    payload["source_sha"] = "f" * 40
    comment["body"] = release._comment_payload("publication", payload)
    with pytest.raises(release._Error, match="does not match"):
        release._published(world(source=source), current, comments)
    payload["source_sha"] = SHA
    payload["approver"] = "mallory"
    comment["body"] = release._comment_payload("publication", payload)
    with pytest.raises(release._Error, match="does not match"):
        release._published(world(source=source), current, comments)
    payload["approver"] = "alice"
    payload["make_latest"] = False
    comment["body"] = release._comment_payload("publication", payload)
    with pytest.raises(release._Error, match="does not match"):
        release._published(world(source=source), current, comments)


def test_prewrite_binds_live_qualification_and_plan() -> None:
    current = state()
    payload = _prewrite_receipt(current)
    comment = trusted_comment(release._comment_payload("prewrite", payload))
    assert release._prewrite([comment], current, RELEASE_APP) is not None
    payload["qualification_run_id"] = 45
    comment["body"] = release._comment_payload("prewrite", payload)
    with pytest.raises(release._Error, match="malformed or stale"):
        release._prewrite([comment], current, RELEASE_APP)


class PublishAPI(API):
    def __init__(self, operations: list[str], *, automation: bool = False) -> None:
        super().__init__()
        self.operations = operations
        self.automation = automation
        self.comments: list[dict[str, Any]] = []
        self.tag = ""
        self.release_value: dict[str, Any] | None = None

    def maybe(self, path: str) -> Any:
        if path.endswith("/immutable-releases"):
            return {"enabled": True}
        if "/releases/tags/" in path:
            return self.release_value
        return None

    def post(self, path: str, data: Mapping[str, Any]) -> Any:
        if path.endswith("/comments"):
            kind = "prewrite" if "prewrite" in str(data["body"]) else "publication"
            self.operations.append(kind)
            item = trusted_comment(str(data["body"]), comment_id=10 + len(self.comments))
            self.comments.append(item)
            return item
        if path.endswith("/git/refs"):
            self.operations.append("automation-ref" if self.automation else "source-ref")
            self.tag = str(data["sha"])
            return {}
        if path.endswith("/releases"):
            self.operations.append("release")
            self.release_value = {
                **data,
                "id": 55,
                "html_url": "https://example/release",
                "author": {"id": 777},
            }
            return self.release_value
        raise AssertionError(path)

    def workflow_dispatch(
        self, repo: str, workflow: str, ref: str, inputs: Mapping[str, str]
    ) -> None:
        self.operations.append("dispatch")
        super().workflow_dispatch(repo, workflow, ref, inputs)


def configure_publish(
    monkeypatch: pytest.MonkeyPatch,
    source: PublishAPI,
    automation: PublishAPI,
    current: dict[str, Any],
    resume_values: list[bool],
) -> release._World:
    state_comment = trusted_comment(release._comment_payload("state", current), APP, comment_id=1)
    state_comment["user"]["id"] = 500
    source.comments.append(state_comment)
    monkeypatch.setattr(release, "_comments", lambda *_: source.comments)
    monkeypatch.setattr(release, "_state_from_comments", lambda *_: (current, 1))
    monkeypatch.setattr(release, "_environment", lambda *_: None)
    monkeypatch.setattr(release, "_tag_rules", lambda *_: None)
    monkeypatch.setattr(release, "_team_member", lambda *_: None)
    monkeypatch.setattr(release, "_latest_ok", lambda *_: True)

    def revalidate(
        _world: Any, _issue: int, _identity: str, *, resume: bool = False
    ) -> tuple[dict[str, Any], str]:
        resume_values.append(resume)
        return current, "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"

    monkeypatch.setattr(release, "_revalidate", revalidate)
    monkeypatch.setattr(
        release,
        "_tag_sha",
        lambda client, _repo, _tag: client.tag,
    )
    return world(source=source, automation=automation)


def test_publish_orders_prewrite_immutable_refs_release_receipt_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations: list[str] = []
    source = PublishAPI(operations)
    automation = PublishAPI(operations, automation=True)
    current = state()
    resumes: list[bool] = []
    target = configure_publish(monkeypatch, source, automation, current, resumes)
    monkeypatch.setenv("GITHUB_SHA", "f" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "90")
    args = argparse.Namespace(
        identity=current["identity"],
        issue=3,
        execute=True,
        approver="alice",
    )
    release._publish(target, args)
    assert resumes == [False]
    assert operations == [
        "prewrite",
        "automation-ref",
        "source-ref",
        "release",
        "publication",
        "dispatch",
    ]
    assert current["identity"] in str(source.release_value["body"])
    publication = release._parse_block(source.comments[-1]["body"], release._RECEIPT.format(kind="publication"))
    assert (publication["controller_actor_id"], publication["release_author_id"],
            publication["controller_sha"], publication["run_id"]) == (500, 777, "f" * 40, "90")
    assert automation.dispatches[0][2] == release._automation_ref(
        current["identity"]
    )
    assert automation.dispatches[0][3] == {
        "identity": current["identity"],
        "record": "valkey-io/valkey#3",
        "receipt": "12",
        "stage": "base",
    }


def test_same_sha_tag_requires_prewrite_and_recovery_revalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations: list[str] = []
    source = PublishAPI(operations)
    automation = PublishAPI(operations, automation=True)
    current = state()
    source.tag = SHA
    resumes: list[bool] = []
    target = configure_publish(monkeypatch, source, automation, current, resumes)
    args = argparse.Namespace(
        identity=current["identity"],
        issue=3,
        execute=False,
    )
    with pytest.raises(release._Error, match="external state is not authorization"):
        release._publish(target, args)
    payload = _prewrite_receipt(current)
    source.comments.append(trusted_comment(release._comment_payload("prewrite", payload)))
    source.tag = ""
    release._publish(target, args)
    source.tag = SHA
    release._publish(target, args)
    assert resumes == [False, False, True]
    monkeypatch.setenv("GITHUB_SHA", "e" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "90")
    args.execute = True
    args.approver = "bob"
    release._publish(target, args)
    receipt = release._parse_block(
        source.comments[-1]["body"], release._RECEIPT.format(kind="publication")
    )
    assert (receipt["controller_sha"], receipt["run_id"], receipt["approver"]) == (
        payload["controller_sha"], payload["run_id"], payload["approver"]
    )
    source.comments[:] = source.comments[:1]
    source.tag, source.release_value = "", None
    operations.clear()

    def concurrent_prewrite(*_a: Any, **_k: Any) -> tuple[dict[str, Any], str]:
        source.comments.append(trusted_comment(release._comment_payload("prewrite", payload)))
        source.tag = SHA
        return current, "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"

    monkeypatch.setattr(release, "_revalidate", concurrent_prewrite)
    release._publish(target, args)
    assert "prewrite" not in operations and "source-ref" not in operations


def revalidation_state() -> tuple[dict[str, Any], dict[str, Any]]:
    current = state()
    plan = {
        "schema": 1,
        "release_id": current["release_id"],
        "tag": current["tag"],
        "source_sha": SHA,
        "qualification_nonce": NONCE,
        "automation_sha": AUTOMATION_SHA,
        "make_latest": True,
        "notes_sha256": release._sha256("Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"),
        "outputs": release._expected(current),
    }
    plan["plan_digest"] = release._sha256(release._json(plan))
    current["identity"] = identity(plan_digest=plan["plan_digest"])
    current["plan"] = plan
    return current, plan


@pytest.mark.parametrize(
    ("ci_ok", "qualification"), [(False, "passed"), (True, "failed"), (True, "passed")]
)
def test_same_sha_recovery_still_requires_ci_and_qualification(
    monkeypatch: pytest.MonkeyPatch, ci_ok: bool, qualification: str
) -> None:
    current, plan = revalidation_state()
    source = API()
    source.gets["/repos/valkey-io/valkey/issues/3"] = {"state": "open"}
    monkeypatch.setattr(release, "_comments", lambda *_: [])
    monkeypatch.setattr(release, "_state_from_comments", lambda *_: (current, 1))
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(
        release, "_ci_status", lambda *_: (ci_ok, {"summary": "live"})
    )
    monkeypatch.setattr(release, "_plan", lambda *_: dict(plan))
    monkeypatch.setattr(
        release, "_qualification", lambda *_: (qualification, {})
    )
    monkeypatch.setattr(release, "_file", lambda *_: "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n")
    with pytest.raises(release._Error):
        release._revalidate(
            world(source=source), 3, current["identity"], resume=True
        )


def test_normal_revalidation_repeats_branch_ci_plan_and_notes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current, plan = revalidation_state()
    source = API()
    source.gets["/repos/valkey-io/valkey/issues/3"] = {"state": "open"}
    monkeypatch.setattr(release, "_comments", lambda *_: [])
    monkeypatch.setattr(release, "_state_from_comments", lambda *_: (current, 1))
    monkeypatch.setattr(release, "_branch_head", lambda *_: SHA)
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(release, "_ci_status", lambda *_: (True, {}))
    monkeypatch.setattr(release, "_automation_head", lambda *_: AUTOMATION_SHA)
    monkeypatch.setattr(release, "_plan", lambda *_: dict(plan))
    monkeypatch.setattr(
        release,
        "_qualification",
        lambda *_: ("passed", {"manifest_sha256": "e" * 64}),
    )
    monkeypatch.setattr(
        release, "_file", lambda *_: "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"
    )
    validated, notes = release._revalidate(
        world(source=source), 3, current["identity"]
    )
    assert validated is current
    assert notes == "Valkey 9.1.0\n\nUpgrade urgency HIGH: notes\n"


def test_published_release_survives_later_branch_advance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state(stage="rc", branch="8.0")
    source = API()
    saved: list[str] = []
    monkeypatch.setattr(release, "_branch_head", lambda *_: pytest.fail("published release resolved branch head"))
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(
        release,
        "_published",
        lambda *_: {
            "release_url": "https://example/release",
            "receipt_id": 9,
        },
    )
    monkeypatch.setattr(
        release,
        "_run",
        lambda *_: {
            "id": 5,
            "status": "in_progress",
            "html_url": "https://example/run",
        },
    )
    monkeypatch.setattr(
        release, "_save", lambda _w, _i, _c, value: saved.append(value["status"])
    )
    release._reconcile_one(
        world(source=source),
        {"number": 1},
        current,
        2,
    )
    assert current["identity"]
    assert saved == ["Published; verifying downstream outputs"]


def test_branch_advance_partial_recovery_and_failed_qualification_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state()
    actions: list[str] = []
    monkeypatch.setattr(release, "_branch_head", lambda *_: SHA)
    monkeypatch.setattr(release, "_notes_pr", lambda *_: None)
    monkeypatch.setattr(release, "_dispatch_once", lambda _w, _s, key, *_a, **_k: actions.append(key))
    monkeypatch.setattr(release, "_save", lambda _w, _i, _c, value: actions.append(f"save:{value['status']}"))
    release._reconcile_one(world(), {"number": 1}, current, 2)
    assert actions == [f"notes:{SHA}", "save:Waiting for release-notes PR merge"]
    actions.clear()
    monkeypatch.setattr(release, "_branch_head", lambda *_: "f" * 40)
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(release, "_published", lambda *_: None)
    release._reconcile_one(world(), {"number": 1}, current, 2)
    assert current["identity"] == ""
    assert actions == ["notes:" + "f" * 40, "save:Candidate invalidated; refreshing release notes"]
    sealed = state()
    monkeypatch.setattr(release, "_branch_head", lambda *_: SHA)
    monkeypatch.setattr(release, "_ci_status", lambda *_: (True, {}))
    monkeypatch.setattr(release, "_seal", lambda _w, value, sha: value.update(
        identity=sealed["identity"], source_sha=sha, plan=sealed["plan"]
    ))
    monkeypatch.setattr(release, "_qualification", lambda *_: ("missing", {}))
    actions.clear()
    release._reconcile_one(world(), {"number": 1}, current, 2)
    assert actions[:2] == ["save:Candidate invalidated; refreshing release notes", f"qualify:{current['identity']}"]
    partial = state()
    prewrite = trusted_comment(release._comment_payload("prewrite", _prewrite_receipt(partial)))
    monkeypatch.setattr(release, "_comments", lambda *_: [prewrite])
    monkeypatch.setattr(release, "_tag_sha", lambda *_: SHA)
    monkeypatch.setattr(release, "_plan", lambda *_: {"plan_digest": DIGEST})
    monkeypatch.setattr(release, "_qualification", lambda *_: ("passed", partial["qualification"]))
    release._reconcile_one(world(), {"number": 1}, partial, 2)
    assert partial["identity"]
    assert actions[-3:] == ["save:Ready to publish", f"publish:{partial['identity']}", "save:Ready to publish"]
    before = len(actions)
    partial["dispatches"][f"publish:{partial['identity']}"] = {}
    release._reconcile_one(world(), {"number": 1}, partial, 2)
    assert f"publish:{partial['identity']}" in actions[before:]
    qualify_key = f"qualify:{partial['identity']}"
    partial["dispatches"][qualify_key] = {}
    monkeypatch.setattr(release, "_qualification", lambda *_: ("failed", {"summary": "failed"}))
    release._reconcile_one(world(), {"number": 1}, partial, 2)
    assert qualify_key not in partial["dispatches"] and partial["status"] == "Qualification failed"
    assert actions[-2:] == [qualify_key, "save:Qualification failed"] and partial["alerts"] == ["failed"]
    partial["qualification"] = state()["qualification"]
    monkeypatch.setattr(release, "_qualification", lambda *_: ("pending", {"summary": "running"}))
    before = len(actions)
    release._reconcile_one(world(), {"number": 1}, partial, 2)
    assert partial["status"] == "Qualification running"
    assert actions[before:] == ["save:Qualification running"]
    wedged = state()
    monkeypatch.setattr(release, "_plan", lambda *_: {"plan_digest": "0" * 64})
    statuses: list[str] = []
    monkeypatch.setattr(release, "_save", lambda _w, _i, _c, value: statuses.append(value["status"]))
    release._reconcile_one(world(), {"number": 1}, wedged, 2)
    assert statuses == ["Blocked"]
    assert wedged["identity"] and wedged["alerts"]


def completion_files() -> list[dict[str, str]]:
    def item(kind: str, name: str, target: str, digest: str) -> dict[str, str]:
        return {
            "kind": kind,
            "name": name,
            "target": target,
            "url": f"https://download.example/{kind}/{name}",
            "sha256": digest * 64,
        }

    files = [
        item(kind, f"{target.replace('/', '-')}.{kind}", target, "1")
        for target in policy()["downstream"]["tarball_targets"]
        for kind in ("archive", "checksum")
    ]
    files += [
        item(kind, f"{kind}-{index}", target, "2")
        for kind, targets in package_targets().items()
        for index, target in enumerate(targets)
    ]
    files.append(item("source", "valkey-9.1.0.tar.gz", "source", "3"))
    files[-1]["url"] = "https://github.com/valkey/archive/9.1.0.tar.gz"
    return files


def test_completion_recomputes_digests_platforms_images_and_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state()
    files = completion_files()
    manifest = {
        "schema": 1,
        "kind": "completion",
        "stage": "base",
        "identity": current["identity"],
        "publication_receipt": "9",
        "result": "passed",
        "files": files,
        "try_valkey_urls": [
            "https://download.valkey.io/try-me-valkey/9.1.0/states/state.bin.gz",
            "https://download.valkey.io/try-me-valkey/9.1.0/fs/alpine-fs.json",
            "https://download.valkey.io/try-me-valkey/9.1.0/fs/alpine-rootfs-flat/bin/valkey",
        ],
        "download_page_url": "https://download.valkey.io/releases/",
    }
    registry_urls: list[str] = []
    downloads: list[tuple[Any, ...]] = []
    monkeypatch.setattr(release, "_signed_artifact", lambda *_: manifest)
    monkeypatch.setattr(
        release, "_download", lambda *args: downloads.append(args) or True
    )
    monkeypatch.setattr(
        release, "_registry", lambda url: registry_urls.append(url) or True
    )
    monkeypatch.setattr(release, "_page_contains", lambda *_: True)
    monkeypatch.setattr(release, "_pr_merged", lambda *_: (True, "https://pr"))
    source = files[-1]
    monkeypatch.setattr(
        release,
        "_file",
        lambda *_: f"hash {source['name']} sha256 {source['sha256']} {source['url']}\n",
    )
    monkeypatch.setattr(release, "_canonical_latest", lambda *_: "9.1.0")
    ok, outputs = release._verify_base(
        world(), current, {"id": 7}, "9"
    )
    assert ok
    assert outputs["packages"]["verified"]
    assert outputs["linux-archives"]["verified"]
    assert outputs["checksums"]["verified"]
    assert {args[2] for args in downloads if len(args) == 3 and args[2]} == {
        release._sha256(f"{item['sha256']}  {item['name']}\n")
        for item in files
        if item["kind"] == "archive"
    }
    assert all(outputs[name]["verified"] for name in ("docker-hub", "ghcr", "ecr"))
    assert any(url.endswith("/9.1.0-trixie") for url in registry_urls)
    assert any(url.endswith("/9.1.0-alpine") for url in registry_urls)
    for try_urls in (manifest["try_valkey_urls"][:2], manifest["try_valkey_urls"][:1] * 3, None):
        manifest["try_valkey_urls"] = try_urls
        assert not release._verify_base(world(), current, {"id": 7}, "9")[1]["try-valkey"]["verified"]
    manifest["try_valkey_urls"] = [
        "https://download.valkey.io/try-me-valkey/9.1.1/states/state.bin.gz",
        "https://download.valkey.io/try-me-valkey/9.1.1/fs/alpine-fs.json",
        "https://download.valkey.io/try-me-valkey/9.1.1/fs/alpine-rootfs-flat/bin/valkey",
    ]
    assert not release._verify_base(world(), current, {"id": 7}, "9")[1]["try-valkey"]["verified"]
    current.update(
        {"version": "9.1.1", "tag": "9.1.1", "identity": identity(tag="9.1.1")}
    )
    manifest["identity"] = current["identity"]
    monkeypatch.setattr(
        release, "_tag_sha", lambda _client, _repo, tag: SHA if tag == "9.1.0" else "f" * 40
    )
    assert not release._verify_base(world(), current, {"id": 7}, "9")[1]["documentation"]["verified"]
    monkeypatch.setattr(release, "_tag_sha", lambda *_: SHA)
    assert release._verify_base(world(), current, {"id": 7}, "9")[1]["documentation"]["verified"]
    next(item for item in files if item["kind"] == "rpm")["target"] = "rpm/unsupported/x86_64"
    assert not release._verify_base(world(), current, {"id": 7}, "9")[1]["packages"]["verified"]
    manifest["publication_receipt"] = "10"
    with pytest.raises(release._Error, match="does not match"):
        release._verify_base(world(), current, {"id": 7}, "9")


def test_dependent_completion_verifies_bundle_registries_and_helm_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state()
    manifest = {
        "schema": 1,
        "kind": "completion",
        "stage": "dependent",
        "identity": current["identity"],
        "publication_receipt": "9",
        "result": "passed",
        "bundle_line": "9.1",
        "helm_version": "2.0.0",
    }
    monkeypatch.setattr(release, "_signed_artifact", lambda *_: manifest)
    monkeypatch.setattr(release, "_registry", lambda *_: True)
    monkeypatch.setattr(
        release,
        "_file",
        lambda *_: json.dumps(
            {
                "9.1": {
                    "version": "bundle-9.1",
                    "valkey-server": {"version": "9.1.0"},
                }
            }
        ),
    )

    index = [
        b"entries:\n  valkey:\n    - version: 2.0.0\n      appVersion: 9.1.0\n"
    ]
    monkeypatch.setattr(release, "_https_get", lambda *_: index[0])
    ok, outputs = release._verify_dependent(
        world(), current, {"id": 8}, "9"
    )
    assert ok
    assert outputs["bundle-images"]["verified"]
    assert outputs["helm-chart"]["verified"]
    index[0] = (
        b"entries:\n  valkey-bundle:\n"
        b"    - version: 2.0.0\n      appVersion: 9.1.0\n"
    )
    assert not release._verify_dependent(world(), current, {"id": 8}, "9")[1][
        "helm-chart"
    ]["verified"]
    manifest["identity"] = identity(qualification_nonce="f" * 32)
    with pytest.raises(release._Error, match="does not match"):
        release._verify_dependent(world(), current, {"id": 8}, "9")


def test_digest_probe_hashes_public_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        release.urllib.request.OpenerDirector,
        "open",
        lambda *_a, **_k: io.BytesIO(b"approved bytes"),
    )
    digest = hashlib.sha256(b"approved bytes").hexdigest()
    assert release._download("https://download.example/file", digest)
    assert release._download("https://download.example/file", digest, digest)
    assert not release._download("https://download.example/file", digest, "0" * 64)
    assert not release._download("https://download.example/file", "0" * 64)
    assert not release._download("http://download.example/file", digest)


def test_public_page_probe_requires_https_content_and_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = [b"release 9.1.0 archive"]
    monkeypatch.setattr(
        release.urllib.request.OpenerDirector,
        "open",
        lambda self, *_a, **_k: io.BytesIO(body[0]),
    )
    assert release._page_contains(
        "https://valkey.io/download/", ["9.1.0", "archive"]
    )
    assert not release._page_contains("http://valkey.io/download/", ["9.1.0"])
    body[0] = b"x" * (release._MAX_DOWNLOAD + 1)
    assert not release._page_contains("https://valkey.io/download/", ["x"])


def test_registry_probe_fails_closed_on_https_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def open_(*_a: Any, **_k: Any) -> None:
        raise release._Error("artifact redirect attempted to leave HTTPS")

    monkeypatch.setattr(release.urllib.request.OpenerDirector, "open", open_)
    assert not release._registry("http://ghcr.io/v2/valkey-io/valkey/manifests/9.1.0")
    assert not release._registry("https://ghcr.io/v2/valkey-io/valkey/manifests/9.1.0")
    assert not release._download("https://download.valkey.io/file", "0" * 64)


def test_downstream_pr_must_change_and_merge_into_the_default_branch() -> None:
    client = API()
    query = "state=all&head=valkey-io%3Arelease-9.1.0"
    client.gets[f"/repos/valkey-io/valkey-hashes/pulls?{query}"] = [{"number": 4}]
    detail = {
        "number": 4,
        "merged_at": "now",
        "base": {"ref": "main"},
        "head": {"repo": {"full_name": "valkey-io/valkey-hashes"}},
        "changed_files": 1,
        "body": identity(),
        "html_url": "https://example/pr/4",
    }
    client.gets["/repos/valkey-io/valkey-hashes/pulls/4"] = detail
    client.gets["/repos/valkey-io/valkey-hashes"] = {"default_branch": "main"}
    assert release._pr_merged(client, "valkey-io/valkey-hashes", "release-9.1.0", identity())[0]
    detail["base"] = {"ref": "side"}
    assert not release._pr_merged(client, "valkey-io/valkey-hashes", "release-9.1.0", identity())[0]
    detail["base"] = {"ref": "main"}
    detail["changed_files"] = 0
    assert not release._pr_merged(client, "valkey-io/valkey-hashes", "release-9.1.0", identity())[0]
    detail["changed_files"] = 1
    detail["head"]["repo"]["full_name"] = "valkey-io/fork"
    assert not release._pr_merged(client, "valkey-io/valkey-hashes", "release-9.1.0", identity())[0]


def test_run_observer_prefers_completed_receipt_over_newer_suppressed_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    automation = API()
    automation.object_values["workflow_runs"] = [
        {
            "id": 12,
            "display_title": f"Build base {identity()}",
            "head_sha": AUTOMATION_SHA,
            "status": "completed",
            "conclusion": "success",
        },
        {
            "id": 11,
            "display_title": f"Build base {identity()}",
            "head_sha": AUTOMATION_SHA,
            "status": "completed",
            "conclusion": "success",
        },
    ]
    monkeypatch.setattr(
        release,
        "_artifact",
        lambda _world, run_id, _name: {"id": 1} if run_id == 11 else None,
    )
    found = release._run(
        world(automation=automation),
        "build-release.yml",
        f"Build base {identity()}",
        AUTOMATION_SHA,
        "completion-base-r0123456789abcdef0123",
    )
    assert found["id"] == 11


def test_latest_is_paginated_semantic_and_ignores_drafts() -> None:
    source = API()
    source.page_values = [
        {
            "tag_name": f"8.0.{index}",
            "draft": False,
            "prerelease": False,
        }
        for index in range(40)
    ] + [
        {"tag_name": "99.0.0", "draft": True, "prerelease": False},
        {"tag_name": "10.0.0-rc1", "draft": False, "prerelease": True},
        {"tag_name": "9.1.10", "draft": False, "prerelease": False},
    ]
    target = world(source=source)
    assert release._canonical_latest(target) == "9.1.10"
    assert not release._latest(target, "8.0.40")
    assert release._latest(target, "9.2.0")
    assert not release._latest(target, "10.0.0-rc2")


def test_controller_closes_only_after_every_expected_public_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state(stage="rc", branch="8.0")
    source = API()
    completion_comments: list[Mapping[str, Any]] = []

    def post(path: str, data: Mapping[str, Any]) -> Mapping[str, Any]:
        source.posts.append((path, data))
        comment = trusted_comment(str(data["body"]), APP, 99)
        completion_comments.append(comment)
        return comment

    source.post = post  # type: ignore[method-assign]
    monkeypatch.setattr(release, "_comments", lambda *_: completion_comments)
    monkeypatch.setattr(release, "_branch_head", lambda *_: "f" * 40)
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(
        release,
        "_published",
        lambda *_: {"release_url": "https://release", "receipt_id": 9},
    )
    monkeypatch.setattr(
        release,
        "_run",
        lambda *_: {
            "id": 5,
            "status": "completed",
            "conclusion": "success",
            "html_url": "https://run",
        },
    )
    verified = {
        name: {"verified": True, "detail": "verified"}
        for name in release._expected(current)
        if name != "github-latest"
    }
    monkeypatch.setattr(release, "_verify_base", lambda *_: (True, verified))
    monkeypatch.setattr(release, "_latest_ok", lambda *_: True)
    monkeypatch.setattr(release, "_save", lambda *_: None)
    release._reconcile_one(world(source=source), {"number": 1}, current, 2)
    release._reconcile_one(world(source=source), {"number": 1}, current, 2)
    assert any(data.get("state") == "closed" for _, data in source.patches)
    assert sum("valkey-release-completion" in str(data) for _, data in source.posts) == 1


def test_failed_production_run_is_reported_and_retried_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = state(stage="rc", branch="8.0")
    retries: list[str] = []
    monkeypatch.setattr(release, "_branch_head", lambda *_: SHA)
    monkeypatch.setattr(release, "_notes_pr", lambda *_: _merged_notes())
    monkeypatch.setattr(
        release,
        "_published",
        lambda *_: {"release_url": "https://release", "receipt_id": 9},
    )
    monkeypatch.setattr(
        release,
        "_run",
        lambda *_: {
            "id": 71,
            "status": "completed",
            "conclusion": "failure",
            "html_url": "https://run/71",
        },
    )
    monkeypatch.setattr(
        release,
        "_dispatch_build",
        lambda _w, _s, key, *_a: retries.append(key),
    )
    monkeypatch.setattr(release, "_notify", lambda *_: None)
    monkeypatch.setattr(release, "_save", lambda *_: None)
    release._reconcile_one(world(), {"number": 1}, current, 2)
    assert retries == [f"retry:base:{current['identity']}:71"]
    assert current["status"] == "Downstream publication incomplete"


def test_dispatches_recover_without_repeating_or_growing_the_record() -> None:
    current = state()
    token = current["identity"]
    automation = API()
    build = {"identity": token, "stage": "base"}
    for run in (71, 72):
        release._dispatch_once(automation, current, f"retry:base:{token}:{run}", "valkey-io/x", "wf.yml", build, ref="main")
    qualify_key = f"qualify:{token}"
    for _ in range(2):
        release._dispatch_once(automation, current, qualify_key, "valkey-io/x", "qualify.yml", {"identity": token}, ref="main")
    assert list(current["dispatches"]) == [f"retry:base:{token}:72", qualify_key]
    assert len(automation.dispatches) == 4 and all(f"head_sha={AUTOMATION_SHA}" in path for path in automation.object_paths)
    notes = {"version": "9.1.0"}
    for head in ("e" * 40, "f" * 40):
        release._dispatch_once(API(), current, f"notes:{head}", "valkey-io/x", "wf.yml", notes, ref="main")
    agent = API()
    note_key = "notes:" + "f" * 40
    agent.object_values["workflow_runs"] = [{"display_title": "Cut Release Notes 9.1.0", "created_at": "2020-01-01T00:00:00Z", "status": "in_progress"}]
    release._dispatch_once(agent, current, note_key, "valkey-io/x", "wf.yml", notes, ref="main")
    run = {
        "display_title": "Cut Release Notes 9.1.0",
        "created_at": current["dispatches"][note_key]["at"],
        "status": "in_progress",
    }
    agent.object_values["workflow_runs"] = [run]
    release._dispatch_once(agent, current, note_key, "valkey-io/x", "wf.yml", notes, ref="main")
    run.update(status="completed", conclusion="failure")
    release._dispatch_once(agent, current, note_key, "valkey-io/x", "wf.yml", notes, ref="main")
    assert len(agent.dispatches) == 2 and current["alerts"]
    agent, publish_key = API(), f"publish:{token}"
    inputs = {"issue": "1", "identity": token}
    release._dispatch_once(agent, current, publish_key, "valkey-io/x", "publish.yml", inputs, ref="main")
    run = {"display_title": f"Publish {token}", "created_at": current["dispatches"][publish_key]["at"], "status": "in_progress"}
    agent.object_values["workflow_runs"] = [run]
    current["dispatches"].pop(publish_key)
    release._dispatch_once(agent, current, publish_key, "valkey-io/x", "publish.yml", inputs, ref="main")
    assert len(agent.dispatches) == 1
    run.update(status="completed", conclusion="success")
    release._dispatch_once(agent, current, publish_key, "valkey-io/x", "publish.yml", inputs, ref="main")
    assert len(agent.dispatches) == 1
    run["conclusion"] = "cancelled"
    release._dispatch_once(agent, current, publish_key, "valkey-io/x", "publish.yml", inputs, ref="main")
    assert len(agent.dispatches) == 2 and current["alerts"] and all("branch=main" in path for path in agent.object_paths)
    release._invalidate(current, "branch advanced")
    assert list(current["dispatches"]) == ["notes:" + "f" * 40]
    for index in range(70):
        release._event(current, f"k{index}", "t")
    assert len(current["events"]) == 60 and current["events"][-1]["key"] == "k69"


def load_script(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_website_tags_match_only_the_exact_release_version(tmp_path: Path) -> None:
    payload = {"matrix": {"include": [
        {"name": "9.1.1", "meta": {"entries": [{"tags": ["valkey:9.1.1", "valkey:9.1.10"]}]}},
        {"name": "9.1.1-alpine", "meta": {"entries": [{"tags": ["valkey:9.1.1-alpine"]}]}},
        {"name": "9.1.10", "meta": {"entries": [{"tags": ["valkey:9.1.10"]}]}},
    ]}}
    steps = workflow(AUTO / ".github/workflows/update-valkey-website.yml")["jobs"]["prepare"]["steps"]
    run = next(step["run"] for step in steps if step.get("name") == "Prepare website patch without credentials")
    script = run.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
    template = tmp_path / "website/templates/valkey-website-template.md"
    template.parent.mkdir(parents=True)
    (tmp_path / "website/content/download/releases").mkdir(parents=True)
    template.write_text("{version}\n{tags}\n", encoding="utf-8")
    subprocess.run([sys.executable, "-", "9.1.1", json.dumps(payload)], input=script, text=True, cwd=tmp_path, check=True)
    content = (tmp_path / "website/content/download/releases/v9-1-1.md").read_text()
    assert content.splitlines() == [
        "9.1.1",
        '                - "9.1.1"',
        '                - "9.1.1-alpine"',
    ]


def test_container_alias_updates_follow_release_kind(tmp_path: Path) -> None:
    helper = load_script("alias_update", AUTO / "scripts/automate_alias_update.py")
    for version, expected in [
        ("9.0.0-rc1", {"7.2": "7", "8.1": "8 latest"}),
        ("8.1.2", {"7.2": "7", "8.1": "8 latest"}),
        ("7.4.0", {"7.2": "7", "8.1": "8 latest"}),
        ("8.2.0", {"7.2": "7", "8.2": "8 latest"}),
        ("9.0.0", {"7.2": "7", "8.1": "8", "9.0": "9 latest"}),
    ]:
        aliases = {"7.2": "7", "8.1": "8 latest"}
        helper._update(aliases, version)
        assert aliases == expected, version
    target = tmp_path / "generate-stackbrew-library.sh"
    target.write_text("x\ndeclare -A aliases=(\n\t[8.1]='8 latest'\n)\ny\n", encoding="utf-8")
    subprocess.run(
        [sys.executable, str(AUTO / "scripts/automate_alias_update.py"), str(target), "8.2.0"],
        check=True,
    )
    assert target.read_text(encoding="utf-8") == "x\ndeclare -A aliases=(\n\t[8.2]='8 latest'\n)\ny\n"


def test_downstream_validator_consumes_exact_identity_receipt_and_live_latest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = load_script("release_contract", AUTO / "scripts/release.py")
    monkeypatch.delenv("GH_TOKEN", raising=False)
    with pytest.raises(contract._Error, match="GH_TOKEN"):
        contract._get("/test")
    requests: list[Any] = []
    monkeypatch.setenv("GH_TOKEN", "read-token")
    monkeypatch.setattr(contract.urllib.request, "urlopen",
                        lambda request, **_: requests.append(request) or io.BytesIO(b"{}"))
    assert contract._get("/test") == {} and requests[0].get_header("Authorization") == "Bearer read-token"
    token = identity()
    with pytest.raises(contract._Error):
        contract._identity(token.replace("|9.1.0|", "|09.1.0|"))
    receipt = publication_receipt(state())
    comment = {
        **trusted_comment(release._comment_payload("publication", receipt)),
        "issue_url": "https://api.github.com/repos/valkey-io/valkey/issues/3",
    }
    live_release = {
        "id": 55,
        "html_url": "https://example/release",
        "draft": False,
        "prerelease": False,
        "body": f"notes\n\n<!-- valkey-release-identity:v1 {token} -->\n",
        "author": {"id": 777},
        "tag_name": "9.1.0",
    }

    def get(path: str) -> Any:
        if path.endswith("/issues/comments/9"):
            return comment
        if "/git/ref/tags/" in path:
            return {"object": {"type": "commit", "sha": SHA}}
        if "/releases/tags/" in path:
            return live_release
        if "/releases?" in path:
            return [live_release]
        raise AssertionError(path)

    monkeypatch.setattr(contract, "_get", get)
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    args = argparse.Namespace(
        identity=token,
        record="valkey-io/valkey#3",
        receipt="9",
        workflow_sha=AUTOMATION_SHA,
        actor_id="500",
        release_app_id=RELEASE_APP,
    )
    contract._authorize(args)
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["source_sha"] == SHA
    assert values["qualification_nonce"] == NONCE
    assert values["currently_latest"] == "true"
    args.actor_id = "777"
    contract._authorize(args)
    args.actor_id = "501"
    with pytest.raises(contract._Error, match="does not bind"):
        contract._authorize(args)
    args.actor_id = "500"
    comment["user"]["type"] = "User"
    with pytest.raises(contract._Error, match="does not bind"):
        contract._authorize(args)
    comment["user"]["type"] = "Bot"
    receipt["controller_sha"] = "bad"
    comment["body"] = release._comment_payload("publication", receipt)
    with pytest.raises(contract._Error, match="does not bind"):
        contract._authorize(args)


def workflow(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_cross_repo_workflow_entrypoints_and_single_production_path() -> None:
    controller = workflow(CI / ".github/workflows/release-start.yml")
    fork = workflow(FORK / ".github/workflows/release-start.yml")
    build_text = (AUTO / ".github/workflows/build-release.yml").read_text()
    assert "repository_dispatch" in str(controller.get(True, controller.get("on")))
    assert "workflow_dispatch" in str(fork.get(True, fork.get("on")))
    assert "workflow_dispatch:" in build_text and "head_sha=${{ github.sha }}" in build_text
    assert "repository_dispatch" not in build_text
    assert not (FORK / ".github/workflows/trigger-build-release.yml").exists()
    assert "release:" not in build_text
    assert "group: release-publication" in (
        CI / ".github/workflows/release-publish.yml"
    ).read_text()
    relay = json.dumps(fork)
    assert "source_run" in relay
    assert "source_repo" not in relay and "source_workflow_sha" not in relay
    assert "github.event.sender.id" in json.dumps(controller)
    assert "cut-notes" not in controller["jobs"]
    start = controller["jobs"]["start"]
    assert start["permissions"]["actions"] == "write"
    assert start["steps"][-1]["env"]["AGENT_TOKEN"] == "${{ github.token }}"


def test_production_credentials_are_environment_gated_and_authorized_first() -> None:
    credential_markers = (
        "secrets.",
        "create-github-app-token",
        "configure-aws-credentials",
        "ghaction-import-gpg",
    )
    forbidden = ("make -C", "pip install -r", "./update.sh", "generate-module-api-doc")
    checked = 0
    for path in sorted((AUTO / ".github/workflows").glob("*.yml")):
        jobs = workflow(path).get("jobs", {})
        for name, job in jobs.items():
            text = json.dumps(job)
            if not any(marker in text for marker in credential_markers):
                continue
            checked += 1
            assert job.get("environment") == "release", f"{path.name}:{name}"
            steps = job.get("steps", [])
            credential_index = next(
                index
                for index, step in enumerate(steps)
                if any(marker in json.dumps(step) for marker in credential_markers)
            )
            authorize_index = next(
                index
                for index, step in enumerate(steps)
                if "scripts/release.py authorize" in json.dumps(step)
            )
            assert authorize_index < credential_index, f"{path.name}:{name}"
            assert steps[authorize_index]["env"]["GH_TOKEN"] == "${{ github.token }}"
            assert not any(item in text for item in forbidden), f"{path.name}:{name}"
    assert checked >= 9
    setup = "\n".join(
        (AUTO / f"scripts/{name}").read_text()
        for name in ("setup-github-pages.sh", "setup-s3-bucket.sh")
    )
    commands = [line for line in setup.splitlines() if re.search(r"gh (?:secret|variable) set ", line)]
    assert commands and all("--env release" in line for line in commands)
    assert 'OIDC_SUBJECT="repo:${REPO}:environment:release"' in setup
    assert 'OIDC_SUBJECT="repo:${REPO}:*"' not in setup
    assert "default_workflow_permissions=read" in setup
    assert "can_approve_pull_request_reviews=false" in setup
    assert setup.count("/packaging") == 4
    assert all(action not in setup for action in ("s3:PutObjectAcl", "s3:DeleteObject"))


def test_release_app_capability_exists_only_in_protected_publish_job() -> None:
    publish = workflow(CI / ".github/workflows/release-publish.yml")
    publish_text = (CI / ".github/workflows/release-publish.yml").read_text()
    assert publish["jobs"]["publish"]["environment"] == "release"
    validate_half = publish_text.split("  publish:", 1)[0]
    # The validate job is read-only and holds no org members grant; team
    # membership is read only where publish --execute verifies the approver.
    assert "permission-contents: write" not in validate_half and "Controller changed after dispatch" in publish_text
    assert "permission-members" not in validate_half and "permission-members: read" in publish_text
    start_text = (CI / ".github/workflows/release-start.yml").read_text()
    assert "permission-checks" not in start_text and "permission-pull-requests" not in start_text
    assert publish_text.count("app-id: ${{ vars.RELEASE_APP_ID }}") == 2
    assert all(publish_text.count(value) == 2 for value in ("CONTROLLER_APP_ID: ${{ vars.APP_ID }}", "RELEASE_APP_ID: ${{ vars.RELEASE_APP_ID }}"))
    assert publish["jobs"]["validate"]["environment"] == "release-control"
    assert workflow(CI / ".github/workflows/release-start.yml")["jobs"]["start"][
        "environment"
    ] == "release-control"
    assert workflow(CI / ".github/workflows/release-reconcile.yml")["jobs"][
        "reconcile"
    ]["environment"] == "release-control"
    reconcile_text = (CI / ".github/workflows/release-reconcile.yml").read_text()
    assert "permission-administration: read" in reconcile_text and "permission-members: read" not in reconcile_text
    assert workflow(CI / ".github/workflows/release-notes-cut.yml")["jobs"]["cut"][
        "environment"
    ] == "release-notes"


def test_workflows_thread_identity_receipt_and_exact_source_everywhere() -> None:
    build = (AUTO / ".github/workflows/build-release.yml").read_text()
    assert 'run-name: "Build ${{ inputs.stage }} ${{ inputs.identity }}"' in build
    assert all(f"{name}:" in build for name in ("identity", "record", "receipt"))
    assert "completion-base-${{ needs.authorize.outputs.release_id }}" in build
    assert "publication_receipt:$receipt" in build
    for path in sorted((AUTO / ".github/workflows").glob("*.yml")):
        text = path.read_text()
        if "workflow_call:" in text and "publish" in text:
            assert "identity:" in text
        # Every remote fetch must refuse an HTTPS->HTTP downgrade on redirects.
        assert text.count("curl ") == text.count("--proto '=https' --proto-redir '=https'"), path.name
        assert "wget" not in text, path.name
    archives = (AUTO / ".github/workflows/call-build-linux-archives.yml").read_text()
    packages = (AUTO / ".github/workflows/packages.yml").read_text()
    hashes = (AUTO / ".github/workflows/update-valkey-hashes.yml").read_text()
    assert all("{identity:$identity,files:" in text for text in (archives, packages, hashes))
    assert "fragment identity mismatch" in build
    assert "ref: ${{ needs.validate.outputs.source_sha }}" in archives
    # Published .sha256 files must record the bare archive name (sha256sum -c)
    # and 8.1+ binaries must be qualified and built with fast float parsing.
    assert '(cd files && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")' in archives
    assert r"\( -type f -o -type l \)" in archives and "cp -a" in archives
    assert "USE_FAST_FLOAT=yes" in archives and "name: archive-${{ needs.validate.outputs.release_id }}-${{ inputs.group }}-" in archives and "pattern: archive-${{ needs.validate.outputs.release_id }}-${{ inputs.group }}-*" in archives
    assert "archive/${SHA}.tar.gz" in packages
    docs = (AUTO / ".github/workflows/update-valkey-doc.yml").read_text()
    assert "valkey.conf.md" in docs and "commits/$PREVIOUS" in docs
    assert "[[ ${line##*.} == 0 ]]" in docs
    assert 'security="$maintenance"' in docs and "'+5 years'" in docs
    assert "Create empty patch marker" not in docs
    assert "personal_token: ${{ steps.pages-token.outputs.token }}" in packages and "secrets.GITHUB_TOKEN" not in packages
    assert "VALKEY_SOURCE_TARBALL is required" in (
        AUTO / "scripts/build-rpm.sh"
    ).read_text()
    assert "refs/heads/" not in (AUTO / "scripts/build-rpm.sh").read_text()
    assert "refs/heads/" not in (AUTO / "scripts/build-deb.sh").read_text()
    assert "ERROR: Architecture mismatch!" in (AUTO / "scripts/build-deb.sh").read_text()


def test_actual_matrices_and_rc_ga_applicability_match_controller_policy() -> None:
    package_config = json.loads(
        (AUTO / ".github/package-platforms.json").read_text()
    )
    rpm = len(package_config["rpm"]["arch"]) * len(
        package_config["rpm"]["platform"]
    )
    deb = len(package_config["deb"]["arch"]) * len(
        package_config["deb"]["platform"]
    )
    shipped = yaml.safe_load((CI / "release_policy.yml").read_text())["repos"][0][
        "downstream"
    ]
    assert (rpm, deb) == (
        shipped["qualification_rpm_jobs"],
        shipped["qualification_deb_jobs"],
    ) == (30, 10)
    archives = json.loads(
        (
            AUTO
            / ".github/actions/generate-package-build-matrix/build-config.json"
        ).read_text()
    )
    actual_targets = sorted(
        f"{item['platform']}/{item['arch']}" for item in archives["linux_targets"]
    )
    assert actual_targets == sorted(shipped["tarball_targets"])
    qualify = (AUTO / ".github/workflows/qualify-release.yml").read_text()
    assert "needs.validate.outputs.stage == 'ga'" in qualify
    build = (AUTO / ".github/workflows/build-release.yml").read_text()
    assert build.count("needs.authorize.outputs.stage == 'ga'") >= 4
    assert "needs: [authorize, hashes, container, try-valkey]" in build


def test_latest_and_dependency_order_are_encoded_in_real_workflows() -> None:
    build = (AUTO / ".github/workflows/build-release.yml").read_text()
    controller = (CI / "scripts/release/main.py").read_text()
    try_workflow = (AUTO / ".github/workflows/update-try-valkey.yml").read_text()
    website = (AUTO / ".github/workflows/update-valkey-website.yml").read_text()
    assert "inputs.stage == 'dependent'" in build
    assert "images_ok and dependent_expected" in controller
    assert "currently_latest" in try_workflow and "--delete" not in try_workflow
    assert "group: try-valkey-latest" in try_workflow
    assert "currently_latest" in website
    assert "group: valkey-website-release-updates" in website
    assert '&& "$current" != "$APP_VERSION"' not in build
    for name in ("build-release", "update-valkey-container", "update-valkey-doc", "update-valkey-website"):
        text = (AUTO / f".github/workflows/{name}.yml").read_text()
        assert "test -f ../" in text and "test ! -s ../" in text


def test_all_external_actions_are_commit_pinned() -> None:
    paths = [
        *sorted((CI / ".github/workflows").glob("release-*.yml")),
        *sorted((AUTO / ".github/workflows").glob("*.yml")),
        FORK / ".github/workflows/release-start.yml",
    ]
    for path in paths:
        for value in re.findall(r"^\s*uses:\s*([^\s#]+)", path.read_text(), re.MULTILINE):
            if value.startswith("./"):
                continue
            assert re.search(r"@[0-9a-f]{40}$", value), f"{path.name}: {value}"


def test_issue_record_exposes_ready_identity_checks_outputs_and_events() -> None:
    current = state()
    current["notes_pr"] = {"number": 7, "url": "https://example/pr/7"}
    current["publish_url"] = "https://example/actions/publish"
    current["events"] = [
        {"key": "ready", "at": "2026-01-01", "text": "qualification passed"}
    ]
    body = release._render(current)
    assert "Ready to publish" in body
    assert current["identity"] in body
    assert "Required CI" in body
    assert "Qualification" in body
    assert "[#7](https://example/pr/7)" in body
    assert "Open protected publication job" in body
    assert "Expected outputs" in body
    assert "Controller record" in body
