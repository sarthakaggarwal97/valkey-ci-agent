"""Stateless Valkey release controller.

The only durable state is a GitHub issue plus App-authored receipt comments.
Every security-sensitive operation after candidate selection carries one
compact immutable identity token:

    v1|release_id|tag|source_sha|qualification_nonce|automation_sha|plan_digest
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

import yaml

from scripts.release_notes.version_bump import current_release_state

_API = "https://api.github.com"
_RECEIPT = "<!-- valkey-release-{kind}:v1 -->"
_STATE = _RECEIPT.format(kind="state")
_LABEL = "release-tracker"
_NUMBER = r"(0|[1-9]\d*)"
_TAG = re.compile(rf"^{_NUMBER}\.{_NUMBER}\.{_NUMBER}(?:-rc([1-9]\d*))?$")
_BRANCH = re.compile(rf"^{_NUMBER}\.{_NUMBER}$")
_SHA = re.compile(r"^[0-9a-f]{40}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_RELEASE_ID = re.compile(r"^r[0-9a-f]{20}$")
_IDENTITY_RE = re.compile(
    r"^v1\|(r[0-9a-f]{20})\|([^|]+)\|([0-9a-f]{40})\|([0-9a-f]{32})\|([0-9a-f]{40})\|([0-9a-f]{64})$"
)
_IDENTITY_KEYS = ("release_id", "tag", "source_sha", "qualification_nonce", "automation_sha", "plan_digest")
_NOTES_HEAD = "agent/release-cut/{version}-{stage}"
_NOTES_FILE = "00-RELEASENOTES"
_VERSION_FILE = "src/version.h"
_MAX_DOWNLOAD = 4 * 1024 * 1024
_OUTPUTS = (
    "github-release",
    "linux-archives",
    "checksums",
    "packages",
    "docker-hub",
    "ghcr",
    "ecr",
    "download-page",
    "documentation",
    "try-valkey",
    "bundle-images",
    "helm-chart",
    "github-latest",
)


class _Error(RuntimeError):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class _HttpsRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: str
    ) -> urllib.request.Request:
        if urllib.parse.urlsplit(newurl).scheme != "https":
            raise _Error("artifact redirect attempted to leave HTTPS")
        return urllib.request.Request(newurl, headers={"User-Agent": "valkey-release-controller"})


class _GitHub:
    def __init__(self, token: str) -> None:
        self.token = token

    def _request(
        self,
        path: str,
        method: str = "GET",
        data: Optional[Mapping[str, Any]] = None,
        *,
        retries: int = 2,
    ) -> tuple[Any, Mapping[str, str]]:
        url = f"{_API}{path}"
        payload = None if data is None else _json(data).encode()
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "valkey-release-controller",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    raw = response.read(_MAX_DOWNLOAD + 1)
                    if len(raw) > _MAX_DOWNLOAD:
                        raise _Error(f"GitHub response exceeds {_MAX_DOWNLOAD} bytes")
                    return (json.loads(raw) if raw else None), response.headers
            except urllib.error.HTTPError as exc:
                raw = exc.read(_MAX_DOWNLOAD)
                exc.close()
                if exc.code >= 500 and method == "GET" and attempt < retries:
                    time.sleep(2**attempt)
                    continue
                detail = raw.decode("utf-8", "replace")[:500]
                raise _Error(f"GitHub {method} {path} returned {exc.code}: {detail}") from exc
            except urllib.error.URLError as exc:
                if method == "GET" and attempt < retries:
                    time.sleep(2**attempt)
                    continue
                raise _Error(f"GitHub {method} {path} failed: {exc}") from exc
        raise AssertionError("unreachable")

    def get(self, path: str) -> Any:
        return self._request(path)[0]

    def maybe(self, path: str) -> Any:
        try:
            return self.get(path)
        except _Error as exc:
            if " returned 404:" in str(exc):
                return None
            raise

    def post(self, path: str, data: Mapping[str, Any]) -> Any:
        return self._request(path, "POST", data, retries=0)[0]

    def patch(self, path: str, data: Mapping[str, Any]) -> Any:
        return self._request(path, "PATCH", data, retries=0)[0]

    def pages(self, path: str) -> Iterator[Any]:
        separator = "&" if "?" in path else "?"
        next_path: Optional[str] = f"{path}{separator}per_page=100"
        while next_path:
            payload, headers = self._request(next_path)
            if not isinstance(payload, list):
                raise _Error(f"expected a list from {next_path}")
            yield from payload
            next_path = _next_link(headers.get("Link", ""))

    def objects(self, path: str, key: str) -> list[Any]:
        result: list[Any] = []
        separator = "&" if "?" in path else "?"
        for page in range(1, 101):
            payload = self.get(f"{path}{separator}per_page=100&page={page}")
            batch = payload.get(key) if isinstance(payload, dict) else None
            if not isinstance(batch, list):
                raise _Error(f"expected {key!r} list from {path}")
            result.extend(batch)
            if len(batch) < 100:
                return result
        raise _Error(f"{path} exceeded 10,000 {key} records")

    def workflow_dispatch(
        self, repo: str, workflow: str, ref: str, inputs: Mapping[str, str]
    ) -> None:
        self.post(
            f"/repos/{repo}/actions/workflows/{urllib.parse.quote(workflow)}/dispatches",
            {"ref": ref, "inputs": dict(inputs)},
        )


class _World:
    def __init__(
        self,
        policy: Mapping[str, Any],
        source: _GitHub,
        automation: _GitHub,
        agent: _GitHub,
        *,
        agent_repo: str,
        controller_app_id: int,
        notes_app_id: int,
        release_app_id: int,
    ) -> None:
        self.policy = policy
        self.repo = str(policy["repo"])
        self.down = policy["downstream"]
        self.source = source
        self.automation = automation
        self.agent = agent
        self.agent_repo = agent_repo
        self.controller_app_id = controller_app_id
        self.notes_app_id = notes_app_id
        self.release_app_id = release_app_id


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _next_link(header: str) -> Optional[str]:
    for part in header.split(","):
        match = re.match(r'\s*<([^>]+)>;\s*rel="([^"]+)"', part)
        if match and match.group(2) == "next":
            url = urllib.parse.urlsplit(match.group(1))
            return f"{url.path}?{url.query}"
    return None


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise _Error(f"{name} is required")
    return value


def _app_id(name: str) -> int:
    raw = os.environ.get(name, "")
    if not raw.isdigit() or int(raw) <= 0:
        raise _Error(f"{name} must be a positive GitHub App id")
    return int(raw)


def _load_policy(path: str, repo: str) -> Mapping[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise _Error("release policy must have schema_version 1")
    for item in payload.get("repos", []):
        if isinstance(item, dict) and item.get("repo") == repo:
            branches = item.get("branches")
            if not isinstance(branches, list) or not branches:
                raise _Error(f"release policy for {repo} has no branches")
            if not all(isinstance(branch, str) and _BRANCH.fullmatch(branch) for branch in branches):
                raise _Error(f"release policy for {repo} has an invalid branch")
            if not item.get("required_checks") or not isinstance(item.get("downstream"), dict):
                raise _Error(f"release policy for {repo} is incomplete")
            return item
    raise _Error(f"{repo} is not configured in {path}")


def _branch(policy: Mapping[str, Any], branch: str) -> None:
    if branch not in policy["branches"] or not _BRANCH.fullmatch(branch):
        raise _Error(f"{branch!r} is not a configured release branch")


def _tag_parts(tag: str) -> tuple[int, int, int, Optional[int]]:
    match = _TAG.fullmatch(tag)
    if not match:
        raise _Error(f"invalid release tag {tag!r}")
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), int(rc) if rc else None


def _derive(branch: str, intent: str, tags: Iterable[str]) -> tuple[str, str]:
    branch_match = _BRANCH.fullmatch(branch)
    if not branch_match:
        raise _Error(f"invalid release branch {branch!r}")
    line = (int(branch_match.group(1)), int(branch_match.group(2)))
    parsed = [parts for tag in tags if _TAG.fullmatch(tag) for parts in [_tag_parts(tag)] if parts[:2] == line]
    stable = sorted(parts[2] for parts in parsed if parts[3] is None)
    rcs = sorted(parts[3] for parts in parsed if parts[2] == 0 and parts[3] is not None)
    base = f"{line[0]}.{line[1]}"
    if intent == "rc":
        if stable:
            raise _Error(f"{base} already has a stable release; a new RC cannot be cut")
        number = (rcs[-1] if rcs else 0) + 1
        return f"{base}.0", f"rc{number}"
    if intent == "ga":
        if stable:
            raise _Error(f"{base} already has a stable release")
        if not rcs:
            raise _Error(f"{base}.0 GA requires a prior release candidate")
        return f"{base}.0", "ga"
    if intent == "patch":
        if not stable:
            raise _Error(f"{base} has no stable release to patch")
        return f"{base}.{stable[-1] + 1}", "ga"
    raise _Error("intent must be rc, ga, or patch")


def _identity(fields: Mapping[str, str]) -> str:
    token = "|".join(["v1", *(fields[key] for key in _IDENTITY_KEYS)])
    _parse_identity(token)
    return token


def _parse_identity(token: str) -> dict[str, str]:
    match = _IDENTITY_RE.fullmatch(token)
    if not match:
        raise _Error("release identity must be a strict seven-field v1 token")
    values = dict(zip(_IDENTITY_KEYS, match.groups()))
    _tag_parts(values["tag"])
    return values


def _comment_payload(kind: str, payload: Mapping[str, Any]) -> str:
    return f"{_RECEIPT.format(kind=kind)}\n```json\n{_json(payload)}\n```"


def _parse_block(body: str, marker: str) -> Optional[dict[str, Any]]:
    if marker not in body:
        return None
    match = re.search(r"```json\s*(\{.*?\})\s*```", body, re.DOTALL)
    if not match:
        return None
    try:
        value = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _trusted(comment: Mapping[str, Any], app_id: int) -> bool:
    app = comment.get("performed_via_github_app")
    user = comment.get("user")
    return (
        isinstance(app, dict)
        and app.get("id") == app_id
        and isinstance(user, dict)
        and user.get("type") == "Bot"
        and isinstance(user.get("id"), int)
    )


def _comments(client: _GitHub, repo: str, issue: int) -> list[Mapping[str, Any]]:
    return [item for item in client.pages(f"/repos/{repo}/issues/{issue}/comments") if isinstance(item, dict)]


def _state_from_comments(
    comments: Sequence[Mapping[str, Any]], app_id: int
) -> tuple[dict[str, Any], int]:
    candidates: list[tuple[dict[str, Any], int]] = []
    for comment in comments:
        if not _trusted(comment, app_id):
            continue
        state = _parse_block(str(comment.get("body", "")), _STATE)
        if state is not None and isinstance(comment.get("id"), int):
            candidates.append((state, int(comment["id"])))
    if len(candidates) != 1:
        raise _Error(f"expected one trusted release state comment, found {len(candidates)}")
    state, comment_id = candidates[0]
    if state.get("schema") != 1 or not _RELEASE_ID.fullmatch(str(state.get("release_id", ""))):
        raise _Error("trusted release state is malformed")
    return state, comment_id


def _issue_states(
    world: _World, *, state: str = "open"
) -> Iterator[tuple[Mapping[str, Any], Optional[dict[str, Any]], int]]:
    """Yield (issue, state, comment_id); state is None for a malformed tracker issue."""
    query = urllib.parse.urlencode({"state": state, "labels": _LABEL, "sort": "created", "direction": "desc"})
    for issue in world.source.pages(f"/repos/{world.repo}/issues?{query}"):
        if not isinstance(issue, dict) or "pull_request" in issue:
            continue
        number = issue.get("number")
        if not isinstance(number, int):
            continue
        try:
            release_state, comment_id = _state_from_comments(
                _comments(world.source, world.repo, number), world.controller_app_id
            )
        except _Error:
            yield issue, None, 0
            continue
        yield issue, release_state, comment_id


def _save(world: _World, issue: int, comment_id: int, state: Mapping[str, Any]) -> None:
    world.source.patch(
        f"/repos/{world.repo}/issues/comments/{comment_id}", {"body": _comment_payload("state", state)}
    )
    world.source.patch(
        f"/repos/{world.repo}/issues/{issue}",
        {"title": f"Release {state['tag']}", "body": _render(state)},
    )


def _event(state: dict[str, Any], key: str, text: str) -> bool:
    events = state.setdefault("events", [])
    if any(isinstance(item, dict) and item.get("key") == key for item in events):
        return False
    events.append({"key": key, "at": _now(), "text": text})
    del events[:-60]  # keep the state comment under GitHub's 65,536-char body limit
    return True


def _expected(state: Mapping[str, Any]) -> list[str]:
    excluded = (
        {"packages", "download-page", "documentation", "try-valkey", "helm-chart"}
        if state["stage"] != "ga"
        else set()
    )
    branch = _BRANCH.fullmatch(str(state["branch"]))
    if branch and (int(branch.group(1)), int(branch.group(2))) < (8, 1):
        excluded.add("bundle-images")
    return [name for name in _OUTPUTS if name not in excluded]


def _render(state: Mapping[str, Any]) -> str:
    status = str(state.get("status", "Preparing"))
    identity = str(state.get("identity", ""))
    notes_pr = state.get("notes_pr", {})
    notes_link = (
        f"[#{notes_pr['number']}]({notes_pr['url']})"
        if isinstance(notes_pr, dict) and notes_pr.get("url")
        else "awaiting automation"
    )
    publish_url = str(state.get("publish_url", ""))
    approval_link = (
        f"[Open protected publication job]({publish_url})"
        if publish_url
        else "available after qualification"
    )
    checks = state.get("checks", {})
    qualification = state.get("qualification", {})
    outputs = state.get("outputs", {})
    events = state.get("events", [])
    alerts = state.get("alerts", [])
    lines = [
        f"# Release {state['tag']}",
        "",
        f"**{status}**",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Owner | @{state['owner']} |",
        f"| Branch | `{state['branch']}` |",
        f"| Intent | `{state['intent']}` |",
        f"| Urgency | `{state['urgency']}` |",
        f"| Release notes | {notes_link} |",
        f"| Candidate | `{state.get('source_sha') or 'awaiting notes merge'}` |",
        f"| Required CI | {checks.get('summary', 'awaiting candidate')} |",
        f"| Qualification | {qualification.get('summary', 'not started')} |",
        f"| Identity | `{identity or 'sealed after candidate CI passes'}` |",
        f"| Approval | {approval_link} |",
        "",
        "## Expected outputs",
    ]
    for name in _expected(state):
        output = outputs.get(name, {}) if isinstance(outputs, dict) else {}
        mark = "x" if isinstance(output, dict) and output.get("verified") else " "
        detail = f" - {output.get('detail')}" if isinstance(output, dict) and output.get("detail") else ""
        lines.append(f"- [{mark}] {name}{detail}")
    if alerts:
        lines.extend(["", "## Needs attention"])
        lines.extend(f"- {item}" for item in alerts)
    if events:
        lines.extend(["", "## Controller record", "| Time (UTC) | Action |", "|---|---|"])
        lines.extend(
            f"| {item.get('at', '')} | {str(item.get('text', '')).replace('|', '&#124;')} |"
            for item in events[-30:]
            if isinstance(item, dict)
        )
    lines.extend(
        [
            "",
            "_This issue is the durable release record. Controller state and receipts are "
            "accepted only from their configured GitHub Apps._",
        ]
    )
    return "\n".join(lines) + "\n"


def _repo_tags(world: _World, branch: str) -> list[str]:
    prefix = urllib.parse.quote(f"{branch}.", safe="")
    refs = world.source.pages(f"/repos/{world.repo}/git/matching-refs/tags/{prefix}")
    return [
        str(item.get("ref", "")).removeprefix("refs/tags/")
        for item in refs
        if isinstance(item, dict) and str(item.get("ref", "")).startswith("refs/tags/")
    ]


def _authorize_start(world: _World, args: argparse.Namespace) -> str:
    if not str(args.source_run).isdigit():
        raise _Error("start receipt has an invalid run id")
    expected_sender = _required_env("START_RELAY_BOT_ID")
    if args.event_sender != expected_sender:
        raise _Error("start request was not sent by the configured relay App")
    repo = world.source.get(f"/repos/{world.repo}")
    default = str(repo["default_branch"])
    run = world.source.get(f"/repos/{world.repo}/actions/runs/{args.source_run}")
    branch = world.source.get(f"/repos/{world.repo}/branches/{urllib.parse.quote(default)}")
    workflow_path = str(run.get("path", "")).split("@", 1)[0]
    run_sha = str(run.get("head_sha", ""))
    if (
        run.get("event") != "workflow_dispatch"
        or run.get("head_branch") != default
        or not _SHA.fullmatch(run_sha)
        or branch.get("commit", {}).get("sha") != run_sha
        or workflow_path != ".github/workflows/release-start.yml"
        or run.get("head_repository", {}).get("full_name") != world.repo
    ):
        raise _Error("start receipt is not the current default-branch Start Release run")
    _environment_policy(
        world.source, world.repo, [(default, "branch")], name="release-start"
    )
    actor = str(run.get("triggering_actor", {}).get("login", ""))
    if not actor:
        raise _Error("start run has no triggering actor")
    _team_member(world, actor)
    return actor


def _ensure_label(world: _World) -> None:
    path = f"/repos/{world.repo}/labels/{urllib.parse.quote(_LABEL)}"
    if world.source.maybe(path) is None:
        world.source.post(
            f"/repos/{world.repo}/labels",
            {"name": _LABEL, "color": "1d76db", "description": "Active release transaction"},
        )


def _start(world: _World, args: argparse.Namespace) -> None:
    _branch(world.policy, args.branch)
    if args.urgency not in {"LOW", "MODERATE", "HIGH", "CRITICAL", "SECURITY"}:
        raise _Error("invalid release-note urgency")
    actor = _authorize_start(world, args)
    run_id = int(args.source_run)
    release_id = f"r{_sha256(f'{world.repo}:{run_id}')[:20]}"
    for issue, existing_state, comment_id in _issue_states(world, state="all"):
        if existing_state is None:
            raise _Error(
                f"tracker issue {issue['html_url']} has no single trusted controller "
                f"state; repair it or remove its {_LABEL} label before starting"
            )
        if existing_state.get("source_run_id") == run_id:
            raise _Error(f"start run {run_id} was already consumed by {issue['html_url']}")
        if issue.get("state") == "open" and existing_state.get("branch") == args.branch:
            _event(existing_state, f"duplicate:{run_id}", f"Rejected duplicate Start Release from @{actor}; active transaction linked here")
            _save(world, int(issue["number"]), comment_id, existing_state)
            raise _Error(f"release branch {args.branch} already has active issue {issue['html_url']}")
    version, stage = _derive(args.branch, args.intent, _repo_tags(world, args.branch))
    tag = version if stage == "ga" else f"{version}-{stage}"
    if args.dry_run:
        return
    _ensure_label(world)
    state: dict[str, Any] = {
        "schema": 1,
        "release_id": release_id,
        "source_run_id": run_id,
        "owner": actor,
        "branch": args.branch,
        "intent": args.intent,
        "urgency": args.urgency,
        "version": version,
        "stage": stage,
        "tag": tag,
        "status": "Preparing release notes",
        "source_sha": "",
        "identity": "",
        "plan": {},
        "events": [],
        "checks": {},
        "qualification": {},
        "outputs": {},
        "alerts": [],
        "dispatches": {},
    }
    _event(state, f"start:{run_id}", f"@{actor} started {tag} from trusted source run {run_id}")
    issue = world.source.post(
        f"/repos/{world.repo}/issues",
        {"title": f"Release {tag}", "body": _render(state), "labels": [_LABEL]},
    )
    number = int(issue["number"])
    comment = world.source.post(
        f"/repos/{world.repo}/issues/{number}/comments", {"body": _comment_payload("state", state)}
    )
    if not _trusted(comment, world.controller_app_id):
        raise _Error("state comment was not attributed to the controller App")
    _cut_notes(world, state, _branch_head(world, args.branch))
    _save(world, number, int(comment["id"]), state)


def _file(client: _GitHub, repo: str, path: str, ref: str = "") -> str:
    query = f"?{urllib.parse.urlencode({'ref': ref})}" if ref else ""
    value = client.get(f"/repos/{repo}/contents/{path}{query}")
    if not isinstance(value, dict) or value.get("encoding") != "base64":
        raise _Error(f"{repo}/{path}@{ref} is not a base64 file")
    try:
        return base64.b64decode(str(value["content"]), validate=False).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise _Error(f"cannot decode {repo}/{path}@{ref}") from exc


def _package_targets(world: _World, automation_sha: str) -> dict[str, list[str]]:
    try:
        config = json.loads(
            _file(world.automation, str(world.down["automation_repo"]), ".github/package-platforms.json", automation_sha)
        )
        targets = {
            kind: sorted(
                f"{kind}/{platform['id']}/{arch}"
                for platform in config[kind]["platform"] for arch in config[kind]["arch"]
            )
            for kind in ("rpm", "deb")
        }
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise _Error("release automation package matrix is malformed") from exc
    if any(not values or len(values) != len(set(values)) for values in targets.values()):
        raise _Error("release automation package matrix is empty or duplicated")
    return targets


def _notes_section(notes: str, tag: str) -> str:
    heading = re.compile(rf"^Valkey\s+{re.escape(tag)}(?:\s|$)", re.MULTILINE)
    match = heading.search(notes)
    if not match:
        raise _Error(f"{_NOTES_FILE} has no section for Valkey {tag}")
    next_heading = re.search(r"^Valkey\s+\d+\.\d+\.\d+", notes[match.end() :], re.MULTILINE)
    end = match.end() + next_heading.start() if next_heading else len(notes)
    return notes[match.start() : end].rstrip() + "\n"


def _notes_pr(world: _World, state: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    query = urllib.parse.urlencode({"state": "all", "base": state["branch"], "sort": "created", "direction": "desc"})
    expected = _NOTES_HEAD.format(version=state["version"], stage=state["stage"])
    for pull in world.source.pages(f"/repos/{world.repo}/pulls?{query}"):
        if not isinstance(pull, dict):
            continue
        if pull.get("base", {}).get("ref") != state["branch"] or pull.get("head", {}).get("ref") != expected:
            continue
        if pull.get("head", {}).get("repo", {}).get("full_name") != world.repo:
            continue
        number = pull.get("number")
        if not isinstance(number, int):
            continue
        issue = world.source.get(f"/repos/{world.repo}/issues/{number}")
        app = issue.get("performed_via_github_app")
        if not isinstance(app, dict) or app.get("id") != world.notes_app_id:
            continue
        return pull
    return None


def _branch_head(world: _World, branch: str) -> str:
    value = world.source.get(f"/repos/{world.repo}/branches/{urllib.parse.quote(branch)}")
    sha = str(value.get("commit", {}).get("sha", ""))
    if not _SHA.fullmatch(sha):
        raise _Error(f"cannot resolve branch {branch}")
    return sha


def _ci_status(world: _World, state: Mapping[str, Any], sha: str) -> tuple[bool, dict[str, Any]]:
    workflow = urllib.parse.quote(str(world.policy["checks_workflow"]))
    query = urllib.parse.urlencode({"branch": state["branch"], "event": "push", "head_sha": sha})
    runs = world.source.get(f"/repos/{world.repo}/actions/workflows/{workflow}/runs?{query}")
    candidates = [
        run
        for run in runs.get("workflow_runs", [])
        if run.get("head_sha") == sha
        and run.get("head_branch") == state["branch"]
        and run.get("event") == "push"
    ]
    if not candidates:
        return False, {"summary": "waiting for the exact-commit CI workflow"}
    run = max(candidates, key=lambda item: (str(item.get("created_at", "")), int(item.get("id", 0))))
    if run.get("status") != "completed":
        return False, {"summary": f"[CI run {run['id']}]({run['html_url']}) is running", "run_id": run["id"]}
    jobs = world.source.objects(f"/repos/{world.repo}/actions/runs/{run['id']}/jobs?filter=latest", "jobs")
    bad_jobs = [
        str(job.get("name", "unnamed"))
        for job in jobs
        if job.get("status") != "completed" or job.get("conclusion") not in {"success", "skipped", "neutral"}
    ]
    job_names = {str(job.get("name", "")) for job in jobs}
    checks = world.source.objects(f"/repos/{world.repo}/commits/{sha}/check-runs", "check_runs")
    required = {str(name): None for name in world.policy["required_checks"]}
    protection = world.source.maybe(
        f"/repos/{world.repo}/branches/{urllib.parse.quote(str(state['branch']))}/protection/required_status_checks"
    )
    if isinstance(protection, dict):
        required.update((str(name), None) for name in protection.get("contexts", []))
        required.update(
            (str(item.get("context")), item.get("app_id"))
            for item in protection.get("checks", [])
            if isinstance(item, dict) and item.get("context")
        )
    missing = []
    for name, app_id in required.items():
        check = max(
            (
                item for item in checks
                if isinstance(item, dict) and item.get("name") == name and (
                    app_id is None or isinstance(item.get("app"), dict)
                    and item["app"].get("id") == app_id
                )
            ),
            key=lambda item: int(item.get("id", 0)), default={},
        )
        if (
            check.get("status") != "completed"
            or check.get("conclusion") not in {"success", "neutral", "skipped"}
            or (name in world.policy["required_checks"] and name not in job_names)
        ):
            missing.append(name)
    missing.sort()
    passed = run.get("conclusion") == "success" and bool(jobs) and not bad_jobs and not missing
    detail = f"[CI run {run['id']}]({run['html_url']}): {len(jobs)} jobs"
    if bad_jobs:
        detail += f"; failing: {', '.join(bad_jobs)}"
    if missing:
        detail += f"; required checks missing/red: {', '.join(missing)}"
    return passed, {"summary": detail, "run_id": run["id"], "jobs": len(jobs), "required": sorted(required)}


def _stable_tags(world: _World) -> list[tuple[tuple[int, int, int], str]]:
    stable: list[tuple[tuple[int, int, int], str]] = []
    for release in world.source.pages(f"/repos/{world.repo}/releases"):
        if not isinstance(release, dict) or release.get("draft") or release.get("prerelease"):
            continue
        name = str(release.get("tag_name", ""))
        if _TAG.fullmatch(name):
            parts = _tag_parts(name)
            if parts[3] is None:
                stable.append((parts[:3], name))
    return stable


def _canonical_latest(world: _World) -> str:
    stable = _stable_tags(world)
    return max(stable)[1] if stable else ""


def _latest(world: _World, tag: str) -> bool:
    parts = _tag_parts(tag)
    stable = _stable_tags(world)
    return parts[3] is None and (not stable or parts[:3] >= max(stable)[0])


def _automation_head(world: _World) -> str:
    repo = world.automation.get(f"/repos/{world.down['automation_repo']}")
    branch = str(repo["default_branch"])
    value = world.automation.get(
        f"/repos/{world.down['automation_repo']}/branches/{urllib.parse.quote(branch)}"
    )
    sha = str(value.get("commit", {}).get("sha", ""))
    if not _SHA.fullmatch(sha):
        raise _Error("cannot resolve release automation default branch")
    return sha


def _plan(world: _World, state: Mapping[str, Any], sha: str, automation_sha: str, nonce: str = "") -> dict[str, Any]:
    recorded_version, recorded_stage = current_release_state(
        _file(world.source, world.repo, _VERSION_FILE, sha)
    )
    if (recorded_version, recorded_stage) != (state["version"], state["stage"]):
        raise _Error(
            f"{_VERSION_FILE}@{sha} records {recorded_version}/{recorded_stage}, "
            f"expected {state['version']}/{state['stage']}"
        )
    notes = _notes_section(_file(world.source, world.repo, _NOTES_FILE, sha), str(state["tag"]))
    urgency = re.search(r"^Upgrade urgency ([A-Z]+):", notes, re.MULTILINE)
    if not urgency or urgency.group(1) != state["urgency"]:
        raise _Error(f"{_NOTES_FILE}@{sha} does not record requested urgency {state['urgency']}")
    plan: dict[str, Any] = {
        "schema": 1,
        "release_id": state["release_id"],
        "tag": state["tag"],
        "source_sha": sha,
        "qualification_nonce": nonce or secrets.token_hex(16),
        "automation_sha": automation_sha,
        "make_latest": _latest(world, str(state["tag"])),
        "notes_sha256": _sha256(notes),
        "outputs": _expected(state),
        "packages": (
            _package_targets(world, automation_sha) if state["stage"] == "ga" else {"rpm": [], "deb": []}
        ),
    }
    plan["plan_digest"] = _sha256(_json(plan))
    return plan


def _seal(world: _World, state: dict[str, Any], sha: str) -> None:
    automation_sha = _automation_head(world)
    plan = _plan(world, state, sha, automation_sha)
    token = _identity({key: str(plan[key]) for key in _IDENTITY_KEYS})
    state["source_sha"] = sha
    state["plan"] = plan
    state["identity"] = token
    state["qualification"] = {"summary": "dispatch pending"}
    _event(state, f"sealed:{token}", f"Sealed immutable identity `{token}`")


def _invalidate(state: dict[str, Any], reason: str) -> None:
    token = str(state.get("identity", ""))
    if token:
        _event(state, f"invalid:{token}", f"Invalidated `{token}`: {reason}")
        # A fresh nonce per seal means this identity never recurs; its dispatch keys are dead.
        state["dispatches"] = {k: v for k, v in state.get("dispatches", {}).items() if token not in k}
    state["identity"] = ""
    state["plan"] = {}
    state["qualification"] = {}
    state["source_sha"] = ""
    state.pop("publish_url", None)


def _dispatch_once(
    client: _GitHub, state: dict[str, Any], key: str, repo: str,
    workflow: str, inputs: Mapping[str, str], *, ref: str = "",
) -> None:
    previous = state.setdefault("dispatches", {}).get(key)
    prefix = key.split(":", 1)[0]
    notes = prefix == "notes"
    target_ref = ref or str(client.get(f"/repos/{repo}")["default_branch"])
    if not notes or previous is not None:
        query = (
            f"branch={urllib.parse.quote(target_ref)}"
            if prefix in {"notes", "publish"}
            else f"head_sha={_parse_identity(inputs['identity'])['automation_sha']}"
        )
        path = f"/repos/{repo}/actions/workflows/{urllib.parse.quote(workflow)}/runs?event=workflow_dispatch&{query}"
        runs = client.objects(path, "workflow_runs")
        verb = prefix.title() if prefix in {"publish", "qualify"} else f"Build {key.split(':', 2)[1]}"
        title = f"Cut Release Notes {inputs['version']}" if notes else f"{verb} {inputs['identity']}"
        matching = [run for run in runs if run.get("display_title") == title]
        if notes:
            matching = [run for run in matching if str(run.get("created_at", "")).replace("Z", "+00:00") >= previous["at"]]
        if any(run.get("status") != "completed" or
               (not notes and run.get("conclusion") == "success") for run in matching):
            return
        if previous is not None or matching:
            state["alerts"] = state["alerts"] or [f"{workflow.removesuffix('.yml')} did not complete; controller is retrying"]
    if key.startswith(("retry:", "notes:")):
        stem = key.rsplit(":", 1)[0] if key.startswith("retry:") else "notes"
        state["dispatches"] = {k: v for k, v in state["dispatches"].items() if not k.startswith(f"{stem}:")}
    dispatched_at = _now()
    client.workflow_dispatch(repo, workflow, target_ref, inputs)
    state["dispatches"][key] = {"at": dispatched_at, "workflow": workflow}
    _event(state, f"dispatch:{key}:{dispatched_at}", f"{'Redispatched' if previous is not None else 'Dispatched'} `{workflow}` ({key})")


def _artifact(world: _World, run_id: int, name: str) -> Optional[Mapping[str, Any]]:
    artifacts = world.automation.objects(
        f"/repos/{world.down['automation_repo']}/actions/runs/{run_id}/artifacts"
        f"?name={urllib.parse.quote(name)}",
        "artifacts",
    )
    matches = [item for item in artifacts if item.get("name") == name and not item.get("expired")]
    if len(matches) > 1:
        raise _Error(f"run {run_id} has {len(matches)} usable {name!r} artifacts")
    return matches[0] if matches else None


def _signed_artifact(world: _World, run_id: int, name: str) -> dict[str, Any]:
    artifact = _artifact(world, run_id, name)
    if artifact is None:
        raise _Error(f"run {run_id} has no usable {name!r} artifact")
    url = str(artifact["archive_download_url"])
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {world.automation.token}",
        "User-Agent": "valkey-release-controller",
    }
    request = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        opener.open(request, timeout=30).close()
    except urllib.error.HTTPError as exc:
        if exc.fp:
            exc.close()
        if exc.code not in {301, 302, 303, 307, 308}:
            raise _Error(f"artifact API returned {exc.code}") from exc
        location = exc.headers.get("Location", "")
    else:
        raise _Error("artifact API did not return a signed redirect")
    raw = _https_get(location, 30)
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            files = [item for item in archive.infolist() if not item.is_dir()]
            if len(files) != 1 or files[0].file_size > _MAX_DOWNLOAD:
                raise _Error("artifact must contain exactly one bounded manifest")
            value = json.loads(archive.read(files[0]))
    except (zipfile.BadZipFile, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _Error("artifact is not a valid JSON manifest zip") from exc
    if not isinstance(value, dict):
        raise _Error("artifact manifest must be an object")
    return value


def _qualification(world: _World, state: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    identity = str(state["identity"])
    run = _run(
        world,
        str(world.down["qualification_workflow"]),
        f"Qualify {identity}",
        _parse_identity(identity)["automation_sha"],
        f"qualification-{state['release_id']}",
    )
    if run is None:
        return "missing", {"summary": "qualification dispatch has not appeared"}
    summary = {"run_id": run["id"], "url": run["html_url"]}
    link = f"[qualification run {run['id']}]({run['html_url']})"
    if run.get("status") != "completed":
        summary["summary"] = f"{link} is running"
        return "pending", summary
    if run.get("conclusion") != "success":
        summary["summary"] = f"{link} concluded {run.get('conclusion')}"
        return "failed", summary
    manifest = _signed_artifact(world, int(run["id"]), f"qualification-{state['release_id']}")
    expected_archives = sorted(str(item) for item in world.down["tarball_targets"])
    coverage = manifest.get("coverage", {})
    valid = (
        manifest.get("schema") == 1
        and manifest.get("kind") == "qualification"
        and manifest.get("identity") == identity
        and manifest.get("result") == "passed"
        and isinstance(coverage, dict)
        and all(isinstance(coverage.get(kind), list) for kind in ("archives", "rpm", "deb"))
        and sorted(coverage.get("archives", [])) == expected_archives
        and {kind: sorted(coverage.get(kind, [])) for kind in ("rpm", "deb")}
        == state["plan"]["packages"]
    )
    if not valid:
        raise _Error(f"qualification run {run['id']} emitted a mismatched manifest")
    summary["summary"] = f"{link} passed"
    summary["manifest_sha256"] = _sha256(_json(manifest))
    return "passed", summary


def _receipt(
    comments: Sequence[Mapping[str, Any]],
    kind: str,
    app_id: int,
    identity: str,
) -> Optional[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    marker = _RECEIPT.format(kind=kind)
    matches: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for comment in comments:
        if not _trusted(comment, app_id):
            continue
        payload = _parse_block(str(comment.get("body", "")), marker)
        if payload is not None and payload.get("identity") == identity:
            matches.append((comment, payload))
    if len(matches) > 1:
        raise _Error(f"multiple trusted {kind} receipts exist for one identity")
    return matches[0] if matches else None


def _prewrite(
    comments: Sequence[Mapping[str, Any]],
    state: Mapping[str, Any],
    app_id: int,
) -> Optional[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    found = _receipt(comments, "prewrite", app_id, str(state["identity"]))
    if found is None:
        return None
    _, payload = found
    qualification = state.get("qualification", {})
    if (
        payload.get("schema") != 1
        or payload.get("source_sha") != state["source_sha"]
        or payload.get("plan_digest") != state["plan"]["plan_digest"]
        or payload.get("qualification_run_id") != qualification.get("run_id")
        or payload.get("qualification_manifest_sha256")
        != qualification.get("manifest_sha256")
        or not _SHA.fullmatch(str(payload.get("controller_sha", "")))
        or not str(payload.get("run_id", "")).isdigit()
        or not payload.get("approver")
    ):
        raise _Error("trusted prewrite receipt is malformed or stale")
    return found


def _tag_sha(client: _GitHub, repo: str, tag: str) -> str:
    ref = client.maybe(f"/repos/{repo}/git/ref/tags/{urllib.parse.quote(tag, safe='')}")
    if not isinstance(ref, dict):
        return ""
    obj = ref.get("object", {})
    for _ in range(4):
        sha = str(obj.get("sha", ""))
        if obj.get("type") == "commit":
            return sha if _SHA.fullmatch(sha) else ""
        if obj.get("type") != "tag" or not _SHA.fullmatch(sha):
            return ""
        tag_obj = client.get(f"/repos/{repo}/git/tags/{sha}")
        obj = tag_obj.get("object", {})
    return ""


def _automation_ref(identity: str) -> str:
    fields = _parse_identity(identity)
    return f"release-automation/{fields['release_id']}-{fields['qualification_nonce']}"


def _published(
    world: _World, state: Mapping[str, Any], comments: Sequence[Mapping[str, Any]]
) -> Optional[Mapping[str, Any]]:
    found = _receipt(comments, "publication", world.release_app_id, str(state["identity"]))
    if found is None:
        return None
    comment, receipt = found
    prewrite = _prewrite(comments, state, world.release_app_id)
    release = world.source.maybe(
        f"/repos/{world.repo}/releases/tags/{urllib.parse.quote(str(state['tag']), safe='')}"
    )
    if (
        not isinstance(release, dict)
        or release.get("draft")
        or bool(release.get("prerelease")) != (state["stage"] != "ga")
        or _tag_sha(world.source, world.repo, str(state["tag"])) != state["source_sha"]
        or receipt.get("release_id") != release.get("id")
        or receipt.get("source_sha") != state["source_sha"]
        or receipt.get("plan_digest") != state["plan"]["plan_digest"]
        or receipt.get("schema") != 1
        or not isinstance(receipt.get("make_latest"), bool)
        or receipt.get("make_latest") != state["plan"]["make_latest"]
        or receipt.get("release_url") != release.get("html_url")
        or receipt.get("release_author_id") != release.get("author", {}).get("id")
        or receipt.get("release_author_id") != comment.get("user", {}).get("id")
        or not isinstance(receipt.get("controller_actor_id"), int)
        or str(state["identity"]) not in str(release.get("body", ""))
        or prewrite is None
        or any(
            receipt.get(key) != prewrite[1].get(key)
            for key in ("controller_sha", "run_id", "approver")
        )
    ):
        raise _Error("trusted publication receipt does not match the live immutable release")
    result = dict(receipt)
    result["receipt_id"] = comment["id"]
    return result


def _run(world: _World, workflow: str, title: str, automation_sha: str, artifact: str) -> Optional[Mapping[str, Any]]:
    payload = world.automation.objects(
        f"/repos/{world.down['automation_repo']}/actions/workflows/"
        f"{urllib.parse.quote(workflow)}/runs"
        f"?event=workflow_dispatch&head_sha={automation_sha}",
        "workflow_runs",
    )
    runs = [item for item in payload if item.get("display_title") == title and item.get("head_sha") == automation_sha]
    runs.sort(key=lambda item: int(item.get("id", 0)), reverse=True)
    for run in runs:
        if run.get("status") == "completed" and run.get("conclusion") == "success":
            if _artifact(world, int(run["id"]), artifact) is not None:
                return run
    active = [run for run in runs if run.get("status") != "completed"]
    return active[0] if active else (runs[0] if runs else None)


def _download(url: str, expected: str, content_sha256: str = "") -> bool:
    if (
        urllib.parse.urlsplit(url).scheme != "https"
        or not _HEX64.fullmatch(expected)
        or (content_sha256 and not secrets.compare_digest(expected, content_sha256))
    ):
        return False
    digest = hashlib.sha256()
    request = urllib.request.Request(url, headers={"User-Agent": "valkey-release-controller"})
    try:
        with urllib.request.build_opener(_HttpsRedirect).open(request, timeout=60) as response:
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
    except (_Error, urllib.error.URLError, TimeoutError):
        return False
    return secrets.compare_digest(digest.hexdigest(), expected)


def _registry(url: str) -> bool:
    if urllib.parse.urlsplit(url).scheme != "https":
        return False
    headers = {
        "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
        "User-Agent": "valkey-release-controller",
    }
    opener = urllib.request.build_opener(_HttpsRedirect)
    try:
        with opener.open(urllib.request.Request(url, headers=headers), timeout=20) as response:
            return 200 <= response.status < 300
    except urllib.error.HTTPError as exc:
        challenge = exc.headers.get("WWW-Authenticate", "") if exc.code == 401 else ""
        if exc.fp:
            exc.close()
    except (_Error, urllib.error.URLError, TimeoutError):
        return False
    match = re.match(r'Bearer\s+realm="([^"]+)"(?:,\s*service="([^"]+)")?(?:,\s*scope="([^"]+)")?', challenge)
    if not match or urllib.parse.urlsplit(match.group(1)).scheme != "https":
        return False
    query = {key: value for key, value in (("service", match.group(2)), ("scope", match.group(3))) if value}
    token_url = f"{match.group(1)}?{urllib.parse.urlencode(query)}"
    try:
        payload = json.loads(_https_get(token_url, 20))
        token = payload.get("token") or payload.get("access_token")
        if not isinstance(token, str) or not token:
            return False
        headers["Authorization"] = f"Bearer {token}"
        with opener.open(
            urllib.request.Request(url, headers=headers), timeout=20
        ) as response:
            return 200 <= response.status < 300
    except (_Error, urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return False


def _https_get(url: str, timeout: int) -> bytes:
    """Fetch bounded public bytes, enforcing HTTPS on every redirect hop."""
    if urllib.parse.urlsplit(url).scheme != "https":
        raise _Error(f"refusing non-HTTPS public URL {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "valkey-release-controller"})
    opener = urllib.request.build_opener(_HttpsRedirect)
    with opener.open(request, timeout=timeout) as response:
        raw = response.read(_MAX_DOWNLOAD + 1)
    if len(raw) > _MAX_DOWNLOAD:
        # Host only: the URL may carry a signed query string and errors reach the public issue.
        raise _Error(f"{urllib.parse.urlsplit(url).netloc} response exceeds {_MAX_DOWNLOAD} bytes")
    return raw


def _page_contains(url: str, needles: Iterable[str]) -> bool:
    try:
        text = _https_get(url, 30).decode("utf-8", "replace")
    except (_Error, urllib.error.URLError, TimeoutError):
        return False
    return all(needle in text for needle in needles)


def _pr_merged(client: _GitHub, repo: str, branch: str, identity: str) -> tuple[bool, str]:
    owner = repo.split("/", 1)[0]
    query = urllib.parse.urlencode({"state": "all", "head": f"{owner}:{branch}"})
    pulls = client.get(f"/repos/{repo}/pulls?{query}")
    if not pulls:
        return False, f"waiting for {repo}:{branch} PR"
    summary = max(pulls, key=lambda item: int(item.get("number", 0)))
    pull = client.get(f"/repos/{repo}/pulls/{summary['number']}")
    default = str(client.get(f"/repos/{repo}")["default_branch"])
    bound = identity in str(pull.get("body", ""))
    valid = (
        bool(pull.get("merged_at"))
        and isinstance(pull.get("base"), dict)
        and pull["base"].get("ref") == default
        and isinstance(pull.get("head"), dict)
        and isinstance(pull["head"].get("repo"), dict)
        and pull["head"]["repo"].get("full_name") == repo
        and isinstance(pull.get("changed_files"), int)
        and pull["changed_files"] > 0
        and bound
    )
    return valid, str(pull.get("html_url", ""))


def _verify_base(
    world: _World,
    state: Mapping[str, Any],
    run: Mapping[str, Any],
    receipt_id: str,
) -> tuple[bool, dict[str, dict[str, Any]]]:
    manifest = _signed_artifact(world, int(run["id"]), f"completion-base-{state['release_id']}")
    if (
        manifest.get("schema") != 1
        or manifest.get("kind") != "completion"
        or manifest.get("stage") != "base"
        or manifest.get("identity") != state["identity"]
        or manifest.get("publication_receipt") != receipt_id
        or manifest.get("result") != "passed"
    ):
        raise _Error("base production manifest does not match the release identity")
    outputs: dict[str, dict[str, Any]] = {}
    files = manifest.get("files")
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise _Error("base production manifest has no valid public files")
    checksum_sha256 = {
        str(item.get("target")): _sha256(f"{item.get('sha256')}  {item.get('name')}\n")
        for item in files if item.get("kind") == "archive"
    }
    bad_files = {
        str(item.get("name", ""))
        for item in files
        if not _download(
            str(item.get("url", "")),
            str(item.get("sha256", "")),
            checksum_sha256.get(str(item.get("target")), "") if item.get("kind") == "checksum" else "",
        )
    }

    def group(*kinds: str) -> tuple[set[str], bool]:
        items = [item for item in files if item.get("kind") in kinds]
        targets = {str(item.get("target")) for item in items}
        return targets, all(str(item.get("name", "")) not in bad_files for item in items)

    expected_archives = set(str(item) for item in world.down["tarball_targets"])
    archive_targets, archives_clean = group("archive")
    checksum_targets, checksums_clean = group("checksum")
    outputs["linux-archives"] = {
        "verified": archive_targets == expected_archives and archives_clean,
        "detail": f"{len(archive_targets)}/{len(expected_archives)} exact-SHA archives verified",
    }
    outputs["checksums"] = {
        "verified": checksum_targets == expected_archives and checksums_clean,
        "detail": "public checksum bytes recomputed",
    }
    if state["stage"] == "ga":
        rpm_targets, rpm_clean = group("rpm")
        deb_targets, deb_clean = group("deb")
        expected_packages = state["plan"]["packages"]
        expected_rpm = set(expected_packages["rpm"])
        expected_deb = set(expected_packages["deb"])
        outputs["packages"] = {
            "verified": (
                rpm_targets == expected_rpm
                and deb_targets == expected_deb
                and rpm_clean
                and deb_clean
            ),
            "detail": (
                f"{len(rpm_targets)}/{len(expected_rpm)} RPM and "
                f"{len(deb_targets)}/{len(expected_deb)} DEB targets verified"
            ),
        }
    tag = str(state["tag"])
    docker_tags = (tag, f"{tag}-trixie", f"{tag}-alpine")
    outputs["docker-hub"] = {
        "verified": all(
            _registry(
                f"https://hub.docker.com/v2/repositories/"
                f"{world.down['dockerhub_repo']}/tags/{image_tag}"
            )
            for image_tag in docker_tags
        ),
        "detail": f"{tag} base and variant manifests",
    }
    outputs["ghcr"] = {
        "verified": _registry(
            f"https://ghcr.io/v2/{world.down['ghcr_image_repo']}/manifests/{tag}"
        ),
        "detail": f"{tag} registry manifest",
    }
    outputs["ecr"] = {
        "verified": _registry(
            f"https://public.ecr.aws/v2/{world.down['ecr_namespace']}/valkey/manifests/{tag}"
        ),
        "detail": f"{tag} registry manifest",
    }
    source_items = [item for item in files if item.get("kind") == "source"]
    hashes_ok, hashes_url = _pr_merged(world.agent, str(world.down["hashes_repo"]), f"release-{tag}", str(state["identity"]))
    source_recorded = False
    if len(source_items) == 1 and source_items[0].get("name") not in bad_files and hashes_ok:
        item = source_items[0]
        readme = _file(world.agent, str(world.down["hashes_repo"]), "README")
        line = f"hash {item['name']} sha256 {item['sha256']} {item['url']}"
        source_recorded = line in readme.splitlines()
    outputs["checksums"]["verified"] = bool(outputs["checksums"]["verified"] and source_recorded)
    outputs["checksums"]["detail"] = hashes_url or outputs["checksums"]["detail"]
    container_ok, container_url = _pr_merged(world.agent, str(world.down["container_repo"]), f"update-{tag}", str(state["identity"]))
    outputs["docker-hub"]["verified"] = bool(outputs["docker-hub"]["verified"] and container_ok)
    outputs["docker-hub"]["detail"] = container_url or outputs["docker-hub"]["detail"]
    if state["stage"] == "ga":
        patch = _tag_parts(tag)[2]
        doc_repo = str(world.down["doc_repo"])
        if patch == 0:
            docs_ok, docs_url = _pr_merged(world.agent, doc_repo, f"update-docs-{tag}", str(state["identity"]))
            docs_ok = bool(docs_ok and _page_contains(
                "https://valkey.io/topics/releases/", [f"<td>{state['branch']}</td>"]
            ))
        else:
            doc_sha = _tag_sha(world.agent, doc_repo, tag)
            docs_ok = bool(doc_sha and doc_sha == _tag_sha(world.agent, doc_repo, f"{state['branch']}.{patch - 1}"))
            docs_url = f"https://github.com/{doc_repo}/releases/tag/{tag}"
        website_ok, _ = _pr_merged(world.agent, str(world.down["website_repo"]), f"update-website-{tag}", str(state["identity"]))
        download_url = str(manifest.get("download_page_url", ""))
        archive_names = [str(item.get("name")) for item in files if item.get("kind") == "archive"]
        download_ok = bool(
            website_ok
            and download_url
            and _page_contains(download_url, [f"/releases/tag/{tag}", *archive_names])
        )
        outputs["documentation"] = {"verified": docs_ok, "detail": docs_url}
        outputs["download-page"] = {
            "verified": download_ok,
            "detail": download_url or "download page is missing from the completion manifest",
        }
        canonical = _canonical_latest(world)
        try_page_ok = bool(canonical and _page_contains(
            "https://valkey.io/try-valkey/", [f"/try-me-valkey/{canonical}/"]
        ))
        if canonical == tag:
            try_urls = manifest.get("try_valkey_urls", [])
            prefix = f"https://download.valkey.io/try-me-valkey/{tag}/"
            try_count = len(try_urls) if isinstance(try_urls, list) else 0
            try_ok = (
                try_count == 3
                and try_urls[:2] == [prefix + "states/state.bin.gz", prefix + "fs/alpine-fs.json"]
                and isinstance(try_urls[2], str) and try_urls[2].startswith(prefix + "fs/alpine-rootfs-flat/")
                and all(_registry(url) for url in try_urls)
                and try_page_ok
            )
            outputs["try-valkey"] = {
                "verified": try_ok,
                "detail": f"{try_count} public objects and deployed Try page",
            }
        else:
            outputs["try-valkey"] = {
                "verified": try_page_ok,
                "detail": f"Try page tracks canonical latest {canonical or 'pending'}",
            }
    return all(item.get("verified") for item in outputs.values()), outputs


def _verify_dependent(
    world: _World,
    state: Mapping[str, Any],
    run: Mapping[str, Any],
    receipt_id: str,
) -> tuple[bool, dict[str, dict[str, Any]]]:
    manifest = _signed_artifact(world, int(run["id"]), f"completion-dependent-{state['release_id']}")
    if (
        manifest.get("schema") != 1
        or manifest.get("kind") != "completion"
        or manifest.get("stage") != "dependent"
        or manifest.get("identity") != state["identity"]
        or manifest.get("publication_receipt") != receipt_id
        or manifest.get("result") != "passed"
    ):
        raise _Error("dependent production manifest does not match the release identity")
    outputs: dict[str, dict[str, Any]] = {}
    if "bundle-images" in _expected(state):
        line = str(manifest.get("bundle_line", ""))
        bundle_repo = str(world.down["bundle_repo"])
        try:
            versions = json.loads(_file(world.agent, bundle_repo, "versions.json"))
            line_data = versions.get(line, {})
            server = line_data.get("valkey-server", {})
            tag = str(line_data.get("version", ""))
            bound = server.get("version") == state["tag"]
        except (AttributeError, json.JSONDecodeError):
            tag = ""
            bound = False
        urls = (
            f"https://hub.docker.com/v2/repositories/{world.down['bundle_dockerhub_repo']}/tags/{tag}",
            f"https://ghcr.io/v2/{world.down['bundle_repo']}/manifests/{tag}",
            f"https://public.ecr.aws/v2/{world.down['ecr_namespace']}/"
            f"{str(world.down['bundle_repo']).split('/', 1)[1]}/manifests/{tag}",
        )
        outputs["bundle-images"] = {
            "verified": bound and bool(tag) and all(_registry(url) for url in urls),
            "detail": f"bundle {tag or 'pending'} in three registries",
        }
    if state["stage"] == "ga":
        helm_version = str(manifest.get("helm_version", ""))
        index = str(world.down["helm_index_url"])
        try:
            text = _https_get(index, 20).decode()
            entries = yaml.safe_load(text).get("entries", {})
            versions = entries.get("valkey", []) if isinstance(entries, dict) else []
            charts = [
                chart
                for chart in versions
                if isinstance(chart, dict) and str(chart.get("version")) == helm_version
            ]
            app_versions = [_tag_parts(str(chart.get("appVersion")))[:3] for chart in charts]
            helm_ok = bool(app_versions) and max(app_versions) >= _tag_parts(str(state["version"]))[:3]
        except (AttributeError, _Error, urllib.error.URLError, TimeoutError, UnicodeDecodeError, yaml.YAMLError):
            helm_ok = False
        outputs["helm-chart"] = {"verified": helm_ok, "detail": f"chart {helm_version or 'pending'}"}
    return all(item.get("verified") for item in outputs.values()), outputs


def _latest_ok(world: _World) -> bool:
    latest = world.source.maybe(f"/repos/{world.repo}/releases/latest")
    tag = str(latest.get("tag_name", "")) if isinstance(latest, dict) else ""
    return tag == _canonical_latest(world)


def _notify(world: _World, issue: int, state: dict[str, Any], alerts: Sequence[str]) -> None:
    fingerprint = _sha256(_json(sorted(alerts)))[:16]
    if not alerts or not _event(
        state,
        f"notify:{fingerprint}",
        f"Notified @{state['owner']} and @{world.policy['authorized_team']}: {'; '.join(alerts)}",
    ):
        return
    body = (
        f"@{state['owner']} @{world.policy['authorized_team']} release `{state['tag']}` "
        f"needs attention:\n\n" + "\n".join(f"- {item}" for item in alerts)
    )
    world.source.post(f"/repos/{world.repo}/issues/{issue}/comments", {"body": body})


def _blocked(world: _World, issue: int, comment_id: int, state: dict[str, Any], alert: str, status: str = "Blocked") -> None:
    state["status"] = status
    state["alerts"] = [alert]
    _notify(world, issue, state, state["alerts"])
    _save(world, issue, comment_id, state)


def _cut_notes(world: _World, state: dict[str, Any], head: str) -> None:
    inputs = {key: str(state[key]) for key in ("version", "stage", "urgency")}
    _dispatch_once(world.agent, state, f"notes:{head}", world.agent_repo, "release-notes-cut.yml",
                   {**inputs, "dry_run": "false"})


def _dispatch_build(
    world: _World,
    state: dict[str, Any],
    key: str,
    identity: str,
    record: str,
    receipt: str,
    stage: str,
) -> None:
    fields = _parse_identity(identity)
    repo = str(world.down["automation_repo"])
    ref = _automation_ref(identity)
    if _tag_sha(world.automation, repo, ref) != fields["automation_sha"]:
        raise _Error("qualified production automation ref is missing or moved")
    _dispatch_once(
        world.automation,
        state,
        key,
        repo,
        str(world.down["build_workflow"]),
        {"identity": identity, "record": record, "receipt": receipt, "stage": stage},
        ref=ref,
    )


def _reconcile_one(world: _World, issue_obj: Mapping[str, Any], state: dict[str, Any], comment_id: int) -> None:
    issue = int(issue_obj["number"])
    state["alerts"] = []
    pull = _notes_pr(world, state)
    if pull is None or not pull.get("merged_at"):
        state["status"] = "Waiting for release-notes PR merge"
        if pull:
            state["notes_pr"] = {"number": pull["number"], "url": pull["html_url"], "merged": False}
        else:
            _cut_notes(world, state, _branch_head(world, str(state["branch"])))
        _notify(world, issue, state, state["alerts"])
        _save(world, issue, comment_id, state)
        return
    merge_sha = str(pull.get("merge_commit_sha", ""))
    state["notes_pr"] = {"number": pull["number"], "url": pull["html_url"], "merged": True}
    if not _SHA.fullmatch(merge_sha):
        _blocked(world, issue, comment_id, state, "Merged release-notes PR has no full merge commit SHA")
        return
    comments = _comments(world.source, world.repo, issue) if state.get("identity") else []
    publication = _published(world, state, comments)
    sealed = publication is None and bool(state.get("identity"))
    prewrite = _prewrite(comments, state, world.release_app_id) if sealed else None
    tag_sha = _tag_sha(world.source, world.repo, str(state["tag"])) if sealed else ""
    resume = prewrite is not None and tag_sha == state["source_sha"]
    if publication is not None and merge_sha != state["source_sha"]:
        raise _Error("published candidate no longer matches its release-notes merge")
    if publication is None and not resume and (head := _branch_head(world, str(state["branch"]))) != merge_sha:
        if state.get("identity"):
            _invalidate(state, f"branch advanced from candidate {merge_sha} to {head}")
        state["status"] = "Candidate invalidated; refreshing release notes"
        _cut_notes(world, state, head)
        _save(world, issue, comment_id, state)
        return
    if publication is None:
        ci_ok, state["checks"] = _ci_status(world, state, merge_sha)
        if not ci_ok:
            if state.get("identity") and not resume:
                _invalidate(state, "required CI no longer passes on the exact candidate")
            state["status"] = "Waiting for required CI on exact candidate"
            _save(world, issue, comment_id, state)
            return
    if publication is None and state.get("identity"):
        fields = _parse_identity(str(state["identity"]))
        automation_sha = fields["automation_sha"] if resume else _automation_head(world)
        if (
            fields["source_sha"] != merge_sha
            or fields["automation_sha"] != automation_sha
            or _plan(world, state, merge_sha, automation_sha, fields["qualification_nonce"])["plan_digest"]
            != fields["plan_digest"]
        ):
            if resume:
                alert = (f"Partially published tag {state['tag']} no longer satisfies its "
                         "sealed publication plan; manual completion required")
                _blocked(world, issue, comment_id, state, alert)
                return
            _invalidate(state, "candidate, automation revision, or publication plan changed")
    if publication is None and not state.get("identity"):
        _seal(world, state, merge_sha)
        _save(world, issue, comment_id, state)
    identity = str(state["identity"])
    fields = _parse_identity(identity)
    if publication is None:
        qual_state, qualification = _qualification(world, state)
        state["qualification"] = qualification
        if qual_state == "failed":
            state["dispatches"].pop(f"qualify:{identity}", None)
            state["alerts"] = [str(qualification["summary"])]
            _notify(world, issue, state, state["alerts"])
        if qual_state in {"missing", "failed"}:
            _dispatch_once(world.automation, state, f"qualify:{identity}",
                           str(world.down["automation_repo"]),
                           str(world.down["qualification_workflow"]), {"identity": identity})
        if qual_state != "passed":
            state["status"] = "Qualification failed" if qual_state == "failed" else "Qualification running"
            _save(world, issue, comment_id, state)
            return
        if tag_sha and (prewrite is None or tag_sha != state["source_sha"]):
            _blocked(world, issue, comment_id, state,
                     f"Tag {state['tag']} exists without a matching protected prewrite receipt")
            return
        state["status"] = "Ready to publish"
        state["publish_url"] = f"https://github.com/{world.agent_repo}/actions/workflows/release-publish.yml"
        key = f"publish:{identity}"
        if key not in state.setdefault("dispatches", {}):
            _save(world, issue, comment_id, state)
        _dispatch_once(
            world.agent,
            state,
            key,
            world.agent_repo,
            "release-publish.yml",
            {"issue": str(issue), "identity": identity},
        )
        _notify(world, issue, state, state["alerts"])
        _save(world, issue, comment_id, state)
        return
    state["status"] = "Published; verifying downstream outputs"
    state["outputs"]["github-release"] = {"verified": True, "detail": publication["release_url"]}
    receipt_id = str(publication["receipt_id"])
    record = f"{world.repo}#{issue}"

    def dispatch_build(key: str, stage: str) -> None:
        _dispatch_build(world, state, key, identity, record, receipt_id, stage)

    def stage_run(stage: str) -> Optional[Mapping[str, Any]]:
        return _run(
            world,
            str(world.down["build_workflow"]),
            f"Build {stage} {identity}",
            fields["automation_sha"],
            f"completion-{stage}-{state['release_id']}",
        )

    base = stage_run("base")
    if base is None:
        dispatch_build(f"build:base:{identity}", "base")
        _save(world, issue, comment_id, state)
        return
    if base.get("status") != "completed":
        _save(world, issue, comment_id, state)
        return
    if base.get("conclusion") != "success":
        dispatch_build(f"retry:base:{identity}:{base['id']}", "base")
        alert = f"Base production run {base['html_url']} concluded {base.get('conclusion')}"
        _blocked(world, issue, comment_id, state, alert, status="Downstream publication incomplete")
        return
    base_ok, base_outputs = _verify_base(world, state, base, receipt_id)
    state["outputs"].update(base_outputs)
    images_ok = all(
        state["outputs"].get(name, {}).get("verified")
        for name in ("docker-hub", "ghcr", "ecr")
    )
    dependent_expected = any(name in _expected(state) for name in ("bundle-images", "helm-chart"))
    dependent_ok = not dependent_expected
    if images_ok and dependent_expected:
        dependent = stage_run("dependent")
        if dependent is None:
            dispatch_build(f"build:dependent:{identity}", "dependent")
        elif dependent.get("status") == "completed" and dependent.get("conclusion") == "success":
            dependent_ok, dependent_outputs = _verify_dependent(
                world, state, dependent, receipt_id
            )
            state["outputs"].update(dependent_outputs)
        elif dependent.get("status") == "completed":
            state["alerts"] = [f"Dependent production run {dependent['html_url']} "
                               f"concluded {dependent.get('conclusion')}"]
            dispatch_build(f"retry:dependent:{identity}:{dependent['id']}", "dependent")
    latest_ok = _latest_ok(world)
    state["outputs"]["github-latest"] = {
        "verified": latest_ok,
        "detail": "canonical monotonic latest decision verified",
    }
    expected_ok = all(state["outputs"].get(name, {}).get("verified") for name in _expected(state))
    if base_ok and dependent_ok and expected_ok:
        completion = {
            "schema": 1,
            "identity": identity,
            "base_run_id": base["id"],
            "completed_at": _now(),
            "outputs": sorted(_expected(state)),
        }
        if _receipt(comments, "completion", world.controller_app_id, identity) is None:
            world.source.post(
                f"/repos/{world.repo}/issues/{issue}/comments",
                {"body": _comment_payload("completion", completion)},
            )
        _event(state, f"complete:{identity}", "Self-verified every required public output")
        state["status"] = "Complete"
        _save(world, issue, comment_id, state)
        world.source.patch(f"/repos/{world.repo}/issues/{issue}", {"state": "closed"})
        return
    if not state["alerts"]:
        state["alerts"] = [
            f"{name}: {state['outputs'].get(name, {}).get('detail', 'not yet verified')}"
            for name in _expected(state)
            if not state["outputs"].get(name, {}).get("verified")
        ]
    state["status"] = "Downstream publication incomplete"
    _notify(world, issue, state, state["alerts"])
    _save(world, issue, comment_id, state)


def _reconcile(world: _World, args: argparse.Namespace) -> None:
    if args.branch:
        _branch(world.policy, args.branch)
    failures: list[str] = []
    for issue, state, comment_id in _issue_states(world):
        number = int(issue["number"])
        if state is None:
            failures.append(f"#{number}: no single trusted controller state comment")
            continue
        if args.branch and state.get("branch") != args.branch:
            continue
        try:
            _reconcile_one(world, issue, state, comment_id)
        except Exception as exc:
            failures.append(f"#{number}: {exc}")
            try:
                _blocked(world, number, comment_id, state, str(exc), status="Controller error")
            except Exception as record_exc:
                failures.append(f"#{number}: failure record not saved: {record_exc}")
    if failures:
        raise _Error("reconcile failures: " + "; ".join(failures))


def _matching_rulesets(client: _GitHub, repo: str, tag: str) -> list[Mapping[str, Any]]:
    summaries = client.pages(f"/repos/{repo}/rulesets?targets=tag")
    details: list[Mapping[str, Any]] = []
    ref = PurePosixPath(f"/refs/tags/{tag}")
    for summary in summaries:
        if summary.get("target") != "tag" or summary.get("enforcement") != "active":
            continue
        detail = client.get(f"/repos/{repo}/rulesets/{summary['id']}")
        names = detail.get("conditions", {}).get("ref_name", {})
        includes = names.get("include", [])
        excludes = names.get("exclude", [])
        included = any(pattern == "~ALL" or ref.match("/" + str(pattern)) for pattern in includes)
        excluded = any(pattern == "~ALL" or ref.match("/" + str(pattern)) for pattern in excludes)
        if included and not excluded:
            details.append(detail)
    return details


def _tag_rules(client: _GitHub, repo: str, tag: str, app_id: int) -> None:
    rulesets = _matching_rulesets(client, repo, tag)
    creation_ok = False
    immutable_ok = False
    for ruleset in rulesets:
        rules = {item.get("type") for item in ruleset.get("rules", []) if isinstance(item, dict)}
        bypass = ruleset.get("bypass_actors")
        if not isinstance(bypass, list):
            continue
        if "creation" in rules:
            creation_ok = creation_ok or (
                len(bypass) == 1
                and bypass[0].get("actor_type") == "Integration"
                and bypass[0].get("actor_id") == app_id
                and bypass[0].get("bypass_mode") == "always"
            )
        if {"deletion", "update"}.issubset(rules) and not bypass:
            immutable_ok = True
    if not creation_ok:
        raise _Error("no active tag ruleset restricts creation to the protected release App")
    if not immutable_ok:
        raise _Error("no separate no-bypass ruleset prevents release-tag update and deletion")


def _environment_policy(
    client: _GitHub,
    repo: str,
    policies: Sequence[tuple[str, str]],
    *,
    reviewer: str = "",
    name: str = "release",
) -> None:
    env = client.get(f"/repos/{repo}/environments/{name}")
    reviewer_rules = [
        rule
        for rule in env.get("protection_rules", [])
        if rule.get("type") == "required_reviewers" and rule.get("reviewers")
    ]
    actual_reviewers = sorted(
        (str(item.get("type", "")), str(item["reviewer"].get("slug", "")))
        for rule in reviewer_rules
        for item in rule["reviewers"]
    )
    policy = env.get("deployment_branch_policy")
    custom = (
        isinstance(policy, dict)
        and policy.get("custom_branch_policies") is True
        and policy.get("protected_branches") is False
    )
    branches = client.get(
        f"/repos/{repo}/environments/{name}/deployment-branch-policies"
    )
    actual = sorted(
        (str(item.get("name", "")), str(item.get("type", "")))
        for item in branches.get("branch_policies", [])
        if isinstance(item, dict)
    )
    if (
        actual_reviewers != ([("Team", reviewer)] if reviewer else [])
        or any(rule.get("prevent_self_review") is not True for rule in reviewer_rules)
        or env.get("can_admins_bypass") is not False
        or not custom
        or branches.get("total_count") != len(policies)
        or actual != sorted(policies)
    ):
        raise _Error(f"{repo} {name} environment has incorrect reviewers or deployment refs")


def _environment(world: _World) -> None:
    repo = world.agent.get(f"/repos/{world.agent_repo}")
    reviewer = str(world.policy["authorized_team"]).split("/", 1)[1]
    for name in ("release", "release-control", "release-notes"):
        _environment_policy(world.agent, world.agent_repo, [(str(repo["default_branch"]), "branch")],
                            reviewer=reviewer if name == "release" else "", name=name)
    _environment_policy(world.automation, str(world.down["automation_repo"]), [("release-automation/*", "tag")])


def _revalidate(world: _World, issue: int, identity: str, *, resume: bool = False) -> tuple[dict[str, Any], str]:
    issue_obj = world.source.get(f"/repos/{world.repo}/issues/{issue}")
    if issue_obj.get("state") != "open":
        raise _Error("release issue is not open")
    state, _ = _state_from_comments(_comments(world.source, world.repo, issue), world.controller_app_id)
    if state.get("identity") != identity:
        raise _Error("approved identity is no longer current")
    fields = _parse_identity(identity)
    if not resume and _branch_head(world, str(state["branch"])) != fields["source_sha"]:
        raise _Error("release branch advanced after approval")
    pull = _notes_pr(world, state)
    if not pull or pull.get("merge_commit_sha") != fields["source_sha"] or not pull.get("merged_at"):
        raise _Error("release-notes merge no longer identifies the candidate")
    ci_ok, _ = _ci_status(world, state, fields["source_sha"])
    if not ci_ok:
        raise _Error("required CI no longer passes on the exact candidate")
    automation_sha = fields["automation_sha"]
    if not resume and _automation_head(world) != automation_sha:
        raise _Error("release automation changed after qualification")
    if (
        _plan(world, state, fields["source_sha"], automation_sha, fields["qualification_nonce"])["plan_digest"]
        != fields["plan_digest"]
    ):
        raise _Error("publication plan changed after approval")
    qual_state, live_qualification = _qualification(world, state)
    if qual_state != "passed":
        raise _Error("qualification evidence is no longer valid")
    if state.get("qualification", {}).get("manifest_sha256") != live_qualification.get("manifest_sha256"):
        raise _Error("recorded qualification does not match the live manifest")
    notes = _notes_section(_file(world.source, world.repo, _NOTES_FILE, fields["source_sha"]), fields["tag"])
    if _sha256(notes) != state["plan"]["notes_sha256"]:
        raise _Error("release notes changed after qualification")
    return state, notes


def _summary(state: Mapping[str, Any], identity: str) -> None:
    plan = state["plan"]
    text = "\n".join([
        f"# Approve Valkey {state['tag']}",
        "",
        f"- Immutable identity: `{identity}`",
        f"- Exact source commit: `{state['source_sha']}`",
        f"- Qualified automation commit: `{plan['automation_sha']}`",
        f"- Plan digest: `{plan['plan_digest']}`",
        f"- GitHub latest: `{str(plan['make_latest']).lower()}`",
        "- Expected outputs:",
        *(f"  - {name}" for name in plan["outputs"]),
        "",
    ])
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as output:
            output.write(text)
    print(text)


def _team_member(world: _World, actor: str) -> None:
    team = str(world.policy["authorized_team"])
    org, slug = team.split("/", 1)
    membership = world.source.maybe(
        f"/orgs/{org}/teams/{slug}/memberships/{urllib.parse.quote(actor)}"
    )
    if not isinstance(membership, dict) or membership.get("state") != "active":
        raise _Error(f"@{actor} is not an active member of @{team}")


def _publish(world: _World, args: argparse.Namespace) -> None:
    identity = args.identity
    fields = _parse_identity(identity)
    controller_sha = _required_env("GITHUB_SHA") if args.execute else ""
    run_id = _required_env("GITHUB_RUN_ID") if args.execute else ""
    comments = _comments(world.source, world.repo, args.issue)
    recorded, state_comment_id = _state_from_comments(comments, world.controller_app_id)
    controller_actor_id = int(next(c for c in comments if c.get("id") == state_comment_id)["user"]["id"])
    prewrite = _prewrite(comments, recorded, world.release_app_id)
    existing_tag = _tag_sha(world.source, world.repo, fields["tag"])
    resume = prewrite is not None and existing_tag == fields["source_sha"]
    _environment(world)
    automation_repo = str(world.down["automation_repo"])
    automation_ref = _automation_ref(identity)
    _tag_rules(world.source, world.repo, fields["tag"], world.release_app_id)
    _tag_rules(world.automation, automation_repo, automation_ref, world.release_app_id)
    immutable = world.source.maybe(f"/repos/{world.repo}/immutable-releases")
    if not isinstance(immutable, dict) or immutable.get("enabled") is not True:
        raise _Error(f"{world.repo} does not have immutable releases enabled")
    if args.execute:
        _team_member(world, args.approver)
    state, notes = _revalidate(world, args.issue, identity, resume=resume)
    if not args.execute:
        if _receipt(comments, "publication", world.release_app_id, identity) is not None:
            raise _Error("release identity already has a publication receipt")
        if existing_tag and not resume:
            raise _Error(f"tag {fields['tag']} already exists; external state is not authorization")
        _summary(state, identity)
        return
    comments = _comments(world.source, world.repo, args.issue)
    state, _ = _state_from_comments(comments, world.controller_app_id)
    if state.get("identity") != identity:
        raise _Error("approved identity changed during publication checks")
    prewrite = _prewrite(comments, state, world.release_app_id)
    existing_tag = _tag_sha(world.source, world.repo, fields["tag"])
    if prewrite is None:
        if existing_tag:
            raise _Error("tag exists without a trusted prewrite receipt")
        qualification = state["qualification"]
        payload = {
            "schema": 1,
            "identity": identity,
            "source_sha": fields["source_sha"],
            "plan_digest": fields["plan_digest"],
            "qualification_run_id": qualification["run_id"],
            "qualification_manifest_sha256": qualification["manifest_sha256"],
            "controller_sha": controller_sha,
            "run_id": run_id,
            "approver": args.approver,
            "recorded_at": _now(),
        }
        world.source.post(
            f"/repos/{world.repo}/issues/{args.issue}/comments",
            {"body": _comment_payload("prewrite", payload)},
        )
        comments = _comments(world.source, world.repo, args.issue)
        prewrite = _prewrite(comments, state, world.release_app_id)
    if prewrite is None:
        raise AssertionError("prewrite receipt disappeared")
    release_author_id = int(prewrite[0]["user"]["id"])
    automation_tag = _tag_sha(world.automation, automation_repo, automation_ref)
    if not automation_tag:
        world.automation.post(
            f"/repos/{automation_repo}/git/refs",
            {
                "ref": f"refs/tags/{automation_ref}",
                "sha": fields["automation_sha"],
            },
        )
    elif automation_tag != fields["automation_sha"]:
        raise _Error("immutable production automation ref points at the wrong commit")
    if not existing_tag:
        try:
            world.source.post(
                f"/repos/{world.repo}/git/refs",
                {"ref": f"refs/tags/{fields['tag']}", "sha": fields["source_sha"]},
            )
        except _Error:
            if _tag_sha(world.source, world.repo, fields["tag"]) != fields["source_sha"]:
                raise
    elif existing_tag != fields["source_sha"]:
        raise _Error("existing release tag does not point at the approved candidate")
    release = world.source.maybe(
        f"/repos/{world.repo}/releases/tags/{urllib.parse.quote(fields['tag'], safe='')}"
    )
    body = f"{notes.rstrip()}\n\n<!-- valkey-release-identity:v1 {identity} -->\n"
    if release is None:
        release = world.source.post(
            f"/repos/{world.repo}/releases",
            {
                "tag_name": fields["tag"],
                "target_commitish": fields["source_sha"],
                "name": fields["tag"],
                "body": body,
                "draft": False,
                "prerelease": state["stage"] != "ga",
                "make_latest": "true" if state["plan"]["make_latest"] else "false",
            },
        )
    if (
        _tag_sha(world.source, world.repo, fields["tag"]) != fields["source_sha"]
        or release.get("draft")
        or bool(release.get("prerelease")) != (state["stage"] != "ga")
        or release.get("name") != fields["tag"]
        or release.get("body") != body
        or release.get("author", {}).get("id") != release_author_id
    ):
        raise _Error("release postconditions do not match the approved identity")
    if not _latest_ok(world):
        raise _Error("GitHub latest pointer does not match the approved monotonic decision")
    comments = _comments(world.source, world.repo, args.issue)
    publication = _receipt(comments, "publication", world.release_app_id, identity)
    if publication is None:
        approval = prewrite[1]
        receipt_body = {
            "schema": 1,
            "identity": identity,
            "source_sha": fields["source_sha"],
            "plan_digest": fields["plan_digest"],
            "release_id": release["id"],
            "release_url": release["html_url"],
            "release_author_id": release_author_id,
            "controller_actor_id": controller_actor_id,
            "make_latest": bool(state["plan"]["make_latest"]),
            "controller_sha": approval["controller_sha"],
            "run_id": approval["run_id"],
            "approver": approval["approver"],
            "recorded_at": _now(),
        }
        comment = world.source.post(
            f"/repos/{world.repo}/issues/{args.issue}/comments",
            {"body": _comment_payload("publication", receipt_body)},
        )
        receipt_id = int(comment["id"])
    else:
        receipt_id = int(publication[0]["id"])
    world.automation.workflow_dispatch(
        automation_repo, str(world.down["build_workflow"]), automation_ref,
        {"identity": identity, "record": f"{world.repo}#{args.issue}", "receipt": str(receipt_id), "stage": "base"},
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default="release_policy.yml")
    parser.add_argument("--repo", default="valkey-io/valkey")
    parser.add_argument("--agent-repo", default=os.environ.get("GITHUB_REPOSITORY", "valkey-io/valkey-ci-agent"))
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--branch", required=True)
    start.add_argument("--intent", choices=("rc", "ga", "patch"), required=True)
    start.add_argument("--urgency", required=True)
    start.add_argument("--source-run", required=True)
    start.add_argument("--event-sender", required=True)
    start.add_argument("--dry-run", action="store_true")
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--branch", default="")
    publish = commands.add_parser("publish")
    publish.add_argument("--issue", type=int, required=True)
    publish.add_argument("--identity", required=True)
    publish.add_argument("--execute", action="store_true")
    publish.add_argument("--approver", default="")
    return parser


def _world(args: argparse.Namespace) -> _World:
    policy = _load_policy(args.policy, args.repo)
    return _World(
        policy,
        _GitHub(_required_env("SOURCE_TOKEN")),
        _GitHub(os.environ.get("AUTOMATION_TOKEN", _required_env("SOURCE_TOKEN"))),
        _GitHub(os.environ.get("AGENT_TOKEN", _required_env("SOURCE_TOKEN"))),
        agent_repo=args.agent_repo,
        controller_app_id=_app_id("CONTROLLER_APP_ID"),
        notes_app_id=_app_id("NOTES_APP_ID"),
        release_app_id=_app_id("RELEASE_APP_ID"),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        world = _world(args)
        if args.command == "start":
            _start(world, args)
        elif args.command == "reconcile":
            _reconcile(world, args)
        elif args.command == "publish":
            if args.execute and not args.approver:
                parser.error("publish --execute requires --approver")
            _publish(world, args)
        else:
            raise AssertionError(args.command)
    except _Error as exc:
        print(f"release controller: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
