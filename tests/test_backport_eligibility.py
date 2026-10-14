"""Daily backport eligibility: history bounds the branches, the model narrows them."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.backport import eligibility
from scripts.backport.eligibility import (
    COMMENT_MARKER,
    Candidate,
    LineOrigin,
    MergedSourcePR,
)
from scripts.backport.registry import load_registry
from scripts.release_notes.models import MergedPR


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


def _commit(repo: Path, path: str, text: str, subject: str) -> str:
    (repo / path).write_text(text)
    _git(repo, "add", path)
    _git(repo, "commit", "-q", "-m", subject)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def upstream(tmp_path: Path) -> dict[str, Any]:
    """unstable with two release forks.

    Both branches fork after ``old.c``. ``feat.c`` arrives on unstable later and
    is cherry-picked to 9.1 only. The fix to ``old.c`` is already on 9.0.
    """
    repo = tmp_path / "upstream"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "unstable")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    _commit(repo, "old.c", "int a;\nint b;\nint c;\n", "base (#1)")
    _git(repo, "branch", "9.0")
    _git(repo, "branch", "9.1")

    feature = _commit(repo, "feat.c", "int f(void) { return 0; }\n", "add feature (#100)")
    fix_feature = _commit(repo, "feat.c", "int f(void) { return 1; }\n", "fix feature (#101)")
    fix_old = _commit(repo, "old.c", "int a;\nint B;\nint c;\n", "fix old (#102)")
    additive = _commit(repo, "more.c", "int m;\n", "add more (#103)")

    _git(repo, "checkout", "-q", "9.1")
    _git(repo, "cherry-pick", feature)
    _git(repo, "checkout", "-q", "9.0")
    _git(repo, "cherry-pick", fix_old)
    _git(repo, "checkout", "-q", "unstable")
    return {
        "path": repo,
        "shas": {101: fix_feature, 102: fix_old, 103: additive},
    }


@pytest.fixture
def clone(upstream: dict[str, Any], tmp_path: Path) -> str:
    dest = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", "-q", "--no-checkout", str(upstream["path"]), str(dest)],
        check=True, capture_output=True,
    )
    return str(dest)


def _pr(number: int, sha: str) -> MergedSourcePR:
    return MergedSourcePR(
        pr=MergedPR(number=number, title=f"PR {number}", author="dev",
                    url=f"https://x/{number}", merge_commit_sha=sha),
        merged_at="2000-01-01T00:00:00Z",
    )


class TestTraceOrigins:
    def test_fix_to_code_on_every_branch_offers_every_branch(self, clone, upstream):
        # base (#1) predates both forks, but 9.0 already carries #102.
        origins = eligibility.trace_origins(
            clone, _pr(102, upstream["shas"][102]), ["9.1", "9.0"],
        )
        assert origins == (LineOrigin("base (#1)", origins[0].committed_at, ("9.1",)),)

    def test_fix_to_cherry_picked_feature_follows_the_cherry_pick(self, clone, upstream):
        # feat.c never reached 9.0. On 9.1 it has another SHA, found by PR number.
        origins = eligibility.trace_origins(
            clone, _pr(101, upstream["shas"][101]), ["9.1", "9.0"],
        )
        assert [(o.subject, o.branches) for o in origins] == [("add feature (#100)", ("9.1",))]

    def test_pr_already_on_every_branch_offers_none(self, clone, upstream):
        origins = eligibility.trace_origins(clone, _pr(102, upstream["shas"][102]), ["9.0"])
        assert origins is not None
        assert Candidate(_pr(102, upstream["shas"][102]), origins).branches == set()

    def test_branch_cut_after_the_merge_already_has_the_pr(self, clone, upstream):
        # A branch forked from unstable after #101 merged carries its commit
        # under the same SHA, even if mergedAt is later than the commit time.
        repo = upstream["path"]
        _git(repo, "branch", "9.2", upstream["shas"][101])
        dest = Path(clone).parent / "clone-92"
        subprocess.run(["git", "clone", "-q", "--no-checkout", str(repo), str(dest)],
                       check=True, capture_output=True)
        source = MergedSourcePR(
            pr=MergedPR(number=101, title="t", author="a", url="u",
                        merge_commit_sha=upstream["shas"][101]),
            merged_at="2999-01-01T00:00:00Z",
        )
        origins = eligibility.trace_origins(str(dest), source, ["9.2", "9.1"])
        assert origins is not None
        assert Candidate(source, origins).branches == {"9.1"}

    def test_purely_additive_pr_is_not_traced(self, clone, upstream):
        assert eligibility.trace_origins(clone, _pr(103, upstream["shas"][103]), ["9.1"]) is None


class TestParseVerdicts:
    def _parse(self, verdicts: Any) -> Any:
        return eligibility._parse_verdicts(
            json.dumps({"verdicts": verdicts}), {1: {"9.1", "9.0"}, 2: {"9.0"}},
        )

    def test_keeps_only_offered_branches_newest_first(self):
        parsed = self._parse([
            {"pr": 1, "backport": True, "branches": ["9.0", "8.0", "9.1"], "reason": " r "},
        ])
        assert parsed == {1: eligibility.Verdict(True, ("9.1", "9.0"), "r")}

    def test_positive_verdict_without_an_offered_branch_is_undecided(self):
        assert self._parse([{"pr": 2, "backport": True, "branches": ["9.1"]}]) == {}
        assert self._parse([{"pr": 2, "backport": True}]) == {}

    def test_negative_verdict_carries_no_branches(self):
        parsed = self._parse([{"pr": 2, "backport": False, "branches": ["9.0"], "reason": "feature"}])
        assert parsed == {2: eligibility.Verdict(False, (), "feature")}

    def test_drops_unknown_duplicate_and_malformed_entries(self):
        parsed = self._parse([
            {"pr": 3, "backport": True, "branches": ["9.0"]},
            {"pr": True, "backport": True, "branches": ["9.0"]},
            {"pr": 1, "backport": "yes", "branches": ["9.0"]},
            {"pr": 2, "backport": False},
            {"pr": 2, "backport": True, "branches": ["9.0"]},
            "junk",
        ])
        assert parsed == {2: eligibility.Verdict(False, (), "")}

    def test_unparseable_output_is_none(self):
        assert eligibility._parse_verdicts("no json here", {1: {"9.0"}}) is None


class TestPrFromNode:
    def _node(self, **overrides: Any) -> dict[str, Any]:
        node: dict[str, Any] = {
            "number": 7, "title": "t", "body": None, "url": "u",
            "mergedAt": "2026-01-01T00:00:00Z", "author": None,
            "mergeCommit": {"oid": "abc"},
            "projectItems": {"nodes": [
                {"project": {"number": 2, "owner": {"login": "Org"}}},
                {"project": {"number": 3, "owner": {"login": "someone-else"}}},
            ]},
            "comments": {"nodes": [{"body": "hi"}]},
        }
        node.update(overrides)
        return node

    def test_reads_projects_of_the_registry_owner_only(self):
        pr = eligibility._pr_from_node(self._node(), "org")
        assert pr is not None
        assert pr.project_numbers == frozenset({2})
        assert (pr.pr.body, pr.pr.author, pr.already_commented) == ("", "", False)

    def test_detects_an_earlier_eligibility_comment(self):
        node = self._node(comments={"nodes": [{"body": f"{COMMENT_MARKER}\nold"}]})
        pr = eligibility._pr_from_node(node, "org")
        assert pr is not None and pr.already_commented

    def test_skips_a_node_without_merge_commit(self):
        assert eligibility._pr_from_node(self._node(mergeCommit=None), "org") is None
        assert eligibility._pr_from_node({}, "org") is None


def test_discover_pages_and_queries_one_utc_day():
    calls: list[dict[str, Any]] = []
    pages = [
        {"search": {"nodes": [{"number": 9, "mergedAt": "x", "mergeCommit": {"oid": "a"}}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"}}},
        {"search": {"nodes": [{"number": 4, "mergedAt": "x", "mergeCommit": {"oid": "b"}}],
                    "pageInfo": {"hasNextPage": False}}},
    ]

    class Gql:
        def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            calls.append(variables)
            return pages[len(calls) - 1]

    prs = eligibility.discover_merged_prs(
        Gql(), repo="org/core", base_branch="unstable", date="2026-10-12", project_owner="org",
    )
    assert [pr.number for pr in prs] == [4, 9]
    assert calls[0]["q"] == (
        "repo:org/core is:pr is:merged base:unstable "
        "merged:2026-10-12T00:00:00Z..2026-10-12T23:59:59Z"
    )
    assert calls[1]["cursor"] == "c1"


_REGISTRY = """
repos:
  - repo: org/core
    project_owner: org
    language: c
    branches:
      - branch: "9.0"
        project_number: 10
      - branch: "9.1"
        project_number: 11
"""


def _repo_entry(tmp_path: Path):
    path = tmp_path / "repos.yml"
    path.write_text(_REGISTRY)
    return load_registry(str(path)).get_repo("org/core")


def test_render_comment_names_branches_and_projects(tmp_path):
    body = eligibility.render_comment(_repo_entry(tmp_path), ("9.1", "9.0"), "Fixes a crash.")
    assert body.splitlines()[0] == COMMENT_MARKER
    assert "eligible for backport to `9.1` and `9.0`." in body
    assert "Fixes a crash." in body
    assert "[9.1](https://github.com/orgs/org/projects/11)" in body
    assert "[9.0](https://github.com/orgs/org/projects/10)" in body


def test_render_comment_bounds_the_model_reason(tmp_path):
    body = eligibility.render_comment(
        _repo_entry(tmp_path), ("9.0",), "Pings @maintainers\n\n## Heading " + "x" * 1000,
    )
    reason = body.splitlines()[3]
    assert reason.startswith("Pings @\u200bmaintainers ## Heading")
    assert len(reason) <= 401
    assert "\n## Heading" not in body


class _FakeIssue:
    def __init__(self, number: int, posted: list[tuple[int, str]]) -> None:
        self._number = number
        self._posted = posted

    def create_comment(self, body: str) -> None:
        self._posted.append((self._number, body))


@pytest.fixture
def wired(monkeypatch, upstream, tmp_path):
    """run() against the fixture repo with GitHub and the model stubbed."""
    posted: list[tuple[int, str]] = []
    shas = upstream["shas"]
    nodes = [
        # #101 fixes the feature that only 9.1 carries.
        {"number": 101, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[101]}},
        # #102 is on the 9.1 project already and has landed on 9.0.
        {"number": 102, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[102]},
         "projectItems": {"nodes": [{"project": {"number": 11, "owner": {"login": "org"}}}]}},
        # #103 adds code only.
        {"number": 103, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[103]}},
        # #104 was handled by an earlier run.
        {"number": 104, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[101]},
         "comments": {"nodes": [{"body": COMMENT_MARKER}]}},
    ]

    class Gql:
        def __init__(self, token: str) -> None:
            pass

        def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            return {"search": {"nodes": nodes, "pageInfo": {"hasNextPage": False}}}

    repo = SimpleNamespace(
        default_branch="unstable",
        get_issue=lambda number: _FakeIssue(number, posted),
    )
    monkeypatch.setattr(eligibility, "Github", lambda **_: SimpleNamespace(get_repo=lambda _: repo))
    monkeypatch.setattr(eligibility, "GitHubGraphQLClient", Gql)
    cloned: list[str] = []

    def clone(repo_name: str, branch: str, dest: str, env: dict[str, str]) -> None:
        cloned.append(branch)
        subprocess.run(["git", "clone", "-q", "--no-checkout", str(upstream["path"]), dest],
                       check=True)

    monkeypatch.setattr(eligibility, "clone_target_branch", clone)
    return SimpleNamespace(entry=_repo_entry(tmp_path), posted=posted, cloned=cloned)


def _model(verdicts: list[dict[str, Any]], prompts: list[str] | None = None):
    def run_fn(prompt: str, **kwargs: Any) -> tuple[str, str, int]:
        assert kwargs["allowed_tools"] == ""
        if prompts is not None:
            prompts.append(prompt)
        return json.dumps({"verdicts": verdicts}), "", 0
    return run_fn


def test_run_comments_only_on_eligible_prs(wired):
    prompts: list[str] = []
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.1"], "reason": "Fixes f."}],
                      prompts),
    )
    assert result["errors"] == []
    assert wired.cloned == ["unstable"]
    assert result["prs"] == {
        "101": {"action": "commented", "branches": ["9.1"], "reason": "Fixes f."},
        "102": {"action": "not-eligible", "detail": "no release branch still needs the change"},
        "103": {"action": "not-eligible", "detail": "no existing code modified, or too large to trace"},
        "104": {"action": "already-commented"},
    }
    # Only #101 reaches the model, with the origin and its branch.
    payload = json.loads(prompts[0].split("## Pull requests (JSON)\n", 1)[1].split("\n## Output", 1)[0])
    assert [entry["number"] for entry in payload] == [101]
    assert payload[0]["line_origins"][0]["commit"] == "add feature (#100)"
    assert payload[0]["line_origins"][0]["candidate_branches"] == ["9.1"]
    assert len(wired.posted) == 1
    number, body = wired.posted[0]
    assert number == 101 and body.startswith(COMMENT_MARKER) and "`9.1`" in body


def test_run_dry_run_posts_nothing(wired):
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t", dry_run=True,
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.1"], "reason": "r"}]),
    )
    assert result["prs"]["101"]["action"] == "would-comment"
    assert wired.posted == []


def test_run_negative_verdict_posts_nothing(wired):
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": False, "reason": "Refactor."}]),
    )
    assert result["prs"]["101"] == {"action": "not-eligible", "detail": "Refactor."}
    assert result["errors"] == [] and wired.posted == []


def test_run_missing_verdict_is_an_error_without_a_comment(wired):
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t", run_fn=_model([]),
    )
    assert result["prs"]["101"] == {"action": "undecided"}
    assert result["errors"] == ["#101: no verdict"]
    assert wired.posted == []
