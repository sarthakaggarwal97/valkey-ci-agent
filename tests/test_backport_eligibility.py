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
from scripts.release_notes import code_age
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
        source_commit_shas=(sha,),
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
            source_commit_shas=(upstream["shas"][101],),
        )
        origins = eligibility.trace_origins(str(dest), source, ["9.2", "9.1"])
        assert origins is not None
        assert Candidate(source, origins).branches == {"9.1"}

    def test_unreadable_branch_raises_instead_of_offering_nothing(self, clone, upstream):
        # A branch whose history cannot be read must not pass for one that
        # lacks the code.
        with pytest.raises(subprocess.CalledProcessError):
            eligibility.trace_origins(clone, _pr(101, upstream["shas"][101]), ["9.9"])

    def test_unreadable_ancestry_raises(self, clone, upstream, monkeypatch):
        def broken(self, sha):
            raise subprocess.CalledProcessError(129, ["git", "for-each-ref"])

        monkeypatch.setattr(eligibility.BranchHistory, "containing", broken)
        with pytest.raises(subprocess.CalledProcessError):
            eligibility.trace_origins(clone, _pr(101, upstream["shas"][101]), ["9.1"])

    def test_pr_adding_only_new_files_has_no_origins(self, clone, upstream):
        assert eligibility.trace_origins(clone, _pr(103, upstream["shas"][103]), ["9.1"]) == ()

    def test_too_large_to_blame_is_unknown(self, clone, upstream, monkeypatch):
        monkeypatch.setattr(code_age, "_MAX_FILES", 0)
        with pytest.raises(eligibility.OriginsUnknown):
            eligibility.trace_origins(clone, _pr(101, upstream["shas"][101]), ["9.1"])

    @pytest.mark.parametrize(
        ("after", "expected"),
        [
            ("int a;\nif (!a) return;\nint b;\nint c;\n", "middle"),
            ("/* guard */\nint a;\nint b;\nint c;\n", "top"),
            ("int a;\nint b;\nint c;\nfree(a);\n", "end"),
        ],
    )
    def test_insertion_only_fix_is_traced_to_the_code_around_it(
        self, tmp_path, after, expected,
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        _git(repo, "init", "-q", "-b", "unstable")
        _git(repo, "config", "user.email", "t@example.com")
        _git(repo, "config", "user.name", "T")
        _commit(repo, "old.c", "int a;\nint b;\nint c;\n", "base (#1)")
        _git(repo, "branch", "9.0")
        fix = _commit(repo, "old.c", after, f"add a check at the {expected} (#2)")
        clone = tmp_path / "clone"
        subprocess.run(["git", "clone", "-q", "--no-checkout", str(repo), str(clone)],
                       check=True, capture_output=True)

        origins = eligibility.trace_origins(str(clone), _pr(2, fix), ["9.0"])
        assert [(o.subject, o.branches) for o in origins] == [("base (#1)", ("9.0",))]

    def test_blame_looks_through_ignored_reformats(self, tmp_path):
        # 9.0 forks before a reformat it never receives. Blame must reach the
        # commit that wrote the line, which 9.0 carries.
        repo = tmp_path / "repo"
        repo.mkdir()
        _git(repo, "init", "-q", "-b", "unstable")
        _git(repo, "config", "user.email", "t@example.com")
        _git(repo, "config", "user.name", "T")
        _commit(repo, "old.c", "int  a;\nint  b;\n", "write code (#1)")
        _git(repo, "branch", "9.0")
        reformat = _commit(repo, "old.c", "int a;\nint b;\n", "reformat (#2)")
        _commit(repo, ".git-blame-ignore-revs", f"# formatting\n{reformat}\n", "ignore it (#3)")
        fix = _commit(repo, "old.c", "int a;\nint B;\n", "fix b (#4)")
        clone = tmp_path / "clone"
        subprocess.run(["git", "clone", "-q", "--no-checkout", str(repo), str(clone)],
                       check=True, capture_output=True)

        origins = eligibility.trace_origins(str(clone), _pr(4, fix), ["9.0"])
        assert [(o.subject, o.branches) for o in origins] == [("write code (#1)", ("9.0",))]

    def test_unreadable_merge_commit_is_a_source_change_error(self, clone):
        source = _pr(999, "0" * 40)
        with pytest.raises(eligibility.SourceChangeError):
            eligibility.trace_origins(clone, source, ["9.1"])

    def test_multi_commit_squash_traces_the_complete_change(self, tmp_path):
        upstream = tmp_path / "upstream"
        upstream.mkdir()
        _git(upstream, "init", "-q", "-b", "main")
        _git(upstream, "config", "user.email", "t@example.com")
        _git(upstream, "config", "user.name", "T")
        _commit(upstream, "value.txt", "one\n", "introduce one (#1)")
        _commit(upstream, "value.txt", "one\ntwo\n", "introduce two (#2)")
        _git(upstream, "branch", "9.0")
        _git(upstream, "checkout", "-q", "-b", "source")
        first = _commit(upstream, "value.txt", "ONE\ntwo\n", "change one")
        second = _commit(upstream, "value.txt", "ONE\nTWO\n", "change two")
        _git(upstream, "checkout", "-q", "main")
        _git(upstream, "merge", "-q", "--squash", "source")
        _git(upstream, "commit", "-q", "-m", "squash change (#10)")
        squash = _git(upstream, "rev-parse", "HEAD")

        clone = tmp_path / "clone"
        subprocess.run(
            ["git", "clone", "-q", "--no-checkout", str(upstream), str(clone)],
            check=True,
            capture_output=True,
        )
        source = MergedSourcePR(
            pr=MergedPR(number=10, title="t", author="a", url="u",
                        merge_commit_sha=squash),
            merged_at="2000-01-01T00:00:00Z",
            source_commit_shas=(first, second),
        )

        origins = eligibility.trace_origins(str(clone), source, ["9.0"])
        assert origins is not None
        assert {(origin.subject, origin.branches) for origin in origins} == {
            ("introduce one (#1)", ("9.0",)),
            ("introduce two (#2)", ("9.0",)),
        }


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
            "commits": {
                "nodes": [{"commit": {"oid": "abc"}}],
                "pageInfo": {"hasNextPage": False},
            },
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
        node = self._node(comments={"nodes": [
            {"body": f"{COMMENT_MARKER}\nold", "viewerDidAuthor": True},
        ]})
        pr = eligibility._pr_from_node(node, "org")
        assert pr is not None and pr.already_commented

    def test_ignores_a_forged_eligibility_comment(self):
        node = self._node(comments={"nodes": [
            {"body": f"{COMMENT_MARKER}\nforged", "viewerDidAuthor": False},
        ]})
        pr = eligibility._pr_from_node(node, "org")
        assert pr is not None and not pr.already_commented

    def test_unreadable_project_item_leaves_projects_unknown(self):
        pr = eligibility._pr_from_node(self._node(projectItems={"nodes": [None]}), "org")
        assert pr is not None and pr.project_numbers is None

    def test_unlisted_project_page_leaves_projects_unknown(self):
        node = self._node()
        node["projectItems"]["pageInfo"] = {"hasNextPage": True}
        pr = eligibility._pr_from_node(node, "org")
        assert pr is not None and pr.project_numbers is None

    def test_skips_a_node_without_merge_commit(self):
        assert eligibility._pr_from_node(self._node(mergeCommit=None), "org") is None
        assert eligibility._pr_from_node({}, "org") is None

    def test_reads_source_commit_listing_completeness(self):
        node = self._node(commits={
            "nodes": [{"commit": {"oid": "one"}}, {"commit": {"oid": "two"}}],
            "pageInfo": {"hasNextPage": True},
        })
        pr = eligibility._pr_from_node(node, "org")
        assert pr is not None
        assert pr.source_commit_shas == ("one", "two")
        assert pr.source_commits_complete is False


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


def test_discover_covers_a_window_of_days():
    calls: list[dict[str, Any]] = []

    class Gql:
        def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            calls.append(variables)
            return {"search": {"nodes": [], "pageInfo": {"hasNextPage": False}}}

    eligibility.discover_merged_prs(
        Gql(), repo="org/core", base_branch="unstable", date="2026-03-01",
        project_owner="org", days=3,
    )
    assert calls[0]["q"].endswith("merged:2026-02-27T00:00:00Z..2026-03-01T23:59:59Z")


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
    assert reason.startswith("> Pings @\u200bmaintainers ## Heading")
    assert len(reason) <= 402
    assert "\n## Heading" not in body


def test_render_comment_reason_cannot_carry_markup(tmp_path):
    body = eligibility.render_comment(
        _repo_entry(tmp_path), ("9.0",),
        "## [details](https://evil.example/x) ![i](http://e/i.png) <img src=x> `c` www.evil.example",
    )
    quote = body.splitlines()[3]
    assert quote.startswith("> details")
    reason = quote[2:]
    for char in "[]()<>!`":
        assert char not in reason
    assert "://" not in reason and "www." not in reason


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
        {"number": 101, "mergedAt": "2000-01-01T00:00:00Z",
         "mergeCommit": {"oid": shas[101]},
         "commits": {"nodes": [{"commit": {"oid": shas[101]}}]}},
        # #102 is on the 9.1 project already and has landed on 9.0.
        {"number": 102, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[102]},
         "commits": {"nodes": [{"commit": {"oid": shas[102]}}]},
         "projectItems": {"nodes": [{"project": {"number": 11, "owner": {"login": "org"}}}]}},
        # #103 adds code only.
        {"number": 103, "mergedAt": "2000-01-01T00:00:00Z",
         "mergeCommit": {"oid": shas[103]},
         "commits": {"nodes": [{"commit": {"oid": shas[103]}}]}},
        # #104 was handled by an earlier run.
        {"number": 104, "mergedAt": "2000-01-01T00:00:00Z", "mergeCommit": {"oid": shas[101]},
         "commits": {"nodes": [{"commit": {"oid": shas[101]}}]},
         "comments": {"nodes": [{"body": COMMENT_MARKER, "viewerDidAuthor": True}]}},
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
    return SimpleNamespace(entry=_repo_entry(tmp_path), posted=posted, cloned=cloned, nodes=nodes)


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
        "103": {"action": "not-eligible", "detail": "only adds new files"},
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


def test_run_unreadable_projects_fail_only_that_pr(wired):
    wired.nodes.append({
        "number": 105, "mergedAt": "2000-01-01T00:00:00Z",
        "mergeCommit": {"oid": wired.nodes[0]["mergeCommit"]["oid"]},
        "projectItems": {"nodes": [None]},
    })
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.1"], "reason": "r"}]),
    )
    assert result["prs"]["105"] == {"action": "undecided"}
    assert result["prs"]["101"]["action"] == "commented"
    assert result["errors"] == ["#105: project items unreadable"]
    assert [number for number, _ in wired.posted] == [101]


def test_run_skips_history_when_every_branch_is_already_queued(wired, monkeypatch):
    wired.nodes[1]["projectItems"]["nodes"].append(
        {"project": {"number": 10, "owner": {"login": "org"}}}
    )
    real_trace_origins = eligibility.trace_origins
    traced: list[int] = []

    def trace(repo_dir, source, branches, **kwargs):
        assert branches
        traced.append(source.number)
        return real_trace_origins(repo_dir, source, branches, **kwargs)

    monkeypatch.setattr(eligibility, "trace_origins", trace)
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": False, "reason": "r"}]),
    )

    assert 102 not in traced
    assert result["prs"]["102"] == {
        "action": "not-eligible",
        "detail": "no release branch still needs the change",
    }


def test_run_source_change_failure_is_contained_to_that_pr(wired, monkeypatch):
    real_trace_origins = eligibility.trace_origins

    def trace(repo_dir, source, branches, **kwargs):
        if source.number == 101:
            raise eligibility.SourceChangeError("unreadable merge history")
        return real_trace_origins(repo_dir, source, branches, **kwargs)

    monkeypatch.setattr(eligibility, "trace_origins", trace)
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t", run_fn=_model([]),
    )

    assert result["prs"]["101"] == {
        "action": "undecided",
        "detail": "complete merged change needs manual eligibility review",
    }
    assert result["errors"] == ["#101: unreadable merge history"]


def test_run_origin_cap_preserves_evidence_for_every_branch(wired, monkeypatch):
    monkeypatch.setattr(eligibility, "_MAX_ORIGINS", 1)
    monkeypatch.setattr(eligibility, "trace_origins", lambda *_, **__: (
        LineOrigin("old (#1)", "2000-01-01T00:00:00+00:00", ("9.0",)),
        LineOrigin("new (#2)", "2001-01-01T00:00:00+00:00", ("9.1",)),
    ))
    prompts: list[str] = []
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.0"]}], prompts),
    )
    assert "new (#2)" in prompts[0] and "old (#1)" in prompts[0]
    assert result["prs"]["101"]["action"] == "commented"
    assert result["prs"]["101"]["branches"] == ["9.0"]


def test_parse_git_timestamp_accepts_utc_z_on_python_39():
    assert eligibility._parse_git_timestamp("2026-10-14T20:09:22Z").isoformat() == (
        "2026-10-14T20:09:22+00:00"
    )


def test_multi_commit_rebase_is_undecided_instead_of_partially_traced(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    _commit(repo, "value.txt", "base\n", "base")
    _git(repo, "checkout", "-q", "-b", "source")
    first = _commit(repo, "value.txt", "first\n", "first source commit")
    second = _commit(repo, "other.txt", "second\n", "second source commit")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "main.txt", "advance\n", "advance main")
    _git(repo, "checkout", "-q", "source")
    _git(repo, "rebase", "-q", "main")
    rebased_tip = _git(repo, "rev-parse", "HEAD")
    assert rebased_tip != second

    source = MergedSourcePR(
        pr=MergedPR(number=10, title="t", author="a", url="u",
                    merge_commit_sha=rebased_tip),
        merged_at="2026-01-01T00:00:00Z",
        source_commit_shas=(first, second),
    )
    with pytest.raises(eligibility.SourceChangeError, match="rebase-merged"):
        eligibility.trace_origins(str(repo), source, ["9.0"])


def test_run_any_tracing_failure_is_contained_to_that_pr(wired, monkeypatch):
    real_trace_origins = eligibility.trace_origins

    def trace(repo_dir, source, branches, **kwargs):
        if source.number == 103:
            raise subprocess.CalledProcessError(128, ["git", "log"])
        return real_trace_origins(repo_dir, source, branches, **kwargs)

    monkeypatch.setattr(eligibility, "trace_origins", trace)
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.1"], "reason": "r"}]),
    )
    assert result["prs"]["103"] == {"action": "undecided", "detail": "tracing failed"}
    assert len(result["errors"]) == 1 and result["errors"][0].startswith("#103: tracing failed")
    assert [number for number, _ in wired.posted] == [101]


def test_run_untraceable_pr_is_undecided(wired, monkeypatch):
    def trace(repo_dir, source, branches, **kwargs):
        raise eligibility.OriginsUnknown("touches more than 40 files")

    monkeypatch.setattr(eligibility, "trace_origins", trace)
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t", run_fn=_model([]),
    )
    assert result["prs"]["101"] == {
        "action": "undecided", "detail": "could not trace: touches more than 40 files",
    }
    assert "#101: could not trace: touches more than 40 files" in result["errors"]


def test_run_comments_after_each_batch(wired, monkeypatch):
    monkeypatch.setattr(eligibility, "_BATCH_SIZE", 1)
    monkeypatch.setattr(eligibility, "trace_origins", lambda *_, **__: (
        LineOrigin("old (#1)", "2000-01-01T00:00:00+00:00", ("9.0",)),
    ))
    calls: list[int] = []

    def run_fn(prompt: str, **kwargs: Any) -> tuple[str, str, int]:
        calls.append(len(wired.posted))
        if len(calls) == 1:
            return json.dumps({"verdicts": [
                {"pr": 101, "backport": True, "branches": ["9.0"], "reason": "r"},
            ]}), "", 0
        return "", "timeout after 900s", 1

    result = eligibility.run(repo_entry=wired.entry, date="2026-10-12", token="t", run_fn=run_fn)
    # The second batch is judged only after the first batch's comment is up.
    assert calls[:2] == [0, 1]
    assert result["prs"]["101"]["action"] == "commented"
    assert result["prs"]["103"] == {"action": "undecided"}


def test_run_ignores_a_registered_branch_that_does_not_exist(wired, upstream):
    _git(upstream["path"], "branch", "-D", "9.0")
    result = eligibility.run(
        repo_entry=wired.entry, date="2026-10-12", token="t",
        run_fn=_model([{"pr": 101, "backport": True, "branches": ["9.1"], "reason": "r"}]),
    )
    assert result["errors"] == []
    assert result["prs"]["101"]["action"] == "commented"
